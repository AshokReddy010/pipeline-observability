"""The network layer, with the HTTP call replaced: retries, 404 handling, and each source's fetch logic."""

import io
import json
import urllib.error
from datetime import timedelta

import pytest

from pipeobs import sources
from pipeobs.sources import FetchError
from tests.conftest import NOW, chicago_rows, usgs_doc, wiki_doc


def test_get_json_retries_then_succeeds(monkeypatch):
    calls = []

    def urlopen(req, timeout):
        calls.append(req.full_url)
        if len(calls) < 3:
            raise urllib.error.URLError("reset")
        return io.BytesIO(json.dumps({"ok": 1}).encode())

    monkeypatch.setattr(sources.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(sources.time, "sleep", lambda s: None)
    assert sources.get_json("https://x") == {"ok": 1} and len(calls) == 3


def test_get_json_gives_up_and_404_is_immediate(monkeypatch):
    monkeypatch.setattr(sources.time, "sleep", lambda s: None)
    monkeypatch.setattr(
        sources.urllib.request, "urlopen", lambda req, timeout: (_ for _ in ()).throw(urllib.error.URLError("down"))
    )
    with pytest.raises(FetchError, match="down"):
        sources.get_json("https://x")
    monkeypatch.setattr(
        sources.urllib.request,
        "urlopen",
        lambda req, timeout: (_ for _ in ()).throw(urllib.error.HTTPError(req.full_url, 404, "nf", {}, None)),
    )
    with pytest.raises(FetchError, match="404"):
        sources.get_json("https://x")


def test_fetch_usgs_and_backfill(monkeypatch):
    monkeypatch.setattr(sources, "get_json", lambda url, **k: usgs_doc(400, now=NOW))
    b = sources.fetch_usgs(NOW)
    assert b.partition == "2026-10-18T12:00Z" and len(b.df) == 400
    bs = sources.backfill_usgs(NOW, 1)
    assert len(bs) == 4 and all(x.meta["backfill"] for x in bs)
    assert bs[-1].fetched_at == NOW - timedelta(hours=6)  # the live fetch covers the latest window
    assert 0 < len(bs[-1].df) < 400


def test_fetch_wiki_falls_back_a_day(monkeypatch):
    def gj(url, **k):
        if url.endswith("/2026/10/17"):
            raise FetchError("404 Not Found: " + url)
        return wiki_doc("2026-10-16", n=50)

    monkeypatch.setattr(sources, "get_json", gj)
    b = sources.fetch_wiki(NOW)
    assert b.partition == "2026-10-16" and len(b.df) == 50
    assert [b.partition for b in sources.backfill_wiki(NOW, 3)] == ["2026-10-14", "2026-10-15", "2026-10-16"]


def test_fetch_chicago_takes_the_day_before_the_newest(monkeypatch):
    def gj(url, **k):
        if "max%28date%29" in url:
            return [{"d": "2026-10-10T23:50:00.000"}]
        assert "2026-10-09T00%3A00%3A00" in url
        return chicago_rows("2026-10-09", 30)

    monkeypatch.setattr(sources, "get_json", gj)
    b = sources.fetch_chicago(NOW)
    assert b.partition == "2026-10-09" and len(b.df) == 30
    monkeypatch.setattr(sources, "get_json", lambda url, **k: [{}])
    with pytest.raises(FetchError):
        sources.fetch_chicago(NOW - timedelta(days=1))
