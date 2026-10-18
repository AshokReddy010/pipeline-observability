import json
from dataclasses import replace
from datetime import timedelta

import pytest

from pipeobs import alert, runner
from pipeobs.cli import main
from pipeobs.sources import SOURCES, Batch, FetchError, parse_usgs
from pipeobs.status import build_status, incidents_for
from tests.conftest import NOW, usgs_doc


@pytest.fixture
def fake_usgs(monkeypatch):
    """Replace the network fetch with a scripted sequence of payloads."""
    script = []

    def fetch(now):
        item = script.pop(0)
        if isinstance(item, Exception):
            raise item
        return Batch("usgs_earthquakes", now.strftime("%Y-%m-%dT%H:00Z"), parse_usgs(item(now)), now, 0.2)

    src = replace(
        SOURCES["usgs_earthquakes"],
        fetch=fetch,
        backfill=lambda now, days: [
            Batch(
                "usgs_earthquakes",
                f"b{i}",
                parse_usgs(usgs_doc(300, seed=i, now=now - timedelta(hours=6 * (days - i)))),
                now - timedelta(hours=6 * (days - i)),
                0,
                {"backfill": True},
            )
            for i in range(days)
        ],
    )
    monkeypatch.setitem(SOURCES, "usgs_earthquakes", src)
    return script


def test_run_records_and_dedupes(store, fake_usgs):
    fake_usgs += [lambda now: usgs_doc(300, now=now)] * 2
    runner.run_all(store, ["usgs_earthquakes"], now=NOW)
    runner.run_all(store, ["usgs_earthquakes"], now=NOW + timedelta(minutes=10))  # same hour -> same partition
    assert len(store.load("usgs_earthquakes")) == 1


def test_fetch_error_is_recorded_not_raised(store, fake_usgs):
    fake_usgs.append(FetchError("503 Service Unavailable"))
    out = runner.run_all(store, ["usgs_earthquakes"], now=NOW)
    rec = out["usgs_earthquakes"][0]
    assert rec["status"] == "fail" and "503" in rec["error"]


def test_backfill_if_empty_then_live(store, fake_usgs):
    fake_usgs.append(lambda now: usgs_doc(300, now=now))
    runner.run_all(store, ["usgs_earthquakes"], now=NOW, backfill_if_empty=3)
    hist = store.load("usgs_earthquakes")
    assert len(hist) == 4 and sum(h["backfill"] for h in hist) == 3


def test_incident_lifecycle_and_status(store, fake_usgs):
    ok = lambda now: usgs_doc(300, now=now)  # noqa: E731
    fake_usgs += [ok] * 6 + [FetchError("timeout"), FetchError("timeout"), ok, ok]
    for i in range(10):
        runner.run_all(store, ["usgs_earthquakes"], now=NOW + timedelta(hours=i))
    incs = incidents_for("usgs_earthquakes", store.load("usgs_earthquakes"))
    assert len(incs) == 1 and incs[0]["monitor"] == "availability" and incs[0]["runs"] == 2
    assert incs[0]["duration_hours"] == 2.0
    st = build_status(store, now=NOW + timedelta(hours=9, minutes=30))
    s = next(x for x in st["sources"] if x["name"] == "usgs_earthquakes")
    assert s["status"] == "pass" and s["uptime_7d"] == 0.8 and s["open_incidents"] == 0
    assert st["mttr_hours_30d"] == 2.0 and len(s["runs"]) == 10


def test_stale_source_opens_freshness_incident(store, fake_usgs):
    fake_usgs.append(lambda now: usgs_doc(300, now=now))
    runner.run_all(store, ["usgs_earthquakes"], now=NOW)
    st = build_status(store, now=NOW + timedelta(hours=12))
    s = next(x for x in st["sources"] if x["name"] == "usgs_earthquakes")
    assert s["status"] == "fail"
    assert st["incidents"][0]["monitor"] == "freshness" and st["incidents"][0]["resolved"] is None


def test_unknown_sources_without_history(store):
    st = build_status(store, now=NOW)
    assert {s["status"] for s in st["sources"]} == {"unknown"} and st["mttr_hours_30d"] is None


def test_store_ignores_torn_lines_and_caps(store, monkeypatch):
    monkeypatch.setattr("pipeobs.store.MAX_RUNS", 3)
    store.append("x", [{"run_at": str(i), "partition": str(i)} for i in range(5)])
    store.path("x").write_text(store.path("x").read_text() + '{"broken\n')
    assert [r["run_at"] for r in store.load("x")] == ["2", "3", "4"]


INC = {
    "source": "s",
    "monitor": "volume",
    "started": "t",
    "runs": 8,
    "first_message": "a",
    "last_message": "b",
    "resolved": None,
}


def test_alert_plan():
    issues = [
        {"number": 1, "title": "[s/volume] volume failing for s"},
        {"number": 2, "title": "[s/schema] gone"},
        {"number": 3, "title": "unrelated"},
    ]
    todo = alert.plan([INC, {**INC, "monitor": "freshness"}, {**INC, "monitor": "x", "backfill": True}], issues)
    assert [i["monitor"] for i in todo["open"]] == ["freshness"]
    assert [i["number"] for i, _ in todo["update"]] == [1]
    assert [i["number"] for i in todo["close"]] == [2]


def test_alert_sync_calls_gh():
    calls = []

    def fake(args):
        calls.append(args[:2])
        return json.dumps([{"number": 9, "title": "[s/schema] old"}]) if args[:2] == ["issue", "list"] else ""

    out = alert.sync({"incidents": [INC]}, run=fake)
    assert out == {"open": 1, "update": 0, "close": 1}
    assert ["issue", "create"] in calls and ["issue", "close"] in calls


def test_cli_status_and_dry_run_alert(tmp_path, store, fake_usgs, capsys, monkeypatch):
    fake_usgs.append(lambda now: usgs_doc(300, now=now))
    runner.run_all(store, ["usgs_earthquakes"])
    out = tmp_path / "status.json"
    assert main(["--history", str(store.root), "status", "-o", str(out)]) == 0
    st = json.loads(out.read_text())
    assert st["sources"][0]["name"] == "usgs_earthquakes"
    monkeypatch.setattr(alert, "gh", lambda args: "[]")
    monkeypatch.setattr(alert.shutil, "which", lambda _: "/usr/bin/gh")
    assert main(["alert", "--status", str(out), "--dry-run"]) == 0
