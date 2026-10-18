"""The monitored feeds: three real public datasets with very different shapes, cadences and failure modes.

Each source knows how to fetch one batch (network) and how to parse it (pure, tested with fixtures), plus what
"healthy" means for it: how fresh it must be and which columns to profile.

    usgs_earthquakes   USGS real-time feed, every earthquake worldwide in the last 24 hours (updates each minute)
    wikipedia_top      Wikimedia's top-1000 most viewed English Wikipedia articles for one day (published daily)
    chicago_crimes     City of Chicago reported crimes for one day (published daily, about a week behind)
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd

USER_AGENT = "pipeline-observability/1.0 (https://github.com/AshokReddy010/pipeline-observability)"


class FetchError(RuntimeError):
    """The source could not be reached or returned something unusable."""


@dataclass
class Batch:
    source: str
    partition: str  # which slice of the feed this is (a day, or a run time for rolling feeds)
    df: pd.DataFrame
    fetched_at: datetime
    fetch_seconds: float = 0.0
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Source:
    name: str
    title: str
    homepage: str
    cadence: str  # human description of how often new data appears
    freshness_sla_hours: float  # newest record older than this -> freshness fails
    event_time: str  # column holding each record's timestamp
    numeric: dict[str, list[float]]  # column -> fixed histogram bin edges (stable PSI across runs)
    categorical: tuple[str, ...]  # columns profiled as category shares
    seasonal: bool  # compare volume with the same weekday
    fetch: Callable[[datetime], Batch]
    backfill: Callable[[datetime, int], list[Batch]]
    period_hours: float = 0.0  # records stamped at the start of a period (a day) are complete this much later


# ------------------------------------------------------------------ http
def get_json(url: str, timeout: float = 60, retries: int = 3) -> Any:
    last: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            if error.code == 404:
                raise FetchError(f"404 Not Found: {url}") from error
            last = error
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
            last = error
        time.sleep(2**attempt)
    raise FetchError(f"{url}: {type(last).__name__}: {last}")


def _timed(fn: Callable[[], Any]) -> tuple[Any, float]:
    t = time.monotonic()
    out = fn()
    return out, round(time.monotonic() - t, 3)


# ------------------------------------------------------------------ USGS earthquakes
USGS_DAY = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
USGS_MONTH = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"


def parse_usgs(doc: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for f in doc.get("features", []):
        p = f.get("properties") or {}
        coords = (f.get("geometry") or {}).get("coordinates") or [None, None, None]
        rows.append(
            {
                "id": f.get("id"),
                "time": p.get("time"),
                "updated": p.get("updated"),
                "mag": p.get("mag"),
                "mag_type": p.get("magType"),
                "place": p.get("place"),
                "type": p.get("type"),
                "status": p.get("status"),
                "net": p.get("net"),
                "sig": p.get("sig"),
                "tsunami": p.get("tsunami"),
                "longitude": coords[0],
                "latitude": coords[1],
                "depth_km": coords[2] if len(coords) > 2 else None,
            }
        )
    df = pd.DataFrame(
        rows,
        columns=[
            "id",
            "time",
            "updated",
            "mag",
            "mag_type",
            "place",
            "type",
            "status",
            "net",
            "sig",
            "tsunami",
            "longitude",
            "latitude",
            "depth_km",
        ],
    )
    for c in ("time", "updated"):
        df[c] = pd.to_datetime(df[c], unit="ms", utc=True)
    return df


def fetch_usgs(now: datetime) -> Batch:
    doc, secs = _timed(lambda: get_json(USGS_DAY))
    df = parse_usgs(doc)
    return Batch(
        "usgs_earthquakes",
        now.strftime("%Y-%m-%dT%H:00Z"),
        df,
        now,
        secs,
        {"generated": (doc.get("metadata") or {}).get("generated")},
    )


def backfill_usgs(now: datetime, days: int) -> list[Batch]:
    """Replay the 30-day feed as if a 24-hour batch had been taken every 6 hours."""
    df = parse_usgs(get_json(USGS_MONTH, timeout=180))
    out = []
    end = now.replace(minute=0, second=0, microsecond=0)
    earliest = df["time"].min()
    for k in range(days * 4, 0, -1):
        t = end - timedelta(hours=6 * k)
        if pd.isna(earliest) or t - timedelta(hours=24) < earliest:
            continue  # only windows the 30-day feed covers completely
        part = df[(df["time"] > t - timedelta(hours=24)) & (df["time"] <= t)].reset_index(drop=True)
        out.append(Batch("usgs_earthquakes", t.strftime("%Y-%m-%dT%H:00Z"), part, t, 0.0, {"backfill": True}))
    return out


# ------------------------------------------------------------------ Wikipedia top articles
WIKI_TOP = "https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access/{y}/{m:02d}/{d:02d}"


def parse_wiki(doc: dict[str, Any]) -> pd.DataFrame:
    items = doc.get("items") or []
    if not items:
        return pd.DataFrame(columns=["day", "article", "views", "rank"])
    it = items[0]
    day = pd.Timestamp(f"{it['year']}-{it['month']}-{it['day']}", tz="UTC")
    df = pd.DataFrame(it.get("articles") or [])
    if df.empty:
        return pd.DataFrame(columns=["day", "article", "views", "rank"])
    df.insert(0, "day", day)
    df["namespace"] = (
        df["article"]
        .str.extract(r"^(Special|Wikipedia|File|Portal|Help|Category|Talk):", expand=False)
        .fillna("Article")
    )
    return df


def _wiki_day(d: date, fetched: datetime) -> Batch:
    doc, secs = _timed(lambda: get_json(WIKI_TOP.format(y=d.year, m=d.month, d=d.day)))
    return Batch("wikipedia_top", d.isoformat(), parse_wiki(doc), fetched, secs)


def fetch_wiki(now: datetime) -> Batch:
    for back in (1, 2, 3):  # yesterday's file appears a few hours into the next UTC day
        try:
            return _wiki_day((now - timedelta(days=back)).date(), now)
        except FetchError as error:
            if "404" not in str(error):
                raise
    raise FetchError("no Wikipedia top-articles file for the last 3 days")


def backfill_wiki(now: datetime, days: int) -> list[Batch]:
    out = []
    for back in range(days + 1, 1, -1):
        d = (now - timedelta(days=back)).date()
        try:
            b = _wiki_day(
                d, datetime.combine(d + timedelta(days=1), datetime.min.time(), timezone.utc) + timedelta(hours=6)
            )
            out.append(b)
        except FetchError:
            continue
    return out


# ------------------------------------------------------------------ Chicago crimes
CHICAGO = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
CHI_COLS = [
    "id",
    "case_number",
    "date",
    "primary_type",
    "description",
    "location_description",
    "arrest",
    "domestic",
    "district",
    "community_area",
    "latitude",
    "longitude",
    "updated_on",
]


def parse_chicago(rows: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    for c in CHI_COLS:
        if c not in df.columns:
            df[c] = None
    df = df[CHI_COLS + [c for c in df.columns if c not in CHI_COLS]]
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    df["updated_on"] = pd.to_datetime(df["updated_on"], errors="coerce", utc=True)
    for c in ("latitude", "longitude"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ("arrest", "domestic"):
        df[c] = df[c].map(lambda v: v if isinstance(v, bool) else str(v).lower() == "true" if v is not None else None)
    df["hour"] = df["date"].dt.hour
    return df


def _soql(params: dict[str, str]) -> str:
    return CHICAGO + "?" + urllib.parse.urlencode(params)


def _chicago_latest_day() -> date:
    rows = get_json(_soql({"$select": "max(date) AS d"}))
    if not rows or not rows[0].get("d"):
        raise FetchError("Chicago: could not find the latest date")
    return pd.Timestamp(rows[0]["d"]).date()


def _chicago_day(d: date, fetched: datetime) -> Batch:
    nxt = d + timedelta(days=1)
    where = f"date >= '{d.isoformat()}T00:00:00' AND date < '{nxt.isoformat()}T00:00:00'"
    rows, secs = _timed(lambda: get_json(_soql({"$where": where, "$limit": "50000", "$order": "date"}), timeout=120))
    return Batch("chicago_crimes", d.isoformat(), parse_chicago(rows), fetched, secs)


def fetch_chicago(now: datetime) -> Batch:
    # the newest day is usually still being loaded; the day before it is the latest complete one
    latest = _chicago_latest_day()
    return _chicago_day(latest - timedelta(days=1), now)


def backfill_chicago(now: datetime, days: int) -> list[Batch]:
    latest = _chicago_latest_day()
    out = []
    for back in range(days, 0, -1):
        d = latest - timedelta(days=back)
        out.append(_chicago_day(d, datetime.combine(d + timedelta(days=8), datetime.min.time(), timezone.utc)))
    return out


SOURCES: dict[str, Source] = {
    s.name: s
    for s in [
        Source(
            "usgs_earthquakes",
            "USGS earthquakes, last 24 hours",
            "https://earthquake.usgs.gov/earthquakes/feed/",
            "continuously, every minute",
            2.0,
            "time",
            {"mag": [-2, 0, 1, 2, 3, 4, 5, 6, 10], "depth_km": [-10, 5, 10, 20, 35, 70, 150, 300, 800]},
            ("mag_type", "type"),  # network and review status swing with aftershock sequences and age
            False,
            fetch_usgs,
            backfill_usgs,
        ),
        Source(
            "wikipedia_top",
            "Wikipedia top 1,000 articles, daily",
            "https://wikimedia.org/api/rest_v1/",
            "daily, usually within a day",
            54.0,
            "day",
            {"views": [0, 2e4, 3e4, 5e4, 1e5, 2e5, 5e5, 1e6, 1e9]},
            ("namespace",),
            True,
            fetch_wiki,
            backfill_wiki,
            period_hours=24,
        ),
        Source(
            "chicago_crimes",
            "Chicago reported crimes, daily",
            "https://data.cityofchicago.org/d/ijzp-q8t2",
            "daily, about 7 days behind",
            24 * 10,
            "date",
            {"hour": [0, 3, 6, 9, 12, 15, 18, 21, 24]},
            ("primary_type", "arrest", "domestic", "district"),
            True,
            fetch_chicago,
            backfill_chicago,
        ),
    ]
}
