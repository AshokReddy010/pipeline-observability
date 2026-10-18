import pandas as pd

from pipeobs.profile import category_shares, histogram, profile
from pipeobs.sources import SOURCES, Batch, parse_chicago, parse_usgs, parse_wiki
from tests.conftest import NOW, chicago_rows, usgs_doc, wiki_doc


def test_parse_usgs():
    df = parse_usgs(usgs_doc(50))
    assert len(df) == 50
    assert str(df["time"].dtype).startswith("datetime64") and df["time"].dt.tz is not None
    assert {"mag", "depth_km", "net", "mag_type"} <= set(df.columns)


def test_parse_usgs_empty_feed_keeps_columns():
    df = parse_usgs({"features": []})
    assert df.empty and "mag" in df.columns


def test_parse_wiki():
    df = parse_wiki(wiki_doc(n=100))
    assert len(df) == 100 and df["day"].iloc[0] == pd.Timestamp("2026-10-16", tz="UTC")
    assert df.loc[df["article"] == "Special:Search", "namespace"].item() == "Special"
    assert df.loc[df["article"] == "Main_Page", "namespace"].item() == "Article"
    assert parse_wiki({"items": []}).empty


def test_parse_chicago():
    df = parse_chicago(chicago_rows(n=40))
    assert len(df) == 40 and df["arrest"].map(type).eq(bool).all()
    assert df["latitude"].dtype == float and df["hour"].between(0, 23).all()
    assert list(parse_chicago([]).columns)[:3] == ["id", "case_number", "date"]


def test_histogram_and_shares():
    h = histogram(pd.Series([-5, 0.5, 1.5, 99]), [0, 1, 2, 3])
    assert h == [0.5, 0.25, 0.25] and abs(sum(h) - 1) < 1e-9  # outliers clip into the end bins
    assert histogram(pd.Series([], dtype=float), [0, 1, 2]) == [0.0, 0.0]
    s = category_shares(pd.Series(list("aab") + [None]))
    assert s == {"a": 0.666667, "b": 0.333333}


def test_profile_usgs():
    src = SOURCES["usgs_earthquakes"]
    df = parse_usgs(usgs_doc(200))
    p = profile(Batch(src.name, "2026-10-18T12:00Z", df, NOW, 0.4), src)
    assert p["rows"] == 200 and p["duplicate_ids"] == 0
    assert 0 < p["freshness_hours"] < 1
    assert p["columns"]["mag"] == "float" and p["columns"]["time"] == "timestamp"
    assert abs(sum(p["histograms"]["mag"]) - 1) < 1e-6
    assert set(p["categories"]) == {"mag_type", "net", "type", "status"}
    assert len(p["schema_hash"]) == 12


def test_profile_chicago_booleans():
    src = SOURCES["chicago_crimes"]
    p = profile(Batch(src.name, "2026-10-09", parse_chicago(chicago_rows()), NOW), src)
    assert p["columns"]["arrest"] == "boolean" and set(p["categories"]["arrest"]) == {"True", "False"}
    assert 24 * 8 < p["freshness_hours"] < 24 * 9.1


def test_daily_feed_freshness_counts_from_the_end_of_the_day():
    src = SOURCES["wikipedia_top"]
    p = profile(Batch(src.name, "2026-10-16", parse_wiki(wiki_doc("2026-10-16", n=10)), NOW), src)
    assert p["newest_event"].startswith("2026-10-17T00:00") and p["freshness_hours"] == 12.0
