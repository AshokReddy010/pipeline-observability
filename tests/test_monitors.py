from datetime import timedelta

import pytest

from pipeobs.monitors import evaluate, psi, robust_z, run_status
from pipeobs.profile import profile
from pipeobs.runner import record_batch
from pipeobs.sources import SOURCES, Batch, parse_chicago, parse_usgs, parse_wiki
from tests.conftest import NOW, chicago_rows, usgs_doc, wiki_doc

USGS = SOURCES["usgs_earthquakes"]
CHI = SOURCES["chicago_crimes"]


def usgs_history(k=10, n=300):
    hist = []
    for i in range(k):
        t = NOW - timedelta(hours=6 * (k - i))
        b = Batch(
            USGS.name, t.strftime("%Y-%m-%dT%H:00Z"), parse_usgs(usgs_doc(n + (i % 3) * 7, seed=i, now=t)), t, 0.3
        )
        hist.append(record_batch(b, USGS, hist))
    return hist


def by(checks):
    return {c.monitor: c for c in checks}


def test_math():
    z, m, scale = robust_z(130, [100, 102, 98, 101, 99])
    assert m == 100 and scale == 5.0 and z == 6.0  # MAD floor of 5% of the median
    assert psi([0.5, 0.5], [0.5, 0.5]) == 0
    assert psi([0.9, 0.1], [0.5, 0.5]) > 0.2


def test_category_psi_pools_categories_missing_from_some_profiles():
    from pipeobs.monitors import category_psi

    past = [{"a": 0.5, "b": 0.3, "(other)": 0.2}, {"a": 0.5, "c": 0.3, "(other)": 0.2}]
    assert category_psi({"a": 0.5, "d": 0.3, "(other)": 0.2}, past) == 0  # same shape, different tail names
    assert category_psi({"a": 0.9, "(other)": 0.1}, past) > 0.2


def test_learning_then_pass():
    first = record_batch(Batch(USGS.name, "p0", parse_usgs(usgs_doc()), NOW), USGS, [])
    c = {x["monitor"]: x for x in first["checks"]}
    assert c["volume"]["status"] == "learning" and c["distribution"]["status"] == "learning"
    assert first["status"] == "pass"
    hist = usgs_history()
    assert all(h["status"] == "pass" for h in hist[5:]), [h["checks"] for h in hist if h["status"] != "pass"]


def _new(df, hist, src=USGS, part="2026-10-18T12:00Z", at=NOW):
    p = profile(Batch(src.name, part, df, at, 0.2), src)
    return by(evaluate(p, hist, src))


def test_volume_spike_and_drop():
    hist = usgs_history()
    assert _new(parse_usgs(usgs_doc(1200)), hist)["volume"].status == "fail"
    assert _new(parse_usgs(usgs_doc(150)), hist)["volume"].status == "fail"
    assert _new(parse_usgs(usgs_doc(310)), hist)["volume"].status == "pass"


def test_empty_batch_fails_volume_and_freshness():
    c = _new(parse_usgs({"features": []}), usgs_history())
    assert c["volume"].status == "fail" and c["freshness"].status == "fail"


def test_freshness_sla():
    stale_at = NOW + timedelta(hours=5)
    c = _new(parse_usgs(usgs_doc()), usgs_history(), at=stale_at)
    assert c["freshness"].status == "fail" and "SLA" in c["freshness"].message


def test_schema_changes():
    hist = usgs_history()
    df = parse_usgs(usgs_doc())
    assert _new(df.drop(columns=["mag"]), hist)["schema"].status == "fail"
    assert _new(df.assign(mag=df["mag"].astype(str)), hist)["schema"].message == "mag float -> string"
    assert _new(df.assign(extra=1), hist)["schema"].status == "warn"


def test_completeness_jump():
    hist = usgs_history()
    df = parse_usgs(usgs_doc())
    df.loc[: int(len(df) * 0.5), "mag"] = None
    c = _new(df, hist)["completeness"]
    assert c.status == "fail" and c.target == "mag"
    df3 = parse_usgs(usgs_doc())
    df3["mag"] = None  # a column that went entirely blank is a completeness failure, not a schema change
    c3 = _new(df3, hist)
    assert c3["completeness"].status == "fail" and c3["schema"].status == "pass"
    df2 = parse_usgs(usgs_doc())
    df2.loc[: int(len(df2) * 0.15), "net"] = None
    assert _new(df2, hist)["completeness"].status == "warn"


def test_duplicates():
    df = parse_usgs(usgs_doc())
    df.loc[1, "id"] = df.loc[0, "id"]
    assert _new(df, usgs_history())["uniqueness"].status == "fail"


def test_distribution_drift_warns():
    df = parse_usgs(usgs_doc())
    df["mag"] = df["mag"] + 3
    c = _new(df, usgs_history())["distribution"]
    assert c.status == "warn" and c.target == "mag"


def test_failed_runs_are_excluded_from_baseline():
    hist = usgs_history()
    bad = record_batch(Batch(USGS.name, "x", parse_usgs(usgs_doc(5000)), NOW), USGS, hist)
    assert bad["status"] == "fail"
    assert _new(parse_usgs(usgs_doc(305)), hist + [bad])["volume"].status == "pass"


def test_weekday_seasonality():
    hist = []
    for i in range(35):  # weekends are half as busy
        d = (NOW - timedelta(days=40 - i)).date()
        n = 300 if d.weekday() >= 5 else 600
        b = Batch(CHI.name, d.isoformat(), parse_chicago(chicago_rows(d.isoformat(), n + i % 5, seed=i)), NOW, 0.5)
        hist.append(record_batch(b, CHI, hist))
    sat = "2026-10-17"
    c = _new(parse_chicago(chicago_rows(sat, 305)), hist, CHI, part=sat)["volume"]
    assert c.status == "pass" and "same weekday" in c.message
    assert _new(parse_chicago(chicago_rows(sat, 600)), hist, CHI, part=sat)["volume"].status == "fail"


def test_fetch_error():
    checks = evaluate({"error": "URLError: timed out"}, [], USGS)
    assert [c.monitor for c in checks] == ["availability"] and run_status(checks) == "fail"


@pytest.mark.parametrize(
    "statuses, expected", [(["pass", "learning"], "pass"), (["pass", "warn"], "warn"), (["warn", "fail"], "fail")]
)
def test_run_status(statuses, expected):
    from pipeobs.monitors import Check

    assert run_status([Check("m", s, "") for s in statuses]) == expected


def test_wiki_profile_passes_against_itself():
    src = SOURCES["wikipedia_top"]
    hist = []
    for i in range(8):
        day = f"2026-10-{i + 1:02d}"
        b = Batch(src.name, day, parse_wiki(wiki_doc(day, seed=i)), NOW.replace(day=i + 2), 0.2)
        hist.append(record_batch(b, src, hist))
    assert hist[-1]["status"] == "pass", hist[-1]["checks"]
