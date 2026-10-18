"""Fixtures shaped exactly like the real feeds' payloads (USGS GeoJSON, Wikimedia REST, Socrata JSON)."""

import random
from datetime import datetime, timedelta, timezone

import pytest

NOW = datetime(2026, 10, 18, 12, 0, tzinfo=timezone.utc)


def usgs_doc(n=300, seed=1, now=NOW, drop=(), extra=None, span_hours=24):
    rng = random.Random(seed)
    feats = []
    for i in range(n):
        t = now - timedelta(minutes=rng.randint(1, span_hours * 60))
        props = {
            "mag": round(rng.uniform(-0.5, 5.5), 2),
            "place": f"{rng.randint(1, 90)} km N of Somewhere",
            "time": int(t.timestamp() * 1000),
            "updated": int(t.timestamp() * 1000) + 60000,
            "status": rng.choice(["automatic", "reviewed"]),
            "tsunami": 0,
            "sig": rng.randint(0, 600),
            "net": rng.choice(["ak", "ci", "nc", "us", "hv"]),
            "magType": rng.choice(["ml", "md", "mb"]),
            "type": "earthquake",
        }
        for k in drop:
            props.pop(k, None)
        if extra:
            props.update(extra)
        feats.append(
            {
                "type": "Feature",
                "id": f"ev{seed}{i:05d}",
                "properties": props,
                "geometry": {
                    "type": "Point",
                    "coordinates": [rng.uniform(-180, 180), rng.uniform(-60, 70), round(rng.uniform(0, 80), 2)],
                },
            }
        )
    return {
        "type": "FeatureCollection",
        "metadata": {"generated": int(now.timestamp() * 1000), "count": n},
        "features": feats,
    }


def wiki_doc(day="2026-10-16", n=1000, seed=1):
    rng = random.Random(seed)
    y, m, d = day.split("-")
    arts = [
        {"article": "Main_Page", "views": 5_000_000, "rank": 1},
        {"article": "Special:Search", "views": 1_200_000, "rank": 2},
    ]
    arts += [
        {"article": f"Article_{seed}_{i}", "views": int(20000 + rng.paretovariate(1.5) * 8000), "rank": i + 3}
        for i in range(n - 2)
    ]
    return {
        "items": [
            {"project": "en.wikipedia", "access": "all-access", "year": y, "month": m, "day": d, "articles": arts}
        ]
    }


def chicago_rows(day="2026-10-09", n=650, seed=1):
    rng = random.Random(seed)
    out = []
    for i in range(n):
        t = datetime.fromisoformat(day) + timedelta(minutes=rng.randint(0, 1439))
        out.append(
            {
                "id": str(13000000 + seed * 10000 + i),
                "case_number": f"JJ{seed}{i:05d}",
                "date": t.strftime("%Y-%m-%dT%H:%M:%S.000"),
                "primary_type": rng.choice(["THEFT", "BATTERY", "CRIMINAL DAMAGE", "ASSAULT", "DECEPTIVE PRACTICE"]),
                "description": "SIMPLE",
                "location_description": "STREET",
                "arrest": rng.random() < 0.12,
                "domestic": rng.random() < 0.2,
                "district": f"{rng.randint(1, 25):03d}",
                "community_area": str(rng.randint(1, 77)),
                "latitude": str(41.6 + rng.random() * 0.4),
                "longitude": str(-87.9 + rng.random() * 0.4),
                "updated_on": "2026-10-16T15:41:22.000",
            }
        )
    return out


@pytest.fixture
def store(tmp_path):
    from pipeobs.store import Store

    return Store(tmp_path / "history")
