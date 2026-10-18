"""The monitors: compare a new profile with the source's own history and say what looks wrong.

    availability   the fetch itself failed (network, HTTP error, unparseable payload)
    freshness      the newest record is older than the source's SLA
    volume         row count is far from what this source normally delivers (robust z-score, weekday-aware)
    schema         a column disappeared or changed type (fail), or a new one appeared (warn)
    completeness   a column's null rate jumped above its normal level
    uniqueness     record ids are duplicated within a batch
    distribution   a numeric or categorical distribution moved (population stability index)

Baselines use medians and the median absolute deviation, so one bad day in the history does not hide the next
one. Until a source has MIN_HISTORY healthy runs, volume and distribution report "learning" instead of guessing.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import date
from statistics import median
from typing import Any

from .sources import Source

MIN_HISTORY = 5
BASELINE_RUNS = 28
Z_WARN, Z_FAIL = 3.0, 4.5
PSI_WARN = 0.2
NULL_WARN, NULL_FAIL = 0.10, 0.30


@dataclass
class Check:
    monitor: str
    status: str  # pass | warn | fail | learning
    message: str
    value: float | None = None
    expected: str | None = None
    target: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}


def healthy(history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Past runs that are usable as a baseline: fetched fine and not themselves failing on volume or schema."""
    out = []
    for h in history:
        if h.get("error"):
            continue
        bad = {c["monitor"] for c in h.get("checks", []) if c["status"] == "fail"}
        if bad & {"volume", "schema"}:
            continue
        out.append(h)
    return out


def weekday(partition: str) -> int | None:
    try:
        return date.fromisoformat(partition[:10]).weekday()
    except ValueError:
        return None


def robust_z(x: float, base: list[float]) -> tuple[float, float, float]:
    m = median(base)
    mad = median(abs(b - m) for b in base) * 1.4826
    scale = max(mad, 0.05 * abs(m), 1.0)
    return (x - m) / scale, m, scale


def psi(actual: list[float], expected: list[float], eps: float = 1e-4) -> float:
    """Population stability index between two share vectors over the same bins."""
    total = 0.0
    for a, e in zip(actual, expected, strict=True):
        a, e = max(a, eps), max(e, eps)
        total += (a - e) * math.log(a / e)
    return round(total, 4)


def category_psi(current: dict[str, float], past: list[dict[str, float]]) -> float:
    """PSI over categories. Profiles keep only the top categories, so a category is compared on its own only
    when every profile recorded it; everything else is pooled into one "(other)" bucket on both sides."""
    every = set(current).intersection(*past) - {"(other)"}
    keys = sorted(every)

    def vec(shares: dict[str, float]) -> list[float]:
        v = [shares.get(k, 0.0) for k in keys]
        return [*v, max(0.0, 1.0 - sum(v))]

    expected = _avg([vec(h) for h in past])
    return psi(vec(current), expected)


def _avg(vectors: list[list[float]]) -> list[float]:
    return [sum(col) / len(vectors) for col in zip(*vectors, strict=True)]


def evaluate(p: dict[str, Any], history: list[dict[str, Any]], src: Source) -> list[Check]:
    if p.get("error"):
        return [Check("availability", "fail", f"fetch failed: {p['error']}")]
    checks = [
        Check(
            "availability", "pass", f"fetched {p['rows']:,} rows in {p['fetch_seconds']:.1f}s", value=p["fetch_seconds"]
        )
    ]
    base = healthy(history)[-BASELINE_RUNS:]

    # freshness
    fh = p.get("freshness_hours")
    if fh is None:
        checks.append(
            Check("freshness", "fail", "no valid event timestamps", expected=f"<= {src.freshness_sla_hours}h")
        )
    else:
        ok = fh <= src.freshness_sla_hours
        checks.append(
            Check(
                "freshness",
                "pass" if ok else "fail",
                f"newest record is {_age(fh)} old" + ("" if ok else f", over the {_age(src.freshness_sla_hours)} SLA"),
                value=fh,
                expected=f"<= {src.freshness_sla_hours}h",
            )
        )

    # volume
    rows = p["rows"]
    if rows == 0:
        checks.append(Check("volume", "fail", "the batch is empty", value=0))
    else:
        pool, capped = base, False
        wd = weekday(p["partition"]) if src.seasonal else None
        if wd is not None and len(base) >= MIN_HISTORY:
            # same-weekday runs, including ones that failed volume before the weekday pattern was known
            fetched = [h for h in history if not h.get("error") and h.get("rows")]
            same = [h for h in fetched if weekday(h["partition"]) == wd][-8:]
            if len(same) >= 4:
                pool = same
            else:
                capped = True  # weekly pattern not learned yet: never page on volume alone
        if len(base) < MIN_HISTORY:
            checks.append(
                Check(
                    "volume",
                    "learning",
                    f"{rows:,} rows; building a baseline ({len(pool)}/{MIN_HISTORY} runs)",
                    value=rows,
                )
            )
        else:
            z, m, scale = robust_z(rows, [h["rows"] for h in pool])
            status = "fail" if abs(z) >= Z_FAIL else "warn" if abs(z) >= Z_WARN else "pass"
            if capped and status == "fail":
                status = "warn"
            label = "same weekday" if pool is not base else "recent runs"
            checks.append(
                Check(
                    "volume",
                    status,
                    f"{rows:,} rows vs a typical {m:,.0f} ({label}); z = {z:+.1f}",
                    value=rows,
                    expected=f"{m - Z_WARN * scale:,.0f}..{m + Z_WARN * scale:,.0f}",
                )
            )

    # schema
    prev = next((h for h in reversed(history) if not h.get("error") and h.get("columns")), None)
    if prev is None:
        checks.append(Check("schema", "pass", f"first run: {len(p['columns'])} columns recorded"))
    else:
        old, new = prev["columns"], p["columns"]
        removed = sorted(set(old) - set(new))
        added = sorted(set(new) - set(old))
        changed = sorted(c for c in set(old) & set(new) if old[c] != new[c] and "null" not in (old[c], new[c]))
        if removed or changed:
            parts = ([f"removed {', '.join(removed)}"] if removed else []) + [
                f"{c} {old[c]} -> {new[c]}" for c in changed
            ]
            checks.append(Check("schema", "fail", "; ".join(parts)))
        elif added:
            checks.append(Check("schema", "warn", f"new columns: {', '.join(added)}"))
        else:
            checks.append(Check("schema", "pass", f"{len(new)} columns, unchanged"))

    # completeness
    worst: tuple[float, str, float, float] | None = None
    for col, rate in p["null_rates"].items():
        past = [h["null_rates"].get(col) for h in base if col in h.get("null_rates", {})]
        normal = median(past) if past else 0.0
        jump = rate - normal
        if worst is None or jump > worst[0]:
            worst = (jump, col, rate, normal)
    if worst is None or worst[0] < NULL_WARN:
        checks.append(Check("completeness", "pass", "null rates at their usual levels"))
    else:
        jump, col, rate, normal = worst
        checks.append(
            Check(
                "completeness",
                "fail" if jump >= NULL_FAIL else "warn",
                f"{col} is {rate:.0%} null (usually {normal:.0%})",
                value=rate,
                target=col,
            )
        )

    # uniqueness
    if p.get("duplicate_ids") is not None:
        d = p["duplicate_ids"]
        checks.append(
            Check(
                "uniqueness",
                "pass" if d == 0 else "fail",
                "ids are unique" if d == 0 else f"{d:,} duplicated ids",
                value=d,
            )
        )

    # distribution
    if len(base) < MIN_HISTORY:
        checks.append(Check("distribution", "learning", f"building a baseline ({len(base)}/{MIN_HISTORY} runs)"))
    else:
        recent = base[-14:]
        scores: list[tuple[float, str]] = []
        for col, hist in p.get("histograms", {}).items():
            past = [
                h["histograms"][col] for h in recent if col in h.get("histograms", {}) and sum(h["histograms"][col])
            ]
            if past and sum(hist):
                scores.append((psi(hist, _avg(past)), col))
        for col, shares in p.get("categories", {}).items():
            past = [h["categories"][col] for h in recent if col in h.get("categories", {})]
            if past and shares:
                scores.append((category_psi(shares, past), col))
        if scores:
            top, col = max(scores)
            checks.append(
                Check(
                    "distribution",
                    "warn" if top >= PSI_WARN else "pass",
                    f"largest shift: {col} (PSI {top:.2f})",
                    value=top,
                    target=col,
                    expected=f"< {PSI_WARN}",
                )
            )
    return checks


def run_status(checks: list[Check]) -> str:
    s = {c.status for c in checks}
    return "fail" if "fail" in s else "warn" if "warn" in s else "pass"


def _age(hours: float) -> str:
    if hours < 1:
        return f"{hours * 60:.0f} min"
    if hours < 48:
        return f"{hours:.1f} h"
    return f"{hours / 24:.1f} days"
