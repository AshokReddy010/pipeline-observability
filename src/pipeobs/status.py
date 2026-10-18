"""Build the status document the live page reads: current health, run history, incidents, uptime and MTTR.

An incident is a streak of consecutive runs in which the same monitor failed for the same source. It opens on
the first failing run and resolves on the first run where that monitor passes again. Warnings are shown but
never open incidents.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from .sources import SOURCES
from .store import Store

PAGE_RUNS = 120


def incidents_for(source: str, history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    open_: dict[str, dict[str, Any]] = {}
    done: list[dict[str, Any]] = []
    for rec in history:
        failing = {c["monitor"]: c for c in rec.get("checks", []) if c["status"] == "fail"}
        for mon, c in failing.items():
            if mon in open_:
                open_[mon]["runs"] += 1
                open_[mon]["last_message"] = c["message"]
                open_[mon]["last_seen"] = rec["run_at"]
            else:
                open_[mon] = {
                    "source": source,
                    "monitor": mon,
                    "started": rec["run_at"],
                    "last_seen": rec["run_at"],
                    "runs": 1,
                    "first_message": c["message"],
                    "last_message": c["message"],
                    "partition": rec.get("partition"),
                    "backfill": bool(rec.get("backfill")),
                }
        evaluated = {c["monitor"] for c in rec.get("checks", [])}
        for mon in list(open_):
            # resolve only when the monitor actually ran and did not fail (a failed fetch evaluates nothing else)
            if mon not in failing and (mon in evaluated or mon == "availability"):
                inc = open_.pop(mon)
                inc["resolved"] = rec["run_at"]
                inc["duration_hours"] = round(_hours(inc["started"], rec["run_at"]), 2)
                done.append(inc)
    for inc in open_.values():
        inc["resolved"] = None
        done.append(inc)
    return sorted(done, key=lambda i: i["started"], reverse=True)


def live_freshness(rec: dict[str, Any] | None, now: datetime) -> float | None:
    if not rec or not rec.get("newest_event"):
        return None
    return round((now - datetime.fromisoformat(rec["newest_event"])).total_seconds() / 3600, 2)


def build_status(store: Store, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    sources, all_incidents = [], []
    for name, src in SOURCES.items():
        hist = store.load(name)
        last_ok = next((h for h in reversed(hist) if not h.get("error")), None)
        latest = hist[-1] if hist else None
        fresh = live_freshness(last_ok, now)
        status = latest["status"] if latest else "unknown"
        stale = fresh is not None and fresh > src.freshness_sla_hours
        if stale:
            status = "fail"
        incs = incidents_for(name, hist)
        if stale and not any(i["monitor"] == "freshness" and i["resolved"] is None for i in incs):
            incs.insert(
                0,
                {
                    "source": name,
                    "monitor": "freshness",
                    "started": _plus(last_ok, src.freshness_sla_hours),
                    "last_seen": now.isoformat(),
                    "runs": 0,
                    "resolved": None,
                    "backfill": False,
                    "first_message": "no new data within the SLA",
                    "last_message": f"newest record is {fresh:.1f} h old (SLA {src.freshness_sla_hours:g} h)",
                },
            )
        all_incidents += incs
        sources.append(
            {
                "name": name,
                "title": src.title,
                "homepage": src.homepage,
                "cadence": src.cadence,
                "freshness_sla_hours": src.freshness_sla_hours,
                "status": status,
                "live_freshness_hours": fresh,
                "last_run": latest["run_at"] if latest else None,
                "latest": {
                    k: latest.get(k) for k in ("partition", "rows", "status", "checks", "error", "fetch_seconds")
                }
                if latest
                else None,
                "uptime_7d": _uptime(hist, now, 7),
                "uptime_30d": _uptime(hist, now, 30),
                "runs_total": len(hist),
                "runs": [
                    {
                        "t": h["run_at"],
                        "p": h.get("partition"),
                        "s": h["status"],
                        "rows": h.get("rows"),
                        "f": h.get("freshness_hours"),
                        "b": 1 if h.get("backfill") else 0,
                    }
                    for h in hist[-PAGE_RUNS:]
                ],
                "open_incidents": sum(1 for i in incs if i["resolved"] is None),
            }
        )
    resolved = [i for i in all_incidents if i["resolved"] and _hours(i["resolved"], now.isoformat()) <= 30 * 24]
    return {
        "generated_at": now.isoformat(),
        "overall": "fail"
        if any(s["status"] == "fail" for s in sources)
        else "warn"
        if any(s["status"] == "warn" for s in sources)
        else "pass",
        "sources": sources,
        "incidents": sorted(all_incidents, key=lambda i: i["started"], reverse=True)[:60],
        "mttr_hours_30d": round(sum(i["duration_hours"] for i in resolved) / len(resolved), 2) if resolved else None,
        "incidents_30d": len(resolved) + sum(1 for i in all_incidents if i["resolved"] is None),
    }


def _uptime(hist: list[dict[str, Any]], now: datetime, days: int) -> float | None:
    since = now - timedelta(days=days)
    runs = [h for h in hist if datetime.fromisoformat(h["run_at"]) >= since]
    if not runs:
        return None
    return round(sum(1 for h in runs if h["status"] != "fail") / len(runs), 4)


def _hours(a: str, b: str) -> float:
    return (datetime.fromisoformat(b) - datetime.fromisoformat(a)).total_seconds() / 3600


def _plus(rec: dict[str, Any] | None, hours: float) -> str:
    if not rec or not rec.get("newest_event"):
        return datetime.now(timezone.utc).isoformat()
    return (datetime.fromisoformat(rec["newest_event"]) + timedelta(hours=hours)).isoformat()
