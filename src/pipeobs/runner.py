"""One monitoring pass: fetch each source, profile it, run the monitors against its history, store the result."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from .monitors import evaluate, run_status
from .profile import profile
from .sources import SOURCES, Batch, Source
from .store import Store

log = logging.getLogger("pipeobs")


def record_batch(batch: Batch, src: Source, history: list[dict[str, Any]]) -> dict[str, Any]:
    p = profile(batch, src)
    checks = evaluate(p, history, src)
    p["checks"] = [c.to_dict() for c in checks]
    p["status"] = run_status(checks)
    return p


def record_error(src: Source, now: datetime, error: Exception, history: list[dict[str, Any]]) -> dict[str, Any]:
    p: dict[str, Any] = {
        "source": src.name,
        "partition": now.strftime("%Y-%m-%dT%H:%MZ"),
        "run_at": now.isoformat(),
        "error": f"{type(error).__name__}: {error}"[:500],
    }
    checks = evaluate(p, history, src)
    p["checks"] = [c.to_dict() for c in checks]
    p["status"] = run_status(checks)
    return p


def run_source(src: Source, store: Store, now: datetime, backfill_days: int = 0) -> list[dict[str, Any]]:
    history = store.load(src.name)
    new: list[dict[str, Any]] = []
    try:
        batches = src.backfill(now, backfill_days) if backfill_days else [src.fetch(now)]
    except Exception as error:  # any failure to get a batch is an availability incident, not a crash
        log.warning("%s: fetch failed: %s", src.name, error)
        rec = record_error(src, now, error, history)
        store.append(src.name, [rec])
        return [rec]
    done = {h["partition"] for h in history if not h.get("error")}
    for b in batches:
        if b.partition in done:  # daily feeds: one record per day however often we run
            log.info("%s: partition %s already recorded; skipping", src.name, b.partition)
            continue
        done.add(b.partition)
        if backfill_days:
            b.meta["backfill"] = True  # replayed history: shown faded, never alerts
        rec = record_batch(b, src, history + new)
        new.append(rec)
        log.info("%s %s: %s rows, %s", src.name, b.partition, rec["rows"], rec["status"])
    if new:
        store.append(src.name, new)
    return new


def run_all(
    store: Store,
    names: list[str] | None = None,
    now: datetime | None = None,
    backfill_days: int = 0,
    backfill_if_empty: int = 0,
) -> dict[str, list[dict[str, Any]]]:
    now = now or datetime.now(timezone.utc)
    out = {}
    for name in names or list(SOURCES):
        src = SOURCES[name]
        days = backfill_days or (backfill_if_empty if not store.load(name) else 0)
        out[name] = run_source(src, store, now, days)
        if days and not backfill_days:  # after a first-time backfill, take today's batch too
            out[name] += run_source(src, store, now, 0)
    return out
