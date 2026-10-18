"""Summarise a batch into a small profile: the numbers every monitor needs, without keeping the raw data.

A profile is a few kilobytes of JSON, so months of history fit in the repository and every monitor runs on
history alone: volume, null rates, schema, distributions and freshness.
"""

from __future__ import annotations

import hashlib
from typing import Any

import numpy as np
import pandas as pd

from .sources import Batch, Source

TOP_CATEGORIES = 30


def dtype_name(s: pd.Series) -> str:
    if len(s) and not s.notna().any() and not pd.api.types.is_datetime64_any_dtype(s):
        return "null"  # an all-blank column has no observable type; completeness reports it
    if pd.api.types.is_bool_dtype(s):
        return "boolean"
    if pd.api.types.is_datetime64_any_dtype(s):
        return "timestamp"
    if pd.api.types.is_integer_dtype(s):
        return "integer"
    if pd.api.types.is_float_dtype(s):
        return "float"
    v = s.dropna()
    if len(v) and v.map(lambda x: isinstance(x, bool)).all():
        return "boolean"
    return "string"


def histogram(s: pd.Series, edges: list[float]) -> list[float]:
    """Share of values in each bin; values outside the edges land in the first or last bin."""
    v = pd.to_numeric(s, errors="coerce").dropna().to_numpy(dtype=float)
    if not len(v):
        return [0.0] * (len(edges) - 1)
    v = np.clip(v, edges[0], np.nextafter(edges[-1], -np.inf))
    counts, _ = np.histogram(v, bins=edges)
    return [round(float(c) / len(v), 6) for c in counts]


def category_shares(s: pd.Series) -> dict[str, float]:
    v = s.dropna().astype(str)
    if not len(v):
        return {}
    shares = v.value_counts(normalize=True)
    top = shares.head(TOP_CATEGORIES)
    out = {str(k): round(float(x), 6) for k, x in top.items()}
    rest = float(shares.iloc[TOP_CATEGORIES:].sum())
    if rest > 0:
        out["(other)"] = round(rest, 6)
    return out


def profile(batch: Batch, src: Source) -> dict[str, Any]:
    df = batch.df
    columns = {str(c): dtype_name(df[c]) for c in df.columns}
    schema_hash = hashlib.sha1(",".join(f"{k}:{v}" for k, v in sorted(columns.items())).encode()).hexdigest()[:12]
    newest = None
    freshness_hours = None
    if src.event_time in df.columns and len(df):
        ts = pd.to_datetime(df[src.event_time], errors="coerce", utc=True).max()
        if not pd.isna(ts):
            newest = ts.isoformat()
            freshness_hours = round((pd.Timestamp(batch.fetched_at) - ts).total_seconds() / 3600, 3)
    dup = None
    if "id" in df.columns and len(df):
        dup = int(df["id"].duplicated().sum())
    return {
        "source": batch.source,
        "partition": batch.partition,
        "run_at": batch.fetched_at.isoformat(),
        "backfill": bool(batch.meta.get("backfill") or False),
        "fetch_seconds": batch.fetch_seconds,
        "rows": int(len(df)),
        "duplicate_ids": dup,
        "columns": columns,
        "schema_hash": schema_hash,
        "null_rates": {str(c): round(float(df[c].isna().mean()), 6) if len(df) else 0.0 for c in df.columns},
        "newest_event": newest,
        "freshness_hours": freshness_hours,
        "histograms": {c: histogram(df[c], edges) for c, edges in src.numeric.items() if c in df.columns},
        "means": {
            c: (round(float(pd.to_numeric(df[c], errors="coerce").mean()), 6) if len(df) else None)
            for c in src.numeric
            if c in df.columns
        },
        "categories": {c: category_shares(df[c]) for c in src.categorical if c in df.columns},
    }
