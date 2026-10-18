"""History on disk: one JSON line per run per source, committed to the repository by the scheduled workflow.

Plain JSON Lines keeps the history diffable and readable without any database. Each file is capped at
MAX_RUNS lines so the repository stays small however long the monitor runs.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

MAX_RUNS = 1500


class Store:
    def __init__(self, root: str | Path):
        self.root = Path(root)

    def path(self, source: str) -> Path:
        return self.root / f"{source}.jsonl"

    def load(self, source: str) -> list[dict[str, Any]]:
        p = self.path(source)
        if not p.exists():
            return []
        out = []
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    continue  # a torn line must not take the monitor down
        return out

    def has_partition(self, source: str, partition: str) -> bool:
        return any(r.get("partition") == partition and not r.get("error") for r in self.load(source))

    def append(self, source: str, records: list[dict[str, Any]]) -> None:
        rows = self.load(source) + records
        rows.sort(key=lambda r: r.get("run_at", ""))
        rows = rows[-MAX_RUNS:]
        self.root.mkdir(parents=True, exist_ok=True)
        tmp = self.path(source).with_suffix(".tmp")
        tmp.write_text(
            "".join(json.dumps(r, separators=(",", ":"), sort_keys=True) + "\n" for r in rows), encoding="utf-8"
        )
        tmp.replace(self.path(source))
