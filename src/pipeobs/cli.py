"""pipeobs: monitor public data feeds, keep their history, publish a status document and raise alerts.

pipeobs run                      fetch every source once and record the results
pipeobs run --source chicago_crimes --backfill 30
pipeobs status -o status/latest.json
pipeobs alert                    open/close GitHub issues for incidents (needs the gh CLI and a token)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from . import __version__
from .alert import sync
from .runner import run_all
from .sources import SOURCES
from .status import build_status
from .store import Store


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="pipeobs", description=__doc__.splitlines()[0])
    p.add_argument("--version", action="version", version=__version__)
    p.add_argument("--history", default="history", help="history directory (default: history)")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--source", action="append", choices=list(SOURCES))
    r.add_argument("--backfill", type=int, default=0, help="replay this many past days")
    r.add_argument("--backfill-if-empty", type=int, default=0, help="backfill a source that has no history yet")
    s = sub.add_parser("status")
    s.add_argument("-o", "--output", default="status/latest.json")
    a = sub.add_parser("alert")
    a.add_argument("--status", default="status/latest.json")
    a.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    store = Store(args.history)

    if args.cmd == "run":
        out = run_all(store, args.source, backfill_days=args.backfill, backfill_if_empty=args.backfill_if_empty)
        for name, recs in out.items():
            for rec in recs[-3:]:
                print(f"{name:<18} {rec.get('partition', ''):<18} {rec['status']:<5} rows={rec.get('rows', '-')}")
        return 0  # a failing feed is data, not a crashed job
    if args.cmd == "status":
        st = build_status(store)
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(json.dumps(st, indent=1, default=str), encoding="utf-8")
        print(f"{st['overall']}: " + ", ".join(f"{s['name']}={s['status']}" for s in st["sources"]))
        return 0
    if args.cmd == "alert":
        st = json.loads(Path(args.status).read_text(encoding="utf-8"))
        print(sync(st, dry_run=args.dry_run))
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
