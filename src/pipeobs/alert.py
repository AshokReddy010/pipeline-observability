"""Alerting: keep one GitHub issue per open incident, and close it with a note when the incident resolves.

Issues are the alert channel because they notify the repository's watchers by email and on mobile, keep a
timeline, and need no extra secrets: the workflow's own token can open and close them. Each issue carries a
stable key in its title, so repeated runs update the same issue instead of opening duplicates.
"""

from __future__ import annotations

import contextlib
import json
import logging
import shutil
import subprocess
from collections.abc import Callable
from typing import Any

log = logging.getLogger("pipeobs")
LABEL = "incident"

Runner = Callable[[list[str]], str]


def key(inc: dict[str, Any]) -> str:
    return f"[{inc['source']}/{inc['monitor']}]"


def title(inc: dict[str, Any]) -> str:
    return f"{key(inc)} {inc['monitor']} failing for {inc['source']}"


def body(inc: dict[str, Any]) -> str:
    return (
        f"**{inc['monitor']}** started failing for **{inc['source']}** at {inc['started']}.\n\n"
        f"- first: {inc['first_message']}\n- latest: {inc['last_message']}\n- failing runs so far: {inc['runs']}\n\n"
        "This issue was opened by the scheduled monitor and will be closed automatically when the check passes. "
        "Live status: https://ashokreddy010.github.io/pipeline-observability/"
    )


def gh(args: list[str]) -> str:
    return subprocess.run(["gh", *args], check=True, capture_output=True, text=True).stdout


def plan(open_incidents: list[dict[str, Any]], issues: list[dict[str, Any]]) -> dict[str, list[Any]]:
    """What to do, as data (so it can be tested without GitHub): issues to open, to comment on, to close."""
    by_key = {}
    for i in issues:
        t = i.get("title", "")
        if t.startswith("[") and "]" in t:
            by_key[t[: t.index("]") + 1]] = i
    want = {key(inc): inc for inc in open_incidents if not inc.get("backfill")}
    return {
        "open": [inc for k, inc in want.items() if k not in by_key],
        "update": [(by_key[k], inc) for k, inc in want.items() if k in by_key],
        "close": [i for k, i in by_key.items() if k not in want],
    }


def sync(status: dict[str, Any], run: Runner | None = None, dry_run: bool = False) -> dict[str, int]:
    if run is None and not shutil.which("gh"):
        log.warning("gh CLI not found; skipping alerts")
        return {"open": 0, "update": 0, "close": 0}
    run = run or gh
    open_incs = [i for i in status["incidents"] if i["resolved"] is None]
    issues = json.loads(
        run(["issue", "list", "--label", LABEL, "--state", "open", "--json", "number,title", "--limit", "100"]) or "[]"
    )
    todo = plan(open_incs, issues)
    if not dry_run:
        if todo["open"]:
            with contextlib.suppress(subprocess.CalledProcessError):  # the label may already exist
                run(["label", "create", LABEL, "--color", "B42318", "--description", "Opened by the data monitor"])
        for inc in todo["open"]:
            run(["issue", "create", "--title", title(inc), "--label", LABEL, "--body", body(inc)])
        for issue, inc in todo["update"]:
            if inc["runs"] and inc["runs"] % 8 == 0:  # a reminder now and then, not on every run
                run(
                    [
                        "issue",
                        "comment",
                        str(issue["number"]),
                        "--body",
                        f"Still failing after {inc['runs']} runs: {inc['last_message']}",
                    ]
                )
        for issue in todo["close"]:
            run(["issue", "close", str(issue["number"]), "--comment", "Resolved: the check passes again."])
    return {k: len(v) for k, v in todo.items()}
