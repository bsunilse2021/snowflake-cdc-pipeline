"""sfpipe - command line entry point."""
from __future__ import annotations

import argparse
import json
import logging
import sys

from . import migrate as mig
from .config import Settings


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="sfpipe", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)
    m = sub.add_parser("migrate", help="apply pending schema migrations")
    m.add_argument("--target", type=int, help="stop at this version")
    r = sub.add_parser("rollback", help="undo the latest migrations")
    r.add_argument("--steps", type=int, default=1)
    sub.add_parser("status", help="show migration state")
    s = sub.add_parser("seed", help="insert synthetic rows into the source table")
    s.add_argument("--rows", type=int, default=10_000)
    c = sub.add_parser("simulate", help="simulate inserts/updates/deletes on the source")
    c.add_argument("--inserts", type=int, default=500)
    c.add_argument("--updates", type=int, default=300)
    c.add_argument("--deletes", type=int, default=100)
    rn = sub.add_parser("run", help="process the pending delta")
    rn.add_argument("--mode", choices=["direct", "task"], default="direct")
    rn.add_argument("--timeout", type=int, default=300)
    sub.add_parser("reconcile", help="verify RAW and CORE match")
    sub.add_parser("history", help="show recent pipeline runs")
    b = sub.add_parser("benchmark", help="incremental vs full-reload timing")
    b.add_argument("--changes", type=int, default=3000)
    return p


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _build_parser().parse_args(argv)
    settings = Settings.from_env()

    if args.cmd == "migrate":
        mig.migrate(settings, args.target)
        return 0
    if args.cmd == "rollback":
        print("Rolled back versions:", mig.rollback(settings, args.steps))
        return 0
    if args.cmd == "status":
        for v, d, st in mig.status(settings):
            print(f"V{v:03d}  {st:8}  {d}")
        return 0

    from . import orchestrator as orch
    from .connection import connect

    with connect(settings) as conn:
        if args.cmd == "seed":
            orch.seed(conn, settings, args.rows)
        elif args.cmd == "simulate":
            orch.simulate_changes(conn, settings, args.inserts, args.updates, args.deletes)
        elif args.cmd == "run":
            run = orch.run_incremental(conn, settings, args.mode, args.timeout)
            print(json.dumps(run, default=str, indent=2) if run else "No run executed")
        elif args.cmd == "reconcile":
            res = orch.reconcile(conn, settings)
            print(json.dumps(res, default=str, indent=2))
            return 0 if res["in_sync"] else 1
        elif args.cmd == "history":
            for row in orch.history(conn, settings):
                print(row)
        elif args.cmd == "benchmark":
            print(json.dumps(orch.benchmark(conn, settings, args.changes), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
