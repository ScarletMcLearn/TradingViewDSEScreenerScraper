# scripts/run_all.py
from __future__ import annotations
import sys
import argparse
import traceback
from typing import Callable, Dict, List

from scripts.utils import make_shared_rate_limiter, save_dse_screener
from scripts import fetch_dse_screener

def task_fetch_dse_screener(save_csv: bool, rate_limiter) -> None:
    df = fetch_dse_screener.fetch_dse_dataframe(rate_limiter=rate_limiter)
    try:
        print("\n[DSE] Sample:")
        print(df.head(10).to_string(index=False))
    except Exception:
        pass
    save_dse_screener(df, save_as_csv=save_csv, _object="screener")

TASKS: Dict[str, Callable[[bool, object], None]] = {
    "dse": task_fetch_dse_screener,
}

def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run one or more data tasks.")
    p.add_argument(
        "--tasks",
        nargs="+",
        default=["dse"],
        help=f"Tasks to run (available: {', '.join(TASKS.keys())})"
    )
    # Make saving the default; allow opting out with --no-save-csv
    g = p.add_mutually_exclusive_group()
    g.add_argument("--save-csv", dest="save_csv", action="store_true")
    g.add_argument("--no-save-csv", dest="save_csv", action="store_false")
    p.set_defaults(save_csv=True)   # <-- default: True (so no flag needed)

    p.add_argument("--fail-fast", action="store_true")
    return p.parse_args(argv)

def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])

    rate_limiter = make_shared_rate_limiter(
        name="orchestrator-shared",
        max_calls=2,
        per_seconds=1.0,
        burst=2,
        max_attempts=6,
        min_wait=1.0,
        max_wait=30.0,
        respect_retry_after=True,
    )

    print(f"=== Starting run_all tasks: {args.tasks} | save_csv={args.save_csv} | fail_fast={args.fail_fast} ===", flush=True)

    failures: List[str] = []
    for tname in args.tasks:
        fn = TASKS.get(tname)
        if not fn:
            print(f"[WARN] Unknown task: {tname} — skipping.")
            continue
        print(f"\n--- Running task: {tname} ---", flush=True)
        try:
            fn(save_csv=args.save_csv, rate_limiter=rate_limiter) # type: ignore
            print(f"[OK] Task '{tname}' completed.", flush=True)
        except Exception as e:
            print(f"[ERR] Task '{tname}' failed: {e}", flush=True)
            traceback.print_exc()
            failures.append(tname)
            if args.fail_fast:
                break

    if failures:
        print(f"\n❌ Completed with failures: {failures}", flush=True)
        return 1

    print("\n✅ All tasks completed successfully.", flush=True)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
