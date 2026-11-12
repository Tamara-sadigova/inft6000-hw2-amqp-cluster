from __future__ import annotations

import argparse
import json
import math
import sys
import time
from pathlib import Path

from run_utils import new_run_dir, start_consumer, start_producer
import analyze


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--total-count", type=int, default=5000,
                   help="total messages to publish across all producer processes")
    p.add_argument("--threads", type=int, default=1,
                   help="number of producer processes")
    p.add_argument("--rate", type=float, default=0,
                   help="messages/sec per producer process, 0 = as fast as possible")
    p.add_argument("--size", type=int, default=100, help="payload bytes")
    p.add_argument("--consumers", type=int, default=1,
                   help="number of consumer processes")
    p.add_argument("--prefetch", type=int, default=100)
    p.add_argument("--work-ms", type=float, default=0)
    p.add_argument("--idle-timeout", type=float, default=15,
                   help="consumer stops after this many seconds without messages")
    p.add_argument("--bucket-seconds", type=float, default=1.0)
    p.add_argument("--timeout", type=float, default=0, help="hard cap, 0 = none")
    args = p.parse_args()

    run_dir = new_run_dir("loadtest")
    print(f"run dir: {run_dir}", file=sys.stderr, flush=True)

    consumers = [
        start_consumer(run_dir, consumer_id=f"c{i+1}", threads=1,
                       prefetch=args.prefetch, work_ms=args.work_ms,
                       idle_timeout=args.idle_timeout)
        for i in range(args.consumers)
    ]
    time.sleep(0.5)
    per_thread = math.ceil(args.total_count / args.threads) if args.total_count else 0
    producers = [
        start_producer(run_dir, producer_id=f"p{i+1}", threads=1,
                       count=per_thread, rate=args.rate, size=args.size)
        for i in range(args.threads)
    ]

    t0 = time.time()
    for pr in producers:
        pr.wait(timeout=args.timeout or None)
    print(f"producers finished in {time.time()-t0:.1f}s", file=sys.stderr, flush=True)
    for c in consumers:
        c.wait(timeout=args.idle_timeout + 60)

    metrics = analyze.compute(run_dir, bucket_seconds=args.bucket_seconds)
    out = run_dir / "metrics.json"
    out.write_text(json.dumps(metrics, indent=2))
    print(json.dumps(metrics, indent=2))
    print(f"metrics: {out}", file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()