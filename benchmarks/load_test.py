from __future__ import annotations

import argparse
import json
import sys
import time

from run_utils import (Workload, auto_timeout, finalize_run, new_run_dir, new_run_tag,
                       queue_info)
from verify_zero_loss import EXIT


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--total-count", type=int, default=5000,
                   help="total messages, split exactly across producer processes")
    p.add_argument("--threads", type=int, default=1,
                   help="number of producer PROCESSES (each runs one publisher thread)")
    p.add_argument("--rate", type=float, default=0,
                   help="messages/sec per producer process, 0 = as fast as possible")
    p.add_argument("--size", type=int, default=100, help="payload bytes")
    p.add_argument("--consumers", type=int, default=1, help="number of consumer processes")
    p.add_argument("--prefetch", type=int, default=100)
    p.add_argument("--work-ms", type=float, default=0)
    p.add_argument("--idle-timeout", type=float, default=0,
                   help="consumer self-stop after N idle seconds; 0 (default) = the harness stops "
                        "consumers once the queue is drained")
    p.add_argument("--bucket-seconds", type=float, default=1.0)
    p.add_argument("--timeout", type=float, default=0,
                   help="overall cap for the publish phase in seconds, 0 = automatic")
    p.add_argument("--drain-timeout", type=float, default=120,
                   help="max seconds to wait for the queue to empty after publishing")
    p.add_argument("--no-plot", action="store_true")
    args = p.parse_args()

    q_before = queue_info()
    if not q_before.get("ok"):
        sys.exit(f"cannot read queue state ({q_before.get('error')}); is the cluster up?")
    if q_before["messages"] != 0:
        sys.exit(f"queue has {q_before.get('messages')} messages before the run; refusing to start "
                 f"(leftovers would be consumed and mixed into this run). Inspect, then purge with: "
                 f"docker exec rabbit1 rabbitmqctl purge_queue hw2.orders")

    run_dir = new_run_dir("loadtest")
    print(f"run dir: {run_dir}", file=sys.stderr, flush=True)
    wl = Workload(run_dir, run_tag=new_run_tag(), total_count=args.total_count,
                  producers=args.threads, rate=args.rate, size=args.size,
                  consumers=args.consumers, prefetch=args.prefetch, work_ms=args.work_ms,
                  consumer_idle=args.idle_timeout)
    meta = {"kind": "loadtest", "args": vars(args), "queue_before": q_before, "errors": []}

    try:
        wl.start()
        timeout = args.timeout or auto_timeout(wl)
        t0 = time.time()
        if not wl.wait_producers(timeout):
            meta["errors"].append(f"producers did not finish within {timeout}s; stopped")
        print(f"producers finished in {time.time()-t0:.1f}s", file=sys.stderr, flush=True)
        drain = wl.wait_drained(args.drain_timeout)
        print(f"drain: {drain['drained']} after {drain['waited_s']}s", file=sys.stderr, flush=True)
    except KeyboardInterrupt:
        meta["errors"].append("interrupted by user")
    finally:
        wl.stop_all()
        meta["queue_after"] = queue_info()
        meta["workload"] = wl.info()

    metrics, verification, graphs = finalize_run(run_dir, meta, args.bucket_seconds,
                                                 make_plots=not args.no_plot)
    print(json.dumps(metrics, indent=2))
    print(f"metrics: {run_dir / 'metrics.json'}", file=sys.stderr)
    print(f"verdict: {verification['verdict']}  reasons={verification['reasons']}", file=sys.stderr)
    for g in graphs:
        print(f"graph: {g}", file=sys.stderr)
    sys.exit(EXIT[verification["verdict"]])


if __name__ == "__main__":
    main()
