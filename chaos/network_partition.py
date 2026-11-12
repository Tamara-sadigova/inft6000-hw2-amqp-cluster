from __future__ import annotations

import argparse
import json
import math
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from run_utils import (CONTAINERS, NETWORK, cluster_status, new_run_dir, run_docker, start_consumer, start_producer)
from health import check, cluster_healthy
from verify_zero_loss import verify


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def wait_healthy(timeout: float, poll: float = 2.0, container: str = "rabbit1") -> float | None:
    t0 = time.time()
    while time.time() - t0 < timeout:
        if cluster_healthy(cluster_status(container)):
            return time.time() - t0
        time.sleep(poll)
    return None


def disconnect(node: str) -> None:
    run_docker("network", "disconnect", NETWORK, node, check=False, timeout=30)


def reconnect(node: str) -> None:
    run_docker("network", "connect", NETWORK, node, check=False, timeout=30)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--node", default="rabbit3", choices=CONTAINERS)
    p.add_argument("--total-count", type=int, default=20000)
    p.add_argument("--threads", type=int, default=2)
    p.add_argument("--rate", type=float, default=200)
    p.add_argument("--partition-after", type=float, default=5.0)
    p.add_argument("--partition-seconds", type=float, default=10.0)
    p.add_argument("--recovery-timeout", type=float, default=90)
    p.add_argument("--consumer-idle", type=float, default=20)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    run_dir = new_run_dir("partition")
    log(f"run dir: {run_dir}")
    if args.dry_run:
        log("dry-run: no network changes")
        return

    pre = check()
    log(f"pre-check healthy={pre['healthy']} running={pre['running_nodes']}")
    if not pre["healthy"]:
        log("cluster not healthy before test; abort")
        sys.exit(2)

    consumers = [start_consumer(run_dir, consumer_id=f"c{i+1}", threads=1,
                                prefetch=100, work_ms=2,
                                idle_timeout=args.consumer_idle) for i in range(2)]
    per = math.ceil(args.total_count / args.threads)
    producers = [start_producer(run_dir, producer_id=f"p{i+1}", threads=1,
                                count=per, rate=args.rate)
                 for i in range(args.threads)]

    time.sleep(args.partition_after)
    log(f"DISCONNECT {args.node} from {NETWORK}")
    t_dis = time.time()
    disconnect(args.node)

    try:
        time.sleep(args.partition_seconds)
    finally:
        log(f"RECONNECT {args.node} to {NETWORK}")
        t_con = time.time()
        reconnect(args.node)

    rec = wait_healthy(args.recovery_timeout)
    log(f"recovery: {'%.2fs' % rec if rec is not None else 'TIMEOUT'}")

    for pr in producers:
        pr.wait(timeout=max(120, args.total_count / max(args.rate, 1) + 120))
    for c in consumers:
        c.wait(timeout=args.consumer_idle + 120)

    result = verify(run_dir, wait_seconds=30)
    result["chaos"] = {
        "node": args.node,
        "network": NETWORK,
        "disconnect_at": t_dis,
        "reconnect_at": t_con,
        "partition_seconds": args.partition_seconds,
        "recovery_s": rec,
    }
    out = run_dir / "verification.json"
    out.write_text(json.dumps(result, indent=2))
    log(f"verification: {out}")
    print(json.dumps(result, indent=2))
    sys.exit(0 if result["integrity_ok"] else 1)


if __name__ == "__main__":
    main()