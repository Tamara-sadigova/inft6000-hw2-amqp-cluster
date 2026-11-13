"""Node failure test: stop one RabbitMQ node under load, restore it, verify zero loss.

Measures three different things and keeps them separate:
  client_outage   longest silence in publisher confirms / consumer acks after the
                  fault (what clients actually experienced), from the client logs
  leader election 'candidate -> leader' events for hw2.orders in the broker logs,
                  and leader/term before, during and after
  node_rejoin_s   from `docker start` being issued until the cluster AND the
                  quorum queue report all three members healthy again
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from run_utils import (CONTAINERS, DockerError, Workload, auto_timeout, container_running,
                       finalize_run, leader_elections, leader_snapshot, new_run_dir,
                       new_run_tag, observer_for, queue_info, run_docker)
from health import check, wait_rejoined
from verify_zero_loss import EXIT


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def restore_node(node: str, attempts: int = 3) -> dict:
    errors = []
    for i in range(attempts):
        proc = run_docker("start", node, check=False, timeout=60)
        if proc.returncode == 0 and container_running(node):
            return {"ok": True, "attempts": i + 1, "errors": errors}
        errors.append((proc.stderr or proc.stdout).strip()[:300])
        time.sleep(2)
    return {"ok": bool(container_running(node)), "attempts": attempts, "errors": errors}


def wait_for_new_leader(observer: str, exclude: str, timeout: float = 20) -> dict:
    t0 = time.time()
    snap = leader_snapshot(observer)
    while time.time() - t0 < timeout:
        snap = leader_snapshot(observer)
        if snap.get("ok") and snap.get("leader") and snap["leader"] != exclude:
            break
        time.sleep(1)
    return snap


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--node", default="leader", choices=[*CONTAINERS, "leader"],
                   help="node to stop; 'leader' (default) = current hw2.orders quorum leader")
    p.add_argument("--total-count", type=int, default=20000)
    p.add_argument("--threads", type=int, default=2, help="producer processes")
    p.add_argument("--rate", type=float, default=200, help="msg/s per producer")
    p.add_argument("--consumers", type=int, default=2)
    p.add_argument("--kill-after", type=float, default=5.0, help="seconds after load start")
    p.add_argument("--down-seconds", type=float, default=5.0, help="how long the node stays stopped")
    p.add_argument("--recovery-timeout", type=float, default=90)
    p.add_argument("--drain-timeout", type=float, default=180)
    p.add_argument("--consumer-idle", type=float, default=0,
                   help="consumer self-stop after N idle s; 0 (default) = harness stops them after drain")
    p.add_argument("--dry-run", action="store_true", help="resolve the target and pre-check only")
    args = p.parse_args()

    pre = check("rabbit1")
    q_before = queue_info("rabbit1")
    leader_before = leader_snapshot("rabbit1")
    log(f"pre-check healthy={pre['healthy']} running={pre['running_nodes']} "
        f"leader={leader_before.get('leader')} term={leader_before.get('term')} "
        f"queue={q_before.get('messages')}")
    if not pre["healthy"]:
        log("cluster/quorum queue not healthy before test; abort")
        sys.exit(2)
    if q_before.get("messages") != 0:
        log(f"queue not empty ({q_before.get('messages')}); abort. Inspect, then: "
            f"docker exec rabbit1 rabbitmqctl purge_queue hw2.orders")
        sys.exit(2)
    target = leader_before["leader"] if args.node == "leader" else args.node
    role = "leader" if target == leader_before.get("leader") else "follower"
    observer = observer_for(target)
    log(f"target={target} ({role}), observer={observer}")
    if role == "follower":
        log("NOTE: target is a follower; no leader election is expected")
    if args.dry_run:
        log("dry-run: nothing stopped")
        return

    run_dir = new_run_dir("node-kill")
    log(f"run dir: {run_dir}")
    wl = Workload(run_dir, run_tag=new_run_tag(), total_count=args.total_count,
                  producers=args.threads, rate=args.rate, size=100, consumers=args.consumers,
                  prefetch=100, work_ms=2, consumer_idle=args.consumer_idle)
    events: list[dict] = []
    fault = {"type": "docker stop (graceful SIGTERM shutdown)", "target": target,
             "target_role": role, "injected": False}
    chaos: dict = {"node": target, "target_selection": args.node, "target_role": role,
                   "observer": observer, "leader_before": leader_before}
    meta = {"kind": "node-kill", "args": vars(args), "queue_before": q_before,
            "events": events, "fault": fault, "chaos": chaos, "errors": []}
    restore = None

    try:
        wl.start()
        time.sleep(args.kill_after)
        try:
            log(f"STOP {target}")
            fault["start_ts"] = time.time()
            events.append({"label": f"stop {target}", "ts": fault["start_ts"]})
            run_docker("stop", target, timeout=60)
            fault["injected"] = True
            fault["stopped_ts"] = time.time()
            log(f"STOP done ({fault['stopped_ts'] - fault['start_ts']:.2f}s)")
            chaos["leader_during"] = wait_for_new_leader(observer, exclude=target)
            log(f"leader during outage: {chaos['leader_during'].get('leader')} "
                f"term={chaos['leader_during'].get('term')}")
            remaining = args.down_seconds - (time.time() - fault["stopped_ts"])
            if remaining > 0:
                time.sleep(remaining)
        except (DockerError, Exception) as exc:
            fault["error"] = repr(exc)
            meta["errors"].append(f"fault injection failed: {exc!r}")
            log(f"ERROR during fault injection: {exc!r}")
        finally:
            log(f"START {target}")
            t_restore = time.time()
            fault["restore_issued_ts"] = t_restore
            events.append({"label": f"start {target}", "ts": t_restore})
            restore = restore_node(target)
            fault["restored"] = restore
            if not restore["ok"]:
                meta["errors"].append(f"could not restart {target}: {restore['errors']}")
                log(f"!!! {target} IS NOT RUNNING. Start it manually: docker start {target}")
            rejoin = wait_rejoined(t_restore, args.recovery_timeout, container=observer)
            chaos["node_rejoin"] = rejoin
            if rejoin["ok"]:
                events.append({"label": "cluster+quorum healthy", "ts": t_restore + rejoin["rejoin_s"]})
            log(f"node rejoin: {'%.2fs' % rejoin['rejoin_s'] if rejoin['ok'] else 'TIMEOUT'}")

        if not wl.wait_producers(auto_timeout(wl, extra=args.recovery_timeout + 60)):
            meta["errors"].append("producers did not finish in time; stopped")
        drain = wl.wait_drained(args.drain_timeout, observer=observer)
        log(f"drain: {drain['drained']} after {drain['waited_s']}s")
    except KeyboardInterrupt:
        meta["errors"].append("interrupted by user")
    finally:
        wl.stop_all()
        if fault.get("injected") and not container_running(target):
            log(f"!!! {target} still stopped; trying once more")
            restore_node(target)
        meta["queue_after"] = queue_info(observer)
        meta["workload"] = wl.info()

    chaos["leader_after"] = leader_snapshot(observer)
    if fault.get("start_ts"):
        elections = leader_elections(since=fault["start_ts"] - 1)
        for e in elections:
            e["after_fault_s"] = round(e["ts"] - fault["start_ts"], 3) if e["ts"] else None
        chaos["leader_elections"] = elections
        if elections and elections[0]["ts"]:
            events.append({"label": f"new leader {elections[0]['node']} (term {elections[0]['term']})",
                           "ts": elections[0]["ts"]})
    lb, la = leader_before.get("term"), chaos["leader_after"].get("term")
    chaos["election_occurred"] = (la > lb) if (lb is not None and la is not None) else None
    chaos["term_before"], chaos["term_after"] = lb, la
    fault["recorded_events"] = events

    metrics, verification, graphs = finalize_run(run_dir, meta)
    out = run_dir / "verification.json"
    log(f"verification: {out}")
    co = metrics.get("client_outage") or {}
    summary = {
        "verdict": verification["verdict"],
        "reasons": verification["reasons"],
        "target": f"{target} ({role})",
        "leader": f"{leader_before.get('leader')} (term {lb}) -> "
                  f"{chaos['leader_after'].get('leader')} (term {la})",
        "elections": [(e["node"], e["term"], e.get("after_fault_s")) for e in chaos.get("leader_elections", [])],
        "client_outage_confirm_s": (co.get("confirm") or {}).get("gap_s"),
        "client_outage_ack_s": (co.get("ack") or {}).get("gap_s"),
        "node_rejoin_s": chaos["node_rejoin"].get("rejoin_s") if chaos.get("node_rejoin") else None,
        "confirmed": verification.get("publisher_confirmed"),
        "acked_unique": verification.get("consumer_acked_unique"),
        "missing": verification.get("missing_count"),
        "duplicates": verification.get("duplicate_deliveries"),
        "graphs": [str(g) for g in graphs],
    }
    print(json.dumps(summary, indent=2))
    if restore is not None and not restore["ok"]:
        sys.exit(2)
    sys.exit(EXIT[verification["verdict"]])


if __name__ == "__main__":
    main()
