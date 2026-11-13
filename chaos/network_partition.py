"""Network partition test: isolate one node from hw2-net under load, reconnect, verify.

`docker network disconnect` removes the node from the only network it is on,
so it loses its peers AND its published client ports at the same time.
Clients connected to it are cut off immediately. RabbitMQ itself only notices
the lost peers after net_ticktime (60 s by default, detection after ~45-75 s).

The result therefore classifies what was actually observed:
  client_interruption_only   no broker evidence of a partition (isolation was
                             shorter than RabbitMQ's failure detection)
  rabbitmq_partition_detected  nodes logged the peer as down / partitions seen
  pause_minority_confirmed   the isolated node logged 'Cluster minority/secondary
                             status detected' (pause_minority acted)
A quorum-queue leader election can happen without any of the above: Ra has
its own, faster failure detector. It is reported separately.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from run_utils import (CONTAINERS, NETWORK, DockerError, Workload, auto_timeout,
                       broker_log, cluster_status, finalize_run, leader_elections,
                       leader_snapshot, network_aliases, network_attached, new_run_dir,
                       new_run_tag, observer_for, queue_info, run_docker)
from health import _node_names, _partitions, check, wait_rejoined
from verify_zero_loss import EXIT


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def disconnect(node: str) -> None:
    proc = run_docker("network", "disconnect", NETWORK, node, check=False, timeout=30)
    attached = network_attached(node)
    if attached is not False:
        raise DockerError(f"disconnect did not take effect (rc={proc.returncode}, attached={attached}): "
                          f"{(proc.stderr or proc.stdout).strip()}")


def reconnect(node: str, aliases: list[str], attempts: int = 3) -> dict:
    alias_args = [a for al in aliases for a in ("--alias", al)]
    errors = []
    for i in range(attempts):
        if network_attached(node):
            return {"ok": True, "attempts": i, "aliases": aliases, "errors": errors}
        proc = run_docker("network", "connect", *alias_args, NETWORK, node, check=False, timeout=30)
        if network_attached(node):
            return {"ok": True, "attempts": i + 1, "aliases": aliases, "errors": errors}
        errors.append((proc.stderr or proc.stdout).strip()[:300])
        time.sleep(2)
    return {"ok": False, "attempts": attempts, "aliases": aliases, "errors": errors}


def sample(target: str, observer: str) -> dict:
    s = {"ts": time.time()}
    st = cluster_status(observer, timeout=10)
    s["majority_running_nodes"] = sorted(_node_names(st)) if st.get("ok") else None
    s["majority_partitions"] = _partitions(st) if st.get("ok") else None
    ls = leader_snapshot(observer)
    s["leader"], s["term"] = ls.get("leader"), ls.get("term")
    proc = run_docker("exec", target, "rabbitmq-diagnostics", "-q", "check_running",
                      check=False, timeout=8)
    s["target_rabbit_running"] = {0: True, 124: None}.get(proc.returncode, False)
    return s


def sampler(stop: threading.Event, samples: list, target: str, observer: str, interval: float) -> None:
    while not stop.is_set():
        started = time.time()
        s = sample(target, observer)
        s["started_ts"], s["done_ts"] = started, time.time()
        samples.append(s)
        log(f"  sample (+{s['done_ts'] - started:.1f}s): majority sees {s['majority_running_nodes']} "
            f"partitions={s['majority_partitions']} leader={s['leader']} term={s['term']} "
            f"target_running={s['target_rabbit_running']}")
        stop.wait(interval)


def net_ticktime(container: str) -> int | None:
    proc = run_docker("exec", container, "rabbitmqctl", "eval", "net_kernel:get_net_ticktime().",
                      check=False, timeout=15)
    m = re.search(r"(\d+)", proc.stdout or "")
    return int(m.group(1)) if proc.returncode == 0 and m else None


def broker_evidence(target: str, observer_nodes: list[str], since: float, until: float) -> dict:
    down_pat = rf"node rabbit@{re.escape(target)} down"
    majority = [e for c in observer_nodes for e in broker_log(c, since, until, [down_pat])]
    target_down = broker_log(target, since, until, [r"node rabbit@\S+ down"])
    minority = broker_log(target, since, until, [r"Cluster minority/secondary status detected"])
    partial = [e for c in CONTAINERS
               for e in broker_log(c, since, until, [r"[Pp]artial partition|inconsistent_database|"
                                                     r"[Pp]artitioned network"])]
    return {
        "majority_logged_target_down": majority[:6],
        "target_logged_peers_down": target_down[:6],
        "pause_minority_log": minority[:3],
        "partition_log": partial[:6],
    }


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--node", default="leader", choices=[*CONTAINERS, "leader"],
                   help="node to isolate; 'leader' (default) = current hw2.orders quorum leader")
    p.add_argument("--total-count", type=int, default=20000)
    p.add_argument("--threads", type=int, default=2)
    p.add_argument("--rate", type=float, default=200)
    p.add_argument("--consumers", type=int, default=2)
    p.add_argument("--partition-after", type=float, default=5.0)
    p.add_argument("--partition-seconds", type=float, default=10.0,
                   help="isolation time; RabbitMQ needs > ~75 s (net_ticktime 60) to detect a partition")
    p.add_argument("--sample-interval", type=float, default=2.0)
    p.add_argument("--recovery-timeout", type=float, default=120)
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
    majority_nodes = [c for c in CONTAINERS if c != target]
    aliases = network_aliases(target) or [target]
    ticktime = net_ticktime(observer)
    log(f"target={target} ({role}), observer={observer}, aliases={aliases}, net_ticktime={ticktime}s")
    if ticktime and args.partition_seconds < ticktime:
        log(f"NOTE: partition ({args.partition_seconds}s) is shorter than net_ticktime ({ticktime}s); "
            f"RabbitMQ will most likely NOT detect a cluster partition (pause_minority not exercised)")
    if args.dry_run:
        log("dry-run: no network changes")
        return

    run_dir = new_run_dir("partition")
    log(f"run dir: {run_dir}")
    wl = Workload(run_dir, run_tag=new_run_tag(), total_count=args.total_count,
                  producers=args.threads, rate=args.rate, size=100, consumers=args.consumers,
                  prefetch=100, work_ms=2, consumer_idle=args.consumer_idle)
    events: list[dict] = []
    fault = {"type": f"docker network disconnect {NETWORK}", "target": target,
             "target_role": role, "injected": False}
    chaos: dict = {"node": target, "network": NETWORK, "target_selection": args.node,
                   "target_role": role, "observer": observer, "net_ticktime_s": ticktime,
                   "partition_seconds_requested": args.partition_seconds,
                   "leader_before": leader_before, "samples": []}
    meta = {"kind": "partition", "args": vars(args), "queue_before": q_before,
            "events": events, "fault": fault, "chaos": chaos, "errors": []}
    recon = None
    stop_sampling = threading.Event()
    sampler_thread = None

    try:
        wl.start()
        time.sleep(args.partition_after)
        try:
            log(f"DISCONNECT {target} from {NETWORK}")
            fault["start_ts"] = time.time()
            events.append({"label": f"disconnect {target}", "ts": fault["start_ts"]})
            disconnect(target)
            fault["injected"] = True
            fault["isolated_ts"] = time.time()
            sampler_thread = threading.Thread(
                target=sampler, args=(stop_sampling, chaos["samples"], target, observer,
                                      args.sample_interval), daemon=True)
            sampler_thread.start()
            time.sleep(max(0.0, fault["isolated_ts"] + args.partition_seconds - time.time()))
        except (DockerError, Exception) as exc:
            fault["error"] = repr(exc)
            meta["errors"].append(f"fault injection failed: {exc!r}")
            log(f"ERROR during partition: {exc!r}")
        finally:
            log(f"RECONNECT {target} to {NETWORK}")
            t_restore = time.time()
            fault["restore_issued_ts"] = t_restore
            recon = reconnect(target, aliases)
            fault["reconnected_ts"] = time.time()
            stop_sampling.set()
            if sampler_thread is not None:
                sampler_thread.join(timeout=30)
            for s in chaos["samples"]:
                s["completed_after_reconnect"] = s["done_ts"] > fault["reconnected_ts"]
            fault["restored"] = recon
            events.append({"label": f"reconnect {target}", "ts": fault["reconnected_ts"]})
            if not recon["ok"]:
                meta["errors"].append(f"could not reconnect {target}: {recon['errors']}")
                log(f"!!! {target} IS STILL ISOLATED. Run: docker network connect {NETWORK} {target}")
            if fault.get("isolated_ts"):
                chaos["partition_seconds_actual"] = round(fault["reconnected_ts"] - fault["isolated_ts"], 3)
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
        if fault.get("injected") and not network_attached(target):
            log(f"!!! {target} still detached; trying once more")
            reconnect(target, aliases)
        meta["queue_after"] = queue_info(observer)
        meta["workload"] = wl.info()

    chaos["leader_after"] = leader_snapshot(observer)
    if fault.get("start_ts"):
        until = time.time()
        ev = broker_evidence(target, majority_nodes, fault["start_ts"] - 1, until)
        chaos["broker_evidence"] = ev
        samples = chaos["samples"]
        majority_saw_down = any(s["majority_running_nodes"] is not None and target not in s["majority_running_nodes"]
                                for s in samples) or bool(ev["majority_logged_target_down"])
        detected = majority_saw_down or bool(ev["target_logged_peers_down"]) or bool(ev["partition_log"]) \
            or any(s["majority_partitions"] for s in samples)
        if ev["pause_minority_log"]:
            chaos["classification"] = "pause_minority_confirmed"
        elif detected:
            chaos["classification"] = "rabbitmq_partition_detected"
        else:
            chaos["classification"] = "client_interruption_only"
        chaos["pause_minority_tested"] = bool(ev["pause_minority_log"])
        for e in ev["majority_logged_target_down"][:1]:
            if e["ts"]:
                events.append({"label": f"majority: {target} down", "ts": e["ts"]})
        for e in ev["pause_minority_log"][:1]:
            if e["ts"]:
                events.append({"label": f"{target} paused (minority)", "ts": e["ts"]})
        elections = leader_elections(since=fault["start_ts"] - 1, until=until)
        for e in elections:
            e["after_fault_s"] = round(e["ts"] - fault["start_ts"], 3) if e["ts"] else None
        chaos["leader_elections"] = elections
        if elections and elections[0]["ts"]:
            events.append({"label": f"new leader {elections[0]['node']} (term {elections[0]['term']})",
                           "ts": elections[0]["ts"]})
    lb, la = leader_before.get("term"), chaos["leader_after"].get("term")
    chaos["term_before"], chaos["term_after"] = lb, la
    chaos["election_occurred"] = (la > lb) if (lb is not None and la is not None) else None

    metrics, verification, graphs = finalize_run(run_dir, meta)
    log(f"verification: {run_dir / 'verification.json'}")
    co = metrics.get("client_outage") or {}
    summary = {
        "verdict": verification["verdict"],
        "reasons": verification["reasons"],
        "target": f"{target} ({role})",
        "classification": chaos.get("classification"),
        "pause_minority_tested": chaos.get("pause_minority_tested"),
        "partition_seconds_actual": chaos.get("partition_seconds_actual"),
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
    if recon is not None and not recon["ok"]:
        sys.exit(2)
    sys.exit(EXIT[verification["verdict"]])


if __name__ == "__main__":
    main()
