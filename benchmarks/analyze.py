from __future__ import annotations

import argparse
import json
import statistics
from collections import Counter, defaultdict
from pathlib import Path

from run_utils import iter_log_files, read_jsonl


def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    k = (len(ordered) - 1) * (pct / 100.0)
    f = int(k)
    c = min(f + 1, len(ordered) - 1)
    if f == c:
        return round(ordered[f], 3)
    return round(ordered[f] + (ordered[c] - ordered[f]) * (k - f), 3)


def summarize(values):
    if not values:
        return {"count": 0, "p50": None, "p95": None, "p99": None,
                "min": None, "max": None, "mean": None}
    return {
        "count": len(values),
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
        "min": round(min(values), 3),
        "max": round(max(values), 3),
        "mean": round(statistics.fmean(values), 3),
    }


def build_timeline(run_dir: Path, run_start: float, bucket: float) -> list[dict]:
    buckets = defaultdict(lambda: {"confirmed": 0, "acked": 0, "consumed": 0})
    for path in iter_log_files(run_dir, "producer"):
        for rec in read_jsonl(path):
            ev = rec.get("event")
            ts = rec.get("ts")
            if ev in ("publish_confirmed",) and ts is not None:
                b = int((ts - run_start) / bucket)
                buckets[b]["confirmed"] += 1
    for path in iter_log_files(run_dir, "consumer"):
        for rec in read_jsonl(path):
            ev = rec.get("event")
            ts = rec.get("ts")
            if ev == "consume" and ts is not None:
                b = int((ts - run_start) / bucket)
                buckets[b]["consumed"] += 1
            elif ev == "ack" and ts is not None:
                b = int((ts - run_start) / bucket)
                buckets[b]["acked"] += 1
    return [{"t_rel": round(b * bucket, 2), **buckets[b]} for b in sorted(buckets)]


def compute(run_dir: Path, bucket_seconds: float | None = None) -> dict:
    sent = Counter()
    confirmed = Counter()
    nacked = Counter()
    confirm_lat = defaultdict(list)
    p_first, p_last, p_node = {}, {}, {}
    confirmed_ids: set[tuple] = set()

    for path in iter_log_files(run_dir, "producer"):
        for rec in read_jsonl(path):
            ev = rec.get("event")
            pid = rec.get("producer_id")
            seq = rec.get("seq")
            ts = rec.get("ts")
            if ev == "publish_sent":
                sent[pid] += 1
                p_first.setdefault(pid, ts)
                p_last[pid] = ts
                p_node[pid] = rec.get("node")
            elif ev == "publish_confirmed":
                confirmed[pid] += 1
                p_last[pid] = ts
                confirmed_ids.add((pid, seq))
                if "latency_ms" in rec:
                    confirm_lat[pid].append(rec["latency_ms"])
            elif ev == "publish_nack":
                nacked[pid] += 1
                p_last[pid] = ts

    consumed = Counter()
    acked = Counter()
    rejected = Counter()
    redelivered = Counter()
    e2e_lat = defaultdict(list)
    c_first, c_last, c_node = {}, {}, {}
    consumed_ids = Counter()
    acked_ids: set[tuple] = set()

    for path in iter_log_files(run_dir, "consumer"):
        for rec in read_jsonl(path):
            ev = rec.get("event")
            cid = rec.get("consumer_id")
            pid = rec.get("producer_id")
            seq = rec.get("seq")
            ts = rec.get("ts")
            if ev == "consume":
                consumed[cid] += 1
                c_first.setdefault(cid, ts)
                c_last[cid] = ts
                c_node[cid] = rec.get("node")
                consumed_ids[(pid, seq)] += 1
                if "latency_ms" in rec:
                    e2e_lat[cid].append(rec["latency_ms"])
                if rec.get("redelivered"):
                    redelivered[cid] += 1
            elif ev == "ack":
                acked[cid] += 1
                acked_ids.add((pid, seq))
            elif ev == "reject":
                rejected[cid] += 1

    all_confirm = [v for lst in confirm_lat.values() for v in lst]
    all_e2e = [v for lst in e2e_lat.values() for v in lst]

    starts = [t for t in [*p_first.values(), *c_first.values()] if t]
    ends = [t for t in [*p_last.values(), *c_last.values()] if t]
    run_start = min(starts) if starts else None
    run_end = max(ends) if ends else None
    duration = (run_end - run_start) if run_start and run_end else None

    total_confirmed = sum(confirmed.values())
    total_acked = sum(acked.values())

    result = {
        "run_dir": str(run_dir),
        "duration_s": round(duration, 3) if duration else None,
        "publish": {
            "sent_attempts": sum(sent.values()),
            "confirmed": total_confirmed,
            "nacked_or_failed": sum(nacked.values()),
            "throughput_msg_s": round(total_confirmed / duration, 2) if duration else None,
            "confirm_latency_ms": summarize(all_confirm),
        },
        "consume": {
            "consumed": sum(consumed.values()),
            "acked": total_acked,
            "redelivered": sum(redelivered.values()),
            "rejected_malformed": sum(rejected.values()),
            "throughput_msg_s": round(total_acked / duration, 2) if duration else None,
            "end_to_end_latency_ms": summarize(all_e2e),
        },
        "per_producer": {
            pid: {"sent": sent[pid], "confirmed": confirmed[pid],
                  "nacked": nacked[pid], "node": p_node.get(pid)}
            for pid in sent
        },
        "per_consumer": {
            cid: {"consumed": consumed[cid], "acked": acked[cid],
                  "redelivered": redelivered[cid],
                  "rejected": rejected[cid], "node": c_node.get(cid)}
            for cid in consumed
        },
        "integrity": {
            "unique_confirmed": len(confirmed_ids),
            "unique_acked": len(acked_ids),
            "missing_after_run": len(confirmed_ids - acked_ids),
            "unexpected_acked": len(acked_ids - confirmed_ids),
            "duplicate_consumed": sum(1 for v in consumed_ids.values() if v > 1),
        },
    }
    if bucket_seconds and run_start and duration:
        result["timeline"] = build_timeline(run_dir, run_start, bucket_seconds)
    return result


def main():
    p = argparse.ArgumentParser()
    p.add_argument("run_dir", type=Path)
    p.add_argument("--bucket-seconds", type=float, default=1.0)
    p.add_argument("--out", type=Path, default=None)
    args = p.parse_args()
    metrics = compute(args.run_dir, bucket_seconds=args.bucket_seconds)
    out = args.out or (args.run_dir / "metrics.json")
    out.write_text(json.dumps(metrics, indent=2))
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()