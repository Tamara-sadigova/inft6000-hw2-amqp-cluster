from __future__ import annotations

import argparse
import json
import statistics
from collections import Counter, defaultdict
from pathlib import Path

from run_utils import iter_log_files, read_jsonl, read_meta


def percentile(values, pct):
    """Linear-interpolation percentile (same as numpy's default 'linear' method)."""
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


def _window(first: float | None, last: float | None) -> float | None:
    if first is None or last is None or last <= first:
        return None
    return last - first


def largest_gaps(ts: list[float], run_start: float, n: int = 3, after: float | None = None) -> list[dict]:
    """Largest silences between consecutive events (sorted timestamps).

    If `after` is given, only gaps that END after that time are considered,
    i.e. silences that overlap or follow a fault.
    """
    gaps = []
    for a, b in zip(ts, ts[1:]):
        if after is not None and b < after:
            continue
        gaps.append((b - a, a, b))
    gaps.sort(reverse=True)
    return [{"gap_s": round(g, 3), "from_rel": round(a - run_start, 3), "to_rel": round(b - run_start, 3),
             "from_ts": a, "to_ts": b} for g, a, b in gaps[:n]]


def build_timeline(series: dict[str, list[float]], run_start: float, run_end: float,
                   bucket: float) -> list[dict]:
    """Per-bucket event counts, with EVERY bucket from 0 to the end present (zeros included)."""
    n_buckets = int((run_end - run_start) / bucket) + 1
    rows = [{"t_rel": round(i * bucket, 3), **{k: 0 for k in series}} for i in range(n_buckets)]
    for key, ts_list in series.items():
        for ts in ts_list:
            i = int((ts - run_start) / bucket)
            if 0 <= i < n_buckets:
                rows[i][key] += 1
    return rows


def compute(run_dir: Path, bucket_seconds: float | None = None) -> dict:
    meta = read_meta(run_dir) or {}

    sent = Counter()
    confirmed = Counter()
    nacked = Counter()
    confirm_lat = []
    p_nodes = defaultdict(set)
    confirmed_ids: set[tuple] = set()
    sent_ts, confirm_ts = [], []
    confirm_ts_by_pid = defaultdict(list)

    for path in iter_log_files(run_dir, "producer"):
        for rec in read_jsonl(path):
            ev = rec.get("event")
            pid = rec.get("producer_id")
            ts = rec.get("ts")
            if ev == "publish_sent":
                sent[pid] += 1
                sent_ts.append(ts)
                if rec.get("node"):
                    p_nodes[pid].add(rec["node"])
            elif ev == "publish_confirmed":
                confirmed[pid] += 1
                confirm_ts.append(ts)
                confirm_ts_by_pid[pid].append(ts)
                confirmed_ids.add((pid, rec.get("seq")))
                if "latency_ms" in rec:
                    confirm_lat.append(rec["latency_ms"])
            elif ev == "publish_nack":
                nacked[pid] += 1

    consumed = Counter()
    acked = Counter()
    rejected = Counter()
    redelivered = Counter()
    e2e_lat = []
    c_nodes = defaultdict(set)
    consumed_ids = Counter()
    acked_ids: set[tuple] = set()
    consume_ts, ack_ts = [], []

    for path in iter_log_files(run_dir, "consumer"):
        for rec in read_jsonl(path):
            ev = rec.get("event")
            cid = rec.get("consumer_id")
            ts = rec.get("ts")
            key = (rec.get("producer_id"), rec.get("seq"))
            if ev == "consume":
                consumed[cid] += 1
                consume_ts.append(ts)
                if rec.get("node"):
                    c_nodes[cid].add(rec["node"])
                consumed_ids[key] += 1
                if "latency_ms" in rec:
                    e2e_lat.append(rec["latency_ms"])
                if rec.get("redelivered"):
                    redelivered[cid] += 1
            elif ev == "ack":
                acked[cid] += 1
                ack_ts.append(ts)
                acked_ids.add(key)
            elif ev == "reject":
                rejected[cid] += 1

    for lst in (sent_ts, confirm_ts, consume_ts, ack_ts):
        lst.sort()

    # Separate windows: publish = first publish_sent -> last publish_confirmed,
    # consume = first consume -> last ack. Using one shared window (as before)
    # forced both rates to be equal whenever the counts were equal.
    pub_window = _window(sent_ts[0] if sent_ts else None, confirm_ts[-1] if confirm_ts else None)
    con_window = _window(consume_ts[0] if consume_ts else None, ack_ts[-1] if ack_ts else None)
    all_ts = [t for t in (sent_ts[:1] + consume_ts[:1]) if t is not None]
    end_ts = [t for t in (confirm_ts[-1:] + ack_ts[-1:]) if t is not None]
    run_start = min(all_ts) if all_ts else None
    run_end = max(end_ts) if end_ts else None

    total_confirmed = sum(confirmed.values())
    unique_acked = len(acked_ids)

    result = {
        "run_dir": str(run_dir),
        "run_id": run_dir.name,
        "run_start_ts": run_start,
        "duration_s": round(run_end - run_start, 3) if run_start and run_end else None,
        "publish": {
            "sent_attempts": sum(sent.values()),
            "confirmed": total_confirmed,
            "nacked_or_failed": sum(nacked.values()),
            "window_s": round(pub_window, 3) if pub_window else None,
            "throughput_msg_s": round(total_confirmed / pub_window, 2) if pub_window else None,
            "confirm_latency_ms": summarize(confirm_lat),
        },
        "consume": {
            "consumed_events": sum(consumed.values()),
            "consumed_unique": len(consumed_ids),
            "acked_events": sum(acked.values()),
            "acked_unique": unique_acked,
            "redelivered": sum(redelivered.values()),
            "rejected_malformed": sum(rejected.values()),
            "window_s": round(con_window, 3) if con_window else None,
            "throughput_msg_s": round(unique_acked / con_window, 2) if con_window else None,
            "end_to_end_latency_ms": summarize(e2e_lat),
        },
        "per_producer": {
            pid: {"sent": sent[pid], "confirmed": confirmed[pid], "nacked": nacked[pid],
                  "nodes": sorted(p_nodes.get(pid, ())),
                  "largest_confirm_gap": (largest_gaps(sorted(confirm_ts_by_pid[pid]), run_start, 1) or [None])[0]}
            for pid in sent
        },
        "per_consumer": {
            cid: {"consumed": consumed[cid], "acked": acked[cid], "redelivered": redelivered[cid],
                  "rejected": rejected[cid], "nodes": sorted(c_nodes.get(cid, ()))}
            for cid in consumed
        },
        "integrity": {
            "unique_confirmed": len(confirmed_ids),
            "unique_consumed": len(consumed_ids),
            "unique_acked": unique_acked,
            "confirmed_not_consumed": len(confirmed_ids - set(consumed_ids)),
            "confirmed_not_acked": len(confirmed_ids - acked_ids),
            "acked_not_confirmed": len(acked_ids - confirmed_ids),
            "duplicate_consumed": sum(1 for v in consumed_ids.values() if v > 1),
            "note": "Authoritative verdict is in verification.json (verify_zero_loss.py).",
        },
        "notes": [
            "Producers use synchronous publisher confirms (one message in flight per "
            "producer), so publish throughput per producer is bounded by ~1/confirm latency; "
            "it is not the cluster's capacity.",
            "Latency percentiles only include messages that were sent. Time a producer spent "
            "blocked during an outage is not spread over the messages it would have sent "
            "(coordinated omission): see 'gaps' / 'client_outage' for outage duration.",
        ],
    }

    if run_start is not None:
        result["gaps"] = {
            "confirm": largest_gaps(confirm_ts, run_start),
            "ack": largest_gaps(ack_ts, run_start),
            "typical_confirm_interval_s": (round(statistics.median(
                [b - a for a, b in zip(confirm_ts, confirm_ts[1:])]), 4) if len(confirm_ts) > 2 else None),
        }

    # Fault markers + client-visible outage (only when the run recorded a fault time).
    events = []
    fault = meta.get("fault") or {}
    if run_start is not None:
        for e in meta.get("events") or []:
            if e.get("ts") is not None:
                events.append({"label": e["label"], "ts": e["ts"], "t_rel": round(e["ts"] - run_start, 3)})
        result["events"] = events
        if fault.get("start_ts"):
            after = fault["start_ts"]
            c = largest_gaps(confirm_ts, run_start, 1, after=after)
            a = largest_gaps(ack_ts, run_start, 1, after=after)
            result["client_outage"] = {
                "definition": "largest gap between consecutive events (all producers / all consumers "
                              "merged) that ends after the fault was injected",
                "fault_start_rel": round(after - run_start, 3),
                "confirm": c[0] if c else None,
                "ack": a[0] if a else None,
            }

    if bucket_seconds and run_start is not None and run_end is not None:
        result["timeline_bucket_s"] = bucket_seconds
        result["timeline"] = build_timeline(
            {"confirmed": confirm_ts, "consumed": consume_ts, "acked": ack_ts},
            run_start, run_end, bucket_seconds)
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
