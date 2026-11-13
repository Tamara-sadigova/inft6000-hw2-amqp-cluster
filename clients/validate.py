import argparse
import glob
import json
import os
import sys
from collections import defaultdict


def read_events(paths):
    for path in paths:
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except ValueError:
                    continue


def collect_paths(log_dirs, files):
    paths = list(files or [])
    for directory in log_dirs or []:
        paths.extend(sorted(glob.glob(os.path.join(directory, "*.jsonl"))))
    if not paths:
        raise SystemExit("no .jsonl log files found")
    return paths


def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[int(round(pct / 100.0 * (len(ordered) - 1)))], 3)


def find_gaps(numbers):
    gaps = []
    expected = 1
    for value in sorted(numbers):
        if value > expected:
            gaps.append((expected, value - 1))
        expected = value + 1
    return gaps


def analyse(events):
    confirmed = defaultdict(set)
    sent = defaultdict(set)
    nacks = defaultdict(int)
    producer_reconnects = 0
    consumer_reconnects = 0
    consumes = []
    for event in events:
        kind = event.get("event")
        producer_id = event.get("producer_id")
        seq = event.get("seq")
        if kind == "publish_confirmed":
            confirmed[producer_id].add(seq)
        elif kind == "publish_sent":
            sent[producer_id].add(seq)
        elif kind == "publish_nack":
            nacks[producer_id] += 1
        elif kind == "consume":
            consumes.append(event)
        elif kind == "reconnect" and event.get("status") == "connected" and event.get("reason") != "start":
            if event.get("consumer_id"):
                consumer_reconnects += 1
            else:
                producer_reconnects += 1

    consumes.sort(key=lambda event: event["ts"])
    known = set(confirmed) | set(sent)
    delivered = defaultdict(list)
    foreign = 0
    for event in consumes:
        if event["producer_id"] in known:
            delivered[event["producer_id"]].append(event)
        else:
            foreign += 1

    streams = {}
    all_latencies = []
    for producer_id in sorted(known):
        events_for_stream = delivered.get(producer_id, [])
        unique = set()
        duplicates = 0
        reorders = 0
        highest = {}
        for event in events_for_stream:
            seq = event["seq"]
            if seq in unique:
                duplicates += 1
                continue
            unique.add(seq)
            consumer = event.get("consumer_id")
            if seq < highest.get(consumer, 0):
                reorders += 1
            highest[consumer] = max(highest.get(consumer, 0), seq)
            if event.get("latency_ms") is not None:
                all_latencies.append(event["latency_ms"])
        lost = sorted(confirmed[producer_id] - unique)
        streams[producer_id] = {
            "confirmed": len(confirmed[producer_id]),
            "max_confirmed_seq": max(confirmed[producer_id]) if confirmed[producer_id] else 0,
            "confirmed_gaps": find_gaps(confirmed[producer_id]),
            "consumed_unique": len(unique),
            "lost": len(lost),
            "lost_sample": lost[:20],
            "duplicates": duplicates,
            "reordered": reorders,
            "delivered_but_unconfirmed": len(unique - confirmed[producer_id]),
            "nack_or_failure_events": nacks[producer_id],
        }

    totals = {key: sum(stream[key] for stream in streams.values()) for key in ("confirmed", "consumed_unique", "lost", "duplicates", "reordered", "delivered_but_unconfirmed", "nack_or_failure_events")}
    first = consumes[0]["ts"] if consumes else None
    last = consumes[-1]["ts"] if consumes else None
    span = (last - first) if consumes else 0
    return {
        "verdict": "PASS" if totals["lost"] == 0 and totals["confirmed"] > 0 else "FAIL",
        "totals": totals,
        "producer_reconnects": producer_reconnects,
        "consumer_reconnects": consumer_reconnects,
        "foreign_messages_ignored": foreign,
        "consume_throughput_msg_s": round(len(consumes) / span, 1) if span > 0 else None,
        "end_to_end_latency_ms": {
            "p50": percentile(all_latencies, 50),
            "p95": percentile(all_latencies, 95),
            "p99": percentile(all_latencies, 99),
            "max": round(max(all_latencies), 3) if all_latencies else None,
        },
        "streams": streams,
    }


def to_markdown(result):
    totals = result["totals"]
    latency = result["end_to_end_latency_ms"]
    verdict = "PASS: zero confirmed messages lost" if result["verdict"] == "PASS" else "FAIL: confirmed messages were lost or nothing was confirmed"
    lines = [
        "## Validation",
        "",
        f"**Verdict: {verdict}**",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Confirmed by broker | {totals['confirmed']} |",
        f"| Consumed (unique) | {totals['consumed_unique']} |",
        f"| Lost (confirmed but never consumed) | {totals['lost']} |",
        f"| Duplicates (redelivered) | {totals['duplicates']} |",
        f"| Out of order | {totals['reordered']} |",
        f"| Delivered although confirm was lost | {totals['delivered_but_unconfirmed']} |",
        f"| Producer nack / failure events | {totals['nack_or_failure_events']} |",
        f"| Producer reconnects | {result['producer_reconnects']} |",
        f"| Consumer reconnects | {result['consumer_reconnects']} |",
        f"| End to end latency p50 / p95 / p99 / max (ms) | {latency['p50']} / {latency['p95']} / {latency['p99']} / {latency['max']} |",
        "",
        "| Producer | Confirmed | Consumed | Lost | Duplicates | Out of order |",
        "|---|---|---|---|---|---|",
    ]
    for producer_id, stream in result["streams"].items():
        lines.append(f"| {producer_id} | {stream['confirmed']} | {stream['consumed_unique']} | {stream['lost']} | {stream['duplicates']} | {stream['reordered']} |")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Compare producer and consumer logs: loss, duplicates, reordering")
    parser.add_argument("--logs", nargs="*", default=[], help="directories containing producer and consumer .jsonl logs")
    parser.add_argument("--files", nargs="*", default=[], help="individual .jsonl log files")
    parser.add_argument("--json-out", default=None)
    parser.add_argument("--md-out", default=None)
    args = parser.parse_args()
    if not args.logs and not args.files:
        args.logs = ["logs"]

    result = analyse(read_events(collect_paths(args.logs, args.files)))
    markdown = to_markdown(result)
    print(markdown)
    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as handle:
            json.dump(result, handle, indent=2)
    if args.md_out:
        with open(args.md_out, "w", encoding="utf-8") as handle:
            handle.write(markdown)
    sys.exit(0 if result["verdict"] == "PASS" else 1)


if __name__ == "__main__":
    main()
