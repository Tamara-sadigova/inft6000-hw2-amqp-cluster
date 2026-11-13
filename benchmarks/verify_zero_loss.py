"""Zero-loss verification for one run directory.

Verdicts
  PASS          every publisher-confirmed message of this run was consumed AND
                acknowledged, and all evidence checks passed.
  FAIL          at least one confirmed message was never consumed (candidate
                permanent loss), with otherwise complete evidence.
  INCONCLUSIVE  the evidence is insufficient to claim PASS: missing or
                truncated logs, a client that had to be force-killed, queue
                state not recorded or not empty, confirmed-but-never-acked
                messages, an incomplete run, or a fault that was not injected.

Terms
  confirmed   broker sent a publisher confirm (publish_confirmed event)
  consumed    a consumer received the message (consume event)
  acked       the consumer sent basic.ack after processing (ack event)
  duplicate   the same (producer_id, seq) was consumed more than once.
              Expected under at-least-once delivery after a failover; it does
              not fail the verdict.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_utils import iter_log_files, read_jsonl, read_meta

REPO_ROOT = Path(__file__).resolve().parents[1]
VALIDATE = REPO_ROOT / "clients" / "validate.py"
EXIT = {"PASS": 0, "FAIL": 1, "INCONCLUSIVE": 3}


def run_validate(run_dir: Path) -> dict | None:
    log_dir = run_dir / "logs"
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "validate.json"
        proc = subprocess.run(
            [sys.executable, str(VALIDATE), "--logs", str(log_dir), "--json-out", str(out)],
            cwd=str(REPO_ROOT), capture_output=True, text=True,
        )
        if not out.exists() or out.stat().st_size == 0:
            return {"error": f"validate.py produced no JSON (exit {proc.returncode})",
                    "stderr": proc.stderr[-1000:]}
        return json.loads(out.read_text())


def _summary_for(log_path: Path) -> Path:
    return log_path.with_name(log_path.stem + "-summary.json")


def check_log_completeness(run_dir: Path) -> list[str]:
    problems = []
    for path in iter_log_files(run_dir):
        summary_path = _summary_for(path)
        if not summary_path.exists():
            problems.append(f"{path.name}: no summary file (client did not exit cleanly; log may be truncated)")
            continue
        try:
            summary = json.loads(summary_path.read_text())
        except json.JSONDecodeError:
            problems.append(f"{summary_path.name}: unreadable")
            continue
        counts = Counter(rec.get("event") for rec in read_jsonl(path))
        if path.name.startswith("producer"):
            pairs = [("confirmed", "publish_confirmed"), ("sent_attempts", "publish_sent")]
        else:
            pairs = [("consumed", "consume"), ("acked", "ack")]
        for skey, ev in pairs:
            if summary.get(skey) != counts.get(ev, 0):
                problems.append(f"{path.name}: summary {skey}={summary.get(skey)} but log has "
                                f"{counts.get(ev, 0)} '{ev}' events (log truncated?)")
        if summary.get("errors"):
            problems.append(f"{summary_path.name}: client reported errors {summary['errors']}")
    return problems


def analyse_logs(run_dir: Path, expected_producers: set[str] | None):
    confirmed = defaultdict(set)
    sent = defaultdict(set)
    consumed = defaultdict(Counter)
    acked = defaultdict(set)
    first_deliveries = []
    foreign = 0

    for path in iter_log_files(run_dir, "producer"):
        for rec in read_jsonl(path):
            pid, seq, ev = rec.get("producer_id"), rec.get("seq"), rec.get("event")
            if ev == "publish_confirmed":
                confirmed[pid].add(seq)
            elif ev == "publish_sent":
                sent[pid].add(seq)

    known = set(confirmed) | set(sent)
    if expected_producers:
        known |= expected_producers

    consumes = []
    for path in iter_log_files(run_dir, "consumer"):
        for rec in read_jsonl(path):
            ev = rec.get("event")
            if ev not in ("consume", "ack"):
                continue
            pid = rec.get("producer_id")
            if pid not in known:
                if ev == "consume":
                    foreign += 1
                continue
            if ev == "consume":
                consumes.append(rec)
            else:
                acked[pid].add(rec.get("seq"))
    consumes.sort(key=lambda r: r.get("ts") or 0)

    reordered = 0
    highest = defaultdict(int)
    for rec in consumes:
        pid, seq, cid = rec["producer_id"], rec["seq"], rec.get("consumer_id")
        consumed[pid][seq] += 1
        if consumed[pid][seq] == 1:
            if seq < highest[(cid, pid)]:
                reordered += 1
            highest[(cid, pid)] = max(highest[(cid, pid)], seq)

    streams = {}
    for pid in sorted(known):
        conf, cons, ack = confirmed[pid], set(consumed[pid]), acked[pid]
        lost = sorted(conf - cons)
        not_acked = sorted((conf & cons) - ack)
        streams[pid] = {
            "confirmed": len(conf),
            "consumed_unique": len(cons),
            "acked_unique": len(ack),
            "missing_never_consumed": len(lost),
            "missing_sample": lost[:20],
            "consumed_not_acked": len(not_acked),
            "consumed_not_acked_sample": not_acked[:20],
            "consumed_but_unconfirmed": len(cons - conf),
            "duplicates": sum(n - 1 for n in consumed[pid].values() if n > 1),
        }
    totals = {k: sum(s[k] for s in streams.values()) for k in
              ("confirmed", "consumed_unique", "acked_unique", "missing_never_consumed",
               "consumed_not_acked", "consumed_but_unconfirmed", "duplicates")}
    totals["reordered"] = reordered
    totals["foreign_consumed"] = foreign
    return streams, totals


def verify(run_dir: Path) -> dict:
    meta = read_meta(run_dir)
    reasons: list[str] = []
    warnings: list[str] = []

    if not any(iter_log_files(run_dir)):
        return {"run_dir": str(run_dir), "verdict": "INCONCLUSIVE", "integrity_ok": False,
                "reasons": [f"no JSONL logs under {run_dir / 'logs'}"]}

    expected = set((meta or {}).get("workload", {}).get("producers", {}) or {})
    streams, totals = analyse_logs(run_dir, expected or None)

    reasons += check_log_completeness(run_dir)

    if meta is None:
        reasons.append("no run_meta.json: queue state before/after was not recorded and producer ids "
                       "may not be run-unique (run predates the current harness)")
    else:
        qb, qa = meta.get("queue_before") or {}, meta.get("queue_after") or {}
        if not qb.get("ok"):
            reasons.append(f"queue state before the run unknown: {qb.get('error')}")
        elif qb.get("messages") != 0:
            reasons.append(f"queue was not empty before the run ({qb.get('messages')} messages)")
        if not qa.get("ok"):
            reasons.append(f"queue state after the run unknown: {qa.get('error')}")
        elif qa.get("messages") != 0:
            reasons.append(f"queue not empty after the run ({qa.get('messages')} messages, "
                           f"{qa.get('unacked')} unacked): consumers did not drain it")
        wl = meta.get("workload") or {}
        drain = wl.get("drain")
        if drain is not None and not drain.get("drained"):
            reasons.append(f"queue drain was not confirmed before consumers were stopped "
                           f"(waited {drain.get('waited_s')}s)")
        for pinfo in wl.get("processes") or []:
            if pinfo.get("forced"):
                reasons.append(f"{pinfo['role']} {pinfo['name']} had to be force-stopped "
                               f"({pinfo['stop_method']}); its log may be incomplete")
            elif pinfo.get("exit_code") not in (0, None):
                reasons.append(f"{pinfo['role']} {pinfo['name']} exited with code {pinfo['exit_code']}")
            if pinfo.get("role") == "producer" and pinfo.get("timed_out"):
                reasons.append(f"producer {pinfo['name']} did not finish before the timeout")
        for pid, n in (wl.get("producers") or {}).items():
            got = streams.get(pid, {}).get("confirmed", 0)
            if got != n:
                reasons.append(f"producer {pid}: {got} confirmed of {n} requested (run incomplete)")
        if meta.get("fault") is not None and not meta["fault"].get("injected"):
            reasons.append(f"fault was not injected: {meta['fault'].get('error')}")
        if meta.get("errors"):
            reasons += [f"harness error: {e}" for e in meta["errors"]]

    if totals["confirmed"] == 0:
        reasons.append("no publisher-confirmed messages")
    if totals["consumed_not_acked"]:
        reasons.append(f"{totals['consumed_not_acked']} confirmed messages were consumed but never "
                       f"acknowledged (processing not proven)")
    if totals["consumed_but_unconfirmed"]:
        reasons.append(f"{totals['consumed_but_unconfirmed']} consumed messages have no publish_confirmed "
                       f"event (producer log incomplete, or confirm lost before the producer stopped)")
    if totals["foreign_consumed"]:
        warnings.append(f"{totals['foreign_consumed']} consumed messages belong to other runs/producers "
                        f"(ignored; they were acked and removed from the queue by this run)")
    if totals["duplicates"]:
        warnings.append(f"{totals['duplicates']} duplicate deliveries (at-least-once redelivery; not a loss)")

    v = run_validate(run_dir)
    if v is None or "error" in v:
        warnings.append(f"validate.py cross-check unavailable: {v and v.get('error')}")
    else:
        vlost = (v.get("totals") or {}).get("lost")
        if vlost != totals["missing_never_consumed"]:
            reasons.append(f"validate.py disagrees: lost={vlost} vs {totals['missing_never_consumed']}")

    if totals["missing_never_consumed"] > 0:
        verdict = "FAIL"
    elif reasons:
        verdict = "INCONCLUSIVE"
    else:
        verdict = "PASS"

    return {
        "run_dir": str(run_dir),
        "verdict": verdict,
        "integrity_ok": verdict == "PASS",
        "publisher_confirmed": totals["confirmed"],
        "consumer_consumed_unique": totals["consumed_unique"],
        "consumer_acked_unique": totals["acked_unique"],
        "missing_count": totals["missing_never_consumed"],
        "missing_sample": [s for st in streams.values() for s in st["missing_sample"]][:20],
        "consumed_not_acked_count": totals["consumed_not_acked"],
        "consumed_but_unconfirmed_count": totals["consumed_but_unconfirmed"],
        "duplicate_deliveries": totals["duplicates"],
        "reordered": totals["reordered"],
        "foreign_consumed": totals["foreign_consumed"],
        "queue_before": (meta or {}).get("queue_before"),
        "queue_after": (meta or {}).get("queue_after"),
        "reasons": reasons,
        "warnings": warnings,
        "validate_py": None if not v or "error" in v else {
            "verdict": v.get("verdict"), "totals": v.get("totals"),
            "producer_reconnects": v.get("producer_reconnects"),
            "consumer_reconnects": v.get("consumer_reconnects")},
        "streams": streams,
        "definitions": {
            "missing_count": "confirmed but never consumed (candidate permanent loss)",
            "consumed_not_acked_count": "confirmed and consumed but no ack logged",
            "duplicate_deliveries": "extra deliveries of an already-consumed message",
        },
    }


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("run_dir", type=Path)
    p.add_argument("--out", type=Path, default=None)
    args = p.parse_args()
    result = verify(args.run_dir)
    out = args.out or (args.run_dir / "verification.json")
    out.write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))
    sys.exit(EXIT[result["verdict"]])


if __name__ == "__main__":
    main()
