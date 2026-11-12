from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
VALIDATE = REPO_ROOT / "clients" / "validate.py"
LOG_SUBDIR = "logs"


def run_validate(run_dir: Path) -> dict:
    log_dir = run_dir / LOG_SUBDIR
    if not log_dir.exists() or not any(log_dir.glob("*.jsonl")):
        raise RuntimeError(f"no JSONL logs under {log_dir}; nothing to verify")
    tmp = tempfile.NamedTemporaryFile("w+", suffix=".json", delete=False)
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        proc = subprocess.run(
            [sys.executable, str(VALIDATE),
             "--logs", str(log_dir),
             "--json-out", str(tmp_path)],
            cwd=str(REPO_ROOT), capture_output=True, text=True,
        )
        if not tmp_path.exists() or tmp_path.stat().st_size == 0:
            raise RuntimeError(
                f"validate.py produced no JSON (exit {proc.returncode}); "
                f"stderr={proc.stderr[-2000:]}"
            )
        return json.loads(tmp_path.read_text())
    finally:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass


def _samples(v: dict, key: str) -> list:
    out = []
    for stream in (v.get("streams") or {}).values():
        for s in (stream.get(key) or []):
            out.append(s)
    return out[:20]


def verify(run_dir: Path, wait_seconds: float = 0.0, poll_interval: float = 2.0) -> dict:
    deadline = time.time() + wait_seconds
    last_lost = None
    stable = 0
    while True:
        raw = run_validate(run_dir)
        lost = (raw.get("totals") or {}).get("lost", 0)
        if lost == 0 or time.time() >= deadline:
            break
        if last_lost is not None and lost >= last_lost:
            stable += 1
            if stable >= 3:
                break
        else:
            stable = 0
        last_lost = lost
        time.sleep(poll_interval)

    raw = run_validate(run_dir)
    totals = raw.get("totals") or {}
    missing = totals.get("lost", 0)
    unexpected = totals.get("delivered_but_unconfirmed", 0)
    duplicates = totals.get("duplicates", 0)
    verdict = raw.get("verdict")

    return {
        "run_dir": str(run_dir),
        "verdict": verdict,
        "publisher_confirmed": totals.get("confirmed", 0),
        "consumer_acked": totals.get("consumed_unique", 0),
        "missing_count": missing,
        "missing_sample": _samples(raw, "lost_sample"),
        "unexpected_acked_count": unexpected,
        "duplicate_consumed_count": duplicates,
        "reordered": totals.get("reordered", 0),
        "nack_or_failure_events": totals.get("nack_or_failure_events", 0),
        "producer_reconnects": raw.get("producer_reconnects", 0),
        "consumer_reconnects": raw.get("consumer_reconnects", 0),
        "integrity_ok": verdict == "PASS" and unexpected == 0,
        "note": (
            "Delegates to clients/validate.py. missing_count = confirmed-but-not-acked "
            "at the final poll. During quorum failure, in-flight messages are expected "
            "to appear here until they drain; use --wait-seconds to let the cluster "
            "settle before calling them lost. duplicates are reported but expected "
            "on redelivery and do not fail integrity_ok."
        ),
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("run_dir", type=Path)
    p.add_argument("--wait-seconds", type=float, default=0.0,
                   help="keep re-checking while 'lost' is still shrinking")
    p.add_argument("--poll-interval", type=float, default=2.0)
    p.add_argument("--out", type=Path, default=None)
    args = p.parse_args()
    result = verify(args.run_dir, wait_seconds=args.wait_seconds,
                    poll_interval=args.poll_interval)
    out = args.out or (args.run_dir / "verification.json")
    out.write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))
    sys.exit(0 if result["integrity_ok"] else 1)


if __name__ == "__main__":
    main()