from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CLIENTS_DIR = REPO_ROOT / "clients"
BENCH_DIR = REPO_ROOT / "benchmarks"
RESULTS_DIR = BENCH_DIR / "results"

NODES = os.getenv("HW2_NODES", "localhost:5672,localhost:5673,localhost:5674")
USER = os.getenv("HW2_USER", "hw2")
PASSWORD = os.getenv("HW2_PASSWORD", "hw2pass")
VHOST = os.getenv("HW2_VHOST", "/")
QUEUE = "hw2.orders"
CONTAINERS = ["rabbit1", "rabbit2", "rabbit3"]
NETWORK = "hw2-net"


def new_run_dir(kind: str) -> Path:
    ts = time.strftime("%Y%m%d-%H%M%S")
    d = RESULTS_DIR / "metrics" / f"{kind}-{ts}"
    (d / "logs").mkdir(parents=True, exist_ok=True)
    return d


def _client(script: str, *args: str) -> list[str]:
    return [sys.executable, str(CLIENTS_DIR / script), *args]


def _common_client_args(log_dir: Path) -> list[str]:
    return [
        "--nodes", NODES,
        "--user", USER,
        "--password", PASSWORD,
        "--vhost", VHOST,
        "--log-dir", str(log_dir),
    ]


class Proc:
    def __init__(self, name: str, proc: subprocess.Popen):
        self.name = name
        self.proc = proc

    def wait(self, timeout: float | None = None) -> int:
        try:
            return self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return -1

    def stop(self, timeout: float = 10.0) -> None:
        if self.proc.poll() is not None:
            return
        try:
            self.proc.terminate()
        except ProcessLookupError:
            return
        try:
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()


def start_producer(run_dir: Path, *, producer_id="p1", threads=1, count=0,
                   duration=0, rate=0, size=100) -> Proc:
    log_dir = run_dir / "logs"
    args = [
        "--producer-id", producer_id,
        "--threads", str(threads),
        "--count", str(count),
        "--duration", str(duration),
        "--rate", str(rate),
        "--size", str(size),
        *_common_client_args(log_dir),
    ]
    proc = subprocess.Popen(_client("producer.py", *args), cwd=str(REPO_ROOT))
    return Proc(producer_id, proc)


def start_consumer(run_dir: Path, *, consumer_id="c1", threads=1, prefetch=100,
                   work_ms=0, idle_timeout=0, duration=0) -> Proc:
    log_dir = run_dir / "logs"
    args = [
        "--consumer-id", consumer_id,
        "--threads", str(threads),
        "--prefetch", str(prefetch),
        "--work-ms", str(work_ms),
        "--idle-timeout", str(idle_timeout),
        "--duration", str(duration),
        *_common_client_args(log_dir),
    ]
    proc = subprocess.Popen(_client("consumer.py", *args), cwd=str(REPO_ROOT))
    return Proc(consumer_id, proc)


def run_docker(*args: str, check: bool = True, timeout: float = 30) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", *args], check=check, capture_output=True, text=True, timeout=timeout
    )


def cluster_status(container: str = "rabbit1", timeout: float = 15) -> dict:
    proc = run_docker(
        "exec", container, "rabbitmqctl", "cluster_status",
        "--formatter", "json", check=False, timeout=timeout,
    )
    if proc.returncode != 0:
        return {"ok": False, "error": (proc.stderr or proc.stdout).strip()}
    try:
        return {"ok": True, "data": json.loads(proc.stdout)}
    except json.JSONDecodeError:
        return {"ok": False, "error": "non-JSON output", "raw": proc.stdout[:2000]}


def queue_stats(container: str = "rabbit1", timeout: float = 15) -> dict:
    proc = run_docker(
        "exec", container, "rabbitmqctl", "list_queues",
        "--formatter", "json",
        "name", "messages", "messages_ready", "messages_unacknowledged",
        check=False, timeout=timeout,
    )
    if proc.returncode != 0:
        return {"ok": False, "error": (proc.stderr or proc.stdout).strip()}
    try:
        return {"ok": True, "data": json.loads(proc.stdout)}
    except json.JSONDecodeError:
        return {"ok": False, "error": "non-JSON output", "raw": proc.stdout[:2000]}


def read_jsonl(path: Path):
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def iter_log_files(run_dir: Path, kind: str | None = None):
    log_dir = run_dir / "logs"
    if not log_dir.exists():
        return
    for p in sorted(log_dir.glob("*.jsonl")):
        if kind is None or p.name.startswith(kind):
            yield p