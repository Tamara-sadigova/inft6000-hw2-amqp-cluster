from __future__ import annotations

import json
import math
import os
import re
import secrets
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CLIENTS_DIR = REPO_ROOT / "clients"
BENCH_DIR = REPO_ROOT / "benchmarks"
# HW2_RESULTS_DIR lets test runs write somewhere other than the repo.
RESULTS_DIR = Path(os.getenv("HW2_RESULTS_DIR") or (BENCH_DIR / "results"))

NODES = os.getenv("HW2_NODES", "localhost:5672,localhost:5673,localhost:5674")
USER = os.getenv("HW2_USER", "hw2")
PASSWORD = os.getenv("HW2_PASSWORD", "hw2pass")
VHOST = os.getenv("HW2_VHOST", "/")
QUEUE = "hw2.orders"
CONTAINERS = ["rabbit1", "rabbit2", "rabbit3"]
NETWORK = "hw2-net"
META_FILE = "run_meta.json"


# --------------------------------------------------------------------------- run dirs

def new_run_dir(kind: str) -> Path:
    ts = time.strftime("%Y%m%d-%H%M%S")
    d = RESULTS_DIR / "metrics" / f"{kind}-{ts}"
    (d / "logs").mkdir(parents=True, exist_ok=True)
    return d


def new_run_tag() -> str:
    """Short unique tag put into every producer_id of a run.

    Producer ids used to be a fixed 'p1', 'p2', ... with seq restarting at 1,
    so a message left in the queue by an earlier run was indistinguishable
    from a message of the current run. With a per-run tag, leftovers show up
    as 'foreign' messages instead of masking a loss.
    """
    return f"r{time.strftime('%H%M%S')}{secrets.token_hex(2)}"


def write_meta(run_dir: Path, meta: dict) -> Path:
    out = run_dir / META_FILE
    out.write_text(json.dumps(meta, indent=2, default=str))
    return out


def read_meta(run_dir: Path) -> dict | None:
    p = run_dir / META_FILE
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except json.JSONDecodeError:
        return None


def split_count(total: int, parts: int) -> list[int]:
    """Split total into `parts` positive counts that sum exactly to total."""
    base, rem = divmod(total, parts)
    return [base + (1 if i < rem else 0) for i in range(parts)]


# --------------------------------------------------------------------------- client processes

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
    def __init__(self, name: str, role: str, proc: subprocess.Popen):
        self.name = name
        self.role = role
        self.proc = proc
        self.timed_out = False
        self.stop_method: str | None = None

    def alive(self) -> bool:
        return self.proc.poll() is None

    def wait(self, timeout: float | None = None) -> int | None:
        """Return the exit code, or None if still running after timeout."""
        try:
            return self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.timed_out = True
            return None

    def stop(self, grace: float = 15.0) -> None:
        """Stop the client so that it still flushes its log and summary.

        Both clients set their stop event on SIGINT and then close the event
        log and write the -summary.json file. The producer has no SIGTERM
        handler, so SIGTERM/SIGKILL are only a last resort and mark the run
        as forced (its log may be incomplete).
        """
        if not self.alive():
            return
        self.stop_method = "sigint"
        try:
            self.proc.send_signal(signal.SIGINT)
            self.proc.wait(timeout=grace)
            return
        except subprocess.TimeoutExpired:
            pass
        except ProcessLookupError:
            return
        self.stop_method = "sigterm"
        try:
            self.proc.terminate()
            self.proc.wait(timeout=5)
            return
        except subprocess.TimeoutExpired:
            pass
        except ProcessLookupError:
            return
        self.stop_method = "sigkill"
        self.proc.kill()
        self.proc.wait()

    def info(self) -> dict:
        return {
            "name": self.name,
            "role": self.role,
            "pid": self.proc.pid,
            "exit_code": self.proc.poll(),
            "timed_out": self.timed_out,
            "stop_method": self.stop_method,
            "forced": self.stop_method in ("sigterm", "sigkill"),
        }


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
    return Proc(producer_id, "producer", proc)


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
    return Proc(consumer_id, "consumer", proc)


class Workload:
    """Producer + consumer processes with a controlled lifecycle.

    Lifecycle: start consumers -> start producers -> wait for producers
    (bounded) -> wait until the broker queue is empty -> stop consumers
    with SIGINT. The harness, not the consumer idle timeout, decides when
    consumers stop, so a long outage cannot make them quit before the
    queue has drained.
    """

    def __init__(self, run_dir: Path, *, run_tag: str, total_count: int, producers: int,
                 rate: float, size: int, consumers: int, prefetch: int, work_ms: float,
                 consumer_idle: float = 0):
        if producers < 1 or consumers < 1:
            raise ValueError("need at least one producer and one consumer")
        if total_count < producers:
            raise ValueError("--total-count must be >= number of producers "
                             "(a producer count of 0 would mean 'unlimited')")
        self.run_dir = run_dir
        self.run_tag = run_tag
        self.counts = split_count(total_count, producers)
        self.producer_ids = [f"{run_tag}-p{i+1}" for i in range(producers)]
        self.consumer_ids = [f"{run_tag}-c{i+1}" for i in range(consumers)]
        self.rate = rate
        self.size = size
        self.prefetch = prefetch
        self.work_ms = work_ms
        self.consumer_idle = consumer_idle
        self.producers: list[Proc] = []
        self.consumers: list[Proc] = []
        self.started_at: float | None = None
        self.drain: dict | None = None

    def expected_publish_seconds(self) -> float:
        if self.rate > 0:
            return max(self.counts) / self.rate
        return max(self.counts) / 200.0  # conservative guess for an unthrottled producer

    def start(self) -> None:
        self.started_at = time.time()
        self.consumers = [
            start_consumer(self.run_dir, consumer_id=cid, threads=1, prefetch=self.prefetch,
                           work_ms=self.work_ms, idle_timeout=self.consumer_idle)
            for cid in self.consumer_ids
        ]
        time.sleep(0.5)
        self.producers = [
            start_producer(self.run_dir, producer_id=pid, threads=1, count=n,
                           rate=self.rate, size=self.size)
            for pid, n in zip(self.producer_ids, self.counts)
        ]

    def wait_producers(self, timeout: float) -> bool:
        """Wait for all producers against ONE overall deadline. Returns True if all exited."""
        deadline = time.time() + timeout
        ok = True
        for p in self.producers:
            if p.wait(timeout=max(0.0, deadline - time.time())) is None:
                ok = False
        for p in self.producers:
            if p.alive():
                p.stop()
        return ok

    def wait_drained(self, timeout: float, observer: str = "rabbit1", poll: float = 1.0,
                     stable_s: float = 6.0) -> dict:
        """Wait until the broker reports 0 messages (ready + unacked) for `stable_s` seconds.

        Call this only after all producers have exited: from then on the
        depth can only go down. The count from rabbitmqctl is refreshed
        periodically (collect_statistics_interval, 5 s by default), so a
        single '0' may be stale; a 0 that persists for longer than one
        refresh interval reflects a snapshot taken after publishing stopped.
        (The client logs cannot be used for this: they are only flushed when
        a later event is written or the client exits.)
        """
        t0 = time.time()
        zero_since = None
        drained = False
        last = None
        while time.time() - t0 < timeout:
            q = queue_info(observer)
            last = q
            if q.get("ok") and q.get("messages") == 0:
                zero_since = zero_since or q["ts"]
                if q["ts"] - zero_since >= stable_s:
                    drained = True
                    break
            else:
                zero_since = None
            if not any(c.alive() for c in self.consumers):
                break
            time.sleep(poll)
        self.drain = {
            "drained": drained,
            "waited_s": round(time.time() - t0, 3),
            "stable_zero_required_s": stable_s,
            "last_queue": last,
            "consumers_alive": sum(c.alive() for c in self.consumers),
        }
        return self.drain

    def stop_consumers(self) -> None:
        for c in self.consumers:
            c.stop()

    def stop_all(self) -> None:
        for p in [*self.producers, *self.consumers]:
            try:
                p.stop()
            except Exception:
                pass

    def info(self) -> dict:
        return {
            "run_tag": self.run_tag,
            "producers": {p: n for p, n in zip(self.producer_ids, self.counts)},
            "consumers": self.consumer_ids,
            "rate_per_producer": self.rate,
            "payload_bytes": self.size,
            "prefetch": self.prefetch,
            "work_ms": self.work_ms,
            "consumer_idle_timeout": self.consumer_idle,
            "started_at": self.started_at,
            "drain": self.drain,
            "processes": [p.info() for p in [*self.producers, *self.consumers]],
        }


# --------------------------------------------------------------------------- docker / rabbitmqctl

class DockerError(RuntimeError):
    pass


def run_docker(*args: str, check: bool = True, timeout: float = 30) -> subprocess.CompletedProcess:
    """Run a docker command. A timeout is reported as returncode 124 (or raised if check)."""
    try:
        proc = subprocess.run(
            ["docker", *args], capture_output=True, text=True, timeout=timeout
        )
    except subprocess.TimeoutExpired as exc:
        if check:
            raise DockerError(f"docker {' '.join(args)} timed out after {timeout}s") from exc
        return subprocess.CompletedProcess(["docker", *args], 124, "", f"timeout after {timeout}s")
    if check and proc.returncode != 0:
        raise DockerError(f"docker {' '.join(args)} failed ({proc.returncode}): "
                          f"{(proc.stderr or proc.stdout).strip()}")
    return proc


def _exec_json(container: str, *cmd: str, timeout: float = 15) -> dict:
    proc = run_docker("exec", container, *cmd, "--formatter", "json", check=False, timeout=timeout)
    if proc.returncode != 0:
        return {"ok": False, "error": (proc.stderr or proc.stdout).strip()[:500]}
    try:
        return {"ok": True, "data": json.loads(proc.stdout)}
    except json.JSONDecodeError:
        return {"ok": False, "error": "non-JSON output", "raw": proc.stdout[:2000]}


def cluster_status(container: str = "rabbit1", timeout: float = 15) -> dict:
    return _exec_json(container, "rabbitmqctl", "cluster_status", timeout=timeout)


def queue_stats(container: str = "rabbit1", timeout: float = 15) -> dict:
    return _exec_json(container, "rabbitmqctl", "list_queues", "name", "messages",
                      "messages_ready", "messages_unacknowledged", timeout=timeout)


def short_node(name: str | None) -> str | None:
    """'rabbit@rabbit2' -> 'rabbit2' (container name == hostname in docker-compose.yml)."""
    if not name:
        return None
    return name.split("@", 1)[1] if "@" in name else name


def queue_info(container: str = "rabbit1", timeout: float = 15) -> dict:
    """Depth, leader and members of hw2.orders as seen from `container`."""
    r = _exec_json(container, "rabbitmqctl", "list_queues", "name", "leader", "members",
                   "messages", "messages_ready", "messages_unacknowledged", timeout=timeout)
    out = {"ts": time.time(), "observer": container, "ok": False}
    if not r["ok"]:
        out["error"] = r.get("error")
        return out
    for q in r["data"]:
        if q.get("name") == QUEUE:
            out.update(ok=True, messages=q.get("messages"), ready=q.get("messages_ready"),
                       unacked=q.get("messages_unacknowledged"),
                       leader=short_node(q.get("leader")),
                       members=sorted(short_node(m) for m in q.get("members") or []))
            return out
    out["error"] = f"queue {QUEUE} not found"
    return out


def quorum_status(container: str = "rabbit1", timeout: float = 15) -> dict:
    """Raft view of hw2.orders: leader, highest term and per-member state."""
    r = _exec_json(container, "rabbitmq-queues", "quorum_status", QUEUE, timeout=timeout)
    out = {"ts": time.time(), "observer": container, "ok": False}
    if not r["ok"]:
        out["error"] = r.get("error")
        return out
    members = []
    for m in r["data"]:
        members.append({
            "node": short_node(m.get("Node Name")),
            "state": m.get("Raft State"),
            "membership": m.get("Membership"),
            "term": m.get("Term"),
            "commit_index": m.get("Commit Index"),
        })
    leaders = [m["node"] for m in members if m["state"] == "leader"]
    terms = [m["term"] for m in members if isinstance(m["term"], int)]
    out.update(ok=True, members=members,
               leader=leaders[0] if len(leaders) == 1 else None,
               leaders=leaders,
               term=max(terms) if terms else None)
    return out


def observer_for(target: str | None) -> str:
    """A node to run rabbitmqctl on that is not the one under test."""
    for c in CONTAINERS:
        if c != target:
            return c
    return CONTAINERS[0]


def container_running(container: str) -> bool | None:
    proc = run_docker("inspect", container, "--format", "{{.State.Running}}", check=False, timeout=15)
    if proc.returncode != 0:
        return None
    return proc.stdout.strip() == "true"


def leader_snapshot(container: str) -> dict:
    """Compact leader/term record for run metadata."""
    qs = quorum_status(container)
    if not qs.get("ok"):
        return {"ts": qs["ts"], "observer": container, "ok": False, "error": qs.get("error")}
    return {"ts": qs["ts"], "observer": container, "ok": True, "leader": qs["leader"],
            "term": qs["term"],
            "members": {m["node"]: {"state": m["state"], "term": m["term"]} for m in qs["members"]}}


def network_attached(container: str, network: str = NETWORK) -> bool | None:
    proc = run_docker("inspect", container, "--format", "{{json .NetworkSettings.Networks}}",
                      check=False, timeout=15)
    if proc.returncode != 0:
        return None
    try:
        return network in (json.loads(proc.stdout) or {})
    except json.JSONDecodeError:
        return None


def network_aliases(container: str, network: str = NETWORK) -> list[str]:
    proc = run_docker("inspect", container, "--format", "{{json .NetworkSettings.Networks}}",
                      check=False, timeout=15)
    try:
        nets = json.loads(proc.stdout) or {}
    except json.JSONDecodeError:
        return []
    aliases = (nets.get(network) or {}).get("Aliases") or []
    return sorted({a for a in aliases if a})


# --------------------------------------------------------------------------- broker logs

_ANSI = re.compile(r"\x1b\[[0-9;]*m")
_LOG_TS = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)([+-]\d{2}:\d{2})")


def broker_log(container: str, since: float, until: float | None = None,
               patterns: list[str] | None = None) -> list[dict]:
    """Broker log lines in [since, until] (unix seconds), optionally filtered by regex."""
    args = ["logs", "--since", f"{since:.3f}"]
    if until is not None:
        args += ["--until", f"{until:.3f}"]
    proc = run_docker(*args, container, check=False, timeout=30)
    text = _ANSI.sub("", (proc.stdout or "") + (proc.stderr or ""))
    rx = [re.compile(p) for p in (patterns or [])]
    out = []
    for line in text.splitlines():
        if rx and not any(r.search(line) for r in rx):
            continue
        m = _LOG_TS.match(line)
        ts = None
        if m:
            try:
                ts = datetime.fromisoformat(m.group(1) + m.group(2)).timestamp()
            except ValueError:
                ts = None
        out.append({"container": container, "ts": ts, "line": line.strip()[:300]})
    return out


def leader_elections(since: float, until: float | None = None) -> list[dict]:
    """'candidate -> leader' events for hw2.orders on any node, oldest first."""
    pat = rf"queue '{re.escape(QUEUE)}' .*candidate -> leader in term: (\d+)"
    events = []
    for c in CONTAINERS:
        for e in broker_log(c, since, until, [pat]):
            m = re.search(pat, e["line"])
            events.append({"node": c, "ts": e["ts"], "term": int(m.group(1)) if m else None})
    return sorted(events, key=lambda e: e["ts"] or 0)


# --------------------------------------------------------------------------- jsonl helpers

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


def auto_timeout(workload: "Workload", extra: float = 120.0) -> float:
    return math.ceil(workload.expected_publish_seconds() + extra)


def finalize_run(run_dir: Path, meta: dict, bucket_seconds: float = 1.0,
                 make_plots: bool = True) -> tuple[dict, dict, list[Path]]:
    """Write run_meta.json, then metrics.json, verification.json and graphs."""
    import analyze
    import plot as plotmod
    import verify_zero_loss

    write_meta(run_dir, meta)
    metrics = analyze.compute(run_dir, bucket_seconds=bucket_seconds)
    (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2))
    verification = verify_zero_loss.verify(run_dir)
    if meta.get("chaos") is not None:
        verification["chaos"] = meta["chaos"]
    if metrics.get("client_outage") is not None:
        verification["client_outage"] = metrics["client_outage"]
    (run_dir / "verification.json").write_text(json.dumps(verification, indent=2, default=str))
    graphs = []
    if make_plots:
        try:
            graphs = plotmod.plot_all(metrics, RESULTS_DIR / "graphs")
        except Exception as exc:  # plotting must never hide a verdict
            verification.setdefault("warnings", []).append(f"plotting failed: {exc!r}")
    return metrics, verification, graphs
