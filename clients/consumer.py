import argparse
import json
import os
import random
import signal
import sys
import threading
import time

import pika

QUEUE = "hw2.orders"
DEFAULT_NODES = "localhost:5672,localhost:5673,localhost:5674"
DEFAULT_USER = "hw2"
DEFAULT_PASSWORD = "hw2pass"
DEFAULT_VHOST = "/"

RECOVERABLE_ERRORS = (
    pika.exceptions.AMQPConnectionError,
    pika.exceptions.AMQPChannelError,
    pika.exceptions.StreamLostError,
    pika.exceptions.ConnectionClosedByBroker,
    pika.exceptions.ChannelClosedByBroker,
    ConnectionError,
    OSError,
)


class QueueMissingError(Exception):
    pass


class EventLog:
    def __init__(self, path):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.lock = threading.Lock()
        self.handle = open(path, "a", encoding="utf-8")
        self.last_flush = time.time()

    def write(self, event, producer_id=None, seq=None, **extra):
        record = {"ts": round(time.time(), 6), "event": event, "producer_id": producer_id, "seq": seq}
        record.update(extra)
        with self.lock:
            self.handle.write(json.dumps(record) + "\n")
            now = time.time()
            if now - self.last_flush >= 1.0:
                self.handle.flush()
                self.last_flush = now

    def close(self):
        with self.lock:
            self.handle.flush()
            self.handle.close()


class ConnectionManager:
    def __init__(self, nodes, user, password, vhost, max_retry_seconds):
        self.nodes = nodes
        self.credentials = pika.PlainCredentials(user, password)
        self.vhost = vhost
        self.max_retry_seconds = max_retry_seconds
        self.next_index = random.randrange(len(nodes))

    def connect(self, stop_event, on_attempt_failed=None):
        delay = 0.25
        started = time.time()
        last_error = None
        while not stop_event.is_set():
            for _ in range(len(self.nodes)):
                host, port = self.nodes[self.next_index]
                self.next_index = (self.next_index + 1) % len(self.nodes)
                parameters = pika.ConnectionParameters(
                    host=host,
                    port=port,
                    virtual_host=self.vhost,
                    credentials=self.credentials,
                    heartbeat=10,
                    blocked_connection_timeout=30,
                    socket_timeout=5,
                    connection_attempts=1,
                )
                try:
                    return pika.BlockingConnection(parameters), f"{host}:{port}"
                except RECOVERABLE_ERRORS as exc:
                    last_error = exc
                    if on_attempt_failed:
                        on_attempt_failed(f"{host}:{port}", exc)
            if time.time() - started > self.max_retry_seconds:
                raise ConnectionError(f"no RabbitMQ node reachable for {self.max_retry_seconds}s: {last_error!r}")
            time.sleep(delay + random.uniform(0, delay / 2))
            delay = min(delay * 2, 5.0)
        raise ConnectionError("stopped while connecting")


class Activity:
    def __init__(self):
        self.lock = threading.Lock()
        self.last_message = None

    def touch(self):
        with self.lock:
            self.last_message = time.time()

    def idle_for(self):
        with self.lock:
            return None if self.last_message is None else time.time() - self.last_message


class ConsumerThread(threading.Thread):
    def __init__(self, consumer_id, args, manager, event_log, stop_event, activity):
        super().__init__(name=consumer_id, daemon=True)
        self.consumer_id = consumer_id
        self.args = args
        self.manager = manager
        self.log = event_log
        self.stop_event = stop_event
        self.activity = activity
        self.connection = None
        self.channel = None
        self.node = None
        self.consumed = 0
        self.acked = 0
        self.redelivered = 0
        self.rejected = 0
        self.reconnects = 0
        self.latencies_ms = []
        self.error = None

    def close_connection(self):
        try:
            if self.connection is not None and self.connection.is_open:
                self.connection.close()
        except Exception:
            pass
        self.connection = None
        self.channel = None

    def connect(self, reason):
        self.close_connection()

        def failed(node, exc):
            self.log.write("reconnect", consumer_id=self.consumer_id, node=node, status="failed", error=repr(exc))

        self.connection, self.node = self.manager.connect(self.stop_event, failed)
        self.channel = self.connection.channel()
        try:
            self.channel.queue_declare(queue=QUEUE, passive=True)
        except pika.exceptions.ChannelClosedByBroker as exc:
            if exc.reply_code == 404:
                raise QueueMissingError(f"queue {QUEUE} does not exist, run scripts/init-queue.sh first") from exc
            raise
        self.channel.basic_qos(prefetch_count=self.args.prefetch)
        self.log.write("reconnect", consumer_id=self.consumer_id, node=self.node, status="connected", reason=reason)

    def handle(self, method, body):
        received = time.time()
        try:
            message = json.loads(body)
            producer_id = message["producer_id"]
            seq = int(message["seq"])
            ts_sent = float(message["ts_sent"])
        except (ValueError, KeyError, TypeError):
            self.channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            self.rejected += 1
            self.log.write("reject", consumer_id=self.consumer_id, node=self.node, reason="malformed message")
            return
        latency_ms = (received - ts_sent) * 1000.0
        self.consumed += 1
        if method.redelivered:
            self.redelivered += 1
        self.latencies_ms.append(latency_ms)
        self.log.write(
            "consume",
            producer_id,
            seq,
            consumer_id=self.consumer_id,
            node=self.node,
            redelivered=bool(method.redelivered),
            latency_ms=round(latency_ms, 3),
        )
        if self.args.work_ms > 0:
            time.sleep(self.args.work_ms / 1000.0)
        self.channel.basic_ack(delivery_tag=method.delivery_tag)
        self.acked += 1
        self.log.write("ack", producer_id, seq, consumer_id=self.consumer_id)
        self.activity.touch()

    def run(self):
        reason = "start"
        while not self.stop_event.is_set():
            try:
                self.connect(reason)
                for method, properties, body in self.channel.consume(QUEUE, auto_ack=False, inactivity_timeout=1):
                    if self.stop_event.is_set():
                        break
                    if method is None:
                        continue
                    self.handle(method, body)
                try:
                    self.channel.cancel()
                except Exception:
                    pass
            except QueueMissingError as exc:
                self.error = str(exc)
                self.stop_event.set()
                break
            except RECOVERABLE_ERRORS as exc:
                if self.stop_event.is_set():
                    break
                self.reconnects += 1
                reason = f"connection lost: {type(exc).__name__}"
                time.sleep(0.2)
                continue
            except Exception as exc:
                self.error = repr(exc)
                break
        self.close_connection()


def parse_nodes(text):
    nodes = []
    for item in text.split(","):
        item = item.strip()
        if item:
            host, _, port = item.partition(":")
            nodes.append((host, int(port or 5672)))
    if not nodes:
        raise ValueError("at least one node is required")
    return nodes


def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[int(round(pct / 100.0 * (len(ordered) - 1)))], 3)


def consumer_ids(base, threads):
    if threads == 1:
        return [base]
    return [f"{base}-t{i}" for i in range(1, threads + 1)]


def build_parser():
    parser = argparse.ArgumentParser(description="HW2 consumer: manual acks, prefetch and failover across the 3 RabbitMQ nodes")
    parser.add_argument("--consumer-id", default="c1")
    parser.add_argument("--threads", type=int, default=1, help="consumer threads, use 1 when checking strict ordering")
    parser.add_argument("--prefetch", type=int, default=50, help="max unacked messages per consumer thread")
    parser.add_argument("--work-ms", type=float, default=0, help="simulated processing time per message")
    parser.add_argument("--idle-timeout", type=float, default=0, help="stop after this many seconds without messages, 0 = never")
    parser.add_argument("--duration", type=float, default=0, help="stop after this many seconds, 0 = no limit")
    parser.add_argument("--nodes", default=os.getenv("HW2_NODES", DEFAULT_NODES))
    parser.add_argument("--user", default=os.getenv("HW2_USER", DEFAULT_USER))
    parser.add_argument("--password", default=os.getenv("HW2_PASSWORD", DEFAULT_PASSWORD))
    parser.add_argument("--vhost", default=os.getenv("HW2_VHOST", DEFAULT_VHOST))
    parser.add_argument("--max-retry-seconds", type=float, default=120)
    parser.add_argument("--log-dir", default="logs")
    return parser


def main():
    args = build_parser().parse_args()
    if args.threads < 1:
        raise SystemExit("--threads must be at least 1")

    run_name = f"consumer-{args.consumer_id}-{time.strftime('%Y%m%d-%H%M%S')}"
    log_path = os.path.join(args.log_dir, f"{run_name}.jsonl")
    event_log = EventLog(log_path)
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, lambda *_: stop_event.set())
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, lambda *_: stop_event.set())

    nodes = parse_nodes(args.nodes)
    activity = Activity()
    threads = [
        ConsumerThread(cid, args, ConnectionManager(nodes, args.user, args.password, args.vhost, args.max_retry_seconds), event_log, stop_event, activity)
        for cid in consumer_ids(args.consumer_id, args.threads)
    ]
    print(f"consumer started: {len(threads)} thread(s), nodes {args.nodes}, log {log_path}", file=sys.stderr)
    started = time.time()
    for thread in threads:
        thread.start()

    last_report = started
    while any(thread.is_alive() for thread in threads):
        time.sleep(0.2)
        now = time.time()
        if args.duration and now - started >= args.duration:
            stop_event.set()
        idle = activity.idle_for()
        if args.idle_timeout and idle is not None and idle >= args.idle_timeout:
            stop_event.set()
        if now - last_report >= 5:
            consumed = sum(thread.consumed for thread in threads)
            reconnects = sum(thread.reconnects for thread in threads)
            print(f"[{now - started:6.1f}s] consumed={consumed} reconnects={reconnects}", file=sys.stderr)
            last_report = now
        if stop_event.is_set():
            for thread in threads:
                thread.join(timeout=5)
            break
    elapsed = time.time() - started
    event_log.close()

    latencies = [value for thread in threads for value in thread.latencies_ms]
    consumed = sum(thread.consumed for thread in threads)
    summary = {
        "run": run_name,
        "threads": len(threads),
        "prefetch": args.prefetch,
        "duration_s": round(elapsed, 3),
        "consumed": consumed,
        "acked": sum(thread.acked for thread in threads),
        "redelivered": sum(thread.redelivered for thread in threads),
        "rejected_malformed": sum(thread.rejected for thread in threads),
        "reconnects": sum(thread.reconnects for thread in threads),
        "end_to_end_latency_ms": {
            "p50": percentile(latencies, 50),
            "p95": percentile(latencies, 95),
            "p99": percentile(latencies, 99),
            "max": round(max(latencies), 3) if latencies else None,
        },
        "errors": {thread.consumer_id: thread.error for thread in threads if thread.error},
        "event_log": log_path,
    }
    with open(os.path.join(args.log_dir, f"{run_name}-summary.json"), "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)
    print(json.dumps(summary, indent=2))
    if summary["errors"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
