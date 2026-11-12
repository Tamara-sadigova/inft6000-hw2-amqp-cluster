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
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        self.path = path
        self.lock = threading.Lock()
        self.handle = open(path, "a", encoding="utf-8")
        self.last_flush = time.time()

    def write(self, event, producer_id, seq=None, **extra):
        record = {"ts": round(time.time(), 6), "event": event, "producer_id": producer_id, "seq": seq}
        record.update(extra)
        line = json.dumps(record)
        with self.lock:
            self.handle.write(line + "\n")
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

    def parameters(self, host, port):
        return pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=self.vhost,
            credentials=self.credentials,
            heartbeat=10,
            blocked_connection_timeout=30,
            socket_timeout=5,
            connection_attempts=1,
        )

    def connect(self, stop_event, on_attempt_failed=None):
        delay = 0.25
        started = time.time()
        last_error = None
        while not stop_event.is_set():
            for _ in range(len(self.nodes)):
                host, port = self.nodes[self.next_index]
                self.next_index = (self.next_index + 1) % len(self.nodes)
                try:
                    connection = pika.BlockingConnection(self.parameters(host, port))
                    return connection, f"{host}:{port}"
                except RECOVERABLE_ERRORS as exc:
                    last_error = exc
                    if on_attempt_failed:
                        on_attempt_failed(f"{host}:{port}", exc)
            if time.time() - started > self.max_retry_seconds:
                raise ConnectionError(f"no RabbitMQ node reachable for {self.max_retry_seconds}s: {last_error!r}")
            time.sleep(delay + random.uniform(0, delay / 2))
            delay = min(delay * 2, 5.0)
        raise ConnectionError("stopped while connecting")


class PublisherThread(threading.Thread):
    def __init__(self, producer_id, args, manager, event_log, stop_event, deadline):
        super().__init__(name=producer_id, daemon=True)
        self.producer_id = producer_id
        self.args = args
        self.manager = manager
        self.log = event_log
        self.stop_event = stop_event
        self.deadline = deadline
        self.connection = None
        self.channel = None
        self.node = None
        self.payload = "x" * max(0, args.size)
        self.sent = 0
        self.confirmed = 0
        self.nacks = 0
        self.reconnects = 0
        self.last_confirmed_seq = 0
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

    def open_channel(self):
        self.channel = self.connection.channel()
        self.channel.confirm_delivery()
        try:
            self.channel.queue_declare(queue=QUEUE, passive=True)
        except pika.exceptions.ChannelClosedByBroker as exc:
            if exc.reply_code == 404:
                raise QueueMissingError(f"queue {QUEUE} does not exist, run scripts/init-queue.sh first") from exc
            raise

    def connect(self, reason):
        self.close_connection()

        def failed(node, exc):
            self.log.write("reconnect", self.producer_id, self.last_confirmed_seq + 1, node=node, status="failed", error=repr(exc))

        self.connection, self.node = self.manager.connect(self.stop_event, failed)
        self.open_channel()
        self.log.write("reconnect", self.producer_id, self.last_confirmed_seq + 1, node=self.node, status="connected", reason=reason)

    def reopen_channel(self):
        if self.connection is None or not self.connection.is_open:
            self.reconnects += 1
            self.connect("connection closed after nack")
            return
        try:
            if self.channel is not None and self.channel.is_open:
                self.channel.close()
        except Exception:
            pass
        self.open_channel()

    def build_message(self, seq):
        body = json.dumps(
            {
                "producer_id": self.producer_id,
                "seq": seq,
                "ts_sent": time.time(),
                "payload": self.payload,
            }
        )
        properties = pika.BasicProperties(
            content_type="application/json",
            delivery_mode=pika.DeliveryMode.Persistent,
            message_id=f"{self.producer_id}:{seq}",
        )
        return body, properties

    def should_continue(self, seq):
        if self.stop_event.is_set():
            return False
        if self.args.count and seq > self.args.count:
            return False
        if self.deadline and time.time() >= self.deadline:
            return False
        return True

    def run(self):
        try:
            self.connect("start")
        except Exception as exc:
            self.error = repr(exc)
            return
        interval = 1.0 / self.args.rate if self.args.rate > 0 else 0.0
        next_send = time.perf_counter()
        seq = 1
        while self.should_continue(seq):
            if interval:
                wait = next_send - time.perf_counter()
                if wait > 0:
                    time.sleep(wait)
                next_send = max(next_send + interval, time.perf_counter() - interval)
            body, properties = self.build_message(seq)
            self.log.write("publish_sent", self.producer_id, seq, node=self.node)
            self.sent += 1
            started = time.perf_counter()
            try:
                self.channel.basic_publish(
                    exchange="",
                    routing_key=QUEUE,
                    body=body,
                    properties=properties,
                    mandatory=True,
                )
            except (pika.exceptions.NackError, pika.exceptions.UnroutableError) as exc:
                self.nacks += 1
                self.log.write("publish_nack", self.producer_id, seq, node=self.node, error=type(exc).__name__)
                time.sleep(0.1)
                try:
                    self.reopen_channel()
                except RECOVERABLE_ERRORS as reconnect_exc:
                    self.reconnects += 1
                    try:
                        self.connect(f"channel reopen failed: {reconnect_exc!r}")
                    except Exception as fatal:
                        self.error = repr(fatal)
                        break
                continue
            except RECOVERABLE_ERRORS as exc:
                self.reconnects += 1
                self.log.write("publish_nack", self.producer_id, seq, node=self.node, error=type(exc).__name__)
                try:
                    self.connect(f"connection lost: {type(exc).__name__}")
                except Exception as fatal:
                    self.error = repr(fatal)
                    break
                continue
            latency_ms = (time.perf_counter() - started) * 1000.0
            self.latencies_ms.append(latency_ms)
            self.confirmed += 1
            self.last_confirmed_seq = seq
            self.log.write("publish_confirmed", self.producer_id, seq, node=self.node, latency_ms=round(latency_ms, 3))
            seq += 1
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
    index = int(round(pct / 100.0 * (len(ordered) - 1)))
    return round(ordered[index], 3)


def producer_ids(base, threads):
    if threads == 1:
        return [base]
    return [f"{base}-t{i}" for i in range(1, threads + 1)]


def build_parser():
    parser = argparse.ArgumentParser(description="HW2 producer: publisher confirms, retry and failover across the 3 RabbitMQ nodes")
    parser.add_argument("--producer-id", default="p1", help="id written into every message (default p1)")
    parser.add_argument("--threads", type=int, default=1, help="publisher threads, each gets its own producer_id and seq")
    parser.add_argument("--count", type=int, default=1000, help="messages per thread, 0 = no limit")
    parser.add_argument("--duration", type=float, default=0, help="stop after this many seconds, 0 = no limit")
    parser.add_argument("--rate", type=float, default=0, help="max messages per second per thread, 0 = as fast as possible")
    parser.add_argument("--size", type=int, default=100, help="payload size in bytes")
    parser.add_argument("--nodes", default=os.getenv("HW2_NODES", DEFAULT_NODES))
    parser.add_argument("--user", default=os.getenv("HW2_USER", DEFAULT_USER))
    parser.add_argument("--password", default=os.getenv("HW2_PASSWORD", DEFAULT_PASSWORD))
    parser.add_argument("--vhost", default=os.getenv("HW2_VHOST", DEFAULT_VHOST))
    parser.add_argument("--max-retry-seconds", type=float, default=120, help="give up if no node is reachable for this long")
    parser.add_argument("--log-dir", default="logs")
    return parser


def main():
    args = build_parser().parse_args()
    if args.threads < 1:
        raise SystemExit("--threads must be at least 1")
    if args.count == 0 and args.duration == 0:
        print("running until Ctrl+C (no --count and no --duration)", file=sys.stderr)

    run_name = f"producer-{args.producer_id}-{time.strftime('%Y%m%d-%H%M%S')}"
    log_path = os.path.join(args.log_dir, f"{run_name}.jsonl")
    event_log = EventLog(log_path)
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, lambda *_: stop_event.set())

    nodes = parse_nodes(args.nodes)
    deadline = time.time() + args.duration if args.duration > 0 else None
    threads = [
        PublisherThread(pid, args, ConnectionManager(nodes, args.user, args.password, args.vhost, args.max_retry_seconds), event_log, stop_event, deadline)
        for pid in producer_ids(args.producer_id, args.threads)
    ]

    print(f"producer started: {len(threads)} thread(s), nodes {args.nodes}, log {log_path}", file=sys.stderr)
    started = time.time()
    for thread in threads:
        thread.start()

    last_report = started
    while any(thread.is_alive() for thread in threads):
        time.sleep(0.2)
        now = time.time()
        if now - last_report >= 5:
            confirmed = sum(thread.confirmed for thread in threads)
            reconnects = sum(thread.reconnects for thread in threads)
            print(f"[{now - started:6.1f}s] confirmed={confirmed} rate={confirmed / (now - started):.0f} msg/s reconnects={reconnects}", file=sys.stderr)
            last_report = now
    elapsed = time.time() - started
    event_log.close()

    latencies = [value for thread in threads for value in thread.latencies_ms]
    confirmed = sum(thread.confirmed for thread in threads)
    summary = {
        "run": run_name,
        "threads": len(threads),
        "payload_bytes": args.size,
        "duration_s": round(elapsed, 3),
        "sent_attempts": sum(thread.sent for thread in threads),
        "confirmed": confirmed,
        "nacks_or_failures": sum(thread.nacks for thread in threads),
        "reconnects": sum(thread.reconnects for thread in threads),
        "throughput_msg_s": round(confirmed / elapsed, 1) if elapsed > 0 else 0,
        "confirm_latency_ms": {
            "p50": percentile(latencies, 50),
            "p95": percentile(latencies, 95),
            "p99": percentile(latencies, 99),
            "max": round(max(latencies), 3) if latencies else None,
        },
        "last_confirmed_seq": {thread.producer_id: thread.last_confirmed_seq for thread in threads},
        "errors": {thread.producer_id: thread.error for thread in threads if thread.error},
        "event_log": log_path,
    }
    summary_path = os.path.join(args.log_dir, f"{run_name}-summary.json")
    with open(summary_path, "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)
    print(json.dumps(summary, indent=2))
    if summary["errors"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
