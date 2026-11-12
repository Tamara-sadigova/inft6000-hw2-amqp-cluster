# INFT 6000 – Homework 2: Distributed AMQP Messaging Cluster

A three-node **RabbitMQ 3.13** cluster with **Quorum Queues** (Raft), monitored with **Prometheus** and **Grafana**, plus multi-threaded **Python (pika)** producer and consumer clients with publisher confirms, manual acknowledgments, retry, failover and a validator for loss, duplicates and ordering.

Course: INFT 6000 Principles of Distributed Systems, ADA University

Team: Ravan Aliyev, Ravan Khidirov, Tamara Sadigova, Fatima Salimzada

## Architecture in short

- 3 brokers: `rabbit1`, `rabbit2`, `rabbit3` (Docker network `hw2-net`)
- Queue `hw2.orders`: durable quorum queue, replicated on all 3 nodes (a message is confirmed after 2 of 3 nodes wrote it)
- Partition policy: `pause_minority`
- Prometheus scrapes every broker every 5 s; Grafana dashboards are provisioned automatically
- Clients run on the host and fail over between the three AMQP endpoints (no load balancer)

The full design, C4 diagrams (`docs/c4/`) and test results are in the technical report (`report/`).

## Prerequisites

- Git
- Docker Desktop (or Docker Engine + Compose) with at least 4 GB of memory for Docker
- Python 3 (for the clients)
- Bash and curl (on Windows use Git Bash or WSL) for `scripts/init-queue.sh`

## Quick start

```bash
git clone https://github.com/Tamara-sadigova/inft6000-hw2-amqp-cluster.git
cd inft6000-hw2-amqp-cluster

cd infra
docker compose up -d        # 3 brokers, Prometheus, Grafana
docker compose ps           # wait until the three rabbit containers are "healthy"
cd ..
./scripts/init-queue.sh     # creates the quorum queue hw2.orders
```

Check the cluster:

```bash
docker exec rabbit1 rabbitmqctl cluster_status
docker exec rabbit1 rabbitmq-queues quorum_status hw2.orders
```

## Endpoints and credentials

| Node    | AMQP            | Management UI            | Prometheus metrics       |
|---------|-----------------|--------------------------|--------------------------|
| rabbit1 | localhost:5672  | http://localhost:15672   | http://localhost:15692   |
| rabbit2 | localhost:5673  | http://localhost:15673   | http://localhost:15693   |
| rabbit3 | localhost:5674  | http://localhost:15674   | http://localhost:15694   |

| Service    | URL                    | Login          |
|------------|------------------------|----------------|
| RabbitMQ   | Management UI above    | `hw2` / `hw2pass`, vhost `/` |
| Prometheus | http://localhost:9090  | –              |
| Grafana    | http://localhost:3000  | `admin` / `admin` |

These are lab credentials only. See `docs/contract.md` for the full shared contract (queue name, message and log format).

## Running the clients

```bash
cd clients
python -m venv .venv
# Windows: .venv\Scripts\activate        Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
```

Start the consumer first, then the producer (two terminals):

```bash
# terminal 1: stops after 10 s without messages
python consumer.py --threads 1 --idle-timeout 10

# terminal 2: 4 threads, 5000 messages per thread, 100-byte payload
python producer.py --threads 4 --count 5000 --size 100
```

Validate the run (loss, duplicates, ordering, latency):

```bash
python validate.py --logs logs --md-out logs/validation.md
```

`validate.py` prints `PASS` when no confirmed message is lost. Logs and summaries are written to `logs/` (ignored by Git).

Main options:

| Client   | Option           | Default  | Meaning |
|----------|------------------|----------|---------|
| producer | `--threads`      | 1        | publisher threads |
| producer | `--count`        | 1000     | messages per thread (0 = no limit) |
| producer | `--duration`     | 0        | run time in seconds (0 = no limit) |
| producer | `--rate`         | 0        | max messages/s per thread (0 = unlimited) |
| producer | `--size`         | 100      | payload size in bytes |
| consumer | `--threads`      | 1        | consumer threads (use 1 to check ordering) |
| consumer | `--prefetch`     | 50       | max unacknowledged messages per thread |
| consumer | `--idle-timeout` | 0        | stop after N s without messages |

## Benchmarks and chaos tests

Scripts are in `benchmarks/` and `chaos/`; results are described in the report.

<!-- Team C: list the script names and one example command for each here. -->

## Repository structure

```
.
├── README.md
├── docs/
│   ├── contract.md          shared contract (queue, ports, message and log format)
│   └── c4/                  C4 diagrams (Mermaid sources and PNG exports)
├── infra/                   docker-compose.yml, rabbitmq.conf, enabled_plugins,
│   │                        rabbit.env, Prometheus and Grafana configuration
│   ├── prometheus/
│   └── grafana/             provisioning and dashboards
├── scripts/
│   └── init-queue.sh        creates the quorum queue hw2.orders
├── clients/                 producer.py, consumer.py, validate.py
├── chaos/                   node-crash and partition tests
├── benchmarks/              load tests and results
└── report/                  technical report
```

## Stopping and resetting

```bash
cd infra
docker compose down        # stop, keep data
docker compose down -v     # stop and delete all volumes (clean cluster)
```

After `down -v` run `./scripts/init-queue.sh` again to recreate the queue.
