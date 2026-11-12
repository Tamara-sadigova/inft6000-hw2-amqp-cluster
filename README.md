# inft6000-hw2-amqp-cluster

Distributed AMQP messaging cluster for **INFT 6000: Principles of Distributed Systems, Homework 2** (ADA University).

A three-node **RabbitMQ 3.13** cluster with **Quorum Queues** (Raft), monitored with **Prometheus and Grafana**, tested with multi-threaded **Python (pika)** clients that use publisher confirms, manual acknowledgments, retry and connection failover. Chaos tests (node crash and network partition) check that the cluster elects a new leader automatically and that no confirmed message is lost.

The full technical report is in [`report/`](report/).

## Architecture

The C4 diagrams (System Context, Container, Component, Deployment) are in [`docs/c4/`](docs/c4/) as Mermaid sources (`.mmd`) and PNG exports. The shared contract (queue, ports, message and log format) is in [`docs/contract.md`](docs/contract.md). The design decisions are explained in Section 2 of the report.

## Repository structure

```
.
├── README.md
├── docs/
│   ├── contract.md        shared contract
│   └── c4/                C4 diagrams (.mmd and .png)
├── infra/                 docker-compose.yml, rabbitmq.conf, Prometheus and Grafana config
├── scripts/
│   └── init-queue.sh      creates the quorum queue hw2.orders
├── clients/               producer.py, consumer.py, validate.py
├── chaos/                 node_kill.py, network_partition.py, health.py
├── benchmarks/            load_test.py, verify_zero_loss.py, analyze.py, plot.py, results/
└── report/                technical report
```

## Quick start

**Prerequisites:** Git, Docker Desktop (WSL 2 backend on Windows) with at least 4 GB of memory for Docker, and Python 3. On Windows, run the shell script in Git Bash or WSL.

### 1. Start the cluster

```bash
cd infra
docker compose up -d        # starts 3 brokers, Prometheus and Grafana
docker compose ps           # wait until the three rabbit containers are "healthy"
cd ..
./scripts/init-queue.sh     # creates the quorum queue hw2.orders
```

| Service | URL | Login |
|---|---|---|
| RabbitMQ Management (rabbit1 / rabbit2 / rabbit3) | http://localhost:15672 / 15673 / 15674 | `hw2` / `hw2pass` |
| Prometheus | http://localhost:9090 | none |
| Grafana | http://localhost:3000 | `admin` / `admin` |

AMQP endpoints: `localhost:5672`, `localhost:5673`, `localhost:5674`.

Check the cluster:

```bash
docker exec rabbit1 rabbitmqctl cluster_status
docker exec rabbit1 rabbitmq-queues quorum_status hw2.orders
```

### 2. Run the clients

```bash
cd clients
python -m venv .venv
# Windows: .venv\Scripts\activate    Linux and macOS: source .venv/bin/activate
pip install -r requirements.txt

# terminal 1: consumer, stops after 10 s without messages
python consumer.py --threads 1 --idle-timeout 10

# terminal 2: producer, 4 threads, 5000 messages per thread, 100-byte payload
python producer.py --threads 4 --count 5000 --size 100

# after both have stopped: check for lost, duplicate and out-of-order messages
python validate.py --logs logs --md-out logs/validation.md
```

The validator prints `PASS` when no confirmed message is lost. All client options are described in the report (Section 4.8).

### 3. Run the tests

From the repository root:

```bash
pip install -r benchmarks/requirements.txt

python benchmarks/load_test.py --threads 4 --total-count 20000        # load test
python chaos/node_kill.py --node rabbit3                              # node crash during load
python chaos/network_partition.py --node rabbit3 --partition-seconds 10   # partition during load
python benchmarks/verify_zero_loss.py benchmarks/results/metrics/<run-directory>
```

Each run writes its logs, metrics and verification result to a new directory under `benchmarks/results/metrics/`. Graphs are written to `benchmarks/results/graphs/`.

### 4. Stop and reset

```bash
cd infra
docker compose down         # stops the stack, keeps the data
docker compose down -v      # also deletes all volumes (clean cluster)
```

After `down -v`, recreate the queue with `./scripts/init-queue.sh`.

## Known limitations

- All containers run on one Docker host, so a crash is simulated by stopping a container and a partition by disconnecting a container from `hw2-net`.
- Delivery is at-least-once: duplicates are possible after a failover and are detected, not removed.
- Prometheus and Grafana use the `latest` image tag.
- Credentials (RabbitMQ user, Erlang cookie, Grafana admin) are stored in the repository for a reproducible lab setup. This is not suitable for production.

## Team

| Member | Area |
|---|---|
| Tamara Sadigova | Infrastructure (`infra/`, `scripts/`) |
| Ravan Khidirov | Clients (`clients/`) |
| Ravan Aliyev | Chaos and benchmarks (`chaos/`, `benchmarks/`) |
| Fatima Salimzada | Documentation and integration (`docs/`, `report/`) |
