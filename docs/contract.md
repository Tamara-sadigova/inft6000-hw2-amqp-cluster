# Shared Contract (FROZEN: do not change without telling everyone)

## Cluster
- Broker: RabbitMQ 3.13, 3 nodes, Quorum Queues (Raft), replication factor 3
- Containers/hostnames: `rabbit1`, `rabbit2`, `rabbit3`
- Docker network name: `hw2-net`
- Partition policy: `pause_minority`

## Connection details (from the host machine, i.e. where clients run)
| Node    | AMQP (host) | Management UI (host) | Prometheus metrics (host) |
|---------|-------------|----------------------|---------------------------|
| rabbit1 | localhost:5672 | localhost:15672 | localhost:15692 |
| rabbit2 | localhost:5673 | localhost:15673 | localhost:15693 |
| rabbit3 | localhost:5674 | localhost:15674 | localhost:15694 |

Inside the Docker network every node uses AMQP 5672, management 15672, metrics 15692,
epmd 4369, inter-node 25672.

- Prometheus: localhost:9090
- Grafana: localhost:3000 (admin / admin)
- Username: `hw2`  Password: `hw2pass`  Vhost: `/`

## Queue
- Name: `hw2.orders`, durable quorum queue (created by `scripts/init-queue.sh`)
- Clients must NOT redeclare it with different arguments. Publish via the default exchange with
  routing key `hw2.orders`.

## Client behavior (B implements)
- Clients know all 3 AMQP addresses above and fail over between them. No load balancer.
- Publisher confirms ON. Consumer manual acks.

## Message format (JSON)
`{"producer_id": "p1", "seq": 123, "ts_sent": 1731234567.123, "payload": "..."}`
- `seq` starts at 1 and increments by 1 per producer. Used to detect loss, duplicates, reordering.

## Log format (JSON lines, one per event)
`{"ts": 1731234567.2, "event": "publish_sent|publish_confirmed|publish_nack|consume|ack|reconnect", "producer_id": "p1", "seq": 123}`
- Logs go to files under `logs/` (gitignored) so C can diff confirmed vs consumed.

## Repo ownership
- infra/ = A, clients/ = B, chaos/ + benchmarks/ = C, docs/ + report/ = D
- Branches: infra/A, clients/B, chaos/C, docs/D. Merge to main via Pull Request.