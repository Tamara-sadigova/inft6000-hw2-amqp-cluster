#!/usr/bin/env bash
set -e
AUTH="hw2:hw2pass"
until curl -sf -u $AUTH http://localhost:15672/api/overview >/dev/null; do sleep 2; done
curl -sf -u $AUTH -X PUT -H 'content-type: application/json' \
  -d '{"durable":true,"auto_delete":false,"arguments":{"x-queue-type":"quorum","x-quorum-initial-group-size":3}}' \
  http://localhost:15672/api/queues/%2F/hw2.orders
echo "queue hw2.orders created"