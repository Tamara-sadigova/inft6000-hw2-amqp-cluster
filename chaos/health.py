"""Cluster health check via docker exec + rabbitmqctl."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
from run_utils import CONTAINERS, cluster_status, queue_stats


def _short_name(n: str) -> str:
    """Convert 'rabbit@rabbit1' or 'rabbit1' -> 'rabbit1'.

    rabbitmqctl returns node names as 'rabbit@<hostname>'; docker container
    names are '<hostname>'. Take the part after '@', not before it.
    """
    if "@" in n:
        return n.split("@", 1)[1]
    return n


def _node_names(status: dict) -> set[str]:
    if not status.get("ok"):
        return set()
    data = status["data"]
    nodes = []
    for key in ("running_nodes", "nodes"):
        val = data.get(key)
        if isinstance(val, list) and val:
            nodes = val
            break
    names = set()
    for n in nodes:
        if isinstance(n, str):
            names.add(_short_name(n))
        elif isinstance(n, dict):
            name = n.get("name") or n.get("node")
            if name:
                names.add(_short_name(name))
    return names


def _partitions(status: dict) -> list:
    if not status.get("ok"):
        return []
    return status["data"].get("partitions") or []


def cluster_healthy(status: dict) -> bool:
    if not status.get("ok"):
        return False
    if _partitions(status):
        return False
    return set(CONTAINERS).issubset(_node_names(status))


def check(container: str = "rabbit1") -> dict:
    status = cluster_status(container)
    queue = queue_stats(container)
    return {
        "container": container,
        "cluster_status": status,
        "queue": queue,
        "healthy": cluster_healthy(status),
        "running_nodes": sorted(_node_names(status)),
        "partitions": _partitions(status),
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--container", default="rabbit1")
    p.add_argument("--json", action="store_true")
    args = p.parse_args()
    out = check(args.container)
    if args.json:
        print(json.dumps(out, indent=2))
    else:
        print(f"healthy={out['healthy']}  running_nodes={out['running_nodes']}  "
              f"partitions={out['partitions']}")
        print(json.dumps(out, indent=2))
    sys.exit(0 if out["healthy"] else 1)


if __name__ == "__main__":
    main()