from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from run_utils import RESULTS_DIR


def plot_throughput(metrics: dict, out: Path) -> None:
    pub = metrics["publish"]["throughput_msg_s"] or 0
    con = metrics["consume"]["throughput_msg_s"] or 0
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.bar(["publish confirmed", "consume acked"], [pub, con], color=["#4c72b0", "#dd8452"])
    ax.set_ylabel("msg/s")
    ax.set_title("Throughput")
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)


def plot_latency_distribution(metrics: dict, out: Path) -> None:
    lat = metrics["consume"]["end_to_end_latency_ms"]
    labels = ["P50", "P95", "P99"]
    vals = [lat.get("p50"), lat.get("p95"), lat.get("p99")]
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.bar(labels, vals, color="#55a868")
    ax.set_ylabel("ms")
    ax.set_title("End-to-end latency percentiles")
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)


def plot_timeline(metrics: dict, out: Path) -> None:
    tl = metrics.get("timeline") or []
    if not tl:
        return
    ts = [b["t_rel"] for b in tl]
    fig, ax = plt.subplots(figsize=(9, 4))
    ax.plot(ts, [b["confirmed"] for b in tl], label="publish_confirmed", marker=".")
    ax.plot(ts, [b["acked"] for b in tl], label="consumer acked", marker=".")
    ax.set_xlabel("seconds since start")
    ax.set_ylabel("events per bucket")
    ax.legend()
    ax.set_title("Throughput timeline")
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("metrics_json", type=Path)
    p.add_argument("--out-dir", type=Path, default=RESULTS_DIR / "graphs")
    args = p.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    metrics = json.loads(args.metrics_json.read_text())
    stem = args.metrics_json.parent.name
    plot_throughput(metrics, args.out_dir / f"{stem}-throughput.png")
    plot_latency_distribution(metrics, args.out_dir / f"{stem}-latency.png")
    plot_timeline(metrics, args.out_dir / f"{stem}-timeline.png")
    print(f"graphs written to {args.out_dir}", file=sys.stderr)


if __name__ == "__main__":
    main()