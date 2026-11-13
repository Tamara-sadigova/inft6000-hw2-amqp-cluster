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
    bars = ax.bar(["publish (confirmed)", "consume (acked)"], [pub, con], color=["#4c72b0", "#dd8452"])
    ax.bar_label(bars, fmt="%.1f")
    ax.set_ylabel("msg/s")
    ax.set_ylim(bottom=0)
    pw, cw = metrics["publish"].get("window_s"), metrics["consume"].get("window_s")
    ax.set_title(f"Throughput (publish window {pw}s, consume window {cw}s)", fontsize=10)
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)


def plot_latency_distribution(metrics: dict, out: Path) -> None:
    labels = ["P50", "P95", "P99", "max"]
    series = [("publisher confirm", metrics["publish"]["confirm_latency_ms"], "#4c72b0"),
              ("end-to-end", metrics["consume"]["end_to_end_latency_ms"], "#55a868")]
    fig, ax = plt.subplots(figsize=(7, 4))
    width = 0.38
    for i, (name, lat, color) in enumerate(series):
        vals = [lat.get(k.lower()) or 0 for k in labels]
        xs = [x + (i - 0.5) * width for x in range(len(labels))]
        bars = ax.bar(xs, vals, width, label=name, color=color)
        ax.bar_label(bars, fmt="%.1f", fontsize=8)
    ax.set_xticks(range(len(labels)), labels)
    ax.set_yscale("log")
    ax.set_ylabel("ms (log scale)")
    ax.set_title("Latency percentiles")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)


def plot_timeline(metrics: dict, out: Path) -> None:
    tl = metrics.get("timeline") or []
    if not tl:
        return
    bucket = metrics.get("timeline_bucket_s") or 1.0
    ts = [b["t_rel"] for b in tl]
    fig, ax = plt.subplots(figsize=(10, 4.5))
    # Step plot: each bucket is drawn flat over its own interval, so an empty
    # bucket shows as a drop to zero instead of being interpolated over.
    ax.step(ts, [b["confirmed"] / bucket for b in tl], where="post", label="publish confirmed")
    ax.step(ts, [b["acked"] / bucket for b in tl], where="post", label="consumer acked", alpha=0.8)
    ax.set_ylim(bottom=0)
    ax.set_xlim(left=0)

    outage = metrics.get("client_outage") or {}
    conf = outage.get("confirm")
    if conf and conf.get("gap_s", 0) > 0:
        ax.axvspan(conf["from_rel"], conf["to_rel"], color="red", alpha=0.15,
                   label=f"longest publish gap after fault: {conf['gap_s']:.2f}s")

    colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    events = sorted(metrics.get("events") or [], key=lambda e: e["t_rel"])
    for i, e in enumerate(events):
        ax.axvline(e["t_rel"], color=colors[(i + 2) % len(colors)], linestyle="--", linewidth=1.2,
                   label=f"t={e['t_rel']:.2f}s  {e['label']}")

    ax.set_xlabel("seconds since run start")
    ax.set_ylabel("msg/s")
    ax.legend(loc="center left", bbox_to_anchor=(1.01, 0.5), fontsize=8)
    ax.set_title(f"Throughput timeline: {metrics.get('run_id', '')}", fontsize=10)
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)


def plot_all(metrics: dict, out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = metrics.get("run_id") or Path(metrics["run_dir"]).name
    paths = [out_dir / f"{stem}-{k}.png" for k in ("throughput", "latency", "timeline")]
    plot_throughput(metrics, paths[0])
    plot_latency_distribution(metrics, paths[1])
    plot_timeline(metrics, paths[2])
    return [p for p in paths if p.exists()]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("metrics_json", type=Path)
    p.add_argument("--out-dir", type=Path, default=RESULTS_DIR / "graphs")
    args = p.parse_args()
    metrics = json.loads(args.metrics_json.read_text())
    plot_all(metrics, args.out_dir)
    print(f"graphs written to {args.out_dir}", file=sys.stderr)


if __name__ == "__main__":
    main()
