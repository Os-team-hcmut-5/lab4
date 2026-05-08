#!/usr/bin/env python3
import csv
import os
import re
import subprocess
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")

import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[2]
SCHEDULER = ROOT / "scheduler"
CSV_FILE = ROOT / "lab4" / "jobs_100.csv"
OUT_DIR = ROOT / "lab4" / "bonus4" / "worker_results"
WORKERS = [2, 4, 8]


SUMMARY_PATTERNS = {
    "policy": re.compile(r"^Policy: (.+)$"),
    "workers": re.compile(r"^Workers: (\d+)$"),
    "total_jobs": re.compile(r"^Total jobs: (\d+)$"),
    "total_simulation_time": re.compile(r"^Total simulation time: (\d+)$"),
    "average_waiting_time": re.compile(r"^Average waiting time: ([0-9.]+)$"),
    "average_turnaround_time": re.compile(r"^Average turnaround time: ([0-9.]+)$"),
    "throughput": re.compile(r"^Throughput: ([0-9.]+) jobs/unit time$"),
    "worker_utilization": re.compile(r"^Worker utilization: ([0-9.]+)%$"),
    "starvation_risk_jobs": re.compile(r"^Starvation-risk jobs: (\d+)$"),
}


def parse_summaries(output):
    rows = []
    current = None
    for line in output.splitlines():
        line = line.strip()
        if line == "--- Run Summary ---" or line.startswith("Policy: "):
            if line.startswith("Policy: "):
                current = {}
            elif current is None:
                current = {}

        if current is None:
            continue

        for key, pattern in SUMMARY_PATTERNS.items():
            match = pattern.match(line)
            if match:
                value = match.group(1)
                if key in {"policy"}:
                    current[key] = value
                elif key in {"average_waiting_time", "average_turnaround_time", "throughput", "worker_utilization"}:
                    current[key] = float(value)
                else:
                    current[key] = int(value)

        if line.startswith("Starvation-risk jobs: ") and current:
            rows.append(current)
            current = None
    return rows


def run_scheduler(workers):
    cmd = [str(SCHEDULER), str(CSV_FILE), "all", str(workers)]
    result = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, check=True)
    log_path = OUT_DIR / f"scheduler_workers_{workers}.log"
    log_path.write_text(result.stdout, encoding="utf-8")
    return parse_summaries(result.stdout)


def save_csv(rows):
    csv_path = OUT_DIR / "worker_comparison.csv"
    fields = [
        "policy",
        "workers",
        "total_jobs",
        "total_simulation_time",
        "average_waiting_time",
        "average_turnaround_time",
        "throughput",
        "worker_utilization",
        "starvation_risk_jobs",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    return csv_path


def plot(rows):
    policies = ["FIFO", "SJF", "Priority"]
    metrics = [
        ("total_simulation_time", "Total simulation time", "Lower is better"),
        ("average_waiting_time", "Average waiting time", "Lower is better"),
        ("average_turnaround_time", "Average turnaround time", "Lower is better"),
        ("throughput", "Throughput", "Higher is better"),
        ("worker_utilization", "Worker utilization (%)", "Higher is better"),
        ("starvation_risk_jobs", "Starvation-risk jobs", "Lower is better"),
    ]

    fig, axes = plt.subplots(2, 3, figsize=(16, 9), constrained_layout=True)
    colors = {"FIFO": "#2f6f9f", "SJF": "#4f8f5f", "Priority": "#c4663a"}

    for ax, (metric, title, subtitle) in zip(axes.flat, metrics):
        for policy in policies:
            policy_rows = sorted((row for row in rows if row["policy"] == policy), key=lambda row: row["workers"])
            ax.plot(
                [row["workers"] for row in policy_rows],
                [row[metric] for row in policy_rows],
                marker="o",
                linewidth=2,
                label=policy,
                color=colors[policy],
            )
        ax.set_title(f"{title}\n{subtitle}", fontsize=11)
        ax.set_xlabel("Workers")
        ax.set_xticks(WORKERS)
        ax.grid(True, linestyle="--", alpha=0.35)

    axes[0][0].set_ylabel("Time units")
    axes[0][1].set_ylabel("Time units")
    axes[0][2].set_ylabel("Time units")
    axes[1][0].set_ylabel("Jobs / time unit")
    axes[1][1].set_ylabel("Percent")
    axes[1][2].set_ylabel("Jobs")

    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=3, frameon=False)
    fig.suptitle("Scheduler comparison by worker count", fontsize=15, y=1.03)

    png_path = OUT_DIR / "worker_comparison.png"
    fig.savefig(png_path, dpi=160)
    plt.close(fig)
    return png_path


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    rows = []
    for workers in WORKERS:
        rows.extend(run_scheduler(workers))

    csv_path = save_csv(rows)
    png_path = plot(rows)

    print(f"Wrote {csv_path}")
    print(f"Wrote {png_path}")
    for row in rows:
        print(
            f"{row['policy']:8s} workers={row['workers']} "
            f"time={row['total_simulation_time']} "
            f"wait={row['average_waiting_time']:.2f} "
            f"turnaround={row['average_turnaround_time']:.2f} "
            f"throughput={row['throughput']:.3f} "
            f"util={row['worker_utilization']:.2f}% "
            f"starve={row['starvation_risk_jobs']}"
        )


if __name__ == "__main__":
    main()
