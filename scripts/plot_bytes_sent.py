import os
import sys

import matplotlib.pyplot as plt
import pandas as pd


def parse_labels(label_str):
    parsed = {}
    if not isinstance(label_str, str):
        return parsed

    for pair in label_str.split(";"):
        if "=" not in pair:
            continue
        key, value = pair.split("=", 1)
        parsed[key.strip()] = value.strip()

    return parsed


def clean_node_value(value):
    return (
        value.replace("NodeAddress { host: ", "")
        .replace(", port: ", ":")
        .replace(" }", "")
        .replace('"', "")
        .replace("\\", "")
    )


def extract_node_label(label_str):
    labels = parse_labels(label_str)
    node = labels.get("target") or labels.get("node")
    if node:
        return clean_node_value(node)
    return clean_node_value(str(label_str))


def resolve_node_label(row):
    node = row.get("node")
    if isinstance(node, str) and node.strip():
        return clean_node_value(node)

    return extract_node_label(row.get("labels", ""))


def plot_metric(csv_file, output_prefix, title):
    if not os.path.exists(csv_file):
        print(f"File not found: {csv_file}")
        return

    try:
        df = pd.read_csv(csv_file)
    except Exception as e:
        print(f"Error reading {csv_file}: {e}")
        return

    if df.empty:
        print(f"No data in {csv_file}")
        return

    df["node_label"] = df.apply(resolve_node_label, axis=1)

    # 1. Plot bytes per iteration
    plt.figure(figsize=(10, 6))
    for node_label, group in df.groupby("node_label"):
        group = (
            group.groupby("iteration", as_index=False)["value"]
            .sum()
            .sort_values("iteration")
        )
        plt.plot(group["iteration"], group["value"], marker="o", label=node_label)

    plt.xlabel("Iteration")
    plt.ylabel("Bytes Sent")
    plt.title(f"{title} (Per Iteration)")
    plt.yscale("log")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f"{output_prefix}_per_iteration.png")
    plt.close()

    # 2. Plot cumulative bytes sent over time (iterations)
    plt.figure(figsize=(10, 6))
    for node_label, group in df.groupby("node_label"):
        group = (
            group.groupby("iteration", as_index=False)["value"]
            .sum()
            .sort_values("iteration")
        )
        cumulative_values = group["value"].cumsum()
        plt.plot(group["iteration"], cumulative_values, marker="o", label=node_label)

    plt.xlabel("Iteration")
    plt.ylabel("Cumulative Bytes Sent")
    plt.title(f"{title} (Cumulative)")
    plt.yscale("log")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f"{output_prefix}_cumulative.png")
    plt.close()

    print(
        f"Saved plots to:\n  - {output_prefix}_per_iteration.png\n  - {output_prefix}_cumulative.png"
    )


if __name__ == "__main__":
    # If a path is provided as argument, use it; otherwise default to metrics_output/default_run
    base_dir = sys.argv[1] if len(sys.argv) > 1 else "metrics_output/default_run"

    if not os.path.exists(base_dir):
        print(f"Directory {base_dir} does not exist. Run the replication engine first.")
        sys.exit(1)

    print(f"Plotting metrics from {base_dir}...")

    plot_metric(
        os.path.join(base_dir, "total_bytes_sent.csv"),
        os.path.join(base_dir, "total_bytes_sent"),
        "Total Bytes Sent",
    )

    plot_metric(
        os.path.join(base_dir, "riblt_bytes_sent.csv"),
        os.path.join(base_dir, "riblt_bytes_sent"),
        "RIBLT Bytes Sent",
    )
