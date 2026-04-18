import argparse
import csv
from pathlib import Path

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


def normalize_file(path: Path) -> bool:
    rows = []

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None:
            return False

        for row in reader:
            if not row:
                continue

            # Old format: iteration,timestamp,value,labels
            if len(row) == 4:
                iteration, timestamp, value, labels = row
                parsed = parse_labels(labels)
                node = (
                    parsed.get("target")
                    or parsed.get("node")
                    or parsed.get("neighbor")
                    or ""
                )
                protocol = parsed.get("protocol", "")
                run_id = parsed.get("run_id", "")
                trial = parsed.get("trial", "")
                similarity = parsed.get("similarity", "")

                rows.append(
                    {
                        "iteration": iteration,
                        "timestamp": timestamp,
                        "value": value,
                        "node": node,
                        "protocol": protocol,
                        "run_id": run_id,
                        "trial": trial,
                        "similarity": similarity,
                        "labels": labels,
                    }
                )
                continue

            # New format (or mixed).
            if len(row) >= 9:
                iteration = row[0]
                timestamp = row[1]
                value = row[2]
                node = row[3]
                protocol = row[4]
                run_id = row[5]
                trial = row[6]
                similarity = row[7]
                labels = row[8]

                if len(row) >= 10 and row[9]:
                    labels = row[9]

                parsed = parse_labels(labels)
                if not node:
                    node = (
                        parsed.get("target")
                        or parsed.get("node")
                        or parsed.get("neighbor")
                        or ""
                    )
                if not protocol:
                    protocol = parsed.get("protocol", "")
                if not run_id:
                    run_id = parsed.get("run_id", "")
                if not trial:
                    trial = parsed.get("trial", "")
                if not similarity:
                    similarity = parsed.get("similarity", "")

                rows.append(
                    {
                        "iteration": iteration,
                        "timestamp": timestamp,
                        "value": value,
                        "node": node,
                        "protocol": protocol,
                        "run_id": run_id,
                        "trial": trial,
                        "similarity": similarity,
                        "labels": labels,
                    }
                )

    if not rows:
        return False

    new_df = pd.DataFrame(rows)
    new_df.to_csv(path, index=False)
    return True


def main():
    parser = argparse.ArgumentParser(description="Normalize metrics CSV label columns")
    parser.add_argument(
        "root", nargs="?", default="metrics_output", help="Metrics root directory"
    )
    args = parser.parse_args()

    root = Path(args.root)
    if not root.exists():
        print(f"Path does not exist: {root}")
        return

    changed = 0
    for file in root.rglob("*.csv"):
        try:
            if normalize_file(file):
                changed += 1
        except Exception as exc:
            print(f"Failed to normalize {file}: {exc}")

    print(f"Normalized {changed} CSV files under {root}")


if __name__ == "__main__":
    main()
