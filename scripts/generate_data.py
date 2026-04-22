import argparse
import json
import math
import os
import random
import string


def random_token(rng, length):
    return "".join(rng.choices(string.ascii_lowercase + string.digits, k=length))


def build_base_entries(total_size, rng):
    entries = []
    for i in range(total_size):
        key = f"base_key_{i}_{random_token(rng, 6)}"
        value = f"base_val_{i}_{random_token(rng, 10)}"
        entries.append((key, value))
    return entries


def build_unique_entries(prefix, count, rng):
    entries = []
    for i in range(count):
        key = f"{prefix}_key_{i}_{random_token(rng, 6)}"
        value = f"{prefix}_val_{i}_{random_token(rng, 10)}"
        entries.append((key, value))
    return entries


def write_node_file(path, entries):
    data = {k: v for k, v in entries}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)


def create_pair(size, similarity, seed, prefix, output_dir):
    rng = random.Random(seed)
    similarity = max(0.0, min(1.0, similarity))

    intersection = int(math.floor(size * similarity))
    unique_per_node = size - intersection

    shared = build_base_entries(intersection, rng)
    node1_unique = build_unique_entries(f"{prefix}_node1_u", unique_per_node, rng)
    node2_unique = build_unique_entries(f"{prefix}_node2_u", unique_per_node, rng)

    node1_entries = shared + node1_unique
    node2_entries = shared + node2_unique

    node1_path = os.path.join(output_dir, f"{prefix}_node1.json")
    node2_path = os.path.join(output_dir, f"{prefix}_node2.json")
    node3_path = os.path.join(output_dir, f"{prefix}_node3.json")

    write_node_file(node1_path, node1_entries)
    write_node_file(node2_path, node2_entries)
    write_node_file(node3_path, node1_entries)

    a = set(k for k, _ in node1_entries)
    b = set(k for k, _ in node2_entries)

    intersection_real = len(a & b)
    union_real = len(a | b)
    symmetric_difference = len(a ^ b)
    jaccard = (intersection_real / union_real) if union_real else 1.0

def parse_sizes(value):
    result = []
    for chunk in value.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        result.append(int(chunk))
    return result


def parse_similarities(value):
    result = []
    for chunk in value.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        result.append(float(chunk))
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Generate deterministic replica datasets"
    )
    parser.add_argument("--output-dir", default="data", help="Output directory")
    parser.add_argument("--size", type=int, help="Single set size per node")
    parser.add_argument(
        "--similarity", type=float, help="Single similarity value in [0,1]"
    )
    parser.add_argument("--seed", type=int, default=12345, help="Base random seed")
    parser.add_argument(
        "--prefix",
        default="custom",
        help="Prefix for generated files in single-mode",
    )
    parser.add_argument(
        "--sizes",
        default="10,100,1000",
        help="Comma-separated sizes for --default-matrix",
    )
    parser.add_argument(
        "--similarities",
        default="0.5,0.7,0.85,0.95,0.99",
        help="Comma-separated similarities for --default-matrix",
    )
    parser.add_argument(
        "--default-matrix",
        action="store_true",
        help="Generate matrix and compatibility aliases",
    )

    args = parser.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    if args.default_matrix:
        sizes = parse_sizes(args.sizes)
        similarities = parse_similarities(args.similarities)

        print("Generating similarity datasets...")
        for size in sizes:
            for sim in similarities:
                prefix = f"n{size}_sim{int(round(sim * 100))}"
                seed = args.seed + size * 1000 + int(round(sim * 100))
                create_pair(size, sim, seed, prefix, args.output_dir)

        create_pair(10, 0.8, args.seed + 10_080, "small", args.output_dir)
        create_pair(100, 0.8, args.seed + 100_080, "medium", args.output_dir)
        create_pair(1000, 0.8, args.seed + 1_000_080, "large", args.output_dir)
        print("Done.")
        return

    if args.size is None or args.similarity is None:
        parser.error("single-mode requires --size and --similarity")

if __name__ == "__main__":
    main()
