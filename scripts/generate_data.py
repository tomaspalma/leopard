import argparse
import bisect
import json
import math
import os
import random
import string


def random_token(rng, length):
    return "".join(rng.choices(string.ascii_lowercase + string.digits, k=length))


def make_length_sampler(dist, base, maximum, zipf_exponent, rng):
    """Return a zero-arg callable yielding a value length, YCSB-style.

    `dist` is one of "constant", "uniform", "zipfian". All randomness is drawn
    from `rng` so generation stays deterministic for a given seed.
    """
    if dist == "constant":
        return lambda: base

    lo, hi = base, max(base, maximum)

    if dist == "uniform":
        return lambda: rng.randint(lo, hi)

    if dist == "zipfian":
        n = hi - lo + 1
        weights = [1.0 / ((r + 1) ** zipf_exponent) for r in range(n)]
        total = sum(weights)
        cum = []
        acc = 0.0
        for w in weights:
            acc += w / total
            cum.append(acc)

        def sample():
            idx = bisect.bisect_left(cum, rng.random())
            if idx >= n:
                idx = n - 1
            return lo + idx

        return sample

    raise ValueError(f"unknown value-size-dist: {dist}")


def make_value_fn(length_sampler):
    """Build the per-entry value generator.

    When `length_sampler` is None we keep the legacy structured value
    (`{tag}_val_{i}_{token}`) so existing seeds reproduce byte-for-byte.
    Otherwise the value is a random token whose length follows the sampler.
    """
    if length_sampler is None:
        def legacy(rng, tag, i):
            return f"{tag}_val_{i}_{random_token(rng, 10)}"

        return legacy

    def realistic(rng, _tag, _i):
        return random_token(rng, length_sampler())

    return realistic


def build_base_entries(total_size, rng, value_fn):
    entries = []
    for i in range(total_size):
        key = f"base_key_{i}_{random_token(rng, 6)}"
        value = value_fn(rng, "base", i)
        entries.append((key, value))
    return entries


def build_unique_entries(prefix, count, rng, value_fn):
    entries = []
    for i in range(count):
        key = f"{prefix}_key_{i}_{random_token(rng, 6)}"
        value = value_fn(rng, prefix, i)
        entries.append((key, value))
    return entries


def write_node_file(path, entries):
    data = {k: v for k, v in entries}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)


def create_pair(
    size,
    similarity,
    seed,
    prefix,
    output_dir,
    value_dist=None,
    value_size=10,
    value_size_max=100,
    zipf_exponent=0.99,
):
    rng = random.Random(seed)
    similarity = max(0.0, min(1.0, similarity))

    length_sampler = (
        None
        if value_dist is None
        else make_length_sampler(value_dist, value_size, value_size_max, zipf_exponent, rng)
    )
    value_fn = make_value_fn(length_sampler)

    intersection = int(math.floor(size * similarity))
    unique_per_node = size - intersection

    shared = build_base_entries(intersection, rng, value_fn)

    node1_unique = build_unique_entries(f"{prefix}_node1_u", unique_per_node, rng, value_fn)
    node2_unique = build_unique_entries(f"{prefix}_node2_u", unique_per_node, rng, value_fn)
    node3_unique = build_unique_entries(f"{prefix}_node3_u", unique_per_node, rng, value_fn)

    node1_entries = shared + node1_unique
    node2_entries = shared + node2_unique
    node3_entries = shared + node3_unique

    node1_path = os.path.join(output_dir, f"{prefix}_node1.json")
    node2_path = os.path.join(output_dir, f"{prefix}_node2.json")
    node3_path = os.path.join(output_dir, f"{prefix}_node3.json")

    write_node_file(node1_path, node1_entries)
    write_node_file(node2_path, node2_entries)
    write_node_file(node3_path, node3_entries)

    a = set(k for k, _ in node1_entries)
    b = set(k for k, _ in node2_entries)

    intersection_real = len(a & b)
    union_real = len(a | b)
    symmetric_difference = len(a ^ b)
    jaccard = (intersection_real / union_real) if union_real else 1.0

    print(
        f"[{prefix}] size={size} target_sim={similarity:.2f} "
        f"intersection={intersection_real} union={union_real} "
        f"sym_diff={symmetric_difference} jaccard={jaccard:.4f}"
    )


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
    parser.add_argument(
        "--seed", type=int, default=None, help="Random seed (single-mode); random if omitted"
    )
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
    parser.add_argument(
        "--value-size-dist",
        choices=["constant", "uniform", "zipfian"],
        default=None,
        help="Value length distribution (YCSB-style fieldlengthdistribution). "
        "Omit for legacy structured values.",
    )
    parser.add_argument(
        "--value-size",
        type=int,
        default=10,
        help="Base/min value length in bytes (used by --value-size-dist)",
    )
    parser.add_argument(
        "--value-size-max",
        type=int,
        default=100,
        help="Max value length for uniform/zipfian distributions",
    )
    parser.add_argument(
        "--zipf-exponent",
        type=float,
        default=0.99,
        help="Zipfian skew for value sizes (higher = more short values)",
    )

    args = parser.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    if args.default_matrix:
        sizes = parse_sizes(args.sizes)
        similarities = parse_similarities(args.similarities)

        print("Generating similarity datasets...")
        for size in sizes:
            for sim in similarities:
                prefix = f"n{size}_sim{int(round(sim * 100)):02d}"
                seed = random.randint(0, 10_000_000_000)
                create_pair(
                    size,
                    sim,
                    seed,
                    prefix,
                    args.output_dir,
                    args.value_size_dist,
                    args.value_size,
                    args.value_size_max,
                    args.zipf_exponent,
                )

        for alias_size, alias_name in ((10, "small"), (100, "medium"), (1000, "large")):
            create_pair(
                alias_size,
                0.8,
                random.randint(0, 10_000_000_000),
                alias_name,
                args.output_dir,
                args.value_size_dist,
                args.value_size,
                args.value_size_max,
                args.zipf_exponent,
            )
        print("Done.")
        return

    if args.size is None or args.similarity is None:
        parser.error("single-mode requires --size and --similarity")

    seed = args.seed if args.seed is not None else random.randint(0, 10_000_000_000)
    create_pair(
        args.size,
        args.similarity,
        seed,
        args.prefix,
        args.output_dir,
        args.value_size_dist,
        args.value_size,
        args.value_size_max,
        args.zipf_exponent,
    )


if __name__ == "__main__":
    main()
