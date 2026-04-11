import json
import os


def generate_data(size, overlap_percentage, num_nodes, prefix):
    os.makedirs("data", exist_ok=True)

    base_size = int(size * overlap_percentage)

    # Generate shared keys
    base_data = {f"shared_key_{i}": f"shared_val_{i}" for i in range(base_size)}

    for node in range(1, num_nodes + 1):
        node_data = base_data.copy()

        unique_size = size - base_size
        for i in range(unique_size):
            key = f"node{node}_unique_key_{i}"
            value = f"node{node}_unique_val_{i}"
            node_data[key] = value

        with open(f"data/{prefix}_node{node}.json", "w") as f:
            json.dump(node_data, f, indent=2)


print("Generating data with simple word keys and values...")
generate_data(size=10, overlap_percentage=0.8, num_nodes=3, prefix="small")
generate_data(size=100, overlap_percentage=0.8, num_nodes=3, prefix="medium")
generate_data(size=1000, overlap_percentage=0.8, num_nodes=3, prefix="large")
print("Done.")
