import json
import os
import random
import string


def random_string(length=16):
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_data(size, overlap_percentage, num_nodes, prefix):
    os.makedirs("data", exist_ok=True)

    base_size = int(size * overlap_percentage)
    base_data = {random_string(12): random_string(32) for _ in range(base_size)}

    for node in range(1, num_nodes + 1):
        node_data = base_data.copy()

        unique_size = size - base_size
        for _ in range(unique_size):
            key = random_string(12)
            value = random_string(32)
            node_data[key] = value

        with open(f"data/{prefix}_node{node}.json", "w") as f:
            json.dump(node_data, f, indent=2)


print("Generating data with real random keys and values...")
generate_data(size=10, overlap_percentage=0.8, num_nodes=3, prefix="small")
generate_data(size=100, overlap_percentage=0.8, num_nodes=3, prefix="medium")
generate_data(size=1000, overlap_percentage=0.8, num_nodes=3, prefix="large")
print("Done.")
