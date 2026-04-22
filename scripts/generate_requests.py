import argparse
import json
import random
import string
import urllib.request
import urllib.error
import concurrent.futures


def random_string(length=16):
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


def generate_requests(address, port, count):
    node_url = f"http://{address}:{port}"

    def send_post(_):
        key = random_string(12)
        value = random_string(32)
        try:
            url = f"{node_url.rstrip('/')}/{key}"
            data = json.dumps({"value": value}).encode("utf-8")
            req = urllib.request.Request(
                url, data=data, headers={"Content-Type": "application/json"}
            )

            with urllib.request.urlopen(req, timeout=5) as response:
                if response.status == 200:
                    return True
                else:
                    print(f"Failed with status code: {response.status}")
                    return False
        except urllib.error.URLError as e:
            print(f"Request failed for {url}: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error: {e}")
            return False

    print(f"Issuing {count} POST requests to {node_url}...")
    success_count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(send_post, i) for i in range(count)]
        for future in concurrent.futures.as_completed(futures):
            if future.result():
                success_count += 1

    print(f"Done. {success_count}/{count} requests succeeded.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Issue POST requests to a node")
    parser.add_argument(
        "--address",
        "-a",
        type=str,
        default="127.0.0.1",
        help="Target node address (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port", "-p", type=int, default=8080, help="Target node port (default: 8080)"
    )
    parser.add_argument(
        "--count",
        "-c",
        type=int,
        default=1,
        help="Exact number of requests to send (default: 1)",
    )

    args = parser.parse_args()
    generate_requests(args.address, args.port, args.count)
