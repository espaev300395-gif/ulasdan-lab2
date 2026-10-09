import concurrent.futures
import math
import subprocess
import time

from client import CounterClient

def run_benchmark(num_clients: int, num_requests_per_client: int, use_quorum: bool, ports: list[int]):
    addrs = [f"localhost:{p}" for p in ports]
    latencies = []

    def worker(client_id: int):
        client = CounterClient(addrs, client_id=f"bench-client-{client_id}", timeout=2.0)
        local_latencies = []
        for _ in range(num_requests_per_client):
            start = time.perf_counter()
            client.incr("bench_counter", delta=1, use_quorum=use_quorum)
            end = time.perf_counter()
            local_latencies.append((end - start) * 1000.0)
        return local_latencies

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_clients) as executor:
        futures = [executor.submit(worker, i) for i in range(num_clients)]
        for f in concurrent.futures.as_completed(futures):
            latencies.extend(f.result())

    latencies.sort()
    n = len(latencies)
    median = latencies[n // 2]
    p95_idx = math.ceil(0.95 * n) - 1
    p95 = latencies[min(p95_idx, n - 1)]

    return median, p95, n

import sys

def main():
    print("Starting Benchmark Servers...")

    p1 = subprocess.Popen([sys.executable, "server.py", "--port", "50051", "--replica-id", "replica-A"])
    p2 = subprocess.Popen([sys.executable, "server.py", "--port", "50052", "--replica-id", "replica-B"])
    p3 = subprocess.Popen([sys.executable, "server.py", "--port", "50053", "--replica-id", "replica-C"])
    time.sleep(1.5)

    try:
        configs = [
            ("Single replica, 1 client", 1, 2000, False, [50051]),
            ("Single replica, 16 clients", 16, 125, False, [50051]),
            ("Quorum (3 replicas), 1 client", 1, 2000, True, [50051, 50052, 50053]),
            ("Quorum (3 replicas), 16 clients", 16, 125, True, [50051, 50052, 50053]),
        ]

        print("\n| Configuration | Median latency (ms) | p95 latency (ms) | Requests |")
        print("|---|---|---|---|")

        for title, clients, reqs_per_client, quorum, ports in configs:
            med, p95, total_reqs = run_benchmark(clients, reqs_per_client, quorum, ports)
            print(f"| {title:<32} | {med:<19.2f} | {p95:<16.2f} | {total_reqs:<8} |")

    finally:
        p1.kill()
        p2.kill()
        p3.kill()

if __name__ == "__main__":
    main()