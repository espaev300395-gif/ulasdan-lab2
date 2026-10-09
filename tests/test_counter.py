import concurrent.futures
import pytest
import grpc

import counter_pb2
import counter_pb2_grpc
from server import CounterServicer
from client import CounterClient


@pytest.fixture
def running_server():
    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=8))
    servicer = CounterServicer(replica_id="replica-A")
    counter_pb2_grpc.add_CounterServicer_to_server(servicer, server)
    server.add_insecure_port("[::]:50051")
    server.start()
    yield servicer
    server.stop(0)


@pytest.fixture
def three_replicas():
    servers = []
    ports = [50051, 50052, 50053]
    for p in ports:
        srv = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=8))
        svc = CounterServicer(replica_id=f"replica-{p}")
        counter_pb2_grpc.add_CounterServicer_to_server(svc, srv)
        srv.add_insecure_port(f"[::]:{p}")
        srv.start()
        servers.append(srv)
    yield
    for srv in servers:
        srv.stop(0)


def test_increment_applies_delta(running_server):
    client = CounterClient(["localhost:50051"])
    val, acks, dup = client.incr("test_counter", delta=5, use_quorum=False)
    assert val == 5
    assert dup is False
    assert acks == 1


def test_duplicate_key_not_reapplied(running_server):
    client = CounterClient(["localhost:50051"])
    v1, _, d1 = client.incr("x", delta=5, key="k-1", use_quorum=False)
    v2, _, d2 = client.incr("x", delta=5, key="k-1", use_quorum=False)
    assert v1 == 5 and d1 is False
    assert v2 == 5 and d2 is True


def test_concurrent_increments_exact(running_server):
    client = CounterClient(["localhost:50051"])
    counter_name = "concurrent_counter"

    def worker():
        for _ in range(1000):
            client.incr(counter_name, delta=1, use_quorum=False)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        f1 = executor.submit(worker)
        f2 = executor.submit(worker)
        f1.result()
        f2.result()

    val, found = client.get(counter_name)
    assert found is True
    assert val == 2000


def test_get_missing_counter(running_server):
    client = CounterClient(["localhost:50051"])
    val, found = client.get("non_existent_key")
    assert found is False
    assert val == 0


def test_retry_after_timeout_is_safe():
    import time

    class OneTimeDelayServicer(CounterServicer):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.delayed_once = False

        def Increment(self, request, context):
            if not self.delayed_once:
                self.delayed_once = True
                time.sleep(0.8)  # Задержка превышает таймаут клиента только в первый раз
            return super().Increment(request, context)

    srv = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=8))
    svc = OneTimeDelayServicer(replica_id="replica-delayed")
    counter_pb2_grpc.add_CounterServicer_to_server(svc, srv)
    srv.add_insecure_port("[::]:50054")
    srv.start()

    try:
        client = CounterClient(["localhost:50054"], timeout=0.4)
        val, acks, dup = client.incr("timeout_counter", delta=10, key="t-key-1", use_quorum=False)
        assert val == 10
    finally:
        srv.stop(0)


def test_majority_commit_two_acks(three_replicas):
    addrs = ["localhost:50051", "localhost:50052", "localhost:59999"]  # 59999 is dead
    client = CounterClient(addrs, timeout=0.5)
    val, acks, dup = client.incr("likes:post-100", delta=1, use_quorum=True)
    assert val == 1
    assert acks == 2


def test_no_commit_below_majority(three_replicas):
    addrs = ["localhost:50051", "localhost:59998", "localhost:59999"]
    client = CounterClient(addrs, timeout=0.3)

    with pytest.raises(RuntimeError) as exc:
        client.incr("likes:post-100", delta=1, use_quorum=True)

    assert "Quorum write failed" in str(exc.value)
    val, found = client.get("likes:post-100", target_addr="localhost:50051")
    assert val == 1


def test_replicas_converge(three_replicas):
    addrs = ["localhost:50051", "localhost:50052", "localhost:50053"]
    client = CounterClient(addrs)

    for _ in range(10):
        client.incr("converge_key", delta=3, use_quorum=True)

    v1, _ = client.get("converge_key", target_addr="localhost:50051")
    v2, _ = client.get("converge_key", target_addr="localhost:50052")
    v3, _ = client.get("converge_key", target_addr="localhost:50053")

    assert v1 == 30 and v2 == 30 and v3 == 30