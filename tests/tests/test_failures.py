import os
import sys
import time
import subprocess
import pytest
from client import CounterClient


def start_server_subprocess(port: int, fault: str = None, delay_ms: int = 0):

    test_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(test_dir, "..", ".."))
    server_script = os.path.join(project_root, "server.py")


    cmd = [sys.executable, server_script, "--port", str(port), "--replica-id", f"replica-{port}"]
    if fault:
        cmd.extend(["--fault", fault])
    if delay_ms > 0:
        cmd.extend(["--delay-ms", str(delay_ms)])

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(1.0)
    return proc

def test_replica_crash_mid_request():
    p1 = start_server_subprocess(50051)
    p2 = start_server_subprocess(50052)
    p3 = start_server_subprocess(50053)

    try:
        client = CounterClient(["localhost:50051", "localhost:50052", "localhost:50053"], timeout=1.0)
        val, acks, _ = client.incr("crash_counter", delta=1)
        assert acks == 3 and val == 1

        p3.kill()
        p3.wait()

        val2, acks2, _ = client.incr("crash_counter", delta=1)
        assert acks2 == 2
        assert val2 == 2
    finally:
        p1.kill()
        p2.kill()
        if p3.poll() is None:
            p3.kill()


def test_request_duplication():
    p1 = start_server_subprocess(50051)
    p2 = start_server_subprocess(50052)
    p3 = start_server_subprocess(50053)

    try:
        client = CounterClient(["localhost:50051", "localhost:50052", "localhost:50053"], timeout=1.0)
        fixed_key = "dup-test-uuid-9999"

        val1, acks1, dup1 = client.incr("dup_counter", delta=10, key=fixed_key)
        assert val1 == 10 and dup1 is False

        val2, acks2, dup2 = client.incr("dup_counter", delta=10, key=fixed_key)
        assert val2 == 10 and dup2 is True
    finally:
        p1.kill()
        p2.kill()
        p3.kill()


def test_induced_timeout_with_retry():
    p1 = start_server_subprocess(50051, delay_ms=3000)
    p2 = start_server_subprocess(50052)
    p3 = start_server_subprocess(50053)

    try:
        client = CounterClient(["localhost:50051", "localhost:50052", "localhost:50053"], timeout=0.8)
        val, acks, dup = client.incr("delay_counter", delta=7)
        assert acks >= 2
        assert val == 7
    finally:
        p1.kill()
        p2.kill()
        p3.kill()