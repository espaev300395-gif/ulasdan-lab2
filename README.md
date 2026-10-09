# Distributed Replicated Counter Service

[![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/)
[![gRPC](https://img.shields.io/badge/gRPC-v1.62-green.svg)](https://grpc.io/)
[![Testing](https://img.shields.io/badge/Testing-pytest-yellow.svg)](https://docs.pytest.org/)

A fault-tolerant, replicated counter service built using gRPC in Python. Features logical time tracking via Lamport Clocks, duplicate suppression via client-generated idempotency keys, and majority-quorum write consensus ($N=3, Q=2$).

---

## System Architecture Overview

- **Part A (gRPC & Idempotency):** Thread-safe in-memory state store protected by reentrant locking. Retried client requests with identical idempotency keys are deduplicated without re-applying deltas.
- **Part B (Lamport Clocks):** Logical clocks embedded directly into gRPC headers across all client-server boundaries for causal event ordering and trace visualization.
- **Part C (Quorum Consensus & Fault Tolerance):** Distributed write path across 3 independent replicas (ports 50051–50053). A write is committed only when acknowledged by at least 2 out of 3 replicas, tolerating 1 node failure ($F=1$).

---

## Repository Structure

```text
dcc-assignment2/
├── counter.proto          # Protocol Buffers service definition
├── counter_pb2.py         # Generated gRPC request/reply types (do not edit)
├── counter_pb2_grpc.py    # Generated gRPC servicer/stub code (do not edit)
├── clocks.py              # Lamport logical clock implementation & trace logger
├── server.py              # Replica server (flags: --port, --replica-id, --fault, --delay-ms)
├── client.py              # CLI client with exponential backoff retries & quorum engine
├── perf_benchmark.py      # Latency & throughput benchmark runner
├── tests/
│   ├── test_counter.py    # Unit & integration test suite (Task C2)
│   └── test_failures.py   # Automated fault-injection tests (Task C3)
├── logs/
│   └── event_trace.log    # Captured Lamport event trace (~30 lines)
└── README.md              # Project documentation & runbook
