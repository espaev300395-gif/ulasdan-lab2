import argparse
import concurrent.futures
import logging
import threading
import time
import sys
import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self, replica_id: str = "replica-A", fault: str = None, delay_ms: int = 0):
        self.replica_id = replica_id
        self.fault = fault
        self.delay_ms = delay_ms

        self._lock = threading.Lock()
        self._values = {}  # counter_id -> int
        self._seen = {}  # idempotency_key -> (counter_id, new_value)
        self.clock = LamportClock(replica_id)

    def Increment(self, request, context):
        recv_l = self.clock.receive_event(request.lamport_time)
        self.clock.log_event(
            "RECV",
            f"Increment(counter={request.counter_id}, delta={request.delta})",
            received_l=request.lamport_time
        )

        if self.delay_ms > 0:
            time.sleep(self.delay_ms / 1000.0)

        if self.fault == "drop-after-recv":
            context.abort(grpc.StatusCode.UNAVAILABLE, "Fault injected: drop request after recv")

        with self._lock:
            if request.idempotency_key in self._seen:
                stored_counter_id, stored_val = self._seen[request.idempotency_key]

                self.clock.send_event()
                self.clock.log_event("APPLY", f"counter={stored_counter_id} -> {stored_val} (DUPLICATE)")

                send_l = self.clock.send_event()
                reply = counter_pb2.IncrementReply(
                    new_value=stored_val,
                    was_duplicate=True,
                    lamport_time=send_l
                )
                self.clock.log_event("SEND", f"IncrementReply(new_value={stored_val}, duplicate=True)")
                return reply

            current_val = self._values.get(request.counter_id, 0)
            new_val = current_val + request.delta
            self._values[request.counter_id] = new_val
            self._seen[request.idempotency_key] = (request.counter_id, new_val)

            self.clock.send_event()
            self.clock.log_event("APPLY", f"counter={request.counter_id} -> {new_val}")

            send_l = self.clock.send_event()
            reply = counter_pb2.IncrementReply(
                new_value=new_val,
                was_duplicate=False,
                lamport_time=send_l
            )
            self.clock.log_event("SEND", f"IncrementReply(new_value={new_val}, duplicate=False)")
            return reply

    def Get(self, request, context):
        self.clock.receive_event(request.lamport_time)
        self.clock.log_event("RECV", f"Get(counter={request.counter_id})", received_l=request.lamport_time)

        with self._lock:
            found = request.counter_id in self._values
            val = self._values.get(request.counter_id, 0)

            send_l = self.clock.send_event()
            self.clock.log_event("SEND", f"GetReply(value={val}, found={found})")
            return counter_pb2.GetReply(
                value=val,
                found=found,
                lamport_time=send_l
            )


def serve(port: int, replica_id: str, fault: str = None, delay_ms: int = 0):
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=8))
    servicer = CounterServicer(replica_id=replica_id, fault=fault, delay_ms=delay_ms)
    counter_pb2_grpc.add_CounterServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logging.info(f"Server {replica_id} listening on port {port}")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Replicated Counter Server")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--replica-id", type=str, default=None)
    parser.add_argument("--fault", type=str, default=None)
    parser.add_argument("--delay-ms", type=int, default=0)
    args = parser.parse_args()

    rid = args.replica_id or {50051: "replica-A", 50052: "replica-B", 50053: "replica-C"}.get(args.port,
                                                                                              f"replica-{args.port}")
    serve(args.port, rid, args.fault, args.delay_ms)