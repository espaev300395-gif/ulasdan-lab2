import argparse
import concurrent.futures
import logging
import sys
import time
import uuid
import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock


class CounterClient:
    def __init__(self, addresses: list[str], client_id: str = "client-1", timeout: float = 2.0):
        self.addresses = addresses
        self.client_id = client_id
        self.timeout = timeout
        self.clock = LamportClock(client_id)

    def _single_replica_incr(self, target_addr: str, counter_id: str, delta: int,
                             idempotency_key: str) -> counter_pb2.IncrementReply:
        max_retries = 3
        backoffs = [0.2, 0.4, 0.8]

        for attempt in range(max_retries + 1):
            try:
                with grpc.insecure_channel(target_addr) as channel:
                    stub = counter_pb2_grpc.CounterStub(channel)
                    send_l = self.clock.send_event()
                    req = counter_pb2.IncrementRequest(
                        counter_id=counter_id,
                        delta=delta,
                        idempotency_key=idempotency_key,
                        lamport_time=send_l
                    )
                    self.clock.log_event("SEND", f"Increment(counter={counter_id}, delta={delta})")

                    reply = stub.Increment(req, timeout=self.timeout)
                    self.clock.receive_event(reply.lamport_time)
                    self.clock.log_event(
                        "RECV",
                        f"IncrementReply(new_value={reply.new_value}, duplicate={reply.was_duplicate})",
                        received_l=reply.lamport_time
                    )
                    return reply

            except grpc.RpcError as e:
                if e.code() in (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE):
                    if attempt < max_retries:
                        time.sleep(backoffs[attempt])
                        continue
                raise e

    def incr(self, counter_id: str, delta: int = 1, key: str = None, use_quorum: bool = True) -> tuple[int, int, bool]:
        idempotency_key = key if key else str(uuid.uuid4())

        if not use_quorum or len(self.addresses) == 1:
            reply = self._single_replica_incr(self.addresses[0], counter_id, delta, idempotency_key)
            return reply.new_value, 1, reply.was_duplicate

        required_acks = (len(self.addresses) // 2) + 1
        successful_replies = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.addresses)) as executor:
            future_to_addr = {
                executor.submit(self._single_replica_incr, addr, counter_id, delta, idempotency_key): addr
                for addr in self.addresses
            }

            for future in concurrent.futures.as_completed(future_to_addr):
                try:
                    reply = future.result()
                    successful_replies.append(reply)
                except Exception:
                    pass

        acks = len(successful_replies)
        if acks >= required_acks:
            return successful_replies[0].new_value, acks, successful_replies[0].was_duplicate
        else:
            raise RuntimeError(
                f"Quorum write failed: {acks}/{len(self.addresses)} replicas acknowledged (required {required_acks})")

    def get(self, counter_id: str, target_addr: str = None) -> tuple[int, bool]:
        addr = target_addr if target_addr else self.addresses[0]
        with grpc.insecure_channel(addr) as channel:
            stub = counter_pb2_grpc.CounterStub(channel)
            send_l = self.clock.send_event()
            req = counter_pb2.GetRequest(counter_id=counter_id, lamport_time=send_l)
            self.clock.log_event("SEND", f"Get(counter={counter_id})")

            reply = stub.Get(req, timeout=self.timeout)
            self.clock.receive_event(reply.lamport_time)
            self.clock.log_event("RECV", f"GetReply(value={reply.value}, found={reply.found})",
                                 received_l=reply.lamport_time)
            return reply.value, reply.found


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    incr_p = subparsers.add_parser("incr")
    incr_p.add_argument("counter_id", type=str)
    incr_p.add_argument("--by", type=int, default=1)
    incr_p.add_argument("--key", type=str, default=None)
    incr_p.add_argument("--single", action="store_true")
    incr_p.add_argument("--ports", type=str, default="50051,50052,50053")

    get_p = subparsers.add_parser("get")
    get_p.add_argument("counter_id", type=str)
    get_p.add_argument("--port", type=int, default=50051)

    args = parser.parse_args()

    if args.command == "incr":
        addrs = [f"localhost:{p.strip()}" for p in args.ports.split(",")]
        client = CounterClient(addrs)
        try:
            val, acks, is_dup = client.incr(args.counter_id, delta=args.by, key=args.key, use_quorum=not args.single)
            dup_str = "yes" if is_dup else "no"
            print(f"OK committed value={val} (replicas acked: {acks}/{len(addrs)}, duplicate: {dup_str})")
        except Exception as e:
            print(f"ERROR: {e}")
            sys.exit(1)

    elif args.command == "get":
        client = CounterClient([f"localhost:{args.port}"])
        try:
            val, found = client.get(args.counter_id)
            print(f"value={val}" if found else "value=0 (not found)")
        except Exception as e:
            print(f"ERROR: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()