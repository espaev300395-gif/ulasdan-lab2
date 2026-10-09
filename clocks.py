import threading
import logging


class LamportClock:
    """
    Thread-safe implementation of Lamport Logical Clocks.
    - Local / Send event: L = L + 1
    - Message Receive: L = max(L, L_received) + 1
    """

    def __init__(self, process_id: str):
        self.process_id = process_id
        self.value = 0
        self._lock = threading.Lock()

    def send_event(self) -> int:
        with self._lock:
            self.value += 1
            return self.value

    def receive_event(self, received_time: int) -> int:
        with self._lock:
            self.value = max(self.value, received_time) + 1
            return self.value

    def get_time(self) -> int:
        with self._lock:
            return self.value

    def log_event(self, event_type: str, details: str, received_l: int = None) -> str:
        curr_l = self.get_time()
        if received_l is not None:
            msg = f"[{self.process_id:<9}] {event_type:<5} {details:<48} L={curr_l:<3} (received L={received_l})"
        else:
            msg = f"[{self.process_id:<9}] {event_type:<5} {details:<48} L={curr_l:<3}"

        logging.info(msg)
        return msg