"""Opt-in process-local JSON telemetry with bounded asynchronous writes."""

import atexit
import itertools
import json
import os
import queue
import socket
import threading
import time
from pathlib import Path


class TelemetrySink:
    def __init__(self, directory: str):
        self.pid = os.getpid()
        self.node = socket.gethostname()
        self.role = os.environ.get("PEGAFLOW_TELEMETRY_ROLE", "connector")
        self.path = Path(directory) / f"{self.role}-{self.node}-{self.pid}.jsonl"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.queue = queue.Queue(maxsize=16384)
        self.sequence = itertools.count()
        self.dropped = 0
        self.error = None
        self.thread = threading.Thread(target=self._write, name="pegaflow-telemetry", daemon=True)
        self.thread.start()
        atexit.register(self.close)

    def emit(self, event: str, **fields):
        record = {
            "schema_version": 1,
            "event": event,
            "sequence": next(self.sequence),
            "unix_ns": time.time_ns(),
            "monotonic_ns": time.monotonic_ns(),
            "pid": self.pid,
            "node": self.node,
            "role": self.role,
            **fields,
        }
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            self.dropped += 1

    def _write(self):
        try:
            with self.path.open("a", encoding="utf-8", buffering=65536) as stream:
                while True:
                    record = self.queue.get()
                    if record is None:
                        stream.write(
                            json.dumps(
                                {
                                    "event": "telemetry_closed",
                                    "pid": self.pid,
                                    "dropped_events": self.dropped,
                                    "unix_ns": time.time_ns(),
                                }
                            )
                            + "\n"
                        )
                        stream.flush()
                        self.queue.task_done()
                        return
                    batch = [record]
                    while len(batch) < 256:
                        try:
                            item = self.queue.get_nowait()
                        except queue.Empty:
                            break
                        if item is None:
                            self.queue.task_done()
                            self.queue.put_nowait(None)
                            break
                        batch.append(item)
                    for item in batch:
                        stream.write(json.dumps(item, separators=(",", ":")) + "\n")
                        self.queue.task_done()
                    stream.flush()
        except Exception as error:
            self.error = f"{type(error).__name__}: {error}"

    def close(self):
        if self.thread.is_alive():
            try:
                self.queue.put(None, timeout=2)
            except queue.Full:
                return
            self.thread.join(timeout=5)


_sink = None


def telemetry_enabled() -> bool:
    return bool(os.environ.get("PEGAFLOW_TELEMETRY_DIR"))


def telemetry_event(event: str, **fields):
    global _sink
    directory = os.environ.get("PEGAFLOW_TELEMETRY_DIR")
    if not directory:
        return
    if _sink is None or _sink.pid != os.getpid():
        _sink = TelemetrySink(directory)
    _sink.emit(event, **fields)
