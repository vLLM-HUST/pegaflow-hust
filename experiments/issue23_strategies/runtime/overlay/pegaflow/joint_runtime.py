"""Real local commits and explicit-source copies for the four-instance policy."""

from __future__ import annotations

import contextlib
import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import asdict

from pegaflow.joint_policy import Copy, Layout, Remove, View, copy_package, execute_plan
from pegaflow.placement_executor import RecoveryPrefix


class CommitConflict(RuntimeError):
    """An exact inventory or capacity contract changed; never blindly retry."""


def json_rpc(url, body=None, timeout=60):
    request = urllib.request.Request(url, data=None if body is None else json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        if error.code == 409:
            raise CommitConflict(error.read().decode()) from error
        raise


class Backend:
    def __init__(self, cache_urls, grpc_addresses, instances):
        count = len(instances)
        if count not in (2, 4) or not all(len(values) == count for values in (cache_urls, grpc_addresses)):
            raise ValueError("Two P calibration or four formal instances required")
        if any(not isinstance(value, str) or not value for values in (cache_urls, grpc_addresses, instances) for value in values):
            raise ValueError("Every configured instance must be actually registered")
        self.cache_urls, self.grpc_addresses, self.instances = cache_urls, grpc_addresses, instances
        self.gates = tuple(threading.RLock() for _ in instances)
        self.links = {name: threading.Lock() for name in ("pd", "dp", "P", "D")}
        self.events, self.events_lock = [], threading.Lock()

    def inventory(self, node):
        query = urllib.parse.urlencode({"instance_id": self.instances[node]})
        return json_rpc(self.cache_urls[node] + "/cache/placement/inventory?" + query, timeout=10)

    def views(self):
        return tuple(View.parse(node, self.inventory(node)) for node in range(len(self.instances)))

    def record(self, event):
        with self.events_lock:
            self.events.append(event)
        return event

    def rdma_bytes(self, node):
        with urllib.request.urlopen(self.cache_urls[node] + "/metrics", timeout=10) as response:
            content = response.read().decode()
        return int(sum(float(line.split()[-1]) for line in content.splitlines()
                       if line.startswith("pegaflow_rdma_fetch_bytes") and 'status="ok"' in line))

    @contextlib.contextmanager
    def locked(self, nodes, *, blocking=True):
        acquired = []
        try:
            ordered = sorted(set(nodes))
            while True:
                for node in ordered:
                    if not self.gates[node].acquire(blocking=False):
                        break
                    acquired.append(node)
                if len(acquired) == len(ordered):
                    break
                for node in reversed(acquired):
                    self.gates[node].release()
                acquired.clear()
                if not blocking:
                    raise CommitConflict("Owned inference/copy is in flight; defer placement")
                # Never hold a P gate while waiting for a busy D (or reverse).
                time.sleep(0.005)
            yield
        finally:
            for node in reversed(acquired):
                self.gates[node].release()

    def copy(self, operation, current):
        source, destination = operation.source, operation.destination
        source_view = View.parse(source, self.inventory(source))
        if not set(operation.hashes) <= {key for unit in source_view.units for key in unit.keys}:
            raise CommitConflict("Explicit source no longer has a complete recovery package")
        request = {"instance_id": self.instances[destination], "epoch": current["cache"]["epoch"],
                   "generation": current["cache"]["generation"],
                   "remote_addr": self.grpc_addresses[source], "request_id": "joint-" + uuid.uuid4().hex,
                   "prefix": [list(value) for value in operation.hashes],
                   "max_allocation_bytes": operation.allocation_bound}
        direction = "pd" if source < 2 <= destination else "dp" if destination < 2 <= source else "P" if source < 2 else "D"
        before = time.monotonic()
        with self.links[direction]:
            lock_seconds = time.monotonic() - before
            started = time.monotonic()
            counter_before = self.rdma_bytes(destination)
            try:
                receipt = json_rpc(self.cache_urls[destination] + "/cache/placement/copy", request)
            except BaseException as error:
                self.record({"kind": "copy_failed", "request": request, "operation": asdict(operation),
                             "rpc_seconds": time.monotonic() - started,
                             "actual_rdma_bytes": self.rdma_bytes(destination) - counter_before,
                             "direction": direction, "error": str(error), "source_preserved": True})
                raise
            actual_rdma = self.rdma_bytes(destination) - counter_before
            seconds = time.monotonic() - started
        actual = {(key["namespace"], bytes(key["hash"])) for key in receipt["published_keys"]}
        expected = {(current["namespace"], value) for value in operation.missing}
        if (receipt["source"] != request["remote_addr"] or actual != expected
            or len(actual) != len(receipt["published_keys"])
            or receipt["generation_before"] != request["generation"]
            or receipt["epoch"] != request["epoch"]
            or receipt["generation_after"] != request["generation"] + 1
            or receipt["payload_bytes"] != operation.payload_bytes
            or actual_rdma != receipt["payload_bytes"]
            or not 0 < receipt["allocated_bytes"] <= operation.allocation_bound
            or receipt["staging_peak_bytes"] != receipt["allocated_bytes"]
            or receipt["published_locally"] is not True
            or receipt["allocated_before_rdma"] is not True
            or receipt["distributed_atomicity_claimed"] is not False):
            raise ValueError("Unverified copy outcome; do not retry or delete source")
        after = View.parse(destination, self.inventory(destination))
        if not set(operation.hashes) <= {key for unit in after.units for key in unit.keys}:
            raise ValueError("Published package is not completely visible; retain source")
        self.record({"kind": "copy", "operation": asdict(operation), "request": request,
                     "receipt": receipt, "rpc_seconds": seconds, "link_wait_seconds": lock_seconds,
                     "actual_rdma_bytes": actual_rdma,
                     "source_preserved": True, "direction": direction})
        return receipt

    def remove(self, operation, current, final):
        snapshot = View.parse(operation.node, current).snapshot
        selected = set(operation.identities)
        if selected - {unit.key for unit in snapshot.units}:
            raise CommitConflict("Physical owner closure changed")
        states = {unit.key: 0 if unit.key in selected else snapshot.host for unit in snapshot.units}
        # Validate every known retained complete prefix separately in the native
        # endpoint. Unknown partial objects are not falsely called packages.
        prefixes = getattr(self, "retained_prefixes", ())
        request = snapshot.eviction(states, prefixes)
        if request is None:
            raise CommitConflict("Selected owner closure no longer exists")
        started = time.monotonic()
        receipt = json_rpc(self.cache_urls[operation.node] + "/cache/placement/evict", request)
        snapshot.verify_receipt(request, receipt)
        self.record({"kind": "remove", "operation": asdict(operation), "request": request,
                     "receipt": receipt, "rpc_seconds": time.monotonic() - started})
        return receipt

    def apply(self, plan, views, history):
        nodes = set()
        for operation in plan.transition.operations:
            nodes.update((operation.source, operation.destination) if isinstance(operation, Copy) else (operation.node,))
        started = time.monotonic()
        try:
            with self.locked(nodes, blocking=False):
                self.retained_prefixes = tuple(RecoveryPrefix(d.key, d.hashes) for d in history if d.hashes)
                result = execute_plan(plan, views, self)
        except CommitConflict as error:
            result = {"status": "deferred", "reason": str(error), "distributed_atomicity_claimed": False}
        result["execution_seconds"] = time.monotonic() - started
        return result

    def fetch_prefix(self, source, destination, hashes):
        started = time.monotonic()
        if source == destination or not hashes:
            return {"status": "resident", "seconds": time.monotonic() - started}
        with self.locked((source, destination)):
            views = self.views()
            layout = Layout.current(views)
            missing = set(hashes) - layout.keys(destination)
            if not missing:
                return {"status": "resident", "seconds": time.monotonic() - started}
            try:
                transition = copy_package(layout, source, destination, hashes)
            except ValueError as error:
                raise CommitConflict(str(error)) from error
            receipt = self.copy(transition.operations[-1], self.inventory(destination))
            return {"status": "copied", "seconds": time.monotonic() - started, "receipt": receipt}
