"""Same native commit contracts, fewer inventory RPCs, explicit copy progress."""

from __future__ import annotations

import contextlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from pegaflow.joint_policy import Layout, View, copy_package
from pegaflow.joint_runtime import Backend, CommitConflict
from pegaflow.resource_queue import direction


class ResourceBackend(Backend):
    def __init__(self, *args):
        super().__init__(*args)
        self.inventory_pool = ThreadPoolExecutor(max_workers=len(self.instances))
        self.latest_views, self.latest_lock = None, threading.Lock()
        self.context = threading.local()
        self.started = None

    def views(self):
        started = time.monotonic()
        payloads = list(self.inventory_pool.map(self.inventory, range(len(self.instances))))
        views = tuple(View.parse(node, value) for node, value in enumerate(payloads))
        with self.latest_lock:
            self.latest_views = views
        self.record(dict(kind="inventory_read", seconds=time.monotonic() - started,
                         nodes=list(range(len(self.instances))), planning_only=False))
        return views

    def cached_views(self):
        with self.latest_lock:
            return self.latest_views

    def close(self):
        self.inventory_pool.shutdown(wait=True)

    @contextlib.contextmanager
    def copy_context(self, queues, key, name):
        self.context.value = queues, key, name
        try:
            yield
        finally:
            self.context.value = None

    def fetch_prefix(self, source, destination, hashes):
        started = time.monotonic()
        context = getattr(self.context, "value", None)

        def skip():
            if context is not None:
                context[0].skip_copy(context[1], context[2])

        if source == destination or not hashes:
            skip()
            return dict(status="resident", seconds=time.monotonic() - started)
        destination_view = View.parse(destination, self.inventory(destination))
        if set(hashes) <= {key for unit in destination_view.units for key in unit.keys}:
            # No copy is performed. Do not borrow a source gate merely to
            # discover a prefix already present on the destination.
            skip()
            return dict(status="resident", seconds=time.monotonic() - started)
        with self.locked((source, destination)):
            acquired = time.monotonic()
            resources = (f"gate{source}", f"gate{destination}", "link:" + direction(source, destination))
            if context is not None:
                context[0].operation(context[1], context[2], "start", acquired - self.started, resources)
            try:
                nodes = (source, destination)
                payloads = list(self.inventory_pool.map(self.inventory, nodes))
                views = list(self.cached_views() or self.views())
                for node, payload in zip(nodes, payloads, strict=True):
                    views[node] = View.parse(node, payload)
                with self.latest_lock:
                    self.latest_views = tuple(views)
                layout = Layout.current(views)
                if set(hashes) <= layout.keys(destination):
                    return dict(status="resident", seconds=time.monotonic() - started)
                try:
                    transition = copy_package(layout, source, destination, hashes)
                except ValueError as error:
                    raise CommitConflict(str(error)) from error
                receipt = self.copy(transition.operations[-1], payloads[1])
                finished = time.monotonic()
                with self.events_lock:
                    copy_event = next(e for e in reversed(self.events) if e.get("kind") == "copy"
                        and e["operation"]["source"] == source and e["operation"]["destination"] == destination)
                held = finished - acquired
                self.record(dict(kind="owned_copy_resource", source=source, destination=destination,
                    gate_wait_seconds=acquired-started, held_seconds=held,
                    link_wait_seconds=copy_event["link_wait_seconds"], payload_bytes=receipt["payload_bytes"],
                    operation_start_seconds=None if self.started is None else acquired-self.started,
                    operation_end_seconds=None if self.started is None else finished-self.started))
                if context is not None:
                    context[0].models.observe_copy(source, destination, receipt["payload_bytes"], held,
                        copy_event["link_wait_seconds"], finished-self.started)
                return dict(status="copied", seconds=finished-started, receipt=receipt)
            finally:
                if context is not None:
                    context[0].operation(context[1], context[2], "finish", time.monotonic()-self.started)
