"""Four independent FIFO device queues with no end-to-end admission slots."""

from concurrent.futures import Future, ThreadPoolExecutor
import threading
import time


class DevicePipeline:
    def __init__(self, started, clock=time.monotonic, observer=None):
        self.started, self.clock = started, clock
        self.observer = observer
        self.queues = tuple(ThreadPoolExecutor(max_workers=1, thread_name_prefix=name)
                            for name in ("P0", "P1", "D0", "D1"))
        self.guard = threading.Lock()
        self.counts = [0] * 4
        self.peaks = [0] * 4
        self.inflight = self.peak_inflight = 0
        self.closed = False

    def _notify(self, key, node, kind):
        if self.observer is not None:
            self.observer(key, node, kind, self.clock() - self.started)

    def _enqueue(self, node, function, timing, label, key):
        timing[label + "_enqueued_seconds"] = self.clock() - self.started
        self._notify(key, node, "enqueued")
        timing[label + "_enqueue_observer_return_seconds"] = self.clock() - self.started
        with self.guard:
            self.counts[node] += 1
            self.peaks[node] = max(self.peaks[node], self.counts[node])

        def run():
            timing[label + "_started_seconds"] = self.clock() - self.started
            self._notify(key, node, "started")
            timing[label + "_start_observer_return_seconds"] = self.clock() - self.started
            try:
                return function()
            finally:
                timing[label + "_finished_seconds"] = self.clock() - self.started
                self._notify(key, node, "finished")
                timing[label + "_finish_observer_return_seconds"] = self.clock() - self.started
                with self.guard:
                    self.counts[node] -= 1

        future = self.queues[node].submit(run)
        timing[label + "_executor_submit_return_seconds"] = self.clock() - self.started
        return future

    def submit(self, p, d, prefill, decode, *, key=None):
        if self.closed or p not in (0, 1) or d not in (2, 3):
            raise ValueError("An open P0/P1 -> D0/D1 pipeline is required")
        result, timing = Future(), {}
        with self.guard:
            self.inflight += 1
            self.peak_inflight = max(self.peak_inflight, self.inflight)

        def finished(future):
            try:
                result.set_result((future.result(), timing))
            except BaseException as error:
                result.set_exception(error)
            finally:
                with self.guard:
                    self.inflight -= 1

        def p_finished(future):
            try:
                value = future.result()
                # The P executor is free as soon as this callback submits D.
                self._enqueue(d, lambda: decode(value), timing, "D", key).add_done_callback(finished)
            except BaseException as error:
                self._notify(key, p, "cancelled")
                result.set_exception(error)
                with self.guard:
                    self.inflight -= 1

        self._enqueue(p, prefill, timing, "P", key).add_done_callback(p_finished)
        return result

    def close(self):
        self.closed = True
        # Drain P first: its callbacks may still append work to the D queues.
        for queue in self.queues:
            queue.shutdown(wait=True)
        return {"queue_peak_pending_including_active": self.peaks,
                "peak_inflight_requests": self.peak_inflight,
                "global_end_to_end_limit": None,
                "per_instance_active_limit": 1,
                "D_queue_includes_priced_handoff": True}
