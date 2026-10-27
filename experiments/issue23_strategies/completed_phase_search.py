"""Learn only completed exclusive D phases, never batched API residence.

The simulator's completed iteration trace substitutes for future instrumented
serving timers. No sampled execution-world duration is accessed by this policy.
"""

import copy
import math
import time
from dataclasses import dataclass

from control_work_search import ControlWorkSimulation


def exclusive_phase_observation(trace, cpu, key, node, now):
    values = {}
    intervals = []
    for kind in ("restore", "save"):
        selected = [
            e
            for e in trace
            if e["node"] == node
            and e["kind"] == kind
            and key in [p[0] for p in e["pieces"]]
            and e["end"] <= now
        ]
        if len(selected) != 1 or len(selected[0]["pieces"]) != 1:
            raise ValueError("One completed exclusive owned phase required")
        event = selected[0]
        charged = [
            e
            for e in cpu
            if e["node"] == node
            and e["kind"] == kind
            and e["origin"] == event["start"]
            and e["requests"] == [key]
            and e["completed"] <= now
        ]
        if len(charged) != 1:
            raise ValueError("Completed controller cost must reconcile once")
        elapsed = event["end"] - event["start"] - charged[0]["seconds"]
        # Floating-point subtraction at a later simulated clock can lose ulps.
        if elapsed < -8 * math.ulp(max(1.0, event["end"])):
            raise ValueError("Controller cost exceeded the exclusive interval")
        values[kind] = max(0.0, elapsed)
        intervals.append(
            {
                "kind": kind,
                "start": event["start"],
                "end": event["end"],
                "cpu": charged[0]["seconds"],
            }
        )
    return dict(**values, intervals=intervals, completed=now, key=key, node=node)


@dataclass(frozen=True)
class CompletedPhaseCosts:
    current: object
    phase_fits: tuple
    phase_latest: float
    phase_version: int

    def __getattr__(self, name):
        return getattr(self.current, name)

    @property
    def fits(self):
        return tuple(
            (self.phase_fits[n - 2][0], *old[1:4], self.phase_fits[n - 2][1])
            if n >= 2
            else old
            for n, old in enumerate(self.current.fits)
        )

    @property
    def latest_completed_seconds(self):
        return max(self.current.latest_completed_seconds, self.phase_latest)

    @property
    def version(self):
        return self.current.version + self.phase_version

    def restore(self, node, prefix):
        if node < 2 or not prefix:
            return self.current.restore(node, prefix)
        coefficients = self.phase_fits[node - 2][0]["coefficients"]
        if coefficients is None:
            return self.current.restore(node, prefix)
        return coefficients[0] + coefficients[1] * prefix * self.bytes_per_token

    def compute(self, node, tokens, prefix):
        return (
            self.current.compute(node, tokens, prefix)
            - self.current.restore(node, prefix)
            + self.restore(node, prefix)
        )

    def decode_tail(self, node, tokens):
        return (
            max(
                0.0, self.current.decode_tail(node, tokens) - self.current.fits[node][4]
            )
            + self.fits[node][4]
        )

    @property
    def decode_seconds(self):
        return tuple(
            max(0.0, self.current.decode_seconds[n - 2] - self.current.fits[n][4])
            + self.fits[n][4]
            for n in (2, 3)
        )


class CompletedPhaseModels:
    def __init__(self, current):
        self.current = current
        self.restore_models = [copy.deepcopy(current.restore[n]) for n in (2, 3)]
        self.finish_totals = [
            [current.control[n][0], current.control[n][3]] for n in (2, 3)
        ]
        self.exclusive_records = []
        self.phase_latest = 0.0

    def __getattr__(self, name):
        return getattr(self.current, name)

    def observe_exclusive(self, record):
        node, prefix = record["node"], record["prefix"]
        if node not in (2, 3) or record["completed"] < self.phase_latest:
            raise ValueError("Ordered completed D phases required")
        if prefix:
            self.restore_models[node - 2].add(
                (1.0, prefix * self.current.original.bytes_per_token), record["restore"]
            )
        total = self.finish_totals[node - 2]
        total[0] += 1
        total[1] += record["save"]
        self.phase_latest = record["completed"]
        self.exclusive_records.append(record)

    def freeze(self, now):
        if self.phase_latest > now:
            raise ValueError("Future D phase cannot train the forecast")
        return CompletedPhaseCosts(
            self.current.freeze(now),
            tuple(
                (model.snapshot(), total / count if count else 0.0)
                for model, (count, total) in zip(
                    self.restore_models, self.finish_totals, strict=True
                )
            ),
            self.phase_latest,
            len(self.exclusive_records),
        )


class CompletedPhaseSimulation(ControlWorkSimulation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.mode != "native":
            began = time.perf_counter()
            self.models = CompletedPhaseModels(self.models)
            self.policy_queues.models = self.models
            elapsed = time.perf_counter() - began
            self.fit_CPU.append(elapsed)
            self.now += elapsed

    def finish_d(self, key):
        if self.mode == "native":
            return super().finish_d(key)
        job = self.jobs[key]
        node, prefix = job["node"], job["count"] * 128
        # Normal release and request completion happen before model maintenance.
        super().finish_d(key)
        began = time.perf_counter()
        record = exclusive_phase_observation(
            self.batch_trace, self.iteration_cpu, key, node, self.now
        )
        record["prefix"] = prefix
        self.models.observe_exclusive(record)
        elapsed = time.perf_counter() - began
        self.fit_CPU.append(elapsed)
        self.now += elapsed
        record["maintenance_CPU"] = elapsed
        record["available"] = self.now

    def run(self):
        result = super().run()
        result["internal_candidate"]["name"] = "completed-exclusive-D-01"
        result["exclusive_D_observations"] = (
            self.models.exclusive_records if self.mode != "native" else []
        )
        result["phase_feedback_uses_world_labels"] = False
        return result
