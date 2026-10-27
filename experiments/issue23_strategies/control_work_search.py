"""Nonnegative, past-completed controller CPU fit from visible search work."""

import itertools
import math
import time

import numpy as np
from control_release_search import (
    ControlReleaseQueues,
    ControlReleaseSimulation,
    ControlReleaseSnapshot,
)
from pegaflow.detached_calendar import token


def fit_control(rows, features, now):
    known = [r for r in rows if r["completed"] <= now]
    result = {
        "samples": len(known),
        "latest": max((r["completed"] for r in known), default=None),
        "features": tuple(features),
        "coefficients": None,
    }
    mean = sum(r["seconds"] for r in known) / len(known) if known else 0.0
    x = np.array([r["features"] for r in known], dtype=float)
    if not known or np.linalg.matrix_rank(x) < len(features):
        return dict(result, predicted=mean, method="mean_until_identifiable")
    y = np.array([r["seconds"] for r in known])
    scale = np.linalg.norm(x, axis=0)
    x = x / scale
    best = float(y @ y), np.zeros(len(features))
    # With three physically motivated features, enumerate all NNLS active sets;
    # no custom tolerance, ridge coefficient or performance-fitted threshold.
    for size in range(1, len(features) + 1):
        for columns in itertools.combinations(range(len(features)), size):
            fitted = np.linalg.lstsq(x[:, columns], y, rcond=None)[0]
            if (fitted < 0).any():
                continue
            coefficients = np.zeros(len(features))
            coefficients[list(columns)] = fitted
            residual = y - x @ coefficients
            error = float(residual @ residual)
            if error < best[0]:
                best = error, coefficients
    coefficients = best[1] / scale
    return dict(
        result,
        coefficients=coefficients.tolist(),
        predicted=float(coefficients @ np.array(features)),
        method="nonnegative_visible_work",
    )


class ControlWorkSnapshot(ControlReleaseSnapshot):
    def forecast(self, route, now):
        selected = super().forecast(route, now)
        self.quoted_routes[token(selected)]["control_CPU_estimator"] = (
            "same-arm completed NNLS over visible work; mean before identifiable"
        )
        return selected


class ControlWorkQueues(ControlReleaseQueues):
    def planning_snapshot(self):
        shadow = super().planning_snapshot()
        shadow.__class__ = ControlWorkSnapshot
        return shadow


class ControlWorkSimulation(ControlReleaseSimulation):
    queue_type = ControlWorkQueues

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.route_feature_history, self.boundary_feature_history = [], []
        self.current_feature_row, self.last_work_features = None, None

    def visible_work(self):
        progress = self.progress()
        units = sum(len(node) for node in self.layout.units)
        quanta = 0
        block = self.batch_config.block_tokens
        for key, job in self.jobs.items():
            if key in progress:
                seq = progress[key]
                quanta += math.ceil(seq.remaining_prefill / block)
                quanta += seq.output_tokens - seq.decoded + 2
            else:
                route = job["live_route"]
                p_prefix = route.p_blocks * block if route else 0
                d_prefix = route.d_blocks * block if route else 0
                work = self.policy_queues.work.get(key)
                if work is None or work.p_done is None:
                    quanta += math.ceil((job["row"]["tokens"] - p_prefix) / block) + 2
                quanta += math.ceil((job["row"]["tokens"] - d_prefix) / block) + 130
        count = len(self.jobs)
        if self.current_feature_row and self.current_feature_row["id"] not in self.jobs:
            quanta += 2 * math.ceil(self.current_feature_row["tokens"] / block) + 132
            count += 1
        # 128 decode steps and restore/save phase boundaries follow the frozen
        # execution contract. The coefficients learn CPU seconds, not latency gain.
        return (1.0, float(quanta), float(units * count))

    def control_profile(self):
        profile = super().control_profile()
        features = self.visible_work()
        self.last_work_features = features
        route = fit_control(self.route_feature_history, features, self.now)
        boundary = fit_control(self.boundary_feature_history, features, self.now)
        profile.update(
            work_features=features,
            routing_predictor=route,
            boundary_predictor=boundary,
            P_delay=self.rpc_route + route["predicted"],
            D_delay=self.rpc_boundary + boundary["predicted"],
        )
        return profile

    def choose(self, row, ready):
        began = time.perf_counter()
        self.current_feature_row = row
        try:
            plan, work, _, route = super().choose(row, ready)
        finally:
            self.current_feature_row = None
        observed = {"features": self.last_work_features}
        self.route_feature_history.append(observed)
        cpu = time.perf_counter() - began
        observed.update(seconds=cpu, completed=self.now + cpu + self.rpc_route)
        self.planning[-1]["CPU_wall"] = cpu
        return plan, work, cpu, route

    def stage(self, key, node):
        before = len(self.boundary_control_history)
        result = super().stage(key, node)
        if len(self.boundary_control_history) > before:
            self.boundary_feature_history.append(
                {
                    **self.boundary_control_history[-1],
                    "features": self.last_work_features,
                }
            )
        return result

    def run(self):
        result = super().run()
        result["control_work_observations"] = {
            "route": self.route_feature_history,
            "boundary": self.boundary_feature_history,
        }
        result["internal_candidate"]["name"] = "control-visible-work-01"
        return result
