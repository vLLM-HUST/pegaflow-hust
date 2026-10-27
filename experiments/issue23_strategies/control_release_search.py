"""Price controller completion before GPU admission using past completed CPU.

RPC constants are the retained initial calibration. The execution world and its
measured control costs are unchanged; this changes only the online forecast.
"""

from pegaflow.detached_calendar import token
from pegaflow.operation_schedule import Operation
from sparse_cache_search import (
    SparseCacheProjection,
    SparseCacheQueues,
    SparseCacheSimulation,
    SparseCacheSnapshot,
)


def completed_mean(rows, now):
    known = [row for row in rows if row["completed"] <= now]
    return {
        "mean": sum(row["seconds"] for row in known) / len(known) if known else 0.0,
        "samples": len(known),
        "latest": max((row["completed"] for row in known), default=None),
    }


class ControlReleaseProjection(SparseCacheProjection):
    def admit(self, work, node, previous, progress):
        operations = super().admit(work, node, previous, progress)
        if node < 2:
            return operations
        known = self.control_profile["active_D_remaining"]
        if work.key in known:
            seconds = known[work.key]
        elif work.d_started is None or work.key == self.boundary_request:
            seconds = self.control_profile["D_delay"]
        else:
            seconds = 0.0
        if seconds:
            # A D request owns its sequence slot during control but no device
            # gate; existing compute/copies may proceed during this interval.
            return (Operation("D_control", seconds, ()), *operations)
        return operations


class ControlReleaseSnapshot(SparseCacheSnapshot):
    projection_type = ControlReleaseProjection
    boundary_request = None

    def configure_projection(self, projection, now):
        projection.control_profile = self.control_profile
        projection.boundary_request = self.boundary_request
        return {
            "admission_releases": {
                "__candidate__": now + self.control_profile["P_delay"]
            }
        }

    def decode_boundary(self, key, *args, **kwargs):
        self.boundary_request = key
        try:
            return super().decode_boundary(key, *args, **kwargs)
        finally:
            self.boundary_request = None

    def forecast(self, route, now):
        selected = super().forecast(route, now)
        self.quoted_routes[token(selected)].update(
            control_release_profile=self.control_profile,
            current_decision_CPU_not_prospectively_predicted=False,
            control_CPU_estimator="mean of completed same-arm decisions; zero before observations",
        )
        return selected


class ControlReleaseQueues(SparseCacheQueues):
    def planning_snapshot(self):
        shadow = super().planning_snapshot()
        shadow.__class__ = ControlReleaseSnapshot
        shadow.control_profile = self.control_profile_reader()
        return shadow


class ControlReleaseSimulation(SparseCacheSimulation):
    queue_type = ControlReleaseQueues

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.boundary_control_history = []
        self.policy_queues.control_profile_reader = self.control_profile

    def control_profile(self):
        routing = completed_mean(
            [
                {
                    "seconds": r["CPU_wall"],
                    "completed": r["origin"] + r["CPU_wall"] + self.rpc_route,
                }
                for r in self.planning
            ],
            self.now,
        )
        boundary = completed_mean(self.boundary_control_history, self.now)
        active = {}
        for key, job in self.jobs.items():
            running = job.get("running")
            if running is not None and running[0] == "D_control":
                started = dict(job["work"].active_operations)["D_control"]
                active[key] = max(0.0, started + running[1] - self.now)
        return {
            "origin": self.now,
            "routing_CPU": routing,
            "boundary_CPU": boundary,
            "P_delay": self.rpc_route + routing["mean"],
            "D_delay": self.rpc_boundary + boundary["mean"],
            "active_D_remaining": active,
        }

    def stage(self, key, node):
        before = len(self.boundary_CPU)
        result = super().stage(key, node)
        if len(self.boundary_CPU) > before:
            seconds = self.boundary_CPU[-1]
            self.boundary_control_history.append(
                {
                    "key": key,
                    "seconds": seconds,
                    "completed": self.now + self.rpc_boundary + seconds,
                }
            )
        return result

    def run(self):
        result = super().run()
        result["boundary_control_history"] = self.boundary_control_history
        result["internal_candidate"].update(
            name="control-release-calendar-01",
            control_delay_projected_without_device_ownership=True,
            current_or_future_CPU_duration_not_used_online=True,
        )
        return result
