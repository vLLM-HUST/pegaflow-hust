"""Decision-local exact memoization; no service bound or candidate pruning."""

import simulate_continuous_batch as base
from batch_calendar import BatchedQueues, BatchedSnapshot
from pegaflow.resource_cache_profile import CacheProfileQueues


class MemoSnapshot(BatchedSnapshot):
    def reference(self):
        shadow = object.__new__(BatchedSnapshot)
        shadow.__dict__ = self.__dict__.copy()
        shadow.work = dict(self.work)
        shadow.routes = dict(self.routes)
        shadow.admission_choices = dict(self.admission_choices)
        shadow.quoted_routes = {}
        shadow.batch_memo = {}
        return shadow

    def refresh(self, layout, costs):
        identity = id(layout), id(costs)
        if getattr(self, "refreshed_identity", None) == identity:
            self.search_stats["refresh_reused"] += 1
            return
        super().refresh(layout, costs)
        self.refreshed_identity = identity
        self.search_stats["refresh_full"] += 1

    def forecast(self, route, now):
        reference = (
            self.reference() if getattr(self, "verify_reference", False) else None
        )
        self.search_stats["considered"] += 1
        # Full immutable raw candidate equality: preserve extra bytes and all
        # tie breakers, rather than guessing that an unused source is equivalent.
        memo = self.__dict__.setdefault("route_memo", {})
        if route in memo:
            self.search_stats["route_reused"] += 1
            selected = memo[route]
        else:
            selected = super().forecast(route, now)
            memo[route] = selected
            self.search_stats["full"] += 1
        if reference is not None and selected != reference.forecast(route, now):
            raise ValueError("Memo route differs from a fresh full forecast")
        return selected

    def decode_boundary(self, *args, **kwargs):
        reference = (
            self.reference() if getattr(self, "verify_reference", False) else None
        )
        answer = super().decode_boundary(*args, **kwargs)
        if reference is not None and answer != reference.decode_boundary(
            *args, **kwargs
        ):
            raise ValueError("Memo boundary differs from a fresh full forecast")
        return answer

    def predict_batch(self, *args, **kwargs):
        before = len(getattr(self, "batch_memo", {}))
        result = super().predict_batch(*args, **kwargs)
        name = "calendar_reused" if len(self.batch_memo) == before else "calendar_full"
        self.search_stats[name] += 1
        return result


class MemoQueues(BatchedQueues):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.search_records = []

    def planning_snapshot(self):
        # Every immutable decision owns a fresh memo. Never share it with a
        # later cost version, native generation or token-progress snapshot.
        shadow = BatchedQueues.planning_snapshot(self)
        shadow.__class__ = MemoSnapshot
        shadow.verify_reference = getattr(self, "verify_reference", False)
        shadow.search_stats = {
            key: 0
            for key in (
                "considered",
                "full",
                "refresh_reused",
                "refresh_full",
                "route_reused",
                "calendar_reused",
                "calendar_full",
            )
        }
        return shadow

    def reserve(self, key, route, now):
        self.search_records.append(
            dict(kind="route", key=key, **self.local_decision.shadow.search_stats)
        )
        return super().reserve(key, route, now)

    def decode_boundary(self, key, route, layout, costs, now, max_candidates=64):
        shadow = self.planning_snapshot()
        answer = shadow.decode_boundary(key, route, layout, costs, now, max_candidates)
        # Same owned-suffix-only commit as the retained detached controller.
        with self.guard:
            current = self.work[key]
            if (
                "D_api" in dict(current.active_operations)
                or "D_api" in current.completed_operations
            ):
                raise ValueError("Cannot revise an acquired or completed D API")
            version = self.progress_version
            self.admission_choices[key, route.d] = shadow.admission_choices[
                key, route.d
            ]
            self.last_boundary_estimate = shadow.last_boundary_estimate
            CacheProfileQueues.revise(self, key, shadow.routes[key], layout, costs)
            self.progress_version += 1
            self.detached_decisions.append(
                {
                    "kind": "boundary",
                    "key": key,
                    "snapshot_version": shadow.progress_version,
                    "commit_version": version,
                    "progress_changed": version != shadow.progress_version,
                    "native_reservation_claimed": False,
                }
            )
        self.search_records.append(
            dict(kind="boundary", key=key, **shadow.search_stats)
        )
        return answer


class MemoSimulation(base.BatchSimulation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.policy_queues = MemoQueues(
            self.models,
            512,
            memoize=True,
            index_projection=True,
            predict_control=False,
            batch_config=self.batch_config,
            progress=self.progress,
        )
        self.policy_queues.started = 0.0

    def run(self):
        result = super().run()
        result["search_records"] = self.policy_queues.search_records
        result["decision_local_memo"] = True
        result["candidate_pruning"] = False
        return result
