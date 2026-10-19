"""Short live-state transactions; forecasts never own the progress mutex.

Snapshots are advisory, not native cache reservations. A fast decision may
become stale while it is computed; native source/lease/generation/staging and
the actual post-P boundary remain authoritative. Only the newly reserved job
or the unstarted suffix of the owned D job is published back to live state.
"""

from __future__ import annotations

import contextlib
import copy
from dataclasses import replace
import threading

from pegaflow.resource_cache_profile import CacheProfileQueues


class DetachedQueues(CacheProfileQueues):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.local_decision = threading.local()
        self.detached_decisions = []
        self.placement_blocks = {}
        if not hasattr(self, "selected_quotes"):
            self.selected_quotes = {}

    def planning_snapshot(self):
        """O(live jobs + completed samples), never copy the growing event log."""
        with self.guard:
            shadow = object.__new__(CacheProfileQueues)
            shadow.__dict__ = self.__dict__.copy()
            shadow.guard = threading.RLock()
            shadow.jobs = {key: replace(job) for key, job in self.jobs.items()}
            for name in ("work", "demands", "routes", "admission_choices", "selected_quotes"):
                setattr(shadow, name, dict(getattr(self, name)))
            shadow.completed = [list(values) for values in self.completed]
            shadow.completed_first = [list(values) for values in self.completed_first]
            shadow.operation_samples = {key: tuple(values)
                                        for key, values in self.operation_samples.items()}
            shadow.events, shadow.operation_events = [], []
            shadow.projection_memo, shadow.context = None, None
            shadow.memo_totals = dict(hits=0, misses=0, peak_entries=0)
            shadow.index_totals = dict(hits=0, peak_entries=0)
            shadow.control_costs = copy.deepcopy(self.control_costs)
            shadow.quoted_routes = {}
            shadow.placement_blocks = dict(self.placement_blocks)
            return shadow

    @contextlib.contextmanager
    def decision(self, demand, layout, costs, offered, now):
        if getattr(self.local_decision, "shadow", None) is not None:
            raise ValueError("Nested detached route decision")
        shadow = self.planning_snapshot()
        self.local_decision.shadow = shadow
        try:
            with shadow.decision(demand, layout, costs, offered, now):
                yield
        finally:
            self.local_decision.shadow = None

    def forecast(self, route, now):
        shadow = getattr(self.local_decision, "shadow", None)
        if shadow is None:
            raise ValueError("A route forecast requires a detached decision scope")
        return shadow.forecast(route, now)

    def reserve(self, key, route, now):
        shadow = getattr(self.local_decision, "shadow", None)
        if shadow is None:
            raise ValueError("Reservation requires its own detached forecast")
        with self.guard:
            version = self.progress_version
            self.context = shadow.context
            self.quoted_routes = shadow.quoted_routes
            self.last_route_estimate = shadow.last_route_estimate
            try:
                super().reserve(key, route, now)
            finally:
                self.context, self.quoted_routes = None, {}
            self.detached_decisions.append(dict(kind="route", key=key,
                snapshot_version=shadow.progress_version, commit_version=version,
                progress_changed=version != shadow.progress_version,
                native_reservation_claimed=False))

    def decode_boundary(self, key, route, layout, costs, now, max_candidates=64):
        shadow = self.planning_snapshot()
        answer = shadow.decode_boundary(key, route, layout, costs, now, max_candidates)
        option = shadow.routes[key]
        with self.guard:
            current = self.work[key]
            if "D_api" in dict(current.active_operations) or "D_api" in current.completed_operations:
                raise ValueError("Cannot revise an acquired or completed D API")
            version = self.progress_version
            self.admission_choices[key, route.d] = shadow.admission_choices[key, route.d]
            self.last_boundary_estimate = shadow.last_boundary_estimate
            super().revise(key, option, layout, costs)
            self.progress_version += 1
            self.detached_decisions.append(dict(kind="boundary", key=key,
                snapshot_version=shadow.progress_version, commit_version=version,
                progress_changed=version != shadow.progress_version,
                native_reservation_claimed=False))
        return answer

    def snapshot(self):
        with self.guard:
            result = super().snapshot()
            result.update(detached_decisions=list(self.detached_decisions),
                expensive_forecast_holds_live_progress_mutex=False,
                snapshots_do_not_replace_other_live_jobs=True)
            return result
