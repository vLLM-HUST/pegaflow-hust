"""Exact lease-state updates without rebuilding unchanged physical allocations.

The immutable layout remains the cache-index identity. No hash-only shortcut,
policy change, relaxed lease check or future observation is introduced.
"""

from dataclasses import replace

from cache_evolving_search import (
    CacheEvolvingQueues,
    CacheEvolvingSimulation,
    CacheEvolvingSnapshot,
    StageCacheProjection,
)


class SparseCacheProjection(StageCacheProjection):
    def sync_protection(self, node):
        original = self.state.layout.units[node]
        updated = None
        for index, unit in enumerate(original):
            protected = (
                node,
                unit.identity,
            ) in self.intrinsic or self.leases.protected(node, unit.identity)
            if protected != unit.protected:
                if updated is None:
                    updated = list(original)
                updated[index] = replace(unit, protected=protected)
        if updated is not None:
            nodes = list(self.state.layout.units)
            nodes[node] = tuple(updated)
            self.state.layout = replace(self.state.layout, units=tuple(nodes))


class SparseCacheSnapshot(CacheEvolvingSnapshot):
    projection_type = SparseCacheProjection


class SparseCacheQueues(CacheEvolvingQueues):
    def planning_snapshot(self):
        shadow = super().planning_snapshot()
        shadow.__class__ = SparseCacheSnapshot
        return shadow


class SparseCacheSimulation(CacheEvolvingSimulation):
    queue_type = SparseCacheQueues

    def run(self):
        result = super().run()
        result["internal_candidate"].update(
            name="sparse-cache-protection-01",
            unchanged_allocation_identity_preserved=True,
            scheduling_semantics_unchanged=True,
        )
        return result
