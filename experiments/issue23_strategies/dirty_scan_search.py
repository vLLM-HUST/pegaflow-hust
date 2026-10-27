"""Exact protection-delta scan without the costly physical-position index.

New publication uses the retained full reconciliation. Stable tuples need only
test dirty identities; no dirty lease transition requires no scan. This avoids
building an index whose reuse did not repay its cost in paired-state profiles.
"""

from dataclasses import replace

from delta_protection_search import DeltaProtectionProjection
from release_cobatch_search import (
    ReleaseCobatchQueues,
    ReleaseCobatchSimulation,
    ReleaseCobatchSnapshot,
)


class DirtyScanProjection(DeltaProtectionProjection):
    def sync_protection(self, node):
        original = self.state.layout.units[node]
        dirty = self.leases.dirty.setdefault(node, set())
        if self.protection_rows.get(node) is not original:
            ReleaseCobatchSnapshot.projection_type.sync_protection(self, node)
        elif dirty:
            updated = None
            for index, unit in enumerate(original):
                if unit.identity not in dirty:
                    continue
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
        self.protection_rows[node] = self.state.layout.units[node]
        dirty.clear()


class DirtyScanSnapshot(ReleaseCobatchSnapshot):
    projection_type = DirtyScanProjection


class DirtyScanQueues(ReleaseCobatchQueues):
    def planning_snapshot(self):
        shadow = super().planning_snapshot()
        shadow.__class__ = DirtyScanSnapshot
        return shadow


class DirtyScanSimulation(ReleaseCobatchSimulation):
    queue_type = DirtyScanQueues

    def run(self):
        result = super().run()
        result["internal_candidate"].update(
            name="dirty-scan-cobatch-01",
            protection_only_forecast_optimization=True,
            protection_position_index=False,
            fresh_publication_forces_full_protection_reconciliation=True,
        )
        return result
