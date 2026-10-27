"""Exact lease protection deltas inside an immutable forecast snapshot.

Publication invalidates the node tuple and forces a full reconciliation. Stable
tuples revisit only physical identities whose lease count crossed zero. No cache
capacity, lease, routing, objective or scheduling rule is relaxed.
"""

from dataclasses import replace

from pegaflow.strategies.continuous_batch import AllocationLeases
from release_cobatch_search import (
    ReleaseCobatchQueues,
    ReleaseCobatchSimulation,
    ReleaseCobatchSnapshot,
)


class DeltaLeases(AllocationLeases):
    def __init__(self):
        super().__init__()
        self.dirty = {}

    def acquire(self, owner, node, identities):
        identities = frozenset(identities)
        changed = {
            identity for identity in identities if not self.protected(node, identity)
        }
        super().acquire(owner, node, identities)
        self.dirty.setdefault(node, set()).update(changed)

    def release(self, owner, node):
        identities = super().release(owner, node)
        self.dirty.setdefault(node, set()).update(
            identity for identity in identities if not self.protected(node, identity)
        )
        return identities


class DeltaProtectionProjection(ReleaseCobatchSnapshot.projection_type):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        prior = self.leases
        self.leases = DeltaLeases()
        for (owner, node), identities in prior.handles.items():
            self.leases.acquire(owner, node, identities)
        self.protection_rows = {}
        self.protection_positions = {}

    def sync_protection(self, node):
        original = self.state.layout.units[node]
        dirty = self.leases.dirty.setdefault(node, set())
        if self.protection_rows.get(node) is original:
            positions = self.protection_positions[node]
            indices = (
                index for identity in dirty for index in positions.get(identity, ())
            )
        else:
            # Copy/save/reclaim may replace owners or protection bits. Retain
            # exact identity validation, including publication with old leases.
            positions = {}
            for index, unit in enumerate(original):
                positions.setdefault(unit.identity, []).append(index)
            self.protection_positions[node] = positions
            indices = range(len(original))
        updated = None
        for index in indices:
            unit = original[index]
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


class DeltaProtectionSnapshot(ReleaseCobatchSnapshot):
    projection_type = DeltaProtectionProjection


class DeltaProtectionQueues(ReleaseCobatchQueues):
    def planning_snapshot(self):
        shadow = super().planning_snapshot()
        shadow.__class__ = DeltaProtectionSnapshot
        return shadow


class DeltaProtectionSimulation(ReleaseCobatchSimulation):
    queue_type = DeltaProtectionQueues

    def run(self):
        result = super().run()
        result["internal_candidate"].update(
            name="delta-protection-cobatch-02",
            protection_only_forecast_optimization=True,
            fresh_publication_forces_full_protection_reconciliation=True,
        )
        return result
