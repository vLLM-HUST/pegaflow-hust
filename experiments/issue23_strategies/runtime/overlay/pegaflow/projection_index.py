"""Decision-local immutable-node indexes for pure shadow layouts, not live inventory."""

from dataclasses import dataclass, field
from functools import cached_property
from types import MappingProxyType

from pegaflow.joint_policy import Layout


@dataclass(frozen=True, eq=False)
class NodeIndex:
    units: tuple

    @cached_property
    def keys(self):
        return frozenset(key for unit in self.units for key in unit.keys)

    @cached_property
    def payload(self):
        values = {}
        for unit in self.units:
            values.update(unit.payload)
        return MappingProxyType(values)

    @cached_property
    def charged(self):
        return sum(unit.charged for unit in self.units)

    @cached_property
    def signature(self):
        return hash(self.units)

    def __hash__(self):
        return self.signature

    def __eq__(self, other):
        return isinstance(other, NodeIndex) and self.units == other.units


class NodeIndexes:
    def __init__(self):
        self.values = {}
        self.hits = 0

    def get(self, units):
        identity = id(units)
        value = self.values.get(identity)
        if value is None:
            # Keep the tuple referenced: object IDs cannot be recycled within
            # the scope and mistaken for a different protected/charged state.
            value = NodeIndex(units)
            self.values[identity] = value
        elif value.units is not units:
            raise ValueError("Immutable tuple identity reused while still referenced")
        else:
            self.hits += 1
        return value


@dataclass(frozen=True)
class IndexedLayout(Layout):
    indexes: NodeIndexes = field(compare=False, repr=False)

    @classmethod
    def wrap(cls, layout, indexes):
        return cls(layout.units, layout.capacities, layout.hidden, indexes)

    def keys(self, node):
        return self.indexes.get(self.units[node]).keys

    def usage(self, node):
        return self.hidden[node] + self.indexes.get(self.units[node]).charged

    @property
    def key_sets(self):
        return tuple(self.keys(node) for node in range(4))

    @property
    def payload_maps(self):
        return tuple(self.indexes.get(units).payload for units in self.units)

    @property
    def usage_values(self):
        return tuple(self.usage(node) for node in range(4))
