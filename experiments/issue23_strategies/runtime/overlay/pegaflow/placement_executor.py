"""Connect the shared-object planner to generation-checked local eviction.

The supported actions retain or remove existing physical allocations. Admission
and migration require a separate destination reservation and publication API.
Node receipts are independent; this module claims no distributed transaction.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass

from pegaflow.placement import CacheObject, PlacementBundle


class UnstableInventory(ValueError):
    """A concurrent allocator sample must be deferred without mutation."""


def _integer(value, label, *, minimum=0):
    if type(value) is not int or value < minimum:
        raise ValueError(f"Invalid {label}")
    return value


def _hash(value):
    if (
        not isinstance(value, list)
        or not value
        or any(type(part) is not int or not 0 <= part <= 255 for part in value)
    ):
        raise ValueError("Invalid logical cache hash")
    return bytes(value)


@dataclass(frozen=True)
class RecoveryPrefix:
    key: str
    hashes: tuple[bytes, ...]

    def __post_init__(self):
        if (
            not self.key
            or not self.hashes
            or any(type(value) is not bytes or not value for value in self.hashes)
            or len({len(value) for value in self.hashes}) != 1
            or len(set(self.hashes)) != len(self.hashes)
        ):
            raise ValueError("A distinct, ordered attention hash chain is required")


@dataclass(frozen=True)
class ReclaimUnit:
    key: str
    allocation_ids: frozenset[str]
    logical_keys: frozenset[tuple[str, bytes]]
    charged_bytes: int
    protected: bool


@dataclass(frozen=True)
class NodeInventory:
    host: int
    instance_id: str
    namespace: str
    epoch: str
    generation: int
    capacity_bytes: int
    allocator_used_bytes: int
    nonvisible_bytes: int
    group_slots: tuple[int, ...]
    units: tuple[ReclaimUnit, ...]
    blocks: Mapping[tuple[str, bytes], dict]
    allocation_to_unit: Mapping[str, str]

    @classmethod
    def parse(cls, host: int, payload: dict) -> NodeInventory:
        if type(host) is not int or host not in (1, 2):
            raise ValueError("One node bit is required")
        instance, namespace = payload["instance_id"], payload["namespace"]
        epoch = payload["cache"]["epoch"]
        if not all(type(value) is str and value for value in (instance, namespace, epoch)):
            raise ValueError("Missing inventory identity")
        generation = _integer(payload["cache"]["generation"], "generation")
        capacity = _integer(payload["allocator_capacity_bytes"], "capacity", minimum=1)
        used = _integer(payload["allocator_used_bytes"], "allocator usage")
        hidden = _integer(payload["nonvisible_used_bytes"], "nonvisible allocation usage")
        slots = tuple(_integer(value, "group slots", minimum=1) for value in payload["group_slots"])
        if not slots or used > capacity or hidden > used:
            raise ValueError("Invalid registered topology or allocator accounting")
        allocations = {}
        for item in payload["cache"]["allocations"]:
            identity = item["id"]
            if type(identity) is not str or not identity or identity in allocations:
                raise ValueError("Duplicate or empty physical allocation")
            if type(item["protected"]) is not bool:
                raise ValueError("Invalid allocation protection")
            _integer(item["charged_bytes"], "rounded allocation bytes", minimum=1)
            allocations[identity] = item
        parents = {identity: identity for identity in allocations}
        ranks = dict.fromkeys(allocations, 0)

        def root(identity):
            while identity != parents[identity]:
                parents[identity] = parents[parents[identity]]
                identity = parents[identity]
            return identity

        def union(first, second):
            first, second = root(first), root(second)
            if first == second:
                return
            if ranks[first] < ranks[second]:
                first, second = second, first
            parents[second] = first
            if ranks[first] == ranks[second]:
                ranks[first] += 1

        blocks = {}
        for item in payload["cache"]["blocks"]:
            key = item["key"]["namespace"], _hash(item["key"]["hash"])
            ids = item["allocation_ids"]
            if (
                type(key[0]) is not str
                or not key[0]
                or key in blocks
                or not ids
                or len(set(ids)) != len(ids)
                or set(ids) - allocations.keys()
                or type(item["protected"]) is not bool
            ):
                raise ValueError("Invalid physical owner closure")
            _integer(item["slots"], "sealed slots", minimum=1)
            blocks[key] = item
            for identity in ids[1:]:
                union(identity, ids[0])
        owners = {identity for item in blocks.values() for identity in item["allocation_ids"]}
        if owners != allocations.keys():
            raise ValueError("A visible allocation has no logical owner")
        components = {}
        for identity in allocations:
            components.setdefault(root(identity), set()).add(identity)
        component_keys = {}
        for key, item in blocks.items():
            component_keys.setdefault(root(item["allocation_ids"][0]), set()).add(key)
        units, allocation_to_unit = [], {}
        for component, identities in components.items():
            keys = frozenset(component_keys[component])
            unit_key = f"{host}:{epoch}:{min(identities)}"
            protected = any(allocations[identity]["protected"] for identity in identities) or any(
                blocks[key]["protected"] or key[0] != namespace for key in keys
            )
            units.append(
                ReclaimUnit(
                    unit_key,
                    frozenset(identities),
                    keys,
                    sum(allocations[identity]["charged_bytes"] for identity in identities),
                    protected,
                )
            )
            allocation_to_unit.update(dict.fromkeys(identities, unit_key))
        units.sort(key=lambda unit: unit.key)
        visible = sum(unit.charged_bytes for unit in units)
        if (
            visible != _integer(payload["visible_allocation_bytes"], "visible allocation usage")
            or visible + hidden != used
        ):
            raise UnstableInventory("Allocator samples changed; acquire a fresh inventory")
        return cls(
            host,
            instance,
            namespace,
            epoch,
            generation,
            capacity,
            used,
            hidden,
            slots,
            tuple(units),
            blocks,
            allocation_to_unit,
        )

    def planner_objects(self) -> tuple[CacheObject, ...]:
        return tuple(
            CacheObject(
                unit.key,
                unit.charged_bytes if self.host == 1 else 0,
                unit.charged_bytes if self.host == 2 else 0,
                (0, self.host),
                self.host if unit.protected else 0,
            )
            for unit in self.units
        )

    def package(self, prefix: RecoveryPrefix) -> PlacementBundle | None:
        required = [(self.namespace, value, self.group_slots[0]) for value in prefix.hashes]
        required.extend(
            (self.namespace, prefix.hashes[-1] + group.to_bytes(4, "big"), slots)
            for group, slots in enumerate(self.group_slots[1:], 1)
        )
        objects = set()
        for namespace, value, slots in required:
            block = self.blocks.get((namespace, value))
            if block is None or block["slots"] != slots:
                return None
            objects.update(
                self.allocation_to_unit[identity] for identity in block["allocation_ids"]
            )
        return PlacementBundle(f"{self.host}:{prefix.key}", frozenset(objects))

    def eviction(
        self, states: Mapping[str, int], prefixes: tuple[RecoveryPrefix, ...]
    ) -> dict | None:
        removed = []
        for unit in self.units:
            state = states.get(unit.key)
            if type(state) is not int or state not in (0, self.host):
                raise ValueError(
                    "This executor supports retain/remove only, not admission or migration"
                )
            if state == 0:
                if unit.protected:
                    raise ValueError("Planner attempted to remove a mandatory physical object")
                removed.extend(unit.allocation_ids)
        if not removed:
            return None
        retained = []
        for prefix in prefixes:
            package = self.package(prefix)
            if package is not None and package.available(states, self.host):
                retained.append([list(value) for value in prefix.hashes])
        return {
            "instance_id": self.instance_id,
            "epoch": self.epoch,
            "generation": self.generation,
            "allocation_ids": sorted(removed),
            "retained_prefixes": retained,
        }

    def verify_receipt(self, request: dict, receipt: dict) -> None:
        selected = set(request["allocation_ids"])
        removed_units = [unit for unit in self.units if selected.intersection(unit.allocation_ids)]
        expected_keys = {key for unit in removed_units for key in unit.logical_keys}
        actual_keys = {(key["namespace"], _hash(key["hash"])) for key in receipt["removed_keys"]}
        if (
            receipt["actions_applied"] is not True
            or receipt["local_only"] is not True
            or receipt["epoch"] != self.epoch
            or type(receipt["generation_before"]) is not int
            or receipt["generation_before"] != self.generation
            or type(receipt["generation_after"]) is not int
            or receipt["generation_after"] != self.generation + 1
            or actual_keys != expected_keys
            or len(actual_keys) != len(receipt["removed_keys"])
            or type(receipt["released_allocation_bytes"]) is not int
            or receipt["released_allocation_bytes"]
            != sum(unit.charged_bytes for unit in removed_units)
            or receipt["staging_reserved_bytes"] != 0
            or type(receipt["staging_reserved_bytes"]) is not int
            or type(receipt["removal_queued"]) is not bool
        ):
            raise ValueError("Unverified actual eviction receipt; do not retry the mutation")


class HttpPlacementBackend:
    def __init__(self, base_url: str, *, timeout_seconds: float = 10):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def _request(self, path, body=None):
        request = urllib.request.Request(
            self.base_url + path,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
            return json.load(response)

    def inventory(self, host: int, instance_id: str) -> NodeInventory:
        query = urllib.parse.urlencode({"instance_id": instance_id})
        return NodeInventory.parse(host, self._request("/cache/placement/inventory?" + query))

    def apply(self, snapshot: NodeInventory, states, prefixes=()) -> dict:
        request = snapshot.eviction(states, prefixes)
        if request is None:
            return {"status": "no_action", "actions_applied": False, "host": snapshot.host}
        try:
            receipt = self._request("/cache/placement/evict", request)
        except urllib.error.HTTPError as error:
            if error.code != 409:
                raise
            return {
                "status": "conflict",
                "actions_applied": False,
                "host": snapshot.host,
                "reason": error.read().decode(),
            }
        snapshot.verify_receipt(request, receipt)
        return {
            "status": "applied",
            "actions_applied": True,
            "host": snapshot.host,
            "request": request,
            "receipt": receipt,
            "distributed_atomicity_claimed": False,
        }
