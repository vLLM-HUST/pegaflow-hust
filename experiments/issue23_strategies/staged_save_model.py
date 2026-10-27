"""Source-derived save publication lifetime, opt-in CPU model revision.

Keep the existing owner-closure allocator geometry. Allocation is private until
the synchronous save completes; failure drops private owners but does not undo
read-cache eviction. This is not a native allocator or fragmentation emulator.
"""

import hashlib
import time
from dataclasses import replace

from pegaflow.joint_policy import Unit
from pegaflow.resource_cache_profile import CacheForecast

VERSION = "staged-save-publication-02"


def published_copy(layout, transition, owner):
    """A completed copy has real per-block owners, not proposed virtual owners."""
    operation = transition.operations[-1]
    destination = operation.destination
    payloads = layout.payload_maps[operation.source]
    nodes = list(layout.units)
    nodes[destination] = (*nodes[destination], *(Unit(
        f"published-copy:{destination}:{owner}:{key.hex()}", frozenset((key,)),
        ((payloads[key] + 4095) // 4096) * 4096, ((key, payloads[key]),),
    ) for key in operation.missing))
    result = replace(layout, units=tuple(nodes))
    if not result.feasible():
        raise ValueError("Published copy exceeds physically quoted capacity")
    return result


class SaveAllocationError(ValueError):
    """The modeled synchronous save failed; no partial success is publishable."""


class StagedCacheForecast(CacheForecast):
    publication_model = VERSION
    reclaim_method = "published-owner-unindex-with-lease-retention"
    restore_scoped_host_lease = True
    incremental_D_save = True

    def finished(self, work, node, operation, at):
        if operation.name not in ("history", "handoff"):
            return super().finished(work, node, operation, at)
        from pegaflow.joint_policy import copy_package

        source, destination, hashes = self.pending_copies[work.key, operation.name]
        try:
            transition = copy_package(self.layout, source, destination, hashes)
        except ValueError:
            return
        self.layout = published_copy(self.layout, transition, work.key)

    def save(self, node, demand):
        return self.save_delta(node, demand, 0, None)

    def save_delta(self, node, demand, begin, end):
        memo_key = (
            VERSION,
            node,
            self.layout.units[node],
            self.layout.hidden[node],
            self.layout.capacities[node],
            demand.key,
            demand.tokens,
            demand.hashes,
            demand.block_tokens,
            self.costs.bytes_per_token,
            self.output_tokens,
            self.reclaim_batch,
            begin, end,
        )
        memo = self.memo["saves"] if self.memo is not None else None
        if memo is not None and memo_key in memo:
            self.memo["hits"] += 1
            units, events, failed, calls = memo[memo_key]
            self.reclaim_removal_calls += calls
        else:
            if self.memo is not None:
                self.memo["misses"] += 1
            units, events, failed, calls = self.allocate(node, demand, begin, end)
            self.reclaim_removal_calls += calls
            if memo is not None:
                memo[memo_key] = units, events, failed, calls
        nodes = list(self.layout.units)
        nodes[node] = units
        self.layout = replace(self.layout, units=tuple(nodes))
        self.events.extend(
            {
                **e,
                "request": demand.key,
                "node": node,
                "at": getattr(self, "event_time", None),
                "publication_model": VERSION,
            }
            for e in events
        )
        if not self.layout.feasible():
            raise AssertionError("Published and held unique owners exceed pool")
        if failed:
            raise SaveAllocationError(
                f"{VERSION}: save allocation failed for {demand.key} on {node}"
            )

    def allocate(self, node, demand, begin=0, end=None):
        units = list(self.layout.units[node])
        used = self.layout.usage(node)
        capacity = self.layout.capacities[node]
        payload = demand.block_tokens * self.costs.bytes_per_token
        charged = ((payload + 4095) // 4096) * 4096
        computed = demand.tokens + (self.output_tokens - 1 if node >= 2 else 0)
        extra = computed // demand.block_tokens - len(demand.hashes)
        unknown = tuple(
            hashlib.sha256(
                b"forecast-space-only-not-reusable\0"
                + demand.key.encode()
                + bytes((node,))
                + i.to_bytes(8, "little")
            ).digest()
            for i in range(extra)
        )
        # Native hash filtering occurs ONCE before allocation. A filtered hit
        # evicted during this allocation is not retroactively copied again.
        present = self.layout.keys(node)
        intended = (*demand.hashes, *unknown)[begin:end]
        missing = tuple(
            dict.fromkeys(k for k in intended if k not in present)
        )
        staging, events, calls = [], [], 0
        peak = used
        for key in missing:
            while used + charged > capacity:
                # Published names may disappear while another request retains
                # an Arc/lease. The empty-key owner remains charged until release.
                victims, count = [], 0
                for unit in units:
                    if unit.keys and not unit.virtual:
                        victims.append(unit)
                        count += len(unit.keys)
                        if count >= self.reclaim_batch:
                            break
                if not victims:
                    break
                victim_ids = {id(u) for u in victims}
                kept = []
                for unit in units:
                    if id(unit) not in victim_ids:
                        kept.append(unit)
                    elif unit.protected:
                        kept.append(replace(unit, keys=frozenset(), payload=()))
                    else:
                        used -= unit.charged
                units = kept
                calls += 1
                events.append(
                    {
                        "kind": "forecast_native_reclaim",
                        "logical_blocks": count,
                        "charged": sum(u.charged for u in victims if not u.protected),
                        "retained_lease_bytes": sum(
                            u.charged for u in victims if u.protected
                        ),
                        "staging_bytes": sum(u.charged for u in staging),
                        "victims": [
                            {
                                "owner": u.identity,
                                "keys": sorted(k.hex() for k in u.keys),
                                "charged": u.charged,
                                "retained_by_lease": u.protected,
                            }
                            for u in victims
                        ],
                    }
                )
            if used + charged > capacity:
                events.append(
                    {
                        "kind": "save_allocation_failed",
                        "requested_blocks": len(missing),
                        "rolled_back_staging_bytes": sum(u.charged for u in staging),
                        "published_new_blocks": 0,
                        "peak_unique_bytes": peak,
                        "capacity_bytes": capacity,
                        "failed_key": key.hex(),
                    }
                )
                return tuple(units), tuple(events), True, calls
            staging.append(
                Unit(
                    f"staged:{node}:{demand.key}:{key.hex()}",
                    frozenset((key,)),
                    charged,
                    ((key, payload),),
                )
            )
            used += charged
            peak = max(peak, used)
        events.append(
            {
                "kind": "save_publication",
                "requested_blocks": len(missing),
                "published_new_blocks": len(staging),
                "peak_unique_bytes": peak,
                "capacity_bytes": capacity,
                "staged_bytes": sum(u.charged for u in staging),
                "new_owners": [u.identity for u in staging],
                "filtered_hits": len(set(intended) & present),
                "save_block_begin": begin, "save_block_end": end,
                "new_keys": [next(iter(u.keys)).hex() for u in staging],
            }
        )
        return (*units, *staging), tuple(events), False, calls


class StagedSaveSimulationMixin:
    """Same correction in both physical stage worlds and every cache forecast."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.policy_queues.forecast_cache_type = StagedCacheForecast
        self.save_model_CPU = []
        self.restored_host_owners = set()
        self.D_save_cursors = {}

    def pin(self, node, hashes, sign, *, owner=None):
        if sign == -1 and owner in self.restored_host_owners:
            self.restored_host_owners.remove(owner)
            return
        super().pin(node, hashes, sign, owner=owner)
        # Empty-key owners were unindexed by pressure, not freed while leased.
        nodes = list(self.layout.units)
        nodes[node] = tuple(
            u for u in nodes[node] if u.keys or u.protected or u.virtual
        )
        self.layout = replace(self.layout, units=tuple(nodes))

    def commit_copy(self, transition, key):
        began = time.perf_counter()
        self.layout = published_copy(self.layout, transition, key)
        operation = transition.operations[-1]
        self.actions.append({
            "kind": "copy_publication", "request": key, "at": self.now,
            "node": operation.destination, "source": operation.source,
            "published_keys": [k.hex() for k in operation.missing],
            "virtual_after_commit": False, "publication_model": VERSION,
        })
        elapsed = time.perf_counter() - began
        self.save_model_CPU.append(elapsed)
        self.now += elapsed

    def batch_iteration_finished(self, node, iteration):
        if iteration.kind == "compute":
            for key, _, _ in iteration.pieces:
                sequence = self.d_batches[node].active[key]
                item = self.item(self.jobs[key]["row"])
                end = (sequence.prefix_tokens + sequence.prefilled + max(0, sequence.decoded - 1)) // item.block_tokens
                begin = self.D_save_cursors.get(key, sequence.prefix_tokens // item.block_tokens)
                if end > begin:
                    self.save_chunk(node, item, begin, end)
                self.D_save_cursors[key] = end
            return
        if iteration.kind != "restore":
            return
        began = time.perf_counter()
        for key, _, phase in iteration.pieces:
            assert phase == "restore"
            owner = key, "D_api"
            identities = self.allocation_leases.handles.get((owner, node), frozenset())
            self.pin(node, set(), -1, owner=owner)
            self.restored_host_owners.add(owner)
            self.actions.append({"kind": "restore_host_lease_release", "request": key,
                                 "node": node, "at": self.now, "owners": sorted(identities),
                                 "D_slot_and_device_KV_still_reserved": True})
        elapsed = time.perf_counter() - began
        self.save_model_CPU.append(elapsed)
        self.now += elapsed

    def save_cache(self, node, item, costs):
        if node >= 2:
            end = (item.tokens + 127) // item.block_tokens
            begin = self.D_save_cursors.pop(item.key)
            if begin != end:
                raise AssertionError("D completed without publishing its computed save ranges")
            self.actions.append({"kind": "save_stage_complete", "request": item.key,
                                 "node": node, "at": self.now, "block_cursor": end,
                                 "publication_model": VERSION})
            return
        self.save_chunk(node, item, 0, None, costs)

    def save_chunk(self, node, item, begin, end, costs=None):
        began = time.perf_counter()
        state = StagedCacheForecast(self.layout, {item.key: item}, {}, costs or self.costs, 512)
        state.event_time = self.now
        try:
            state.save_delta(node, item, begin, end)
        finally:
            self.layout = state.layout
            self.actions.extend(state.events)
            elapsed = time.perf_counter() - began
            self.save_model_CPU.append(elapsed)
            self.now += elapsed

    def run(self):
        result = super().run()
        result["save_publication_model"] = {
            "version": VERSION,
            "same_world_and_forecast": True,
            "save_model_CPU_wall": sum(self.save_model_CPU),
            "CPU_in_virtual_clock": True,
            "native_binary_executed": False,
            "D_host_lease_end": "completed restore; device KV and sequence slot remain until full finish",
            "D_publication": "computed block deltas at modeled iteration end; save cost already embedded in retained service",
            "geometry": "retained whole physical owner closures, 4096 rounding, reclaim512",
            "limitation": "not layer-stride fragmentation or native insert-worker timing emulation",
        }
        result["forecast_reclaim"] = {
            "policy": "published owner closures, source batch512; leases retain physical ownership",
            "implementation": StagedCacheForecast.reclaim_method,
            "physical_execution": VERSION,
            "cost_accounting": "forecast CPU and actual save model CPU both charged",
        }
        if self.mode != "native":
            assert all(
                q["forecast_save_model"] == VERSION
                for q in result["initial_cache_forecasts"].values()
            )
        return result
