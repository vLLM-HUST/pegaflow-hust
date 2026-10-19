"""Native selected-headroom bridge and conservative complete-control guard."""

from dataclasses import asdict
import math
import time

from pegaflow.joint_policy import Layout, View
from pegaflow.joint_runtime import CommitConflict, json_rpc
from pegaflow.placement_executor import RecoveryPrefix, UnstableInventory
from pegaflow.resource_headroom_profile import native_headroom


def shifted_profile(value, work, extra):
    """Price additional measured commit preparation without shifting past firsts."""
    if not math.isfinite(extra) or extra < 0:
        raise ValueError("Finite nonnegative additional control duration required")
    def update(values, observed):
        fixed = list(observed)
        answer = []
        for entry in values:
            if entry in fixed:
                fixed.remove(entry)
                answer.append(entry)
            else:
                answer.append(entry + extra)
        return tuple(sorted(answer, reverse=True))
    first = update(value[1], (w.actual_first - w.offered for w in work if w.actual_first is not None))
    full = tuple(v + extra for v in value[2])
    p_end = update(value[3], (w.p_done - w.offered for w in work if w.p_done is not None))
    rank = math.ceil(0.99 * len(first))
    return first[len(first) - rank], first, full, p_end


def selected_guard(backend, queues, item, node, started):
    """Only a chosen guard may remove fresh native physical-owner closures.

    No long-lived capacity reservation, distributed atomicity or cleanup-all.
    Native epoch/generation/lease/retained-prefix checks remain authoritative.
    """
    if not queues.admission_choices.get((item.key, node), False):
        return None
    began = time.monotonic()
    cpu = time.thread_time()
    operation = ("P" if node < 2 else "D") + "_admission"
    record = dict(kind="selected_headroom", request_id=item.key, node=node,
                  chosen_by_route=True, started_seconds=began - started)
    with backend.locked((node,)):
        acquired = time.monotonic()
        queues.operation(item.key, operation, "start", acquired - started, (f"gate{node}",))
        try:
            payload = backend.inventory(node)
            view = View.parse(node, payload)
            views = list(backend.cached_views() or backend.views())
            views[node] = view
            layout = Layout.current(views)
            with queues.models.guard:
                frozen = queues.models.freeze(time.monotonic() - started)
            plan = native_headroom(layout, node, item, frozen)
            record["plan"] = asdict(plan) if plan is not None else None
            if plan is None:
                record.update(status="normal_defer", reason="protected_or_insufficient_owner_closure")
            elif not plan.victims:
                record.update(status="no_action", reason="no_current_write_shortage")
            else:
                selected = set(plan.victims)
                record["selected_physical_units"] = [dict(identity=unit.key,
                    allocation_ids=list(unit.allocation_ids), charged_bytes=unit.charged_bytes,
                    protected=unit.protected,
                    logical_keys=[dict(namespace=namespace, hash=key.hex())
                                  for namespace, key in sorted(unit.logical_keys)])
                    for unit in view.snapshot.units if unit.key in selected]
                states = {unit.key: 0 if unit.key in selected else view.snapshot.host
                          for unit in view.snapshot.units}
                length = layout.prefix(node, item.hashes)
                retained = ((RecoveryPrefix(item.key, item.hashes[:length]),) if length else ())
                request = view.snapshot.eviction(states, retained)
                if request is None:
                    raise CommitConflict("Chosen owner closure disappeared before native commit")
                intent = time.monotonic()
                receipt = json_rpc(backend.cache_urls[node] + "/cache/placement/evict", request)
                view.snapshot.verify_receipt(request, receipt)
                backend.record(dict(kind="remove", operation={"node": node, "identities": list(plan.victims)},
                    request=request, receipt=receipt, rpc_seconds=time.monotonic() - intent,
                    request_id=item.key, action_scope="chosen_fast_write_headroom_native_receipt"))
                record.update(status="applied", native_request=request, native_receipt=receipt)
                fresh = View.parse(node, backend.inventory(node))
                record["post_inventory_accounting"] = dict(epoch=fresh.snapshot.epoch,
                    generation=fresh.snapshot.generation, used_bytes=fresh.snapshot.allocator_used_bytes,
                    nonvisible_bytes=fresh.snapshot.nonvisible_bytes)
                with backend.latest_lock:
                    latest = list(backend.latest_views)
                    latest[node] = fresh
                    backend.latest_views = tuple(latest)
        except (CommitConflict, UnstableInventory) as error:
            record.update(status="normal_defer", reason=str(error))
        finally:
            ended = time.monotonic()
            queues.operation(item.key, operation, "finish", ended - started)
            queues.models.observe_admission(ended - acquired, ended - started)
            record.update(completed_seconds=ended - started, gate_wait_seconds=acquired - began,
                          held_seconds=ended - acquired, cpu_seconds=time.thread_time() - cpu)
            backend.record(record)
    return record
