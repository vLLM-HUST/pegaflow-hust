"""
Scheduler-side connector logic.
"""

import os
import time
from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from pegaflow.connector.common import (
    CacheGroupLayout,
    ConnectorContext,
    LoadIntent,
    PegaConnectorMetadata,
    PegaKVConnectorStats,
    PegaWorkerMetadata,
    RecurrentLoadHold,
    SaveIntent,
    logger,
    reconcile_hybrid_hit,
)
from pegaflow.connector.connector_metrics import PrefetchTracker
from pegaflow.connector.telemetry import telemetry_event
from pegaflow.connector.tp_shards import ShardedQueryReady, TpShardQueryClient
from pegaflow.pegaflow import EngineRpcClient

if TYPE_CHECKING:
    from vllm.v1.core.kv_cache_manager import KVCacheBlocks
    from vllm.v1.core.sched.output import SchedulerOutput
    from vllm.v1.outputs import KVConnectorOutput
    from vllm.v1.request import Request


@dataclass(slots=True)
class _QueryProbe:
    """One remote prefix-query snapshot.

    A probe snapshots:

    * ``computed_blocks`` — blocks already computed locally when the query was
      issued
    * ``query_hashes`` — remaining full-block hashes plus at most one derived
      tail key sent to the backend
    * ``tail_tokens`` — valid rows in that tail block, used for token accounting
      and request-drift validation

    If the request keeps making local progress while the backend is loading,
    the current query key may drift.  A *Ready* result is accepted only if the
    current key still matches this snapshot.
    """

    computed_blocks: int
    query_hashes: tuple[bytes, ...]
    tail_tokens: int = 0
    # DeepSeek-V4: physical blocks of the hash group span several hash-chain
    # positions (block_size / virtual_block_size). The backend query is
    # sub-sampled to one hash per physical block; hit counts are expanded
    # back to hash units so the rest of the scheduler's vLLM-facing math is
    # unchanged.
    hash_span: int = 1

    # ``None`` means the backend is still loading.
    hit_blocks: int | None = None
    leases: tuple[bytes, ...] = ()
    attention_lease_blocks: int = 0
    # Hybrid (HMA): pinned recurrent checkpoints from the membership queries,
    # set together with `leases` when the hybrid reconcile found a boundary.
    recurrent_hold: RecurrentLoadHold | None = None
    # Sorted query positions a hybrid hit may end at (intersection over all
    # recurrent groups and shards, below the attention prefix). Used to
    # re-derive a legal boundary when the token budget shrinks the hit.
    usable_positions: frozenset[int] = frozenset()

    @property
    def is_ready(self) -> bool:
        return self.hit_blocks is not None

    def matches(
        self,
        computed_blocks: int,
        query_hashes: tuple[bytes, ...],
        tail_tokens: int,
    ) -> bool:
        return (
            self.computed_blocks == computed_blocks
            and self.query_hashes == query_hashes
            and self.tail_tokens == tail_tokens
        )

    def mark_ready(self, ready: ShardedQueryReady) -> None:
        sampled_hits = ready.num_hit_blocks
        if sampled_hits > len(self.query_hashes):
            raise RuntimeError(
                f"invariant violated: server returned {sampled_hits} hits for "
                f"{len(self.query_hashes)} hashes"
            )
        # Expand sub-sampled backend hits back to vLLM hash-chain units: one
        # physical hash-group block covers `hash_span` chain positions, all of
        # which restore together. Capped at the queried chain length.
        self.hit_blocks = min(
            sampled_hits * self.hash_span,
            len(self.query_hashes),
        )
        self.leases = ready.leases
        self.attention_lease_blocks = ready.attention_lease_blocks or ready.num_hit_blocks
        self.recurrent_hold = ready.recurrent_hold
        self.usable_positions = frozenset(ready.usable_positions)

    def require_hit_blocks(self) -> int:
        if self.hit_blocks is None:
            raise RuntimeError("query probe is still loading")
        return self.hit_blocks


@dataclass
class _HybridQuery:
    block_hashes: tuple[bytes, ...]
    attention: ShardedQueryReady
    groups: dict[int, list[tuple[tuple[int, ...], bytes]]] = field(default_factory=dict)
    remote_checkpoint: int | None = None


class SchedulerConnector:
    """Holds scheduler-only state and behaviors."""

    def __init__(
        self,
        context: ConnectorContext,
        engine_clients: tuple[EngineRpcClient, ...] | None = None,
        pd_tail_save: bool = False,
        pd_tail_load: bool = False,
        vllm_config=None,
        kv_cache_config=None,
    ):
        self._ctx = context
        engine_clients = tuple(engine_clients or (context.engine_client,))
        expected_shards = context.tp_shards.shard_count if context.tp_shards is not None else 1
        if len(engine_clients) != expected_shards:
            raise ValueError(
                f"scheduler has {len(engine_clients)} PegaFlow clients for "
                f"{expected_shards} TP shards"
            )
        self._tp_shard_client = TpShardQueryClient(engine_clients)
        self._cache_groups = CacheGroupLayout.from_config(kv_cache_config)
        if self._cache_groups.has_recurrent_state and (pd_tail_save or pd_tail_load):
            raise ValueError("P/D tail-block caching is not supported with HMA")
        self._get_local_cached_blocks = None
        self._get_hma_cached_block = None

        # P/D tail-block extension (`pegaflow.pd_tail_save`): vLLM only hashes
        # full blocks, so a prompt's partial tail block never enters the tier
        # and a strict no-prefill decode peer would have to recompute it.
        # When enabled, the step that schedules the final prompt chunk also
        # saves the partial tail block under a key derived with vLLM's OWN
        # hash function over (last_full_hash, tail_prompt_token_ids, None) —
        # well-defined, and independently derivable by the decode peer.
        # vLLM derives NONE_HASH from PYTHONHASHSEED when it is set. A fixed
        # seed makes the configured hash function reproducible across the
        # scheduler processes participating in the transfer.
        self._tail_save_enabled = pd_tail_save
        self._tail_load_enabled = pd_tail_load
        self._tail_hash_fn = None
        if pd_tail_save or pd_tail_load:
            assert vllm_config is not None
            algo = vllm_config.cache_config.prefix_caching_hash_algo
            if os.environ.get("PYTHONHASHSEED") is None:
                enabled_options = ", ".join(
                    option
                    for enabled, option in (
                        (pd_tail_save, "pegaflow.pd_tail_save"),
                        (pd_tail_load, "pegaflow.pd_tail_load"),
                    )
                    if enabled
                )
                raise ValueError(
                    "P/D tail-block caching requires a fixed PYTHONHASHSEED "
                    f"across vLLM processes; enabled options: {enabled_options}"
                )
            from vllm.utils.hashing import get_hash_fn_by_name
            from vllm.v1.core import kv_cache_utils

            self._tail_hash_fn = get_hash_fn_by_name(algo)
            self._hash_block_tokens = kv_cache_utils.hash_block_tokens
            # NONE_HASH is only assigned by vLLM's init_none_hash(), which
            # runs after connector construction — it must be read lazily
            # through the module, never imported by name here.
            self._kv_cache_utils = kv_cache_utils
            logger.info(
                "[PegaKVConnector] P/D tail-block cache enabled (save=%s load=%s algo=%s)",
                pd_tail_save,
                pd_tail_load,
                algo,
            )
        self._tail_saved: set[str] = set()

        # Load state
        self._pending_load_intents: dict[str, LoadIntent] = {}
        self._prefetch_start_times: dict[str, float] = {}
        self._pending_query_probes: dict[str, _QueryProbe] = {}
        self._hybrid_queries: dict[str, _HybridQuery] = {}
        self._hma_saved_hashes: dict[str, list[set[bytes]]] = {}
        self._hma_save_pins: dict[str, dict[int, dict[int, object]]] = {}
        self._hma_save_acks: dict[tuple[str, int], set[tuple[int, int]]] = {}
        self._next_hma_save_ticket = 0
        self._touch_save_blocks = None
        self._free_save_blocks = None

        # Prefetch tracking (for metrics)
        self._prefetch_tracker = PrefetchTracker()

        # Save state (per-request)
        self._block_hashes: dict[str, tuple[bytes, ...]] = {}
        self._external_matched_blocks: dict[str, int] = {}
        self._block_index_offsets: dict[str, int] = {}
        self._allocated_blocks: dict[str, list[list[int]]] = {}
        self._scheduled_tokens: dict[str, int] = {}
        self._next_stored_block_idx: dict[str, int] = {}

        # Live Request references – used to refresh block_hashes during decode
        # so that newly completed blocks can be saved, not just prefill blocks.
        self._requests: dict[str, Request] = {}

        # Completion tracking
        self._deferred_save_intents: dict[str, SaveIntent] = {}
        self._pending_saves: set[str] = set()
        self._held_requests: set[str] = set()

    def bind_gpu_block_pool(self, gpu_block_pool) -> None:
        self._get_local_cached_blocks = gpu_block_pool.get_cached_block
        if self._cache_groups.has_recurrent_state:
            from vllm.v1.core.kv_cache_utils import make_block_hash_with_group_id

            def get_hma_cached_block(block_id, block_hash, group_index):
                block = gpu_block_pool.blocks[block_id]
                expected = make_block_hash_with_group_id(block_hash, group_index)
                return block if block.block_hash == expected else None

            self._get_hma_cached_block = get_hma_cached_block
            self._touch_save_blocks = gpu_block_pool.touch
            self._free_save_blocks = gpu_block_pool.free_blocks
        if self._cache_groups.group_count <= 1:
            return

        # vLLM can cache an async-loaded dense group and expose it to a sibling
        # before the sparse group has a usable state. PegaFlow is the sole HMA
        # prefix index until vLLM exposes an atomic all-group cache hook.
        def no_local_hma_prefix_hit(*_args, **_kwargs) -> None:
            return None

        gpu_block_pool.get_cached_block = no_local_hma_prefix_hit

    def get_num_new_matched_tokens(
        self,
        request: "Request",
        num_computed_tokens: int,
    ) -> tuple[int | None, bool]:
        req_id = request.request_id

        if not self._ctx.read_enabled:
            logger.debug(
                "[PegaKVConnector] req=%s cache_lookup_skipped: mode=%s",
                req_id,
                self._ctx.mode.value,
            )
            return (0, False)

        computed_blocks = num_computed_tokens // self._ctx.virtual_block_size
        query_hashes, tail_tokens = self._build_query(request, computed_blocks)

        # Nothing remains to query remotely.
        if not query_hashes:
            self._release_pending_query_probe(req_id)
            self._external_matched_blocks[req_id] = computed_blocks
            return (0, False)

        probe = self._pending_query_probes.get(req_id)

        # Ready result already cached.  Reuse it only if the request identity
        # has not drifted since the query was issued.
        if probe is not None and probe.is_ready:
            if probe.matches(computed_blocks, query_hashes, tail_tokens):
                return self._finish_cache_lookup(
                    req_id=req_id,
                    num_tokens=request.num_tokens,
                    probe=probe,
                    lookup_us=None,
                    reused=True,
                )

            # Cached Ready is stale.  It has a lease, so release it.
            self._release_pending_query_probe(req_id)
            probe = None

        # A Loading task is keyed by req_id server-side. If the current query
        # drifted, finish polling with the original identity so the TP layer can
        # validate the stale Ready before we release it below.
        backend_query_hashes = query_hashes
        if probe is not None and not probe.matches(computed_blocks, query_hashes, tail_tokens):
            backend_query_hashes = probe.query_hashes

        # No reusable Ready result. Ask backend.
        lookup_start = time.perf_counter()
        ready = self._count_available_block_prefix(backend_query_hashes, req_id)
        lookup_us = (time.perf_counter() - lookup_start) * 1e6

        # Backend is still loading.  Keep the original snapshot.
        if ready is None:
            if probe is None:
                self._pending_query_probes[req_id] = _QueryProbe(
                    computed_blocks=computed_blocks,
                    query_hashes=query_hashes,
                    tail_tokens=tail_tokens,
                    hash_span=self._hash_group_span,
                )
            return (None, False)

        # A previous Loading probe exists, but the request has moved on.
        # This Ready belongs to the old query.  Do not consume it.
        if probe is not None and not probe.matches(computed_blocks, query_hashes, tail_tokens):
            logger.warning(
                "[PegaKVConnector] req=%s query identity drifted: "
                "snapshot computed=%d/%d hashes, current computed=%d/%d hashes "
                "- discarding stale Ready",
                req_id,
                probe.computed_blocks,
                len(probe.query_hashes),
                computed_blocks,
                len(query_hashes),
            )
            self._release_leases(ready.leases, req_id)
            if ready.recurrent_hold is not None:
                for group_index, group_leases in enumerate(ready.recurrent_hold.leases):
                    self._tp_shard_client.release(group_leases, f"{req_id}:g{group_index}")
            self._pending_query_probes.pop(req_id, None)
            return (None, False)

        # Either:
        #   1. IDLE -> Ready
        #   2. Loading probe -> Ready (identity matched above)
        if probe is None:
            probe = _QueryProbe(
                computed_blocks=computed_blocks,
                query_hashes=query_hashes,
                tail_tokens=tail_tokens,
                hash_span=self._hash_group_span,
            )
            self._pending_query_probes[req_id] = probe

        probe.mark_ready(ready)
        return self._finish_cache_lookup(
            req_id=req_id,
            num_tokens=request.num_tokens,
            probe=probe,
            lookup_us=lookup_us,
            reused=False,
        )

    def _finish_cache_lookup(
        self,
        *,
        req_id: str,
        num_tokens: int,
        probe: _QueryProbe,
        lookup_us: float | None,
        reused: bool,
    ) -> tuple[int, bool]:
        hit_blocks = probe.require_hit_blocks()
        computed_blocks = probe.computed_blocks
        vbs = self._ctx.virtual_block_size
        tail_hit = probe.tail_tokens > 0 and hit_blocks == len(probe.query_hashes)
        # _build_query appends the tail key last and the backend reports prefix
        # hits, so a full query hit makes the final block the partial tail.
        last_block_tokens = probe.tail_tokens if tail_hit else vbs
        hit_tokens = (hit_blocks - 1) * vbs + last_block_tokens

        # A request still needs one forward token to produce logits. The last
        # loaded page may contain that token's KV, but vLLM must recompute and
        # overwrite it unless a P/D router supplied a separate decode token.
        locally_computed_tokens = computed_blocks * vbs
        hit_tokens = min(hit_tokens, max(0, num_tokens - locally_computed_tokens - 1))

        if probe.recurrent_hold is not None:
            # A mamba checkpoint is valid only at its own block boundary. If
            # the token budget cut inside the reconciled span, fall back to
            # the best earlier boundary; if none survives, drop the hit (a
            # partial mamba resume cannot exist).
            boundary_blocks = hit_tokens // vbs
            usable = [p for p in probe.usable_positions if p < boundary_blocks]
            if not usable:
                if self._pending_query_probes.get(req_id) is probe:
                    self._release_pending_query_probe(req_id)
                return (0, False)
            checkpoint = max(usable)
            hit_blocks = checkpoint + 1
            hit_tokens = hit_blocks * vbs
            probe.hit_blocks = hit_blocks
            probe.recurrent_hold = replace(probe.recurrent_hold, checkpoint=checkpoint)

        # Cacheable tails contain at least two tokens, so recomputing the final
        # prompt token cannot remove the last leased block from the load.
        loaded_blocks = (hit_tokens + vbs - 1) // vbs
        self._external_matched_blocks[req_id] = computed_blocks + loaded_blocks

        if reused:
            logger.debug(
                "[PegaKVConnector] req=%s cache_lookup_reuse: hit_blocks=%d "
                "computed_blocks=%d hit_tokens=%d num_tokens=%d total_query_hashes=%d "
                "tail_hit=%s tail_tokens=%d",
                req_id,
                hit_blocks,
                computed_blocks,
                hit_tokens,
                num_tokens,
                len(probe.query_hashes),
                tail_hit,
                probe.tail_tokens,
            )
        else:
            logger.info(
                "[PegaKVConnector] req=%s cache_lookup: hit_blocks=%d computed_blocks=%d "
                "hit_tokens=%d num_tokens=%d lookup_us=%.0f total_query_hashes=%d "
                "tail_hit=%s tail_tokens=%d",
                req_id,
                hit_blocks,
                computed_blocks,
                hit_tokens,
                num_tokens,
                lookup_us or 0.0,
                len(probe.query_hashes),
                tail_hit,
                probe.tail_tokens,
            )

        if hit_blocks == 0:
            # A completed miss remains valid while this request's query
            # identity is unchanged. Retrying it while waiting for token
            # budget would duplicate a remote fetch and re-enter a cohort gate.
            return (0, False)

        if hit_tokens <= 0:
            # No external load will consume this lease.
            self._release_pending_query_probe(req_id)
            return (0, False)

        return (hit_tokens, True)

    def update_state_after_alloc(
        self,
        request: "Request",
        blocks: "KVCacheBlocks",
        num_external_tokens: int,
    ) -> None:
        req_id = request.request_id

        # Keep a live reference so we can refresh block_hashes during decode
        # (Request.block_hashes grows as new full blocks are completed).
        self._requests[req_id] = request

        # request.block_hashes are already at virtual_block_size granularity
        # (1 hash per scheduler block =
        # block_size * dcp_world_size * pcp_world_size tokens).
        # They are 1-to-1 with block_ids from the scheduler.
        self._block_hashes[req_id] = tuple(request.block_hashes)
        if req_id not in self._allocated_blocks:
            # The first locally allocated block may be after an external-hit
            # prefix. Track that global block index explicitly.
            base_block_idx = self._external_matched_blocks.get(req_id, 0)
            self._block_index_offsets[req_id] = base_block_idx
            self._allocated_blocks[req_id] = [[] for _ in range(self._cache_groups.group_count)]
            self._scheduled_tokens[req_id] = 0
            self._next_stored_block_idx[req_id] = base_block_idx

        if num_external_tokens > 0:
            block_ids_by_group = (
                self._copy_block_ids_by_group(blocks.get_block_ids())
                if blocks
                else tuple(() for _ in range(self._cache_groups.group_count))
            )
            hash_group_index = self._cache_groups.hash_group_index
            num_computed_blocks = (
                sum(block.block_hash is not None for block in blocks.blocks[hash_group_index])
                if blocks
                else 0
            )
            start_block_idx = num_computed_blocks
            vbs = self._ctx.virtual_block_size
            num_load_blocks = (num_external_tokens + vbs - 1) // vbs
            try:
                load_block_ids_by_group = self._load_block_ids_by_group(
                    block_ids_by_group,
                    start_block_idx,
                    num_load_blocks,
                )
            except RuntimeError:
                self._release_pending_query_probe(req_id)
                raise

            pending_probe = self._pending_query_probes.get(req_id)
            load_intent = LoadIntent(
                block_ids_by_group=load_block_ids_by_group,
                leases=pending_probe.leases if pending_probe is not None else (),
                num_tokens=num_external_tokens,
                recurrent_hold=(
                    pending_probe.recurrent_hold if pending_probe is not None else None
                ),
                attention_lease_blocks=(
                    pending_probe.attention_lease_blocks if pending_probe is not None else 0
                ),
            )
            if pending_probe is not None:
                query_hashes, tail_tokens = self._build_query(request, num_computed_blocks)
                if not pending_probe.matches(num_computed_blocks, query_hashes, tail_tokens):
                    self._release_pending_query_probe(req_id)
                    raise RuntimeError(f"req {req_id} query identity changed before external load")
                leased_blocks = pending_probe.require_hit_blocks()
                if leased_blocks != num_load_blocks:
                    self._release_pending_query_probe(req_id)
                    raise RuntimeError(
                        f"req {req_id} leased block mismatch: "
                        f"leased={leased_blocks} load={num_load_blocks}"
                    )
            if not load_intent.leases or any(not lease for lease in load_intent.leases):
                raise RuntimeError(f"req {req_id} missing query lease for external load")
            self._pending_load_intents[req_id] = load_intent
            self._pending_query_probes.pop(req_id, None)
            logger.debug(
                "[PegaKVConnector] req=%s alloc: total_blocks=%d computed_blocks=%d "
                "load_blocks=%d start_block_idx=%d load_tokens=%d pending_loads=%d",
                req_id,
                len(block_ids_by_group[hash_group_index]),
                num_computed_blocks,
                len(load_intent.block_ids_by_group[hash_group_index]),
                start_block_idx,
                load_intent.num_tokens,
                len(self._pending_load_intents),
            )

    def build_connector_meta(self, scheduler_output: "SchedulerOutput") -> PegaConnectorMetadata:
        # Collect all save intents that became available this scheduler step.
        ready_save_intents = self._deferred_save_intents
        self._deferred_save_intents = {}
        potential_saves: dict[str, SaveIntent] = {}

        load_intents = self._pending_load_intents
        self._pending_load_intents = {}

        # Process new requests
        for req in scheduler_output.scheduled_new_reqs:
            req_id = req.req_id
            num_tokens = scheduler_output.num_scheduled_tokens.get(req_id, 0)

            # Verify update_state_after_alloc was called for this request
            assert req_id in self._block_hashes, (
                f"req {req_id} not initialized in update_state_after_alloc"
            )

            # Populate block IDs from scheduler_output — single source of
            # truth for the save path (consistent with offloading connector).
            if req.block_ids:
                self._allocated_blocks[req_id] = [
                    list(group) for group in self._copy_block_ids_by_group(req.block_ids)
                ]

            if self._ctx.read_enabled:
                self._scheduled_tokens[req_id] += num_tokens
            else:
                self._scheduled_tokens[req_id] = max(
                    self._scheduled_tokens.get(req_id, 0),
                    req.num_computed_tokens + num_tokens,
                )

            # Positions with valid KV after this step, from the scheduler's
            # own invariant (num_computed_tokens covers prefix-cache hits and
            # is reset on preemption — no connector-side bookkeeping can be
            # trusted across a preempt/resume cycle).
            written = req.num_computed_tokens + num_tokens
            if save_intent := self._consume_save_intent(
                req_id,
                written,
                req.num_computed_tokens,
            ):
                potential_saves[req_id] = save_intent

        # Process cached (running) requests
        cached_reqs = scheduler_output.scheduled_cached_reqs
        for idx, req_id in enumerate(cached_reqs.req_ids):
            if req_id not in self._block_hashes:
                continue

            # Refresh block hashes from the live Request object so that
            # newly completed blocks during decode are also saved.
            req = self._requests.get(req_id)
            if req is not None:
                self._block_hashes[req_id] = tuple(req.block_hashes)

            num_tokens = scheduler_output.num_scheduled_tokens.get(req_id, 0)

            # Append newly allocated blocks
            new_block_ids = cached_reqs.new_block_ids[idx]
            if req_id in cached_reqs.resumed_req_ids:
                self._allocated_blocks[req_id] = (
                    [list(group) for group in self._copy_block_ids_by_group(new_block_ids)]
                    if new_block_ids
                    else [[] for _ in range(self._cache_groups.group_count)]
                )
            elif new_block_ids:
                for allocated, new_group in zip(
                    self._allocated_blocks[req_id],
                    self._copy_block_ids_by_group(new_block_ids),
                    strict=True,
                ):
                    allocated.extend(new_group)

            if self._ctx.read_enabled:
                self._scheduled_tokens[req_id] += num_tokens
            else:
                prior_computed_tokens = cached_reqs.num_computed_tokens[idx]
                self._scheduled_tokens[req_id] = max(
                    self._scheduled_tokens.get(req_id, 0),
                    prior_computed_tokens + num_tokens,
                )

            written = cached_reqs.num_computed_tokens[idx] + num_tokens
            if save_intent := self._consume_save_intent(
                req_id,
                written,
                cached_reqs.num_computed_tokens[idx],
            ):
                potential_saves[req_id] = save_intent

        save_intents = potential_saves

        # Track requests with pending saves
        self._pending_saves.update(save_intents.keys())

        logger.debug(
            "[PegaKVConnector] build_connector_meta: %d loads, %d saves",
            len(load_intents),
            len(save_intents),
        )

        return PegaConnectorMetadata(
            load_intents=load_intents,
            save_intents=save_intents,
            ready_save_intents=ready_save_intents,
            preempted_req_ids=scheduler_output.preempted_req_ids or None,
        )

    def _consume_save_intent(
        self,
        req_id: str,
        written: int | None = None,
        computed_before_step: int = 0,
    ) -> SaveIntent | None:
        # Upstream-test compatibility: the pre-#433 call shape had no
        # scheduler-authoritative `written`; derive it from scheduled tokens.
        if written is None:
            written = self._scheduled_tokens.get(req_id, 0)
        """Calculate and return SaveIntent for new blocks that need saving.

        `written` = positions with valid KV once this step's schedule runs
        (scheduler-authoritative num_computed_tokens + this step's tokens).
        """
        regular = self._consume_full_block_saves(req_id, written, computed_before_step)
        tail = self._consume_tail_save(req_id, written)
        if tail is None:
            return regular
        if regular is None:
            return tail
        return SaveIntent(
            block_ids_by_group=tuple(
                regular_ids + tail_ids
                for regular_ids, tail_ids in zip(
                    regular.block_ids_by_group,
                    tail.block_ids_by_group,
                    strict=True,
                )
            ),
            block_hashes=regular.block_hashes + tail.block_hashes,
        )

    def _consume_tail_save(self, req_id: str, written: int) -> SaveIntent | None:
        """P/D tail extension: save the prompt's partial tail block once its
        prompt rows are final (the step scheduling the final prompt chunk).

        The saved page may contain rows past the prompt (the first generated
        token lands in it on the next step, racing the async D2H) — harmless:
        the key covers only the tail *prompt* tokens, and the decode peer
        recomputes every position past them anyway.
        """
        if not self._tail_save_enabled or req_id in self._tail_saved:
            return None
        if self._hash_group_span > 1:
            # Tail keys are synthetic chain-granularity hashes; pairing them
            # with a span>1 hash-group block would misalign (DeepSeek-V4).
            return None
        req = self._requests.get(req_id)
        if req is None:
            return None
        vbs = self._ctx.virtual_block_size
        prompt_len = req.num_prompt_tokens
        tail = self._derive_tail_block(req)
        if tail is None:
            return None
        tail_key, tail_len = tail
        if written < prompt_len:
            return None  # tail prompt rows not written yet
        tail_idx = prompt_len // vbs
        allocated = self._allocated_blocks.get(req_id, [])
        block_hashes = self._block_hashes.get(req_id) or ()
        if (
            not allocated
            or any(tail_idx >= len(group) for group in allocated)
            or tail_idx > len(block_hashes)
        ):
            return None  # tail block not allocated / full-block hashes lagging
        self._tail_saved.add(req_id)
        logger.info(
            "[PegaKVConnector] req=%s pd_tail_save: block_id=%d tail_tokens=%d key=%s",
            req_id,
            allocated[self._cache_groups.hash_group_index][tail_idx],
            tail_len,
            tail_key.hex(),
        )
        return SaveIntent(
            block_ids_by_group=tuple((group[tail_idx],) for group in allocated),
            block_hashes=(tail_key,),
        )

    def _derive_tail_block(self, request: "Request") -> tuple[bytes, int] | None:
        if self._tail_hash_fn is None:
            return None
        if self._hash_group_span > 1:
            return None  # T6: tail keys are chain-granularity; span>1 misaligns
        # The tail key carries no extra_keys. Reusing it for salted, LoRA, or
        # multimodal requests would alias distinct vLLM cache identities.
        if request.lora_request is not None or request.cache_salt or request.mm_features:
            return None
        vbs = self._ctx.virtual_block_size
        prompt_len = request.num_prompt_tokens
        tail_len = prompt_len % vbs
        # vLLM must recompute the final prompt token to produce logits. A
        # one-token tail therefore cannot reduce local work; treating it as a
        # hit would also lease one more hash than vLLM allocates load blocks.
        if tail_len <= 1:
            return None
        tail_idx = prompt_len // vbs
        block_hashes = tuple(request.block_hashes)
        if tail_idx > len(block_hashes):
            raise RuntimeError(
                f"req {request.request_id} missing parent hash for tail block: "
                f"tail_idx={tail_idx} full_hashes={len(block_hashes)}"
            )
        parent = block_hashes[tail_idx - 1] if tail_idx > 0 else self._kv_cache_utils.NONE_HASH
        tail_tokens = list(request.prompt_token_ids[tail_idx * vbs : prompt_len])
        tail_key = bytes(self._hash_block_tokens(self._tail_hash_fn, parent, tail_tokens, None))
        return tail_key, tail_len

    def _build_query(
        self, request: "Request", computed_blocks: int
    ) -> tuple[tuple[bytes, ...], int]:
        query_hashes = tuple(request.block_hashes[computed_blocks:])
        if self._cache_groups.has_recurrent_state:
            # A checkpoint must leave at least one prompt token for logits.
            limit = max(
                0, (request.num_tokens - 1) // self._ctx.virtual_block_size - computed_blocks
            )
            query_hashes = query_hashes[:limit]
        if not self._tail_load_enabled:
            return query_hashes, 0

        tail = self._derive_tail_block(request)
        if tail is None:
            return query_hashes, 0
        return query_hashes + (tail[0],), tail[1]

    def _consume_full_block_saves(
        self,
        req_id: str,
        written: int,
        computed_before_step: int | None = None,
    ) -> SaveIntent | None:
        # block_hashes are at virtual_block_size granularity, 1-to-1 with block_ids.
        if self._cache_groups.has_recurrent_state:
            return self._consume_hma_saves(
                req_id, written if computed_before_step is None else computed_before_step
            )
        block_hashes = self._block_hashes.get(req_id)
        if block_hashes is None:
            return None

        allocated = self._allocated_blocks.get(req_id, [])
        # Legacy (pre-cache-group) shape: a flat block-id list instead of a
        # per-group tuple of lists. Normalize to one group.
        if allocated and not isinstance(allocated[0], (list, tuple)):
            allocated = [tuple(allocated)]
        scheduled = self._scheduled_tokens.get(req_id, 0)
        base_block_idx = self._block_index_offsets.get(req_id, 0)
        start_block_idx = self._next_stored_block_idx.get(req_id, base_block_idx)

        # _allocated_blocks tracks request block IDs in global request order.
        # In external-hit cases, the prefix-loaded block IDs are still present at
        # the front, so save intents must slice by global block index rather than
        # rebasing to a local-only view.
        # Other groups may hold fewer blocks at different hash granularities.
        hash_group = self._cache_groups.hash_group_index
        hash_group_blocks = len(allocated[hash_group]) if allocated else 0
        span = self._hash_group_span
        local_saveable = min(
            hash_group_blocks * span,
            scheduled // self._ctx.virtual_block_size,
        )
        saveable_block_idx = min(len(block_hashes), base_block_idx + local_saveable)
        new_blocks = saveable_block_idx - start_block_idx
        if new_blocks <= 0:
            return None

        hash_start = start_block_idx
        save_hashes = block_hashes[hash_start : hash_start + new_blocks]
        span = self._hash_group_span
        if span > 1:
            # DeepSeek-V4: hash-group physical blocks span `span` chain
            # positions. Pair each completed block with the cumulative hash at
            # its completed END boundary — the only chain hash that uniquely
            # fingerprints the full block (keying the region's first hash
            # would alias prefixes sharing only the block's opening slice).
            # Groups whose block size differs from the chain granularity
            # cannot pair 1:1 at all and are skipped (save-only would need
            # per-group hash chains — official future work).
            hash_group = self._cache_groups.hash_group_index
            g_start = hash_start // span
            g_saveable = saveable_block_idx // span
            sampled_hashes = self._block_end_hashes(
                block_hashes[g_start * span : g_saveable * span], span
            )
            save_block_ids_by_group = tuple(
                tuple(
                    group[g_start:g_saveable]
                    if group_index == hash_group
                    else ()  # non-hash groups: no 1:1 pairing at this granularity
                )
                for group_index, group in enumerate(allocated)
            )
            save_hashes = sampled_hashes
        else:
            save_block_ids_by_group = tuple(
                tuple(group[hash_start : hash_start + new_blocks]) for group in allocated
            )
        self._next_stored_block_idx[req_id] = saveable_block_idx

        logger.debug(
            "[PegaKVConnector] req=%s save_intent: start=%d hash_start=%d "
            "base_block_idx=%d saveable_block_idx=%d new_blocks=%d total_hashes=%d",
            req_id,
            hash_start,
            hash_start,
            base_block_idx,
            saveable_block_idx,
            new_blocks,
            len(block_hashes),
        )

        return SaveIntent(
            block_ids_by_group=save_block_ids_by_group,
            block_hashes=save_hashes,
        )

    def _consume_hma_saves(self, req_id: str, written: int) -> SaveIntent | None:
        if (
            self._get_hma_cached_block is None
            or self._touch_save_blocks is None
            or self._free_save_blocks is None
        ):
            raise RuntimeError("HMA block pool must support owned cached-block lookup and pinning")
        hashes = self._block_hashes.get(req_id, ())
        # Decode kernels can produce a different recurrent trajectory than
        # prefill for the same tokens. Publish only checkpoints within the
        # prompt, so a generated continuation cannot poison the prompt index.
        prompt_tokens = self._requests[req_id].num_prompt_tokens
        count = min(len(hashes), min(written, prompt_tokens) // self._ctx.virtual_block_size)
        allocated = self._allocated_blocks.get(req_id, ())
        saved = self._hma_saved_hashes.setdefault(
            req_id, [set() for _ in range(self._cache_groups.group_count)]
        )
        pins = {}
        rows = [[] for _ in saved]
        selected_hashes = []
        positions = []
        for position, block_hash in enumerate(hashes[:count]):
            selected = []
            for group_index, group_saved in enumerate(saved):
                if block_hash in group_saved:
                    selected.append(None)
                    continue
                group = allocated[group_index] if group_index < len(allocated) else ()
                block_id = group[position] if position < len(group) else 0
                # Hash lookup can return another request's older decode state.
                # Preserve the identity of this request's cached physical slot.
                block = (
                    self._get_hma_cached_block(block_id, block_hash, group_index)
                    if block_id
                    else None
                )
                if block is not None and (block.block_id == 0 or getattr(block, "is_null", False)):
                    block = None
                selected.append(block)
            if not any(block is not None for block in selected):
                continue
            for group_index, block in enumerate(selected):
                rows[group_index].append(block.block_id if block is not None else 0)
                if block is not None:
                    if block.block_id not in pins:
                        self._touch_save_blocks([block])
                        pins[block.block_id] = block
                    saved[group_index].add(block_hash)
            selected_hashes.append(block_hash)
            positions.append(position)
        if not selected_hashes:
            return None
        self._next_hma_save_ticket += 1
        ticket = self._next_hma_save_ticket
        self._hma_save_pins.setdefault(req_id, {})[ticket] = pins
        telemetry_event(
            "checkpoint_publish",
            request_id=req_id,
            ticket=ticket,
            boundaries=[(position + 1) * self._ctx.virtual_block_size for position in positions],
            blocks_by_group=rows,
            prompt_tokens=prompt_tokens,
            written_tokens=written,
        )
        logger.debug(
            "[PegaKVConnector] HMA checkpoint save: req=%s ticket=%d boundary_tokens=%s "
            "block_ids_by_group=%s hashes=%s",
            req_id,
            ticket,
            [(position + 1) * self._ctx.virtual_block_size for position in positions],
            rows,
            [block_hash.hex() for block_hash in selected_hashes],
        )
        return SaveIntent(
            block_ids_by_group=tuple(tuple(row) for row in rows),
            block_hashes=tuple(selected_hashes),
            save_ticket=ticket,
        )

    def _consume_finished_hma_save(
        self,
        req_id: str,
        block_ids: tuple[list[int], ...],
        written: int,
    ) -> SaveIntent | None:
        self._allocated_blocks[req_id] = [
            list(group) for group in self._copy_block_ids_by_group(block_ids)
        ]
        return self._consume_hma_saves(req_id, written)

    def _copy_block_ids_by_group(self, block_ids) -> tuple[tuple[int, ...], ...]:
        groups = tuple(tuple(group) for group in block_ids)
        if len(groups) != self._cache_groups.group_count:
            raise RuntimeError(
                "KV cache group count mismatch: "
                f"expected={self._cache_groups.group_count} actual={len(groups)}"
            )
        if any(block_id < 0 for group in groups for block_id in group):
            raise RuntimeError("KV cache block IDs must be non-negative")
        return groups

    def _load_block_ids_by_group(
        self,
        block_ids_by_group: tuple[tuple[int, ...], ...],
        start_block_idx: int,
        num_load_blocks: int,
    ) -> tuple[tuple[int | None, ...], ...]:
        end_block_idx = start_block_idx + num_load_blocks
        # Per-group spans: vLLM's per-group block lists are at each group's own
        # block size (DeepSeek-V4: 512/16384/128/8/32 tokens), while the hash
        # chain runs at virtual_block_size granularity. Slice each group by
        # the hash positions its blocks cover.
        vbs = self._ctx.virtual_block_size
        block_sizes = self._cache_groups.group_block_sizes
        spans = (
            tuple(bs // vbs if bs and bs % vbs == 0 else 1 for bs in block_sizes)
            if block_sizes
            else (1,) * len(block_ids_by_group)
        )
        available = [len(group) for group in block_ids_by_group]
        if any(
            length < (end_block_idx + span - 1) // span
            for length, span in zip(available, spans, strict=True)
        ):
            raise RuntimeError(
                f"load block mismatch: start={start_block_idx} count={num_load_blocks} "
                f"available_by_group={available}"
            )

        result: list[tuple[int | None, ...]] = []
        for group_index, block_ids in enumerate(block_ids_by_group):
            span = spans[group_index]
            g_start = start_block_idx // span
            g_end = (end_block_idx + span - 1) // span
            destinations = block_ids[g_start:g_end]
            if group_index in self._cache_groups.recurrent_group_indices and destinations:
                destinations = (None,) * (len(destinations) - 1) + (destinations[-1],)
            result.append(destinations)
        return tuple(result)

    def update_connector_output(self, connector_output: "KVConnectorOutput") -> None:
        metadata = getattr(connector_output, "kv_connector_worker_meta", None)
        if isinstance(metadata, PegaWorkerMetadata):
            expected = {
                (pp, tp) for pp in range(self._ctx.pp_size) for tp in range(self._ctx.tp_size)
            }
            for (req_id, ticket), ranks in metadata.saved_tickets.items():
                pending = self._hma_save_pins.get(req_id, {})
                if ticket not in pending:
                    continue  # repeated acknowledgement or an already completed request
                if not ranks <= expected:
                    raise RuntimeError("HMA save acknowledgement has unexpected worker ranks")
                key = (req_id, ticket)
                acknowledged = self._hma_save_acks.setdefault(key, set())
                acknowledged.update(ranks)
                if acknowledged == expected:
                    pins = pending.pop(ticket)
                    self._free_save_blocks(list(pins.values()))
                    self._hma_save_acks.pop(key, None)
                    if not pending:
                        self._hma_save_pins.pop(req_id, None)
        for req_id in connector_output.finished_sending or []:
            self._pending_saves.discard(req_id)
            logger.debug("[PegaKVConnector] Request %s save completed", req_id)

            # Clean up if request already finished
            if req_id in self._held_requests and req_id not in self._deferred_save_intents:
                self._cleanup_request(req_id)
                self._held_requests.discard(req_id)

    def has_pending_push_work(self) -> bool:
        """Keep vLLM stepping until finish-time save work is acknowledged."""
        return bool(self._deferred_save_intents or self._pending_saves)

    def request_finished(
        self,
        request: "Request",
        block_ids: tuple[list[int], ...],
    ) -> tuple[bool, dict | None]:
        req_id = request.request_id

        if self._cache_groups.has_recurrent_state and req_id in self._block_hashes:
            self._block_hashes[req_id] = tuple(request.block_hashes)
            final_save = self._consume_finished_hma_save(
                req_id,
                block_ids,
                request.num_computed_tokens,
            )
            if final_save is not None:
                self._deferred_save_intents[req_id] = final_save
                self._pending_saves.add(req_id)

        # Check if there are pending saves for this request
        if req_id in self._pending_saves:
            self._held_requests.add(req_id)
            logger.debug(
                "[PegaKVConnector] Request %s blocks held for async save",
                req_id,
            )
            return (True, None)

        # No pending saves, clean up immediately
        self._cleanup_request(req_id)
        return (False, None)

    def _cleanup_request(self, req_id: str) -> None:
        """Clean up all state for a completed request."""
        self._release_pending_query_probe(req_id)
        self._requests.pop(req_id, None)
        self._block_hashes.pop(req_id, None)
        self._external_matched_blocks.pop(req_id, None)
        self._block_index_offsets.pop(req_id, None)
        self._allocated_blocks.pop(req_id, None)
        self._scheduled_tokens.pop(req_id, None)
        self._next_stored_block_idx.pop(req_id, None)
        self._deferred_save_intents.pop(req_id, None)
        self._pending_saves.discard(req_id)
        self._tail_saved.discard(req_id)
        tickets = self._hma_save_pins.pop(req_id, {})
        if tickets and self._free_save_blocks is not None:
            for ticket, pins in tickets.items():
                self._free_save_blocks(list(pins.values()))
                self._hma_save_acks.pop((req_id, ticket), None)
        self._hma_saved_hashes.pop(req_id, None)

    @staticmethod
    def _block_end_hashes(chain: Iterable[bytes], span: int) -> tuple[bytes, ...]:
        """One key per physical block: the cumulative hash at each block's
        COMPLETED end boundary.

        vLLM's hash chain is cumulative (each hash fingerprints the whole
        prefix up to it), so the hash at position `(k + 1) * span - 1` — the
        last chain position inside block k — uniquely encodes block k's full
        content. Keying on the FIRST position of a block would alias any two
        prefixes that share only that block's opening slice.
        """
        chain = list(chain)
        if span <= 1:
            return tuple(chain)
        # positions span-1, 2*span-1, 3*span-1, ... (last position per block)
        return tuple(chain[span - 1 :: span])

    @property
    def _hash_group_span(self) -> int:
        """Hash-chain positions covered by one physical hash-group block.

        1 when the hash group's block size equals the virtual block size (the
        classic uniform layout). DeepSeek-V4 runs an 8-token scheduler chain
        over a 512-token MLA hash group: span = 512 / 8 = 64.
        """
        vbs = self._ctx.virtual_block_size
        block_sizes = getattr(self._cache_groups, "group_block_sizes", ())
        if not block_sizes or vbs <= 0:
            return 1
        block_size = block_sizes[self._cache_groups.hash_group_index]
        if block_size and block_size % vbs == 0:
            return block_size // vbs
        return 1

    def _count_available_block_prefix(
        self, block_hashes: Iterable[bytes], req_id: str
    ) -> ShardedQueryReady | None:
        """Query available blocks with prefetch support.

        Returns:
            ShardedQueryReady: Common ready block count and one lease per TP shard
            None: Blocks are being prefetched from DFS, retry later
        """
        block_hash_list = list(block_hashes)
        query_started = time.perf_counter()
        span = self._hash_group_span
        if span > 1:
            # DeepSeek-V4: query the completed end-boundary hash of each
            # physical hash-group block (chain is cumulative; the last chain
            # position inside a block uniquely fingerprints it). The backend
            # stores blocks under exactly those keys; hit counts are expanded
            # back to chain units in _QueryProbe.mark_ready.
            block_hash_list = self._block_end_hashes(block_hash_list, span)
        hybrid = self._hybrid_queries.get(req_id)
        if hybrid is not None and hybrid.block_hashes != tuple(block_hash_list):
            self._release_hybrid_query(req_id)
            hybrid = None
        ready = (
            hybrid.attention
            if hybrid is not None
            else self._tp_shard_client.query(
                self._ctx.instance_id, block_hash_list, req_id, self._ctx.wait_for_full_prefix
            )
        )
        if ready is not None and self._cache_groups.has_recurrent_state:
            ready = self._reconcile_hybrid(block_hash_list, ready, req_id)
        if ready is None:
            if req_id not in self._prefetch_start_times:
                self._prefetch_start_times[req_id] = time.perf_counter()
                telemetry_event(
                    "cache_query_wait",
                    request_id=req_id,
                    query_blocks=len(block_hash_list),
                    call_ms=(time.perf_counter() - query_started) * 1000,
                )
                self._prefetch_tracker.on_prefetch_start()
                logger.debug(
                    "[PegaKVConnector] Prefetch started: req=%s pending_prefetches=%d",
                    req_id,
                    self._prefetch_tracker.pending_prefetches,
                )
            return None

        prefetch_duration_ms = 0.0
        if req_id in self._prefetch_start_times:
            prefetch_duration_ms = (
                time.perf_counter() - self._prefetch_start_times.pop(req_id)
            ) * 1000
            self._prefetch_tracker.on_prefetch_complete(prefetch_duration_ms, ready.num_hit_blocks)

            logger.debug(
                "[PegaKVConnector] Prefetch completed: req=%s hit_blocks=%d "
                "prefetch_duration_ms=%.2f pending_prefetches=%d",
                req_id,
                ready.num_hit_blocks,
                prefetch_duration_ms,
                self._prefetch_tracker.pending_prefetches,
            )

        telemetry_event(
            "cache_query_ready",
            request_id=req_id,
            query_blocks=len(block_hash_list),
            hit_blocks=ready.num_hit_blocks,
            hit_tokens=ready.num_hit_blocks * self._ctx.virtual_block_size,
            recurrent_checkpoint=(
                ready.recurrent_hold.checkpoint if ready.recurrent_hold is not None else None
            ),
            wait_ms=prefetch_duration_ms,
            call_ms=(time.perf_counter() - query_started) * 1000,
        )
        return ready

    def _reconcile_hybrid(
        self,
        block_hashes: list[bytes],
        ready: ShardedQueryReady,
        req_id: str,
    ) -> ShardedQueryReady | None:
        """Gate an attention-prefix hit on a usable recurrent boundary.

        HMA can resume only where every recurrent group cached its state on
        every shard: attention KV alone cannot skip mamba's sequential
        prefill. On success returns the reduced hit with the membership
        leases attached; on failure every lease acquired is released and the
        result degrades to a plain miss.
        """
        if ready.num_hit_blocks == 0:
            return ready

        group_ids = tuple(
            self._cache_groups.storage_group_ids[index]
            for index in sorted(self._cache_groups.recurrent_group_indices)
        )
        state = self._hybrid_queries.setdefault(req_id, _HybridQuery(tuple(block_hashes), ready))
        block_hashes = block_hashes[: ready.num_hit_blocks]
        try:
            if state.remote_checkpoint is None:
                for group_id in group_ids:
                    result = self._tp_shard_client.query_group_membership(
                        self._ctx.instance_id, block_hashes, f"{req_id}:g{group_id}", group_id
                    )
                    if result is None:
                        raise RuntimeError("local membership query must finish immediately")
                    state.groups[group_id] = result
                hit, checkpoint, usable = reconcile_hybrid_hit(
                    ready.num_hit_blocks,
                    tuple(tuple(positions for positions, _ in state.groups[g]) for g in group_ids),
                )
                if hit and checkpoint is not None:
                    return self._finish_hybrid_query(req_id, checkpoint, usable)
                self._release_hybrid_groups(req_id, state)
                state.remote_checkpoint = ready.num_hit_blocks - 1

            while state.remote_checkpoint >= 0:
                checkpoint = state.remote_checkpoint
                for group_id in group_ids:
                    if group_id not in state.groups:
                        result = self._tp_shard_client.query_group_membership(
                            self._ctx.instance_id,
                            [block_hashes[checkpoint]],
                            f"{req_id}:g{group_id}:checkpoint-{checkpoint}",
                            group_id,
                            prefetch=True,
                        )
                        if result is None:
                            return None
                        state.groups[group_id] = [
                            ((checkpoint,) if positions else (), lease)
                            for positions, lease in result
                        ]
                    if any(not positions for positions, _ in state.groups[group_id]):
                        break
                else:
                    return self._finish_hybrid_query(req_id, checkpoint, frozenset({checkpoint}))
                self._release_hybrid_groups(req_id, state)
                state.remote_checkpoint -= 1
        except Exception:
            self._release_hybrid_query(req_id)
            raise
        self._release_hybrid_query(req_id)
        return ShardedQueryReady(0, tuple(b"" for _ in ready.leases))

    def _finish_hybrid_query(
        self, req_id: str, checkpoint: int, usable: frozenset[int]
    ) -> ShardedQueryReady:
        state = self._hybrid_queries.pop(req_id)
        group_ids = tuple(
            self._cache_groups.storage_group_ids[index]
            for index in sorted(self._cache_groups.recurrent_group_indices)
        )
        logger.info(
            "[PegaKVConnector] HMA checkpoint ready: req=%s query_position=%d "
            "hit_blocks=%d attention_lease_blocks=%d storage_groups=%s",
            req_id,
            checkpoint,
            checkpoint + 1,
            state.attention.num_hit_blocks,
            group_ids,
        )
        return ShardedQueryReady(
            checkpoint + 1,
            state.attention.leases,
            RecurrentLoadHold(
                leases=tuple(
                    tuple(lease for _, lease in state.groups[group]) for group in group_ids
                ),
                hit_positions=tuple(
                    tuple(positions for positions, _ in state.groups[group]) for group in group_ids
                ),
                checkpoint=checkpoint,
            ),
            usable_positions=tuple(sorted(usable)),
            attention_lease_blocks=state.attention.num_hit_blocks,
        )

    def _release_hybrid_groups(self, req_id: str, state: _HybridQuery) -> bool:
        released = True
        for index in sorted(self._cache_groups.recurrent_group_indices):
            group_id = self._cache_groups.storage_group_ids[index]
            suffix = (
                f":checkpoint-{state.remote_checkpoint}"
                if state.remote_checkpoint is not None
                else ""
            )
            group_req_id = f"{req_id}:g{group_id}{suffix}"
            released = (
                self._tp_shard_client.cancel_membership(
                    self._ctx.instance_id, group_req_id, group_id
                )
                and released
            )
            result = state.groups.pop(group_id, [])
            released = (
                self._tp_shard_client.release(tuple(lease for _, lease in result), group_req_id)
                and released
            )
        return released

    def _release_hybrid_query(self, req_id: str) -> bool:
        state = self._hybrid_queries.pop(req_id, None)
        if state is None:
            return True
        groups_released = self._release_hybrid_groups(req_id, state)
        return self._release_leases(state.attention.leases, req_id) and groups_released

    def _cancel_prefetch_tracking(self, req_id: str) -> None:
        """Drop in-flight prefetch metrics when polling stops before QueryReady."""
        if req_id not in self._prefetch_start_times:
            return

        started_at = self._prefetch_start_times.pop(req_id)
        self._prefetch_tracker.on_prefetch_cancel()
        waited_ms = (time.perf_counter() - started_at) * 1000
        logger.warning(
            "[PegaKVConnector] Prefetch aborted before ready: req=%s waited_ms=%.2f "
            "pending_prefetches=%d",
            req_id,
            waited_ms,
            self._prefetch_tracker.pending_prefetches,
        )

    def get_stats(self) -> PegaKVConnectorStats | None:
        """Get current connector stats for metrics exposure."""
        # Get stats from prefetch tracker
        prefetch_stats = self._prefetch_tracker.get_stats()

        data: dict = {
            "pending_prefetches": prefetch_stats["pending_prefetches"],
            "prefetch_duration": prefetch_stats["prefetch_duration"],
            "prefetch_blocks": prefetch_stats["prefetch_blocks"],
        }

        stats = PegaKVConnectorStats(data=data)
        if stats.is_empty():
            return None
        return stats

    def shutdown(self) -> None:
        for req_id in set(self._pending_query_probes) | set(self._hybrid_queries):
            self._release_pending_query_probe(req_id)

    def _release_pending_query_probe(self, req_id: str) -> bool:
        hybrid_released = self._release_hybrid_query(req_id)
        pending_released = self._tp_shard_client.cancel_pending(self._ctx.instance_id, req_id)
        pending_released = pending_released and hybrid_released
        probe = self._pending_query_probes.pop(req_id, None)
        if probe is None:
            self._cancel_prefetch_tracking(req_id)
            return pending_released

        return self._release_query_probe(req_id, probe) and pending_released

    def _release_query_probe(self, req_id: str, probe: _QueryProbe) -> bool:
        released = True
        if probe.leases and any(probe.leases):
            released = self._release_leases(probe.leases, req_id)
        else:
            self._cancel_prefetch_tracking(req_id)
        hold = probe.recurrent_hold
        if hold is not None:
            for group_index, group_leases in enumerate(hold.leases):
                if not self._tp_shard_client.release(group_leases, f"{req_id}:g{group_index}"):
                    released = False
        return released

    def _release_leases(self, leases: tuple[bytes, ...], req_id: str) -> bool:
        return self._tp_shard_client.release(leases, req_id)


__all__ = ["SchedulerConnector"]
