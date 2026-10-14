"""Hybrid checkpoint identity, sparse publication and async load regressions."""

import sys
from dataclasses import dataclass
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock

import pytest

from .unit_stubs import install_connector_unit_stubs

install_connector_unit_stubs()

from pegaflow.connector.common import (  # noqa: E402
    CacheGroupLayout,
    LoadIntent,
    PegaConnectorMetadata,
    PegaWorkerMetadata,
    RecurrentLoadHold,
    SaveIntent,
    TpShardTopology,
    derive_namespace,
    resolve_model_revision,
)
from pegaflow.connector.scheduler import SchedulerConnector  # noqa: E402
from pegaflow.connector.tp_shards import TpShardQueryClient  # noqa: E402
from pegaflow.connector.worker import _infer_kv_cache_registration  # noqa: E402
from pegaflow.pegaflow import QueryLoading, QueryReady  # noqa: E402

from .test_combine_hashes import _make_ctx  # noqa: E402
from .test_connector_fault_tolerance import _make_worker, _stub_forward_context  # noqa: E402
from .test_derive_namespace import _make_vllm_config  # noqa: E402
from .test_worker_kv_layout import make_tensor  # noqa: E402


@pytest.fixture
def hybrid_config(monkeypatch):
    # Spec doubles model only the scheduler/worker's layout contract.
    interface = ModuleType("vllm.v1.kv_cache_interface")

    @dataclass
    class FullAttentionSpec:
        block_size: int = 2048
        page_size_bytes: int = 2097152
        dtype: str = "bfloat16"

    @dataclass
    class MambaSpec:
        block_size: int = 2048
        page_size_bytes: int = 2097152
        shapes: tuple = ((512, 4), (16, 128, 128))
        dtypes: tuple = ("bfloat16", "float32")
        mamba_cache_mode: str = "align"
        num_speculative_blocks: int = 0

    interface.FullAttentionSpec = FullAttentionSpec
    interface.MambaSpec = MambaSpec
    interface.MLAAttentionSpec = type("MLAAttentionSpec", (), {})
    interface.SlidingWindowSpec = type("SlidingWindowSpec", (), {})
    interface.UniformTypeKVCacheSpecs = type("UniformTypeKVCacheSpecs", (), {})
    monkeypatch.setitem(sys.modules, interface.__name__, interface)
    utils = ModuleType("vllm.v1.core.kv_cache_utils")
    utils.make_block_hash_with_group_id = lambda key, group: key + group.to_bytes(4, "big")
    monkeypatch.setitem(sys.modules, utils.__name__, utils)
    return SimpleNamespace(
        kv_cache_groups=[
            SimpleNamespace(layer_names=("attn",), kv_cache_spec=FullAttentionSpec()),
            SimpleNamespace(layer_names=("gdn",), kv_cache_spec=MambaSpec()),
        ]
    )


def scheduler_for(config, clients):
    topology = TpShardTopology(
        tuple(f"http://shard-{i}" for i in range(len(clients))), len(clients), len(clients)
    )
    ctx = _make_ctx(
        block_size=2048,
        engine_client=clients[0],
        tp_shards=topology,
        tp_size=len(clients),
        world_size=len(clients),
    )
    return SchedulerConnector(ctx, engine_clients=clients, kv_cache_config=config)


def test_issue23_starts_every_group_before_attention_finishes(hybrid_config):
    import copy

    config = copy.deepcopy(hybrid_config)
    config.kv_cache_groups.extend([copy.deepcopy(config.kv_cache_groups[1]) for _ in range(2)])
    client = MagicMock()
    polls = {}
    calls = []

    def query(instance, hashes, req_id, group_id=0, **kwargs):
        calls.append(group_id)
        polls[group_id] = polls.get(group_id, 0) + 1
        if polls[group_id] == 1:
            return QueryLoading()
        return QueryReady(
            2 if group_id == 0 else 1,
            f"lease-g{group_id}".encode(),
            list(range(2)) if group_id == 0 else [0],
        )

    client.query_prefetch.side_effect = query
    scheduler = scheduler_for(config, (client,))
    scheduler._issue23_parallel_groups = True
    assert scheduler._count_available_block_prefix([b"a", b"b"], "request") is None
    assert calls == [0, 1, 2, 3]
    ready = scheduler._count_available_block_prefix([b"a", b"b"], "request")
    assert ready.num_hit_blocks == 2
    assert ready.recurrent_hold.checkpoint == 1
    assert len(ready.recurrent_hold.leases) == 3
    assert not scheduler._hybrid_queries


def test_issue23_missing_checkpoint_fails_and_releases_all_ready_leases(hybrid_config):
    client = MagicMock()

    def query(instance, hashes, req_id, group_id=0, **kwargs):
        return QueryReady(2, b"attention", [0, 1]) if group_id == 0 else QueryReady(0, b"", [])

    client.query_prefetch.side_effect = query
    scheduler = scheduler_for(hybrid_config, (client,))
    scheduler._issue23_parallel_groups = True
    with pytest.raises(RuntimeError, match="checkpoint is unavailable"):
        scheduler._count_available_block_prefix([b"a", b"b"], "request")
    assert not scheduler._hybrid_queries
    assert any(call.args[0] == b"attention" for call in client.release.call_args_list)


def test_qwen_attention_planes_and_state_slots_have_independent_addressing():
    plane = _infer_kv_cache_registration(
        make_tensor((160, 128, 1, 256)), 2048, is_attention_plane=True
    )
    assert (plane.num_blocks, plane.bytes_per_block, plane.physical_blocks_per_logical_block) == (
        10,
        1048576,
        16,
    )
    # A two-row allocation still describes one attention plane, never K+V.
    two_rows = _infer_kv_cache_registration(
        make_tensor((2, 128, 1, 256)), 256, is_attention_plane=True
    )
    assert (two_rows.num_blocks, two_rows.segments) == (1, 1)
    state = _infer_kv_cache_registration(make_tensor((10, 2, 16)), 2048, is_recurrent_state=True)
    assert (state.num_blocks, state.bytes_per_block, state.physical_blocks_per_logical_block) == (
        10,
        64,
        1,
    )


def test_sparse_save_uses_cached_boundary_and_pins_until_ack(hybrid_config):
    scheduler = scheduler_for(hybrid_config, (MagicMock(), MagicMock()))
    blocks = {
        key: SimpleNamespace(
            block_id=value, is_null=False, block_hash=key[0] + key[1].to_bytes(4, "big")
        )
        for key, value in {
            (b"a", 0): 10,
            (b"b", 0): 11,
            (b"b", 1): 20,
        }.items()
    }
    pool = MagicMock()
    pool.get_cached_block.side_effect = lambda key, groups: (
        [blocks[(key, groups[0])]] if (key, groups[0]) in blocks else None
    )
    pool.blocks = {block.block_id: block for block in blocks.values()}
    pool.blocks[99] = SimpleNamespace(block_id=99, block_hash=None)
    scheduler.bind_gpu_block_pool(pool)
    scheduler._requests["request"] = SimpleNamespace(num_prompt_tokens=4100)
    scheduler._allocated_blocks["request"] = [[10, 11, 12], [0, 20, 99]]
    scheduler._block_hashes["request"] = (b"a", b"b", b"unfinished")
    intent = scheduler._consume_full_block_saves("request", 4100, 4096)
    assert intent.block_hashes == (b"a", b"b")
    assert intent.block_ids_by_group == ((10, 11), (0, 20))
    assert pool.touch.call_count == 3
    # The request's running state is slot 2; the cached 4096 checkpoint is slot 1.
    assert (
        scheduler._consume_finished_hma_save("request", ([10, 11, 12], [0, 20, 99]), 4100) is None
    )
    pool.free_blocks.assert_not_called()
    # A checkpoint published later gets its own group-only save row.
    blocks[(b"a", 1)] = SimpleNamespace(
        block_id=21, is_null=False, block_hash=b"a" + (1).to_bytes(4, "big")
    )
    pool.blocks[21] = blocks[(b"a", 1)]
    scheduler._allocated_blocks["request"][1][0] = 21
    late = scheduler._consume_full_block_saves("request", 4100, 4096)
    assert (late.block_hashes, late.block_ids_by_group) == ((b"a",), ((0,), (21,)))

    def acknowledge(ticket, ranks):
        scheduler.update_connector_output(
            SimpleNamespace(
                finished_sending=set(),
                kv_connector_worker_meta=PegaWorkerMetadata({("request", ticket): ranks}),
            )
        )

    acknowledge(intent.save_ticket, {(0, 0)})
    acknowledge(intent.save_ticket, {(0, 0)})
    pool.free_blocks.assert_not_called()
    # The next save can finish first without releasing the prior batch's sources.
    acknowledge(late.save_ticket, {(0, 0), (0, 1)})
    pool.free_blocks.assert_called_once()
    assert {block.block_id for block in pool.free_blocks.call_args.args[0]} == {21}
    acknowledge(intent.save_ticket, {(0, 1)})
    assert {block.block_id for block in pool.free_blocks.call_args.args[0]} == {10, 11, 20}
    assert pool.free_blocks.call_count == 2
    acknowledge(intent.save_ticket, {(0, 0), (0, 1)})
    assert pool.free_blocks.call_count == 2
    assert "request" not in scheduler._hma_save_pins
    assert not scheduler._hma_save_acks


@pytest.mark.parametrize("written", [4096, 4100])
def test_finish_checkpoint_requires_exact_cached_hash(hybrid_config, written):
    scheduler = scheduler_for(hybrid_config, (MagicMock(), MagicMock()))
    pool = MagicMock()
    pool.get_cached_block.return_value = None
    pool.blocks = {
        block_id: SimpleNamespace(block_id=block_id, block_hash=None) for block_id in (10, 11, 99)
    }
    scheduler.bind_gpu_block_pool(pool)
    scheduler._requests["request"] = SimpleNamespace(num_prompt_tokens=written)
    scheduler._block_hashes["request"] = (b"a", b"b")
    assert scheduler._consume_finished_hma_save("request", ([10, 11], [0, 99]), written) is None
    pool.touch.assert_not_called()


@pytest.mark.parametrize("prompt_tokens,expected", [(2047, 0), (2048, 1), (4095, 1), (4096, 2)])
def test_decode_boundaries_cannot_enter_hybrid_prompt_cache(hybrid_config, prompt_tokens, expected):
    scheduler = scheduler_for(hybrid_config, (MagicMock(),))
    pool = MagicMock()
    pool.blocks = {
        block_id: SimpleNamespace(
            block_id=block_id, is_null=False, block_hash=key + group.to_bytes(4, "big")
        )
        for block_id, key, group in ((10, b"a", 0), (11, b"b", 0), (20, b"a", 1), (21, b"b", 1))
    }
    scheduler.bind_gpu_block_pool(pool)
    scheduler._requests["request"] = SimpleNamespace(num_prompt_tokens=prompt_tokens)
    scheduler._block_hashes["request"] = (b"a", b"b")
    intent = scheduler._consume_finished_hma_save("request", ([10, 11], [20, 21]), 6144)
    assert (() if intent is None else intent.block_hashes) == (b"a", b"b")[:expected]
    assert pool.touch.call_count == expected * 2


@pytest.mark.parametrize("own_hash", [b"a" + (1).to_bytes(4, "big"), b"other", None])
def test_hybrid_save_selects_owned_slot_instead_of_older_hash_match(hybrid_config, own_hash):
    scheduler = scheduler_for(hybrid_config, (MagicMock(),))
    old = SimpleNamespace(block_id=19, is_null=False, block_hash=b"a" + (1).to_bytes(4, "big"))
    own = SimpleNamespace(block_id=20, is_null=False, block_hash=own_hash)
    pool = MagicMock()
    pool.blocks = {20: own}
    pool.get_cached_block.return_value = [old]
    scheduler.bind_gpu_block_pool(pool)
    scheduler._requests["request"] = SimpleNamespace(num_prompt_tokens=2048)
    scheduler._block_hashes["request"] = (b"a",)
    intent = scheduler._consume_finished_hma_save("request", ([0], [20]), 2048)
    if own_hash == old.block_hash:
        assert intent.block_ids_by_group == ((0,), (20,))
        pool.touch.assert_called_once_with([own])
    else:
        assert intent is None
        pool.touch.assert_not_called()


def shard_with_remote(outcomes):
    client = MagicMock()
    sequence = iter(outcomes)

    def query(_instance, _hashes, *, group_id=0, wait_for_full_prefix=False, **_kwargs):
        if group_id == 0:
            return QueryReady(2, b"attention")
        if not wait_for_full_prefix:
            return QueryReady(0, b"")
        return next(sequence)

    client.query_prefetch.side_effect = query
    return client


def test_remote_candidate_retains_attention_and_ready_tp_shard(hybrid_config):
    first = shard_with_remote([QueryReady(1, b"state-first", [0])])
    second = shard_with_remote([QueryLoading(), QueryReady(1, b"state-second", [0])])
    scheduler = scheduler_for(hybrid_config, (first, second))
    assert scheduler._count_available_block_prefix([b"a", b"b"], "request") is None
    result = scheduler._count_available_block_prefix([b"a", b"b"], "request")
    assert result.num_hit_blocks == result.attention_lease_blocks == 2
    assert result.recurrent_hold.hit_positions == (((1,), (1,)),)
    assert result.recurrent_hold.leases == ((b"state-first", b"state-second"),)
    assert first.query_prefetch.call_count == 3  # attention, local, remote once
    assert second.query_prefetch.call_count == 4
    first.release.assert_not_called()
    second.release.assert_not_called()
    assert not scheduler._hybrid_queries
    assert not scheduler._tp_shard_client._membership_pending
    assert scheduler._prefetch_tracker.pending_prefetches == 0
    assert scheduler._prefetch_tracker.pending_prefetches == 0


def test_remote_missing_candidate_falls_back_without_refetching_attention(hybrid_config):
    first = shard_with_remote([QueryReady(1, b"latest", [0]), QueryReady(1, b"earlier-1", [0])])
    second = shard_with_remote([QueryReady(0, b""), QueryReady(1, b"earlier-2", [0])])
    scheduler = scheduler_for(hybrid_config, (first, second))
    result = scheduler._count_available_block_prefix([b"a", b"b"], "request")
    assert result.num_hit_blocks == 1
    assert result.attention_lease_blocks == 2
    assert result.recurrent_hold.checkpoint == 0
    first.release.assert_called_once_with(b"latest")
    assert [call.args[1] for call in first.query_prefetch.call_args_list] == [
        [b"a", b"b"],
        [b"a", b"b"],
        [b"b"],
        [b"a"],
    ]


@pytest.mark.parametrize("action", ["cancel", "shutdown", "drift"])
def test_abandoned_remote_query_releases_completed_leases(hybrid_config, action):
    first = shard_with_remote([QueryReady(1, b"state", [0])])
    second = shard_with_remote([QueryLoading()])
    scheduler = scheduler_for(hybrid_config, (first, second))
    assert scheduler._count_available_block_prefix([b"a", b"b"], "request") is None
    if action == "cancel":
        scheduler._release_pending_query_probe("request")
    elif action == "shutdown":
        scheduler.shutdown()
    else:
        first.query_prefetch.return_value = QueryReady(0, b"")
        first.query_prefetch.side_effect = None
        second.query_prefetch.return_value = QueryReady(0, b"")
        second.query_prefetch.side_effect = None
        assert scheduler._count_available_block_prefix([b"changed"], "request").num_hit_blocks == 0
    assert [call.args[0] for call in first.release.call_args_list] == [b"state", b"attention"]
    second.release.assert_called_once_with(b"attention")
    assert not scheduler._hybrid_queries
    assert not scheduler._tp_shard_client._membership_pending


def test_no_common_remote_checkpoint_releases_attention(hybrid_config):
    first = shard_with_remote([QueryReady(0, b""), QueryReady(0, b"")])
    second = shard_with_remote([QueryReady(0, b""), QueryReady(0, b"")])
    scheduler = scheduler_for(hybrid_config, (first, second))
    assert scheduler._count_available_block_prefix([b"a", b"b"], "request").num_hit_blocks == 0
    first.release.assert_called_once_with(b"attention")
    second.release.assert_called_once_with(b"attention")


def test_remote_wait_retains_completed_recurrent_group(hybrid_config):
    hybrid_config.kv_cache_groups.append(
        SimpleNamespace(
            layer_names=("gdn-second",),
            kv_cache_spec=hybrid_config.kv_cache_groups[1].kv_cache_spec,
        )
    )
    first, second = MagicMock(), MagicMock()
    poll = iter([QueryLoading(), QueryReady(1, b"g2-shard1", [0])])

    def query(_instance, _hashes, *, group_id=0, wait_for_full_prefix=False, **_kwargs):
        if group_id == 0:
            return QueryReady(2, b"attention")
        if not wait_for_full_prefix:
            return QueryReady(0, b"")
        return QueryReady(1, f"g{group_id}".encode(), [0])

    first.query_prefetch.side_effect = query
    second.query_prefetch.side_effect = lambda *args, **kwargs: (
        next(poll)
        if kwargs.get("group_id") == 2 and kwargs.get("wait_for_full_prefix")
        else query(*args, **kwargs)
    )
    scheduler = scheduler_for(hybrid_config, (first, second))
    assert scheduler._count_available_block_prefix([b"a", b"b"], "request") is None
    ready = scheduler._count_available_block_prefix([b"a", b"b"], "request")
    assert ready.recurrent_hold.leases == ((b"g1", b"g1"), (b"g2", b"g2-shard1"))
    assert first.query_prefetch.call_count == 5
    first.release.assert_not_called()
    second.release.assert_not_called()


@pytest.mark.parametrize("positions", [[1], [0, 0]])
def test_invalid_membership_releases_current_and_retained_lease(positions):
    first, second = MagicMock(), MagicMock()
    first.query_prefetch.return_value = QueryReady(1, b"first", [0])
    second.query_prefetch.side_effect = [
        QueryLoading(),
        QueryReady(len(positions), b"invalid", positions),
    ]
    client = TpShardQueryClient((first, second))
    assert client.query_group_membership("instance", [b"a"], "request", 1, prefetch=True) is None
    with pytest.raises(RuntimeError):
        client.query_group_membership("instance", [b"a"], "request", 1, prefetch=True)
    first.release.assert_called_once_with(b"first")
    second.release.assert_called_once_with(b"invalid")


def test_shortened_attention_lease_loads_only_selected_prefix(hybrid_config):
    worker, client, _ = _make_worker(kv_cache_config=hybrid_config, block_size=2048)
    worker._cross_layer_mode = False
    worker._registered_layers = ["attn", "gdn"]
    client.load = MagicMock(return_value=(True, "ok"))
    hold = RecurrentLoadHold(leases=((b"state",),), hit_positions=(((0,),),), checkpoint=0)
    metadata = PegaConnectorMetadata(
        load_intents={
            "request": LoadIntent(
                block_ids_by_group=((10,), (20,)),
                leases=(b"attention",),
                num_tokens=2048,
                recurrent_hold=hold,
                attention_lease_blocks=2,
            )
        }
    )
    try:
        forward = _stub_forward_context()
        forward.no_compile_layers = {"attn": MagicMock(), "gdn": MagicMock()}
        worker.start_load_kv(metadata, forward)
        assert client.load.call_args.args[-1] == [
            (b"attention", [[10, None], [None, None]]),
            (b"state", [[None], [20]]),
        ]
    finally:
        worker.shutdown()


@pytest.mark.parametrize("failure", [False, True, "exception", "malformed"])
def test_hybrid_save_barrier_and_no_forward_completion(hybrid_config, failure):
    worker, client, state = _make_worker(kv_cache_config=hybrid_config, block_size=2048)
    worker._cross_layer_mode = False
    worker._registered_layers = ["attn", "gdn"]
    client.save = MagicMock(return_value=(not failure, "save rejected"))
    if failure == "exception":
        client.save.side_effect = RuntimeError("transport failed")
    groups = ((10,), ()) if failure == "malformed" else ((10,), (20,))
    worker._current_metadata = PegaConnectorMetadata(
        save_intents={
            "request": SaveIntent(
                block_ids_by_group=groups,
                block_hashes=(b"a",),
                save_ticket=7,
            )
        }
    )
    try:
        assert worker._sync_save_on_finish
        if failure:
            with pytest.raises(RuntimeError, match="save.*(failed|mismatch)"):
                worker.get_finished({"request"})
            state.mark_unavailable.assert_called_once()
            assert worker.build_connector_worker_meta() is None
        else:
            assert worker.get_finished(set())[0] is None
            assert worker.get_finished({"request"})[0] == {"request"}
            assert client.save.call_count == 1
            assert worker._current_metadata is None
            assert worker.build_connector_worker_meta().saved_tickets == {("request", 7): {(0, 0)}}
            assert worker.build_connector_worker_meta() is None
    finally:
        worker.shutdown()


def test_hybrid_checkpoint_waits_for_every_layer_to_finish(hybrid_config):
    worker, client, _ = _make_worker(kv_cache_config=hybrid_config, block_size=2048)
    worker._cross_layer_mode = False
    worker._registered_layers = ["attn", "gdn"]
    layer_outputs = {"attn": "computed", "gdn": "pending"}
    snapshots = []

    def save(*_args):
        snapshots.append(dict(layer_outputs))
        return True, "saved"

    client.save = MagicMock(side_effect=save)
    worker._current_metadata = PegaConnectorMetadata(
        save_intents={
            "request": SaveIntent(
                block_ids_by_group=((10,), (20,)), block_hashes=(b"a",), save_ticket=7
            )
        }
    )
    try:
        worker.save_kv_layer(worker._current_metadata, "attn", None, None)
        client.save.assert_not_called()
        assert worker._current_metadata is not None
        layer_outputs["gdn"] = "computed"
        worker.save_kv_layer(worker._current_metadata, "gdn", None, None)
        client.save.assert_not_called()
        worker.wait_for_save()
        assert snapshots == [{"attn": "computed", "gdn": "computed"}]
        assert worker.build_connector_worker_meta().saved_tickets == {("request", 7): {(0, 0)}}
    finally:
        worker.shutdown()


def test_namespace_isolates_revision_and_hybrid_semantics(hybrid_config):
    config = _make_vllm_config()
    config.cache_config.block_size = 2048
    config.model_config.hf_config = SimpleNamespace(_commit_hash="revision-one")

    def namespace():
        return derive_namespace(config, 2, kv_cache_config=hybrid_config)

    previous = namespace()
    for mutate in [
        lambda: setattr(config.model_config.hf_config, "_commit_hash", "revision-two"),
        lambda: setattr(config.cache_config, "block_size", 4096),
        lambda: setattr(hybrid_config.kv_cache_groups[1].kv_cache_spec, "shapes", ((512, 8),)),
        lambda: setattr(hybrid_config.kv_cache_groups[1].kv_cache_spec, "dtypes", ("float32",)),
        lambda: setattr(
            hybrid_config.kv_cache_groups[1].kv_cache_spec, "num_speculative_blocks", 1
        ),
    ]:
        mutate()
        current = namespace()
        assert current != previous
        assert current == namespace()
        previous = current


def test_attention_group_owns_storage_zero_even_if_config_order_is_reversed(hybrid_config):
    hybrid_config.kv_cache_groups.reverse()
    layout = CacheGroupLayout.from_config(hybrid_config)
    assert layout.hash_group_index == 1
    assert layout.storage_group_ids == (1, 0)


def test_unequal_hybrid_block_sizes_are_rejected(hybrid_config):
    hybrid_config.kv_cache_groups[1].kv_cache_spec.block_size = 4096
    with pytest.raises(RuntimeError, match="common logical block size"):
        CacheGroupLayout.from_config(hybrid_config)


def test_additional_attention_group_requires_its_own_restore_contract(hybrid_config):
    hybrid_config.kv_cache_groups.append(
        SimpleNamespace(
            layer_names=("other-attention",),
            kv_cache_spec=hybrid_config.kv_cache_groups[0].kv_cache_spec,
        )
    )
    with pytest.raises(RuntimeError, match="one attention group"):
        CacheGroupLayout.from_config(hybrid_config)


def test_resolved_revision_is_used_instead_of_mutable_default():
    config = _make_vllm_config()
    config.model_config.revision = "main"
    assert resolve_model_revision(config) is None
    config.model_config.hf_config = SimpleNamespace(_commit_hash="a" * 40)
    assert resolve_model_revision(config) == "a" * 40
    config.kv_transfer_config = SimpleNamespace(
        get_from_extra_config=lambda _key, _default: "b" * 64,
    )
    assert resolve_model_revision(config) == "b" * 64


@pytest.mark.parametrize("condition", ["revision", "scheduler-block", "dcp", "pcp"])
def test_invalid_hybrid_configuration_fails_before_connecting(hybrid_config, condition):
    from pegaflow.connector import PegaKVConnector
    from vllm.distributed.kv_transfer.kv_connector.v1.base import KVConnectorRole

    config = _make_vllm_config()
    config.model_config.hf_text_config = SimpleNamespace(kv_lora_rank=None)
    config.cache_config.block_size = 2048
    config.parallel_config.tensor_parallel_size = 2
    config.parallel_config.world_size = 2
    config.kv_transfer_config = SimpleNamespace(
        engine_id="test",
        get_from_extra_config=lambda _key, default: default,
    )
    if condition != "revision":
        config.model_config.hf_config = SimpleNamespace(_commit_hash="a" * 40)
    if condition == "scheduler-block":
        config.cache_config.block_size = 128
    if condition == "dcp":
        config.parallel_config.decode_context_parallel_size = 2
    if condition == "pcp":
        config.parallel_config.prefill_context_parallel_size = 2
    with pytest.raises(ValueError, match="HMA requires"):
        PegaKVConnector(config, KVConnectorRole.SCHEDULER, hybrid_config)


def test_hybrid_hash_query_keeps_a_forward_token_at_exact_boundary(hybrid_config):
    scheduler = scheduler_for(hybrid_config, (MagicMock(), MagicMock()))
    exact = SimpleNamespace(num_tokens=4096, block_hashes=(b"a", b"b"))
    extension = SimpleNamespace(num_tokens=4097, block_hashes=(b"a", b"b"))
    assert scheduler._build_query(exact, 0) == ((b"a",), 0)
    assert scheduler._build_query(extension, 0) == ((b"a", b"b"), 0)


def test_save_ack_uses_physical_rank_and_aggregates_without_duplicates(hybrid_config):
    topology = TpShardTopology(("http://first", "http://second"), 2, 2)
    worker, client, _ = _make_worker(
        kv_cache_config=hybrid_config,
        block_size=2048,
        tp_rank=1,
        tp_size=2,
        world_size=2,
        tp_shards=topology,
    )
    worker._cross_layer_mode = False
    worker._registered_layers = ["attn", "gdn"]
    client.save = MagicMock(return_value=(True, "ok"))
    worker._current_metadata = PegaConnectorMetadata(
        save_intents={
            "request": SaveIntent(
                block_ids_by_group=((10,), (20,)),
                block_hashes=(b"a",),
                save_ticket=7,
            )
        }
    )
    try:
        worker.wait_for_save()
        assert client.save.call_args.args[1] == 0  # rank inside the node-local engine
        metadata = worker.build_connector_worker_meta()
        assert metadata.saved_tickets == {("request", 7): {(0, 1)}}
        peer = PegaWorkerMetadata({("request", 7): {(0, 0)}})
        combined = peer.aggregate(metadata).aggregate(metadata)
        assert combined.saved_tickets == {("request", 7): {(0, 0), (0, 1)}}
        assert peer.saved_tickets == {("request", 7): {(0, 0)}}
    finally:
        worker.shutdown()
