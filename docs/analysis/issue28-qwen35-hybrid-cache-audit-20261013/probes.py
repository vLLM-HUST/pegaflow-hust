"""Source-only diagnostics for the Issue 28 mixed-cache code audit."""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

parser = argparse.ArgumentParser()
parser.add_argument("--repo", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
sys.path.insert(0, str(args.repo / "python"))

from tests.unit_stubs import install_connector_unit_stubs

install_connector_unit_stubs()

from pegaflow.connector.common import RecurrentLoadHold, derive_namespace
from pegaflow.connector.scheduler import SchedulerConnector, _QueryProbe
from pegaflow.connector.tp_shards import ShardedQueryReady, TpShardQueryClient
from pegaflow.connector.worker import _infer_kv_cache_registration
from pegaflow.pegaflow import QueryReady
from tests.test_worker_kv_layout import FakeTensor

rows, kernel_tokens, heads, head_dim, element_bytes = 160, 128, 1, 256, 2
registration = _infer_kv_cache_registration(
    FakeTensor(
        shape=(rows, kernel_tokens, heads, head_dim),
        stride=(kernel_tokens * heads * head_dim, heads * head_dim, head_dim, 1),
        element_size=element_bytes,
    ),
    logical_block_size=2048,
    is_mla=False,
)
layout = {
    "input_shape": [rows, kernel_tokens, heads, head_dim],
    "logical_tokens": 2048,
    "actual": dataclasses.asdict(registration),
    "expected_for_contiguous_split_attention_plane": {
        "num_blocks": 10,
        "bytes_per_block": 2048 * heads * head_dim * element_bytes,
        "physical_blocks_per_logical_block": 16,
    },
    "logical_block_id_1_actual_byte_offset": registration.bytes_per_block,
    "logical_block_id_1_expected_byte_offset": 2048 * heads * head_dim * element_bytes,
}
assert registration.num_blocks == 160
assert registration.bytes_per_block == 65536
assert registration.physical_blocks_per_logical_block == 1

ctx = SimpleNamespace(virtual_block_size=2048)
scheduler = object.__new__(SchedulerConnector)
scheduler._ctx = ctx
scheduler._cache_groups = SimpleNamespace(group_count=2, recurrent_group_indices={1})
scheduler._block_hashes = {"request": (b"boundary-2048", b"boundary-4096")}
scheduler._next_stored_block_idx = {}
scheduler._block_index_offsets = {}
save = scheduler._consume_finished_hma_save(
    "request", ([100, 101, 102], [0, 201, 202]), written=4100
)
checkpoint = {
    "written_tokens": 4100,
    "block_tokens": 2048,
    "attention_block_ids": [100, 101, 102],
    "recurrent_block_ids": [0, 201, 202],
    "save_block_ids_by_group": save.block_ids_by_group,
    "save_hashes": [value.decode() for value in save.block_hashes],
    "pinned_vllm_running_state_table_index": (4100 - 1) // 2048,
    "boundary_4096_table_index": (4096 - 1) // 2048,
    "scope": "metadata selection and pinned-index arithmetic; no tensor contents measured",
}
assert save.block_ids_by_group[1] == (0, 202)
assert save.block_hashes[-1] == b"boundary-4096"
scheduler._next_stored_block_idx = {}
try:
    scheduler._consume_finished_hma_save(
        "request", ([100, 101], [0, 201]), written=4096
    )
except RuntimeError as error:
    checkpoint["aligned_finish_error"] = str(error)
else:
    raise AssertionError("Expected the exact-boundary final table to be rejected")

probe = _QueryProbe(computed_blocks=0, query_hashes=(b"h0", b"h1"))
probe.mark_ready(
    ShardedQueryReady(
        num_hit_blocks=2,
        leases=(b"two-block-attention-lease",),
        recurrent_hold=RecurrentLoadHold(
            leases=((b"recurrent-membership-lease",),),
            hit_positions=(((0, 1),),),
            checkpoint=1,
        ),
        usable_positions=(0, 1),
    )
)
scheduler._pending_query_probes = {"clamp": probe}
scheduler._external_matched_blocks = {}
scheduler._tp_shard_client = MagicMock()
hit_tokens, async_load = scheduler._finish_cache_lookup(
    req_id="clamp", num_tokens=4096, probe=probe, lookup_us=None, reused=False
)
clamp = {
    "prompt_tokens": 4096,
    "block_tokens": 2048,
    "original_attention_lease_blocks": 2,
    "matched_tokens_after_logits_clamp": hit_tokens,
    "probe_hit_blocks_after_clamp": probe.hit_blocks,
    "probe_attention_lease_after_clamp": [value.decode() for value in probe.leases],
    "attention_requery_calls": scheduler._tp_shard_client.query.call_count,
    "async_load": async_load,
    "scope": "scheduler metadata; real engine validates lease size equals destination vector size",
}
assert hit_tokens == 2048 and async_load
assert probe.hit_blocks == 1
assert probe.leases == (b"two-block-attention-lease",)
assert scheduler._tp_shard_client.query.call_count == 0


def config(block_tokens: int, revision: str):
    return SimpleNamespace(
        model_config=SimpleNamespace(
            model="/data/shared_upload/models/vspec/Qwen3.5-35B-A3B",
            revision=revision,
            dtype="bfloat16",
            get_total_num_kv_heads=lambda: 2,
            get_head_size=lambda: 256,
            get_total_num_hidden_layers=lambda: 40,
        ),
        cache_config=SimpleNamespace(cache_dtype="auto", block_size=block_tokens),
        parallel_config=SimpleNamespace(pipeline_parallel_size=1),
        additional_config={},
    )


namespaces = {
    "128_tokens_revision_a": derive_namespace(config(128, "a"), tp_size=2),
    "2048_tokens_revision_a": derive_namespace(config(2048, "a"), tp_size=2),
    "2048_tokens_revision_b": derive_namespace(config(2048, "b"), tp_size=2),
}
assert len(set(namespaces.values())) == 1

first, second = MagicMock(), MagicMock()
good, invalid = QueryReady(1, b"first"), QueryReady(1, b"invalid")
good.hit_positions = (0,)
invalid.hit_positions = (1,)
first.query_prefetch.return_value = good
second.query_prefetch.return_value = invalid
client = TpShardQueryClient((first, second))
try:
    client.query_group_membership("instance", [b"one-hash"], "request:g1", 1)
except RuntimeError as error:
    validation_error = str(error)
else:
    raise AssertionError("Expected membership validation to reject the response")
lease = {
    "error": validation_error,
    "first_lease_released": first.release.call_count == 1,
    "invalid_response_lease_released": second.release.call_count == 1,
    "scope": "malformed Ready fault injection; ordinary server emits valid positions",
}
assert lease["first_lease_released"]
assert not lease["invalid_response_lease_released"]

result = {
    "schema_version": 1,
    "evidence_type": "source-only diagnostic; no model inference or RDMA",
    "non_mla_split_registration": layout,
    "finish_checkpoint_metadata": checkpoint,
    "hybrid_logits_clamp_lease": clamp,
    "namespace_collision": namespaces,
    "membership_validation_lease_cleanup": lease,
}
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(
    json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
)
print(json.dumps(result, ensure_ascii=False, indent=2))
