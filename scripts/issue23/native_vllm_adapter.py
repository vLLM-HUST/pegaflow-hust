"""Run the installed vLLM 0.23 scheduler with simulated model/connector outputs.

Runtime imports are lazy: ordinary simulator use does not require torch or vLLM.
The adapter never replaces native scheduling, block allocation or preemption.
"""

import hashlib
import importlib.metadata
import inspect
import os
from contextvars import ContextVar
from pathlib import Path

_CONNECTOR_CONTEXT = ContextVar("pegaflow_simulation_connector", default=None)
_HASH_INITIALIZED = False
_HASH_SEED = None


def _register_bridge_type():
    if "SimulationKVConnector" in globals():
        return
    from vllm.distributed.kv_transfer.kv_connector.v1.base import (
        KVConnectorBase_V1,
        KVConnectorMetadata,
        KVConnectorRole,
        SupportsHMA,
    )

    class SimulationKVConnector(KVConnectorBase_V1, SupportsHMA):
        def __init__(self, vllm_config, role, kv_cache_config):
            super().__init__(vllm_config, role, kv_cache_config)
            self.callbacks = _CONNECTOR_CONTEXT.get()
            if role != KVConnectorRole.SCHEDULER or self.callbacks is None:
                raise RuntimeError("SimulationKVConnector requires a simulator scheduler context")

        def get_num_new_matched_tokens(self, request, num_computed_tokens):
            return self.callbacks.query(request, num_computed_tokens)

        def update_state_after_alloc(self, request, blocks, num_external_tokens):
            self.callbacks.allocated(request, blocks, num_external_tokens)

        def build_connector_meta(self, scheduler_output):
            return KVConnectorMetadata()

        def request_finished(self, request, block_ids):
            return self.callbacks.finished(request, block_ids), None

        def request_finished_all_groups(self, request, block_ids):
            if len(block_ids) != 1:
                raise ValueError("Native simulation requires one full-attention KV group")
            return self.request_finished(request, block_ids[0])

        def start_load_kv(self, *args, **kwargs):
            raise RuntimeError("Simulation connector has no inference worker")

        def wait_for_layer_load(self, *args, **kwargs):
            raise RuntimeError("Simulation connector has no inference worker")

        def save_kv_layer(self, *args, **kwargs):
            raise RuntimeError("Simulation connector has no inference worker")

        def wait_for_save(self):
            raise RuntimeError("Simulation connector has no inference worker")

    globals()["SimulationKVConnector"] = SimulationKVConnector


class NativeVllmEngine:
    """One native scheduler and its authoritative KV block pool, without a model."""

    def __init__(
        self,
        *,
        model_path,
        num_blocks,
        block_tokens=128,
        max_sequences=128,
        token_budget=2048,
        max_model_len=32768,
        enable_prefix_caching=True,
        scheduler_options=None,
        transfer=None,
    ):
        global _HASH_INITIALIZED, _HASH_SEED
        try:
            version = importlib.metadata.version("vllm")
        except importlib.metadata.PackageNotFoundError as e:
            raise RuntimeError(
                "Native scheduler backend requires an installed vLLM 0.23 runtime"
            ) from e
        if version.split(".")[:2] != ["0", "23"]:
            raise RuntimeError(f"Native scheduler adapter targets vLLM 0.23; found {version}")
        import torch
        from vllm.config import (
            CacheConfig,
            KVTransferConfig,
            ModelConfig,
            ParallelConfig,
            SchedulerConfig,
            VllmConfig,
        )
        from vllm.utils.hashing import sha256
        from vllm.v1.core.kv_cache_utils import get_request_block_hasher, init_none_hash
        from vllm.v1.core.sched.scheduler import Scheduler
        from vllm.v1.core.single_type_kv_cache_manager import register_all_kvcache_specs
        from vllm.v1.kv_cache_interface import (
            FullAttentionSpec,
            KVCacheConfig,
            KVCacheGroupSpec,
        )
        from vllm.v1.structured_output import StructuredOutputManager

        self.version = version
        self.block_tokens = block_tokens
        self.usable_blocks = num_blocks - 1
        self.max_model_len = max_model_len
        self.pending_recvs = set()
        self.pending_sends = set()
        self.pending_output = None
        self.requests = {}
        self.preemptions = 0
        model = ModelConfig(
            model=model_path,
            dtype="bfloat16",
            max_model_len=max_model_len,
            skip_tokenizer_init=True,
        )
        if (
            model.is_encoder_decoder
            or model.is_multimodal_model
            or getattr(model.hf_text_config, "sliding_window", None)
            or any(
                kind != "full_attention"
                for kind in getattr(model.hf_text_config, "layer_types", []) or []
            )
        ):
            raise ValueError(
                "Native simulation supports text decoders with a single full-attention KV group"
            )
        options = dict(scheduler_options or {})
        reserved = {
            "max_num_seqs",
            "max_num_batched_tokens",
            "max_model_len",
            "is_encoder_decoder",
            "async_scheduling",
            "scheduler_cls",
        }
        if set(options) & reserved:
            raise ValueError(
                "Use engine budgets and synchronous simulation settings for reserved scheduler fields"
            )
        valid = set(inspect.signature(SchedulerConfig).parameters)
        if set(options) - valid:
            raise ValueError(
                "Unsupported installed scheduler options: " + str(sorted(set(options) - valid))
            )
        options.setdefault("enable_chunked_prefill", True)
        scheduling = SchedulerConfig(
            max_num_seqs=max_sequences,
            max_num_batched_tokens=token_budget,
            max_model_len=max_model_len,
            is_encoder_decoder=False,
            async_scheduling=False,
            **options,
        )
        cache = CacheConfig(
            block_size=block_tokens,
            gpu_memory_utilization=0.9,
            enable_prefix_caching=enable_prefix_caching,
        )
        cache.num_gpu_blocks = num_blocks
        transfer_config = None
        if transfer is not None:
            _register_bridge_type()
            transfer_config = KVTransferConfig(
                kv_connector="SimulationKVConnector",
                kv_role="kv_both",
                kv_connector_module_path=__name__,
            )
        config = VllmConfig(
            model_config=model,
            scheduler_config=scheduling,
            cache_config=cache,
            parallel_config=ParallelConfig(),
            kv_transfer_config=transfer_config,
        )
        # Scheduling uses logical page counts; no layer tensors are allocated.
        kv_config = KVCacheConfig(
            num_blocks=num_blocks,
            kv_cache_tensors=[],
            kv_cache_groups=[
                KVCacheGroupSpec(
                    ["simulation.attention"],
                    FullAttentionSpec(
                        block_size=block_tokens,
                        num_kv_heads=1,
                        head_size=1,
                        dtype=torch.bfloat16,
                    ),
                )
            ],
        )
        register_all_kvcache_specs(config)
        context = _CONNECTOR_CONTEXT.set(transfer)
        try:
            self.scheduler = Scheduler(
                vllm_config=config,
                kv_cache_config=kv_config,
                structured_output_manager=StructuredOutputManager(config),
                block_size=block_tokens,
                log_stats=False,
            )
        finally:
            _CONNECTOR_CONTEXT.reset(context)
        if not _HASH_INITIALIZED:
            init_none_hash(sha256)
            _HASH_SEED = os.environ.get("PYTHONHASHSEED")
            _HASH_INITIALIZED = True
        self.token_hasher = get_request_block_hasher(block_tokens, sha256)
        source = Path(inspect.getsourcefile(Scheduler))
        self.identity = {
            "backend": "vllm",
            "version": version,
            "hash_seed": _HASH_SEED,
            "scheduler_class": Scheduler.__module__ + "." + Scheduler.__qualname__,
            "scheduler_source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
            "scheduler_options": options,
            "scheduler_reserve_full_isl": getattr(
                self.scheduler, "scheduler_reserve_full_isl", None
            ),
            "execution": "native scheduler with simulated model outputs; no model kernel execution",
        }

    def add(
        self,
        request_id,
        prompt_tokens,
        output_tokens,
        arrival_s,
        *,
        prefix_hashes=None,
        prompt_token_ids=None,
        cache_salt=None,
        priority=0,
        native_prefix_hashes=None,
    ):
        from vllm.sampling_params import SamplingParams
        from vllm.v1.request import Request

        if prompt_token_ids is not None and len(prompt_token_ids) != prompt_tokens:
            raise ValueError("Native prompt token IDs differ from prompt length")
        if (
            prompt_tokens < 1
            or output_tokens < 1
            or prompt_tokens + output_tokens > self.max_model_len
        ):
            raise ValueError("Request token lengths exceed the native model context")
        required_blocks = (prompt_tokens + output_tokens - 2) // self.block_tokens + 1
        if required_blocks > self.usable_blocks:
            raise ValueError(
                "Request cannot fit the native KV pool including its reserved null page"
            )
        params = SamplingParams(max_tokens=output_tokens, temperature=0, ignore_eos=True)
        params.update_from_generation_config({}, 0)
        known = list(prefix_hashes or [])

        def identity_hasher(request):
            result = []
            for index in range(len(request.block_hashes), request.num_tokens // self.block_tokens):
                value = known[index] if index < len(known) else f"generated:{request_id}:{index}"
                try:
                    raw = bytes.fromhex(value)
                    if len(raw) != 32:
                        raise ValueError("not a 32-byte identity")
                except ValueError:
                    raw = hashlib.sha256(value.encode()).digest()
                if cache_salt is not None:
                    raw = hashlib.sha256(str(cache_salt).encode() + b"\x00" + raw).digest()
                result.append(raw)
            return result

        request = Request(
            request_id=request_id,
            prompt_token_ids=list(prompt_token_ids)
            if prompt_token_ids is not None
            else [0] * prompt_tokens,
            sampling_params=params,
            pooling_params=None,
            arrival_time=arrival_s,
            cache_salt=cache_salt,
            priority=priority,
            block_hasher=self.token_hasher if prompt_token_ids is not None else identity_hasher,
        )
        if native_prefix_hashes is not None:
            if len(native_prefix_hashes) != len(request.block_hashes):
                raise ValueError("Recorded native prefix hash count differs")
            request.block_hashes[:] = [bytes.fromhex(value) for value in native_prefix_hashes]
        self.requests[request_id] = request
        self.scheduler.add_request(request)
        return request

    def signal_transfer(self, request_id, *, sending=False):
        (self.pending_sends if sending else self.pending_recvs).add(request_id)

    def flush_transfers(self):
        if self.pending_output is not None or not (self.pending_recvs or self.pending_sends):
            return
        from vllm.v1.core.sched.output import SchedulerOutput
        from vllm.v1.outputs import KVConnectorOutput, ModelRunnerOutput

        output = ModelRunnerOutput(
            req_ids=[],
            req_id_to_index={},
            kv_connector_output=KVConnectorOutput(
                finished_recving=set(self.pending_recvs),
                finished_sending=set(self.pending_sends),
            ),
        )
        self.pending_recvs.clear()
        self.pending_sends.clear()
        self.scheduler.update_from_output(SchedulerOutput.make_empty(), output)
        for key in list(self.requests):
            if key not in self.scheduler.requests:
                del self.requests[key]

    def schedule(self):
        if self.pending_output is not None:
            raise RuntimeError("Complete the preceding native iteration before scheduling again")
        self.flush_transfers()
        before = {key: r.num_preemptions for key, r in self.scheduler.requests.items()}
        output = self.scheduler.schedule()
        self.pending_output = output
        self.preemptions += sum(
            max(0, r.num_preemptions - before.get(key, 0))
            for key, r in self.scheduler.requests.items()
        )
        assignments = [
            (key, count, self.scheduler.requests[key].num_computed_tokens - count)
            for key, count in output.num_scheduled_tokens.items()
        ]
        return assignments

    def complete(self, sampled=None):
        from vllm.v1.outputs import KVConnectorOutput, ModelRunnerOutput

        output = self.pending_output
        if output is None:
            raise RuntimeError("There is no pending native iteration")
        ids = list(output.num_scheduled_tokens)
        samples = []
        refs = {key: self.scheduler.requests[key] for key in ids}
        for key in ids:
            request = refs[key]
            generated = [1000] if request.num_computed_tokens >= request.num_tokens else []
            samples.append(list(sampled.get(key, [])) if sampled is not None else generated)
        connector = None
        if self.pending_recvs or self.pending_sends:
            connector = KVConnectorOutput(
                finished_recving=set(self.pending_recvs),
                finished_sending=set(self.pending_sends),
            )
            self.pending_recvs.clear()
            self.pending_sends.clear()
        result = self.scheduler.update_from_output(
            output,
            ModelRunnerOutput(
                req_ids=ids,
                req_id_to_index={key: i for i, key in enumerate(ids)},
                sampled_token_ids=samples,
                kv_connector_output=connector,
            ),
        )
        self.pending_output = None
        states = {
            key: {
                "computed": request.num_computed_tokens,
                "outputs": request.num_output_tokens,
                "preemptions": request.num_preemptions,
                "finished": request.is_finished(),
            }
            for key, request in refs.items()
        }
        for key in refs:
            if key not in self.scheduler.requests:
                self.requests.pop(key, None)
        return {
            "finished": [key for key, request in refs.items() if request.is_finished()],
            "sampled": dict(zip(ids, samples, strict=True)),
            "native_outputs": result,
            "states": states,
        }

    def snapshot(self):
        return {
            key: {
                "computed": r.num_computed_tokens,
                "outputs": r.num_output_tokens,
                "status": r.status.name,
                "preemptions": r.num_preemptions,
                "block_ids": self.scheduler.kv_cache_manager.get_block_ids(key),
            }
            for key, r in self.scheduler.requests.items()
        }

    def shutdown(self):
        self.scheduler.shutdown()


class NativePoolView:
    """Read-only simulation accounting over vLLM's authoritative block pool."""

    def __init__(self, engine):
        self.engine = engine
        self.block_size = engine.block_tokens
        self.evictions = None

    @property
    def blocks(self):
        return self.engine.scheduler.kv_cache_manager.block_pool.blocks

    @property
    def cached(self):
        return self.engine.scheduler.kv_cache_manager.block_pool.cached_block_hash_to_block._cache

    def release(self, job):
        self.engine.signal_transfer(job.request.id, sending=True)
