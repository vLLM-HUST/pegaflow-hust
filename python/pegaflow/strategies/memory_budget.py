"""Explicit 910B2/Qwen2.5 BF16 capacity scenario, not a new device measurement.

Runtime-visible total and weight bytes have archived provenance. Runtime peak
is unknown without an NPU; 4 GiB is a capacity assumption, not a fitted latency
parameter. See experiments/issue23_strategies/memory/910b2-qwen25-bf16.json.
Real serving omits kv_cache_memory_bytes and uses vLLM's memory profiler.
"""

from dataclasses import asdict, dataclass

GIB = 1024**3


@dataclass(frozen=True)
class Ascend910B2Budget:
    runtime_visible_bytes: int = 65452113920
    utilization_numerator: int = 9
    utilization_denominator: int = 10
    weights_bytes: int = 15231233024
    runtime_reserve_bytes: int = 4 * GIB
    block_tokens: int = 128
    bytes_per_token: int = 57344

    def __post_init__(self):
        for value in asdict(self).values():
            if type(value) is not int or value <= 0:
                raise ValueError("Memory budget requires positive integer byte/count fields")
        if self.utilization_numerator > self.utilization_denominator:
            raise ValueError("Device budget exceeds its visible memory")
        if self.kv_bytes <= 0:
            raise ValueError("No memory remains for KV")

    @property
    def kv_bytes(self):
        executor = (
            self.runtime_visible_bytes * self.utilization_numerator // self.utilization_denominator
        )
        block = self.block_tokens * self.bytes_per_token
        return (executor - self.weights_bytes - self.runtime_reserve_bytes) // block * block

    def report(self, max_sequences=4, max_model_len=32768, output_tokens=128):
        # Deliberately overestimates: a real max_model_len normally includes output.
        blocks = (max_model_len + output_tokens + self.block_tokens - 1) // self.block_tokens
        bound = max_sequences * blocks * self.block_tokens * self.bytes_per_token
        return {
            **asdict(self),
            "kv_bytes": self.kv_bytes,
            "kv_gib": self.kv_bytes / GIB,
            "max_active_reservation_bound_bytes": bound,
            "nonbinding_for_active_sequences": self.kv_bytes >= bound,
            "profiled_current_device": False,
            "GPU_persistent_prefix_cache_modelled": False,
        }


def serving_kwargs():
    """Normal allocation: profile non-KV memory instead of fixing KV bytes."""
    return {"gpu_memory_utilization": 0.9}
