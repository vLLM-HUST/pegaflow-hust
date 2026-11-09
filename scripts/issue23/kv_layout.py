"""Per-worker KV page geometry for cache capacities and transfer payloads."""

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class KVLayout:
    block_tokens: int = 128
    bytes_per_block: int = 7340032
    cache_domain: str = "Qwen2.5-7B-Instruct:BF16"

    @classmethod
    def from_profile(cls, profile):
        spec = profile.get("kv_layout")
        if spec is None:
            metadata = [
                value.get("metadata", {})
                for value in profile.get("models", {}).values()
            ]
            models = {value.get("model", "").split("/")[-1] for value in metadata}
            if models - {"", "Qwen2.5-7B-Instruct"}:
                raise ValueError(
                    "Supply kv_layout for a different model's cache geometry"
                )
            for value in metadata:
                dtype = str(value.get("kv_cache_dtype", value.get("dtype", ""))).lower()
                if dtype == "auto":
                    dtype = str(value.get("dtype", "")).lower()
                if dtype not in {"", "bf16", "bfloat16", "torch.bfloat16"}:
                    raise ValueError("Supply kv_layout for a different KV dtype")
            return cls()
        allowed = {"block_tokens", "bytes_per_block", "cache_domain", "dense_attention"}
        if set(spec) - allowed:
            raise ValueError(
                "Unknown KV layout fields: " + str(sorted(set(spec) - allowed))
            )
        values = {key: value for key, value in spec.items() if key != "dense_attention"}
        tokens = values.get("block_tokens", 128)
        if not isinstance(tokens, int) or isinstance(tokens, bool) or tokens < 1:
            raise ValueError("block_tokens requires a positive integer")
        if "cache_domain" not in spec:
            raise ValueError(
                "Explicit kv_layout requires a cache_domain identifying model and KV dtype"
            )
        if "dense_attention" in spec:
            shape = spec["dense_attention"]
            required = {
                "layers_per_worker",
                "kv_heads_per_worker",
                "head_dim",
                "dtype_bytes",
            }
            if set(shape) != required:
                raise ValueError(
                    "dense_attention requires per-worker layers, KV heads, head_dim and dtype_bytes"
                )
            if any(
                not isinstance(value, int) or isinstance(value, bool) or value < 1
                for value in shape.values()
            ):
                raise ValueError("Dense attention geometry requires positive integers")
            size = tokens * 2
            for value in shape.values():
                size *= value
            if "bytes_per_block" in values and values["bytes_per_block"] != size:
                raise ValueError(
                    "KV block bytes disagree with dense attention geometry"
                )
            values["bytes_per_block"] = size
        elif "bytes_per_block" not in values:
            raise ValueError("Supply bytes_per_block or dense_attention")
        for field in ["block_tokens", "bytes_per_block"]:
            value = values.get(field, 128)
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ValueError(field + " requires a positive integer")
        if not isinstance(
            values.get("cache_domain", cls.cache_domain), str
        ) or not values.get("cache_domain", cls.cache_domain):
            raise ValueError("cache_domain requires a nonempty string")
        return cls(**values)

    def to_dict(self):
        return asdict(self)
