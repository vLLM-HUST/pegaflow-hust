"""Explicit opt-in engine request contract; legacy workers fail closed.

Both policies must share prefix-authority and memory-budget settings. This
helper does not silently switch GPU-cache authority or install a worker.
"""


def sampling_extra(body, block_tokens):
    if type(block_tokens) is not int or block_tokens <= 0:
        raise ValueError("Actual positive engine block size required")
    if type(body.get("policy_native_cache")) is not bool:
        raise ValueError("The serving adapter must explicitly declare its cache mode")
    if body["policy_native_cache"]:
        if any(key in body for key in ("max_prefix_tokens", "max_prefix_blocks", "commit_generation")):
            raise ValueError("A native cache request must not carry a policy restore cap")
        return {}
    tokens = body.get("max_prefix_tokens")
    if type(tokens) is not int or tokens < 0 or tokens % block_tokens:
        raise ValueError("Policy token boundary must align to actual engine blocks")
    if "max_prefix_blocks" in body:
        raise ValueError("Do not mix token and block units at the policy boundary")
    return {"kv_transfer_params": {"pegaflow_joint": {
        "max_prefix_blocks": tokens // block_tokens, "local_only": True}}}


def validate_serving_capabilities(capabilities):
    required = ("native_lookup_without_policy_cap", "controlled_local_boundary",
                "actual_engine_hash_preview", "same_gpu_prefix_authority_both_arms",
                "actual_RDMA_counter_with_sources")
    if not all(capabilities.get(key) is True for key in required):
        raise ValueError("Legacy worker cannot certify the native/controlled comparison")
