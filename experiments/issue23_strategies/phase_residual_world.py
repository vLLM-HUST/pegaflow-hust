"""Joint phase allocation of retained calibration residuals, not a DMA model.

The caller retains the original total first/tail durations. This module changes
where their variation occupies an exclusive restore/save gate versus shared
iteration service. Its labels belong to the execution world, never the policy.
"""

import hashlib


def joint_phase_split(seed, world, key, node, prefix, first, tail):
    restore = min(first, world.restore(node, prefix)) if prefix else 0.0
    save = min(tail, world.fits[node][4]) if node >= 2 else 0.0
    candidates = [
        (result, wire)
        for n, result, wire in seed
        if n == node and bool(result["num_cached_tokens"]) == bool(prefix)
    ]
    selected = None
    if candidates:
        # Exactly the same common-random-number row as the aggregate world.
        index = int.from_bytes(
            hashlib.sha256((key + ":" + str(node)).encode()).digest()[:8], "big"
        ) % len(candidates)
        result, wire = candidates[index]
        raw = result["raw_metrics"]
        queued, scheduled, first_at, last = (
            raw[field]
            for field in (
                "queued_ts",
                "scheduled_ts",
                "first_token_ts",
                "last_token_ts",
            )
        )
        if not queued <= scheduled <= first_at <= last:
            raise ValueError("Joint residuals require ordered completed stage metrics")
        if prefix:
            residual = (
                scheduled - queued - world.restore(node, result["num_cached_tokens"])
            )
            restore = min(first, max(0.0, world.restore(node, prefix) + residual))
        if node >= 2:
            # The retained post-response mean is constant, so mean + this
            # sample's residual equals its observed post interval.
            observed_post = max(
                0.0,
                wire["complete_seconds"] - wire["first_seconds"] - (last - first_at),
            )
            save = min(tail, observed_post)
        selected = result["id"]
    return (
        restore,
        save,
        {
            "request": key,
            "node": node,
            "prefix": prefix,
            "calibration_id": selected,
            "first_total": first,
            "tail_total": tail,
            "restore_path": restore,
            "remaining_first_path": first - restore,
            "decode_path": tail - save,
            "finish_path": save,
            "not_isolated_DMA_or_save_kernel": True,
        },
    )
