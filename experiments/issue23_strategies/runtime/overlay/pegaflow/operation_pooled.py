"""Shared physical prefill shape across equivalent instances of each role.

Restore/control/tail observations remain device-specific. No load-balancing
quota is added; pooling avoids learning a hardware-work law from only the
requests that a greedy router happens to send to one instance.
"""
from __future__ import annotations

from dataclasses import replace

from pegaflow.operation_context import ContextOperationCosts
from pegaflow.operation_cost import NonnegativeFit, prefill_features


class PooledOperationCosts(ContextOperationCosts):
    def __init__(self, original, block_tokens=128, output_tokens=128):
        super().__init__(original, block_tokens, output_tokens)
        self.role_prefill = [NonnegativeFit(4), NonnegativeFit(4)]

    def observe(self, node, result, wire, completed_seconds, *, calibration=False):
        with self.guard:
            super().observe(node, result, wire, completed_seconds, calibration=calibration)
            raw = result["raw_metrics"]
            self.role_prefill[node//2].add(
                prefill_features(result["prompt_tokens"], result["num_cached_tokens"], self.block_tokens),
                raw["first_token_ts"]-raw["scheduled_ts"])

    def freeze(self, now):
        with self.guard:
            frozen = super().freeze(now)
            return replace(frozen, fits=tuple((row[0], self.role_prefill[node//2].snapshot(), *row[2:])
                for node, row in enumerate(frozen.fits)))

    def snapshot(self):
        with self.guard:
            return {**super().snapshot(), "kind": "same-hardware-role-shared-physical-prefill-NNLS",
                "role_prefill": [f.snapshot() for f in self.role_prefill],
                "device_specific": ["restore", "control", "post", "decode_context"],
                "role_shared_only": "physical prefill+sync work features; a falsifiable common-shape assumption",
                "load_balance_quota_or_task_rate_exception": False}


class AffinePooledOperationCosts(ContextOperationCosts):
    """Common role work slopes with separately identified node intercepts."""
    def __init__(self, original, block_tokens=128, output_tokens=128):
        super().__init__(original, block_tokens, output_tokens)
        self.role_affine = [NonnegativeFit(5), NonnegativeFit(5)]
        self.node_samples = [0] * 4

    def observe(self, node, result, wire, completed_seconds, *, calibration=False):
        with self.guard:
            super().observe(node, result, wire, completed_seconds, calibration=calibration)
            raw = result["raw_metrics"]
            features = prefill_features(result["prompt_tokens"], result["num_cached_tokens"], self.block_tokens)
            self.role_affine[node//2].add((float(node % 2 == 0), float(node % 2 == 1), *features[1:]),
                raw["first_token_ts"]-raw["scheduled_ts"])
            self.node_samples[node] += 1

    def freeze(self, now):
        with self.guard:
            frozen = super().freeze(now)
            rows = []
            for node, row in enumerate(frozen.fits):
                values = self.role_affine[node//2].snapshot()
                beta = values["coefficients"]
                values["coefficients"] = (beta[node % 2], *beta[2:]) if beta else None
                values["node_intercept_observations"] = self.node_samples[node]
                rows.append((row[0], values, *row[2:]))
            return replace(frozen, fits=tuple(rows))

    def snapshot(self):
        with self.guard:
            return {**super().snapshot(), "kind": "same-role-physical-slopes-plus-node-intercept-NNLS",
                "role_affine": [f.snapshot() for f in self.role_affine], "node_samples": self.node_samples,
                "features": ["node0_intercept", "node1_intercept", "uncached_tokens",
                             "causal_attention_pairs", "prefill_chunks"],
                "device_specific": ["prefill_sync_intercept", "restore", "control", "post", "decode_context"],
                "load_balance_quota_or_tuned_shrinkage_penalty": False}
