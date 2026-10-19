"""Same-model prefill work learned across P/D, with node API intercepts.

Total API-to-first minus measured resident restore is directly observable;
no cold wire interval is fabricated as a GPU-only timing measurement.
"""
from __future__ import annotations

from dataclasses import replace

from pegaflow.operation_context import ContextOperationCosts
from pegaflow.operation_cost import NonnegativeFit, prefill_features
from pegaflow.resource_headroom_profile import HeadroomOperationCosts


class UnifiedPrefillOperationCosts(ContextOperationCosts):
    def __init__(self, original, block_tokens=128, output_tokens=128):
        super().__init__(original, block_tokens, output_tokens)
        self.api_prefill = NonnegativeFit(7)
        self.original_cold = []

    def add_api_body(self, node, tokens, prefix, seconds):
        shape = prefill_features(tokens, prefix, self.block_tokens)
        self.api_prefill.add((*[float(node == n) for n in range(4)], *shape[1:]), seconds)

    def observe(self, node, result, wire, completed_seconds, *, calibration=False):
        with self.guard:
            super().observe(node, result, wire, completed_seconds, calibration=calibration)
            raw = result["raw_metrics"]
            restore = raw["scheduled_ts"]-raw["queued_ts"]
            body = wire["first_seconds"]-restore
            if body < 0:
                raise ValueError("API first interval cannot be shorter than its measured restore")
            self.add_api_body(node, result["prompt_tokens"], result["num_cached_tokens"], body)

    def seed_original_cold(self, source, calibration):
        with self.guard:
            if self.original_cold or len(calibration) != 32 or any(not r["calibration"] for r in self.records):
                raise ValueError("Only the original independent calibration may supply cold donors")
            if source["heldout_data_used"] is not False:
                raise ValueError("Heldout measurements must not initialize a policy")
            pools = {int(node): [tuple(v) for v in values] for node, values in source["samples"].items()}
            for value in calibration:
                for role in ("P", "D"):
                    result = value[role]
                    node = value["lane"]+(2 if role == "D" else 0)
                    sample = (result["uncached_prompt_tokens"], result["num_cached_tokens"]*self.original.bytes_per_token,
                              value[role+"_wire"]["complete_seconds" if role == "P" else "first_seconds"])
                    pools[node].remove(sample)
            if pools[0] or pools[1] or len(pools[2]) != 1 or len(pools[3]) != 1:
                raise ValueError("Cost source does not match the recorded one-cold-per-D calibration contract")
            for node in (2, 3):
                tokens, resident_bytes, seconds = pools[node][0]
                if resident_bytes != 0:
                    raise ValueError("A donor cannot invent an unobserved restore decomposition")
                self.add_api_body(node, tokens, 0, seconds)
                self.original_cold.append(dict(node=node, prompt_tokens=tokens, first_wire_seconds=seconds,
                    calibration=True, raw_stage_decomposition_claimed=False))

    def freeze(self, now):
        with self.guard:
            frozen = super().freeze(now)
            shared = self.api_prefill.snapshot()
            beta = shared["coefficients"]
            fits = tuple((row[0], {**shared, "coefficients": (beta[node], *beta[4:]) if beta else None},
                          0.0, *row[3:]) for node, row in enumerate(frozen.fits))
            return replace(frozen, fits=fits, version=frozen.version+len(self.original_cold))

    def snapshot(self):
        with self.guard:
            return {**super().snapshot(), "kind": "same-model-cross-role-prefill-work-plus-node-API-intercepts",
                "api_prefill": self.api_prefill.snapshot(), "original_cold_donors": self.original_cold,
                "target": "first_wire_seconds minus measured resident restore; includes API/control, not isolated kernel",
                "restore_control_double_counted": False,
                "cold_seed_scope": "two existing isolated-calibration warmups already in original frozen cost file"}


class UnifiedPrefillHeadroomCosts(HeadroomOperationCosts, UnifiedPrefillOperationCosts):
    """Orthogonal known-write safeguard and physically shared API-path model."""
