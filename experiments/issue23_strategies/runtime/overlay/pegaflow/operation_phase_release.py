"""Observable API phases; no invented RDMA congestion or H2D penalty."""

from dataclasses import dataclass

from pegaflow.operation_context import decode_features
from pegaflow.operation_cost import NonnegativeFit
from pegaflow.resource_headroom_profile import HeadroomOperationCosts


@dataclass(frozen=True)
class PhaseCosts:
    current: object
    api_decode: tuple
    output_tokens: int

    def __getattr__(self, name):
        return getattr(self.current, name)

    def decode_tail(self, node, tokens):
        if node not in (2, 3):
            raise ValueError("Only D owns a post-first decode phase")
        coefficients = self.api_decode[node - 2]["coefficients"]
        if coefficients is None:
            return self.current.decode_tail(node, tokens)
        context = decode_features(tokens, self.output_tokens)[1]
        return coefficients[0] + coefficients[1] * context


class PhaseReleaseCosts(HeadroomOperationCosts):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api_decode = [NonnegativeFit(2), NonnegativeFit(2)]
        self.phase_calibration = []

    def observe(self, node, result, wire, completed_seconds, *, calibration=False):
        with self.guard:
            super().observe(node, result, wire, completed_seconds, calibration=calibration)
            if calibration:
                self.phase_calibration.append((node, result, wire))
            if node >= 2:
                duration = wire["complete_seconds"] - wire["first_seconds"]
                if duration < 0:
                    raise ValueError("Full API return cannot precede its first token")
                context = decode_features(result["prompt_tokens"], self.output_tokens)[1]
                self.api_decode[node - 2].add((1.0, context), duration)

    def freeze(self, now):
        with self.guard:
            return PhaseCosts(super().freeze(now), tuple(f.snapshot() for f in self.api_decode),
                              self.output_tokens)

    def snapshot(self):
        with self.guard:
            return {**super().snapshot(), "candidate": "phase-conditioned-resource-release-02",
                    "api_decode": [f.snapshot() for f in self.api_decode],
                    "post_first_target": "wire full minus wire first; decode plus synchronous finish work",
                    "prefirst_target": "aggregate restore plus scheduled prefill/save plus API control",
                    "native_H2D_and_save_timers_added_again": False,
                    "RDMA_congestion_penalty": None}
