"""Completed-only context-dependent decode occupancy for a fixed output budget."""
from __future__ import annotations

from dataclasses import dataclass

from pegaflow.operation_cost import FrozenOperationCosts, NonnegativeFit, OperationCosts


def decode_features(prompt_tokens, output_tokens):
    if type(prompt_tokens) is not int or prompt_tokens < 1:
        raise ValueError("Positive integer prompt length required")
    if type(output_tokens) is not int or output_tokens < 1:
        raise ValueError("Positive integer output budget required")
    steps = output_tokens - 1
    return float(steps), float(steps * prompt_tokens + steps * (steps + 1) // 2)


@dataclass(frozen=True)
class ContextCosts(FrozenOperationCosts):
    decode_fits: tuple[dict, ...] = ()
    output_tokens: int = 128

    def decode_tail(self, node, prompt_tokens):
        if node not in (2, 3):
            raise ValueError("Decode tail belongs to a D device")
        coefficients = self.decode_fits[node-2]["coefficients"]
        if coefficients is None:
            return self.decode_seconds[node-2]
        raw_tail = sum(a*b for a,b in zip(coefficients,
            decode_features(prompt_tokens,self.output_tokens),strict=True))
        return raw_tail + self.fits[node][4]


class ContextOperationCosts(OperationCosts):
    def __init__(self, original, block_tokens=128, output_tokens=128):
        super().__init__(original,block_tokens)
        decode_features(1,output_tokens)
        self.output_tokens = output_tokens
        self.decode_fits = [NonnegativeFit(2),NonnegativeFit(2)]

    def observe(self,node,result,wire,completed_seconds,*,calibration=False):
        with self.guard:
            super().observe(node,result,wire,completed_seconds,calibration=calibration)
            if node >= 2:
                raw = result["raw_metrics"]
                self.decode_fits[node-2].add(decode_features(result["prompt_tokens"],
                    self.output_tokens),raw["last_token_ts"]-raw["first_token_ts"])

    def freeze(self,now):
        with self.guard:
            frozen=super().freeze(now)
            return ContextCosts(**frozen.__dict__,
                decode_fits=tuple(f.snapshot() for f in self.decode_fits),
                output_tokens=self.output_tokens)

    def snapshot(self):
        with self.guard:
            return {**super().snapshot(),"decode_features":["output_tokens_minus_one",
                "sum_of_context_lengths_for_remaining_output_tokens"],
                "decode_context_fits":[f.snapshot() for f in self.decode_fits],
                "output_tokens":self.output_tokens}
