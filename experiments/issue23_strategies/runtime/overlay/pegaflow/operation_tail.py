"""Completed-only empirical tail envelope on physical operation shapes.

The confidence level is the requested TTFT percentile, not a fitted margin.
Empirical CVaR is coherent/subadditive for a specified joint distribution, but
these small conditional samples do NOT certify out-of-sample TTFT quantiles.
"""
from __future__ import annotations

from dataclasses import dataclass
import math

from pegaflow.operation_context import ContextOperationCosts
from pegaflow.operation_cost import prefill_features


def empirical_cvar(values, level=.99):
    values = sorted(values, reverse=True)
    if not values or not 0 <= level < 1 or any(not math.isfinite(v) or v < 0 for v in values):
        raise ValueError("Nonnegative completed samples and an explicit percentile required")
    mass = len(values) * (1 - level)
    whole = math.floor(mass)
    total = sum(values[:whole])
    if whole < len(values):
        total += (mass - whole) * values[whole]
    return total / mass


def parts(frozen, node, tokens, prefix):
    old = frozen.original.service[node]
    return dict(restore=frozen.restore(node, prefix),
        prefill=frozen._dot(frozen.fits[node][1], prefill_features(tokens, prefix, frozen.block_tokens),
            old.intercept_seconds + old.seconds_per_uncached_token * (tokens - prefix)),
        tail=(frozen.decode_tail(node, tokens) - frozen.fits[node][4] if node >= 2 else 0),
        control=frozen.fits[node][2], post=frozen.fits[node][4])


@dataclass(frozen=True)
class TailCosts:
    original_fit: object
    current: object
    envelopes: tuple[dict, ...]
    copy_envelopes: tuple[float, ...]
    level: float

    def __getattr__(self, name):
        return getattr(self.current, name)

    def _value(self, node, name, value):
        multiplier, extra = self.envelopes[node][name]
        return value * multiplier + extra

    def restore(self, node, prefix):
        return self._value(node, "restore", self.original_fit.restore(node, prefix)) if prefix else 0.0

    def compute(self, node, tokens, prefix):
        p = parts(self.original_fit, node, tokens, prefix)
        return (self.restore(node, prefix) + self._value(node, "prefill", p["prefill"])
                + self._value(node, "control", p["control"])
                + (self._value(node, "post", p["post"]) if node < 2 else 0))

    def decode_tail(self, node, prompt_tokens):
        p = parts(self.original_fit, node, prompt_tokens, 0)
        return self._value(node, "tail", p["tail"]) + self._value(node, "post", p["post"])

    def copy_seconds(self, source, destination, payload):
        if not payload or source == destination:
            return 0.0
        direction = 0 if source < 2 <= destination else 1 if destination < 2 <= source else 2 if source < 2 else 3
        slopes = (self.seconds_per_pd_byte, self.seconds_per_dp_byte,
                  self.seconds_per_copy_byte, self.seconds_per_copy_byte)
        return (self.transfer_fixed + payload * slopes[direction]) * self.copy_envelopes[direction]


class TailOperationCosts(ContextOperationCosts):
    def __init__(self, original, block_tokens=128, output_tokens=128, level=.99):
        super().__init__(original, block_tokens, output_tokens)
        if not 0 <= level < 1:
            raise ValueError("Explicit target percentile required")
        self.level, self.seed_fit = level, None
        self.samples = [{name: [] for name in ("restore", "prefill", "tail", "control", "post")}
                        for _ in range(4)]
        self.copy_samples = [[] for _ in range(4)]
        self.calibration_data = []

    def _sample(self, node, result, wire):
        raw = result["raw_metrics"]
        q, s, f, e = (raw[k] for k in ("queued_ts", "scheduled_ts", "first_token_ts", "last_token_ts"))
        actual = dict(restore=s-q, prefill=f-s, tail=e-f,
            control=max(0, wire["first_seconds"]-(f-q)),
            post=max(0, wire["complete_seconds"]-wire["first_seconds"]-(e-f)))
        shaped = parts(self.seed_fit, node, result["prompt_tokens"], result["num_cached_tokens"])
        for name, seconds in actual.items():
            if name == "restore" and not result["num_cached_tokens"]:
                continue
            scale = shaped[name]
            self.samples[node][name].append((seconds / scale, 0.0) if scale > 0 else (1.0, seconds))

    def observe(self, node, result, wire, completed_seconds, *, calibration=False):
        with self.guard:
            if self.seed_fit is None and not calibration:
                raise ValueError("Calibration must be explicitly sealed before online observations")
            if self.seed_fit is not None and calibration:
                raise ValueError("A sealed calibration seed cannot be extended with test observations")
            super().observe(node, result, wire, completed_seconds, calibration=calibration)
            if calibration:
                self.calibration_data.append((node, result, wire))
            else:
                self._sample(node, result, wire)

    def seal_calibration(self):
        with self.guard:
            if self.seed_fit is not None or not self.calibration_data:
                raise ValueError("One nonempty immutable calibration seed is required")
            self.seed_fit = super().freeze(self.latest)
            for node, result, wire in self.calibration_data:
                self._sample(node, result, wire)

    def observe_copy(self, source, destination, payload, held_seconds, link_wait, completed_seconds):
        super().observe_copy(source, destination, payload, held_seconds, link_wait, completed_seconds)
        with self.guard:
            direction = 0 if source < 2 <= destination else 1 if destination < 2 <= source else 2 if source < 2 else 3
            slopes = (self.original.seconds_per_pd_byte, self.original.seconds_per_dp_byte,
                      self.original.seconds_per_copy_byte, self.original.seconds_per_copy_byte)
            base = self.original.transfer_fixed + payload * slopes[direction]
            if base > 0:
                self.copy_samples[direction].append((held_seconds-link_wait) / base)

    def freeze(self, now):
        with self.guard:
            current = super().freeze(now)
            if self.seed_fit is None:
                raise ValueError("Tail samples require an explicitly sealed calibration shape")
            envelopes = tuple({name: (empirical_cvar([p[0] for p in values], self.level),
                                     empirical_cvar([p[1] for p in values], self.level))
                if values else (1.0, 0.0) for name, values in node.items()} for node in self.samples)
            return TailCosts(self.seed_fit, current, envelopes,
                tuple(empirical_cvar(values, self.level) if values else 1.0 for values in self.copy_samples), self.level)

    def snapshot(self):
        with self.guard:
            return {**super().snapshot(), "kind": "completed-only-empirical-CVaR-physical-envelope",
                "level": self.level, "samples": self.samples, "copy_samples": self.copy_samples,
                "initial_shape": "sealed original calibration only; no test refit of shape",
                "guarantee": "no finite-sample conditional coverage or TTFT p99 guarantee"}
