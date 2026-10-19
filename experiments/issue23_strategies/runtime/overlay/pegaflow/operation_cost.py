"""Past-completed operation fits with physical features and no tuned windows.

The scheduled prefill path includes synchronous saves; it is not isolated
kernel time. Restore is fitted separately from that path and from FIFO waits.
"""

from __future__ import annotations

from dataclasses import dataclass
import itertools
import math
import threading

from pegaflow.placement_cost import _solve


class NonnegativeFit:
    def __init__(self, dimensions):
        self.dimensions = dimensions
        self.gram = [[0.0] * dimensions for _ in range(dimensions)]
        self.rhs = [0.0] * dimensions
        self.scales = [1.0] * dimensions
        self.coefficients = None
        self.observations = 0
        self.square_seconds = 0.0

    def add(self, features, seconds):
        if len(features) != self.dimensions or not all(math.isfinite(v) and v >= 0 for v in (*features, seconds)):
            raise ValueError("Finite nonnegative physical features and completed duration required")
        for i, value in enumerate(features):
            self.scales[i] = max(self.scales[i], value)
            self.rhs[i] += value * seconds
            for j, other in enumerate(features):
                self.gram[i][j] += value * other
        self.observations += 1
        self.square_seconds += seconds * seconds
        matrix = [[self.gram[i][j] / (self.scales[i] * self.scales[j])
                   for j in range(self.dimensions)] for i in range(self.dimensions)]
        values = [v / s for v, s in zip(self.rhs, self.scales, strict=True)]
        best = None
        for bits in itertools.product((False, True), repeat=self.dimensions):
            active = [i for i, value in enumerate(bits) if value]
            beta = [0.0] * self.dimensions
            if active:
                solved = _solve([[matrix[i][j] for j in active] for i in active], [values[i] for i in active])
                if solved is None or any(not math.isfinite(v) or v < 0 for v in solved):
                    continue
                for i, value in zip(active, solved, strict=True):
                    beta[i] = value
            error = self.square_seconds - 2 * sum(b * v for b, v in zip(beta, values, strict=True))
            error += sum(beta[i] * beta[j] * matrix[i][j] for i in range(self.dimensions) for j in range(self.dimensions))
            candidate = (max(0.0, error), sum(v * v for v in beta), tuple(beta))
            if best is None or candidate < best:
                best = candidate
        self.coefficients = tuple(b / s for b, s in zip(best[2], self.scales, strict=True))

    def predict(self, features):
        if self.coefficients is None:
            raise ValueError("No completed operation observations")
        return sum(x * b for x, b in zip(features, self.coefficients, strict=True))

    def snapshot(self):
        return {"observations": self.observations, "coefficients": self.coefficients}


def prefill_features(tokens, prefix, block):
    if any(type(v) is not int for v in (tokens, prefix, block)) or not 0 <= prefix < tokens or block <= 0:
        raise ValueError("A causal prefix must leave the logits token")
    uncached = tokens - prefix
    attention_pairs = (tokens * (tokens + 1) - prefix * (prefix + 1)) // 2
    return 1.0, float(uncached), float(attention_pairs), float(math.ceil(uncached / block))


@dataclass(frozen=True)
class FrozenOperationCosts:
    original: object
    fits: tuple[tuple[dict, dict, float, float, float], ...]
    latest_completed_seconds: float
    version: int
    block_tokens: int = 128
    copy_fits: tuple[dict, ...] = ()

    def __getattr__(self, name):
        return getattr(self.original, name)

    def _dot(self, data, features, fallback):
        coefficients = data["coefficients"]
        return fallback if coefficients is None else sum(x * b for x, b in zip(features, coefficients, strict=True))

    def restore(self, node, prefix):
        old = self.original.service[node]
        return self._dot(self.fits[node][0], (1.0, prefix * self.original.bytes_per_token),
                         old.seconds_per_rdma_byte * prefix * self.original.bytes_per_token) if prefix else 0.0

    def compute(self, node, tokens, prefix):
        old = self.original.service[node]
        body = self._dot(self.fits[node][1], prefill_features(tokens, prefix, self.block_tokens),
                         old.intercept_seconds + old.seconds_per_uncached_token * (tokens - prefix))
        # P emits one token but keeps its gate through completed-response work.
        return self.restore(node, prefix) + body + self.fits[node][2] + (self.fits[node][4] if node < 2 else 0.0)

    def copy_seconds(self, source, destination, payload):
        if not payload or source == destination:
            return 0.0
        direction = 0 if source < 2 <= destination else 1 if destination < 2 <= source else 2 if source < 2 else 3
        slope = (self.original.seconds_per_pd_byte, self.original.seconds_per_dp_byte,
                 self.original.seconds_per_copy_byte, self.original.seconds_per_copy_byte)[direction]
        fallback = self.original.transfer_fixed + payload * slope
        return self._dot(self.copy_fits[direction], (1.0, payload), fallback) if self.copy_fits else fallback

    @property
    def decode_seconds(self):
        return tuple(self.fits[node][3] + self.fits[node][4] for node in (2, 3))


class OperationCosts:
    def __init__(self, original, block_tokens=128):
        self.original, self.block_tokens = original, block_tokens
        self.restore = [NonnegativeFit(2) for _ in range(4)]
        self.prefill = [NonnegativeFit(4) for _ in range(4)]
        self.control = [[0, 0.0, 0.0, 0.0] for _ in range(4)]
        self.guard = threading.RLock()
        self.records, self.seen = [], set()
        self.latest = 0.0
        self.copy_fits = [NonnegativeFit(2) for _ in range(4)]
        self.copy_records = []

    def observe_copy(self, source, destination, payload, held_seconds, link_wait, completed_seconds):
        if source == destination or payload <= 0 or not 0 <= link_wait <= held_seconds:
            raise ValueError("Completed explicit-source copy required")
        direction = 0 if source < 2 <= destination else 1 if destination < 2 <= source else 2 if source < 2 else 3
        with self.guard:
            # Link waiting is a resource queue, not transfer bandwidth. Inventory,
            # allocation and publication performed under gates remain priced.
            self.copy_fits[direction].add((1.0, payload), held_seconds - link_wait)
            self.latest = max(self.latest, completed_seconds)
            self.copy_records.append(dict(source=source, destination=destination, payload_bytes=payload,
                held_seconds=held_seconds, link_wait_seconds=link_wait, completed_seconds=completed_seconds))

    def observe(self, node, result, wire, completed_seconds, *, calibration=False):
        raw = result["raw_metrics"]
        queued, scheduled, first, last = (raw[k] for k in ("queued_ts", "scheduled_ts", "first_token_ts", "last_token_ts"))
        if not queued <= scheduled <= first <= last or not math.isfinite(completed_seconds) or completed_seconds < 0:
            raise ValueError("Only completed valid stage intervals may train operation costs")
        tokens, prefix = result["prompt_tokens"], result["num_cached_tokens"]
        features = prefill_features(tokens, prefix, self.block_tokens)
        identity = node, result["id"]
        with self.guard:
            if identity in self.seen:
                raise ValueError("Duplicate completed stage observation")
            self.seen.add(identity)
            if prefix:
                self.restore[node].add((1.0, prefix * self.original.bytes_per_token), scheduled - queued)
            self.prefill[node].add(features, first - scheduled)
            count, network, tail, post = self.control[node]
            network += max(0.0, wire["first_seconds"] - (first - queued))
            tail += last - first
            post += max(0.0, wire["complete_seconds"] - wire["first_seconds"] - (last - first))
            self.control[node] = [count + 1, network, tail, post]
            self.latest = max(self.latest, completed_seconds)
            self.records.append({"node": node, "id": result["id"], "completed_seconds": completed_seconds,
                "calibration": calibration, "prompt_tokens": tokens, "accepted_cache_tokens": prefix,
                "restore_seconds": scheduled - queued, "prefill_sync_path_seconds": first - scheduled,
                "decode_tail_seconds": last - first, "model_version_after": len(self.records) + 1})

    def freeze(self, now):
        with self.guard:
            if self.latest > now:
                raise ValueError("An operation model cannot use future completion observations")
            fits = []
            for node in range(4):
                count, network, tail, post = self.control[node]
                fits.append((self.restore[node].snapshot(), self.prefill[node].snapshot(),
                    network / count if count else 0.0,
                    tail / count if count else (self.original.decode_seconds[node - 2] if node >= 2 else 0.0),
                    post / count if count else 0.0))
            return FrozenOperationCosts(self.original, tuple(fits), self.latest,
                len(self.records) + len(self.copy_records), self.block_tokens,
                tuple(f.snapshot() for f in self.copy_fits))

    def snapshot(self):
        with self.guard:
            return {"kind": "completed-operation-NNLS-no-window-or-regularization",
                    "records": list(self.records), "latest_completed_seconds": self.latest,
                    "copies": list(self.copy_records),
                    "features": ["intercept", "uncached_tokens", "causal_attention_pairs", "prefill_chunks"],
                    "prefill_includes_sync_save": True, "kernel_isolation_claimed": False}
