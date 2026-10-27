"""CPU iteration scheduler; batch scaling is an explicit, unmeasured assumption.

This is not a vLLM runtime or a model of concurrent independent GPU engines.
Restore and synchronous completion work acquire the same device as iterations.
KV is conservatively reserved for the whole context, without sharing/preemption.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field

from .memory_budget import Ascend910B2Budget


@dataclass
class AllocationLeases:
    """Release the acquired allocation set, never a new hash-to-owner lookup."""

    handles: dict = field(default_factory=dict)
    counts: dict = field(default_factory=dict)

    def acquire(self, owner, node, identities):
        key = owner, node
        if key in self.handles:
            raise ValueError("Duplicate owned allocation lease")
        identities = frozenset(identities)
        self.handles[key] = identities
        for identity in identities:
            item = node, identity
            self.counts[item] = self.counts.get(item, 0) + 1

    def release(self, owner, node):
        identities = self.handles.pop((owner, node))
        for identity in identities:
            item = node, identity
            if self.counts.get(item, 0) <= 0:
                raise ValueError("Allocation lease double release")
            self.counts[item] -= 1
        return identities

    def protected(self, node, identity):
        return self.counts.get((node, identity), 0) > 0


@dataclass(frozen=True)
class BatchConfig:
    max_sequences: int = 4
    token_budget: int = 128
    block_tokens: int = 128
    kv_bytes: int = Ascend910B2Budget().kv_bytes
    bytes_per_token: int = 57344
    shared_fraction: float = 0.0

    def __post_init__(self):
        for value in (
            self.max_sequences,
            self.token_budget,
            self.block_tokens,
            self.kv_bytes,
            self.bytes_per_token,
        ):
            if type(value) is not int or value <= 0:
                raise ValueError("Positive integer batch and physical memory limits required")
        if not math.isfinite(self.shared_fraction) or not 0 <= self.shared_fraction <= 1:
            raise ValueError("Explicit shared work sensitivity must be in [0, 1]")


@dataclass
class Sequence:
    key: str
    prompt_tokens: int
    prefix_tokens: int
    first_seconds: float
    tail_seconds: float
    restore_seconds: float = 0.0
    save_seconds: float = 0.0
    output_tokens: int = 128
    prefilled: int = 0
    decoded: int = 0
    restored: bool = False
    phase: str = "restore"
    first_at: float | None = None

    def __post_init__(self):
        if (
            not self.key
            or type(self.output_tokens) is not int
            or self.output_tokens < 1
            or not 0 <= self.prefix_tokens < self.prompt_tokens
        ):
            raise ValueError("Identified causal sequence with a logits token required")
        for value in (
            self.first_seconds,
            self.tail_seconds,
            self.restore_seconds,
            self.save_seconds,
        ):
            if not math.isfinite(value) or value < 0:
                raise ValueError("Nonnegative, finite singleton phase costs required")

    @property
    def remaining_prefill(self):
        return self.prompt_tokens - self.prefix_tokens - self.prefilled

    def reservation(self, config):
        tokens = self.prompt_tokens + self.output_tokens
        return (
            math.ceil(tokens / config.block_tokens) * config.block_tokens * config.bytes_per_token
        )


@dataclass(frozen=True)
class Iteration:
    kind: str
    seconds: float
    pieces: tuple[tuple[str, int, str], ...]
    repeats: int = 1


@dataclass
class ContinuousBatcher:
    compute_phase_order = ("decode", "prefill")
    config: BatchConfig
    waiting: deque = field(default_factory=deque)
    active: dict[str, Sequence] = field(default_factory=dict)
    used_bytes: int = 0
    peak_bytes: int = 0
    peak_sequences: int = 0
    peak_iteration_sequences: int = 0
    busy: Iteration | None = None

    def submit(self, sequence):
        if sequence.key in self.active or any(s.key == sequence.key for s in self.waiting):
            raise ValueError("Duplicate batch sequence")
        if sequence.reservation(self.config) > self.config.kv_bytes:
            raise ValueError("One sequence exceeds the frozen NPU KV budget")
        self.waiting.append(sequence)

    def admit(self):
        while self.waiting and len(self.active) < self.config.max_sequences:
            sequence = self.waiting[0]
            amount = sequence.reservation(self.config)
            if self.used_bytes + amount > self.config.kv_bytes:
                break
            self.waiting.popleft()
            self.active[sequence.key] = sequence
            self.used_bytes += amount
        self.peak_bytes = max(self.peak_bytes, self.used_bytes)
        self.peak_sequences = max(self.peak_sequences, len(self.active))

    def begin(self, eligible=None):
        if self.busy is not None:
            return None
        self.admit()
        sequences = [s for s in self.active.values() if eligible is None or eligible(s.key)]
        # Device-exclusive restore/save remain barriers; only compute is batched.
        for sequence in sequences:
            if sequence.phase in ("restore", "save"):
                seconds = (
                    sequence.restore_seconds
                    if sequence.phase == "restore"
                    else sequence.save_seconds
                )
                self.busy = Iteration(sequence.phase, seconds, ((sequence.key, 0, sequence.phase),))
                return self.busy
        budget = self.config.token_budget
        pieces, decode_seconds, prefill_seconds = [], [], []
        # Existing decodes first, then FIFO chunked prefill using the remaining budget.
        for phase in self.compute_phase_order:
            for sequence in sequences:
                if sequence.phase != phase or not budget:
                    continue
                if phase == "decode":
                    count = 1
                    duration = sequence.tail_seconds / max(1, sequence.output_tokens - 1)
                else:
                    if sequence.remaining_prefill <= 0:
                        raise ValueError(
                            "Prefill phase has no remaining tokens; inconsistent progress"
                        )
                    count = min(budget, sequence.remaining_prefill)
                    # Singleton fit already includes context-sensitive attention and saves.
                    # Distribute its remaining body linearly; no unmeasured kernel fit claimed.
                    duration = (
                        sequence.first_seconds
                        * count
                        / (sequence.prompt_tokens - sequence.prefix_tokens)
                    )
                pieces.append((sequence.key, count, phase))
                (decode_seconds if phase == "decode" else prefill_seconds).append(duration)
                budget -= count
        if not pieces:
            return None
        shared = self.config.shared_fraction
        # Prefill fits contain synchronous per-chunk save work, so never assume
        # that component becomes free through cross-request batching.
        duration = sum(prefill_seconds) + (1 - shared) * sum(decode_seconds)
        duration += shared * max(decode_seconds, default=0.0)
        self.peak_iteration_sequences = max(self.peak_iteration_sequences, len(pieces))
        self.busy = Iteration("compute", duration, tuple(pieces))
        return self.busy

    def finish(self, iteration, now):
        if self.busy != iteration:
            raise ValueError("Iteration ownership changed")
        self.busy = None
        first, finished = [], []
        for key, count, phase in iteration.pieces:
            sequence = self.active[key]
            count *= iteration.repeats
            if phase == "restore":
                sequence.restored, sequence.phase = True, "prefill"
            elif phase == "prefill":
                sequence.prefilled += count
                if sequence.remaining_prefill == 0:
                    sequence.decoded, sequence.first_at = 1, now
                    first.append(key)
                    sequence.phase = "decode" if sequence.output_tokens > 1 else "save"
            elif phase == "decode":
                sequence.decoded += count
                if sequence.decoded == sequence.output_tokens:
                    sequence.phase = "save"
            elif phase == "save":
                sequence.phase = "done"
                finished.append(key)
            else:
                raise ValueError("Unknown iteration phase")
        for key in finished:
            self.used_bytes -= self.active[key].reservation(self.config)
            del self.active[key]
        return first, finished

    @property
    def idle(self):
        return not self.active and not self.waiting and self.busy is None
