"""Immutable admission-time facts and explicit all-cost forecast contracts."""

from __future__ import annotations

import math
from dataclasses import dataclass


def nonnegative(value):
    if not math.isfinite(value) or value < 0:
        raise ValueError("A finite nonnegative measured/predicted quantity is required")


@dataclass(frozen=True)
class Request:
    key: str
    session: str
    prompt_tokens: int
    ready: float
    sequence: int

    def __post_init__(self):
        if not self.key or not self.session or type(self.prompt_tokens) is not int:
            raise ValueError("Request identity and integer token count are required")
        if self.prompt_tokens <= 0 or type(self.sequence) is not int or self.sequence < 0:
            raise ValueError("Invalid prompt length or admission sequence")
        nonnegative(self.ready)


@dataclass(frozen=True)
class Worker:
    node: int
    role: str
    local_prefix: int
    queued_prefill_tokens: int
    active_sequences: int
    queued_requests: int
    queue_service_seconds: float
    released_at: float

    def __post_init__(self):
        if self.role not in ("P", "D"):
            raise ValueError("Worker role must be P or D")
        for value in (self.node, self.local_prefix, self.queued_prefill_tokens,
                      self.active_sequences, self.queued_requests):
            if type(value) is not int or value < 0:
                raise ValueError("Worker counters must be nonnegative integers")
        nonnegative(self.queue_service_seconds)
        nonnegative(self.released_at)


@dataclass(frozen=True)
class Candidate:
    p: int
    d: int
    source: int
    p_prefix: int
    d_prefix: int
    p_first_seconds: float
    p_full_seconds: float
    d_first_seconds: float
    d_full_seconds: float
    history_copy_seconds: float = 0.0
    handoff_seconds: float = 0.0
    control_seconds: float = 0.0
    feasible: bool = True
    generation: tuple[int, ...] = ()
    layout_actions: tuple[str, ...] = ()

    def __post_init__(self):
        for value in (self.p, self.d, self.source, self.p_prefix, self.d_prefix):
            if type(value) is not int or value < 0:
                raise ValueError("Invalid plan node or prefix")
        for value in (self.p_first_seconds, self.p_full_seconds, self.d_first_seconds,
                      self.d_full_seconds, self.history_copy_seconds,
                      self.handoff_seconds, self.control_seconds):
            nonnegative(value)
        if self.p_full_seconds < self.p_first_seconds or self.d_full_seconds < self.d_first_seconds:
            raise ValueError("A first token cannot release unfinished service")


@dataclass(frozen=True)
class Snapshot:
    request: Request
    now: float
    workers: tuple[Worker, ...]
    candidates: tuple[Candidate, ...]
    pending_first: frozenset[str]
    observed_first: frozenset[str] = frozenset()

    def __post_init__(self):
        nonnegative(self.now)
        if self.now < self.request.ready:
            raise ValueError("Routing before causal admission is forbidden")
        ids = [worker.node for worker in self.workers]
        if len(set(ids)) != len(ids):
            raise ValueError("Duplicate worker identity")
        if self.pending_first & self.observed_first:
            raise ValueError("An immutable first token must not remain in the pending objective")
        if self.request.key not in self.pending_first:
            raise ValueError("The candidate request must enter the pending objective")
        for candidate in self.candidates:
            p, d = self.worker(candidate.p), self.worker(candidate.d)
            self.worker(candidate.source)
            if p.role != "P" or d.role != "D":
                raise ValueError("Candidate roles differ from actual worker roles")
            if max(candidate.p_prefix, candidate.d_prefix) >= self.request.prompt_tokens:
                raise ValueError("Recovery must retain the engine's logits-token contract")

    def worker(self, node):
        matches = [worker for worker in self.workers if worker.node == node]
        if len(matches) != 1:
            raise ValueError("Unknown worker identity")
        return matches[0]

    def feasible(self):
        return tuple(candidate for candidate in self.candidates if candidate.feasible)


@dataclass(frozen=True)
class Forecast:
    first_latencies: tuple[tuple[str, float], ...]
    full_release: tuple[tuple[int, float], ...]
    latest_completed: float
    origin: float
    includes_control_and_migration: bool

    def validate(self, snapshot):
        nonnegative(self.origin)
        nonnegative(self.latest_completed)
        if self.origin != snapshot.now or self.latest_completed > self.origin:
            raise ValueError("Forecast uses a different origin or future completed labels")
        if not self.includes_control_and_migration:
            raise ValueError("The tail objective must include control and migration costs")
        keys = [key for key, _ in self.first_latencies]
        if len(set(keys)) != len(keys) or set(keys) != set(snapshot.pending_first):
            raise ValueError("Forecast must price every pending first exactly once")
        for _, value in (*self.first_latencies, *self.full_release):
            nonnegative(value)
        nodes = [node for node, _ in self.full_release]
        if len(nodes) != len(set(nodes)) or set(nodes) != {w.node for w in snapshot.workers}:
            raise ValueError("Forecast must retain every device's release calendar")


@dataclass(frozen=True)
class Decision:
    policy: str
    p: int
    d: int
    candidate: Candidate | None
    native_cache: bool
    provenance: str
    candidates_evaluated: int = 0

    def worker_options(self, role):
        if role not in ("P", "D"):
            raise ValueError("Invalid execution role")
        if self.native_cache:
            return {"policy_native_cache": True}
        if self.candidate is None:
            raise ValueError("A controlled policy needs a declared boundary")
        limit = self.candidate.p_prefix if role == "P" else self.candidate.d_prefix
        return {"policy_native_cache": False, "max_prefix_tokens": limit,
                "commit_generation": list(self.candidate.generation)}
