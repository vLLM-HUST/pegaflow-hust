"""Shared, source-only policy interfaces; no native runtime is replaced."""

from .baselines import MemServeStrategy, NativeStrategy
from .model import Candidate, Decision, Forecast, Request, Snapshot, Worker
from .mooncake import MooncakePrefillStrategy
from .tailguard import TailguardStrategy


def create_strategy(name, **configuration):
    factories = {
        "vllm-pegaflow-native": NativeStrategy,
        "memserve-eq12-adapter": MemServeStrategy,
        "mooncake-fast25-prefill-no-reject-adapter": MooncakePrefillStrategy,
        "tailguard-pending-release-v1": TailguardStrategy,
    }
    if name not in factories:
        raise ValueError(f"Unknown or not-yet-qualified policy: {name}")
    return factories[name](**configuration)


__all__ = ["Candidate", "Decision", "Forecast", "Request", "Snapshot", "Worker",
           "create_strategy"]
