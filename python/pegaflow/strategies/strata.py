"""Strata OSDI26 Algorithm 1 selection, separated from its GPU I/O runtime.

The paper's load/compute-token ratio of 100 is explicit, not a measured 910B2
threshold. No bandwidth, overlap, bundle-sharing or CUDA speedup is invented.
"""

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class BatchDemand:
    key: str
    load_tokens: int
    compute_tokens: int
    reservation_bytes: int
    context: tuple = ()

    def __post_init__(self):
        if (
            not self.key
            or self.load_tokens < 0
            or self.compute_tokens <= 0
            or self.reservation_bytes <= 0
        ):
            raise ValueError("Identified nonnegative load and positive compute/capacity required")


def balanced_batch(queue, slots, free_bytes, *, loading_ratio):
    """Oldest first, bundle hits, non-loading-bound additions, FIFO backfill.

    Context equality controls selection priority only: the shared execution
    fixture still charges every request's actual load and KV allocation.
    """
    if not math.isfinite(loading_ratio) or loading_ratio <= 0:
        raise ValueError("Declare a positive load/compute ratio")
    queue = list(queue)
    if len({r.key for r in queue}) != len(queue):
        raise ValueError("Duplicate queued request")
    if not queue or slots <= 0 or queue[0].reservation_bytes > free_bytes:
        return ()
    chosen, deferred = [], []

    def fits(row):
        return (
            len(chosen) < slots
            and sum(r.reservation_bytes for r in chosen) + row.reservation_bytes <= free_bytes
        )

    def bundle():
        for row in tuple(queue):
            if row.context and any(row.context == r.context for r in chosen) and fits(row):
                chosen.append(row)
                queue.remove(row)

    chosen.append(queue.pop(0))
    bundle()
    while queue and len(chosen) < slots:
        row = queue.pop(0)
        load = row.load_tokens + sum(r.load_tokens for r in chosen)
        compute = row.compute_tokens + sum(r.compute_tokens for r in chosen)
        if not fits(row) or load > loading_ratio * compute:
            deferred.append(row)
        else:
            chosen.append(row)
            bundle()
    for row in deferred:
        if fits(row):
            chosen.append(row)
    return tuple(r.key for r in chosen)
