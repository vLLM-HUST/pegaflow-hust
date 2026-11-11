"""Lightweight scheduling observation without changing native decisions."""

import atexit
import json
import os
import time
from pathlib import Path

from vllm.v1.core.sched.scheduler import Scheduler

_file = None


def emit(event, **fields):
    global _file
    if _file is None:
        p = Path(os.environ["SCHEDULING_TRACE_DIR"])
        p.mkdir(exist_ok=True, parents=True)
        _file = (p / f"{os.getpid()}.jsonl").open("w", buffering=65536)
        atexit.register(_file.close)
    _file.write(
        json.dumps({"event": event, "epoch_ns": time.time_ns(), **fields}, default=str) + "\n"
    )


class TraceScheduler(Scheduler):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.trace_index = 0
        emit(
            "config",
            scheduler=repr(self.scheduler_config),
            kv=repr(self.kv_cache_config),
        )

    def schedule(self):
        self.trace_index += 1
        t, c = time.perf_counter_ns(), time.process_time_ns()
        out = super().schedule()
        emit(
            "schedule",
            step=self.trace_index,
            wall_ns=time.perf_counter_ns() - t,
            cpu_ns=time.process_time_ns() - c,
            scheduled=out.num_scheduled_tokens,
            waiting=len(self.waiting),
            running=len(self.running),
            requests={
                k: {
                    "prompt": r.num_prompt_tokens,
                    "computed": r.num_computed_tokens,
                    "outputs": len(r.output_token_ids),
                    "cached": getattr(r, "num_cached_tokens", None),
                }
                for k, r in self.requests.items()
            },
        )
        return out

    def update_from_output(self, scheduled, outputs):
        t = time.perf_counter_ns()
        result = super().update_from_output(scheduled, outputs)
        emit("update", step=self.trace_index, wall_ns=time.perf_counter_ns() - t)
        if not self.requests and _file is not None:
            _file.flush()
        return result
