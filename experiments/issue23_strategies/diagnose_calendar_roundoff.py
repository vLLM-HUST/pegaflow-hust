"""Independent-stream numerical diagnostic, not a policy or workload result.

An unrelated P completion changes coalescing horizons, but shares no gate,
cache, link or D batch with request a. Its timing cannot physically change a's
first-token time. Compare binary endpoints with coalescing on and off, without
adding a gain epsilon. The scalars recreate the observed roundoff scale only.
"""

import argparse
import json
from pathlib import Path

import simulate_continuous_batch  # noqa: F401
from batch_calendar import schedule_batched
from pegaflow.joint_policy import Calendar, Demand
from pegaflow.operation_schedule import Operation, Work
from pegaflow.resource_cache_profile import CacheRoute
from pegaflow.strategies.continuous_batch import BatchConfig


class Costs:
    fits = [(0, 0, 0, 0, 0)] * 4

    def compute(self, node, tokens, prefix):
        return 6.344471176237789 if node == 2 else 0.3

    def restore(self, *args):
        return 0.0

    def decode_tail(self, *args):
        return 1.0


def route(p, d):
    return CacheRoute(
        p=p,
        d=d,
        source=p,
        p_blocks=0,
        d_blocks=0,
        predicted_p_end=0,
        predicted_first=0,
        predicted_end=0,
        extra_bytes=0,
        calendar=Calendar(),
    )


def diagnostic(*, exact_timeline=False):
    origin = 166.9
    costs = Costs()
    a = Work(
        "a",
        1,
        2,
        0,
        origin,
        (),
        (
            Operation(
                "D_api",
                costs.compute(2, 1000, 0) + 1,
                ("gate2",),
                costs.compute(2, 1000, 0),
            ),
        ),
        p_done=origin,
        d_enqueued=origin,
    )
    demands = {
        k: Demand(k, k, 1000, tuple(bytes([i]) for i in range(7))) for k in ("a", "b")
    }
    rows = []
    for coalesce in (True, False):
        for duration in (0.1, 0.2, 0.41992161351259943, 0.6, 1.0, 2.5, 4.0, 6.0, 10.0):
            b = Work(
                "b",
                0,
                3,
                1,
                origin,
                (Operation("P_api", duration, ("gate0",)),),
                (Operation("D_api", 1.3, ("gate3",), 0.3),),
            )
            prediction = schedule_batched(
                (a, b),
                origin,
                lambda w, op, age: op.seconds,
                demands,
                {"a": route(1, 2), "b": route(0, 3)},
                costs,
                BatchConfig(token_budget=128, block_tokens=128, bytes_per_token=4096),
                {},
                coalesce=coalesce,
                exact_timeline=exact_timeline,
            )
            endpoint = prediction["first"]["a"]
            rows.append(
                {
                    "coalesce": coalesce,
                    "unrelated_P_seconds": duration,
                    "a_first": endpoint,
                    "a_first_hex": endpoint.hex(),
                }
            )
    return {
        "exact_timeline": exact_timeline,
        "synthetic_independent_stream_diagnostic": True,
        "not_TTFT_performance_or_new_policy": True,
        "no_shared_resources_or_cache": True,
        "different_endpoints": {
            str(c): len({r["a_first_hex"] for r in rows if r["coalesce"] == c})
            for c in (True, False)
        },
        "rows": rows,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--exact-timeline", action="store_true")
    args = parser.parse_args()
    result = diagnostic(exact_timeline=args.exact_timeline)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
