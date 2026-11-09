"""Check native scheduler decisions against an observed synchronous vLLM trace."""

import argparse
import inspect
import json
from collections import deque
from pathlib import Path
from types import SimpleNamespace

from native_vllm_adapter import NativeVllmEngine


def replay(events, model_path):
    from vllm.config import SchedulerConfig

    config = next(e for e in events if e["event"] == "config")
    options = dict(config["scheduler_fields"])
    maximum = options.pop("max_num_seqs")
    budget = options.pop("max_num_batched_tokens")
    for key in [
        "max_model_len",
        "is_encoder_decoder",
        "async_scheduling",
        "scheduler_cls",
    ]:
        options.pop(key, None)
    valid = set(inspect.signature(SchedulerConfig).parameters)
    options = {key: value for key, value in options.items() if key in valid}
    queries = deque(e for e in events if e["event"] == "external_query")
    allocations = deque(e for e in events if e["event"] == "external_allocated")
    finishes = deque(e for e in events if e["event"] == "external_finished")
    inferred_finishes = not finishes
    if inferred_finishes:
        for event in events:
            if event["event"] != "update":
                continue
            for key, before in event["before"].items():
                after = event["after"].get(key)
                if not before["status"].startswith("FINISHED") and (
                    after is None or after["status"].startswith("FINISHED")
                ):
                    finishes.append({"request_id": key, "delayed": after is not None})

    def query(request, computed):
        row = queries.popleft()
        assert (row["request_id"], row["computed"]) == (request.request_id, computed)
        return tuple(row["result"])

    def allocated(request, blocks, external):
        row = allocations.popleft()
        assert (row["request_id"], row["external"], row["blocks"]) == (
            request.request_id,
            external,
            [list(group) for group in blocks.get_block_ids()],
        )

    def finished(request, blocks):
        row = finishes.popleft()
        assert row["request_id"] == request.request_id
        return row["delayed"]

    has_connector = bool(queries or allocations)
    if not has_connector:
        finishes.clear()
    engine = NativeVllmEngine(
        model_path=model_path,
        num_blocks=config["num_gpu_blocks"],
        block_tokens=config["block_size"],
        max_sequences=maximum,
        token_budget=budget,
        max_model_len=config.get("max_model_len", 32768),
        scheduler_options=options,
        transfer=SimpleNamespace(query=query, allocated=allocated, finished=finished)
        if has_connector
        else None,
    )
    mismatches = []
    counts = {"requests": 0, "schedules": 0, "updates": 0, "scheduled_tokens": 0}

    def compare(frame):
        actual, expected = engine.snapshot(), frame["after"]
        if actual.keys() != expected.keys():
            mismatches.append(
                {
                    "step": frame.get("step"),
                    "event": frame["event"],
                    "field": "requests",
                    "actual": list(actual),
                    "expected": list(expected),
                }
            )
            return
        for key in actual:
            for field in ["computed", "outputs", "status", "preemptions", "block_ids"]:
                value = json.loads(json.dumps(actual[key][field]))
                if value != expected[key][field]:
                    mismatches.append(
                        {
                            "step": frame.get("step"),
                            "event": frame["event"],
                            "request": key,
                            "field": field,
                            "actual": value,
                            "expected": expected[key][field],
                        }
                    )

    try:
        for frame in events:
            kind = frame["event"]
            if kind == "add_request":
                engine.add(
                    frame["request_id"],
                    len(frame["prompt_token_ids"]),
                    frame["max_tokens"],
                    frame["arrival_time"],
                    prompt_token_ids=frame["prompt_token_ids"],
                    cache_salt=frame["cache_salt"],
                    priority=frame["priority"],
                    native_prefix_hashes=frame["block_hashes"],
                )
                counts["requests"] += 1
            elif kind == "reset_prefix_cache":
                engine.scheduler.reset_prefix_cache(*frame["args"], **frame["kwargs"])
            elif kind == "schedule":
                if (
                    engine.pending_output is not None
                    and not engine.pending_output.num_scheduled_tokens
                ):
                    engine.complete()
                planned = {key: count for key, count, _ in engine.schedule()}
                if planned != frame["tokens"]:
                    mismatches.append(
                        {
                            "step": frame["step"],
                            "field": "scheduled_tokens",
                            "actual": planned,
                            "expected": frame["tokens"],
                        }
                    )
                counts["schedules"] += 1
                counts["scheduled_tokens"] += sum(planned.values())
                compare(frame)
            elif kind == "update":
                for key in frame.get("recvs", []):
                    engine.signal_transfer(key)
                for key in frame.get("sends", []):
                    engine.signal_transfer(key, sending=True)
                engine.complete(frame["sampled"])
                counts["updates"] += 1
                compare(frame)
            if mismatches:
                break
        if not mismatches and any([queries, allocations, finishes]):
            mismatches.append(
                {
                    "field": "unconsumed_connector_calls",
                    "counts": [len(queries), len(allocations), len(finishes)],
                }
            )
        return {
            "passed": not mismatches,
            "counts": counts,
            "identity": engine.identity,
            "connector": has_connector,
            "finish_callback_source": "request retention in recorded update states"
            if inferred_finishes and has_connector
            else "observed callback",
            "mismatches": mismatches,
            "scope": "Decision/state parity with recorded model outputs and connector results; excludes timing prediction",
        }
    finally:
        engine.shutdown()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trace", type=Path, required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError(args.output)
    result = replay([json.loads(line) for line in args.trace.read_text().splitlines()], args.model)
    args.output.write_text(json.dumps(result, indent=2))
    print(json.dumps(result))
    if not result["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
