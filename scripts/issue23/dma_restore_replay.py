"""Causal full-session replay using actual P/D model endpoints and RDMA cache."""

import argparse
import asyncio
import gzip
import json
import math
import random
import statistics
import time
from collections import Counter
from pathlib import Path

import httpx
from dma_restore_tools import external_output

ROOT = None
CFG = {}
INST = {}


def url(name):
    r = INST[name]
    return f"http://{CFG['hosts'][r['role']]}:{r['model_port']}"


def append(path, row):
    with path.open("a") as f:
        f.write(json.dumps(row) + "\n")


def quantiles(values):
    if not values:
        return None
    v = sorted(values)
    return {
        "mean": statistics.mean(v),
        "p50": statistics.median(v),
        "p95": v[math.ceil(0.95 * len(v)) - 1],
        "p99": v[math.ceil(0.99 * len(v)) - 1],
        "max": max(v),
    }


async def run(args):
    data = args.workload.resolve() if args.workload is not None else ROOT / "workload.jsonl.gz"
    with gzip.open(data, "rt") as f:
        cases = [json.loads(line) for line in f]
    if args.sessions:
        longest = {}
        for row in cases:
            longest[row["session"]] = max(longest.get(row["session"], 0), row["prompt_tokens"])
        selected = set(sorted(longest, key=lambda s: (-longest[s], s))[: args.sessions])
        cases = [row for row in cases if row["session"] in selected]
    tasks = sorted({r["session"] for r in cases})
    route = {s: i % 2 for i, s in enumerate(tasks)}
    cases.sort(key=lambda r: (r["turn"], r["session"]))
    if args.smoke:
        # Real snapshots test every route before the complete-session campaign.
        cases = [
            min(cases, key=lambda r: r["prompt_tokens"]),
            min(cases, key=lambda r: abs(r["prompt_tokens"] - 12000)),
            min(cases, key=lambda r: abs(r["prompt_tokens"] - 32000)),
            max(cases, key=lambda r: r["prompt_tokens"]),
        ]
    if not cases:
        raise ValueError("Workload is empty")
    for row in cases:
        needed = math.ceil((row["prompt_tokens"] + 1) / 128) * 128 * CFG["kv_bytes_per_token"]
        if needed > CFG["source_budget_bytes"]:
            raise ValueError("Request exceeds source budget")
        if args.d_pending_budget_gib and needed > args.d_pending_budget_gib * 2**30:
            raise ValueError("Request exceeds D pending budget")
    out = ROOT / "runs" / args.label
    out.mkdir(exist_ok=False)
    rng = random.Random(args.seed)
    routes = random.Random(args.seed + 2000)
    previous = {}
    at = 0
    work = []
    for index, r in enumerate(cases):
        if index:
            at += rng.expovariate(args.rate)
        d = route[r["session"]] if args.routing == "sticky" else routes.randrange(2)
        p = route[r["session"]]
        if args.smoke:
            p = index // 2
            d = index % 2
        work.append(
            {
                "index": index,
                "row": r,
                "p": p,
                "d": d,
                "offered": at,
                "previous": previous.get(r["session"]),
            }
        )
        previous[r["session"]] = index
    (out / "plan.json").write_text(
        json.dumps([{k: v for k, v in x.items() if k != "row"} for x in work])
    )
    p_slots = [asyncio.Semaphore(4) for _ in range(2)]
    d_slots = [asyncio.Semaphore(args.d_inflight) for _ in range(2)]
    complete = [asyncio.Event() for _ in work]
    reserved = [0, 0]
    condition = asyncio.Condition()
    d_reserved = [0, 0]
    d_budget = int(args.d_pending_budget_gib * 2**30)
    failure_event = asyncio.Event()
    collected = []
    (ROOT / "phase.json").write_text(
        json.dumps(
            {
                "label": args.label,
                "routing": args.routing,
                "rate": args.rate,
                "requests": len(work),
            }
        )
    )
    async with httpx.AsyncClient(
        timeout=600, trust_env=False, limits=httpx.Limits(max_connections=96)
    ) as client:
        clocks = {}
        for role, port in [("P", 38698), ("D", 38699)]:
            samples = []
            for _ in range(5):
                begin = time.time_ns()
                try:
                    response = await client.get(
                        f"http://{CFG['hosts'][role]}:{CFG.get('clock_ports', {}).get(role, port)}/"
                    )
                    response.raise_for_status()
                    value = response.json()
                    end = time.time_ns()
                    samples.append(
                        {
                            "rtt_ns": end - begin,
                            "offset_ns": value["epoch_ns"] - (begin + end) // 2,
                            "remote_epoch_ns": value["epoch_ns"],
                        }
                    )
                except Exception as error:
                    samples.append({"error": repr(error)})
            clocks[role] = samples
        (out / "clock-calibration.json").write_text(json.dumps(clocks, indent=2))
        origin = time.monotonic() + 0.1
        (out / "manifest.json").write_text(
            json.dumps(
                {
                    "label": args.label,
                    "routing": args.routing,
                    "rate": args.rate,
                    "seed": args.seed,
                    "smoke": args.smoke,
                    "requests": len(work),
                    "sessions": len({x["row"]["session"] for x in work}),
                    "output_cap": args.output_cap,
                    "d_inflight": args.d_inflight,
                    "runner_source": __file__,
                    "config": CFG,
                    "selected_sessions": tasks,
                    "D_pending_budget_bytes": d_budget,
                    "origin_epoch_ns": time.time_ns() + int((origin - time.monotonic()) * 1e9),
                    "origin_monotonic_s": origin,
                    "workload": str(data),
                },
                indent=2,
            )
        )

        async def request(item):
            r = item["row"]
            p = item["p"]
            d = item["d"]
            key = f"{args.label}-{item['index']:04d}"
            needed = math.ceil((r["prompt_tokens"] + 1) / 128) * 128 * CFG["kv_bytes_per_token"]
            record = {
                "id": key,
                "session": r["session"],
                "turn": r["turn"],
                "prompt_tokens": r["prompt_tokens"],
                "output_tokens_target": 16
                if args.smoke
                else min(r["qwen_output_tokens"], args.output_cap or r["qwen_output_tokens"]),
                "P": f"P{p}",
                "D": f"D{d}",
                "offered_s": item["offered"],
            }
            held = False
            d_held = False
            try:
                await asyncio.sleep(max(0, origin + item["offered"] - time.monotonic()))
                if item["previous"] is not None:
                    await complete[item["previous"]].wait()
                record["ready_s"] = time.monotonic() - origin
                async with asyncio.timeout(600):
                    async with p_slots[p]:
                        record["P_slot_s"] = time.monotonic() - origin
                        async with condition:
                            await condition.wait_for(
                                lambda: reserved[p] + needed <= CFG["source_budget_bytes"]
                            )
                            reserved[p] += needed
                            held = True
                        record["P_submit_s"] = time.monotonic() - origin
                        payload = {
                            "model": CFG.get("served_model_name", "qwen14"),
                            "prompt": r["prompt_token_ids"],
                            "temperature": 0,
                            "ignore_eos": True,
                            "request_id": key,
                            "seed": args.seed,
                            "cache_salt": f"longpd-{args.label}-{r['session']}",
                        }
                        result = await client.post(
                            url(f"P{p}") + "/v1/completions",
                            json={**payload, "max_tokens": 1, "stream": False},
                        )
                        result.raise_for_status()
                        record["P_response_s"] = time.monotonic() - origin
                        record["P_usage"] = result.json().get("usage")
                        marker = ROOT / "visibility" / f"P{p}" / f"{key}.tp0.visible"
                        while not marker.exists():
                            await asyncio.sleep(0.002)
                        record["visible_s"] = time.monotonic() - origin
                    async with d_slots[d]:
                        record["D_slot_s"] = time.monotonic() - origin
                        if d_budget:
                            if needed > d_budget:
                                raise ValueError("Request exceeds D pending budget")
                            async with condition:
                                await condition.wait_for(lambda: d_reserved[d] + needed <= d_budget)
                                d_reserved[d] += needed
                                d_held = True
                        record["D_submit_s"] = time.monotonic() - origin
                        record["token_times_s"] = []
                        async with client.stream(
                            "POST",
                            url(f"D{d}") + "/v1/completions",
                            json={
                                **payload,
                                "max_tokens": record["output_tokens_target"],
                                "stream": True,
                                "stream_options": {"include_usage": True},
                                "return_token_ids": True,
                            },
                        ) as response:
                            response.raise_for_status()
                            for_done = False
                            async for line in response.aiter_lines():
                                if not line.startswith("data: "):
                                    continue
                                text = line[6:]
                                if text == "[DONE]":
                                    for_done = True
                                    break
                                chunk = json.loads(text)
                                if chunk.get("error"):
                                    raise RuntimeError(str(chunk["error"]))
                                if chunk.get("usage"):
                                    record["D_usage"] = chunk["usage"]
                                for choice in chunk.get("choices", []):
                                    if choice.get("token_ids") or choice.get("text"):
                                        now = time.monotonic() - origin
                                        if "first_s" not in record:
                                            record["first_s"] = now
                                            async with condition:
                                                reserved[p] -= needed
                                                held = False
                                                condition.notify_all()
                                                if d_held:
                                                    d_reserved[d] -= needed
                                                    d_held = False
                                                    condition.notify_all()
                                        record["token_times_s"].append(now)
                        if not for_done or "first_s" not in record:
                            raise RuntimeError("Incomplete decode stream")
                        record["status"] = "ok"
            except asyncio.CancelledError:
                record.update(
                    status="cancelled",
                    error="Campaign cancelled after a request failure",
                )
                raise
            except Exception as e:
                record.update(status="error", error=repr(e))
                failure_event.set()
            finally:
                if held:
                    async with condition:
                        reserved[p] -= needed
                        condition.notify_all()
                if d_held:
                    async with condition:
                        d_reserved[d] -= needed
                        condition.notify_all()
                record["finish_s"] = time.monotonic() - origin
                record["epoch_ns"] = time.time_ns()
                append(out / "requests.jsonl", record)
                collected.append(record)
                complete[item["index"]].set()
                print(
                    json.dumps(
                        {
                            k: record.get(k)
                            for k in [
                                "id",
                                "status",
                                "prompt_tokens",
                                "first_s",
                                "finish_s",
                                "error",
                            ]
                        }
                    ),
                    flush=True,
                )
            return record

        pending = [asyncio.create_task(request(item)) for item in work]
        group = asyncio.gather(*pending, return_exceptions=True)
        failed = asyncio.create_task(failure_event.wait())
        await asyncio.wait([group, failed], return_when=asyncio.FIRST_COMPLETED)
        if failure_event.is_set():
            for task in pending:
                if not task.done():
                    task.cancel()
        await group
        failed.cancel()
        records = collected
    good = [r for r in records if r["status"] == "ok"]
    span = time.monotonic() - origin
    metrics = {
        "TTFT_s": lambda r: r["first_s"] - r["ready_s"],
        "nominal_to_first_s": lambda r: r["first_s"] - r["offered_s"],
        "session_wait_s": lambda r: r["ready_s"] - r["offered_s"],
        "P_queue_s": lambda r: r["P_slot_s"] - r["ready_s"],
        "source_budget_wait_s": lambda r: r["P_submit_s"] - r["P_slot_s"],
        "P_service_s": lambda r: r["P_response_s"] - r["P_submit_s"],
        "publish_wait_s": lambda r: r["visible_s"] - r["P_response_s"],
        "D_queue_s": lambda r: r["D_submit_s"] - r["visible_s"],
        "D_pending_budget_wait_s": lambda r: r["D_submit_s"] - r.get("D_slot_s", r["D_submit_s"]),
        "D_to_first_s": lambda r: r["first_s"] - r["D_submit_s"],
        "E2E_s": lambda r: r["finish_s"] - r["ready_s"],
    }
    summary = {
        "label": args.label,
        "routing": args.routing,
        "rate": args.rate,
        "seed": args.seed,
        "counts": dict(Counter(r["status"] for r in records)),
        "span_s": span,
        "completed_rps": len(good) / span,
        "output_tokens_s": sum(r.get("D_usage", {}).get("completion_tokens", 0) for r in good)
        / span,
        **{k: quantiles([fn(r) for r in good]) for k, fn in metrics.items()},
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary), flush=True)
    if len(good) != len(work):
        raise RuntimeError("Replay contains failed or incomplete requests")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--label", required=True)
    p.add_argument("--rate", type=float, default=3)
    p.add_argument("--routing", choices=["sticky", "random"], default="random")
    p.add_argument("--seed", type=int, default=17)
    p.add_argument("--sessions", type=int, default=0)
    p.add_argument("--d-pending-budget-gib", type=float, default=0)
    p.add_argument("--d-inflight", type=int, choices=[1, 2, 4, 8, 16], default=16)
    p.add_argument("--output-cap", type=int, default=0)
    p.add_argument("--smoke", action="store_true")
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--workload", type=Path)
    args = p.parse_args()
    if not math.isfinite(args.rate) or args.rate <= 0 or args.output_cap < 0 or args.sessions < 0:
        p.error("rate must be positive; output-cap and sessions must be non-negative")
    if not math.isfinite(args.d_pending_budget_gib) or args.d_pending_budget_gib < 0:
        p.error("D pending budget must be finite and non-negative")
    if (
        Path(args.label).name != args.label
        or args.label in (".", "..")
        or "/" in args.label
        or "\\" in args.label
    ):
        p.error("label must be one directory name")
    ROOT = external_output(args.root)
    CFG = json.loads((ROOT / "config.json").read_text())
    INST = {r["id"]: r for r in CFG["instances"]}
    (ROOT / "runs").mkdir(exist_ok=True)
    asyncio.run(run(args))
