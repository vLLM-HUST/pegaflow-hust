"""Repeated cold/local-host/remote-host comparisons with fixed token budgets."""

import argparse
import asyncio
import copy
import json
import time
from pathlib import Path

import httpx
from client import make_client, open_loop, stream_request
from common import distribution, read_json, sha256, write_json


async def reset(client, deployment, host_cache):
    response = await client.post(deployment["decode_url"] + "/reset_prefix_cache", timeout=30)
    response.raise_for_status()
    if host_cache:
        for engine in deployment["engines"]:
            if engine.get("role", "decode") != "decode":
                continue
            response = await client.post(engine["url"] + "/cache/memory/cleanup", timeout=30)
            response.raise_for_status()


async def metrics(client, deployment, output, label):
    endpoints = {
        "prefill": deployment["prefill_url"],
        "decode": deployment["decode_url"],
        **{e["engine_id"]: e["url"] for e in deployment["engines"]},
    }
    for name, url in endpoints.items():
        response = await client.get(url + "/metrics", timeout=30)
        response.raise_for_status()
        (output / f"{label}-{name}.prom").write_text(response.text, encoding="utf-8")


def select_cases(prepared: Path, max_tokens: int, cohort_gap: float = 4):
    cases = []
    for split in ("smoke", "frontier-smoke"):
        path = prepared / f"{split}.json"
        for index, original in enumerate(read_json(path)[:4]):
            case = copy.deepcopy(original)
            case["request_id"] = f"perf-{split}-{index}"
            case["cohort_id"] = f"perf-{split}"
            case["smooth_slot"] = index
            case["planned_offset_seconds"] = (
                cohort_gap if split == "frontier-smoke" else 0
            ) + index * 0.02
            case["payload"].update(
                model="hybrid-qwen35",
                max_tokens=max_tokens,
                ignore_eos=True,
                return_token_ids=True,
                request_id=case["request_id"],
            )
            cases.append(case)
    if len(cases) != 8:
        raise ValueError("Expected four MMLU-Pro and four FrontierScience prepared requests")
    return cases


def summary(rows, elapsed):
    successful = [r for r in rows if r["status"] == "ok"]
    tokens = sum(r["usage"]["completion_tokens"] for r in successful)
    return {
        "requests": len(rows),
        "successful": len(successful),
        "ttft_seconds": distribution([r["ttft_seconds"] for r in successful]),
        "e2e_seconds": distribution([r["e2e_seconds"] for r in successful]),
        "inter_text_event_ms": distribution(
            [x for r in successful for x in r["inter_text_event_ms"]]
        ),
        "output_tokens": distribution([r["usage"]["completion_tokens"] for r in successful]),
        "dispatch_lateness_ms": distribution([r["dispatch_lateness_ms"] for r in rows]),
        "elapsed_seconds": elapsed,
        "output_tokens_per_second": tokens / elapsed,
        "requests_per_second": len(successful) / elapsed,
    }


async def benchmark(root, deployment, prepared, rounds, output_tokens, cohort_gap=4):
    if rounds <= 0 or output_tokens <= 0 or cohort_gap < 0:
        raise ValueError(
            "Rounds and output budget must be positive; cohort gap must be nonnegative"
        )
    root.mkdir(parents=True, exist_ok=False)
    cases = select_cases(prepared, output_tokens, cohort_gap)
    write_json(root / "cases.json", cases)
    write_json(
        root / "contract.json",
        {
            "rounds": rounds,
            "cohort_gap_seconds": cohort_gap,
            "measurement_scope": "decode serving; source production and local-host priming precede measurement",
            "max_tokens": output_tokens,
            "ignore_eos": True,
            "deployment": deployment,
            "cases_sha256": sha256(root / "cases.json"),
            "prepared_inputs": {
                s: sha256(prepared / f"{s}.json") for s in ("smoke", "frontier-smoke")
            },
            "arms": ["cold", "local_host", "remote_host"],
            "output_policy": "record output differences; stop only for explicit semantic or serving failures",
            "clock_scope": "client monotonic times; each worker/engine clock analyzed separately",
        },
    )
    report = {"status": "running", "rounds": [], "aggregate": {}}
    aggregates = {name: [] for name in ("cold", "local_host", "remote_host")}
    elapsed_by_arm = dict.fromkeys(aggregates, 0)
    async with make_client(8) as client:
        for url in (deployment["prefill_url"], deployment["decode_url"]):
            last_health_error = None
            for attempt in range(300):
                try:
                    response = await client.get(url + "/health", timeout=3)
                    if response.status_code == 200:
                        break
                except httpx.HTTPError as error:
                    last_health_error = type(error).__name__
                if attempt % 15 == 0:
                    print("WAITING_MODEL", url, attempt, last_health_error, flush=True)
                await asyncio.sleep(2)
            else:
                raise RuntimeError("Model startup timed out")
        # Exercise both model paths once before measured requests.
        warm = root / "warmup"
        warm.mkdir()
        for role in ("prefill", "decode"):
            case = copy.deepcopy(cases[0])
            case["request_id"] = f"perf-warm-{role}"
            case["payload"].update(
                request_id=case["request_id"],
                max_tokens=16,
                cache_salt=f"perf-warm-{role}",
            )
            await stream_request(
                client, deployment[role + "_url"], case, warm, time.monotonic(), 0, 240
            )
        for round_index in range(rounds):
            salt = f"issue23-perf-{root.name}-{round_index}"
            ordered = ["cold", "local_host", "remote_host"]
            ordered = ordered[round_index % 3 :] + ordered[: round_index % 3]
            current = copy.deepcopy(cases)
            for case in current:
                case["request_id"] += f"-r{round_index:02d}"
                case["payload"].update(
                    request_id=case["request_id"],
                    cache_salt=salt + "-" + case["request_id"],
                )
            preparation = root / f"round-{round_index:02d}-source"
            preparation.mkdir()
            write_json(
                root / "current-phase.json",
                {"round": round_index, "phase": "source_prepare"},
            )
            for case in current:
                prime = copy.deepcopy(case)
                prime["request_id"] += "-source"
                prime["payload"].update(request_id=prime["request_id"], max_tokens=1)
                await stream_request(
                    client,
                    deployment["prefill_url"],
                    prime,
                    preparation,
                    time.monotonic(),
                    0,
                    240,
                )
            round_report = {"round": round_index, "order": ordered, "arms": {}}
            for arm in ordered:
                output = root / f"round-{round_index:02d}-{arm}"
                output.mkdir()
                await reset(client, deployment, host_cache=True)
                if arm == "local_host":
                    prime_output = output / "prime"
                    prime_output.mkdir()
                    for case in current:
                        prime = copy.deepcopy(case)
                        prime["request_id"] += "-prime"
                        prime["payload"].update(request_id=prime["request_id"], max_tokens=1)
                        await stream_request(
                            client,
                            deployment["decode_url"],
                            prime,
                            prime_output,
                            time.monotonic(),
                            0,
                            240,
                        )
                    await reset(client, deployment, host_cache=False)
                measured = copy.deepcopy(current)
                for case in measured:
                    case["request_id"] += "-" + arm
                    case["payload"]["request_id"] = case["request_id"]
                    if arm == "cold":
                        case["payload"]["cache_salt"] += "-cold"
                write_json(output / "cases.json", measured)
                await metrics(client, deployment, output, "before")
                write_json(root / "current-phase.json", {"round": round_index, "phase": arm})
                started = time.monotonic()
                rows = await open_loop(client, deployment["decode_url"], measured, output, 240)
                elapsed = time.monotonic() - started
                if any(row["usage"]["completion_tokens"] != output_tokens for row in rows):
                    raise RuntimeError("Fixed output token budget was not respected")
                await metrics(client, deployment, output, "after")
                result = summary(rows, elapsed)
                write_json(output / "summary.json", result)
                round_report["arms"][arm] = result
                aggregates[arm].extend(rows)
                elapsed_by_arm[arm] += elapsed
                print("ARM_RESULT", round_index, arm, json.dumps(result), flush=True)
            report["rounds"].append(round_report)
            report["aggregate"] = {
                a: summary(rows, elapsed_by_arm[a]) for a, rows in aggregates.items()
            }
            write_json(root / "summary.json", report)
        report["status"] = "completed"
        write_json(root / "current-phase.json", {"phase": "completed"})
        write_json(root / "summary.json", report)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--deployment", type=Path, required=True)
    parser.add_argument("--prepared", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=10)
    parser.add_argument("--output-tokens", type=int, default=64)
    parser.add_argument("--cohort-gap", type=float, default=4)
    args = parser.parse_args()
    asyncio.run(
        benchmark(
            args.root,
            read_json(args.deployment),
            args.prepared,
            args.rounds,
            args.output_tokens,
            args.cohort_gap,
        )
    )
