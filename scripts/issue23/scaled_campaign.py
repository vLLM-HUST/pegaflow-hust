"""Persistent paired transport experiments with frozen cohort sizes."""

import argparse
import asyncio
import json
import os
import time
from pathlib import Path

from client import make_client, open_loop, stream_request
from common import distribution, read_json, read_jsonl, sha256, write_json
from deployment_runtime import inference_settings
from freeze_plan import freeze
from lifecycle import request, start

ORDERS = (
    ("rdma-burst", "rdma-smooth", "local_copy-smooth", "local_copy-burst"),
    ("rdma-smooth", "local_copy-burst", "rdma-burst", "local_copy-smooth"),
    ("local_copy-burst", "local_copy-smooth", "rdma-smooth", "rdma-burst"),
    ("local_copy-smooth", "rdma-burst", "local_copy-burst", "rdma-smooth"),
)


async def stop_services(config, service, output):
    errors = []
    for role in ("decode", "prefill"):
        try:
            await asyncio.to_thread(request, config, role, "stop", service)
        except Exception as error:
            errors.append({"role": role, "error": f"{type(error).__name__}: {error}"})
    write_json(
        output / "cleanup.json",
        {"status": "failed" if errors else "stopped", "errors": errors},
    )
    if errors:
        raise RuntimeError(f"Service cleanup failed: {errors}")


async def control(client, url, path, body=None):
    response = await client.post(url + path, json=body, timeout=60)
    if response.is_error:
        raise RuntimeError(f"{path}: {response.status_code} {response.text[:600]}")
    return response.json() if response.content else None


async def clear_host(client, urls):
    for url in urls:
        await control(client, url, "/cache/memory/cleanup")


async def reset_device(client, url):
    for _attempt in range(30):
        result = await control(client, url, "/reset_prefix_cache")
        if result is not False:
            return
        await asyncio.sleep(0.2)
    raise RuntimeError("Model still owns requests while resetting prefix cache")


def cohort_groups(cases, contract):
    size = contract["cohort_size"]
    if not 2 <= size <= 16 or len(cases) % size:
        raise ValueError("Complete cohorts of 2..16 requests are required")
    return [cases[i : i + size] for i in range(0, len(cases), size)]


async def start_network_background(config, service):
    settings = config.get("network_background")
    if not settings:
        return
    for role in ("prefill", "decode"):
        if settings.get("prestarted"):
            break
        argv = [
            config["runtime"]["python"],
            str(Path(config["runtime"]["scripts"]) / "rdma_background.py"),
            "--port",
            str(settings["port"]),
            "--expected-gbps",
            str(settings["target_gbps"]),
            "--qps",
            str(settings.get("qps", 4)),
            "--output",
            str(service / f"network-background-{role}.jsonl"),
        ]
        if role == "decode":
            argv += ["--host", config["nodes"]["prefill"]["host"]]
        await asyncio.to_thread(
            request,
            config,
            role,
            "start",
            service,
            [{"name": "network-background", "argv": argv}],
        )
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        await asyncio.sleep(1)
        try:
            result = await asyncio.to_thread(
                request,
                config,
                "decode",
                "read-trace",
                service,
                request_ids=["network-background-decode.jsonl"],
            )
        except (OSError, RuntimeError):
            continue
        events = result["events"]
        if any(e["event"] == "background_finished" for e in events):
            raise RuntimeError("RDMA competitor exited during warmup")
        measured = [
            e["bandwidth_gbps"] for e in events if e["event"] == "background_bandwidth"
        ]
        if len(measured) >= 2:
            observed = sum(measured[-2:]) / 2
            target = settings["target_gbps"]
            if not 0.8 * target <= observed <= 1.15 * target:
                raise RuntimeError(
                    f"RDMA competitor failed calibration: {observed} vs {target}"
                )
            write_json(
                service / "network-background-warmup.json",
                {
                    "target_gbps": target,
                    "observed_gbps": observed,
                    "calibrated_capacity_gbps": settings["capacity_gbps"],
                },
            )
            return
    raise TimeoutError("No RDMA competitor bandwidth samples")


def foreground_summary(output, plan, events=None):
    rows = read_jsonl(output / "client.jsonl")
    events = events if events is not None else read_jsonl(output / "engine.jsonl")
    expected = {r["request_id"]: r for r in plan["requests"]}
    completed = {
        e["request_id"]: e for e in events if e.get("event") == "bundle_completed"
    }
    posted = {e["request_id"]: e for e in events if e.get("event") == "bundle_posted"}
    eligible = {
        e["request_id"]: e for e in events if e.get("event") == "bundle_eligible"
    }
    if (
        len(rows) != len(expected)
        or {row["request_id"] for row in rows} != set(expected)
        or any(row["status"] != "ok" for row in rows)
    ):
        raise RuntimeError("Foreground request failed or is missing")
    if (
        set(completed) != set(expected)
        or set(posted) != set(expected)
        or set(eligible) != set(expected)
    ):
        raise RuntimeError("A foreground request bypassed the complete transfer path")
    for rid, item in expected.items():
        value = completed[rid]
        if value["status"] != "ok" or value["completed_payload_bytes"] != sum(
            op["logical_payload_bytes"] for op in item["operations"]
        ):
            raise RuntimeError("Incomplete or erroneous cache transfer")
    result = {
        "status": "complete",
        "requests": len(rows),
        "result_dir": str(output),
        "payload_bytes": sum(e["completed_payload_bytes"] for e in completed.values()),
        "ttft_seconds": distribution([r["ttft_seconds"] for r in rows]),
        "transfer_completion_ms": distribution(
            [
                (completed[rid]["monotonic_ns"] - posted[rid]["monotonic_ns"]) / 1e6
                for rid in expected
            ]
        ),
        "cache_access_ms": distribution(
            [
                (completed[rid]["monotonic_ns"] - eligible[rid]["monotonic_ns"]) / 1e6
                for rid in expected
            ]
        ),
        "timing_deviations": [
            e
            for e in events
            if e.get("event") == "cohort_posting_spread" and not e["pass"]
        ],
    }
    result["ttft_mean_seconds"] = sum(r["ttft_seconds"] for r in rows) / len(rows)
    write_json(output / "summary.json", result)
    return result


async def observers(client, model, output, label, max_tokens=4096):
    output.mkdir(parents=True, exist_ok=True)
    tasks = []
    for slot in range(4):
        rid = f"issue23-observer-{label}-{slot}"
        case = {
            "request_id": rid,
            "cohort_id": label,
            "input_tokens": 64,
            "payload": {
                "model": "hybrid-qwen35",
                "prompt": [16 + slot] * 64,
                "request_id": rid,
                "cache_salt": rid,
                "stream": True,
                "stream_options": {"include_usage": True},
                "return_token_ids": True,
                "temperature": 0,
                "seed": 17,
                "max_tokens": max_tokens,
                "ignore_eos": True,
                "add_special_tokens": False,
            },
        }
        tasks.append(
            asyncio.create_task(
                stream_request(client, model, case, output, time.monotonic(), 0, 600)
            )
        )
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        paths = (
            list((output / "sse").glob("*.jsonl")) if (output / "sse").exists() else []
        )
        if len(paths) == 4 and all(len(p.read_text().splitlines()) >= 8 for p in paths):
            return tasks
        if any(t.done() for t in tasks):
            await asyncio.gather(*tasks)
            raise RuntimeError("Observer exited before the foreground window")
        await asyncio.sleep(0.1)
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    raise RuntimeError("Observers failed to enter decode")


async def campaign(
    config,
    output,
    limit=None,
    rate=1_000_000_000,
    interval=1200,
    period=35,
    background_rate=0,
    observer_tokens=4096,
    baseline_seconds=0,
    recovery_seconds=0,
):
    root = Path(config["root"])
    cases = read_json(root / "prepared/cases.json")
    contract = read_json(root / "prepared/contract.json")
    output.mkdir(parents=True, exist_ok=True)
    frozen = {
        **contract,
        "rdma_rate_bps": rate,
        "smooth_interval_ms": interval,
        "min_trial_window_seconds": period,
        "background_rdma_rate_bps": background_rate,
        "observer_max_tokens": observer_tokens,
        "inference": inference_settings(config),
        "network_background": config.get("network_background"),
        "campaign_budget_seconds": config.get("campaign_budget_seconds"),
        "foreground_timeout_seconds": config.get("foreground_timeout_seconds", 180),
        "baseline_seconds": baseline_seconds,
        "recovery_seconds": recovery_seconds,
        "event_window_seconds": config.get("event_window_seconds", 0),
        "source_discovery_grace_seconds": config.get("discovery_grace_seconds", 5),
        "limit": limit,
        "runtime_config_sha256": sha256(root / "runtime/two-node.json"),
        "transport_model": "aggregate software RDMA injection budget"
        if rate
        else "native RDMA with independent network background"
        if config.get("network_background")
        else "native RDMA",
        "observer_policy": "four fresh decoder requests per arm; warm at eight SSE events",
    }
    contract_path = output / "campaign-contract.json"
    if contract_path.exists() and read_json(contract_path) != frozen:
        raise ValueError("Resume requires the same frozen campaign contract")
    write_json(contract_path, frozen)
    write_json(output / "status.json", {"status": "running"})
    os.environ.update(
        TG_MODE="capture", TG_BACKEND="rdma", TG_SCHEDULE="burst", TG_PLANS_DIR=""
    )
    service = (
        Path(config["prestarted_service"])
        if config.get("prestarted_service")
        else output / f"service-{time.time_ns()}"
    )
    service.mkdir(parents=True, exist_ok=bool(config.get("prestarted_service")))
    p = config["nodes"]["prefill"]
    d = config["nodes"]["decode"]
    pengine = f"http://{p['host']}:{p['engine_http_port']}"
    dengine = f"http://{d['host']}:{d['engine_http_port']}"
    pmodel = f"http://{p['host']}:{p['model_port']}"
    dmodel = f"http://{d['host']}:{d['model_port']}"
    router = f"http://{p['host']}:{config['router_port']}"
    groups = cohort_groups(cases, contract)
    if limit is not None:
        groups = groups[:limit]
    try:
        if not config.get("prestarted_service"):
            await asyncio.to_thread(start, config, service)
        await start_network_background(config, service)
        deadline = time.monotonic() + config.get(
            "campaign_budget_seconds", float("inf")
        )
        async with make_client(20) as client:
            for index, members in enumerate(groups):
                cohort_root = output / f"cohort-{index:04d}"
                if (cohort_root / "complete.json").exists():
                    continue
                cohort = cohort_root / f"attempt-{time.time_ns()}"
                cohort.mkdir(parents=True, exist_ok=True)
                capture = cohort / "capture"
                capture.mkdir(exist_ok=True)
                trace = capture / f"engine-{time.time_ns()}.jsonl"
                await control(
                    client,
                    dengine,
                    "/issue23/configure",
                    {
                        "plan": None,
                        "backend": "rdma",
                        "schedule": "burst",
                        "trace_path": str(trace),
                    },
                )
                await control(client, pengine, "/issue23/release-source")
                await clear_host(client, [pengine, dengine])
                await reset_device(client, pmodel)
                await reset_device(client, dmodel)
                write_json(capture / "cases.json", members)
                write_json(
                    capture / "contract.json",
                    {**contract, "smooth_interval_ms": interval, "rdma_rate_bps": rate},
                )
                await open_loop(
                    client,
                    router,
                    members,
                    capture,
                    config.get("foreground_timeout_seconds", 180),
                )
                local = await asyncio.to_thread(
                    request,
                    config,
                    "decode",
                    "read-trace",
                    capture,
                    request_ids=[trace.name],
                )
                observed = capture / "observed.jsonl"
                observed.write_text(
                    "".join(json.dumps(e) + "\n" for e in local["events"])
                )
                freeze(
                    capture / "cases.json",
                    capture / "contract.json",
                    observed,
                    cohort / "plan.json",
                    "decode-engine",
                    [0, 1, 2, 3],
                    [0, 1],
                )
                plan = read_json(cohort / "plan.json")
                frozen = await control(client, pengine, "/issue23/freeze-source", plan)
                write_json(cohort / "source.json", frozen)
                summaries = {}
                for arm in ORDERS[index % 4]:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Campaign measurement budget reached")
                    trial = cohort / arm
                    trial.mkdir(exist_ok=True)
                    if (trial / "summary.json").exists():
                        summaries[arm] = read_json(trial / "summary.json")
                        continue
                    backend, schedule = arm.rsplit("-", 1)
                    await clear_host(client, [dengine])
                    await reset_device(client, pmodel)
                    await reset_device(client, dmodel)
                    await control(
                        client,
                        dengine,
                        "/issue23/configure",
                        {
                            "plan": plan,
                            "backend": backend,
                            "schedule": schedule,
                            "trace_path": str(trial / "engine.jsonl"),
                            "inherit_hidden": True,
                        },
                    )
                    pacing_before = await client.get(dengine + "/issue23/pacing")
                    pacing_before.raise_for_status()
                    write_json(trial / "pacing-before.json", pacing_before.json())
                    activation = await control(client, dengine, "/issue23/activate")
                    write_json(trial / "activation.json", activation)
                    for case in members:
                        for rank in (0, 1):
                            (
                                service
                                / "visibility"
                                / f"{case['request_id']}.tp{rank}.visible"
                            ).unlink(missing_ok=True)
                    background = await observers(
                        client,
                        dmodel,
                        trial / "observers",
                        f"{index:04d}-{arm}",
                        observer_tokens,
                    )
                    window = {"baseline_started_unix_ns": time.time_ns()}
                    await asyncio.sleep(baseline_seconds)
                    network_background = False
                    started = time.monotonic()
                    window["foreground_started_unix_ns"] = time.time_ns()
                    write_json(
                        service / "phase.json",
                        {"phase": "measuring", "cohort": index, "arm": arm},
                    )
                    try:
                        if background_rate:
                            op = next(
                                op
                                for op in plan["requests"][0]["operations"]
                                if op["storage_group_id"] == 0
                            )
                            await control(
                                client,
                                dengine,
                                "/issue23/background/start",
                                {
                                    "remote_addr": op["source"],
                                    "namespace": op["namespace"],
                                    "block_hash_hex": op["block_hash_hex"],
                                    "rate_bps": background_rate,
                                },
                            )
                            network_background = True
                        await open_loop(
                            client,
                            router,
                            members,
                            trial,
                            config.get("foreground_timeout_seconds", 180),
                        )
                        window["foreground_finished_unix_ns"] = time.time_ns()
                        local = await asyncio.to_thread(
                            request,
                            config,
                            "decode",
                            "read-trace",
                            trial,
                            request_ids=["engine.jsonl"],
                        )
                        (trial / "observed.jsonl").write_text(
                            "".join(json.dumps(e) + "\n" for e in local["events"])
                        )
                        summaries[arm] = foreground_summary(
                            trial, plan, local["events"]
                        )
                        eligible_times = [
                            event["wall_time_unix_ns"]
                            for event in local["events"]
                            if event["event"] == "bundle_eligible"
                        ]
                        window["cohort_ready_unix_ns"] = max(eligible_times)
                        window["event_window_ends_unix_ns"] = max(eligible_times) + int(
                            config.get("event_window_seconds", 0) * 1e9
                        )
                        await asyncio.sleep(
                            max(
                                0,
                                (window["event_window_ends_unix_ns"] - time.time_ns())
                                / 1e9,
                            )
                        )
                        await asyncio.sleep(recovery_seconds)
                        await asyncio.sleep(
                            max(0, period - (time.monotonic() - started))
                        )
                        if any(task.done() for task in background):
                            raise RuntimeError(
                                "A decoder observer exited before the trial window ended"
                            )
                        window["observation_finished_unix_ns"] = time.time_ns()
                        write_json(trial / "measurement-window.json", window)
                        if config.get("network_background"):
                            observed_background = await asyncio.to_thread(
                                request,
                                config,
                                "decode",
                                "read-trace",
                                service,
                                request_ids=["network-background-decode.jsonl"],
                            )
                            events = observed_background["events"]
                            if any(e["event"] == "background_finished" for e in events):
                                raise RuntimeError(
                                    "RDMA competitor exited during trial"
                                )
                            write_json(
                                trial / "network-background.json",
                                {
                                    "report_scope": "two-second reports overlapping the foreground window",
                                    "samples": [
                                        e
                                        for e in events
                                        if e["event"] == "background_bandwidth"
                                        and e["unix_ns"]
                                        >= window["foreground_started_unix_ns"]
                                        and e["unix_ns"] - 2_000_000_000
                                        <= window["observation_finished_unix_ns"]
                                    ],
                                },
                            )
                    finally:
                        network_stats = {}
                        try:
                            if network_background:
                                network_stats = await control(
                                    client, dengine, "/issue23/background/stop"
                                )
                                write_json(
                                    trial / "network-background.json", network_stats
                                )
                        finally:
                            for task in background:
                                task.cancel()
                            await asyncio.gather(*background, return_exceptions=True)
                        if network_background and network_stats.get("error"):
                            raise RuntimeError(network_stats["error"])
                    pacing_after = await client.get(dengine + "/issue23/pacing")
                    pacing_after.raise_for_status()
                    write_json(trial / "pacing-after.json", pacing_after.json())
                    await reset_device(client, dmodel)
                    print(
                        json.dumps({"cohort": index, "arm": arm, **summaries[arm]}),
                        flush=True,
                    )
                write_json(
                    cohort_root / "complete.json",
                    {"attempt": str(cohort), "summaries": summaries},
                )
            write_json(
                output / "status.json",
                {
                    "status": "complete",
                    "cohorts": len(groups),
                    "requests_per_arm": sum(len(group) for group in groups),
                },
            )
    except BaseException as error:
        write_json(
            output / "status.json",
            {"status": "failed", "error": f"{type(error).__name__}: {error}"},
        )
        raise
    finally:
        await stop_services(config, service, output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--rate", type=int, default=1_000_000_000)
    parser.add_argument("--interval", type=int, default=1200)
    parser.add_argument("--period", type=float, default=35)
    parser.add_argument("--background-rate", type=int, default=0)
    parser.add_argument("--observer-tokens", type=int, default=4096)
    parser.add_argument("--baseline-seconds", type=float, default=0)
    parser.add_argument("--recovery-seconds", type=float, default=0)
    args = parser.parse_args()
    config = read_json(args.config)
    asyncio.run(
        campaign(
            config,
            Path(config["root"]) / "runs" / args.label,
            args.limit,
            args.rate,
            args.interval,
            args.period,
            args.background_rate,
            args.observer_tokens,
            args.baseline_seconds,
            args.recovery_seconds,
        )
    )
