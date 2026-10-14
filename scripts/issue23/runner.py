"""Run capture or complete experimental arms after deployment qualification."""

import argparse
import asyncio
import json
import os
import re
import subprocess
import time
from pathlib import Path

from analyze import analyze_run
from client import make_client, open_loop, stream_request
from common import read_json, read_jsonl, sha256, write_json
from freeze_plan import freeze, validate_plan


def validate_cases(cases, contract, split, cases_path):
    summary = contract["split_summary"][split]
    if sha256(cases_path) != summary["sha256"] or len(cases) != summary["requests"]:
        raise ValueError("Request manifest changed after preparation")
    cohorts, ids = {}, set()
    for case in cases:
        rid = case["request_id"]
        if not re.fullmatch(r"[A-Za-z0-9._-]+", rid) or rid in ids:
            raise ValueError("Invalid or duplicate request identity")
        ids.add(rid)
        cohorts.setdefault(case["cohort_id"], []).append(case["smooth_slot"])
        if case["payload"]["request_id"] != rid or case["payload"]["prompt"] != case["prompt"]:
            raise ValueError("Payload drift")
    if any(sorted(slots) != [0, 1, 2, 3] for slots in cohorts.values()):
        raise ValueError("Incomplete cohort")


def validate_deployment(deployment):
    q = deployment.get("qualification", {})
    required = [
        "single_replica",
        "rdma_link",
        "runtime_identity",
        "cache_reset",
        "marker_visibility",
        "rank_bundle_topology",
        "memlock",
    ]
    if deployment.get("experiment_scope") == "attention_transfer_diagnostic":
        if deployment.get("hybrid_prefix_reuse") is not False:
            raise ValueError("Diagnostic deployment must record recurrent-state fallback")
        required.append("pd_output_match")
    elif deployment.get("experiment_scope") == "hybrid_performance":
        if deployment.get("hybrid_prefix_reuse") is not True:
            raise ValueError("Hybrid performance deployment must record actual prefix reuse")
        if deployment.get("output_policy") != "observe_numerical_differences":
            raise ValueError("Hybrid performance deployment must record its output policy")
        required.append("pd_hybrid_semantics")
        if sorted(deployment.get("expected_storage_groups", [])) != [0, 1, 2, 3]:
            raise ValueError("Qwen3.5 four-arm deployment must freeze storage groups 0,1,2,3")
        sources = deployment.get("source_engines", [])
        if not sources or any(not e.get("url") or not e.get("plan_engine_id") for e in sources):
            raise ValueError("Hybrid four-arm deployment requires frozen source engines")
    else:
        required.append("pd_hybrid_correctness")
    policy = deployment.get("scheduler_policy", "sched_fifo")
    if policy not in ("sched_fifo", "sched_other"):
        raise ValueError("Unknown scheduler policy")
    required.append("normal_scheduler" if policy == "sched_other" else "realtime_scheduler")
    missing = [key for key in required if q.get(key) is not True]
    if missing:
        raise ValueError(f"Deployment qualification pending: {', '.join(missing)}")
    for key in ["router_url", "decode_url", "prefill_url"]:
        if not isinstance(deployment.get(key), str) or not deployment[key].startswith(
            ("http://", "https://")
        ):
            raise ValueError(f"Missing service URL: {key}")
    if deployment.get("prefill_ack_ranks") != 2:
        raise ValueError("Router must require both physical TP visibility markers")
    if not deployment.get("engines"):
        raise ValueError("No decode engines configured")
    ids = set()
    for engine in deployment["engines"]:
        if not re.fullmatch(r"[A-Za-z0-9._-]+", engine["engine_id"]) or engine["engine_id"] in ids:
            raise ValueError("Invalid or duplicate engine identity")
        ids.add(engine["engine_id"])
        if (
            not engine.get("url")
            or not engine.get("trace_path")
            or not engine.get("covered_tp_ranks")
        ):
            raise ValueError("Engine URL, trace path and physical TP mapping are required")
    ranks = [rank for e in deployment["engines"] for rank in e["covered_tp_ranks"]]
    if sorted(ranks) != [0, 1]:
        raise ValueError("Decode engines must cover physical TP ranks 0 and 1")
    for key in ["start_argv", "stop_argv"]:
        if not deployment.get(key) or not all(isinstance(v, str) for v in deployment[key]):
            raise ValueError(f"Missing lifecycle command: {key}")


async def hook(deployment, key, environment, output, timeout):
    command = deployment.get(key)
    if not command:
        return
    env = os.environ.copy()
    env.update(environment)
    with (output / f"{key}.log").open("ab") as log:
        await asyncio.to_thread(
            subprocess.run,
            command,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            check=True,
            timeout=timeout,
            shell=False,
        )


async def post_control(client, url, path, output, name):
    response = await client.post(url.rstrip("/") + path, timeout=30)
    response.raise_for_status()
    try:
        value = response.json()
    except ValueError:
        value = {"response_text": response.text}
    if value is False:
        raise ValueError(f"Control endpoint refused: {path}")
    write_json(output / f"{name}.json", value)
    return value


async def clear_decode(client, deployment, output, label):
    await post_control(
        client,
        deployment["decode_url"],
        "/reset_prefix_cache",
        output,
        f"{label}-device-cache",
    )
    for engine in deployment["engines"]:
        await post_control(
            client,
            engine["url"],
            "/cache/memory/cleanup",
            output,
            f"{label}-{engine['engine_id']}-host-cache",
        )


async def save_metrics(client, deployment, output, label):
    endpoints = [
        *deployment["engines"],
        *deployment.get("source_engines", []),
        {"engine_id": "prefill-model", "url": deployment["prefill_url"]},
        {"engine_id": "decode-model", "url": deployment["decode_url"]},
    ]
    for engine in endpoints:
        response = await client.get(engine["url"].rstrip("/") + "/metrics", timeout=30)
        response.raise_for_status()
        (output / f"{label}-{engine['engine_id']}.prom").write_text(response.text, encoding="utf-8")


def clear_markers(directory, cases, ranks):
    directory.mkdir(parents=True, exist_ok=True)
    for case in cases:
        for rank in range(ranks):
            (directory / f"{case['request_id']}.tp{rank}.visible").unlink(missing_ok=True)


def trace_paths(deployment, output):
    return {
        e["engine_id"]: Path(e["trace_path"].format(run_dir=str(output), engine_id=e["engine_id"]))
        for e in deployment["engines"]
    }


async def run_one(root, deployment, split, mode, arm, label):
    cases_path, contract_path = (
        root / f"prepared/{split}.json",
        root / "prepared/contract.json",
    )
    cases, contract = read_json(cases_path), read_json(contract_path)
    validate_cases(cases, contract, split, cases_path)
    validate_deployment(deployment)
    output = root / "runs" / label
    marker_dir = output / "visibility"
    paths = trace_paths(deployment, output)
    backend, schedule = arm.rsplit("-", 1)
    plans_dir = root / "prepared" / f"plans-{split}"
    plans = {}
    if mode == "arm":
        for engine in deployment["engines"]:
            plan_path = plans_dir / f"{engine['engine_id']}.json"
            plan = read_json(plan_path)
            validate_plan(plan)
            if (
                deployment.get("experiment_scope") == "hybrid_performance"
                and plan["schema_version"] != 2
            ):
                raise ValueError("Hybrid four-arm run requires a new schema-2 capture")
            if plan["freeze_provenance"]["cases_sha256"] != sha256(cases_path) or plan[
                "freeze_provenance"
            ]["contract_sha256"] != sha256(contract_path):
                raise ValueError("Frozen transfer plan provenance mismatch")
            plans[engine["engine_id"]] = plan
    output.mkdir(parents=True, exist_ok=False)
    env = {
        "TG_RUN_DIR": str(output),
        "TG_MARKER_DIR": str(marker_dir),
        "TG_MODE": mode,
        "TG_BACKEND": backend,
        "TG_SCHEDULE": schedule,
        "TG_PLANS_DIR": str(plans_dir),
        "TG_CASES": str(cases_path),
        "TG_CONTRACT": str(contract_path),
        "TG_SCHEDULER_POLICY": deployment.get("scheduler_policy", "sched_fifo"),
        "TG_HYBRID_PREFETCH": "1"
        if deployment.get("experiment_scope") == "hybrid_performance"
        else "0",
    }
    write_json(
        output / "run-contract.json",
        {
            "mode": mode,
            "arm": arm,
            "split": split,
            "cases_sha256": sha256(cases_path),
            "contract_sha256": sha256(contract_path),
            "deployment": deployment,
            "hook_environment": env,
        },
    )
    state = {"status": "starting", "started_unix": time.time()}
    write_json(output / "phase.json", {"phase": "starting", "arm": arm})
    try:
        await hook(deployment, "start_argv", env, output, 1200)
        async with make_client(len(cases)) as client:
            for url in [
                deployment["decode_url"],
                deployment["prefill_url"],
                *(e["url"] for e in deployment["engines"]),
            ]:
                response = await client.get(url.rstrip("/") + "/health", timeout=30)
                response.raise_for_status()
            preparation = output / "prepare"
            preparation.mkdir()
            await save_metrics(client, deployment, output, "before-prepare")
            state["status"] = "preparing"
            write_json(output / "phase.json", {"phase": "preparing", "arm": arm})
            for index, case in enumerate(cases):
                clear_markers(marker_dir, [case], deployment["prefill_ack_ranks"])
                await clear_decode(client, deployment, preparation, f"request-{index:04d}")
                prepare_case = {**case, "payload": dict(case["payload"])}
                if "prepare_max_tokens" in deployment:
                    cap = deployment["prepare_max_tokens"]
                    if not isinstance(cap, int) or cap <= 0:
                        raise ValueError("prepare_max_tokens must be a positive integer")
                    prepare_case["payload"]["max_tokens"] = cap
                await stream_request(
                    client,
                    deployment["router_url"],
                    prepare_case,
                    preparation,
                    time.monotonic(),
                    0,
                    contract["timeout_seconds"],
                )
            await save_metrics(client, deployment, output, "after-prepare")
            if mode == "arm":
                write_json(output / "phase.json", {"phase": "freezing", "arm": arm})
                for source in deployment.get("source_engines", []):
                    plan = plans[source["plan_engine_id"]]
                    response = await client.post(
                        source["url"].rstrip("/") + "/issue23/freeze-source", json=plan, timeout=30
                    )
                    response.raise_for_status()
                    frozen = response.json()
                    unique = {
                        (
                            op["namespace"],
                            op["block_hash_hex"],
                            op["slot_index"],
                            op["segment"],
                        ): op["logical_payload_bytes"]
                        for request in plan["requests"]
                        for op in request["operations"]
                    }
                    blocks = {(namespace, hash_value) for namespace, hash_value, _, _ in unique}
                    if (
                        frozen.get("status") != "PASS"
                        or frozen.get("source_blocks") != len(blocks)
                        or frozen.get("source_bytes") != sum(unique.values())
                    ):
                        raise ValueError(
                            "Frozen source snapshot differs from the full transfer plan"
                        )
                    write_json(output / f"source-snapshot-{source['plan_engine_id']}.json", frozen)
                await clear_decode(client, deployment, output, "pre-activation")
                for engine in deployment["engines"]:
                    activation = await post_control(
                        client,
                        engine["url"],
                        "/issue23/activate",
                        output,
                        f"activate-{engine['engine_id']}",
                    )
                    if (
                        activation.get("status") != "PASS"
                        or activation.get("hidden_blocks", 0) <= 0
                    ):
                        raise ValueError("Incomplete hidden snapshot activation")
                clear_markers(marker_dir, cases, deployment["prefill_ack_ranks"])
                await hook(deployment, "marker_clear_argv", env, output, 120)
                state["status"] = "measuring"
                write_json(output / "phase.json", {"phase": "measuring", "arm": arm})
                await open_loop(
                    client,
                    deployment["router_url"],
                    cases,
                    output,
                    contract["timeout_seconds"],
                )
                await save_metrics(client, deployment, output, "after-measurement")
        state["status"] = "completed"
    except BaseException as error:
        state.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        write_json(output / "phase.json", {"phase": "stopping", "arm": arm})
        write_json(output / "status.json", state)
        try:
            await hook(deployment, "stop_argv", env, output, 120)
        except BaseException as error:
            state.update(status="failed", teardown_error=f"{type(error).__name__}: {error}")
        try:
            await hook(deployment, "collect_argv", env, output, 120)
        except BaseException as error:
            state.update(status="failed", collection_error=f"{type(error).__name__}: {error}")
        state["finished_unix"] = time.time()
        write_json(output / "status.json", state)
    if state["status"] != "completed":
        raise ValueError("Lifecycle cleanup or collection failed; see status.json")
    try:
        if mode == "capture":
            summaries = {}
            plans_dir.mkdir(exist_ok=True)
            for engine, path in paths.items():
                summaries[engine] = freeze(
                    cases_path,
                    contract_path,
                    path,
                    plans_dir / f"{engine}.json",
                    engine,
                    deployment.get("expected_storage_groups"),
                    next(
                        e["covered_tp_ranks"]
                        for e in deployment["engines"]
                        if e["engine_id"] == engine
                    ),
                )
            write_json(plans_dir / "summary.json", summaries)
        else:
            traces = {engine: read_jsonl(path) for engine, path in paths.items()}
            summary = analyze_run(
                cases,
                read_jsonl(output / "client.jsonl"),
                traces,
                plans,
                schedule,
                backend,
                deployment.get("scheduler_policy", "sched_fifo"),
            )
            write_json(output / "analysis.json", summary)
            if not summary["valid_timing_arm"]:
                raise ValueError("Arm validity checks failed; see analysis.json")
    except BaseException as error:
        state.update(status="failed", analysis_error=f"{type(error).__name__}: {error}")
        write_json(output / "status.json", state)
        raise
    return str(output)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--deployment", required=True)
    parser.add_argument(
        "--split",
        choices=["main", "smoke", "calibration", "frontier-smoke"],
        default="smoke",
    )
    parser.add_argument("--mode", choices=["capture", "arm", "matrix"], default="capture")
    parser.add_argument(
        "--arm",
        choices=["rdma-smooth", "rdma-burst", "local_copy-smooth", "local_copy-burst"],
        default="rdma-smooth",
    )
    parser.add_argument("--label")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    contract = read_json(args.root / "prepared/contract.json")
    cases_path = args.root / f"prepared/{args.split}.json"
    validate_cases(read_json(cases_path), contract, args.split, cases_path)
    deployment = read_json(args.deployment)
    if args.dry_run:
        try:
            validate_deployment(deployment)
            qualification = "passed"
        except ValueError as error:
            qualification = str(error)
        print(
            json.dumps(
                {
                    "manifest": "passed",
                    "split": args.split,
                    "requests": contract["split_summary"][args.split]["requests"],
                    "deployment": qualification,
                    "round_order": contract["round_arm_order"],
                },
                indent=2,
            )
        )
        return
    if not args.label or not re.fullmatch(r"[A-Za-z0-9._-]+", args.label):
        raise ValueError("A unique safe --label is required")
    if args.mode == "matrix":
        for index, order in enumerate(contract["round_arm_order"], 1):
            for arm in order:
                print(
                    asyncio.run(
                        run_one(
                            args.root,
                            deployment,
                            args.split,
                            "arm",
                            arm,
                            f"{args.label}-round-{index:02d}-{arm}",
                        )
                    ),
                    flush=True,
                )
    else:
        print(
            asyncio.run(run_one(args.root, deployment, args.split, args.mode, args.arm, args.label))
        )


if __name__ == "__main__":
    main()
