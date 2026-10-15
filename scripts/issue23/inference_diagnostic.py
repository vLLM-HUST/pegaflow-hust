"""Compare inference execution profiles with network and external cache removed."""

import argparse
import asyncio
import json
import os
import re
import signal
import subprocess
import threading
import time
import urllib.request
from pathlib import Path

from client import make_client, open_loop
from common import distribution, read_json, write_json
from deployment_runtime import (
    environment,
    inference_settings,
    model_command,
    node_environment,
)


def remove_option(argv, name):
    index = argv.index(name)
    del argv[index : index + 2]


def sample(stop, phase, output):
    with output.open("w", encoding="utf-8", buffering=1) as stream:
        while not stop.is_set():
            started = time.monotonic()
            value = {
                "unix_ns": time.time_ns(),
                "phase": dict(phase),
                "cpu_stat": Path("/sys/fs/cgroup/cpu.stat").read_text(),
                "cpu_pressure": Path("/sys/fs/cgroup/cpu.pressure").read_text(),
            }
            try:
                process = subprocess.run(
                    ["npu-smi", "info"],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )
                value["npu_smi"] = process.stdout
                value["npu_smi_exit_code"] = process.returncode
                value["npu_usage"] = {}
                for device in re.findall(
                    r"^\|\s+(\d+)\s+910", process.stdout, re.MULTILINE
                ):
                    detail = subprocess.run(
                        ["npu-smi", "info", "-t", "usages", "-i", device, "-c", "0"],
                        capture_output=True,
                        text=True,
                        timeout=10,
                        check=False,
                    )
                    value["npu_usage"][device] = detail.stdout
            except (OSError, subprocess.TimeoutExpired) as error:
                value["sample_error"] = str(error)
            value["sampling_seconds"] = time.monotonic() - started
            stream.write(json.dumps(value) + "\n")
            stop.wait(max(0, 2 - (time.monotonic() - started)))


async def measure(config, role, output):
    model = (
        f"http://{config['nodes'][role]['host']}:{config['nodes'][role]['model_port']}"
    )
    results = []
    phase = {"name": "warmup"}
    stop = threading.Event()
    sampler = threading.Thread(
        target=sample, args=(stop, phase, output / "resources.jsonl")
    )
    sampler.start()
    try:
        async with make_client(40) as client:
            for label, concurrency, prompt, tokens in (
                ("warmup", 4, 64, 16),
                ("decode-4", 4, 64, 256),
                ("decode-16", 16, 64, 256),
                ("prefill-8", 8, 4097, 1),
            ):
                reset = await client.post(model + "/reset_prefix_cache", timeout=30)
                reset.raise_for_status()
                if reset.content and reset.json() is False:
                    raise RuntimeError("Previous diagnostic workload did not drain")
                path = output / label
                path.mkdir()
                cases = []
                for slot in range(concurrency):
                    rid = f"diagnostic-{label}-{slot}"
                    cases.append(
                        {
                            "request_id": rid,
                            "cohort_id": label,
                            "input_tokens": prompt,
                            "planned_offset_seconds": 0,
                            "payload": {
                                "model": "hybrid-qwen35",
                                "prompt": [16 + slot] * prompt,
                                "request_id": rid,
                                "cache_salt": rid,
                                "stream": True,
                                "stream_options": {"include_usage": True},
                                "return_token_ids": True,
                                "temperature": 0,
                                "seed": 17,
                                "max_tokens": tokens,
                                "ignore_eos": True,
                                "add_special_tokens": False,
                            },
                        }
                    )
                phase.update(name=label, concurrency=concurrency, input_tokens=prompt)
                started = time.monotonic()
                rows = await open_loop(client, model, cases, path, 300)
                elapsed = time.monotonic() - started
                if any(r["usage"]["completion_tokens"] != tokens for r in rows):
                    raise RuntimeError(
                        "Diagnostic output token budget did not complete"
                    )
                itl = [v for r in rows for v in r["inter_text_event_ms"]]
                value = {
                    "phase": label,
                    "requests": len(rows),
                    "elapsed_seconds": elapsed,
                    "output_tokens_per_second": concurrency * tokens / elapsed,
                    "ttft_seconds": distribution([r["ttft_seconds"] for r in rows]),
                    "itl_ms": distribution(itl),
                }
                results.append(value)
                write_json(output / "measurements.json", results)
                print(json.dumps(value), flush=True)
    finally:
        stop.set()
        sampler.join(timeout=15)
    return results


def run(config, role, output):
    output.mkdir(parents=True, exist_ok=False)
    argv = model_command(config, role)
    remove_option(argv, "--kv-transfer-config")
    env = node_environment(config, environment(config, role, output))
    write_json(
        output / "contract.json",
        {"argv": argv, "inference": inference_settings(config)},
    )
    with (output / "model.log").open("ab", buffering=0) as log:
        process = subprocess.Popen(
            argv,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    write_json(output / "owned-process.json", {"pid": process.pid, "argv": argv})
    model = (
        f"http://{config['nodes'][role]['host']}:{config['nodes'][role]['model_port']}"
    )
    try:
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(f"Model exited during startup: {process.returncode}")
            try:
                with urllib.request.urlopen(model + "/health", timeout=2) as response:
                    if response.status == 200:
                        break
            except OSError:
                pass
            time.sleep(2)
        else:
            raise TimeoutError("Model startup did not finish")
        values = asyncio.run(measure(config, role, output))
        write_json(
            output / "status.json", {"status": "complete", "measurements": values}
        )
    except BaseException as error:
        write_json(output / "status.json", {"status": "failed", "error": str(error)})
        raise
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--role", choices=("prefill", "decode"), required=True)
    parser.add_argument(
        "--profile", choices=("performance", "eager_sync"), required=True
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    config = read_json(args.config)
    config["inference_profile"] = args.profile
    run(config, args.role, args.output)
