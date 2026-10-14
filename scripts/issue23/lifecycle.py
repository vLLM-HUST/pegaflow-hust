"""Run the frozen two-node four-arm deployment through node-local agents."""

import argparse
import json
import os
import socket
import time
import urllib.request
from pathlib import Path

from common import read_json
from deployment_runtime import collector_command, engine_command, environment, model_command


def request(config, role, action, output, processes=None, request_ids=None):
    node = config["nodes"][role]
    body = json.dumps(
        {
            "action": action,
            "run_dir": str(output),
            "processes": processes or [],
            "environment": environment(config, role, output),
            "request_ids": request_ids or [],
        }
    ).encode()
    token = Path(config["control_token_path"]).read_text().strip()
    message = urllib.request.Request(
        f"http://{node['host']}:{node['control_port']}/control",
        data=body,
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(message, timeout=90) as response:
        result = json.load(response)
    if result.get("status") == "failed":
        raise RuntimeError(result["error"])
    return result


def wait_health(urls, timeout=900):
    pending = set(urls)
    deadline = time.monotonic() + timeout
    while pending and time.monotonic() < deadline:
        for url in list(pending):
            try:
                with urllib.request.urlopen(url + "/health", timeout=3) as response:
                    if response.status == 200:
                        pending.remove(url)
            except OSError:
                pass
        if pending:
            time.sleep(2)
    if pending:
        raise TimeoutError(f"Services did not become ready: {sorted(pending)}")


def start(config, output):
    runtime = config["runtime"]
    decode = config["nodes"]["decode"]
    meta = [
        str(Path(runtime["bin"]) / "pegaflow-metaserver"),
        "--addr",
        f"{decode['host']}:{decode['meta_port']}",
        "--http-addr",
        f"0.0.0.0:{decode['meta_http_port']}",
    ]
    request(config, "decode", "start", output, [{"name": "metaserver", "argv": meta}])
    wait_health([f"http://{decode['host']}:{decode['meta_http_port']}"], 90)
    for role in config["nodes"]:
        argv = engine_command(
            config,
            role,
            output,
            os.environ["TG_MODE"],
            os.environ["TG_BACKEND"],
            os.environ["TG_SCHEDULE"],
            os.environ["TG_PLANS_DIR"],
        )
        request(config, role, "start", output, [{"name": "engine", "argv": argv}])
    wait_health(
        [f"http://{node['host']}:{node['engine_http_port']}" for node in config["nodes"].values()],
        120,
    )
    for role in config["nodes"]:
        request(
            config, role, "start", output, [{"name": "model", "argv": model_command(config, role)}]
        )
    wait_health(
        [f"http://{node['host']}:{node['model_port']}" for node in config["nodes"].values()]
    )
    prefill = config["nodes"]["prefill"]
    router = [
        str(Path(runtime["bin"]) / "pegaflow-router"),
        "--host",
        "0.0.0.0",
        "--port",
        str(config["router_port"]),
        "--prefill",
        f"http://{prefill['host']}:{prefill['model_port']}",
        "--decode",
        f"http://{decode['host']}:{decode['model_port']}",
        "--prefill-ack-dir",
        str(output / "visibility"),
        "--prefill-ack-ranks",
        "2",
        "--prefill-ack-timeout-seconds",
        "120",
    ]
    request(config, "prefill", "start", output, [{"name": "router", "argv": router}])
    deadline = time.monotonic() + 30
    while True:
        try:
            with socket.create_connection((prefill["host"], config["router_port"]), timeout=2):
                break
        except OSError:
            if time.monotonic() >= deadline:
                raise TimeoutError("Router did not bind its configured port") from None
            time.sleep(0.5)
    for role in config["nodes"]:
        request(
            config,
            role,
            "start",
            output,
            [{"name": "collector", "argv": collector_command(config, role, output)}],
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("action", choices=("start", "stop", "clear-markers"))
    args = parser.parse_args()
    config = read_json(args.config)
    output = Path(os.environ["TG_RUN_DIR"])
    if args.action == "start":
        start(config, output)
    elif args.action == "clear-markers":
        request(
            config,
            "prefill",
            "clear-markers",
            output,
            request_ids=[case["request_id"] for case in read_json(os.environ["TG_CASES"])],
        )
    else:
        errors = []
        for role in ("prefill", "decode"):
            try:
                request(config, role, "stop", output)
            except Exception as error:
                errors.append(f"{role}: {error}")
        if errors:
            raise RuntimeError("; ".join(errors))
