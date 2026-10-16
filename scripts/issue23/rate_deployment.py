"""Node-local lifecycle for four independent TP1 cache replay instances."""

import argparse
import json
import os
import signal
import subprocess
import time
import urllib.request
from pathlib import Path


def command(config, instance):
    root = Path(config["root"])
    device = instance["device"]
    env = dict(os.environ)
    env.update(
        {
            "PYTHONHOME": "/usr/local/python3.12.13",
            "PYTHONPATH": f"{root}/source/python:{root}/source/scripts/issue23:{config['site_packages']}:"
            + env.get("PYTHONPATH", ""),
            "LD_LIBRARY_PATH": "/usr/local/python3.12.13/lib:/usr/local/Ascend/ascend-toolkit/latest/lib64:"
            + env.get("LD_LIBRARY_PATH", ""),
            "ASCEND_RT_VISIBLE_DEVICES": str(device),
            "HF_HUB_OFFLINE": "1",
            "TOKENIZERS_PARALLELISM": "false",
            "OMP_NUM_THREADS": "2",
            "TOKIO_WORKER_THREADS": "2",
            "VLLM_SERVER_DEV_MODE": "1",
            "PYTHONHASHSEED": "17",
            "PEGAFLOW_TELEMETRY_DIR": str(root / "services/telemetry" / instance["id"]),
            "PEGAFLOW_TELEMETRY_ROLE": instance["id"],
            "PEGAFLOW_ISSUE23_DEFAULT_SCHEDULER": "1",
            "PEGAFLOW_SYNC_SAVE_ON_FINISH": "1",
            "PEGAFLOW_SYNC_SAVE": "1",
            "PEGAFLOW_SAVE_ACK_DIR": str(root / "services/visibility" / instance["id"]),
            "HCCL_IF_IP": instance["host"],
            "GLOO_SOCKET_IFNAME": "bond1",
            "RUST_LOG": "info",
        }
    )
    engine = [
        f"{config['bin']}/pegaflow-server",
        "--addr",
        f"{instance['host']}:{instance['engine_port']}",
        "--http-addr",
        f"0.0.0.0:{instance['engine_http_port']}",
        "--devices",
        str(device),
        "--pool-size",
        str(instance.get("host_pool_bytes", config.get("host_pool_bytes", 8 * 2**30)))
        + "b",
        "--disable-numa-affinity",
        "--enable-prometheus",
        "--nics",
        "mlx5_2",
        "--qps-per-peer",
        "2",
        "--metaserver-addr",
        f"http://{config['meta_host']}:{config['meta_port']}",
    ]
    transfer = {
        "kv_connector": "PegaKVConnector",
        "kv_connector_module_path": "pegaflow.connector",
        "kv_role": "kv_both",
        "kv_connector_extra_config": {
            "pegaflow.model_revision": config["model_revision"],
            "pegaflow.transfer_backend": "ascend_direct",
            "pegaflow.mode": "save_only" if instance["role"] == "P" else "read_write",
            "pegaflow.host": "http://" + instance["host"],
            "pegaflow.port": instance["engine_port"],
            "pegaflow.wait_for_full_prefix": instance["role"] == "D",
        },
    }
    model = [
        config["python"],
        "-m",
        "vllm.entrypoints.cli.main",
        "serve",
        config["model_path"],
        "--host",
        "0.0.0.0",
        "--port",
        str(instance["model_port"]),
        "--served-model-name",
        "qwen25",
        "--tensor-parallel-size",
        "1",
        "--pipeline-parallel-size",
        "1",
        "--dtype",
        "bfloat16",
        "--max-model-len",
        "32768",
        "--max-num-seqs",
        str(config.get("max_num_seqs", 8)),
        "--max-num-batched-tokens",
        "4096",
        "--kv-cache-memory-bytes",
        str(config.get("kv_cache_bytes", 16 * 2**30)),
        "--enable-prefix-caching",
        "--prefix-caching-hash-algo",
        "sha256",
        "--seed",
        "17",
        "--async-scheduling",
        "--compilation-config",
        json.dumps(
            {
                "mode": 3,
                "cudagraph_mode": "FULL_DECODE_ONLY",
                "cudagraph_capture_sizes": [1, 2, 4, 8],
            }
        ),
        "--generation-config",
        "vllm",
        "--worker-cls",
        "telemetry_worker.TelemetryNPUWorker",
        "--kv-transfer-config",
        json.dumps(transfer),
    ]
    return env, engine, model


def spawn(root, name, argv, env):
    folder = root / "services"
    folder.mkdir(parents=True, exist_ok=True)
    with (folder / f"{name}.log").open("ab") as log:
        process = subprocess.Popen(
            argv,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    record = {
        "name": name,
        "pid": process.pid,
        "argv": argv,
        "started_ns": time.time_ns(),
    }
    (folder / f"{name}.process.json").write_text(json.dumps(record, indent=2))
    print(json.dumps(record), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--node", required=True)
    parser.add_argument("--instance")
    parser.add_argument(
        "action", choices=["meta", "engines", "models", "resources", "stop"]
    )
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    root = Path(config["root"])
    if args.action == "stop":
        for item in (root / "services").glob("*.process.json"):
            rec = json.loads(item.read_text())
            if not rec["name"].startswith(args.node + "-"):
                continue
            if args.instance and f"-{args.instance}-" not in rec["name"]:
                continue
            pid = rec["pid"]
            try:
                if os.getpgid(pid) == pid:
                    os.killpg(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        return
    if args.action == "meta":
        env = dict(os.environ)
        env["LD_LIBRARY_PATH"] = "/usr/local/python3.12.13/lib:" + env.get(
            "LD_LIBRARY_PATH", ""
        )
        spawn(
            root,
            args.node + "-meta",
            [
                f"{config['bin']}/pegaflow-metaserver",
                "--addr",
                f"{config['meta_host']}:{config['meta_port']}",
                "--http-addr",
                f"0.0.0.0:{config['meta_http_port']}",
            ],
            env,
        )
        return
    if args.action == "resources":
        instance = next(i for i in config["instances"] if i["node"] == args.node)
        env, _, _ = command(config, instance)
        env.pop("ASCEND_RT_VISIBLE_DEVICES", None)
        spawn(
            root,
            args.node + "-resources",
            [
                config["python"],
                str(root / "source/scripts/issue23/rate_resources.py"),
                "--config",
                str(args.config),
                "--node",
                args.node,
            ],
            env,
        )
        return
    for instance in config["instances"]:
        if instance["node"] != args.node:
            continue
        if args.instance and instance["id"] != args.instance:
            continue
        env, engine, model = command(config, instance)
        if args.action == "engines":
            env.pop("ASCEND_RT_VISIBLE_DEVICES", None)
        elif args.action == "models":
            deadline = time.monotonic() + 90
            url = f"http://{instance['host']}:{instance['engine_http_port']}/health"
            while True:
                try:
                    with urllib.request.urlopen(url, timeout=2) as response:
                        if response.status == 200:
                            break
                except OSError:
                    pass
                if time.monotonic() > deadline:
                    raise TimeoutError(f"Engine not ready: {instance['id']}")
                time.sleep(1)
        spawn(
            root,
            args.node + "-" + instance["id"] + "-" + args.action,
            engine if args.action == "engines" else model,
            env,
        )


if __name__ == "__main__":
    main()
