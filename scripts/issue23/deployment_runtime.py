"""Build reproducible service commands for the original four-arm protocol."""

import json
import os
from pathlib import Path


def inference_settings(config):
    profile = config.get("inference_profile", "performance")
    if profile == "eager_sync":
        return {"profile": profile, "enforce_eager": True, "async_scheduling": False}
    if profile != "performance":
        raise ValueError(f"Unknown inference profile: {profile}")
    maximum = config.get("max_num_seqs", 8)
    sizes = sorted(
        {value for value in (1, 2, 4, 8, 12, 16, maximum) if value <= maximum}
    )
    return {
        "profile": profile,
        "enforce_eager": False,
        "async_scheduling": True,
        "compilation_config": {
            "mode": 3,
            "cudagraph_mode": "FULL_DECODE_ONLY",
            "cudagraph_capture_sizes": sizes,
        },
    }


def environment(config, role, output):
    node = config["nodes"][role]
    return {
        "HCCL_IF_IP": node["host"],
        "GLOO_SOCKET_IFNAME": "bond1",
        "HF_HUB_OFFLINE": "1",
        "TOKENIZERS_PARALLELISM": "false",
        "PYTHONHASHSEED": "17",
        "OMP_NUM_THREADS": "4",
        "TOKIO_WORKER_THREADS": "4",
        "VLLM_SERVER_DEV_MODE": "1",
        "PEGAFLOW_ISSUE23_DEFAULT_SCHEDULER": "1",
        "PYTHONHOME": "/usr/local/python3.12.13",
        "PEGAFLOW_TELEMETRY_DIR": str(output / "telemetry" / role),
        "PEGAFLOW_TELEMETRY_ROLE": role,
        "PEGAFLOW_SYNC_SAVE_ON_FINISH": "1",
        "PEGAFLOW_SAVE_ACK_DIR": str(output / "visibility"),
        "PEGAFLOW_ISSUE23_HYBRID_PREFETCH": "1" if role == "decode" else "0",
        "PEGAFLOW_ISSUE23_DISCOVERY_GRACE_SECONDS": str(
            config.get("discovery_grace_seconds", 5)
        ),
        "PEGAFLOW_ISSUE23_LOCAL_OBSERVERS": "1" if config.get("scaled") else "0",
        "RUST_LOG": "info",
    }


def engine_command(
    config, role, output, mode, backend="rdma", schedule="burst", plans_dir=None
):
    node = config["nodes"][role]
    runtime = config["runtime"]
    meta = config["nodes"]["decode"]
    argv = [
        str(Path(runtime["bin"]) / "pegaflow-server"),
        "--addr",
        f"{node['host']}:{node['engine_port']}",
        "--http-addr",
        f"0.0.0.0:{node['engine_http_port']}",
        "--devices",
        "0,1",
        "--pool-size",
        str(config.get("host_pool_bytes", 4 * 1024**3)) + "b",
        "--disable-numa-affinity",
        "--enable-prometheus",
        "--nics",
        "mlx5_2",
        "--qps-per-peer",
        "2",
        "--metaserver-addr",
        f"http://{meta['host']}:{meta['meta_port']}",
    ]
    if role == "decode":
        argv += ["--issue23-trace-path", str(output / "trace-decode-engine.jsonl")]
        if mode == "arm":
            argv += [
                "--issue23-plan",
                str(Path(plans_dir) / "decode-engine.json"),
                "--issue23-backend",
                backend,
                "--issue23-schedule",
                schedule,
            ]
    return argv


def model_command(config, role):
    node = config["nodes"][role]
    transfer = {
        "kv_connector": "PegaKVConnector",
        "kv_connector_module_path": "pegaflow.connector",
        "kv_role": "kv_both",
        "kv_connector_extra_config": {
            "pegaflow.model_revision": config["model_revision"],
            "pegaflow.transfer_backend": "ascend_direct",
            "pegaflow.mode": "save_only" if role == "prefill" else "read_write",
            "pegaflow.host": "http://" + node["host"],
            "pegaflow.port": node["engine_port"],
            "pegaflow.wait_for_full_prefix": role == "decode",
        },
    }
    settings = inference_settings(config)
    execution = (
        ["--enforce-eager", "--no-async-scheduling"]
        if settings["enforce_eager"]
        else [
            "--async-scheduling",
            "--compilation-config",
            json.dumps(settings["compilation_config"]),
        ]
    )
    return [
        config["runtime"]["python"],
        "-m",
        "vllm.entrypoints.cli.main",
        "serve",
        config["model_path"],
        "--host",
        "0.0.0.0",
        "--port",
        str(node["model_port"]),
        "--served-model-name",
        "hybrid-qwen35",
        "--tensor-parallel-size",
        "2",
        "--pipeline-parallel-size",
        "1",
        "--dtype",
        "bfloat16",
        "--max-model-len",
        "8192",
        "--max-num-seqs",
        str(config.get("max_num_seqs", 8)),
        "--max-num-batched-tokens",
        "4096",
        "--kv-cache-memory-bytes",
        str(4 * 1024**3),
        "--enable-prefix-caching",
        "--prefix-caching-hash-algo",
        "sha256",
        "--mamba-cache-mode",
        "align",
        "--seed",
        "17",
        *execution,
        "--limit-mm-per-prompt",
        '{"image":0,"video":0}',
        "--generation-config",
        "vllm",
        "--worker-cls",
        "telemetry_worker.TelemetryNPUWorker",
        "--kv-transfer-config",
        json.dumps(transfer),
    ]


def collector_command(config, role, output):
    node = config["nodes"][role]
    argv = [
        config["runtime"]["python"],
        str(Path(config["runtime"]["scripts"]) / "resource_telemetry.py"),
        "--output",
        str(output / f"resources-{role}.jsonl"),
        "--run-dir",
        str(output),
        "--role",
        role,
    ]
    for name, port in (
        ("model", node["model_port"]),
        ("engine", node["engine_http_port"]),
    ):
        argv += ["--metrics", f"{name}=http://{node['host']}:{port}"]
    if config.get("scaled"):
        argv += ["--smi-every", "0"]
    return argv


def node_environment(config, extra):
    env = dict(os.environ)
    env.update(extra)
    runtime = config["runtime"]
    env["PYTHONPATH"] = ":".join(
        [
            str(Path(runtime["source"]) / "python"),
            runtime["scripts"],
            runtime["site_packages"],
            env.get("PYTHONPATH", ""),
        ]
    )
    env["LD_LIBRARY_PATH"] = (
        "/usr/local/python3.12.13/lib:/usr/local/Ascend/ascend-toolkit/latest/lib64:"
        + env.get("LD_LIBRARY_PATH", "")
    )
    return env
