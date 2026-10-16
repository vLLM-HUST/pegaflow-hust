"""Low-cost per-node resources and model queue sampling for rate replay."""

import argparse
import concurrent.futures
import json
import subprocess
import time
import urllib.request
from pathlib import Path


def read(path):
    try:
        return Path(path).read_text().strip()
    except OSError:
        return None


def fetch(url):
    try:
        with urllib.request.urlopen(url, timeout=1) as response:
            text = response.read().decode()
            selected = []
            for line in text.splitlines():
                if line.startswith("#") or "_bucket{" in line:
                    continue
                if line.startswith(
                    (
                        "vllm:num_requests_",
                        "vllm:kv_cache_usage",
                        "vllm:gpu_cache_usage",
                        "vllm:prompt_tokens_total",
                        "vllm:generation_tokens_total",
                        "vllm:num_preemptions",
                        "vllm:prefix_cache_",
                        "pegaflow_",
                    )
                ):
                    selected.append(line)
            return "\n".join(selected)
    except (OSError, TimeoutError) as error:
        return {"error": str(error)}


def collect(config, node):
    root = Path(config["root"])
    instances = [i for i in config["instances"] if i["node"] == node]
    endpoints = {}
    for instance in instances:
        for kind in ["model", "engine"]:
            port = instance["model_port" if kind == "model" else "engine_http_port"]
            endpoints[instance["id"] + "-" + kind] = (
                f"http://{instance['host']}:{port}/metrics"
            )
    with (
        concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool,
        (root / "services" / f"resources-{node}.jsonl").open(
            "a", buffering=65536
        ) as stream,
    ):
        index = 0
        while True:
            start = time.monotonic()
            row = {
                "unix_ns": time.time_ns(),
                "monotonic_ns": time.monotonic_ns(),
                "node": node,
                "phase": read(root / "phase.json"),
                "cgroup": {},
                "processes": {},
                "rdma": {},
                "network": {},
                "metrics": {},
            }
            for key in [
                "cpu.stat",
                "cpu.max",
                "cpu.pressure",
                "cpuset.cpus.effective",
                "memory.current",
                "memory.events",
                "memory.pressure",
            ]:
                row["cgroup"][key] = read(Path("/sys/fs/cgroup") / key)
            for pid in (read("/sys/fs/cgroup/cgroup.procs") or "").splitlines():
                cmd = read(f"/proc/{pid}/cmdline") or ""
                if str(root) in cmd or "EngineCore" in cmd or "resource_tracker" in cmd:
                    row["processes"][pid] = {
                        "cmd": cmd.replace("\x00", " "),
                        "stat": read(f"/proc/{pid}/stat"),
                        "status": read(f"/proc/{pid}/status"),
                    }
            for family in ["counters", "hw_counters"]:
                for path in Path(f"/sys/class/infiniband/mlx5_2/ports/1/{family}").glob(
                    "*"
                ):
                    row["rdma"][family + "/" + path.name] = read(path)
            for interface in ["bond1", "enp195s0f0np0"]:
                row["network"][interface] = {
                    key: read(f"/sys/class/net/{interface}/statistics/{key}")
                    for key in ["rx_bytes", "tx_bytes", "rx_dropped", "tx_dropped"]
                }
            futures = {name: pool.submit(fetch, url) for name, url in endpoints.items()}
            for name, future in futures.items():
                row["metrics"][name] = future.result()
            if index % 2 == 0:
                try:
                    smi = subprocess.run(
                        ["npu-smi", "info"],
                        capture_output=True,
                        text=True,
                        timeout=3,
                        check=False,
                    )
                    row["npu_smi"] = {"status": smi.returncode, "text": smi.stdout}
                except (OSError, subprocess.TimeoutExpired) as error:
                    row["npu_smi"] = {"error": str(error)}
            row["collector_ms"] = (time.monotonic() - start) * 1000
            stream.write(json.dumps(row) + "\n")
            stream.flush()
            index += 1
            time.sleep(max(0, 1 - (time.monotonic() - start)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--node", required=True)
    args = parser.parse_args()
    collect(json.loads(args.config.read_text()), args.node)
