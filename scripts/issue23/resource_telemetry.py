"""Independent host, NPU and engine sampling for a frozen four-arm run."""

import argparse
import json
import os
import socket
import subprocess
import time
import urllib.request
from pathlib import Path


def read(path):
    try:
        return Path(path).read_text().strip()
    except OSError:
        return None


def collect(output, role, run_dir, endpoints, interface, nic, interval=1):
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8", buffering=1) as stream:
        index = 0
        while True:
            started = time.monotonic()
            row = {
                "schema_version": 1,
                "sample": index,
                "node": socket.gethostname(),
                "role": role,
                "pid": os.getpid(),
                "run_dir": str(run_dir),
                "unix_ns": time.time_ns(),
                "monotonic_ns": time.monotonic_ns(),
                "cgroup": {},
                "network": {},
                "rdma": {},
                "processes": {},
                "metrics": {},
                "load_average": os.getloadavg(),
                "phase": read(run_dir / "phase.json"),
            }
            for name in (
                "cpu.stat",
                "cpu.max",
                "cpu.pressure",
                "cpuset.cpus.effective",
                "memory.current",
                "memory.stat",
                "memory.events",
                "memory.pressure",
                "io.pressure",
            ):
                row["cgroup"][name] = read(Path("/sys/fs/cgroup") / name)
            for name in (
                "rx_bytes",
                "tx_bytes",
                "rx_packets",
                "tx_packets",
                "rx_dropped",
                "tx_dropped",
            ):
                row["network"][name] = read(f"/sys/class/net/{interface}/statistics/{name}")
            for path in Path(f"/sys/class/infiniband/{nic}/ports/1/counters").glob("*"):
                row["rdma"][path.name] = read(path)
            metadata = run_dir / f"processes-{role}.json"
            for process in json.loads(metadata.read_text()) if metadata.exists() else []:
                pid = process["pid"]
                row["processes"][process["name"]] = {
                    "pid": pid,
                    "stat": read(f"/proc/{pid}/stat"),
                    "status": read(f"/proc/{pid}/status"),
                    "statm": read(f"/proc/{pid}/statm"),
                    "io": read(f"/proc/{pid}/io"),
                }
            for name, url in endpoints.items():
                try:
                    with urllib.request.urlopen(
                        url.rstrip("/") + "/metrics", timeout=2
                    ) as response:
                        row["metrics"][name] = response.read().decode()
                except Exception as error:
                    row["metrics"][name] = {"error": type(error).__name__}
            if index % 5 == 0:
                try:
                    result = subprocess.run(
                        ["npu-smi", "info"], capture_output=True, text=True, timeout=10
                    )
                    row["npu_info"] = {"exit_code": result.returncode, "text": result.stdout}
                except (OSError, subprocess.TimeoutExpired) as error:
                    row["npu_info"] = {"error": type(error).__name__}
            row["collector_elapsed_ms"] = (time.monotonic() - started) * 1000
            stream.write(json.dumps(row, separators=(",", ":")) + "\n")
            index += 1
            time.sleep(max(0, interval - (time.monotonic() - started)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--role", required=True)
    parser.add_argument("--metrics", action="append", default=[], help="name=base_url")
    parser.add_argument("--interface", default="bond1")
    parser.add_argument("--nic", default="mlx5_2")
    args = parser.parse_args()
    collect(
        args.output,
        args.role,
        args.run_dir,
        dict(value.split("=", 1) for value in args.metrics),
        args.interface,
        args.nic,
    )
