"""Immutable configuration and bounded control helpers for four TP1 instances."""

from __future__ import annotations

import hashlib
import json
import math
import socket
import subprocess
import time
import urllib.request
from pathlib import Path

ROOT = Path("/root/issue23-2p2d/20261014")
MODEL = Path("/data/shared_models/Qwen2.5-7B-Instruct")
PYTHON = Path("/root/issue23-tools/pegaflow-2p2d-02/.venv/bin/python")
P_IP, D_IP = "10.17.119.1", "10.17.119.71"
META_GRPC, META_HTTP, AGENT_PORT = 50260, 19260, 18390
TEST_TASKS = ("bn-fit-modify", "break-filter-js-from-html", "cancel-async-tasks",
              "constraints-scheduling", "custom-memory-heap-crash")
PHASES = ("calibration", "baseline", "treatment")


def split(task):
    value = hashlib.sha256(("issue23-natural-agent-task-split-v1:" + task).encode()).digest()
    return "calibration" if int.from_bytes(value, "big") % 5 == 0 else "test"


def file_sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while block := stream.read(4 * 1024**2):
            digest.update(block)
    return digest.hexdigest()


def dump(path, value):
    with Path(path).open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")


def rpc(url, body=None, timeout=600):
    request = urllib.request.Request(url, data=None if body is None else json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def wait(url, processes=(), timeout=1800):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if any(p.poll() is not None for p in processes):
            raise RuntimeError("Owned child exited during initialization")
        if url.startswith(f"http://{D_IP}:1830"):
            state = rpc(f"http://{D_IP}:{AGENT_PORT}/status", timeout=5)
            if any(child["exit_code"] is not None for child in state["children"]):
                raise RuntimeError("Owned D worker exited during initialization; inspect its log")
        try:
            if url.endswith("/health"):
                with urllib.request.urlopen(url, timeout=3) as response:
                    if response.status == 200:
                        return {"ready": True}
            response = rpc(url, timeout=3)
            if response.get("ready") is True or response.get("status") in ("ok", "healthy"):
                return response
            if response.get("error"):
                raise RuntimeError(response["error"])
        except OSError:
            time.sleep(1)
    raise TimeoutError(f"Readiness timed out: {url}")


def assert_port_free(ip, ports):
    for port in ports:
        with socket.socket() as active:
            active.settimeout(0.25)
            if active.connect_ex((ip, port)) == 0:
                raise RuntimeError(f"Refusing to replace active listener {ip}:{port}")
        with socket.socket() as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind((ip, port))
            probe.listen(1)


def stop(children):
    for child in reversed(children):
        if child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=30)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=15)


def worker_url(role, slot):
    return f"http://{P_IP if role == 'P' else D_IP}:{18300 + slot}"


def cache_url(role, slot):
    return f"http://{P_IP if role == 'P' else D_IP}:{19300 + slot}"


def native_device_scope(slot):
    if type(slot) is not int or slot not in (0, 1):
        raise ValueError("An allocated container logical device is required")
    # NpuIPCWrapper exports the unmasked container index, not process-local zero.
    return {"visibility": "0,1", "allowed_devices": str(slot)}


def service_ttft(metrics):
    value = metrics["first_token_latency"]
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError("Missing or invalid measured vLLM first-token latency")
    return float(value)
