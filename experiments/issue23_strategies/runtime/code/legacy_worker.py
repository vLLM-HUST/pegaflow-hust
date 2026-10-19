"""One TP1 NPU: prefill plus one token on P, continuation generation on D."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import importlib.util
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
import urllib.request
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from common import (META_GRPC, META_HTTP, MODEL, P_IP, assert_port_free, cache_url,
                    dump, file_sha, native_device_scope, rpc, service_ttft, stop, wait)


class Worker:
    host_pool = "4gb"

    def parameters(self, body, count):
        from vllm import SamplingParams

        return SamplingParams(temperature=0, max_tokens=count, ignore_eos=True)

    def __init__(self, args):
        self.args, self.children, self.logs = args, [], []
        self.llm, self.instance = None, None
        self.observations = args.output / "observations"
        self.lock = threading.Lock()
        self.count = 0

    def initialize(self):
        args = self.args
        args.output.mkdir(parents=True, exist_ok=False)
        os.environ["ASCEND_RT_VISIBLE_DEVICES"] = str(args.slot)
        os.environ["PEGAFLOW_ENABLE_PLACEMENT_API"] = "1"
        os.environ["PEGAFLOW_SYNC_SAVE"] = "1"
        os.environ["PEGAFLOW_SYNC_SAVE_ON_FINISH"] = "1"
        os.environ["PEGAFLOW_PLACEMENT_OBSERVATIONS_DIR"] = str(self.observations)
        for key in ("PEGA_DIAG_KV_CHECKSUM", "PEGA_DIAG_HYBRID_SEMANTICS", "PEGAFLOW_SAVE_ACK_DIR"):
            os.environ.pop(key, None)
        import torch
        import torch_npu  # noqa: F401

        if torch.npu.device_count() != 1:
            raise RuntimeError("Each TP1 instance must see exactly its one allocated NPU")
        free, total = torch.npu.mem_get_info(0)
        dump(args.output / "capacity.json", {"container_logical": args.slot,
             "process_logical": 0, "free_bytes": free, "total_bytes": total,
             "gpu_memory_utilization": 0.35, "kv_cache_memory_bytes": 2 * 1024**3})
        if free < 0.35 * total:
            raise RuntimeError("Insufficient actual free memory for the explicit TP1 budget")
        assert_port_free(args.local_ip, (50300 + args.slot, 19300 + args.slot, 18300 + args.slot))
        wait(f"http://{P_IP}:{META_HTTP}/health", timeout=300)
        from pegaflow._bin_utils import binary_env, find_binary

        environment = binary_env()
        scope = native_device_scope(args.slot)
        environment["ASCEND_RT_VISIBLE_DEVICES"] = scope["visibility"]
        environment["PEGAFLOW_SYNC_SAVE"] = "1"
        binary = Path(find_binary("pegaflow-server-py")).resolve()
        if not binary.is_relative_to(Path(sys.prefix).resolve()):
            raise RuntimeError("Native server is outside this runtime")
        log = (args.output / "native.log").open("x", encoding="utf-8")
        self.logs.append(log)
        self.children.append(subprocess.Popen([
            str(binary), "--addr", f"{args.local_ip}:{50300 + args.slot}",
            "--http-addr", f"{args.local_ip}:{19300 + args.slot}", "--devices", scope["allowed_devices"],
            "--pool-size", self.host_pool, "--disable-numa-affinity", "--nics", "mlx5_2",
            "--metaserver-addr", f"http://{P_IP}:{META_GRPC}",
            "--enable-prometheus", "--log-level", "info"],
            stdout=log, stderr=subprocess.STDOUT, env=environment))
        wait(cache_url(args.role, args.slot) + "/health", self.children, timeout=180)
        sources = {}
        for module in ("vllm", "vllm_ascend", "pegaflow"):
            origin = Path(importlib.util.find_spec(module).origin)
            sources[module] = {"path": str(origin), "sha256": file_sha(origin)}
            if module == "pegaflow":
                for path in sorted(origin.parent.rglob("*.py")):
                    sources["pegaflow/" + str(path.relative_to(origin.parent))] = file_sha(path)
                for path in sorted(origin.parent.glob("*.so")):
                    sources[path.name] = file_sha(path)
        sources["native-server"] = file_sha(binary)
        dump(args.output / "provenance.json", {"python": sys.executable,
             "role": args.role, "slot": args.slot, "phase": args.phase,
             "sources": sources, "visibility": os.environ["ASCEND_RT_VISIBLE_DEVICES"],
             "native_device_scope": scope})
        from vllm import LLM
        from vllm.config import KVTransferConfig

        config = dict(model=str(MODEL), tensor_parallel_size=1,
            distributed_executor_backend="mp", dtype="bfloat16", enforce_eager=True,
            async_scheduling=False, max_model_len=32768, max_num_seqs=1,
            max_num_batched_tokens=128, enable_chunked_prefill=True,
            gpu_memory_utilization=0.35, kv_cache_memory_bytes=2 * 1024**3,
            enable_prefix_caching=True, block_size=128, seed=20261014,
            safetensors_load_strategy="lazy", max_parallel_loading_workers=1,
            disable_log_stats=False,
            kv_transfer_config={"kv_connector": "PegaKVConnector",
                "kv_connector_module_path": "pegaflow.connector", "kv_role": "kv_both",
                "engine_id": f"issue23-2p2d-20261014-{args.phase}-{args.role}{args.slot}",
                "kv_connector_extra_config": {"pegaflow.host": f"http://{args.local_ip}",
                    "pegaflow.port": 50300 + args.slot, "pegaflow.transfer_backend": "ascend_direct",
                    "pegaflow.mode": "read_write", "pegaflow.wait_for_full_prefix": True}})
        dump(args.output / "configuration.json", config)
        kwargs = dict(config)
        kwargs["kv_transfer_config"] = KVTransferConfig(**config["kv_transfer_config"])
        started = time.monotonic()
        self.llm = LLM(**kwargs)
        actual = self.llm.llm_engine.vllm_config.cache_config.block_size
        if actual != 128:
            raise RuntimeError("Actual hash/cache block size changed")
        dump(args.output / "engine.json", {"initialization_seconds": time.monotonic() - started,
             "actual_block_tokens": actual, "tensor_parallel_size": 1})

    def metrics(self, name):
        with urllib.request.urlopen(cache_url(self.args.role, self.args.slot) + "/metrics", timeout=10) as response:
            content = response.read().decode()
        path = self.args.output / name
        with path.open("x", encoding="utf-8") as stream:
            stream.write(content)
        return int(sum(float(line.split()[-1]) for line in content.splitlines()
                       if line.startswith("pegaflow_rdma_fetch_bytes") and 'status="ok"' in line))

    def observe(self, output, row):
        from pegaflow.placement_policy import CompletedDemand

        frontend = str(output.request_id)
        deadline = time.monotonic() + 120
        while True:
            matches = []
            for path in self.observations.glob("*.json"):
                value = json.loads(path.read_text(encoding="utf-8"))
                actual = value.get("request_id", "")
                if actual == frontend or re.fullmatch(re.escape(frontend) + r"-[0-9a-f]{8}", actual):
                    expected_name = hashlib.sha256((value["instance_id"] + ":" + actual).encode()).hexdigest() + ".json"
                    if expected_name != path.name:
                        raise ValueError("Observation identity/hash mismatch")
                    matches.append(value)
            if len(matches) == 1:
                value = matches[0]
                if not value["instance_id"].startswith(f"issue23-2p2d-20261014-{self.args.phase}-{self.args.role}{self.args.slot}"):
                    raise ValueError("Observation is outside this actual worker")
                receipt = {"id": row["id"], "success": True, "instance_id": value["instance_id"],
                           "request_id": value["request_id"], "prompt_tokens": row["prompt_tokens"]}
                CompletedDemand.join(value, receipt, local_decision_ns=time.monotonic_ns())
                self.instance = value["instance_id"]
                return value
            if len(matches) > 1 or time.monotonic() >= deadline:
                raise RuntimeError("Missing or ambiguous successful completed request observation")
            time.sleep(0.05)

    def generate(self, body, emit):
        with self.lock:
            row = body["row"]
            tokens = row["prompt_token_ids"]
            if len(tokens) != row["prompt_tokens"] or not 1 <= len(tokens) <= 32768 - 128:
                raise ValueError("Frozen input/context contract failed")
            from vllm import SamplingParams

            cold = body.get("cold") is True
            if cold and self.args.role != "D":
                raise ValueError("Integrated cold reference is a D-only warmup")
            count = 128 if self.args.role == "D" else 1
            before = self.metrics(f"metrics-{self.count:04d}-before.txt")
            started = time.monotonic()
            prompt = {"prompt_token_ids": tokens}
            if cold:
                prompt["cache_salt"] = f"cold-{self.args.phase}-{self.args.slot}-{self.count}"
            engine = self.llm.llm_engine
            engine.add_request(f"r{self.count}", prompt,
                               self.parameters(body, count))
            output, first = None, False
            while engine.has_unfinished_requests():
                for update in engine.step():
                    if update.outputs and update.outputs[0].token_ids and not first:
                        emit({"event": "first_token", "token_id": update.outputs[0].token_ids[0]})
                        first = True
                    if update.finished:
                        output = update
            if output is None or not first:
                raise RuntimeError("No completed streamed generation")
            elapsed = time.monotonic() - started
            output_ids = list(output.outputs[0].token_ids)
            metrics = asdict(output.metrics) if output.metrics is not None else None
            if len(output_ids) != count or metrics is None or metrics.get("is_corrupted", False):
                raise RuntimeError("Output length/metrics/corruption contract failed")
            observation = self.observe(output, row)
            after = self.metrics(f"metrics-{self.count:04d}-after.txt")
            cached = int(output.num_cached_tokens or 0)
            if cold and (cached != 0 or after != before):
                raise RuntimeError("Salted cold reference unexpectedly consumed cached state")
            ttft = service_ttft(metrics)
            result = {"id": row["id"], "task": row["task"], "step_id": row["step_id"],
                "role": self.args.role, "slot": self.args.slot, "phase": self.args.phase,
                "prompt_tokens": len(tokens), "output_token_ids": output_ids,
                "output_sha256": hashlib.sha256(json.dumps(output_ids).encode()).hexdigest(),
                "num_cached_tokens": cached, "uncached_prompt_tokens": len(tokens) - cached,
                "rdma_bytes": after - before, "rdma_bytes_cumulative": after,
                "wall_seconds": elapsed, "ttft_seconds": ttft, "raw_metrics": metrics,
                "observation": observation, "local_completed_demand_join_passed": True,
                "cold_reference": cold}
            inventory = rpc(cache_url(self.args.role, self.args.slot) +
                            "/cache/placement/inventory?instance_id=" + self.instance, timeout=10)
            dump(self.args.output / f"inventory-{self.count:04d}.json", inventory)
            dump(self.args.output / f"request-{self.count:04d}.json", result)
            self.count += 1
            return result

    def close(self):
        try:
            if self.llm is not None:
                self.llm.llm_engine.engine_core.shutdown()
        finally:
            stop(self.children)
            for log in self.logs:
                log.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", choices=("P", "D"), required=True)
    parser.add_argument("--slot", type=int, choices=(0, 1), required=True)
    parser.add_argument("--phase", choices=("calibration", "baseline", "treatment"), required=True)
    parser.add_argument("--local-ip", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    worker = Worker(args)
    server = None
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"ready": True, "role": args.role,
                 "slot": args.slot, "phase": args.phase, "instance_id": worker.instance}).encode())

        def do_POST(self):
            streaming = False
            try:
                if self.path != "/generate":
                    raise ValueError("Unknown worker operation")
                size = int(self.headers["Content-Length"])
                if not 0 < size < 4 * 1024**2:
                    raise ValueError("Invalid request size")
                body = json.loads(self.rfile.read(size))
                self.send_response(200)
                self.send_header("Content-Type", "application/x-ndjson")
                self.send_header("Connection", "close")
                self.end_headers()
                self.close_connection = True
                streaming = True

                def emit(value):
                    self.wfile.write((json.dumps(value) + "\n").encode())
                    self.wfile.flush()

                result = worker.generate(body, emit)
                emit({"event": "complete", "result": result})
                return
            except Exception as error:
                payload, code = {"error": str(error), "type": type(error).__name__}, 500
                failure = args.output / f"failure-{time.time_ns()}.json"
                dump(failure, payload)
            if streaming:
                with contextlib.suppress(BrokenPipeError):
                    self.wfile.write((json.dumps({"event": "error", **payload}) + "\n").encode())
                    self.wfile.flush()
                return
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            with contextlib.suppress(BrokenPipeError):
                self.wfile.write(json.dumps(payload).encode())

        def log_message(self, *_):
            pass

    try:
        worker.initialize()
        server = ThreadingHTTPServer((args.local_ip, 18300 + args.slot), Handler)
        dump(args.output / "readiness.json", {"ready": True, "role": args.role, "slot": args.slot,
             "phase": args.phase, "model": str(MODEL), "tp": 1})
        server.serve_forever(poll_interval=0.2)
    except Exception as error:
        args.output.mkdir(parents=True, exist_ok=True)
        dump(args.output / "failure.json", {"error": str(error), "type": type(error).__name__})
        raise
    finally:
        if server is not None:
            server.server_close()
        worker.close()


if __name__ == "__main__":
    main()
