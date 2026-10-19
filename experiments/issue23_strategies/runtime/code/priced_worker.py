"""TP1 worker with actual-engine hash preview and priced restore boundaries."""

import json
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import argparse
import signal
import sys
import os

from common import cache_url, dump
from legacy_worker import Worker as ServingWorker
from pegaflow.joint_runtime import json_rpc


class Worker(ServingWorker):
    host_pool = "2147483648"

    def parameters(self, body, count):
        from vllm import SamplingParams

        extra = {}
        if not body.get("cold"):
            limit = body.get("max_prefix_blocks")
            if type(limit) is not int or limit < 0:
                raise ValueError("Every formal request requires an explicit recovery boundary")
            extra = {"kv_transfer_params": {"pegaflow_joint": {
                "max_prefix_blocks": limit, "local_only": True}}}
        return SamplingParams(temperature=0, max_tokens=count, ignore_eos=True, extra_args=extra)

    def initialize(self):
        os.environ["PEGAFLOW_JOINT_HOST_AUTHORITY"] = "1"
        super().initialize()
        from vllm.utils.hashing import get_hash_fn_by_name
        from vllm.v1.core import kv_cache_utils

        self.hash_fn = get_hash_fn_by_name(self.llm.llm_engine.vllm_config.cache_config.prefix_caching_hash_algo)
        kv_cache_utils.init_none_hash(self.hash_fn)
        base = cache_url(self.args.role, self.args.slot)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            values = json_rpc(base + "/instances")
            candidates = values if isinstance(values, list) else values.get("instances", [])
            ids = [v if isinstance(v, str) else v.get("instance_id") for v in candidates]
            ids = [v for v in ids if v and v.startswith(f"issue23-2p2d-20261014-{self.args.phase}-{self.args.role}{self.args.slot}")]
            if len(ids) == 1:
                self.instance = ids[0]
                break
            time.sleep(0.1)
        if self.instance is None:
            raise RuntimeError("Missing unambiguous actual registered TP1 instance")
        dump(self.args.output / "prefix-authority.json", {
            "host_inventory_authority": True, "opaque_GPU_local_hits_disabled_in_both_arms": True,
            "explicit_boundaries_and_local_only": True})

    def preview(self, row):
        from vllm.v1.core.kv_cache_utils import NONE_HASH, hash_block_tokens

        tokens, parent, hashes = row["prompt_token_ids"], NONE_HASH, []
        if len(tokens) != row["prompt_tokens"]:
            raise ValueError("Input token ledger changed")
        for start in range(0, ((len(tokens) - 1) // 128) * 128, 128):
            parent = hash_block_tokens(self.hash_fn, parent, tokens[start:start + 128], None)
            hashes.append(bytes(parent).hex())
        return {"hashes": hashes, "instance_id": self.instance, "block_tokens": 128}

    def generate(self, body, emit):
        result = super().generate(body, emit)
        if not body.get("cold"):
            if result["num_cached_tokens"] > body["max_prefix_blocks"] * 128:
                raise ValueError("Actual restore exceeded the selected priced boundary")
            predicted = self.preview(body["row"])["hashes"]
            actual = result["observation"]["full_prompt_hashes"][:len(predicted)]
            if predicted != actual:
                raise ValueError("Preview hashes differ from actual registered engine observation")
        result["explicit_restore_limit"] = body.get("max_prefix_blocks")
        return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", choices=("P", "D"), required=True)
    parser.add_argument("--slot", type=int, choices=(0, 1), required=True)
    parser.add_argument("--phase", choices=("calibration", "baseline", "treatment"), required=True)
    parser.add_argument("--local-ip", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    worker = Worker(args)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps({"ready": True, "role": args.role, "slot": args.slot,
                 "phase": args.phase, "instance_id": worker.instance}).encode())

        def do_POST(self):
            streaming = False
            try:
                size = int(self.headers["Content-Length"])
                if not 0 < size < 4 * 1024**2:
                    raise ValueError("Invalid request body size")
                body = json.loads(self.rfile.read(size))
                if self.path == "/preview":
                    payload = worker.preview(body["row"])
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(json.dumps(payload).encode())
                    return
                if self.path != "/generate":
                    raise ValueError("Unsupported operation")
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
            except Exception as error:
                payload = {"event": "error", "type": type(error).__name__, "error": str(error)}
                dump(args.output / f"failure-{time.time_ns()}.json", payload)
                if not streaming:
                    self.send_response(500)
                    self.end_headers()
                self.wfile.write((json.dumps(payload) + "\n").encode())

        def log_message(self, *_):
            pass

    server = None
    try:
        worker.initialize()
        server = ThreadingHTTPServer((args.local_ip, 18300 + args.slot), Handler)
        dump(args.output / "readiness.json", {"ready": True, "instance_id": worker.instance,
             "role": args.role, "slot": args.slot, "phase": args.phase, "tp": 1})
        server.serve_forever(poll_interval=0.2)
    finally:
        if server is not None:
            server.server_close()
        worker.close()


if __name__ == "__main__":
    main()
