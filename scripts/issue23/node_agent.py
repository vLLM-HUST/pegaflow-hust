"""Authenticated control of experiment-owned process groups on each node."""

import argparse
import contextlib
import hmac
import json
import os
import re
import signal
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from common import read_json, write_json
from deployment_runtime import node_environment


def stop_records(records):
    for record in reversed(records):
        pid = record["pid"]
        command = Path(f"/proc/{pid}/cmdline")
        if command.exists() and command.read_bytes():
            if (
                command.read_bytes().split(b"\0")[: len(record["argv"])]
                != [a.encode() for a in record["argv"]]
                or os.getpgid(pid) != pid
            ):
                raise RuntimeError("Owned process identity changed")
            os.killpg(pid, signal.SIGTERM)
    deadline = time.monotonic() + 25
    groups = {r["pid"] for r in records}
    while time.monotonic() < deadline:
        alive = False
        for path in Path("/proc").glob("[0-9]*/stat"):
            try:
                fields = path.read_text().rsplit(") ", 1)[1].split()
                if fields[0] != "Z" and int(fields[2]) in groups:
                    alive = True
                    break
            except (OSError, ValueError):
                pass
        if not alive:
            return
        time.sleep(0.5)
    for pid in groups:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(pid, signal.SIGKILL)


def process_job(config, role, job):
    root = Path(config["root"]).resolve()
    output = Path(job["run_dir"]).resolve()
    if root / "runs" not in output.parents:
        raise ValueError("Run directory outside the configured experiment")
    output.mkdir(parents=True, exist_ok=True)
    metadata = output / f"processes-{role}.json"
    records = read_json(metadata) if metadata.exists() else []
    action = job["action"]
    if action == "read-trace":
        names = job.get("request_ids", [])
        if len(names) != 1 or not re.fullmatch(r"[A-Za-z0-9._-]+\.jsonl", names[0]):
            raise ValueError("A node-local JSONL filename is required")
        path = output / names[0]
        text = path.read_text()
        complete_lines = text[: text.rfind("\n") + 1].splitlines()
        return {
            "status": "read",
            "events": [json.loads(line) for line in complete_lines if line.strip()],
        }
    if action in ("stop", "stop-model"):
        stop_records([r for r in records if action == "stop" or r["name"] == "model"])
        return {"status": "stopped"}
    if action == "clear-markers":
        for rid in job["request_ids"]:
            if not re.fullmatch(r"[A-Za-z0-9._-]+", rid):
                raise ValueError("Unsafe marker identity")
            for rank in (0, 1):
                (output / "visibility" / f"{rid}.tp{rank}.visible").unlink(
                    missing_ok=True
                )
        return {"status": "cleared"}
    if action != "start":
        raise ValueError("Unknown lifecycle action")
    allowed = {
        config["runtime"]["python"],
        *(
            str(Path(config["runtime"]["bin"]) / name)
            for name in ("pegaflow-server", "pegaflow-router", "pegaflow-metaserver")
        ),
    }
    env = node_environment(config, job.get("environment", {}))
    for specification in job["processes"]:
        argv, name = specification["argv"], specification["name"]
        if argv[0] not in allowed or not re.fullmatch(r"[A-Za-z0-9-]+", name):
            raise ValueError(
                "Executable or process name outside the configured runtime"
            )
        for old in records:
            if old["name"] == name:
                path = Path(f"/proc/{old['pid']}/cmdline")
                if path.exists() and path.read_bytes():
                    raise RuntimeError(f"Owned process already live: {name}")
        with (output / f"{role}-{name}.log").open("ab", buffering=0) as log:
            child = subprocess.Popen(
                argv,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        records.append(
            {
                "pid": child.pid,
                "name": name,
                "argv": argv,
                "started_unix_ns": time.time_ns(),
                "environment": job.get("environment", {}),
            }
        )
        write_json(metadata, records)
    return {"status": "started", "pids": [r["pid"] for r in records]}


def serve(config, role):
    token = Path(config["control_token_path"]).read_text().strip()
    if len(token) < 32:
        raise ValueError(
            "A private control token of at least 32 characters is required"
        )
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            if self.path != "/control" or not hmac.compare_digest(
                self.headers.get("Authorization", ""), "Bearer " + token
            ):
                self.send_error(403)
                return
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 1_000_000:
                self.send_error(400)
                return
            with lock:
                try:
                    result = process_job(
                        config, role, json.loads(self.rfile.read(length))
                    )
                except Exception as error:  # noqa: BLE001 -- return structured control errors
                    result = {
                        "status": "failed",
                        "error": f"{type(error).__name__}: {error}",
                    }
            body = json.dumps(result).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    node = config["nodes"][role]
    ThreadingHTTPServer((node["host"], node["control_port"]), Handler).serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--role", choices=("prefill", "decode"), required=True)
    args = parser.parse_args()
    serve(read_json(args.config), args.role)
