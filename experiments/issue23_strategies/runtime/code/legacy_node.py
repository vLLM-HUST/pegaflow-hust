"""Manage only the two owned D workers; never accepts arbitrary shell commands."""

from __future__ import annotations

import argparse
import contextlib
import json
import signal
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from common import AGENT_PORT, D_IP, PHASES, PYTHON, ROOT, assert_port_free, dump, stop


class Workers:
    def __init__(self, role, ip, output):
        self.role, self.ip, self.output = role, ip, output
        self.children, self.logs = [], []
        self.phase = None

    def start(self, phase):
        if self.children or phase not in PHASES:
            raise ValueError("Invalid phase or owned workers already active")
        self.phase = phase
        directory = self.output / phase
        directory.mkdir(parents=True, exist_ok=False)
        for slot in (0, 1):
            log = (directory / f"{self.role}{slot}-console.log").open("x", encoding="utf-8")
            self.logs.append(log)
            self.children.append(subprocess.Popen([
                str(PYTHON), str(ROOT / "code/worker.py"), "--role", self.role,
                "--slot", str(slot), "--phase", phase, "--local-ip", self.ip,
                "--output", str(directory / f"{self.role}{slot}")],
                stdout=log, stderr=subprocess.STDOUT))
        dump(directory / "processes.json", [{"pid": p.pid, "command": p.args} for p in self.children])

    def close(self):
        stop(self.children)
        self.children = []
        for log in self.logs:
            log.close()
        self.logs = []


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    assert_port_free(D_IP, (AGENT_PORT,))
    workers = Workers("D", D_IP, args.output)
    done, lock, cursor = threading.Event(), threading.Lock(), 0
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            value = {"ready": True, "phase": workers.phase, "cursor": cursor,
                     "children": [{"pid": p.pid, "exit_code": p.poll()} for p in workers.children]}
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps(value).encode())

        def do_POST(self):
            nonlocal cursor
            try:
                size = int(self.headers["Content-Length"])
                if not 0 < size < 1024:
                    raise ValueError("Invalid control body")
                body = json.loads(self.rfile.read(size))
                with lock:
                    if self.path == "/start":
                        if cursor >= len(PHASES) or body != {"phase": PHASES[cursor]}:
                            raise ValueError("Phase order/identity differs")
                        workers.start(PHASES[cursor])
                    elif self.path == "/stop":
                        if body != {"phase": workers.phase} or not workers.children:
                            raise ValueError("No matching owned worker phase")
                        workers.close()
                        cursor += 1
                    elif self.path == "/finish":
                        if cursor != len(PHASES) or workers.children:
                            raise ValueError("Cannot finish an incomplete pair")
                        dump(args.output / "node-complete.json", {"phases_finished": cursor})
                        done.set()
                    elif self.path == "/abort":
                        workers.close()
                        dump(args.output / "aborted.json", {"by_owned_P_controller": True})
                        done.set()
                    else:
                        raise ValueError("Unknown control operation")
                result, code = {"ok": True}, 200
            except Exception as error:
                result, code = {"error": str(error)}, 409
            self.send_response(code)
            self.end_headers()
            with contextlib.suppress(BrokenPipeError):
                self.wfile.write(json.dumps(result).encode())

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer((D_IP, AGENT_PORT), Handler)
    server.timeout = 1
    dump(args.output / "node-ready.json", {"ready": True, "address": D_IP, "port": AGENT_PORT})
    try:
        while not done.is_set():
            server.handle_request()
    finally:
        workers.close()
        server.server_close()


if __name__ == "__main__":
    main()
