"""D ownership adapter: three fresh services, one immutable serving phase."""

from __future__ import annotations

import argparse
import contextlib
import json
from pathlib import Path
import signal
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from common import AGENT_PORT, D_IP, ROOT, assert_port_free, dump, file_sha
from confirmation_control import ARMS, SERVING_PHASE, Sequence
from confirmation_state import allocation_identity, require_same
from legacy_node import Workers


PORTS = (18300, 18301, 19300, 19301, 50300, 50301)


def source_identity():
    manifest = json.loads((ROOT / "confirmation-sources.json").read_text(encoding="utf-8"))
    actual = {name: file_sha(ROOT / name) for name in manifest}
    require_same(manifest, actual, "shared executed sources")
    return actual


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    initial = {"allocation": allocation_identity(), "sources": source_identity()}
    dump(args.output / "identity-initial.json", initial)
    assert_port_free(D_IP, (AGENT_PORT, *PORTS))
    workers = Workers("D", D_IP, args.output)
    sequence, done, lock = Sequence(), threading.Event(), threading.Lock()
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))

    def identity(label=None):
        current = {"allocation": allocation_identity(), "sources": source_identity()}
        require_same(initial, current, "D allocation/control sources")
        if label:
            dump(args.output / (label + ".json"), current)
        return current

    class Handler(BaseHTTPRequestHandler):
        def send(self, code, value):
            self.send_response(code)
            self.end_headers()
            with contextlib.suppress(BrokenPipeError):
                self.wfile.write(json.dumps(value).encode())

        def do_GET(self):
            try:
                if self.path == "/identity":
                    value = identity()
                elif self.path in ("/", "/status"):
                    value = {"ready": True, "cursor": sequence.cursor,
                             "phase": SERVING_PHASE if sequence.active else None,
                             "arm": sequence.active, "root": str(ROOT),
                             "confirmation_sources": initial["sources"],
                             "children": [{"pid": child.pid, "exit_code": child.poll()}
                                          for child in workers.children]}
                else:
                    raise ValueError("Unknown confirmation read")
                self.send(200, value)
            except Exception as error:
                self.send(409, {"error": str(error)})

        def do_POST(self):
            try:
                size = int(self.headers["Content-Length"])
                if not 0 < size < 1024:
                    raise ValueError("Invalid control body")
                body = json.loads(self.rfile.read(size))
                with lock:
                    if self.path == "/start":
                        arm = sequence.next_arm(body)
                        identity(arm + "-before")
                        assert_port_free(D_IP, PORTS)
                        workers.output = args.output / arm
                        workers.start(SERVING_PHASE)
                        sequence.started(arm)
                    elif self.path == "/stop":
                        sequence.check_stop(body)
                        arm = sequence.active
                        children = tuple(workers.children)
                        workers.close()
                        if not children or any(child.poll() is None for child in children):
                            raise ValueError("Owned workers were not terminated/waited")
                        assert_port_free(D_IP, PORTS)
                        identity(arm + "-after")
                        sequence.stopped()
                    elif self.path == "/finish":
                        sequence.finish(body)
                        identity("identity-final")
                        dump(args.output / "node-complete.json", {"arms": ARMS, "finished": len(ARMS)})
                        done.set()
                    elif self.path == "/abort" and body == {}:
                        workers.close()
                        dump(args.output / "aborted.json", {"cursor": sequence.cursor,
                                                            "arm": sequence.active})
                        done.set()
                    else:
                        raise ValueError("Unknown control operation")
                self.send(200, {"ok": True})
            except Exception as error:
                self.send(409, {"error": str(error)})

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer((D_IP, AGENT_PORT), Handler)
    server.timeout = 1
    dump(args.output / "node-ready.json", {"ready": True, "root": str(ROOT),
         "address": D_IP, "port": AGENT_PORT, "arms": ARMS, "serving_phase": SERVING_PHASE})
    try:
        while not done.is_set():
            server.handle_request()
    finally:
        workers.close()
        server.server_close()


if __name__ == "__main__":
    main()
