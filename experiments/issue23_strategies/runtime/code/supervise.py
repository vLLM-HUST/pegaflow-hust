"""Bound an owned process group and preserve logs without restarting containers."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import signal
import subprocess
import time
from pathlib import Path


def interrupt(_signum, _frame) -> None:
    raise KeyboardInterrupt


def signal_owned_group(process: subprocess.Popen, signum: int) -> None:
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signum)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=int, required=True)
    parser.add_argument("--memory-stop-gib", type=float, default=58.0)
    parser.add_argument("--job-kind", default="owned_runtime_preparation_not_formal_sample")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command or args.timeout <= 0:
        raise SystemExit("A bounded command is required")
    args.output.mkdir(parents=True, exist_ok=False)
    memory_path = Path("/sys/fs/cgroup/memory.current")
    before = int(memory_path.read_text()) if memory_path.exists() else None
    limit = int(args.memory_stop_gib * 1024**3)
    if before is not None and before >= limit:
        raise SystemExit("Container memory is already above the safety stop")
    started = time.monotonic()
    peak = before or 0
    stop_reason = None
    for signum in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, interrupt)
    with (args.output / "console.log").open("x") as log:
        process = subprocess.Popen(
            command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True
        )
        (args.output / "process.json").write_text(
            json.dumps(
                {
                    "pid": process.pid,
                    "process_group": process.pid,
                    "command": command,
                    "started_unix": time.time(),
                    "timeout_seconds": args.timeout,
                    "memory_stop_bytes": limit,
                    "kind": args.job_kind,
                },
                indent=2,
            )
            + "\n"
        )
        try:
            while process.poll() is None:
                current = int(memory_path.read_text()) if memory_path.exists() else 0
                peak = max(peak, current)
                if current >= limit:
                    stop_reason = "container_memory_safety_stop"
                    break
                if time.monotonic() - started >= args.timeout:
                    stop_reason = "timeout"
                    break
                time.sleep(0.5)
        except (KeyboardInterrupt, SystemExit):
            stop_reason = "supervisor_interrupted"
        finally:
            if process.poll() is None:
                signal_owned_group(process, signal.SIGTERM)
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    signal_owned_group(process, signal.SIGKILL)
                    process.wait(timeout=15)
            signal_owned_group(process, signal.SIGTERM)
    result = {
        "kind": args.job_kind,
        "exit_code": process.returncode,
        "stop_reason": stop_reason,
        "seconds": time.monotonic() - started,
        "container_memory_peak_bytes": peak,
        "formal_valid_sample": False,
    }
    (args.output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result), flush=True)
    raise SystemExit(0 if process.returncode == 0 and stop_reason is None else 1)


if __name__ == "__main__":
    main()
