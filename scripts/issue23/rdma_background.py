"""Measure an owned host-memory RC READ competitor with perftest."""

import argparse
import json
import signal
import subprocess
import time
from pathlib import Path


def command(host, port, duration=2, qps=4, rate_gbps=None, continuous=True):
    argv = [
        "/usr/bin/stdbuf",
        "-oL",
        "-eL",
        "/usr/bin/ib_read_bw",
        "-d",
        "mlx5_2",
        "-i",
        "1",
        "-x",
        "3",
        "-p",
        str(port),
        "-s",
        "1048576",
        "-q",
        str(qps),
        "-t",
        "32",
        "-o",
        "16",
        "-D",
        str(duration),
        "-f",
        "0",
        "-F",
        "--report_gbits",
    ]
    if continuous:
        if rate_gbps is not None:
            raise ValueError("paced perftest READ requires --finite-seconds")
        argv.append("--run_infinitely")
    if rate_gbps is not None:
        argv.extend([f"--rate_limit={rate_gbps}", "--rate_limit_type=SW"])
    if host:
        argv.append(host)
    return argv


def measure(args):
    args.output.parent.mkdir(parents=True, exist_ok=True)
    finite_seconds = getattr(args, "finite_seconds", None)
    argv = command(
        args.host,
        args.port,
        duration=finite_seconds or 2,
        qps=args.qps,
        rate_gbps=getattr(args, "rate_gbps", None),
        continuous=finite_seconds is None,
    )
    stopping = False
    child = subprocess.Popen(
        argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )

    def stop(signum, frame):
        nonlocal stopping
        stopping = True
        if child.poll() is None:
            child.terminate()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    with args.output.open("a", buffering=1) as stream:

        def event(kind, **fields):
            stream.write(
                json.dumps({"event": kind, "unix_ns": time.time_ns(), **fields}) + "\n"
            )

        event(
            "background_started",
            pid=child.pid,
            argv=argv,
            expected_gbps=args.expected_gbps,
        )
        try:
            for line in child.stdout:
                event("perftest_output", text=line.rstrip())
                fields = line.split()
                if (
                    len(fields) in (4, 5)
                    and fields[0].isdigit()
                    and fields[1].isdigit()
                ):
                    try:
                        bandwidth = float(fields[-2])
                    except ValueError:
                        continue
                    event(
                        "background_bandwidth",
                        bandwidth_gbps=bandwidth,
                        report_period_seconds=finite_seconds or 2,
                    )
        finally:
            if child.poll() is None:
                child.terminate()
            code = child.wait(timeout=10)
            event("background_finished", returncode=code, requested_stop=stopping)
    if code and not stopping:
        raise RuntimeError(f"RDMA background exited: {code}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host")
    parser.add_argument("--port", type=int, default=29942)
    parser.add_argument("--expected-gbps", type=float, required=True)
    parser.add_argument("--qps", type=int, default=4)
    parser.add_argument("--rate-gbps", type=float)
    parser.add_argument("--finite-seconds", type=int)
    parser.add_argument("--output", type=Path, required=True)
    measure(parser.parse_args())
