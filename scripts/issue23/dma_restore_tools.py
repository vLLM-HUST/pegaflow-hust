"""Build and launch opt-in Ascend DMA restore experiments."""

import argparse
import json
import os
import re
import subprocess
import time
import urllib.request
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[2]
SOURCES = Path(__file__).with_name("dma_restore_sources")
KINDS = {
    "staging": ("dma_staging.c", "dma_staging.so"),
    "shadow": ("shadow_dma_toggle.c", "shadow_dma_toggle.so"),
    "host-probe": ("host_dma_cache.c", "host_dma_cache"),
}


def external_output(path):
    path = Path(path).expanduser().resolve()
    if path.is_relative_to(REPOSITORY):
        raise ValueError("Experiment outputs must be outside the repository checkout")
    return path


def build_command(kind, cc, includes, library_dirs, output):
    source, _ = KINDS[kind]
    command = [cc, "-O2", "-fPIC"]
    if kind != "host-probe":
        command.append("-shared")
    command.extend(f"-I{Path(path).resolve()}" for path in includes)
    command.extend([str(SOURCES / source), "-o", str(output)])
    command.extend(f"-L{Path(path).resolve()}" for path in library_dirs)
    if kind == "host-probe":
        command.append("-lascendcl")
    command.extend(["-ldl", "-pthread"])
    return command


def staging_environment(library, log, parent):
    library = Path(library).expanduser().resolve()
    if not library.is_file():
        raise FileNotFoundError(library)
    log = external_output(log)
    environment = dict(parent)
    environment["LD_PRELOAD"] = ":".join(
        item for item in (str(library), environment.get("LD_PRELOAD", "")) if item
    )
    environment["DMA_STAGE_LOG"] = str(log)
    return environment


def reset_caches(config, timeout):
    deadline = time.monotonic() + timeout
    endpoints = []
    while True:
        pending = {}
        endpoints.clear()
        for instance in config["instances"]:
            host = config["hosts"][instance["role"]]
            model = f"http://{host}:{instance['model_port']}"
            with urllib.request.urlopen(model + "/metrics", timeout=3) as response:
                metrics = response.read().decode()
            values = [
                float(match[1])
                for line in metrics.splitlines()
                if (
                    match := re.match(
                        r"vllm:pega_pending_(?:save_requests|prefetches)(?:\{[^}]*\})?\s+(\S+)",
                        line,
                    )
                )
            ]
            if not values:
                raise RuntimeError(f"Missing cache drain gauges: {instance['id']}")
            pending[instance["id"]] = sum(values)
            endpoints.extend(
                [
                    model + "/reset_prefix_cache",
                    f"http://{host}:{instance['http_port']}/cache/memory/cleanup",
                ]
            )
        if not any(pending.values()):
            break
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Cache work did not drain: {pending}")
        time.sleep(1)
    for endpoint in endpoints:
        with urllib.request.urlopen(
            urllib.request.Request(endpoint, method="POST"), timeout=30
        ) as response:
            if response.status != 200:
                raise RuntimeError(endpoint)
    return pending


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    build = commands.add_parser("build", help="Compile an experimental library or probe")
    build.add_argument("--kind", choices=KINDS, required=True)
    build.add_argument("--cc", default="cc")
    build.add_argument("--include", action="append", required=True)
    build.add_argument("--library-dir", action="append", default=[])
    build.add_argument("--output-dir", type=Path, required=True)
    launch = commands.add_parser("exec", help="Preload staging into one explicit native command")
    launch.add_argument("--library", type=Path, required=True)
    launch.add_argument("--log", type=Path, required=True)
    launch.add_argument("command", nargs=argparse.REMAINDER)
    reset = commands.add_parser("reset", help="Drain then clear caches on explicit endpoints")
    reset.add_argument("--config", type=Path, required=True)
    reset.add_argument("--timeout", type=float, default=20)
    args = parser.parse_args()
    if args.action == "build":
        directory = external_output(args.output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        output = directory / KINDS[args.kind][1]
        if output.exists():
            raise FileExistsError(output)
        command = build_command(args.kind, args.cc, args.include, args.library_dir, output)
        subprocess.run(command, check=True)
        output.with_name(output.name + ".build.json").write_text(
            json.dumps({"command": command, "epoch_ns": time.time_ns()}, indent=2) + "\n"
        )
        print(output)
    elif args.action == "exec":
        command = args.command[1:] if args.command[:1] == ["--"] else args.command
        if not command:
            parser.error("An explicit native server command is required after --")
        log = external_output(args.log)
        if log.exists() or log.with_suffix(".launch.json").exists():
            raise FileExistsError(log)
        log.parent.mkdir(parents=True, exist_ok=True)
        environment = staging_environment(args.library, log, os.environ)
        log.with_suffix(".launch.json").write_text(
            json.dumps(
                {
                    "epoch_ns": time.time_ns(),
                    "command": command,
                    "LD_PRELOAD": environment["LD_PRELOAD"],
                    "DMA_STAGE_LOG": str(log),
                },
                indent=2,
            )
            + "\n"
        )
        os.execvpe(command[0], command, environment)
    else:
        if args.timeout <= 0:
            parser.error("timeout must be positive")
        print(json.dumps(reset_caches(json.loads(args.config.read_text()), args.timeout)))


if __name__ == "__main__":
    main()
