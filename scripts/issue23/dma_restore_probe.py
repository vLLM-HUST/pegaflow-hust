"""Finite fixed-buffer native IPC restoration crossover on one allocated NPU."""

import argparse
import contextlib
import hashlib
import json
import os
import pickle
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
import traceback
import urllib.request
from pathlib import Path

from dma_restore_tools import external_output

PLANES = 96
BLOCK_BYTES = 262144


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def resources():
    return {
        n: (Path("/sys/fs/cgroup") / n).read_text()
        for n in ["cpu.stat", "cpu.pressure", "memory.current", "memory.events"]
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=socket.gethostname())
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--python-source", type=Path)
    parser.add_argument("--restore-timeout", type=float, default=180)
    parser.add_argument("--device", type=int, required=True)
    parser.add_argument("--numa", type=int, required=True)
    parser.add_argument("--name", default="initial")
    parser.add_argument("--layouts", default="contiguous")
    parser.add_argument("--counts", default="161")
    parser.add_argument("--policies", default="29,30,29")
    parser.add_argument("--capacity", type=int, default=512)
    parser.add_argument("--pool-gib", type=int, default=5)
    parser.add_argument("--all-tags", action="store_true")
    args = parser.parse_args()
    counts = [int(x) for x in args.counts.split(",")]
    policies = [int(x) for x in args.policies.split(",")]
    if not counts or min(counts) < 1 or max(counts) + 256 > args.capacity:
        parser.error("counts must fit in the source and destination regions")
    if not policies or any(mode not in (0, 29, 30) for mode in policies) or args.pool_gib < 4:
        parser.error("use policies 0/29/30 and a pool of at least 4GiB")
    if any(
        layout not in ("contiguous", "reversed", "random") for layout in args.layouts.split(",")
    ):
        parser.error("layouts must be contiguous, reversed or random")
    if not args.binary.is_file() or not args.library.is_file():
        parser.error("binary and library must exist")
    if max(counts) * PLANES * BLOCK_BYTES > args.pool_gib * 2**30:
        parser.error("source blocks exceed the CPU pool")
    if args.restore_timeout <= 0:
        parser.error("restore-timeout must be positive")
    source_count = max(counts)
    root = external_output(args.output)
    root.mkdir(parents=True, exist_ok=False)
    import shutil

    shutil.copy2(Path(__file__), root / "executed-probe.py")
    live = root / "native"
    live.mkdir()
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ["MKL_NUM_THREADS"] = "1"
    if args.python_source is not None:
        sys.path.insert(0, str(args.python_source.resolve()))
    stop = threading.Event()
    server = None

    def emit(name, row):
        with (root / name).open("a") as stream:
            stream.write(json.dumps(row) + "\n")

    def monitor():
        while not stop.is_set():
            emit("resources.jsonl", dict(epoch_ns=time.time_ns(), **resources()))
            stop.wait(1)

    def watchdog():
        if not stop.wait(900):
            (root / "watchdog.json").write_text(json.dumps({"epoch": time.time()}))
            if server is not None:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(server.pid, signal.SIGKILL)
            os._exit(124)

    threading.Thread(target=monitor, daemon=True).start()
    threading.Thread(target=watchdog, daemon=True).start()
    try:
        import torch
        import torch_npu
        from pegaflow.npu_ipc_wrapper import NpuIPCWrapper
        from pegaflow.pegaflow import EngineRpcClient, PyLoadState

        torch.set_num_threads(1)
        torch.npu.set_device(args.device)
        # Fixed real-model geometry: 96 planes, 512 pages each, 256KiB/page.
        planes = [
            torch.full(
                (args.capacity, BLOCK_BYTES // 2),
                0.25,
                dtype=torch.bfloat16,
                device=f"npu:{args.device}",
            )
            for _ in range(PLANES)
        ]
        for i, tensor in enumerate(planes):
            tensor.view(torch.int16)[:, 0] = (
                (
                    torch.arange(args.capacity, device=f"npu:{args.device}", dtype=torch.int32)
                    + i * 521
                    + 1000
                )
                % 30000
            ).to(torch.int16)
        torch.npu.synchronize()
        wrappers = [pickle.dumps(NpuIPCWrapper(tensor)) for tensor in planes]
        names = [f"plane-{i}" for i in range(PLANES)]
        instance = f"causal-{args.host}-{args.name}-{args.device}"
        port, http = free_port(), free_port()
        policy = root / "policy.bin"
        policy.write_bytes(struct.pack("i", 0))
        env = dict(
            os.environ,
            PEGAFLOW_SYNC_SAVE="1",
            TOKIO_WORKER_THREADS="2",
            LD_PRELOAD=str(args.library.resolve()),
            CAUSAL_IMMUTABLE_SOURCE="1",
            NATIVE_COPY_POLICY=str(policy),
            NATIVE_COPY_TRACE_LOG=str(live / "api.jsonl"),
        )
        if args.python_source is not None:
            env["PYTHONPATH"] = (
                str(args.python_source.resolve()) + os.pathsep + env.get("PYTHONPATH", "")
            )
        command = [
            "numactl",
            f"--membind={args.numa}",
            str(args.binary.resolve()),
            "--devices",
            str(args.device),
            "--pool-size",
            f"{args.pool_gib * 2**30}b",
            "--disable-numa-affinity",
            "--addr",
            f"127.0.0.1:{port}",
            "--http-addr",
            f"127.0.0.1:{http}",
            "--enable-prometheus",
        ]
        config = {
            "host": args.host,
            "device": args.device,
            "numa": args.numa,
            "command": command,
            "source": str(args.python_source),
            "planes": PLANES,
            "capacity": args.capacity,
            "block_bytes_per_plane": BLOCK_BYTES,
            "pool_bytes": args.pool_gib * 2**30,
            "source_blocks": source_count,
            "counts": counts,
            "stage_policies": policies,
            "layouts": args.layouts,
            "torch": torch.__version__,
            "torch_npu": torch_npu.__version__,
            "resources": resources(),
            "preload": env["LD_PRELOAD"],
            "pointers": [tensor.data_ptr() for tensor in planes],
            "start_epoch_ns": time.time_ns(),
        }
        (root / "config.json").write_text(json.dumps(config, indent=2))
        log = (live / "native.log").open("w")
        server = subprocess.Popen(
            command,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        (root / "native-process.json").write_text(
            json.dumps({"pid": server.pid, "command": command})
        )
        deadline = time.monotonic() + 90
        while True:
            if server.poll() is not None:
                raise RuntimeError("Native process exited")
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{http}/health", timeout=1).read()
                break
            except OSError as error:
                if time.monotonic() > deadline:
                    raise TimeoutError("Native startup") from error
                time.sleep(0.2)
        client = EngineRpcClient(f"http://127.0.0.1:{port}")
        ok, message = client.register_context_batch(
            instance,
            instance,
            0,
            0,
            1,
            1,
            args.device,
            names,
            wrappers,
            [args.capacity] * PLANES,
            [BLOCK_BYTES] * PLANES,
            [0] * PLANES,
            [1] * PLANES,
            "ascend_direct",
            False,
        )
        if not ok:
            raise RuntimeError(message)
        hashes = [
            hashlib.sha256(f"{instance}:block:{i}".encode()).digest() for i in range(source_count)
        ]
        start = time.monotonic()
        ok, message = client.save(
            instance,
            0,
            0,
            args.device,
            [(name, list(range(source_count)), hashes) for name in names],
        )
        if not ok:
            raise RuntimeError(message)
        emit(
            "operations.jsonl",
            {
                "event": "seed",
                "elapsed_s": time.monotonic() - start,
                "epoch_ns": time.time_ns(),
            },
        )
        counter = 0

        def native_cpu():
            rows = []
            for task in Path(f"/proc/{server.pid}/task").iterdir():
                with contextlib.suppress(OSError):
                    rows.append(
                        {
                            "tid": int(task.name),
                            "name": (task / "comm").read_text().strip(),
                            "scheduler": os.sched_getscheduler(int(task.name)),
                            "schedstat": (task / "schedstat").read_text(),
                            "cpu_group": (task / "cgroup").read_text(),
                        }
                    )
            return rows

        def restore(count, phase, mode, layout="contiguous"):
            nonlocal counter
            counter += 1
            policy.write_bytes(struct.pack("i", mode))
            destinations = list(range(256, 256 + count))
            if layout == "reversed":
                destinations = destinations[::-1]
            elif layout == "random":
                import random

                destinations = random.Random(17).sample(list(range(256, args.capacity)), count)
            checks = list(range(count)) if args.all_tags else [0, count // 2, count - 1]
            for plane in range(PLANES) if args.all_tags else [0, 95]:
                planes[plane].view(torch.int16)[[destinations[i] for i in checks], 0] = 0
            torch.npu.synchronize()
            query = client.query_prefetch(instance, hashes[:count], f"{instance}-{counter}")
            deadline = time.monotonic() + 30
            while not hasattr(query, "num_hit_blocks"):
                if time.monotonic() > deadline:
                    raise TimeoutError("Prefix visibility")
                time.sleep(0.005)
                query = client.query_prefetch(instance, hashes[:count], f"{instance}-{counter}")
            if query.num_hit_blocks != count:
                raise RuntimeError(f"Incomplete prefix {query.num_hit_blocks}/{count}")
            state = PyLoadState()
            before = resources()
            threads_before = native_cpu()
            epoch = time.time_ns()
            start = time.monotonic()
            ok, message = client.load(
                instance,
                0,
                args.device,
                state.shm_name(),
                [names],
                [(query.lease, [destinations])],
            )
            submitted = time.monotonic()
            if not ok:
                raise RuntimeError(message)
            deadline = time.monotonic() + args.restore_timeout
            while not state.is_ready():
                if time.monotonic() > deadline:
                    raise TimeoutError(
                        "Restore timed out; stopping the native process before another policy"
                    )
                time.sleep(0.001)
            end = time.monotonic()
            if state.get_state() < 0:
                raise RuntimeError("Native restore state error")
            observed = []
            for plane in range(PLANES) if args.all_tags else [0, 95]:
                values = (
                    planes[plane]
                    .view(torch.int16)[[destinations[i] for i in checks], 0]
                    .cpu()
                    .tolist()
                )
                expected = [(i + plane * 521 + 1000) % 30000 for i in checks]
                if values != expected:
                    raise RuntimeError(f"Copy tags differ: {values} vs {expected}")
                observed.append(values)
            row = {
                "event": "restore",
                "sequence": counter,
                "phase": phase,
                "policy": mode,
                "layout": layout,
                "blocks": count,
                "bytes": count * PLANES * BLOCK_BYTES,
                "descriptors": count * PLANES,
                "start_epoch_ns": epoch,
                "end_epoch_ns": time.time_ns(),
                "complete_s": end - start,
                "submit_s": submitted - start,
                "verified_tag_count": sum(len(x) for x in observed),
                "all_tags": args.all_tags,
                "verified_tags": observed,
                "resources_before": before,
                "resources_after": resources(),
            }
            row.update(native_threads_before=threads_before, native_threads_after=native_cpu())
            emit("operations.jsonl", row)
            print(
                json.dumps({k: row[k] for k in ["phase", "policy", "blocks", "complete_s"]}),
                flush=True,
            )

        restore(min(counts), "warm-original", 0)
        for stage, mode in enumerate(policies):
            for count in counts if stage % 2 == 0 else list(reversed(counts)):
                for layout in args.layouts.split(","):
                    restore(count, f"stage-{stage}", mode, layout)
        client.unregister_context(instance)
        (root / "done.json").write_text(json.dumps({"epoch": time.time(), "restores": counter}))
    except BaseException as error:
        (root / "failure.json").write_text(
            json.dumps(
                {
                    "error": repr(error),
                    "traceback": traceback.format_exc(),
                    "epoch": time.time(),
                }
            )
        )
        raise
    finally:
        stop.set()
        if server is not None and server.poll() is None:
            os.killpg(server.pid, signal.SIGTERM)
            try:
                server.wait(10)
            except subprocess.TimeoutExpired:
                os.killpg(server.pid, signal.SIGKILL)
                server.wait()


if __name__ == "__main__":
    main()
