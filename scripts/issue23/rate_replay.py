"""Open-loop full-history 2P2D replay with request-local KV readiness."""

import argparse
import asyncio
import json
import math
import random
import re
import time
from collections import Counter
from pathlib import Path

import httpx


def percentile(values, quantile):
    if not values:
        return None
    return sorted(values)[max(0, math.ceil(len(values) * quantile) - 1)]


def schedule(cases, seed, rate):
    order, arrivals, routes = (
        random.Random(seed),
        random.Random(seed + 1000),
        random.Random(seed + 2000),
    )
    shuffled = list(cases)
    order.shuffle(shuffled)
    elapsed = 0.0
    rows = []
    for index, case in enumerate(shuffled):
        if index:
            elapsed += arrivals.expovariate(1)
        rows.append(
            {
                "case": case,
                "index": index,
                "p": index % 2,
                "d": routes.randrange(2),
                "unit_offset": elapsed,
                "offset": elapsed / rate if rate else 0.0,
            }
        )
    return rows


class Replay:
    def __init__(self, config, output, timeout=180):
        self.config, self.output, self.timeout = config, output, timeout
        self.root = Path(config["root"])
        self.instances = {x["id"]: x for x in config["instances"]}
        self.output.mkdir(parents=True, exist_ok=True)
        self.events = (output / "events.jsonl").open("a", buffering=65536)
        self.results = (output / "requests.jsonl").open("a", buffering=65536)
        self.active = 0
        self.p_budget = [0, 0]
        self.capacity_condition = asyncio.Condition()
        self.max_source_bytes = int(config.get("host_pool_bytes", 8 * 2**30) * 0.75)

    def telemetry_positions(self):
        positions = {
            str(path): path.stat().st_size
            for role in ("D0", "D1")
            for path in (self.root / "services/telemetry" / role).glob("*.jsonl")
        }
        for role in ("D0", "D1"):
            path = self.root / "services" / f"91-{role}-models.log"
            positions[str(path)] = path.stat().st_size if path.exists() else 0
        return positions

    def path_coverage(self, results, positions):
        hits = {}
        for role in ("D0", "D1"):
            for path in (self.root / "services/telemetry" / role).glob("*.jsonl"):
                with path.open() as stream:
                    stream.seek(positions.get(str(path), 0))
                    for line in stream:
                        try:
                            event = json.loads(line)
                        except ValueError:
                            continue
                        if event.get("event") != "cache_query_ready":
                            continue
                        raw = event["request_id"].removeprefix("cmpl-")
                        request_id = raw.rsplit("-0-", 1)[0]
                        hits[request_id] = max(
                            hits.get(request_id, 0), event.get("hit_tokens", 0)
                        )
            log = self.root / "services" / f"91-{role}-models.log"
            if log.exists():
                with log.open() as stream:
                    stream.seek(positions.get(str(log), 0))
                    for line in stream:
                        match = re.search(
                            r"req=(\S+) cache_lookup: .*?hit_tokens=(\d+)", line
                        )
                        if match:
                            request_id = (
                                match[1].removeprefix("cmpl-").rsplit("-0-", 1)[0]
                            )
                            hits[request_id] = max(
                                hits.get(request_id, 0), int(match[2])
                            )
        rows = []
        for result in results:
            expected = (result["input_tokens"] - 1) // 128 * 128
            actual = hits.get(result["request_id"])
            status = (
                "unobserved"
                if actual is None
                else "full_blocks"
                if actual >= expected
                else "miss"
                if actual == 0
                else "partial"
            )
            rows.append(
                {
                    "request_id": result["request_id"],
                    "expected_full_block_tokens": expected,
                    "remote_hit_tokens": actual,
                    "status": status,
                }
            )
        return rows

    def event(self, event, **fields):
        self.events.write(
            json.dumps(
                {
                    "run_id": self.output.name,
                    "event": event,
                    "unix_ns": time.time_ns(),
                    "monotonic_ns": time.monotonic_ns(),
                    **fields,
                }
            )
            + "\n"
        )

    def url(self, name):
        node = self.instances[name]
        return f"http://{node['host']}:{node['model_port']}"

    async def reset(self, client):
        for name in self.instances:
            response = await client.post(
                self.url(name) + "/reset_prefix_cache", timeout=30
            )
            response.raise_for_status()
        for node in self.instances.values():
            response = await client.post(
                f"http://{node['host']}:{node['engine_http_port']}/cache/memory/cleanup",
                timeout=30,
            )
            response.raise_for_status()

    async def metrics(self, client, folder, label):
        folder.mkdir(parents=True, exist_ok=True)
        for name, node in self.instances.items():
            for kind, port in [
                ("model", node["model_port"]),
                ("engine", node["engine_http_port"]),
            ]:
                try:
                    response = await client.get(
                        f"http://{node['host']}:{port}/metrics", timeout=3
                    )
                    (folder / f"{label}-{name}-{kind}.prom").write_text(response.text)
                except (httpx.HTTPError, OSError) as error:
                    self.event(
                        "metrics_error", instance=name, kind=kind, error=str(error)
                    )

    async def request(self, client, item, point, origin, output_tokens, gate=None):
        case, p, d = item["case"], item["p"], item["d"]
        rid = f"tg-{self.output.name}-{point}-{item['index']:03d}"
        planned = origin + item["offset"]
        await asyncio.sleep(max(0, planned - time.monotonic()))
        started = time.monotonic()
        row = {
            "request_id": rid,
            "run_id": self.output.name,
            "point": point,
            "task": case["task"],
            "step_id": case["step_id"],
            "input_tokens": case["input_tokens"],
            "p_id": f"P{p}",
            "d_id": f"D{d}",
            "planned_offset_s": item["offset"],
            "dispatch_lag_ms": (started - planned) * 1000,
            "started_unix_ns": time.time_ns(),
            "token_ids_sha256": case["token_ids_sha256"],
            "status": "running",
        }
        self.event("arrived", **row)
        if gate is None and self.active >= 64:
            row.update(status="rejected", error="admission_capacity_64")
            self.results.write(json.dumps(row) + "\n")
            return row
        self.active += 1
        reservation = False
        gated = False
        payload = {
            "model": "qwen25",
            "prompt": case["prompt_token_ids"],
            "temperature": 0,
            "top_p": 1,
            "ignore_eos": True,
            "request_id": rid,
            "cache_salt": rid,
            "seed": 17,
        }
        try:
            async with asyncio.timeout(self.timeout):
                if gate is not None:
                    await gate.acquire()
                    gated = True
                expected_bytes = case["input_tokens"] * 57344
                row["source_budget_wait_start_s"] = time.monotonic() - started
                async with self.capacity_condition:
                    await self.capacity_condition.wait_for(
                        lambda: (
                            self.p_budget[p] + expected_bytes <= self.max_source_bytes
                        )
                    )
                    self.p_budget[p] += expected_bytes
                    reservation = True
                row["source_budget_wait_s"] = (
                    time.monotonic() - started - row["source_budget_wait_start_s"]
                )
                self.event(
                    "p_submit",
                    request_id=rid,
                    p_id=f"P{p}",
                    d_id=f"D{d}",
                    active=self.active,
                    source_reserved_bytes=self.p_budget[p],
                )
                row["p_submit_s"] = time.monotonic() - started
                response = await client.post(
                    self.url(f"P{p}") + "/v1/completions",
                    json={**payload, "max_tokens": 1, "stream": False},
                    headers={"X-Request-Id": rid},
                )
                response.raise_for_status()
                p_result = response.json()
                row["p_usage"] = p_result.get("usage")
                row["p_response_id"] = p_result.get("id")
                row["p_response_s"] = time.monotonic() - started
                self.event("p_response", request_id=rid, usage=row["p_usage"])
                marker = (
                    self.root / "services/visibility" / f"P{p}" / f"{rid}.tp0.visible"
                )
                while not marker.exists():
                    await asyncio.sleep(0.01)
                row["source_visible_s"] = time.monotonic() - started
                row["ack_observe_wait_s"] = (
                    row["source_visible_s"] - row["p_response_s"]
                )
                self.event("source_visible", request_id=rid, p_id=f"P{p}")
                row["d_submit_s"] = time.monotonic() - started
                self.event("d_submit", request_id=rid, d_id=f"D{d}")
                chunks, text, first, last, usage, done = [], [], None, None, None, False
                async with client.stream(
                    "POST",
                    self.url(f"D{d}") + "/v1/completions",
                    json={
                        **payload,
                        "max_tokens": output_tokens,
                        "stream": True,
                        "stream_options": {"include_usage": True},
                        "return_token_ids": True,
                    },
                    headers={"X-Request-Id": rid},
                ) as stream:
                    stream.raise_for_status()
                    async for line in stream.aiter_lines():
                        if not line.startswith("data: "):
                            continue
                        data = line[6:]
                        if data == "[DONE]":
                            done = True
                            break
                        event = json.loads(data)
                        if event.get("error"):
                            raise RuntimeError(str(event["error"]))
                        usage = event.get("usage") or usage
                        for choice in event.get("choices", []):
                            piece, ids = (
                                choice.get("text", ""),
                                choice.get("token_ids") or [],
                            )
                            if piece or ids:
                                now = time.monotonic()
                                if first is None:
                                    first = now
                                    self.event(
                                        "first_token", request_id=rid, d_id=f"D{d}"
                                    )
                                    if reservation:
                                        async with self.capacity_condition:
                                            self.p_budget[p] -= expected_bytes
                                            reservation = False
                                            self.capacity_condition.notify_all()
                                last = now
                                text.append(piece)
                                chunks.append(
                                    {
                                        "offset_s": now - started,
                                        "chars": len(piece),
                                        "tokens": len(ids) if ids else None,
                                    }
                                )
                if not done or first is None or usage is None:
                    raise RuntimeError("incomplete decode stream")
                row.update(
                    status="ok",
                    ttft_s=first - started,
                    d_ttft_s=first - started - row["d_submit_s"],
                    tpot_s=(last - first) / max(1, usage["completion_tokens"] - 1),
                    output_tokens=usage["completion_tokens"],
                    d_usage=usage,
                    stream_chunks=chunks,
                    output_text="".join(text),
                )
                if usage.get("prompt_tokens") != case["input_tokens"]:
                    row.update(status="error", error="server_input_length_mismatch")
        except (Exception, asyncio.CancelledError) as error:  # noqa: BLE001 -- preserve failed requests
            row.update(
                status="cancelled"
                if isinstance(error, asyncio.CancelledError)
                else "error",
                error=f"{type(error).__name__}: {error}",
            )
        finally:
            if reservation:
                async with self.capacity_condition:
                    self.p_budget[p] -= case["input_tokens"] * 57344
                    self.capacity_condition.notify_all()
            if gated:
                gate.release()
            self.active -= 1
            row["e2e_s"] = time.monotonic() - started
            row["finished_unix_ns"] = time.time_ns()
            self.results.write(json.dumps(row) + "\n")
            self.results.flush()
            self.event(
                "finished",
                request_id=rid,
                status=row["status"],
                active=self.active,
                e2e_s=row["e2e_s"],
            )
            self.events.flush()
        return row

    async def point(
        self,
        client,
        cases,
        label,
        seed,
        rate,
        output_tokens=256,
        concurrency=None,
        four_routes=False,
    ):
        folder = self.output / label
        if (folder / "summary.json").exists():
            return json.loads((folder / "summary.json").read_text())
        folder.mkdir(parents=True, exist_ok=True)
        await self.reset(client)
        await self.metrics(client, folder, "before")
        positions = self.telemetry_positions()
        rows = schedule(cases, seed, rate)
        if four_routes:
            for index, row in enumerate(rows):
                row.update(p=index // 2 % 2, d=index % 2)
        (folder / "schedule.json").write_text(
            json.dumps(
                [
                    {k: v for k, v in row.items() if k != "case"}
                    | {"case": row["case"]["request_id"]}
                    for row in rows
                ],
                indent=2,
            )
        )
        (self.root / "phase.json").write_text(
            json.dumps(
                {"point": label, "rate": rate, "seed": seed, "unix_ns": time.time_ns()}
            )
        )
        print(
            "START",
            label,
            "requests",
            len(rows),
            "rate",
            rate,
            "concurrency",
            concurrency,
            flush=True,
        )
        origin = time.monotonic() + 0.1
        gate = asyncio.Semaphore(concurrency) if concurrency else None
        results = await asyncio.gather(
            *[
                self.request(client, row, label, origin, output_tokens, gate)
                for row in rows
            ]
        )
        elapsed = time.monotonic() - origin
        summary = summarize(results, elapsed, rate)
        coverage = self.path_coverage(results, positions)
        (folder / "cache-paths.json").write_text(json.dumps(coverage, indent=2))
        summary["cache_paths"] = dict(Counter(row["status"] for row in coverage))
        (folder / "summary.json").write_text(json.dumps(summary, indent=2))
        await self.metrics(client, folder, "after")
        print("RESULT", label, json.dumps(summary), flush=True)
        return summary


def summarize(rows, elapsed, rate):
    good = [r for r in rows if r["status"] == "ok"]
    result = {
        "planned_requests": len(rows),
        "statuses": dict(Counter(r["status"] for r in rows)),
        "elapsed_s": elapsed,
        "configured_rate": rate,
        "completion_rps": len(good) / elapsed,
        "input_tps": sum(r["input_tokens"] for r in good) / elapsed,
        "output_tps": sum(r["output_tokens"] for r in good) / elapsed,
        "routes": dict(Counter(r["p_id"] + "->" + r["d_id"] for r in rows)),
    }
    for name in [
        "ttft_s",
        "d_ttft_s",
        "tpot_s",
        "e2e_s",
        "source_budget_wait_s",
        "dispatch_lag_ms",
    ]:
        values = [r[name] for r in good if name in r]
        result[name] = {
            "p50": percentile(values, 0.5),
            "p95": percentile(values, 0.95),
            "max": max(values) if values else None,
        }
    return result


async def run(args):
    config = json.loads(args.config.read_text())
    cases = [json.loads(line) for line in args.requests.read_text().splitlines()]
    calibration = [c for c in cases if c["partition"] == "calibration"]
    main = [c for c in cases if c["partition"] == "main"]
    replay = Replay(config, args.output, args.timeout)
    limits = httpx.Limits(
        max_connections=140, max_keepalive_connections=140, keepalive_expiry=2
    )
    async with httpx.AsyncClient(
        timeout=None, trust_env=False, limits=limits
    ) as client:
        deadline = time.monotonic() + 900
        while True:
            states = await asyncio.gather(
                *[
                    client.get(replay.url(name) + "/health", timeout=2)
                    for name in replay.instances
                ],
                return_exceptions=True,
            )
            if all(
                isinstance(s, httpx.Response) and s.status_code == 200 for s in states
            ):
                break
            if time.monotonic() > deadline:
                raise TimeoutError("four model services not ready")
            print(
                "WAIT_SERVICES",
                [getattr(s, "status_code", type(s).__name__) for s in states],
                flush=True,
            )
            await asyncio.sleep(5)
        if args.mode in ("smoke", "campaign"):
            ordered = sorted(calibration, key=lambda c: c["input_tokens"])
            smoke = [ordered[0], ordered[len(ordered) // 2], ordered[-2], ordered[-1]]
            result = await replay.point(
                client,
                smoke,
                "smoke",
                17,
                0,
                output_tokens=16,
                concurrency=2,
                four_routes=True,
            )
            if result["statuses"].get("ok", 0) != 4:
                raise RuntimeError("smoke failed; inspect requests.jsonl")
            if result["cache_paths"].get("miss", 0) or result["cache_paths"].get(
                "partial", 0
            ):
                raise RuntimeError("smoke did not restore the expected remote prefix")
            if args.mode == "smoke":
                return
        if args.mode in ("calibration", "campaign"):
            low = await replay.point(
                client, calibration, "cal-low", 17, 0, concurrency=4
            )
            load = await replay.point(
                client, calibration, "cal-load", 29, 0, concurrency=16
            )
            if load["statuses"].get("ok", 0) != 32:
                raise RuntimeError("calibration failed")
            main_mean = sum(c["input_tokens"] for c in main) / len(main)
            cal_mean = sum(c["input_tokens"] for c in calibration) / len(calibration)
            correction = min(1.2, (cal_mean / main_mean) ** 0.5)
            reference = load["completion_rps"] * correction
            calibration_result = {
                "lambda_ref": reference,
                "mix_correction": correction,
                "cal_low": low,
                "cal_load": load,
            }
            (args.output / "calibration.json").write_text(
                json.dumps(calibration_result, indent=2)
            )
            if args.mode == "calibration":
                return
        else:
            reference = args.rate
        if args.mode == "point":
            await replay.point(client, main, args.label, 17, args.rate)
        elif args.mode == "campaign":
            begin = time.monotonic()
            summaries = {}
            factors = {"L": 0.5, "M": 0.85, "H": 1.1}
            seeds = [int(x) for x in args.seeds.split(",")]
            for index, seed in enumerate(seeds):
                order = ["L", "M", "H"]
                order = order[index % 3 :] + order[: index % 3]
                for name in order:
                    if time.monotonic() - begin > args.budget:
                        (args.output / "campaign.json").write_text(
                            json.dumps(summaries, indent=2)
                        )
                        print("BUDGET_REACHED", flush=True)
                        return
                    label = f"s{seed}-{name}"
                    result = await replay.point(
                        client, main, label, seed, reference * factors[name]
                    )
                    summaries[label] = result
                    (args.output / "campaign.json").write_text(
                        json.dumps(summaries, indent=2)
                    )
                    if result["statuses"].get("error", 0) >= 3:
                        raise RuntimeError(
                            "main point contains errors; inspect before proceeding"
                        )
    replay.events.close()
    replay.results.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--requests", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--mode",
        choices=["smoke", "calibration", "campaign", "point"],
        default="campaign",
    )
    parser.add_argument("--seeds", default="17,29,43")
    parser.add_argument("--budget", type=float, default=900)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--rate", type=float, default=1)
    parser.add_argument("--label", default="manual-point")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
