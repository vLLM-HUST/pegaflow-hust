"""Streaming client with absolute open-loop deadlines and arm-wide cancellation."""

import asyncio
import json
import time
from pathlib import Path

import httpx


class ArmFailure(RuntimeError):
    pass


async def stream_request(client, url, case, output, origin, planned_offset, timeout):
    rid = case["request_id"]
    planned = origin + planned_offset
    await asyncio.sleep(max(0, planned - time.monotonic()))
    started = time.monotonic()
    record = {"request_id": rid, "cohort_id": case["cohort_id"],
              "planned_offset_seconds": planned_offset, "started_monotonic_ns": time.monotonic_ns(),
              "dispatch_lateness_ms": (started - planned) * 1000, "status": "running"}
    stream_dir = Path(output) / "sse"
    stream_dir.mkdir(parents=True, exist_ok=True)
    text, first_text, first_event, done, usage, finish_reason = [], None, None, False, None, None
    try:
        async with asyncio.timeout(timeout):
            async with client.stream("POST", url.rstrip("/") + "/v1/completions",
                                     json=case["payload"], headers={"X-Request-Id": rid}) as response:
                response.raise_for_status()
                if "text/event-stream" not in response.headers.get("content-type", ""):
                    raise ArmFailure("Expected an SSE response")
                with (stream_dir / f"{rid}.jsonl").open("w", encoding="utf-8") as raw:
                    data_lines = []
                    async for line in response.aiter_lines():
                        if line.startswith("data:"):
                            data_lines.append(line[5:].lstrip(" "))
                            continue
                        if line or not data_lines:
                            continue
                        event_time = time.monotonic()
                        data = "\n".join(data_lines)
                        data_lines.clear()
                        raw.write(json.dumps({"offset_seconds": event_time - started, "data": data}, ensure_ascii=False) + "\n")
                        raw.flush()
                        first_event = first_event if first_event is not None else event_time
                        if data == "[DONE]":
                            done = True
                            break
                        event = json.loads(data)
                        if event.get("error"):
                            raise ArmFailure(f"SSE error: {event['error']}")
                        usage = event.get("usage") or usage
                        for choice in event.get("choices", []):
                            piece = choice.get("text", "")
                            if piece:
                                first_text = first_text if first_text is not None else event_time
                                text.append(piece)
                            finish_reason = choice.get("finish_reason") or finish_reason
                    if data_lines:
                        raise ArmFailure("Truncated SSE event")
                if not done or first_text is None or finish_reason is None:
                    raise ArmFailure("Incomplete stream, missing generated text, or missing finish reason")
                if not usage or not isinstance(usage.get("completion_tokens"), int):
                    raise ArmFailure("Server did not provide final token usage")
                if usage.get("prompt_tokens") != case["input_tokens"]:
                    raise ArmFailure("Server prompt token count differs from the frozen manifest")
        record.update(status="ok", ttft_seconds=first_text - started,
                      first_sse_seconds=first_event - started, usage=usage, finish_reason=finish_reason)
    except BaseException as error:
        record.update(status="cancelled" if isinstance(error, asyncio.CancelledError) else "error",
                      error=f"{type(error).__name__}: {error}")
        raise
    finally:
        record.update(e2e_seconds=time.monotonic() - started, text="".join(text))
        with (Path(output) / "client.jsonl").open("a", encoding="utf-8") as target:
            target.write(json.dumps(record, ensure_ascii=False) + "\n")
    return record


async def open_loop(client, url, cases, output, timeout):
    origin = time.monotonic()
    tasks = [asyncio.create_task(stream_request(client, url, case, output, origin,
                                              case["planned_offset_seconds"], timeout)) for case in cases]
    try:
        return await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


def make_client(request_count):
    return httpx.AsyncClient(timeout=None, trust_env=False,
                            limits=httpx.Limits(max_connections=request_count + 4,
                                               max_keepalive_connections=request_count + 4))
