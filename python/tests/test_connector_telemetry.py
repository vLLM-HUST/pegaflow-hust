"""Telemetry must preserve serving progress and finish complete JSON records."""

import itertools
import json
import queue

from .unit_stubs import install_connector_unit_stubs

install_connector_unit_stubs()

from pegaflow.connector import telemetry  # noqa: E402


def test_disabled_telemetry_does_not_allocate_writer(monkeypatch):
    monkeypatch.delenv("PEGAFLOW_TELEMETRY_DIR", raising=False)
    monkeypatch.setattr(telemetry, "_sink", None)
    telemetry.telemetry_event("disabled", value=object())
    assert telemetry._sink is None


def test_async_writer_preserves_clock_identity_and_drains(tmp_path):
    sink = telemetry.TelemetrySink(str(tmp_path))
    sink.emit("start", request_id="request", tp_rank=0)
    sink.emit("end", request_id="request", duration_ms=0.5)
    sink.close()
    rows = [json.loads(line) for line in sink.path.read_text().splitlines()]
    assert sink.error is None and not sink.thread.is_alive()
    assert [r["event"] for r in rows] == ["start", "end", "telemetry_closed"]
    assert [r["sequence"] for r in rows[:2]] == [0, 1]
    assert all(r["pid"] == sink.pid for r in rows)
    assert rows[1]["monotonic_ns"] >= rows[0]["monotonic_ns"]
    assert rows[-1]["dropped_events"] == 0


def test_full_telemetry_queue_drops_instead_of_blocking():
    sink = telemetry.TelemetrySink.__new__(telemetry.TelemetrySink)
    sink.pid, sink.node, sink.role = 1, "fixture", "fixture"
    sink.queue = queue.Queue(maxsize=1)
    sink.sequence = itertools.count()
    sink.dropped = 0
    sink.emit("first")
    sink.emit("second")
    assert sink.queue.qsize() == 1 and sink.dropped == 1
