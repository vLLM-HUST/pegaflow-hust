# Qwen2.5 2P2D rate replay: 2026-10-16

This evidence subset contains three completed 95-request rate points with 8 GiB
per consumer host pool and a paired 95-request middle-rate control with 16 GiB.
The runs completed 379/380 measurement requests. Failed requests stay in the
planned denominator; latency distributions use successful requests.

- [Results and interpretation](../../../docs/experiments/issue23-2p2d-rate-results-20261016.md)
- [Workload, deployment and run commands](../../../docs/issue23-2p2d-rate-replay.md)
- `base/`: main scan analysis, per-request stages and bounded resource samples.
- `target16/`: paired memory control, including remaining-wait decomposition.
- `config.json` / `config-target16.json`: recorded deployment configurations.
- `calibration.json` / `c0-client.log`: workload rate reference and measured RDMA capacity.
- `execution-manifest.json`: source baseline, changed implementation files and runtime.
- `archive-record.json`: raw evidence bundle size and coverage.

Inputs are the fixed public trajectory revision documented above; model token
IDs and complete prompts can be reconstructed with `prepare_trajectories.py`.
The shared experiment root is
`/data/shared_upload/hust-zsx/issue23-2p2d-20261016`. Its
`first-scan-evidence.tar.gz` preserves configs, source overlay, native logs,
worker telemetry, full before/after metrics and client events. That raw bundle
is retained on shared storage and locally; this Git subset contains the compact
measurements needed to inspect the reported aggregates. The raw archive path
is not a public download URL.

Reanalyze the original run directory with:

```bash
python scripts/issue23/analyze_rate_replay.py --root /path/to/experiment \
  --campaign campaign-03 --output /path/to/main-analysis --plot
python scripts/issue23/analyze_rate_replay.py --root /path/to/experiment \
  --campaign target16-confirmation --output /path/to/control-analysis --plot
```

The extra `bottleneck-detail.json` uses before/after metric deltas and 1 Hz
resource samples bounded to the control point. Its `worker_load_and_query`
entries count events, while `load_per_request` attributes shared load spans to
each request. A load span includes engine scheduling, restore and completion
observation; it is not a device-only DMA measurement.

This is a single-seed exploratory scan and one sequential memory control.
Whole-run throughput includes arrival and drain. No stable p99, compute
saturation or fabric congestion conclusion is claimed.
