# Qwen3.5 hybrid-cache four-arm experiment

The Issue #23 experiment compares `rdma-burst`, `rdma-smooth`,
`local_copy-burst`, and `local_copy-smooth` through the P/D router. A request's
attention group and three recurrent storage groups form one transfer bundle,
covering both physical TP ranks. A cohort contains four complete bundles.
`Tg` is the latest bundle eligibility time in the cohort. Burst submits all
four at `Tg + 15 ms`; smooth submits slots 0–3 at `Tg + 0/10/20/30 ms`.
Both use the container's default scheduler, `SCHED_OTHER`, priority 0.

## Frozen inputs and runtime

The first cross-node round uses node 81 for prefill and node 91 for decode,
with two Ascend 910B2 NPUs per node and one PegaEngine per node owning logical
devices 0 and 1. Each engine therefore applies one gate to the full TP bundle.
The model is `/data/shared_upload/models/vspec/Qwen3.5-35B-A3B`, revision
`59d61f3ce65a6d9863b86d2e96597125219dc754`, BF16, TP=2, eager execution and
synchronous scheduling. The vLLM runtime is 0.23.0 with vLLM-Ascend 0.23.0,
torch 2.10.0 and torch-npu 2.10.0.post4.

`prepare_four_arm.py` freezes four MMLU-Pro and four FrontierScience requests
from the prepared benchmark manifests. The input token IDs and benchmark
questions are preserved. Each arm uses the same request IDs, cache salts,
arrival trace and output budget of 64 tokens, with EOS ignored. There are two
cohorts, 20 ms between arrivals within a cohort and 8 seconds between cohorts.
The maximum context is 8192 tokens, maximum scheduled tokens 4096, maximum
sequences 8, device KV budget 4 GiB per NPU and host pool 4 GiB per node.

The preparation sequence produces each prefix on P and restores it on D.
The captured schema-2 plan freezes namespaces, groups, hashes, slots, segments,
bytes and QP assignments. Before measurement, P pins the source objects and D
creates its hidden local snapshot, clears its visible host/device caches and
activates the gate. New P publications cannot replace the pinned RDMA source.
Local-copy operations read the corresponding hidden snapshot on D. Each arm
starts fresh model/engine processes and repeats the same preparation sequence.

## Hybrid transfer coordination

Experimental D scheduling starts the attention query and all three recurrent
checkpoint queries while the attention query is still loading. Native fetches
are matched by length-prefixed HMA request identity, namespace and storage
group. The gate joins every planned group, prepares one combined RDMA batch
and releases all group waiters after full completion. Missing state or manifest
drift fails the arm with explicit evidence.

The connector prefetch behavior is opt-in through
`PEGAFLOW_ISSUE23_HYBRID_PREFETCH=1`. P uses ordinary `save_only` publication.
`POST /issue23/freeze-source` pins its prepared source objects, and
`POST /issue23/activate` activates D's hidden snapshot.

## Telemetry and interpretation

| Layer | Recorded evidence |
|---|---|
| Client | Intended and actual arrival times, HTTP headers, raw SSE, first text/token IDs, TTFT, inter-event times, final usage, E2E and output hashes |
| Connector | Per-group query/ready/load/save spans, checkpoint boundaries, actual TP rank and visibility acknowledgements |
| Native engine | Per-group discovery, remote query, allocation/descriptors, complete bundle assembly, eligibility, posting and completion, payload bytes and QP manifest |
| Local copy | Actual CPU copy start/end/duration, independently of completion observation |
| Model | Scheduled and computed tokens per request and TP rank, host execute-model return spans and explicit step errors |
| Resources | 1 Hz cgroup CPU throttling, memory/pressure, process statistics, network/RoCE counters, engine/model Prometheus metrics, NPU snapshots every fifth sample |
| Provenance | Frozen cases/contract/plan hashes, process argv/PIDs, native binary SHA-256 and phase/status files |

Native monotonic clocks are analyzed per engine. `bundle_posted` timestamps the
return from submission; `submit_ns` identifies its preceding host submission
span. Local copying may begin during that span. Model spans measure the host
call's return and add no device synchronization. RoCE/network counters may
include traffic from other containers on the physical host; sampling overhead
is recorded. Fine gate timing comes from native events, not 1 Hz sampling.

Arm validity checks request/group coverage, bytes, frozen manifests, timing
order and burst submission spread ≤1 ms. Numerical output differences remain
observations. Restoring the wrong state, missing a required group or explicit
model/transfer failures require investigation. Benchmark answer quality is
supplementary because the fixed short output may truncate reasoning.

## Evidence and execution

Shared root: `/data/shared_upload/hust-zsx/issue23-four-arm-20261014`.
`runtime/two-node.json` supplies node-local commands and
`runtime/deployment.json` supplies the original runner's P/D endpoints.
Authenticated node agents own the recorded process groups; teardown verifies
their identities before stopping them. The private control token stays outside
the result archive.

```bash
PY=/data/shared_upload/hust-zsx/hybrid-cache-acceptance-20261013/.venv/bin/python
ROOT=/data/shared_upload/hust-zsx/issue23-four-arm-20261014
RUNNER=$ROOT/source/scripts/issue23/runner.py
$PY "$RUNNER" --root "$ROOT" --deployment "$ROOT/runtime/deployment.json" \
  --mode capture --arm rdma-burst --label crossnode-capture-01
$PY "$RUNNER" --root "$ROOT" --deployment "$ROOT/runtime/deployment.json" \
  --mode matrix --label crossnode-pilot-01
```

Preparation verification passed 51 experiment-tool tests, 51 connector/TP state
tests, three checkpoint tests, twelve native gate tests and one frozen-source
test. The Ascend/RDMA release binaries built successfully. Node 81's sequential
TP=2 functional test qualified RDMA-burst and local-copy-burst with 4/4 successful
requests per arm and all four groups per request. Its frozen source held 20
objects and 593,100,800 bytes. The cross-node RDMA probe completed 100 reads of
1 MiB before starting the P/D capture.

The initial matrix has eight measured requests per arm. Its raw samples and
gate/resource evidence support a first performance diagnosis; robust p99 claims
need more requests, balanced repeated rounds and explicit warmup control.
