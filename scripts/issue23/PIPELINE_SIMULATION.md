# Asynchronous P/D pipeline model

`pd_pipeline_simulator.py` replays requests through independently scheduled P and D
engines. It models CPU cache publication, discovery and network prefetch, NPU KV
restoration, scheduler completion feedback, and continuous batched inference.

Run with a measured component profile and a workload containing tokenized prefix
hashes and explicit session dependencies:

```sh
python scripts/issue23/pd_pipeline_simulator.py \
  --profile profile.json --workload workload.json \
  --config configuration.json --output prediction.json
```

The output includes per-request stages, compute iterations, restore cohorts,
resource occupancy, summary percentiles, and calibration coverage warnings.

## Cache publication and restoration

P engine completion and CPU KV publication are distinct events. A
`P_publish_model` profile supplies a post-response publication curve and a
baseline acknowledgment delay. `source_publish_gate=true` waits for publication
before submitting D; otherwise D discovery can wait for the source data.
Post-response publication curves describe remaining work, so they must not
include the entire D2H service that already overlapped prefill.

Pending restoration reserves destination pages and pins an existing NPU prefix.
New cache keys become reusable only when copying completes. Scheduler observation
can follow completion at a later engine iteration. References transfer from the
pending reservation to the running request at admission.

## Deployment resource inputs

Optional configuration fields extend the basic P/D counts, token budgets,
concurrency, cache capacities, routing and scheduling settings:

| Field | Meaning |
|---|---|
| `frontend_serialization` | Serialize measured inbound processing per engine |
| `source_publish_gate` | Wait for committed source KV before D submission |
| `network_sharing` | `fifo` or max-min `processor_sharing` across NIC endpoints |
| `host_copy_GBps` | Copy capacity keyed by host |
| `device_copy_GBps` | Copy capacity keyed by stringified engine index |
| `device_restore_curves` | Per-engine points with `blocks` and `seconds` |
| `device_publish_curves` | Per-engine remaining-publication points |

Engine indices place all P engines first, followed by D engines. Device-specific
capabilities take precedence over shared host defaults. Resource inputs describe
measured deployment properties rather than request identities.

The network model shares source outgoing and destination incoming capacities.
It does not model packet-level queues, ECN or PFC. Copy/compute interaction and
native runtime interference require measured component calibration.

## Calibration

`fit_pipeline_profile.py` accepts multiple normalized evidence files. Compute
samples are balanced by experimental condition; validation leaves whole
deployment phases or context bands out. Copy model selection uses dimensionless
ratios to independently supplied deployment capabilities. Isolated geometry
probes provide those capabilities and are excluded from coefficient scoring.

Freeze the model and structure choices before evaluating held-out request timing.
Report request-paired errors separately from compute-step errors. Repeated runs
of a history improve repeatability estimates but are not independent workloads.
Small-sample p99 values and deployment transfer with state-dependent copy tails
need explicit uncertainty in the experimental report.
