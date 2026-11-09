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

## KV page geometry

A profile can declare the per-worker KV layout explicitly:

```json
{
  "kv_layout": {
    "block_tokens": 128,
    "cache_domain": "Qwen2.5-7B-Instruct:BF16",
    "dense_attention": {
      "layers_per_worker": 28,
      "kv_heads_per_worker": 4,
      "head_dim": 128,
      "dtype_bytes": 2
    }
  }
}
```

Dense attention pages include both K and V. These dimensions describe one worker;
the simulator does not infer KV-head sharding or replication from a TP count.
Alternatively, supply an explicit integer `bytes_per_block` and `cache_domain`
for a measured fixed-size page layout. This does not implement the separate
allocation and lifetime rules of recurrent or hybrid cache groups.

The layout controls CPU eviction capacity, NPU page capacity, recovered prefix
tokens, payload accounting and capacity-based copy service. Compute and empirical
service curves still require profiles for the requested model, precision and
deployment. Profiles without a layout retain the historical Qwen2.5 BF16 layout;
a profile identifying another model must supply its geometry.

`prepare_pd_workload.py --block-tokens 64` creates hashes and workload metadata for
64-token pages. The workload page size must match the profile. The historical
experiment acquisition scripts remain deployment-specific; normalize new
measurements with the corresponding layout before fitting another deployment.

## Explicit transfer paths

Set `network_sharing` to `processor_sharing` and supply `network_topology` to model
directed paths with shared bottlenecks. For example:

```json
{
  "network_sharing": "processor_sharing",
  "engine_hosts": ["81", "81", "91", "91"],
  "network_topology": {
    "links": {
      "81-tx": {"bandwidth_GBps": 12.5},
      "fabric": {"bandwidth_GBps": 10, "latency_us": 4},
      "91-rx": {"bandwidth_GBps": 12.5},
      "81-cpu-copy": {"bandwidth_GBps": 30}
    },
    "routes": [
      {"source": "81", "target": "91", "links": ["81-tx", "fabric", "91-rx"]},
      {"source": "81", "target": "81", "links": ["81-cpu-copy"], "transport": "cpu_copy"}
    ]
  }
}
```

The example capacities are illustrative. `bandwidth_GBps` uses decimal **bytes**
per second; `network_Gbps` in the legacy endpoint model uses decimal **bits** per
second. Explicit paths are authoritative and do not inherit a second NIC limit
from `network_Gbps`. Every used source/target host pair needs a route.

Reusing a link ID makes flows contend for the same capacity. Separate IDs model
independent paths or full-duplex directions. Max-min allocation is recomputed on
flow arrival and completion, including flows limited by other links. Route startup
latency precedes payload service; it is not counted as bandwidth occupancy. Results
record link paths, transport labels, startup latency and transferred bytes per link.

Same-host CPU-pool movement needs a CPU-copy path. Placement on one host alone
does not imply a GPU-to-GPU NVLink transfer. H2D restoration remains a separate
device service; this path model does not add shared CPU-memory/DMA arbitration
between H2D and RDMA, packet queues, QP scheduling or congestion-control protocols.

Architecture and source review: [related simulators](../../docs/experiments/simulator-related-work-20261109.md).

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
