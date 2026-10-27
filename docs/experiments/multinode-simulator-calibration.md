# Configurable P/D simulation

`scripts/issue23/pd_multinode_simulator.py` uses separate measured P/D step
costs, CPU work, CPU-to-NPU restore services, prefix identities and physical NIC
queues. The calibration deployment on 2026-10-27 used Qwen2.5-7B-Instruct BF16,
TP1, two producer cards on ascend81 and one consumer card on ascend91.

Experiment evidence and fitted profiles belong in the benchmark repository.
The simulator consumes a workload and a profile independently of measured
request latencies.

## Workload and arrival process

Convert tokenized JSONL histories into requests containing only IDs, session
identity, token counts, prefix hashes and arrival times:

```bash
python scripts/issue23/prepare_pd_workload.py \
  --requests requests.jsonl --split test --rps 2 --arrival poisson \
  --output workload.json
```

Each input needs `id`, `prompt_token_ids` and optionally `task`, `step_id`,
`cache_salt`, `output_tokens`. Explicit `prompt_tokens` and `hashes` can describe
a synthetic workload. Input tokenization must match the profile's model and
chat template. A custom arrival trace can be provided directly as `arrival_s`
in the workload JSON.

By default, requests in one session wait for that session's preceding request
to finish. Consequently, a nominal arrival rate can exceed the achieved rate.
Set `session_dependencies` to false to model independent open-loop requests.

## Scenario parameters

```json
{
  "P": 2,
  "D": 2,
  "P_routing": "session_affinity",
  "D_routing": "least_outstanding_tokens",
  "P_token_budget": 8192,
  "D_token_budget": 2048,
  "P_max_sequences": 128,
  "D_max_sequences": 32,
  "P_CPU_GiB": 8,
  "D_CPU_GiB": 16,
  "P_NPU_KV_GiB": 39,
  "D_NPU_KV_GiB": 40,
  "engine_hosts": ["P-host", "P-host", "D-host", "D-host"],
  "host_CPU_cores": {"P-host": 8, "D-host": 8},
  "host_memory_GBps": {"P-host": 15, "D-host": 13},
  "engine_policy": "fcfs",
  "handoff_policy": "full",
  "GPU_APC": true,
  "copy_compute_overlap": true,
  "session_dependencies": true,
  "seed": 20261027
}
```

This is a scenario configuration. Its 2P2D topology has not received the
end-to-end validation performed for the measured 2P1D deployment. CPU and memory
capacities should match the resources assigned to each physical host.

Run a scenario and optionally change arrival rate or output length:

```bash
python scripts/issue23/pd_multinode_simulator.py \
  --profile profile.json --workload workload.json --config scenario.json \
  --reroute --rps 4 --arrival poisson --output-tokens 256 \
  --output prediction.json
```

`--reroute` removes explicit P/D assignments in an existing workload so the
configured routing policy can select engines. Supported routing policies are
`session_affinity`, `round_robin`, seeded `random`, and
`least_outstanding_tokens`. Engine admission supports `fcfs` and
`shortest_remaining_tokens`; already running decode requests retain their
continuous-batching slots.

NIC capacity is shared by engines with the same physical host identity.
`network_Gbps` changes the aggregate payload capacity. `host_memory_GBps`
provides a shared memory-service budget for RDMA and restore traffic. Model
process CPU work shares the configured host core budget. Native copy-worker
CPU demand and exact CPU/NPU/DMA overlap remain approximations.

`handoff_policy: full` requires the producer's completed prefix to be available.
`best_effort` allows D to recompute an unavailable suffix. GPU APC can be
disabled independently of external prefix recovery. CPU cache uses eviction
order and transfer pins; NPU pages use reference-counted allocation. A workload
requiring uncalibrated NPU preemption stops with an explicit error.

## Calibration and interpretation

`prepare_multinode_profile.py` separates calibration conditions from validation
conditions. It derives schedule-to-update costs, model-process CPU time, native
restore service time excluding its queue, discovery overhead, actual NPU page
budgets, and a first-compute handoff audit. Request latency observations from
validation sessions are used only in the comparison stage.

The October 27 evidence contains 71 valid conditions and 509 requests. It
includes all 32 designated calibration inputs, cold/warm prefixes, fan-in
batches of 4/8/16, copy/decode overlap, resident-cache decode batches of
1/2/4/8/16, and two 95-request validation runs from separate sessions.

Mean E2E prediction errors were below 0.3%; request-level TTFT MAPE was about
16% and 28% at nominal 0.5 and 2 requests/s. These summarize different metrics:
matching mean E2E does not establish accurate request-level TTFT or tail timing.
At 2 requests/s, approximately +109 ms of mean TTFT error and -112 ms of
post-first-token generation error cancel in mean E2E. This calibration supports
trend studies with explicit uncertainty; it does not establish precise timing
for every request or topology.
With 95 requests, nearest-rank p99 is the maximum and needs more independent
observations for a stable tail estimate.

Prediction output includes `coverage` and counted compute-feature
extrapolations. Changing P/D counts or validation configuration is identified
explicitly. Model, hardware, TP and execution mode changes require a matching
profile; graph and eager costs are separate. Broader workloads and higher
contention can be explored as scenarios, with validation uncertainty attached.
This model describes aggregate service and queueing; packet-level RDMA
congestion control, switch buffers, ECN/PFC and retry dynamics need a separate
network model or measurements.

## Published evidence

[Calibration report and raw captures](https://github.com/vLLM-HUST/vllm-hust-benchmark/tree/dc5425891e708a5389773114489d0d7e0133fb4e/reports/pegaflow-multinode-calibration-20261027) preserve measured inputs, frozen profiles, predictions and validation limits.
