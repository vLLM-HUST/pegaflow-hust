# Shared native-cache baseline and causal resource feedback

## Research scope

Tail latency in causal, naturally prefix-sharing PD-disaggregated agent replay
with finite host KV capacity. The observed accelerator restore, synchronous
save/publication and full-decode resource-release paths can couple requests.
RDMA bandwidth congestion is not a required premise. Four single-card TP1
Qwen2.5-7B-Instruct BF16 replicas form2P2D; 95 exposed requests/eight sessions,
128 output tokens on D, context32K, cache/prefill128,2GiB host plus2GiB NPU KV
per replica. Same three rates and seed20261014; no task/rate special cases.

The native baseline is an explicit round-robin frontend with default PegaFlow
lookup, native implicit fetch and native replacement, without our restore cap,
cost-based boundary, explicit per-request migration or slow layout planner.
Both policies retain the existing common host-authority fixture (opaque GPU
prefix hits disabled) and the same underlying vLLM/native implementation.
This isolates shared-stack policy behavior, not the performance of untouched
production vLLM, pure vLLM without PegaFlow, or full Mooncake/MemServe systems.

## Objective and resources

a_r is nominal offered time, c_r=max(a_r,e_previous_session), f_r is the observed
client first-token event and e_r is full completion. Optimize Q_.99(f_r-c_r);
report f_r-a_r and c_r-a_r separately. Intermediate P_end/D_first/D_full are
predictions needed by the resource DAG, not three final outputs.

An operation v starts only after its predecessors and required resources:
s_v >= max(c_r, max(e_u : u in pred(v))), e_v=s_v+d_v.
P/D executor FIFO, actual P-completion D enqueue ordering, source/destination
software gates, physical allocation ownership, staging and leases constrain
feasibility. D remains occupied after first through decode/save/release.
Observed-first jobs remain holders but leave the mutable pending-first target.

For a first-path duration, use distinct restore-path and scheduled-prefill/save
terms, plus API/frontend control. Full D duration adds the observed wire tail
(wire_complete-wire_first), including synchronous finish work. Native H2D-task
timers include descriptors/backend/synchronization and nest in restore/API;
do not add them to an enclosing duration again or label scheduler gaps pure DMA.
Completed-only physical fits use bytes, uncomputed tokens, attention-pair work,
prefill chunks and decode context lengths. No tuned window or congestion tax.

Live layout candidates still use complete packages, native generation/lease/
staging validation and truthful receipts. No cleanup-all or residency reward.
The first comparison retains slow=off, matching the promising preserved parent;
it compares joint routing/source/boundary decisions, not forced cache actions.
Full optional admit/copy/exchange/move/remove parent code remains available.

## CPU screening and limitations

The new simulator feeds changing routes back into physical cache projection,
four executor queues, gate waits, admission and next-session readiness. Policy
sees only current admitted input and completed own observations. World phase
costs/residuals use the original isolated32, not per-request future test service
times. Current-prompt hashes are deterministic known input; generated output
is space-only and is never supplied as a future reusable prefix.

Routing/boundary CPU is measured from the same live solver. Online fitting CPU
advances a conservative serialized-controller clock; native does not pay our
learner cost. RPC overhead is an aggregate of380 previously completed requests,
not a per-task future oracle. Copy RPC cost and full D decode/save occupancy
remain in simulated latency. The actual implementation's direction software
mutex acquires after device gates and is modeled as hold-and-wait; this is NOT
evidence of physical RDMA congestion. Active partial-resource ownership remains
a model limitation to investigate if link waiting becomes material.

| Nominal rate | Mean-world B p99 | Mean-world T p99 | Residual-world B p99 | Residual-world T p99 |
| --- | ---: | ---: | ---: | ---: |
| .2451617077 |100.7227|34.0595|100.4591|34.4802|
| .4903234154 |87.7721|34.5451|86.5491|34.6706|
| .9806468308 |93.0495|35.8564|98.4178|34.8661|

Seconds, nearest-rank n95 p99=max. This demonstrates model-conditional potential,
not measured gains, stable population p99 or significance. Source-choice order,
incremental native publication and unobserved hardware/allocator interference
still differ from a full hardware emulator. Only real comparison can verify
whether the apparent benefit survives those mismatches and actual overhead.

Earlier fixed-route diagnostics and generic-solver screens are preserved and
not used as proof of this same-solver result. Every candidate/result is retained.
The real source is frozen separately, with strict same-initial-state warmup,
same allocation/source checks,95x128 exact output gate, per-worker independent
records, source-native logs, physical receipts and counter reconciliation.
The first real matrix contains only naive B and our T. Other baselines wait.

## Related-work sufficiency

MemServe and Mooncake are useful first strongly related strategies but not an
exhaustive evaluation. Before a paper claim add simple least-work/cache-affinity
references and ablations for the measured bottlenecks (restore-aware boundary,
full-decode release, source gates, learned service and planner cost). LMetric's
colocated multiplication kernel is not silently called a full PD baseline.
Full-system comparisons require their actual transports/operators, not merely
renaming strategy adapters. No additional baseline experiment is started now.
