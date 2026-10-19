# Unified strategy contract and evidence-first iteration

The authoritative source repository is `/root/pegaflow-hust` on yanbo-81.
The repository was fast-forwarded from ac1b2d3 to c961995; existing experiment
directories and the frozen serving environments were not replaced. All new
work is source-only until its executor integration is verified.

## Comparison order

1. Audit the completed phase-release B / parent / revised / B feedback.
2. Compare Tailguard against **vLLM + PegaFlow default cache behavior** under
   the same four-worker frontend, workload, capacity and output configuration.
3. Only after evidence of an all-cost p99 advantage, enable the related-work
   matrix. Do not claim that an adapter is an entire published system.

`python/pegaflow/strategies` contains a common immutable snapshot, candidate,
forecast and decision interface. A real executor must recheck generation,
leases, complete published prefix and staging capacity. Selector output is not
an action receipt. Layout actions may include admit/copy/exchange/move/remove;
no-op is valid, and no action is forced to preserve a fast/slow architecture.

## Strategy identities

- `vllm-pegaflow-native`: deterministic round-robin frontend only. No cost-based
  boundary search, inventory-driven layout operation, or Tailguard predictor.
  It emits **no per-request prefix cap/local-only override**. Existing legacy
  B arms do not satisfy this definition: their controller selected a boundary.
  The current legacy worker rejects this new mode; real matrix launch must
  fail closed until worker/runner integration explicitly supports it.
- `memserve-eq12-adapter`: implements the Eq1 queue-plus-prefill comparison and
  Eq2 saved-execution-versus-transfer test from
  [MemServe](https://arxiv.org/html/2406.17565#S5.SS3.SSS1). The execution/copy
  model is shared measured PegaFlow cost; the D selector is explicit least-work.
  This is not a reproduction of MemPool or MemServe's operator profiler.
- `lmetric_score`: implements only the multiplication kernel from
  [LMetric](https://arxiv.org/html/2603.15202v1#S5). Inputs must be the declared
  queued/new prefill-token and real engine batch indicators. A full PD adapter
  and hotspot detector are not implemented or registered; substituting FIFO
  depth for batch size would not faithfully reproduce the paper.
- `tailguard-pending-release-v1`: uses a resource-calendar forecast containing
  every pending-first request, including control and migration. It minimizes
  the descending pending TTFT profile, then full-release profile. Already-first
  requests retain full decode/save/resource occupancy but no longer provide an
  immutable lower bound that suppresses all useful first-token improvements.
  This is a new candidate, not an already measured winner.

## Model and timing boundaries

Conventional TTFT is client first-token time minus causal-ready admission.
Nominal offered latency and dependency delay remain separate diagnostics.
The resource forecast must include source/destination acquisition, actual
complete copy RPC, H2D/restore, pre-first service and necessary save/sync,
full decode and resource release. Never add nested native H2D/save totals again
to an API duration already containing them. Do not presume RDMA congestion.

The pending objective is still an online surrogate for the final full-run p99.
With only eight sessions, empirical live p99 is max; descending profiles avoid
an arbitrary tie weight but do not prove an unknown-future optimal policy.
Immutable source generations and actual receipts remain mandatory.

## Iteration contract

For each frozen candidate: preserve source and seed -> three-rate causal CPU
screen -> promising real native-B/T/B -> full archive and exact-output audit ->
align request/stage/queue/action behavior -> identify a falsifiable model gap ->
next independent candidate. Keep losses and partial successes. No task/rate
exceptions, manually fitted gain thresholds, future service-time oracle, or
reruns of unchanged failures until success. Simulated results do not establish
real performance, and a single exposed 95-request p99=max does not establish
stable significance. Calibrated costs and cache evolution must be tested against
the real feedback, not only replayed observed routing.

D is queued as of this implementation. No new four-card experiment or model
occupancy process was launched, and the paused automation was not resumed.
