# TailGuard in PegaFlow

TailGuard is the active research topic; PegaFlow is the software and repository.
The separate `pegaflow-tailguard` repository is an archived standalone prototype
and Phase 5–9 experiment controller, not a second production implementation.

The historical Python `TailGuardController` supplied a token bucket, in-flight
reservation, priority ordering, and proposed refill/migration/placement actions.
Only demand remote-read admission has a matching execution path today. PegaFlow
therefore ports that narrow, useful subset into its Rust storage path rather
than exposing the prototype's unimplemented actions as live capabilities.

For a request, PegaFlow first reuses its local CPU prefix. If a remote prefix is
discoverable, it obtains the exact remote bundle byte count from
`QueryBlocksForTransfer`, then calls `TailGuardRemoteReadController` **before**
destination allocation and RDMA submission. An admitted transfer holds a
byte-count permit through completion/error/cancellation; dropping it releases
the in-flight reservation. A denied transfer releases the source transfer lock,
does not submit RDMA, and treats the remaining prefix as missing so the engine
can compute it. The decision is retained for subsequent polls of that request.
Local CPU hits are not discarded. SSD fallback remains available.

This is an opt-in implementation seam, not the proposed TTFT-p99 scheduler.
Rate, burst, and in-flight limits are explicit experimental inputs; no network
capacity or queueing delay is inferred from them. There is no priority queue,
per-flow rate control, active KV copy/placement, or D-to-P history recovery
interface in this change. The default path is unchanged, and the TailGuard
limits cannot be combined with frozen Issue23 experiment mode.

The current P=`save_only`, D=`read_write` deployment does **not** acquire D's
historical KV on P. This hook also cannot yet label a remote read as optional
history recovery versus necessary P-to-D handoff. Do not enable it on the
current D handoff service: a denial there would affect required work. Before a
production scheduler uses this hook, the request path must identify optional
reads and verify coordinated outcomes across its TP ranks.

To enable this narrow policy on an isolated PegaFlow server, supply all three
flags together (values shown are examples, **not** calibrated limits):

```text
--tailguard-remote-read-rate 8gb \
--tailguard-remote-read-burst 2gb \
--tailguard-remote-read-max-inflight 2gb
```

Omit all three flags to disable it. A denial is a fetch/recompute choice, not a
capacity wait; operators must account for recomputation load on the serving
engine. Enabling it in a live P/D deployment requires a separate correctness
and performance test. No current formal experiment was rerun with these flags.
