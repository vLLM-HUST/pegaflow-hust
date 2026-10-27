# Staged save model — 2026-10-27

## Evidence and scope

Parent evidence: save-publication-20261027.md and ready-prefill-20261027.md FINAL.
The installed wheel direct_url identifies build04 SHA b7f9c3f968e0a34154a04d97b30f4786e51eff60a529c03c1d46cdbc382616ec. Its build script uses source-fragmented-01; the retained offload.rs is byte-identical to current/P source (d41f5946899c905646aa13023f6a612c06b3d5ad749093f92d7a3a174a6e3588). Retained storage SHA ce43fa4a6d1e0df7254c7afbcbff713cb2a7fa9528e0073a6210c2901f0347de, service e02998a9fa614c21e78e2a7a3bd1c2ac0a6a4f8f3f5dd7475e359aa82b707832, installed connector065e9ac679c93905c3075ad0f254c5ebcb4c92ee2644c22275c482cc280ebb30. This links historical build provenance, not a newly executed native binary.

Allocation errors return before RawSaveBatch publication; local Arc holders drop, while earlier read-cache evictions are not rolled back. SYNC_SAVE propagates the error, and the installed connector records failure and raises at its visible-save barrier. Thus silent partial suffix success is wrong. Native read-cache eviction can remove names while references keep physical bytes alive; allocator reclamation is not equivalent to logical eviction.

## Opt-in world revision staged-save-publication-01

Separate published names U, private save owners S and leased owners L. Physical capacity is charged once per owner in U union S union L. Save hash filtering is a single preallocation snapshot; filtered old hits evicted later are NOT recopied. New owners remain private throughout allocation; publish only after complete reservation, represented at the synchronous stage boundary under its exclusive device gate. Failure frees S, preserves prior logical evictions and held L, emits a failure receipt and invalidates the incomplete simulation. No suffix publication or success metrics.

Reclamation removes published owner closures up to the unchanged512logical-block batch. A leased owner becomes an empty-key tombstone, still charged, until its last lease releases. This preserves the existing whole-owner and virtual-copy representation; it does NOT implement per-layer stride fragmentation, arbitrary native insert-worker concurrency, or claim an exact native allocator. Intrinsic protected owners stay charged. The model retains original API-long lease duration assumptions and actual parallel serving remains unvalidated.

Same opt-in factory in physical P and D worlds and every cache-evolving forecast, including shadow snapshots. Memo keys include revision plus complete state; cached failure reproduces rollback. Actual save model CPU is additionally measured/charged once, forecast cost is already in planning. Full H2D/compute/decode/save/copy/queue costs remain. Actual publication and reclaim receipts contain request/time/owner/key lineage and capacity peaks. Old frozen results are untouched.

## Evaluation

Thirteen small tests cover fitting save, oversized failure, lease-retained shared owner, one-time hash filtering, memo failure/success and rounding, plus retained arbitration tests. One original request through native/Prefix02/ready-prefill confirms both P/D save and selected forecast integration, explicitly not a performance run. All pass locally; P checks precede launch.

New full-cost matrix compares unchanged ready-prefill routing/arbitration policy against same-round Prefix02, both using this SAME corrected world, plus the same native control. Three original rates,95requests/8sessions/seed20261014,original32/64calibration,2GiBhost/39379271680BdeviceKV,D4slots. Defaultsharing0 first; sharing1 is only an unmeasured sensitivity, gated on successful default completion/audits. No task/rate tuning. New-world versus old-world cache change is model sensitivity, not policy gain. Any allocation failure must be diagnosed from partial lineage rather than hidden or automatically rerun.

Next: audit all results/activation/capacity/work/source, report same-world net p99 with complete controller overhead and per-request queues/cache evolution. If failures, inspect first failing owner/allocation and retained native geometry, preserve evidence, then derive a targeted correction. If mixed, identify useful and harmful interventions; do not stop at equivalence.


## Snapshot update: version01 failed; version02 incomplete

The description above records the frozen version01 plan. The current status,
failed full-cost run, version02 changes and remaining work are documented in
[the current snapshot](README.md). No complete staged-world p99 is available.
