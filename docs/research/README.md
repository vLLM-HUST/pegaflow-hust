# Current research snapshot — 2026-10-27

This branch preserves the current algorithm, CPU simulator, tests, analysis tools,
mathematical model and positive/negative research evidence. It is a development
snapshot, not a validated serving release. No experiment is running at this checkpoint.

## Start here

- [Research model and related work](tailguard-pcie.md)
- [Version and evidence registry](pcie-versions-20261026.json)
- [Simulation entry points and reproduction](../../experiments/issue23_strategies/README.md)
- [Continuous batching assumptions and input retrieval](../../experiments/issue23_strategies/CONTINUOUS-BATCHING.md)
- [Latest model investigation](staged-save-20261027.md)

The retained reference is TimedMemo-normal-910B2. Some candidates improve the
default CPU scenario but regress under other rates or the unmeasured decode-sharing
sensitivity. None of the new parallel candidates has been validated on NPUs.
The latest completed ready-prefill comparison improves default high-rate p99 by
23.01%, but regresses at low/middle rates by 7.73%/6.36%; all three sharing-1
sensitivity rates regress. It is not promoted. Historical results remain versioned.

## Current incomplete work: staged-save-publication-02

The native source allocates unpublished save buffers before publishing cache names.
The prior simulator inserted each block immediately and could evict a just-created
prefix during the same save. The opt-in staged world distinguishes published,
unpublished and lease-retained physical ownership; allocation failure is explicit.

The frozen staged-save-publication-01 full-cost matrix failed. Its full failed-output
archive is retained (SHA256 29517f5f482499d4b717872b6b0b2d0ffecbeab543cbd176db55155659d9ea1d).
There are no complete new three-rate p99 results. A custom exception also exposed
a multiprocessing error-reporting problem; the mutable runner now propagates a
built-in exception and writes results atomically.

The current opt-in prototype additionally fixes committed copies remaining virtual,
releases host restore leases after H2D while retaining D device/slot occupancy, and
publishes completed D blocks incrementally in execution and forecast paths. These
are source-derived model corrections, not measured hardware improvements.
Sequential native-control diagnostics progressed from 35 to 58 to 67 to 89 completed
requests before explicit allocation failures; they are mechanism probes, not
comparable policy performance runs. The latest failure is dna-insert:15 on P0:
1,321,205,760 protected bytes plus 822,083,584 bytes of rolled-back staging.

Outstanding work before a new formal matrix:

1. Model incremental P save/publication and restore/copy lease lifetimes consistently
   in execution and forecasts. P currently still saves its full request at API end.
2. Account for all new callback bookkeeping CPU without double-counting nested saves.
3. Update check_staged_integration.py and audit_staged_save.py: their old assumption
   of exactly one P and one D publication per request is stale after D chunk publication.
4. Add focused delta-publication/lease/progress tests, then freeze a new source and
   evaluate reference and candidate under the SAME corrected world. Do not conceal
   allocation failures or compare old-world/new-world p99 as algorithm improvement.

The staged world is opt-in (`--staged-save-world`); the retained default is unchanged.
At this snapshot, the current nine staged-save unit contracts pass locally. Earlier
integration checks belong to frozen version01 and do not certify this prototype.
No new full matrix or real-device qualification is claimed.

## Fixed experiment contract

95 requests / 8 natural causal sessions, seed20261014; rates
0.24516170769071072, 0.49032341538142143, 0.9806468307628429 requests/s.
2P2D, TP1 Qwen2.5-7B-Instruct BF16, 32K, eager, 128-token blocks/prefill,
P generates/discards one token and D generates128. D simulation has up to4 sequences.
910B2 device KV budget is39,379,271,680 bytes per instance; host pool is separately2GiB.
Original isolated32-request/64-stage calibration and own completed observations only.
Decode-sharing0 is the default; sharing1 is an unmeasured sensitivity boundary.
Report conventional causal-ready-to-first TTFT including all control, migration,
H2D, synchronous save, queue and release costs; retain nominal latency/full drain.
With95 samples, nearest-rank p99 is the maximum, not a stable population estimate.

Only source, compact evidence and reproducibility metadata are in Git. Models,
environments and large full raw archives remain external, identified by SHA in the
registry and evidence receipts. Do not download a model or replace cost data to run
the CPU simulator. Preserve failures and all repeated measurements.
