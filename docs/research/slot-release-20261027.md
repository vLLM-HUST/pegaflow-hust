> Current continuation: [forecast-feedback-20261027.md](forecast-feedback-20261027.md). The hourly loop is active; the older completed matrices below remain immutable.

# Known-slot release scheduling

Status: preserve a promising **default-world CPU TTFT branch**, not a robust
sharing-independent or production winner. Five three-rate matrices completed;
all sources/results remain independent. Parent `pending-first-compute-01` and
retained default `TimedMemo-normal-910B2` are preserved. That older checkpoint is complete; hourly research is ACTIVE and the current running candidate is recorded in the continuation link above.

Hypothesis: the parent's first-token priority still leaves new P-completed
requests outside D while all four holders decode. One measured simulated tail
waited 12.681 s before D control, whose own interval was only 0.503 s. Advancing
one holder's complete release may reduce that specific admission wait. This is
not an RDMA-congestion hypothesis or a reason to force slow cache operations.

Candidate `slot-release-first-01` chooses minimum predicted remaining decode plus
synchronous finish work only with a nonempty external D FIFO and four admitted
post-first decoders. Other states retain pending-first scheduling. All physical
work, full outputs, memory/slot reservations, exclusive phases, leases and final
drain remain. Route forecasts use the same rule. The selection reads no simulated
future duration; actual routing, model and scheduling CPU remain charged.

The explicit world is unchanged: 95 requests / eight causal sessions, the three
existing nominal Poisson arrays and seed, normal 910B2 KV scenario, 2 GiB host
pool per instance, four D sequences and zero assumed batch-sharing speedup.
Costs start from the original isolated calibration and learn completed same-arm
observations only. The real worker is unchanged and still lacks a qualified
parallel deployment. Frozen source SHA256:
`f4d68a5ff0e703096c3c1bbe763d31c62adfd6f6a17942b52634e97a21a34cbf`.

Fourteen quick contract checks passed, including fixed-work release improvement,
no future world cost in the selection, unchanged no-pressure behavior,
nonpreemptive restore/save and forecast coalescing versus literal replay.

## Reproduction

```bash
PYTHONPATH=python:experiments/issue23_strategies \
python experiments/issue23_strategies/run_pcie_matrix.py \
  --repo "$SOURCE_ROOT" --inputs "$INPUTS" --seed "$CALIBRATION_SEED" \
  --output "$FRESH_OUTPUT" --policy slot-release --workers 3 \
  --residual-world --phase-residual-world
```

Source: `/root/issue23-slot-release-source-01`.
First result: `/root/issue23-slot-release-01`, bounded to 1800 seconds.
Independent work conservation, source/stream accounting and release-decision
audits must precede any result claim. Preserve losses and completion regressions.

## Release-only results against pending-first

All costs include measured planning/control/model/iteration CPU, physical phases,
full output and final drain. These are CPU simulations, not measured NPU gains.
Do not compare arms across rows or choose the best run for each rate.

| Rate | First-order parent → release p99 (s) | Reduction | Reverse-order parent → release p99 (s) | Reduction |
|---:|---:|---:|---:|---:|
| 0.24516 | 22.027 → 17.530 | 20.42% | 24.572 → 18.151 | 26.13% |
| 0.49032 | 19.157 → 17.387 | 9.24% | 24.015 → 18.138 | 24.47% |
| 0.98065 | 22.427 → 17.938 | 20.01% | 21.910 → 19.629 | 10.41% |

In the first comparison the rule selected 48/38/34 distinct holders, with
2718/2205/1787 single-token release decisions. All were actual queued-waiter
states with four already-first holders. Independent reconstruction from original
calibration and completed same-arm observations reproduced every release rank.
No future sampled execution duration entered selection.

**The mechanism did not make all admission waiting smaller.** Mean slot wait
increased 0.845→2.022 / 0.713→1.718 / 0.429→1.231 seconds. First-order
ready-to-full p99 increased 50.016→60.247 / 54.345→60.483 / 51.816→56.378 s.
First-order nominal-origin p99 increased in all three cases; reverse-order
nominal results were also mixed. These outcomes stay in the report because
conventional TTFT can improve while full/session latency worsens.

Routing, acceptance and interleaving changed along with release order. In the
first high-rate comparison, `distribution-search:10` changed D1→D0, still with
zero D prefix; its D API-to-first fell 19.205→0.680 s. Both its own modeled
compute and overlapping other work changed. This is **not** an isolated causal
proof that a slot policy alone saved those 18.5 seconds, nor evidence of RDMA
congestion or superior real D0 hardware. The fixed-work unit experiment separately
establishes that local release can be advanced without deleting decode work.

## Strong-sharing sensitivity exposed a limitation

Only the explicit scenario coefficient changed: decode work is the maximum
rather than the sum of participating singleton contributions. The value 1 is
an optimistic boundary, not an estimated 910B2 parameter. Every arm uses the
same value; no arrival, capacity, input, seed or algorithm gain parameter changed.

| Rate | Pending-first → release-only p99 (s) | Pending-first → co-batch follow-up p99 (s) |
|---:|---:|---:|
| 0.24516 | 16.103 → 22.233 | 16.181 → 15.405 |
| 0.49032 | 14.641 → 17.037 | 14.459 → 18.814 |
| 0.98065 | 17.333 → 14.289 | 18.048 → 18.805 |

The follow-up `release-preserving-cobatch-01` admits other decodes only when
their predicted joint iteration does not extend the selected release predecessor.
Its shared-cost equation and assumptions are maintained in `tailguard-pcie.md`.
It is not promoted: it improved low-rate p99 but regressed middle/high in this
sensitivity. Its default zero-sharing comparison is recorded separately in the
evidence bundle; do not substitute it for these failures.

The follow-up's separate default-world p99 was 21.489→18.531,
26.405→18.436 and 28.870→16.613 seconds against pending-first. No companions
were admitted in that world, as required by the positive-cost sum model. It
therefore preserves the release-only mechanism there, but this additional
favorable row does not erase the strong-sharing regressions.

The mean-versus-maximum hypothesis was tested rather than presumed. Observed
simulated companion stretch totaled only 0.011 / 0.119 / 0.093 device-seconds,
so it does not by itself explain the multi-second tail changes. These sums are
not additive TTFT penalties. In the middle-rate tail `headless-terminal:13`,
both arms chose P1/D0, but accepted P prefix changed 10752→0. P API increased
3.150→8.077 s and P→D gap 3.740→9.618 s, while before-P decreased
4.925→0.752 s; same-request TTFT increased 12.353→18.814 s. The original
route already commanded zero P prefix in the candidate, so this is not evidence
that an accepted checkpoint silently disappeared during execution.

Middle-rate total routing CPU also rose 50.899→66.383 s and boundary CPU
30.911→35.290 s. This supports investigating **control-cost, route and cache
feedback together**. It does not isolate CPU as the sole cause: changed states
also change the amount of planning. No new balancing quota or H2D penalty was
introduced to force a favorable result.

## Preserved artifacts and next evidence

- Source01: release-only, SHA above; source02 driver adds explicit same-arm
  sharing sensitivity, SHA `3fbf11672615b71e07a3585b7af8dea20ac299f50fc5abad53f4a21ba857bacb`.
- Co-batch source01 SHA `40bd4bda8282b4d0917d3f5b8d2a17c7a3f91f591ae214f7d765a0fc8265b336`.
- Full results remain in distinct P `/root/issue23-slot-release-{01,02}`,
  `/root/issue23-slot-release-sharing-01`, `/root/issue23-release-cobatch-sharing-01`
  and `/root/issue23-release-cobatch-01`, with full archives and local SHA-verified copies.
- Compact reports and archive hashes: `evidence/slot-release-20261027/`.
  `analyze_slot_release.py` separates slot/control/gate waiting;
  `audit_release_estimates.py` rebuilds ranking and co-batching from past-only costs;
  `diagnose_release_cobatch.py` uses world labels **offline only** to test the
  companion-stretch hypothesis. `analyze_pending_first.py` checks full work.

Next model work should measure/diagnose the cost of forecasting and its effect on
cache/queue interleaving, while preserving the default-world TTFT gain. A real
parallel replay must measure batch-sharing behavior before either limiting CPU
scenario is treated as deployment evidence. Ninety-five exposed requests imply
nearest-rank p99=max; these matrices do not establish significance or new-workload
generalization. No new runtime, GPU job, timer, commit or push was made.


Latest 2026-10-27 continuation: the slot-release matrices above are historical.
Read the final section of `forecast-feedback-20261027.md` and the version registry.
Yielding02 completed24 audited CPU arms with mixed gains. Frozen
prefill-source-revalidation01 is now running a paired three-rate default0 then
sharing1 matrix; inspect `/root/issue23-prefill-revalidation-pipeline-01/result.json`
before any new launch. Hourly research remains ACTIVE; retained default unchanged.

Later checkpoint: that P-revalidation pipeline has now completed18 audited arms
and both full archives are SHA verified. No active job. Its mixed/negative result
and a reproducible coalescing-roundoff defect are documented in the FINAL section
of forecast-feedback-20261027.md; next work is numerical/control-feedback diagnosis.
Do not duplicate the finished pipeline. Hourly research remains ACTIVE.

Newer continuation: numerical-calendar-20261027.md records the opt-in exact
dyadic calendar fix and its independent diagnostic run. Read that checkpoint and
the version registry before launching anything; the old slot-release work above
remains historical evidence, not current activity.

Latest continuation: numerical-choice05 completed fixed-state profiling and
found repeated forecast reclaim deletion, not exact-time conversion, as a
measured hotspot. See `linear-reclaim-20261027.md` and the version registry.
The new equivalent controller implementation is in a frozen, bounded diagnostic
matrix; inspect its actual process/result before any launch. Old jobs above
are completed. No candidate promotion; hourly research remains ACTIVE.

Newest: linear01 charged tests are complete and mixed, with full audit/archive.
The eligible-prefix follow-up is source02 with repaired launcher03; read
`prefix-reclaim-20261027.md` and registry for the active pipeline. No old slot-
release, exact-calendar or linear01 matrix remains active. Do not repeat them.

Latest checkpoint: prefix-reclaim-20261027.md FINAL has full-cost mixed outcomes;
no-decode-advice-20261027.md and pcie-versions active_experiment now track the
new same-parent optional advisory ablation. Do not restart completed prefix03.
