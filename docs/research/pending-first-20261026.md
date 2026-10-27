# Pending-first compute scheduling, 2026-10-26

Status: **promising CPU TTFT candidate, preserved for release-aware work and real
validation**. Parent `acquired-exclusive-phase-01` remains an unpromoted branch.
The default reference and installed serving worker are not replaced.

The preceding high-rate failure contained 9.958 seconds of other overlapping
compute while its cold D first token was predicted within 0.016 seconds. Earlier
age/laxity experiments reordered prefills but retained decode-first iteration
visitation. This experiment changes that specific order, without a new penalty.

For an idle D compute iteration, first allocate the fixed 128-token budget to
already admitted, restored, pending-first sequences in existing FIFO order.
Only leftover budget visits active decodes, one token each as before. Running
iterations cannot be preempted. Exclusive restore/save order, KV/slot admission,
leases, full 128-token output, publication and session-release dependencies are
unchanged. The prediction calendar uses exactly the same visitation rule as the
execution model. No sampled duration is read to make this scheduling choice.

With zero assumed batch speedup, iteration work is still the sum of allocated
prefill and decode service. The rule moves work rather than erasing it. For a
fixed admitted set it can improve first tokens by delaying already-first work;
this is **not** a global p99 theorem. Decode completion, future slot/session
release and inter-token latency can worsen. Closed-loop measurements must include
those effects, total completion time and the unchanged final drain. Do not claim
an ITL/throughput improvement or silently switch the objective after results.

This is a serving-scheduler candidate, not a claim that changing the router alone
changes native vLLM. The installed NPU worker stays unchanged. Evaluate the same
three-rate CPU matrix with all controller and phase costs charged; reverse paired
order if there is potential. Hardware parallel/batch timings remain unvalidated.

## All completed comparisons

Same 95 requests / 8 causal sessions, seed and three nominal arrival arrays;
same isolated calibration, normal 910B2 KV scenario, 2 GiB host pool, max four D
sequences, zero assumed compute-sharing speedup and joint phase-residual world.
Conventional TTFT is causal-ready to first. Measured route, boundary, iteration
and model CPU, recovery, copy, save, full decode and final drain are included.
No costs are subtracted, and asynchronous wall is not added a second time.

| requests/s | Parent → candidate p99, first order (s) | Reduction | Parent → candidate p99, reverse order (s) | Reduction |
|---:|---:|---:|---:|---:|
| 0.24516 | 32.221 → 19.893 | 38.26% | 33.175 → 19.120 | 42.37% |
| 0.49032 | 34.437 → 21.148 | 38.59% | 33.359 → 20.937 | 37.24% |
| 0.98065 | 31.962 → 20.721 | 35.17% | 32.726 → 27.527 | 15.89% |

An additional same-world comparison used retained `TimedMemo-normal-910B2`.
Only the matrix driver's reference selector changed; every candidate algorithm
source hash stayed identical. A win against a recently failed branch alone is
insufficient evidence against the retained starting point.

| requests/s | TimedMemo → candidate p50 (s) | p95 (s) | p99 (s) | p99 reduction |
|---:|---:|---:|---:|---:|
| 0.24516 | 14.553 → 6.846 | 32.465 → 17.497 | 44.513 → 29.593 | 33.52% |
| 0.49032 | 16.113 → 8.272 | 24.488 → 18.157 | 26.644 → 22.966 | 13.80% |
| 0.98065 | 14.856 → 6.959 | 26.729 → 20.356 | 32.731 → 21.846 | 33.26% |

These are three CPU comparisons, not NPU experiments. Do not pool their arms or
select the best run per rate. Candidate low-rate p99 itself ranged 19.120–29.593 s;
measured controller costs change route/cache/session interleaving. Ninety-five
exposed requests make nearest-rank p99 the maximum. Two arm orders and one
retained-reference comparison do not establish statistical significance, hardware
isolation, native-vLLM performance or generalization to new workloads.

## Mechanism and costs that moved elsewhere

In the first high-rate comparison, the parent's worst request
`feal-differential-cryptanalysis:12` had zero accepted D prefix in both arms.
Its D API-to-first fell 16.139→1.385 s; same-request TTFT fell 31.962→13.588 s.
This does not require more D hits. Routes, P work and competing jobs also change,
so the pair alone does not identify a pure scheduling causal effect. The fixed
two-job unit experiment separately verifies that visitation moves decode work
without deleting service or output.

The limitation is concrete. First middle-rate candidate
`fix-code-vulnerability:11` waited 12.681 s from P completion to D-control start.
Four D API holders already had first tokens; the earliest released after
12.676 s. Its control interval was only 0.503 s and control-end to API-start zero.
This is D-slot release blocking, not evidence of RDMA congestion. The first
low-rate tail similarly waited 7.528 s before control and 0.596 s inside control.

Full completion did not uniformly improve. First comparison makespan changed
−1.39% / +2.04% / +4.17%; reverse order +4.09% / +1.51% / +7.47%.
Mean post-first-to-full time rose about 6–7 s. Reverse high-rate ready-to-full
p99 rose 52.647→83.873 s; `git-multibranch:11` spent 65.578 s after first before
release, despite TTFT of 18.295 s. First comparison nominal-origin p99 worsened
118.492→120.769 / 291.182→296.387 / 386.658→407.216 s. Retain these outcomes;
do not relabel them as improvements or change the main metric after observing them.

The four-sequence constraint was already modeled. Priority changes the release
distribution: this is a scheduling tradeoff, not a newly missing capacity counter.
The next hypothesis should target delayed release when known ready requests are
slot-blocked, retaining this TTFT branch as a comparison. Avoid another arbitrary
H2D penalty, balancing quota or fitted decode/prefill weight. Forecast and execute
any proposed rule consistently; include planning and full/nominal completion.
Real batch service/restore/save measurements remain necessary before promotion.

## Reproduction and preservation

```bash
PYTHONPATH=python:experiments/issue23_strategies \
  python experiments/issue23_strategies/run_pcie_matrix.py \
  --repo "$SOURCE_ROOT" --inputs "$INPUTS" --seed "$CALIBRATION_SEED" \
  --output "$FRESH_OUTPUT" --policy pending-first --workers 3 \
  --residual-world --phase-residual-world
# Another fresh directory: --candidate-first
# Source02 changes only the driver: --reference-policy timed-memo
```

Source01 SHA256 `48fb548365ace3bcc80520b1970ba81a7efbc745148669009279439df40ff90d`;
driver-extension source02 SHA256 `92698d6453bba5f28eda34a05a70a079a7be92ea5f90f1d54b2f7c3e00ae2a5f`.
P immutable sources: `/root/issue23-pending-first-source-01` and `-02`.
Results: `/root/issue23-pending-first-01`, `-02`, and
`/root/issue23-pending-first-reference-01`. All completed with exit 0, no supervisor
stop. Full archives remain under P `/root` and locally; SHA and compact reports:
`evidence/pending-first-20261026/archive-receipt-03.json`.

All 27 arms passed stream/source/accounting checks. Each D request retained its
entire accepted-prefix remainder, 127 subsequent decode tokens, one restore and
one save. Reverse-run acquired-state quotes were reconstructed from completed
observations. This is simulation conservation, not a model-output correctness
audit. `analyze_control_work.py`, `analyze_pending_first.py` and
`diagnose_pending_release.py` reproduce the analyses. The timer remains paused;
there was no hardware experiment, runtime upgrade, commit or push.
