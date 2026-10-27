# Acquired exclusive phase calendar, 2026-10-26

Status: isolated CPU candidate, completed but **not promoted**. Parent is
`completed-exclusive-D-01`; it is not a validated three-rate winner. Keep the
retained default `TimedMemo-normal-910B2` unchanged.

## Evidence and hypothesis

The previous reversed high-rate tail request accepted no D cache and its first
forecast missed by only about 0.016 s. Other requests' overlapping compute grew
from 2.545 to 9.958 s. Therefore H2D penalties and improved point estimates alone
cannot explain that failure. Its competing requests were already older; a simple
age priority is not a new solution. Ready-but-unrouted backlog was absent at this
late decision, so adding hypothetical pending requests would not be justified.

A separate, checkable calendar omission exists at 12–17 of 95 route origins per
previous arm: an exclusive restore/save is already running. The old calendar
restarts its full duration and does not pre-reserve its acquired device against
new copies. Fix this constraint first; do not claim it caused the cold tail.

## Model

An operation v observed to start at s and still active at t has age a=t-s. Given
current phase/work features z, estimate remaining occupancy as

`R_v(t) = E[S_v(z) - a | S_v(z) > a, completed history at t]`.

The samples are `max(0, point(z) + observed_i - point(z_i) + control_overhead)`.
Only completed same-node, same-phase samples are eligible; restore also matches
cold versus nonzero prefix. Use the mean excess of surviving samples. If all
completed samples are shorter than the observed age, extrapolate by their mean
residual lifetime (the retained memoryless-tail assumption), rather than declare
an active operation complete or invent a fitted timeout. This extrapolation is
explicit uncertainty, not measured tail accuracy. Cold restore has zero physical
body in the current world; do not reinterpret queue-to-scheduled noise as DMA.

The device gate remains owned until t+R_v(t). New copies cannot preempt it. At
that event, apply the observed phase transition exactly once, then release its
gate and continue the original resource DAG, cache publication, leases and FIFO
semantics. The correction changes route/boundary predictions, not execution-world
service durations. It does not use scheduled future end times, sampled world
labels, future sessions, task/rate tuning or forced balancing.

This first candidate conditions exclusive restore/save only. The small in-flight
compute iteration is still conservatively restarted; that limitation remains
visible. Initial calibration proxies are not isolated DMA/save-kernel timers.

## Evaluation contract

Same original 95 requests / 8 causal sessions, three rates, seed, calibration,
normal 910B2 device KV / 2 GiB host pool, four-sequence D execution and joint phase
residual world. Compare candidate with its parent in the same run, with measured
route/boundary/iteration/model CPU and all copies/save/decode occupancy charged.
Preserve full results and reverse arm order if the first comparison has potential.
No NPU experiment, background monitor, commit or push is implied.

## Completed comparison

| requests/s | Parent p99 (s) | Candidate p99 (s) | Reduction |
|---:|---:|---:|---:|
| 0.24516 | 32.238 | 32.784 | -1.69% |
| 0.49032 | 32.199 | 28.460 | 11.61% |
| 0.98065 | 28.557 | 31.780 | -11.29% |

All nine arms passed accounting; 109 acquired-phase quotes were reconstructed
from the original seed and completed same-arm observations. The correction was
applicable at 21/13/18 of 95 route origins. A valid constraint repair did not give
consistent performance gains, so no all-rate confirmation claim or promotion.

High-rate first-endpoint MAE improved 1.815→1.428 s while p99 worsened. Its candidate
tail `feal-differential-cryptanalysis:11` accepted zero D prefix in both arms:
same-request D API-to-first rose 13.467→17.692 s, P API 7.892→10.044 s and P-to-D
0.474→2.023 s. Forty-six P and 34 D assignments changed across the full replay.
These are interacting path changes, not an identified isolated H2D effect.
The next candidate addresses decode-first compute visitation, retaining this
calendar branch as its explicit reference rather than mixing historical arms.

Source archive SHA256 `b3e36360450a26b1dbd914299d909bc2a4a6e4ac7ad92cf33a25ae634a147e55`.
Full result archive SHA256 `3a1bdf6da3de1114b12ef096a9b1af9b596893226c474366d5dab325a0446bb2`.
Both are preserved locally and under P `/root`; the result root is
`/root/issue23-acquired-phase-01`. Reproduce with `run_pcie_matrix.py --policy
acquired-phase --residual-world --phase-residual-world` and the same explicit
input/seed/source paths. Full source hashes are in the result's `scope.json`.
