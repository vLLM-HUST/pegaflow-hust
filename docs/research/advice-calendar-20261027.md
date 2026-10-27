# Advice calendar window01: pending CPU candidate

Parent/reference: prefix-reclaim-calendar-02. Retained default remains
TimedMemo-normal-910B2. This does not promote the mixed release-window01.

Evidence: advice-window-20261027.md FINAL identifies known queued source-copy
gate work omitted by the token-progress-only trigger, plus limited7/151 skip
activation and substantial pre-intervention route/cache drift.

Hypothesis: the useful optional-search window ends at the original FIFO
acquisition, not at isolated decode completion. Reuse the already required
keep forecast's resource DAG stage_start(q,D), including known copy gates,
H2D, complete decode, synchronous save, queue/slot/control dependencies and
cache evolution. No second snapshot or future sampled event/duration is read.
After that measured keep quantum/RPC completes, estimate remaining candidate
CPU plus commit from fully completed prior searches only. If predicted original
acquisition <= now + mean_remaining_search_and_commit, cancel the remainder.
Cold history/unknown acquisition retains the original search. This is an
expected opportunity rule, not a guaranteed deadline or proof of net benefit.

The first forecast/RPC is still paid. Checking the window occupies controller
time and is reported separately; cancelled partial searches do not train full
search cost. Existing acquisition and new-request priority cancellation wins.
No forced moves, quota, task/rate condition, or fitted benefit threshold.
Actual reroutes still require owned unacquired work and fresh commit checks.

Evaluation: same95/8/seed/rates/910B2 KV budget/2GiB host/isolated costs;
three CPU workers, default0 and unmeasured sharing1 sensitivity; full charged
control/stages/final drain. Same-round reference Prefix02 and native retained.
15 scoped local tests passed: partial/future exclusion, sunk RPC exclusion,
known source-gate acquisition, reused forecast, charged check and cancellation,
parent candidate sequence and acquired-operation contracts. Next freeze,
repeat these on P, run bounded full-cost three-rate matrices and independent
source/history/partial-search/resource accounting audits. All outcomes kept.

## Actual launch checkpoint

Source215files SHA db9523e651348d50cc9e4e0976ac22482787fe68791d9e1ff825c9f1741af4c0. RunnerSHA 2599ac1dd34c8985c64187d19cb21788b67dc4872a7f96bff552b12145f51b33.
PID/PGID502011, started1793081842.9160888; exact command ['/root/issue23-tools/pegaflow-joint-pipeline-01/.venv/bin/python', '/root/advice-calendar-pipeline-01.py'].
Cwd /root/issue23-advice-calendar-source-01. Root /root/issue23-advice-calendar-charged-pipeline-01; outputs ['/root/issue23-advice-calendar-charged-default-01', '/root/issue23-advice-calendar-charged-sharing-01'].
4500s total/1800s each,3workers. Entry imports and15tests passed on P.
14:18 Beijing default native arms completed; reference/candidate pending.
Do not confuse initial native output with candidate completion.

## FINAL: complete accounting, mixed results, not promoted

All18 CPU arms finished. Default simulation and per-case audits passed before an offline summary failed on a missing top-level all_checks_passed field. The original failure/archive is preserved. Only the offline reporter was repaired; default was not rerun. Sharing was previously unstarted and then completed from the same immutable simulation source. Both full archives were verified on P and locally.

| Scenario | Rate | Reference p99 s | Candidate p99 s | Reduction % | Checks / cancellations |
|---|---:|---:|---:|---:|---:|
| default | 0.2452 | 27.458775 | 14.869022 | +45.850 | 42 / 1 |
| default | 0.4903 | 20.742384 | 17.472912 | +15.762 | 40 / 3 |
| default | 0.9806 | 18.286494 | 18.383440 | -0.530 | 42 / 1 |
| sharing | 0.2452 | 15.619728 | 22.334477 | -42.989 | 8 / 0 |
| sharing | 0.4903 | 15.717513 | 20.370167 | -29.602 | 8 / 0 |
| sharing | 0.9806 | 14.813580 | 24.924792 | -68.256 | 14 / 0 |

Default low/middle improved but high slightly regressed; sharing1 sensitivity regressed at all rates with zero intended cancellations. Default middle/high and sharing middle initial routes diverged before the first cancellation/check respectively. Do not attribute the complete closed-loop difference to cancellation. Controller timing, queue interleaving and cache evolution are endogenous. Sharing1 is unmeasured, not a hardware property. TimedMemo-normal-910B2 remains the retained default.

### New observed mechanism and limits

Default low git-multibranch:5 D API-to-first 8.336772s contains 4.004694s of later feal:6 restore and 4.332059s of compute batches containing the target. The latter is batch residence, not exclusively attributable GPU time. The scheduler scans all restore/save phases before ready compute, allowing a later restore to overtake earlier prefill. Default middle fix-code-vulnerability:7 D residence 11.582207s includes 1.753269s other restore and 9.213832s target-containing compute.

This explains concrete blocking intervals, not the entire p99 change. Sharing high headless-terminal:13 instead has P pre-wait 2.220368→16.256889s and accepted P cache 6784→640 tokens, while its D first residence stays about0.52/0.56s. Default high feal:14 has more P cache and faster P API but P→D wait7.369267→13.049823s. Thus phase reordering may help one mechanism while leaving upstream cache/queue feedback unresolved. No RDMA bandwidth-congestion inference.

Full570 aligned request pairs, initial/revised forecasts, first intervention, actual cache and D phase partitions: evidence/forecast-feedback-20261027/advice-calendar-{default,sharing}-alignment-01.json. Partition intervals are observed accounting, not counterfactual gains. All CPU/control/restore/decode/save/final-drain costs are retained;95 exposed requests,p99=max,no statistical or NPU claim.

Full archive `advice-calendar-completed-default-01-full.tar.gz` SHA `97a1e0bda2848b981315b619b4dfe2774df320dd86f1752aecb181e6d489660b` (19411328B), P/local identical.

Full archive `advice-calendar-completed-sharing-01-full.tar.gz` SHA `cea99ec9aef0e6997dc0e60ce1d0f817ad28fa31d06675aa4ea1eb2b6b6012c4` (17388609B), P/local identical.
