# Numerical-calendar decision and control-cost diagnosis

Continue from `numerical-calendar-20261027.md`; retain all partial gains/losses.
Exact-calendar02 completed18 fully charged CPU arms, with default same-round
parent p99 reductions8.12/8.40/−1.59% and unmeasured sharing1 boundary
17.32/−6.13/−0.93%. It is not promoted; default TimedMemo remains retained.

## Questions before changing another policy

The sharing-middle worst request git-multibranch:12 spent12.5225s before P API,
versus0.8047s for its same-input reference. Actual P API was slightly faster and
the chosen route's forecast first222.2612s was close to actual221.8778s. Thus
blindly adding a restore penalty would not address the observed discrepancy.

We must distinguish:

- An implementation failure to minimize its declared budgeted objective.
- A correctly selected current pending-first profile with worse later causal
  queue/cache evolution or phase-cost residuals.
- More controller work and/or slower arithmetic changing admission timing.

The new offline helper `profile_numerical_choice.py` replays original95 causal
requests at the middle nominal rate with sharing1 and zero measured control
time, then records all actually scored/pruned candidates at git-multibranch:12.
It asserts that the chosen route equals the minimum fully scored ordering.
The fixed-control state is NOT a reconstruction of the previous charged run.
Target selection is diagnostic only; it is not a policy feature or benchmark
subset. The original full replay still completes, and physical/RPC work remains.

The first captured calendar is also evaluated with float/exact arithmetic on
identical copied mutable state. Both disable the same early-termination bound
for full-calendar profiling. Completed-only immutable cost snapshots are shared
read-only; residual queries are evaluated at the captured origin, not sampled
from future physical execution. These profiler times are not net TTFT results.

## Diagnostic implementation history

Initial helper02 failed at target when deepcopy reached the snapshot RLock.
Helper03 preserved residual values but failed at Python deepcopy of a frozen
cost delegation proxy. Both exit1 records/logs remain in separate `/root` output
directories; neither is a policy failure or a completed performance result.
Helper04 copies mutable schedule/cache state, retains immutable completed-only
costs, and passed a one-original-request local invocation before the full run.
No source02 algorithm, simulation physics, rate, input or completed evidence was
edited. No long NPU qualification or native compilation is involved.

Helper04 SHA `e592b7cbd5f1a5ef61b5c6225de7b557ffd15cac86762559a45b19e9c8cfebdb`.
Actual command uses `/root/profile_numerical_choice-04.py`, frozen source
`/root/issue23-exact-calendar-source-02`, original inputs/seed and stable Python.
Process PID/PGID344827, started1793070528.243755, limit600s, one CPU worker,
OMP/BLAS1. Root `/root/issue23-numerical-choice-profile-04` contains the exact
command/process/result/log. Preserve failures and inspect its actual result.

Next: use measured hotspots and candidate score evidence to choose a bounded
change. Do not widen the budget, add a gain epsilon, balance quota or use future
realized timings. Hourly research ACTIVE; no real parallel NPU validation claim.

## FINAL diagnostic05 (supersedes running04 above)

Helper04 completed all95 requests but failed while serializing a frozenset in
the diagnostic report. Its complete replay and partial report are retained.
Helper05 serializes sets before opening the report file and completed in110.09s,
exit0/no stop. Archive `/root/numerical-choice-profile-evidence-05.tar.gz` includes
all02–05 helpers/processes/failures, SHA
`4a9ce4c542e42855a9924b5c58b20aca6c20b8ab5d6cec640eb8d98833281937`,
verified both remotely and locally. Script05 SHA
`fccdf1d67476861f4242395c846036c22ebdf848ddc933de53d43bf7e0869d26`.
All95 per-request records, stages, resource events, batch iterations, decisions
and original forecasts exactly match the uninstrumented zero-control replay;
p99 remains16.15410805846085s. This establishes diagnostic noninterference only.

At the captured fixed-control state, git-multibranch:12 choosesP0/D0 with2048
P recovery tokens and zero D recovery, and predicted pending-first maximum
10.0199164554s. It is the minimum fully scored ordering within the unchanged
budget. This state is different from the old charged P1 selection; no conclusion
that the old P1 was suboptimal follows. Scoring the current admitted set also
does not optimize unknown future sessions or guarantee final global p99.

On the identical first full-calendar state, float/exact/exact/float profiler
durations were31.42/30.48/31.11/31.54ms. Exact time conversion accounts for about
1.4% cumulative time here, not a dominant source. CacheForecast.save accounts
for roughly18.5–19.1ms;929 repeated list.remove calls about5.14ms. These nested
profiler totals must not be added together or treated as online costs. Whole
search profiling also finds protection synchronization and scheduling hot;
this single-state profile does not prove the same mixture in all requests.

The evidence supports an equivalent victim-removal computation optimization,
not a restore penalty or weakened arithmetic. Continue in
`linear-reclaim-20261027.md`; preserve all earlier mixed outcomes and default.
