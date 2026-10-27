# Visible-work controller cost: control-visible-work-01

Status: a CPU research candidate derived from `control-release-calendar-01`.
The retained default is unchanged. This does not deploy parallel NPU serving.

## Observation and hypothesis

In the parent's first high-rate run, `feal-differential-cryptanalysis:12` had
first-token endpoint error of only about 0.054 s, yet its route calculation took
3.317 s versus a completed-history mean near 0.825 s. Its P completion was
underpredicted by about 2.396 s; later overprediction canceled that error.
Endpoint accuracy alone therefore hid component miscalibration.

The hypothesis is that a constant controller mean fails to represent the work
of a larger pending calendar and physical inventory. This candidate changes the
controller estimate, not the service world, rate, input, cache capacity, search
budget, or pending-tail objective.

## Estimator and release constraints

At a decision origin t, define visible features x(t) = [1, Q(t), U(t)L(t)].
Q counts planned remaining prefill chunks, decode token steps and restore/save
boundaries of admitted work plus the current request. Running D progress is
observed; other stages use their currently selected boundaries. U is the current
physical-unit count, and L is the live-request count. Future arrivals, eventual
cache acceptance, task names, request rate and latency rewards are not features.

Fit route CPU and D-boundary CPU separately:

```text
H_X(t) = {(x_i, c_i): control_i has completed no later than t}
beta_X(t) = argmin_{beta >= 0} sum_{i in H_X(t)} (c_i - x_i dot beta)^2
c_hat_X(t) = x(t) dot beta_X(t)
```

Until the observed design matrix has full column rank, use the completed sample
mean (zero with no observations). Enumerate the seven nonempty active sets of
the three-variable NNLS problem after column normalization. Standard floating
point rank/lstsq tolerances apply; there is no tuned regularizer, reward margin,
rate-specific coefficient, or coefficient borrowed from another arm.

The estimates enter the inherited resource calendar as release constraints:

```text
P enqueue = decision origin + calibrated route RPC + c_hat_route
D control end = D-slot admission + calibrated boundary RPC + c_hat_boundary
copy start >= D control end and all required resource availability times
```

Control consumes no device gate. Full decode, H2D/restore, synchronous save,
source/destination copy ownership, physical allocation reclaim and leases retain
their existing contracts. Fitting and feature construction are included in the
measured controller cost. The original current decision's measured CPU is never
an input to its own prediction.

## Evaluation contract

Same 95 requests, eight causal sessions, three original Poisson arrays and
isolated service seed. Three independent four-core CPU workers evaluate the
rates; each arm resets online observations. Run native, parent and candidate,
then reverse parent/candidate order using the same frozen source. Preserve every
result and analyze component error plus fully charged conventional TTFT p99.
The 95-sample nearest-rank p99 is the maximum, not a stable-tail certification.

Source: `control-work-source-01.tar.gz`, SHA256
`6a22f7e617de7a8b4bf5cf2aeda116e4d625b47cd0caae540f18e2ae1ddd6cc9`.

The replay's real control wall time can vary with CPU contention and affects
subsequent routing, cache evolution and causal arrival. A better CPU fit is
therefore a diagnostic improvement, not a guarantee of lower closed-loop p99.

## Completed results and feedback

Both complete three-rate matrices passed the accounting audit. All 570 original
route quotes were recomputed from completed-only observations; feature/CPU labels
match the charged decision and boundary histories. The source/inputs were hashed.

| Rate | Parent → candidate p99, first order (s) | Parent → candidate p99, reverse order (s) |
|---|---:|---:|
| 0.2452 | 33.333 → 26.940 | 28.892 → 27.923 |
| 0.4903 | 31.469 → 27.235 | 29.889 → 31.850 |
| 0.9806 | 32.603 → 33.401 | 26.025 → 28.560 |

The candidate wins low-rate p99 in both runs, but high-rate p99 regresses in
both; middle-rate direction changes. Keep the source as a partial-success model
branch, **not a replacement default or a stable three-rate winner**. All costs
are included. Hardware benefit and statistical significance are not established.

To avoid comparing different routing states, a separate archive-only sequential
diagnostic evaluates both estimators on the candidate's same recorded features.
Visible-work CPU MAE is lower than past mean in all six traces: first order
0.340→0.250, 0.272→0.225, 0.416→0.318 s; reverse order 0.330→0.241,
0.400→0.323, 0.314→0.281 s. This diagnostic does not change policy behavior.
Selecting between mean and NNLS by cumulative completed prequential squared loss
slightly worsens MAE in all six traces versus NNLS alone. That selector is
therefore not implemented as another routing candidate merely to keep iterating.

In the first high-rate replay, the candidate's maximum is `dna-insert:13`:
9.082 s before P API + 7.140 s P API + 0.488 s between P and D API + 16.691 s
D API-to-first = 33.401 s. Its initial first forecast was 399.557 s and actual
first 399.483 s; P completion prediction was also close. It was waiting behind
an admitted P job and then shared the D execution path. The inherited fixed-D
choice is made before P completion; by that point, other sessions may have
advanced. A snapshot-local after-P D-binding diagnostic tests whether this is
an actionable decision-timing limitation. It cannot establish closed-loop benefit
until the extra decision cost, queue changes, state revalidation and resource
ownership are modeled in an actual candidate replay.

The high-rate candidate made 41/54 P assignments and 58/37 D assignments in
round one, not a collapse onto one P. D restore-path sums were 93.167 s versus
80.636 s, but those overlapping per-request costs are not added again to TTFT.
Software link wait was zero in both arms. This is simulation accounting, not
evidence that real RDMA cannot congest.

The after-P snapshot diagnostic found only 2/95 node-change opportunities,
maximum predicted pending-tail reduction 0.521 s; only one exceeded its own
diagnostic calculation wall. The checks cost 13.102 s in total. This uses a
zero-control trajectory with RPC retained, compares a finite set of local/full/
zero/original boundaries, and changes no routes. It is neither an optimality
proof nor a net-policy gain. It does not justify adding unconditional D rebinding
to every request, so no such candidate was deployed.

## Reproduction and evidence

### Retained residual sensitivity

The same frozen candidate was also tested in the retained aggregate-residual
world. Parent → candidate conventional TTFT p99 (seconds) was 31.800 → 34.177,
38.373 → 24.474, and 25.677 → 29.905 at the three rates. This adds a low-rate
regression and again loses at the high rate. All nine arms and 285 original
past-only controller quotes passed the independent accounting check. This is
additional falsification, not a reason to select the favorable middle-rate run.

The subsequent joint-phase execution-model sensitivity is documented separately
in [phase allocation](phase-residual-20261026.md). A different execution world
must not be presented as a new algorithm improvement over an old-world result.

NumPy is required for this experimental controller estimator. The P runtime used
NumPy 1.26.4; no dependency was installed or upgraded for this work.

```bash
PYTHONPATH=python:experiments/issue23_strategies \
  python experiments/issue23_strategies/run_pcie_matrix.py \
  --repo "$SOURCE_ROOT" --inputs "$INPUTS" --seed "$CALIBRATION_SEED" \
  --output "$FRESH_OUTPUT" --policy control-work --workers 3
# A separate fresh output with --candidate-first reverses the paired order.
PYTHONPATH=python:experiments/issue23_strategies \
  python experiments/issue23_strategies/analyze_control_work.py \
  --result "$RESULT" --inputs "$INPUTS" --source-receipt "$SOURCE_RECEIPT" \
  --output "$FRESH_AUDIT_JSON"
```

Complete source/results remain as independent SHA-verified archives under P
`/root` and the local `cache-profile-20261026` collection. Small analyses and the
archive manifest are in `docs/research/evidence/control-work-20261026/`. Do not
substitute a current mutable checkout for an archived source when reproducing a
particular measurement. The current feature branch does not change the installed
worker/native binary or resume the paused automation.
