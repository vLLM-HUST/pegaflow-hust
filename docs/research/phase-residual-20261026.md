# Joint residual location in the 2P2D execution model

This is an execution-model sensitivity correction, not another claimed policy
improvement. `control-release-calendar-01` and `control-visible-work-01` remain
unchanged and use the same world inside each comparison. All previous results
retain their original source and world definition.

## Concrete omission

The retained residual world perturbs the entire pre-first interval F and the
post-first interval T with residuals of one isolated calibration sample, selected
by the existing hash of request key and node within cold/warm strata. D restore
and finish/save durations nevertheless remained near their fitted means. Extra
restore-path time could consequently be assigned to interleavable token work
instead of the exclusive restore gate. Equal aggregate service is insufficient
to identify the delay imposed on competing requests.

The new option uses the **same sample** for both aggregate durations and their
phase location. With r(x) the original fitted restore path, sample s and current
prefix x, and clip(z,0,u) the projection onto the physically valid interval:

```text
restore' = clip(r(x) + observed_restore(s) - r(prefix_s), 0, F), if x > 0
restore' = 0, if x = 0
finish'  = clip(observed_post_response(s), 0, T), on D
first_body' = F - restore'
decode_body' = T - finish'
```

Observed restore is scheduled_ts−queued_ts in the isolated calibration.
Observed post-response is max(0, wire_complete−wire_first−(last_ts−first_ts)).
The original F and T are unchanged at the same request/node/prefix coordinates.
Restore and finish retain exclusive device occupancy; the remaining service
follows the same iteration kernel. P still runs an uninterrupted prefill API;
its completed synthetic stage record now reflects the same phase split when
updating the past-only operation model. Batched D residence is still not trained
as singleton compute time.

The policy cannot read the sampled residual or future execution label. It sees
only existing completed samples and observed stage progress. This changes no
task/rate feature, gains threshold, capacity, arrival seed, output length,
candidate budget or native/RDMA ownership rule. The default world is unchanged;
enable this scenario explicitly with both `--residual-world` and
`--phase-residual-world`.

## Interpretation limits and checks

These are stage proxies, **not isolated H2D DMA, PCIe saturation or save-kernel
measurements**. Scheduled-minus-queued can contain additional host/scheduler
work. Applying additive residuals across payload sizes and treating this path
as exclusive remain assumptions. The small original calibration does not prove
the distribution under new parallel serving. This correction makes the modeled
resource location explicit; only future instrumented real runs can validate it.

Tests check joint sample selection, cold/node separation, nonnegative phases,
and exact aggregate first/tail conservation. The offline audit recomputes all
sample labels and durations from the original seed, checks 190 stage records per
arm and reconciles restore/save trace occupancy after subtracting the already
charged iteration-controller CPU. It does not audit model output correctness
because these are CPU simulations, not new generated token results.

Frozen source: `phase-residual-source-01.tar.gz`, SHA256
`f7a70b711424b4583edcda267c98048f68c2c6ad972e9d0b908b07814934a376`.
Run the same `run_pcie_matrix.py --policy control-work` command with the two
flags above and a fresh output. Audit with `analyze_control_work.py` using this
source receipt, plus `audit_phase_residual_world.py --result ... --inputs ...
--seed ... --output ...`. Do not compare two different worlds as a paired
algorithm effect.

## Completed screen and mechanism follow-up

All nine arms finished. The independent checks reconciled 190 phase rows per
arm against the original isolated calibration, conserved aggregate first/tail
service, and reconciled exclusive restore/save occupancy after accounting for
iteration-controller CPU once. Restore allocation shifted by 10.73–30.69 seconds
per arm in total at the same stage coordinates. These sums overlap request
lifetimes and must not be added to aggregate TTFT.

| Nominal requests/s | Mean-CPU parent p99 | Visible-work candidate p99 | Reduction |
|---:|---:|---:|---:|
| 0.24516 | 32.040 | 27.834 | 13.13% |
| 0.49032 | 32.629 | 30.476 | 6.60% |
| 0.98065 | 31.942 | 33.930 | −6.22% |

The high-rate regression remains. No candidate is promoted on this evidence.
The native middle-rate p99 changed from 81.408 in the earlier aggregate world
to 62.663 in this world, but that is **not an isolated phase-placement effect**.

An attempted diagnostic exposed a false assumption: `NativeStrategy` rotates by
actual admission sequence, not immutable input index. Causal completion changes
can therefore change later P/D assignments. The first low-rate diagnostic
observed six changed request routes and stopped at its fixed-route assertion;
both outputs and the failed-assumption diagnosis are retained, not relabeled
as a successful experiment.

A separate diagnostic explicitly fixed routing by input index and set control
CPU to zero in both worlds. It retained cache feedback and all original
execution constraints, but is **not the native baseline or net-policy test**.
All three comparisons had identical routes and 190/190 identical accepted-prefix
coordinates. Aggregate → joint-phase p99 was 67.326 → 68.254, 67.504 → 68.432,
and 67.445 → 68.373 seconds. The ~0.928-second increase shows a direct blocking
location effect in this controlled scenario. It cannot explain the earlier
~19-second difference by itself. Admission ordering, routing and cache evolution
must be included in any explanation of that larger closed-loop difference.

The next actionable omission is online observation: excluding batched D API
residence avoids training queue wait as service, but the current policy also
discards separately observable completed exclusive restore/save intervals.
A new experimental branch updates only those components, keeping prefill and
decode compute fits unchanged. See `completed_phase_search.py`; this needs a
charged three-rate comparison, not an assumption that more observations win.
