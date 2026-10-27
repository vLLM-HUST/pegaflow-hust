# Ready prefill before later restore — 2026-10-27

Candidate ready-prefill-before-restore-01, parent/reference prefix-reclaim-calendar-02. The mixed advice-calendar trigger is retained as evidence, not combined into this candidate. All original parent routing, P revalidation, optional D advice, cache evolution and release-cohort rules remain.

## Observed bottleneck and rule

The preceding fully charged simulation shows a later H2D restore blocking already-ready earlier D prefill for4.004694s. In the existing scheduler, a scan for restore/save precedes all compute. At an idle device decision, inspect eligible admitted FIFO phases. If the first barrier is restore and older ready prefill exists, advance only those older prefills. If first barrier is save, or no older prefill exists, use the parent rule. Never interrupt an issued operation. Decode and synchronous save still occupy their full resource intervals and release their slots only on completion. No sampled future duration, fitted threshold, task/rate identity, forced balancing or free transfer.

## Model correction

Resource capacity and operation duration are insufficient to specify waiting: phase arbitration defines predecessor edges in the resource DAG. For an older ready prefill p and later restore h on an exclusive device, the old edge h→p becomes p→h only under the observed eligible FIFO condition. In the isolated two-operation case without new arrivals, moving p before h advances p's first token by h's duration and leaves their total work unchanged. This is not a global p99 theorem: other decode/release waiting, co-batching, new arrivals and cache decisions can change. Predict and execute with the same ReadyPrefillBatcher; selected forecast provenance records its class. Forecasts use completed-only cost models; execution uses the unchanged world.

## Evaluation and status

Fixed95/8/seed20261014, three original rates, deviceKV39379271680B and2GiB host, D4slots, original isolated64stage seed. Default0 then unmeasured sharing1, native+same-roundPrefix02+candidate, max3CPUworkers/OMP1,1800s per matrix/4500s total. Full measured control, model work, inventory, H2D, copy gates, save, wait and final drain charged once. No NPU claims.15 scoped tests pass locally; Ruff passes. Frozen source and P tests/actual launch receipt must be read for current state.

Record every actual phase inversion and check its next iteration, first predictions versus accepted cache/actual endpoints, P and D queue waits, tail requests and full completion. Zero activation cannot establish the mechanism. Mixed results require diagnosis of the first changed routing/cache/phase event, not unchanged reruns or stopping at equivalence. Preserve all outcomes; default TimedMemo-normal-910B2 is unchanged.

## Actual launch

Source SHA `e409d33886ca206a49eba847719a4cce999a4f576dd81d794ad6fc54474dfc8d`; runner SHA `de68f3ddd860479227bd93301c26ca51cccf588006fbcb7e8d8cde9e1e26201d`. PID/PGID 560774, started1793086175.0921462, cwd `/root/issue23-ready-prefill-source-01`. Command `['/root/issue23-tools/pegaflow-joint-pipeline-01/.venv/bin/python', '/root/ready-prefill-pipeline-01.py']`. Root `/root/issue23-ready-prefill-charged-pipeline-01`; default then sharing output names in launch receipt. Both exact class/auditor imports and15 targeted P tests passed. First native arms sampled; candidate results pending. Every arm source/inputs/fulldecode/phase/history audit remains mandatory.

## FINAL:18 arms completed, all audits passed, not promoted

All source/control, input/work, full-decode, release-history, acquired-boundary and actual phase-intervention checks passed. P/local full archive SHA verified. No active simulation remains. Every outcome is retained; TimedMemo default is unchanged.

| Scenario | Rate | Reference p99 s | Candidate p99 s | Net reduction % | Phase reorder records |
|---|---:|---:|---:|---:|---:|
| default | 0.2452 | 18.483378 | 19.912314 | -7.731 | 59 |
| default | 0.4903 | 19.662582 | 20.913654 | -6.363 | 178 |
| default | 0.9806 | 25.576928 | 19.691652 | +23.010 | 192 |
| sharing | 0.2452 | 17.815834 | 17.981846 | -0.932 | 327 |
| sharing | 0.4903 | 16.383019 | 17.678510 | -7.908 | 151 |
| sharing | 0.9806 | 16.221663 | 18.279649 | -12.687 | 171 |

Default low/mid regress7.73/6.36%; high improves23.01%. Sharing1 low/mid/high regress0.93/7.91/12.69%. Actual reorder records show activation, not necessarily net saved wall time. All6cases have initial route differences before first physical reorder: the revised forecast can change routing before execution, and measured CPU timing also feeds the trace; this is not an isolated causal estimate of the swap.

Default low worst fix-git:12 D API-to-first12.015676s contains1.823264s already-running other restore,2.969124s other compute and7.223272s target-containing compute. This restore was already issued, so the rule correctly does not preempt it. Original first forecast underpredicts by0.700061s; not all delay is missing queue prediction.

Default middle worst git-multibranch:10 changes P cache12928→640, P API2.001829→7.581518s, P→D0.495640→10.387598s; D first residence improves10.997799→0.761206s. Original first endpoint residual+5.919879s; after-P keep forecast327.231211 versus actual327.156692s. Do not replace original prediction with revision or call the entire residual pure queue error.

Sharing middle worst headless-terminal:12 gains P cache1280→7680 and P API5.166193→2.150791s but P pre-wait6.482898→12.677045s. Sharing high feal:12 same P1/D0 sees P→D1.527747→6.508794s. Thus D local ordering cannot by itself remove upstream/cache feedback.

Independent offline phase-cost diagnosis matched original predicted node and effective prefix to actual world sample:1023/1140 default and998/1140 sharing comparisons; exclude all changed paths. Largest absolute first-service residuals2.272/2.694s respectively, not queue/batch residence. World labels are diagnostic only, not online inputs. This narrows the next priority to cache lifetime/publication and queue-state evolution before arbitrary restoration penalties.

### New modeling limitation and next priority

See [save-publication-20261027.md](save-publication-20261027.md). A6-block fitting-save counterexample exposes current simulated immediate-publication versus native-source staging ownership. This is a source/model mismatch, not proof it explains every observed cache collapse or installed-wheel behavior. Retain old model outcomes with the limitation; do not silently correct historical results or claim real deployment gains.

Full archive `ready-prefill-completed-default-01-full.tar.gz` SHA `8a0c03f87c280191f3532db7932a7bfd7d5ac1dc1204d1477b6a08e6e47d4b98` (19372325B), P/local identical.

Full archive `ready-prefill-completed-sharing-01-full.tar.gz` SHA `3bb92839f2db4abb59e313f024bc6c8ea8031882fc125c8d7845bce09b20f1a7` (17458882B), P/local identical.
