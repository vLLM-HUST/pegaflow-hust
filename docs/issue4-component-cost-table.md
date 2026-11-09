# Issue #4 Ascend component cost table

This table is generated from `results/trace-audit/20260813-090821/trace_audit.json` by
`python3 scripts/analyze_issue4_component_cost.py`. The custody classification is
**exploratory/history-only**. It is selector evidence only, not a
formal paper, end-to-end, native-vLLM, cross-system, CUDA, RDMA, or SSD result.

| Component | Retained observation | Interpretation |
|---|---:|---|
| Host packing | unavailable | No request-scoped duration was recorded; do not fold it into DMA. |
| Pinned-host copy | unavailable | The retained load-task timer does not separate CPU/pinned copy from H2D. |
| H2D, Q0 (76 blocks, 1,434,451,968 bytes) | n=12; mean 88.698 ms; median 92.070 ms; range 69.070–98.610 ms | Directly measured aggregate `ascend_batch` load task. |
| H2D, Q2 (1 block, 18,874,368 bytes) | n=12; mean 3.031 ms; median 2.145 ms; range 1.560–9.720 ms | Directly measured aggregate `ascend_batch` load task. |
| D2H | unavailable | No store-task duration was retained. |
| 76-block lookup/scheduling | n=12; median total 38.266 µs (key build 7.956, scan 21.865, schedule 0.220) | Direct server-log timing; not host KV packing. |
| 1-block lookup/scheduling | n=12; median total 13.351 µs (key build 1.045, scan 3.055, schedule 0.205) | Direct server-log timing; not host KV packing. |
| Block registration | 15 startup events; duration unavailable | Registration occurrence is proven, but no cost can be claimed. |
| Queue wait | unavailable | Every retained `queue_time_ms` value is `-1`; no zero-cost inference is allowed. |

## Selector decision

For Q0, median TTFT falls from 1086.6 ms to
380.7 ms, a retained median saving of
705.9 ms. The measured H2D load costs 88.7 ms
on average. That supports the narrow **GO** decision already recorded in #4: cross-instance
KV reuse can clear the single-node Ascend break-even threshold for this long-prompt Q0
selector case.

It does not close the formal evidence gap. Packing, pinned-copy, D2H, registration duration,
and queue wait require new request-scoped instrumentation, while a current-main run must pass
the artifact-custody gate before any formal or paper claim.
