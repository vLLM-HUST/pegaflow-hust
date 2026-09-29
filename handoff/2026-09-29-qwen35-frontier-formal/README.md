# Qwen3.5 Frontier formal evidence

This directory records the clean qualification and five formal 900-second
Frontier windows for PegaFlow on Qwen3.5-35B-A3B. The tested PegaFlow commit is
`cd64ecc283ff856a44437a9a25659929ef3a0653`.

## Result

The only baseline is `swe-unified-native-20260927`. The aggregate is computed
from the five point ratios, not averaged from rounded percentages:

`(geomean(candidate / native) - 1) * 100 = +6.342297886583825%`

| Concurrency | Candidate output tok/s | Native output tok/s | Gain | P90 decode tok/s/user | In-window output tokens | Completed + drain | Failed |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| C1 | 65.52333333333333 | 91.54555555555555 | -28.425434816909622% | 113.9060417414391 | 58,971 | 117 + 1 | 0 |
| C2 | 167.14333333333335 | 152.7211111111111 | +9.443502680994431% | 109.48681228877716 | 150,429 | 250 + 2 | 0 |
| C4 | 237.96777777777777 | 217.47555555555556 | +9.422770375214572% | 87.30566240630844 | 214,171 | 350 + 4 | 0 |
| C8 | 361.4411111111111 | 291.1066666666667 | +24.161055893983118% | 73.03849278224763 | 325,297 | 519 + 8 | 0 |
| C16 | 459.71555555555557 | 359.75 | +27.787506756235047% | 46.37622765934938 | 413,744 | 690 + 16 | 0 |

All five summaries are valid, use exactly 900 measurement seconds, and report
zero failed requests. Across the five windows, 1,162,612 output tokens were
counted in-window. Request-level validation confirms that `token_ids`, usage
completion tokens, expected output tokens, and streamed chunk counts agree for
all 1,957 requests.

## Contract

- Protocol: `swe-prefix-reuse/v1`
- Prepared workload SHA256: `8044561ffa1bb430bea8f778ef814d96649321e1a92654b95f64263b996d5e85`
- Tokenizer fingerprint: `3f9ca78537850303ee04bfa6640c020be89723c62f37121c0f27a4c0babc53e0`
- Model revision: `712cf74392b05026a6db2bf213d343747d1f6d45`
- vLLM: `d0f22d2bda562156e4dbf433ce645e1769b4f804`
- vLLM-Ascend: `03766ac696fde5ab1980d80ca0b8543d3580c989`
- BF16, Ascend 910B2, TP2/PP1/DP1, expert parallel disabled
- `max_model_len=262144`, `max_num_seqs=16`, `max_num_batched_tokens=4096`
- APC and async scheduling enabled, Mamba cache mode `align`
- native MTP with two draft tokens, thinking enabled, temperature zero
- graph mode `FULL_AND_PIECEWISE`
- KV cache memory `26038239232` bytes per serving chip

The Extension Manager records prove inspect, compatibility check, plan,
configure, enable, status, and rendered activation. The installed wheel imports
PegaFlow from the recorded site-packages path and activates `PegaKVConnector`
in `read_write` mode.

## Mechanism evidence

Every formal point exercised both PegaFlow loads and saves. Cumulative deltas
across the five independent windows are:

- 3,386 successful loads and 3,050 successful saves
- zero load failures and zero save failures
- 177,294,016,512 bytes loaded and 83,560,382,464 bytes saved
- 22,128 cache block hits, 1,152 misses, and 4,944 insertions
- 794,265 accepted MTP draft tokens out of 803,298 draft tokens

PegaFlow exposes no non-OK RPC metric series during any formal window. The only
failure-like service log line occurs after all windows, during shutdown, when a
duplicate `unregister_context` follows successful session cleanup; it does not
overlap a measurement or affect a request.

## Qualification

The clean C4/60 qualification at the same tested commit produced 4,498
in-window output tokens, 74.96666666666667 output tok/s, P90 decode throughput
26.82417748039618 tok/s/user, 20 completed requests plus four drain requests,
and zero failures. It exercised real save/load RPCs and was not used as a
formal result.

## Layout and verification

- `raw/formal/`: five request streams, summaries, metrics, service logs, NPU
  snapshots, start/stop boundaries, source identities, analysis, and release
  evidence.
- `raw/qualification/`: the clean C4/60 qualification only.
- `raw/manager/`: Extension Manager lifecycle evidence.
- `raw/audit_formal.py`: request, metric, and comparison recomputation.
- `raw/SHA256SUMS`: checksums for every raw evidence file other than nested
  checksum manifests.

Run `sha256sum -c raw/SHA256SUMS` from this directory to verify the evidence.
