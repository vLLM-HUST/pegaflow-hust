# Qwen3.5 原生 APC 输出差异：投影形状与状态生成路径

2026-10-13，在一台双卡 Ascend 910B2 节点完成原生 vLLM/APC 对照、
逐层采样和算子重放。**整体验收仍未通过。** 本轮确定了一个输出分叉的起点：
BF16 `out_proj` 矩阵乘法随 token 批次形状改变而出现数值差异。
固定 GDN 输出投影的计算形状后，四组对照由 **2/4 提高到 3/4**。
decode 生成边界状态的用例仍失败，需要继续处理计算路径之间的数值一致性。

本轮承接 [跨节点验收报告](issue28-qwen35-hybrid-cache-acceptance-20261013.md)。
PegaFlow 混合缓存 PR 的完整正确性门槛保持开放。

## 环境与比较方法

- Qwen3.5-35B-A3B；沿用已固定模型 revision
  `59d61f3ce65a6d9863b86d2e96597125219dc754` 和输入 token IDs。
- vLLM `0fc695fc6d1d82e9a5ac6835ac8e4e1c83703665`；
  vLLM-Ascend `5cb98caaadeff42b5b62b996e34bb2aaa29d20fd`。
- torch `2.10.0+cpu`、torch-npu `2.10.0.post4`、CANN 9.1.0；
  TP=2、BF16、eager、同步调度、mamba align、APC 开启。
- max_model_len=8192、max_num_batched_tokens=4096、max_num_seqs=4；
  每 rank 缓存 2 GiB；默认 SCHED_OTHER/nice=0。
- `kv_transfer_config=None`，完整模型对照不接入 PegaFlow。
- 每组依次运行：清缓存后的目标输入两次、清缓存后的生产输入一次、
  保留生产缓存后的目标输入一次。固定 seed=17、temperature=0、top_p=1，
  输出 32 tokens，比较完整 token ID 序列。

输入沿用 [cases.json](issue28-qwen35-hybrid-cache-evidence-20261013/cases.json)，SHA256：
`d7a4e6ed56199c4511783a16c5d8d1987c0f3acbbf45f518f442e5c5adba527c`。
这些是同一题目构造的缓存边界诊断用例，结果用于定位输出一致性问题。

## 模型级结果

“一致”表示缓存命中后的 32 个输出 token ID 全部等于该配置下的冷计算结果。
全部组别的两次冷计算均一致。命中数取 Prometheus 累积计数的请求前后差值；
所有请求的外部缓存命中增量为零。

| 组别 | 生产输入 → 目标输入 | 本地命中 tokens | 原生结果 | 固定 GDN 投影形状后 |
| --- | --- | ---: | --- | --- |
| split4k | 2048 → 4096 | 2048 | 第 2 个输出 token 分叉 | 32/32 一致 |
| decode2k | 2047 → 2049；2048 边界状态由 decode 产生 | 2048 | 第 4 个输出 token 分叉 | 第 4 个仍分叉 |
| prefill2k | 2048 → 2049 | 2048 | 32/32 一致 | 32/32 一致 |
| coherent4k | 4096 → 4097 | 4096 | 32/32 一致 | 32/32 一致 |

在同一个已加载模型的进程中，针对 split4k 切回原实现后重新出现第 2 token 分叉，
再次启用投影形状修正后恢复 32/32 一致。两次切换均重新清缓存，重新执行四阶段对照。
将固定形状扩大到全部 `AscendUnquantizedLinearMethod` 稠密投影，decode2k 仍失败。

## 第一类差异的定位链

1. 两个 TP rank、30 个 GDN 层的 conv 和 SSM checkpoint，保存后与恢复前的
   **120/120 组张量逐位一致**。这一检查针对 split4k 的原生 APC 恢复。
2. 第一层的完整 QKV、a/b、GDN 输出和 gated norm 输出，在相同 token 位置逐位一致。
3. 紧接着的第一层 `linear_attn.out_proj` 输入仍一致，输出开始不同。
   完整 4096-token 计算与两段 2048-token 计算的 TP 合计输出中，
   **469 个元素不同**，最大绝对差 `0.00390625`，RMS 差约 `1.38e-6`。
4. 用保存的投影输入与模型权重，在独立进程中分别重放两个 TP 分片的
   `torch.nn.functional.linear`，再将分片结果相加，可以**逐位还原**模型实测的
   完整计算输出和分段计算输出。因此已将该处差异定位到稠密矩阵乘法，
   无需缓存管理器、RDMA 或 MoE 参与即可复现。
5. 差异经过后续层扩大。split4k 的第一个输出 token 尚相同时，完整 logits 已不同；
   第二个 token 的冷计算首选为 ` DNA`，概率约 92.8%，缓存续算首选为 ` a`，
   概率约 99.3%。输出分叉点并非两个候选概率接近。

独立重放中，将 2048 行输入补零到 4096 行计算，再截取有效行，投影结果逐位一致。
改用 FP32 计算后转回 BF16，完整与分段 TP 合计仍有 405 个元素不同。

模型级诊断补丁只对 30 个 GDN `out_proj` 中 `1 < M < 4096` 的输入补零，
保留原来的缓存命中和有效输出长度。该补丁用于验证因果关系；它增加计算量，
且不能覆盖 decode2k，尚不足以成为完整的生产修复。

## 第二类差异的证据与边界

对捕获的同一组第一层输入，独立比较“2048 tokens 一次 prefill”与
“2047 tokens prefill，再执行一个 decode token”：

- conv 输出和最终 conv 状态逐位一致。
- GDN 输出最大绝对差 `0.000244140625`。
- 最终 SSM 状态最大绝对差约 `0.04214`，RMS 差约 `0.0002022`，
  相对 RMS 差约 `0.0680%`。
- 将最后一个 token 改走长度为 1 的 chunk prefill，也未获得逐位一致的状态。

这表明在固定输入、脱离缓存复制和整模型投影的条件下，GDN 的计算分段与
prefill/decode 路径本身也会产生状态数值差异。它与 decode2k 的现象相符；
本轮尚未完成该失败用例的逐层端到端因果归因，也未证明全部差异都来自同一个算子。

## 最小复现与证据

在已安装 torch/torch-npu 的 Ascend 环境运行：

```bash
python scripts/issue28/repro_ascend_linear_batch_shape.py --device 0 --seed 17
```

脚本只生成随机 BF16 矩阵，输入形状 `[4096, 2048]`，权重形状 `[2048, 2048]`。
实测同形状重复计算逐位一致；拆成两次 2048 行计算后有 **1452 个元素不同**，
最大绝对差 `0.001953125`；补零保持 4096 行形状后差异归零。
可在单张 NPU 上运行，无需模型权重。最终格式化后的脚本在机器上重新运行，结果一致。

本轮 [证据目录](issue28-native-apc-diagnosis-20261013/) 包含：

- `results.json`：56 次完整模型请求的输出 token IDs、缓存命中增量和分组结果。
- `activation-comparison.json`、`layer-comparison.json`：完整激活与 checkpoint 比较。
- `projection-replay.json`、`core-replay.json`、`decode-replay.json`：独立算子重放结果。
- `synthetic-matmul.json`：随机矩阵最小复现结果。
- `diagnostic-scripts.zip`：诊断脚本与固定输入快照；启动脚本保留执行环境路径。
  解压后需要按实际模型和 Python 环境修改 `manage.py`；逐层重放还需对应原始张量。

约 4.14 GB 的原始日志和张量保存在共享目录
`/data/shared_upload/hust-zsx/native-apc-diagnosis-20261013`，并生成逐文件 SHA256 清单。
诊断结束后已停止本轮启动的模型服务，两个 NPU 均无本轮残留进程。

## 后续工作

优先跟进稠密 GEMM 的批次形状数值稳定性，以及 GDN 在 prefill/decode 和不同分段
之间的状态一致性。继续用首次分叉前的同 token 激活定位 decode2k，确定可接受的
内核修复或具有明确限制的 checkpoint 使用策略。之后再恢复双节点验收，检查
attention/recurrent 的组合与跨节点传输，最后开展 #23 的延迟实验。
当前诊断补丁与结果均不构成混合缓存整体验收通过或性能提升的结论。
