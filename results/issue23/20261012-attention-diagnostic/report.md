# Issue #23 首版实验结果

2026-10-12 已完成默认优先级下的四组实验，32/32 次测量请求成功。四组的请求身份、冻结传输清单、完成字节数和传输时序均通过校验，本机从原始 client / trace 文件重算结果一致。实验服务和本轮控制进程已停止。

**当前实验范围：attention 传输诊断。Qwen3.5 的 recurrent / Mamba 状态查询仍只覆盖本地 engine，D 侧没有复用完整混合缓存，仍执行全量预填充。以下结果应按这个实际执行路径解释。**

| 实验组 | TTFT p50（秒） | TTFT p99（秒） | E2E p99（秒） | 答对题数 | 达输出上限 |
| --- | ---: | ---: | ---: | ---: | ---: |
| RDMA，错开 | 5.348 | 7.383 | 41.517 | 3/8 | 5/8 |
| 本地拷贝，错开 | 5.409 | 7.996 | 55.215 | 2/8 | 6/8 |
| RDMA，集中 | 5.288 | 7.280 | 32.013 | 3/8 | 5/8 |
| 本地拷贝，集中 | 5.555 | 8.083 | 37.121 | 2/8 | 6/8 |

每组为相同的 8 题、单轮测量。p99 使用线性插值，样本很少，接近最大值，仅作描述性统计。RDMA 集中相对错开的 TTFT p99 为 -1.4%；本地拷贝为 +1.1%。这些差异不足以支持稳定的尾延迟收益判断。

**固定配置与执行方式**

- 两节点：hust-zsx-81 为 P，zsx-91 为 D；各使用 2 张 Ascend 910B2，TP=2。每节点 8 CPU、64 GiB 容器内存；每 TP rank 一个 PegaFlow engine，NIC mlx5_2，每 peer 2 QP。
- 模型：`/data/shared_upload/models/vspec/Qwen3.5-35B-A3B`，BF16，revision `59d61f3ce65a6d9863b86d2e96597125219dc754`。vLLM / vLLM-Ascend 为 0.23.0 系列，准确源码和安装文件身份见归档中的 setup 记录。
- 数据：MMLU-Pro，math / physics 各 4 题；固定 5-shot validation 示例。为产生完整传输块，提示追加固定参考段落，实际输入 4,099–4,111 tokens。因此准确率对应这一受控提示协议。
- 请求：seed=17，temperature=0，thinking 关闭，输出上限 512，允许 EOS，超时 300 秒；使用已渲染的 completions 提示。服务启用编译图；`PYTHONHASHSEED=17`。
- 到达：每 8 秒同时发 4 个请求，共 2 个 cohort。传输门控等四请求就绪，错开目标为 0/10/20/30 ms，集中目标为共同的 15 ms。顺序为 RDMA 错开、本地拷贝错开、RDMA 集中、本地拷贝集中。每臂独立重启服务并执行 prepare、缓存清理、activation、measure、teardown。
- 调度：smooth 实际 trace 为 `SCHED_OTHER / 0`；burst 使用普通工作线程。整个实验按默认优先级执行。

**验证结果与局限**

每个 D engine 每臂完成 8 个计划 bundle、20 MiB 逻辑 fetch 数据，两 rank 合计 40 MiB；传输错误为 0。两种后端都使用同名 fetch 计数器，后端身份由 trace 确认，计数代表逻辑完成量。所有 burst cohort 的实际提交 spread 小于 1 ms，最大 0.622 ms。默认调度存在提交迟到：本地拷贝 burst 的 rank 0 首个 cohort 比目标约晚 25 ms；相关原始时刻已保留。

同样的 seed 下，各臂生成文本和长度仍有变化。以 RDMA 错开为参照，文本与 usage 完全相同的请求数分别为本地拷贝错开 1/8、RDMA 集中 3/8、本地拷贝集中 1/8；输出上限也使部分题目未生成最终答案。因此 E2E 和题目正确率不能简单归因于传输方式。

本地拷贝错开测量完成后，工作区连接中断；已保存全部响应、指标和 trace。重连后核验环境身份、确认服务退出，再执行后两组。恢复记录与此前未完成的调试轮次分别保留。

后续有效 P/D 实验首先需要支持跨节点 recurrent 状态复用，并核对 128-token 物理块与 2048-token 逻辑块的映射及完整字节覆盖；通过同输入完整输出和缓存命中验证后，再执行多轮主测。当前 `hybrid_prefix_reuse=false`、`pd_hybrid_correctness=false`，不能据此评估完整 P/D 加速或 TailGuard 收益。

**结果与复现材料**

- [汇总及环境范围](runs/smoke-pilot-result-evidence.json)、[逐臂完整分析](runs/smoke-pilot-default-summary.json)、[本机重算校验](verification.json)。
- [冻结请求合同](prepared/contract.json)、[实际请求与 token IDs（gzip）](prepared/smoke.json.gz)、[rank 0 传输计划](prepared/plans-smoke/decode-engine-0.json)、[rank 1 传输计划](prepared/plans-smoke/decode-engine-1.json)。
- [原始证据文件清单](manifest.json)：包括逐请求响应、SSE、engine trace、指标、进程命令、资格检查及源码补丁。SHA256：`89ef821292fbcb7a14d04898f359bb6d7aaecb6c46c3a81c9ed09ed75afe4f16`。
- 源码基于 `ac1b2d3e943c27e9344a9dba9115ad31f4791d43`；完整补丁为 `runtime/issue23-pilot.patch`。运行包包含 wheel 及额外 Python overlay，重建时需应用完整补丁。相关检查已通过：74 项 connector 测试、19 项准备脚本测试、10 项 Rust 核心测试。

共享目录：`/data/shared_upload/hust-zsx/tailguard-issue23-20261012`。128 题主测和 FrontierScience 请求已准备，当前结果覆盖上述 MMLU-Pro 四组接入实验。
