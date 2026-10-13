# Qwen3.5 混合缓存性能基线：81 / 91 双节点

2026-10-13，在两个 Ascend 910B2 节点上完成冷计算、本地 host 缓存复用、跨节点
host 缓存复用三条路径的性能采样，共 312 个计时请求，全部完成固定输出预算。
轻负载下，复用明显缩短 TTFT 中位数；较重负载下，请求等待和部分远端回退影响
了尾延迟。跨节点复用成功 92/104，本地复用成功 104/104。

机器为 `hust-zsx-81`（源端，192.168.0.4）和 `zsx-91`（目标端，192.168.0.2），
每台两卡、TP=2。源端物理卡 0/1，目标端物理卡 4/6。两台容器均为 8 CPU、64GiB
内存。使用 SCHED_OTHER/nice=0，RoCE 网卡 `mlx5_2`，每个 rank 一个 PegaFlow
engine，host pool 每 rank 2GB。两组计时均在目标端发起；源端生产缓存及本地
host 缓存预热在计时前完成。因此本文衡量目标端服务延迟，完整 P/D 请求链路
的成本需要另行计时。本轮三路径基线可用于准备 #23 的 smooth/burst 对照。

代码基线为 `main@f3cfb1904d967a00780b63a370c15e6c274fc396`，叠加本次 Python
遥测。native engine/binding 为已验收的 0.23.4 构建。模型目录为
`/data/shared_upload/models/vspec/Qwen3.5-35B-A3B`，revision 为
`59d61f3ce65a6d9863b86d2e96597125219dc754`。vLLM 为
`0fc695fc6d1d82e9a5ac6835ac8e4e1c83703665`，vLLM-Ascend 为
`5cb98caaadeff42b5b62b996e34bb2aaa29d20fd`，torch 2.10.0+cpu、torch-npu
2.10.0.post4、CANN 9.1。固定 BF16、eager、同步调度、APC、mamba align，
max model len 8192、max batched tokens 4096、max seqs 4、每 rank 2GiB device KV。
这组配置属于受控基线，较长上下文及其他执行模式需独立采样。

用例取自已准备的 `smoke.json` 和 `frontier-smoke.json`，各前四题，输入长度为
4097–4111 token。MMLU-Pro 的 source IDs 为 8495、8733、7942、8443；
FrontierScience 的 IDs、输入校验和及完整 payload 保存在结果 JSON 和原始归档。
用例包含准备阶段生成的固定上下文扩展；本轮用于性能采样。每轮使用新的 cache salt，
源端顺序生产八份缓存，目标端在各臂开始前清理 device APC 和自己的 host cache。
本地臂在目标端预热 host cache 后只清理 device APC；跨节点臂使用源端 host cache；
冷臂使用未命中的 salt，PegaFlow 连接器仍启用。源端 host cache 跨轮保留。

每轮八个请求分为两组，每组四个，以 20ms 间隔到达。第一组固定为 MMLU-Pro，
第二组固定为 FrontierScience，因此两组的性能差异也包含到达位置差异，不能直接
解释为数据集差异。三臂顺序逐轮循环轮换。主组执行 10 轮、固定输出 64 token、
两组间隔 4 秒；补充组执行 3 轮、输出 16 token、间隔 10 秒。均设置 ignore_eos。
两种负载同时改变输出预算和到达间隔，性能效果应优先在各负载内部比较。

TTFT 从 HTTP 提交到第一个非空文本 SSE；E2E 从提交到收到完成 SSE。表中分位数
按每臂全部请求合并计算，使用线性插值。吞吐为固定输出 token 数除以各轮计时
窗口总和，包含到达间隔。补充组每臂仅 24 个样本，其 p99 是探索性观察。

| 主组：每臂 80 请求 | TTFT p50 秒 | TTFT p95 秒 | TTFT p99 秒 | E2E p50 秒 | 输出 token/s | 真实复用 |
|---|---:|---:|---:|---:|---:|---:|
| 冷计算 | 8.433 | 15.748 | 16.355 | 23.989 | 14.96 | 0/80 |
| 本地 host | 7.324 | 15.371 | 18.980 | 22.018 | 16.18 | 80/80 |
| 跨节点 host | 8.115 | 17.006 | 19.972 | 22.399 | 15.57 | 70/80 |

| 补充组：每臂 24 请求 | TTFT p50 秒 | TTFT p95 秒 | TTFT p99 秒 | E2E p50 秒 | 输出 token/s | 真实复用 |
|---|---:|---:|---:|---:|---:|---:|
| 冷计算 | 2.214 | 3.302 | 3.316 | 6.007 | 7.66 | 0/24 |
| 本地 host | 0.792 | 1.371 | 1.411 | 4.295 | 8.82 | 24/24 |
| 跨节点 host | 0.879 | 3.433 | 3.703 | 4.490 | 8.78 | 22/24 |

补充组 TTFT p50 相对冷臂下降 64.2%（本地）和 60.3%（跨节点）。主组按每轮配对，
本地 TTFT p50 在 8/10 轮更低，跨节点在 7/10 轮更低；每轮 p99 分别在 9/10 和
7/10 轮更低。合并 p99 却更高，说明轮间波动和最慢请求值得单独检查，当前数据
不能支持稳定改善尾延迟的结论。

实际模型工作遥测覆盖全部 312 请求的两个 TP rank，首次 forward 的恢复边界
无 rank 间分歧。冷臂计算 4097–4111 个 prefill token，命中的两条复用路径从
4096 恢复，只计算剩余 1–15 个输入 token。每个命中请求的双 rank load 字节数
为 148,275,200（约 141.41MiB）。本地计时臂 RDMA 字节增量为零；跨节点主组 RDMA
总量为 10,379,264,000 字节，恰对应 70 次完整复用，补充组为 3,262,054,400 字节，
对应 22 次。device preemption 计数增量均为零。

主组第一到达组的 TTFT p50 为冷 2.378、本地 0.833、跨节点 1.210 秒；第二组为
14.724、12.252、13.528 秒。第二组到达时前四个请求仍在生成输出，且 max seqs=4。
遥测中该组首次 forward 等待中位数为 13.610、11.889、13.158 秒，说明相当一部分
延迟发生在首次计算之前。缓存查询等待、恢复完成观察与模型排队时间有重叠，
这些字段应分阶段分析，不能直接相加。

原生 RDMA fetch histogram 的平均操作耗时为主组 3.47ms、补充组 3.23ms。
这是单次原生 fetch（含控制路径）的统计，共 560/176 次操作；原生 load 的平均
耗时为主组本地 272ms、跨节点 299ms，补充组本地 114ms、跨节点 120ms，统计单位
为 load 调用/批次。这些值与按请求记录的 worker 完成观察有不同计时边界，不可
互换为单请求的纯 DMA 时间。

12 个远端回退分布在主组 r03（3 个）、r06（7 个）及补充组 r00（2 个）。源端
双 rank 累计淘汰计数在相应轮次变为 280、560、840，其余相邻轮次保持不变。
回退与淘汰波次同时发生，是需要优先核查的缓存驻留问题；原始指标尚不能逐键
证明具体淘汰对象。回退请求实际重新计算完整 prefill，计时结果保留，并通过
`real_reuse` 字段与成功恢复的请求区分。

未观察到 native load 失败、恢复边界越界或 TP 恢复边界分歧。输出文本和 token
IDs 保留为独立观察；主组 44/80、补充组 19/24 个三臂请求组合的文本 hash 完全
相同。输出差异未用于剔除性能样本。逐层 checksum、logits 探针及投影补零关闭，
本轮的语义观察沿用已归档的缓存验收基础。

新增遥测包括客户端 HTTP/SSE 时间及输出预算、scheduler 查询和 checkpoint
发布、worker load 提交/完成、实际 computed/scheduled/prompt token 数、模型
execute_model 主机跨度，以及独立的 1Hz CPU/内存/进程/网卡/原生 Prometheus
采样和每 5 个样本一次 NPU 状态。遥测写入使用有界异步队列，没有额外张量复制
或 device synchronize；本轮所有臂都开启遥测，尚未单独标定其扰动。网卡和 RoCE
设备计数可能包含同宿主其他业务流量，传输量结论使用 engine 计数增量。

六份 process telemetry 文件均可完整解析，已写入序列没有缺口；每个计时请求都
有两个 rank 的实际执行记录。退出时 vLLM 强制结束子进程，未收齐 `telemetry_closed`
记录，因此无法用关闭记录证明所有末尾事件都已落盘。归档保留该限制和原始序列。
80 项连接器相关测试及 37 项实验工具测试通过；Ruff 和 `git diff --check` 通过。

下一轮优先固定源端缓存驻留：每轮生产前清理源端旧 host cache，核对全部待用
边界仍可用，再运行计时；将驻留率与传输调度分别控制。随后增加独立 cohort 的
到达轨迹、按成功复用/回退分层统计尾延迟，并为 #23 的四臂门控补齐 recurrent
group、完整 namespace 与多组 bundle 的请求关联。

机器可读汇总见 [结果 JSON](issue23-cache-path-results-20261013.json)。完整原始记录
位于共享目录 `/data/shared_upload/hust-zsx/issue23-performance-20261013`：`runs/`
含 payload、SSE、客户端行和前后指标；`telemetry/` 和 `resources/` 含进程及资源
遥测；`runtime-provenance.json`、`source-identity.json` 和 `archive-manifest.json`
记录实际参数、代码/二进制身份及文件校验和。每组 `runtime/` 保留运行脚本。
所有本次启动的 benchmark、模型、engine、metaserver 和 collector 进程已停止，
服务器工作区保留。
