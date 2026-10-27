# 同条件相关工作适配与迭代（2026-10-26）

本轮先实现再模拟，所有名称带 **adapter / 适配**：不是在 910B2 上复现完整论文系统，也不是论文系统的真实性能排名。当前仅 P 的 CPU 可用，定时任务保持暂停。

## 为什么选择这些对照

| 对照 | 已实现的决策 | 本轮不覆盖的部分 |
|---|---|---|
| Native RR | 固定请求轮询，公共 PegaFlow 主机缓存发现/回收模型 | 不是实际 vLLM 连续批处理测量，未建模持久 GPU prefix cache |
| Mooncake FAST25 prefill adapter | 全 P 前缀索引，比较估计传输、P 等待、prefill；D 选择已分配请求较少者；提交前完整前缀/容量检查 | 不丢弃请求、不重现 SLO 拒绝、CPP、Mooncake Store/Transfer Engine |
| MemServe Eq1/2 adapter | 先比较本地排队与 prefill，再在复制成本不超过重算节省时选更长远端前缀 | 不重现 MemPool 和独立网络实现；D 调度显式适配 |
| Strata balanced-admission adapter | oldest-first、bundle-hit 优先、负载/计算比筛选及 FIFO 回填；实际 admission CPU 延迟计入 | 不重现 GPU-assisted I/O、层级流水、delay-hit transient tree、GPU 前缀共享、存储策略；不赠送 I/O/计算重叠 |
| Tailguard FIFO reference | 现有完整资源日历、恢复边界选择和决策内精确 memo；D FIFO chunked prefill | 内部历史类名 `FairMemoSimulation`，**不是外部论文 FairMemo** |
| Tailguard release-first candidate | 只把已完成请求的同步保存提前，随后释放 KV/lease/会话依赖；其他 FIFO 顺序保持 | 不声称全局最优、不强制分配均衡、不加恢复惩罚系数 |

这些对照分别检验路由/复用、搬运是否值得、批次组织与释放阻塞。保留之前高档退化的 recovery-deadline 候选和证据，不把它按速率切换成不同算法。

一手来源：[Mooncake FAST25](https://www.usenix.org/system/files/fast25-qin.pdf) Algorithm 1、[MemServe](https://arxiv.org/html/2406.17565#S5.SS3.SSS1) Eq.1/2、[Strata OSDI26](https://www.usenix.org/system/files/osdi26-xie-zhiqiang.pdf) §4.3 Algorithm 1。

## 参数、公式与可比性

Mooncake 用 `estimated_transfer + estimated_P_queue + estimated_prefill` 选择 P。论文的 KV balancing ratio 是手动设置项，未给可直接照搬的唯一默认值。本轮明确取 **1.0：不额外增加启发式 margin**，不根据结果调参，也不称论文默认。D 的 load 是已经分配但未完成的请求数，包括尚在 P 的请求；未来未到达请求不进入计数。P 排队估计来自当前资源日历。这样提供相同的观测与成本，而不把 Tailguard 的全局尾部目标注入论文决策。

MemServe 的复制判断为 `estimated_copy <= local_prefill - restored_prefill`。两个路由适配在 P 发布后均使用**当前安全可恢复的最长 D 前缀**，不借用 Tailguard 的尾部边界搜索。所有复制最终仍受公共 allocation、lease、容量和发布检查；失败只能正常回退。

Strata 用 `sum(load_tokens) / sum(compute_tokens)` 形成批次。本轮保留论文公开的默认 **100**，明确未在 910B2 上 profile。`bundle hit` 只改变选择优先级；当前公共模型没有共享 GPU allocation，因此仍收取每条请求的实际加载/容量，不能凭共同前缀凭空消除成本。Strata 的主要 I/O 重叠依赖完整数据路径，当前 exclusive gate 世界不能据此估算完整 Strata 的收益或判定论文无效。

公共成本包括 H2D 恢复路径、全 decode、同步保存/发布、端点 gate、复制方向、路由/边界/迭代/admission CPU wall。它们已进入 TTFT 时间线，汇总时不重复相加。H2D 历史测量是含 backend/sync 的任务时间，不冒充纯 DMA。Native RR / Strata 使用原隐式复制模型的目的端保护；controlled adapters / Tailguard 使用显式双端保护。这是必须披露的公共执行接口差异，不能把所有差值归因于选择公式。

固定 95 请求、8 个完整自然会话，原 seed 和三速率 0.2451617077 / 0.4903234154 / 0.9806468308 rps；同一隔离 32 请求成本种子。2P2D、TP1、Qwen2.5-7B-Instruct BF16；D maxseq=4、128 token/chunk、无未经测量的批处理加速。设备 KV 使用 [910B2 正常预算](910b2-memory-20261026.md) 36.6748 GiB/实例，主机池仍 2 GiB。离线世界与策略不得读取测试的未来执行标签。

本轮 p99 使用常规 `causal-ready → first`；95 样本 nearest-rank p99 等于最大值。名义到达、同会话依赖等待另报。CPU 结果是筛选证据，不能称真实 A/B 或稳定显著收益。

## 代码和复现

核心机制位于 `python/pegaflow/strategies/{mooncake,baselines,strata,release_first}.py`；运行接入位于 `experiments/issue23_strategies/paper_baseline_search.py`。没有修改正在运行的服务或旧冻结 runtime。

```bash
export PYTHONPATH=python:experiments/issue23_strategies
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
python experiments/issue23_strategies/run_pcie_matrix.py \
  --inputs "$INPUT_ROOT/experiments/issue23_handoff/simulation-inputs.json" \
  --seed "$INPUT_ROOT/experiments/issue23_strategies/runtime/calibration-seed" \
  --output /root/unique-paper-adapter-result \
  --policy release-first --paper-baselines --workers 3
```

每个速率完整执行六组，不复用另一时段测得的 reference 充当同轮对照。三种适配均已走过实际小型因果请求测试。第一次封装遗漏包入口且 launcher 带 CRLF，CLI 在任何模拟开始前失败；独立 `-02` 补齐入口并使用 LF 后重新执行，未修改策略或负载，失败档保留。

## 已完成结果与修复

`paper-baselines-03` 于北京时间 18:21:46 启动、18:26:47 结束，18 组完整 CPU 记账通过。随后机制检查发现 Strata 适配的调度所有权错误：准入决策未完成时，同一个 D 调度器仍提交旧批次。**该组三档 Strata 数值不作为有效策略效果**，原档不改。修正后的独立 `strata-repair-04` 对 RR/Strata 重跑三档，共 6 组通过；其余 03 策略未改变。

常规 TTFT p99 秒（95 样本，p99=max）：

| 策略 | 0.2452 rps | 0.4903 rps | 0.9806 rps |
|---|---:|---:|---:|
| Native RR 模型，03 | 69.738 | 78.638 | 71.010 |
| Tailguard FIFO/memo，03 | 39.742 | 27.424 | 30.001 |
| Tailguard 保存优先候选，03 | 33.980 | 34.876 | 32.055 |
| Mooncake 路由适配，03 | 45.776 | 41.966 | 41.212 |
| MemServe 适配，03 | 40.115 | 34.181 | 29.837 |
| Strata 准入适配，修正 04 | 69.739 | 78.639 | 71.011 |

04 自己的匹配 RR 为 69.738/78.638/71.011 秒。不要将跨轮尾部相差几毫秒称收益。三档 Strata 实际重排次数均为 **0**：当前 8 个因果会话、D maxseq4 和队列机会没有激活该选择机制；缺少完整重叠/GPU 共享路径，因此不能据此否定完整 Strata。

控制开销归零的**机制诊断**中，修复前 Strata 与 RR 在三档分别有 26/59/79 条请求不同；修复后 **285 条 TTFT 全部逐条相同**。这只验证空操作语义，不是净收益实验，正文性能表仍包含实际 CPU 开销。记录见 `strata-zero-control-before-fix.json` 与 `strata-zero-control-diagnostic.json`。

保存优先候选相对原参考 p99 变化为 **−14.50% / +27.17% / +6.84%**，不推广。真正 save 抢在 restore 前的机会只有 **0/1/0** 次，所以不能把低档优势归因于执行了大量释放优化；预测分支及 CPU 时序也会改变后续路由/缓存轨迹。高档 D restore 总量 94.65→77.58 秒，但 compute 占用 856.77→870.89 秒，p99 仍变坏：减少恢复单项不是充分目标。这里的 compute 区间仍含混合 batch 的其他成员与已有同步成本，不等于独占 FLOPs。

相关基线产生了有用的后续优化证据：Mooncake/MemServe 路由+边界 wall 约 **5–6 秒**；Tailguard FIFO/memo 三档约 **122.39/121.16/134.25 秒**。MemServe 高档 29.837 秒与我们 30.001 秒接近，低档同样接近。不能只用相对 Native RR 的大幅改善掩盖决策复杂度的代价，也不能把 0.165 秒的单轮高档差称稳定优势。

当前只模拟隔离标定均值、无真实 NPU 连续批处理校准。图中 RR 的 p50 更小，但尾部大；我们的优化目标是 p99，仍报告完整分布，不能声称全分位领先。

[三速率 CDF](evidence/paper-adapters-20261026/paper-baselines-cdf-corrected.png)；[完整分位数/阶段/最慢请求汇总](evidence/paper-adapters-20261026/review.json)。图中 Strata 来自独立修正配对，其他曲线来自 03。

## 根据结果继续迭代：结构化服务成本代表集

下一候选 `service-portfolio` 不加 task/rate 特判，不增加惩罚权重。对当前已发布前缀构造完整可行计划，分别给每个 P/D 组合选择一个服务成本代表：

\[
c^*_{p,d}=\arg\min_{c\in\mathcal C_{p,d}}
\bigl[\widehat T_{history}(c)+\widehat T_{P,first}(c)
+\widehat T_{handoff}(c)+\widehat T_{D,first}(c)\bigr].
\]

再把 **2×2=4** 条代表送入完整资源日历，按 pending-first 尾部和释放时间选择。4 来自拓扑，不是收益拟合的搜索阈值。日历仍保留完整 decode、H2D、保存、双端复制 gate 和当前竞争。P 发布后，对全部当前可行 D 边界求 `copy + restore + compute` 最小值，不做 64 次完整 batch 重放。所有实际规划 wall 仍计入。

这是一项明确的**近似**：同一 P/D 上另一条前缀可能在竞争条件下更优，阶段最优代表不保证全局最优。源码 `service_portfolio_search.py`，冻结 source05 并独立重跑，未修改 03/04。

source05 于北京时间 18:39:16–18:41:58 完成 9 组 CPU 记账审计。净 p99 结果如下，**仍不推广该候选**：

| 名义 rps | 同轮 FIFO/memo 参考 | 服务成本代表集 | 相对参考退化 |
|---:|---:|---:|---:|
| 0.2452 | 27.0763 | 30.9179 | 14.19% |
| 0.4903 | 31.2879 | 35.6701 | 14.00% |
| 0.9806 | 32.0045 | 32.5705 | 1.77% |

它确实把三档规划+边界 wall 从 **133.44/102.60/125.29 秒**降到 **10.20/11.03/10.28 秒**，但这不足以抵消选择损失。中档 P 分发 43/52→30/65，D 分发 58/37→60/35；阶段成本最小的代表更偏向局部便宜路径，不能自动代表竞争条件下的最小尾延迟。下一步有依据的方向是保留服务/占用/复制资源之间的非支配候选，而非每个 P/D 只留一个标量最小值；不能用强制均衡比例或挑速率掩盖这个失败。数据见 [service-portfolio-review.json](evidence/paper-adapters-20261026/service-portfolio-review.json)。

必须保留的波动证据：03/05 的参考版本共同执行的资源日历、FIFO、精确 memo 和成本模块源码相同，但低档 p99 从 39.742 变为 27.076。两轮都实时测量并收取控制器 CPU wall；该时序变化会反馈到会话就绪、资源竞争、路由和缓存状态。源码一致不能把原因直接定位为 CPU wall，更不能据此声称实机硬件漂移；当前属于模拟运行的重复性限制。后续要做固定控制时间的配对机制回放与实测 CPU 预算敏感性，分开算法动作和运行计时影响，**不能选择有利的一次参考**。三个速率必须继续使用同一候选，不为每档挑不同方法。

另一个保留问题：旧正常显存矩阵中，82–88/95 个请求在原 route 预测之后遇到新进入批次的请求。完整释放端点 MAE 约 6.9–9.3 秒，首 token 约 1.0–1.5 秒。`later-admission-diagnostic.json` 只记录实测轨迹中的占用范围；混合 batch 不能拆成精确独占成本，也不能用未来请求作为在线 oracle。下一轮应对这个限制和规划成本分别诊断，不能用单一 H2D 系数补齐所有残差。

## 证据位置

P 原始目录 `/root/issue23-paper-baselines-03`、`/root/issue23-strata-repair-04`；同名完整 tar 已取回本地 `baseline-iteration-20261026` 并核整体 SHA。

- 03 完整档：`6dfc3f9c9da48b4617e70e810a92fda739193283b9afb0f883df55193d9e1779`
- 04 修正档：`bd54a24f82f5d8b88ac6f606b69e9aad5763bac9f4f9b2fc41e2ff31a2631ebe`
- 03 source：`93d31112145a35ed44641c235e728225ddd8f98e8b947ccf3e381015d391a0d4`
- 04 source：`5a2b67076d849c77cdd675bcdcacb26b37e96e0e4d4f2d1f6d7525b4f4307480`
- 05 source：`d6047a808188cfca9441d1012859e4dd614ec9409ef1e986708cb6d30399b35e`
- 05 完整档：`0940478d453493b33d54678735c14c37fc2ecf248e52c3b50486760cf94992ac`

失败 01/02、03 中失效的 Strata 机制证据、所有负收益和此前候选均保留。原始逐请求档供复核，紧凑报告入研究文档；未提交/推送，未恢复定时任务。
