# Issue23 / Tailguard 研究交接

更新：2026-10-19。接手时先读本页，再读[研究问题、模型与相关工作](RESEARCH-PROBLEM-MODEL-RELATED-WORK.md)。**目前没有经重复确认的三速率赢家。** 本包保留有实机价值的父版本、对应的完整配对结果、最新待实测候选和可运行的 CPU 模拟入口，不把模拟收益写成实机收益。

## 1. 研究什么、已经排除了什么

场景：RDMA 互联的 P/D 分离服务，节点侧有限主机 KV 池，多会话有因果依赖。研究对象是“主机缓存命中后，状态何时真正可执行”，以及完整 decode、恢复/保存、资源释放如何将等待传播到 TTFT 尾部。问题定义不预设必须使用快慢架构、主动迁移或强制均衡。

原假说“流量突发导致 RDMA 拥塞，从而主导 p99”**没有得到当前实验证据支持**。网络约束仍保留，但不能把应用锁、H2D 或保存同步叫作 RDMA 拥塞。最接近工作包括 MemServe、Mooncake、DualPath/AMPD，以及恢复路径相关的 Strata、CacheFlow、Bidaw；完整分类和逐项重合风险见研究说明，不能宣称首次联合缓存与负载。

## 2. 固定实验范围（不是全量基准）

| 项目 | 设置 |
|---|---|
| 数据 | TerminalBench 2.1 公开 ATIF 轨迹，revision `3f5b93ff1c5ea84b2c94cf479d0c942227191b4a`，267 个原始文件校验 |
| 测试范围 | 8 个完整可容纳会话、95 请求：distribution-search、dna-insert、feal-differential-cryptanalysis、fix-code-vulnerability、fix-git、git-leak-recovery、git-multibranch、headless-terminal |
| 隔离标定 | adaptive-rejection-sampler、code-from-image，共 32 请求 / 64 个 P/D 阶段。每个 arm 重新播种，不能用 B 的测试测量初始化 T |
| 选择规则 | 既有任务 hash split；排除已使用的 5 个旧会话；按任务字典序选择前 8 个在 32K 内完整可容纳的会话。不按 reward 或收益选择，不补长、不重复任务 |
| 输入冻结 | Qwen2.5 tokenizer；完整输入在 `../issue23_strategies/runtime/workload/`，requests SHA256 `0390aad542dc55747f8fdb48195aa1baf821cd561d9a61b19b75df772a47f2b7` |
| 模型与设备 | Qwen2.5-7B-Instruct BF16，4 × Ascend 910B，2P2D，每卡 TP=1；只读已有共享权重，不下载 |
| Serving | 32K、eager、max_num_seqs=1、128-token block/prefill；每实例 2 GiB 主机池 + 2 GiB NPU KV；P 生成 1 token 后丢弃，D 对同 prompt 生成 128 greedy tokens |
| 到达 | 名义 Poisson：0.2451617077 / 0.4903234154 / 0.9806468308 requests/s，同 seed 20261014，offset 按比例缩放 |
| 因果性 | 四独立 FIFO，无全局两请求限制；每会话至多一轮在途，`ready=max(nominal, previous_full_end)`，tool/think time=0。实际入站因此不是独立 Poisson，且可随策略变化 |

这是**已暴露的开发 replay**，不执行轨迹命令，不测 agent 解题准确率，不是完整 TB2.1，也不是生产连续批处理吞吐上限或严格 zero-prefill PD。

### 指标不可混用

- 常规 TTFT：客户端首 token − causal-ready。当前优化主目标，包含真实规划、库存 RPC、门禁、恢复和迁移等待。
- nominal latency：首 token − nominal-offered，另含前轮依赖等待。早期协议以它为主目标，历史结果不重标。
- 报 mean/P50/P90/P95/P99、会话完成、实际吞吐和包含 final async drain 的 arm 时长。95 样本的 nearest-rank p99 就是最大值。
- H2D-task 嵌套在 restore 中，save RPC 包含分配/提交/同步等；它们不能与模型 API 总时长再相加。不能扣掉规划成本，也不能重复加异步墙钟。

## 3. 真实瓶颈及证据边界

已完成 22 个 arm / 2090 请求 / 4180 P/D 阶段的逐请求对齐。详见[阶段匹配证据](evidence/endpoint-drift-alignment.json.gz)和[诊断结论](evidence/endpoint-drift-findings.txt)。大 JSON 用无损 gzip 保存，解压内容哈希见 `evidence/COMPRESSED-CONTENTS.json`。主要事实：

| 同一中档 bracket 的两个 B | B-before | B-after |
|---|---:|---:|
| 正式 RDMA 字节 | 7.597 GB | 7.575 GB |
| fetch 完成等待累计 | 0.690 s | 0.671 s |
| RDMA batch 工作队列等待累计 | 4.185 ms | 5.104 ms |
| fetch 等待 / 累计常规 TTFT | 0.02041% | 0.01716% |
| D1：44 个同恢复量请求，19.216 GB H2D-task | 16.269 s | 32.959 s |
| D0：48 个恢复、保存工作量均相同请求的 save RPC | 31.729 s | 234.201 s |
| 常规 TTFT p99 | 75.369 s | 86.759 s |

最慢请求本身可以 cache=0、H2D=0；其前驱完整 decode/save 占用和 FIFO 仍影响它。失败交接还有“先等待忙碌源 gate，随后发现完整包不可用并回退”的路径：**无 RDMA 字节不代表无成本**。

固定观测路由/缓存的敏感性诊断中，仅替换 D 恢复时长未重现该 bracket 漂移，替换调度后至 first 的整体区间更接近实测。它是 trace-conditioned 记账，不是新策略因果效果。短 CANN 诊断中同 73.4 MB 的 submit 约 1.85 ms 不变，sync 从 40 ms 到 1.029 s；仍未识别 driver/stream/allocator/NUMA/宿主竞争中的唯一根因。缺连续 RNIC 遥测，不能证明网络永不拥塞。

## 4. 算法版本与实机结果

下表均为**常规 TTFT p99 秒，包含全部实测开销**。全分布、nominal latency、阶段时间和行为见 [汇总 JSON](evidence/real-results-summary.json)。不同版本的 B 不能跨轮配成因果对照。

| 版本 | 0.2452 B→T | 0.4903 B→T | 0.9806 B→T | 保留理由 / 局限 |
|---|---:|---:|---:|---|
| event-queue-01 | 110.206→80.251 | 64.601→121.256 | 131.044→84.103 | 第一版两档有利、中档严重退化；保留完整三档，不能只上传赢的两档 |
| resource-DAG-01 | 79.317→77.748 | 80.356→72.992 | 81.750→52.390 | 三个单 pair 描述性尾部改善，但低档很小，均值/部分分位退化；预热 phase salt 的严格初态限制未消除 |

以上旧 B 是**同 PegaFlow 栈 parity 路由 + 成本化恢复边界**，不是朴素原生缓存基线，更不是纯 vLLM。

最值得作为优化起点的是 **detached-calendar-live-01 / slow OFF**：共享控制代码、同物理分配、严格规范缓存初态的中档四段实测：

| 指标（秒） | B-before | T-off | T-on | B-after |
|---|---:|---:|---:|---:|
| mean | 35.581 | 33.367 | 37.069 | 41.134 |
| P50 | 34.140 | 33.219 | 41.168 | 33.016 |
| P95 | 66.820 | 48.343 | 51.733 | 83.319 |
| P99 | 75.369 | 57.598 | 57.726 | 86.759 |
| arm + final drain | 725.689 | 603.929 | 650.141 | 849.867 |

OFF p99 相对两个 B 改善 23.58% / 33.61%，仅中档单 bracket，不是三档赢家或稳定显著性。其 P/D API 返回到台账更新累计降至约 0.009 / 0.010 s，旧共享锁长持有机制得到改善；不同时间/版本间不能将全部收益归给单个修复。

ON 调用慢规划 92 次、异步 wall 69.904 s / CPU 32.375 s，有 8 个 proposal、**实际布局回执 0**（队列变更、in-flight 保护、native 冲突导致 defer）。ON 相对 OFF 的 mean 和 arm 时长变差，不能将 OFF 优势归因慢布局。保留 ON 汇总/审计作为机制证据，不再发布大量失败候选代码。

### 对应源码与数据

- `versions/event-queue-source-03.tar.gz`：event-queue-01 的冻结源；完整三档配对请求和审计在 `evidence/event-queue-01/`。
- `versions/resource-DAG-source-01.tar.gz`：resource-DAG-01；完整三档在 `evidence/resource-DAG-01/`。
- `versions/detached-calendar-live-source-02.tar.gz`：保留父版本，实际比较 `T-off`；三个有效参照 arm 的逐请求记录、严格初态合同、四段完整审计和 ON 结论在 `evidence/detached-calendar-01/`。
- 历史完整大归档未重复上传；原档 SHA/审计和本包文件 manifest 用于追溯。精简包不是完整原始日志归档。

## 5. 最新候选：需要先完成双端实测

`../issue23_strategies/runtime/` 是 source02 的冻结独立 controller/overlay；统一策略在 `python/pegaflow/strategies/`。2026-10-19 已重新读取 P 仓库，策略源码与快照一致。**未找到新候选的 P 端正式 job 目录，不存在可核实的正式收益结论**。不要将之前的 D 等待控制器、编译/CPU 检查或模型初始化称作正式完成。

候选保留：

1. 短锁不可变快照，执行进度和资源释放不被长规划锁住。
2. 当前已发布前缀和实际物理容量投影；联合选择 P/D、源、恢复/重算边界，D 服务前按真实 P 发布再决策。
3. H2D/restore、prefill+必要同步保存、first 后完整 decode/save/release 分开建模。活跃操作超出点预测不能直接视作已释放，first 事件更新剩余寿命估计。
4. 资源日历显式计源/目的 gate、真实软件复制锁，不假定物理 RDMA 拥塞。只用初始隔离标定和本 arm 已完成样本，不能喂测试未来。
5. 慢布局 OFF；完整物理动作/保护代码保留，不强迫迁移、均衡配额或按 rate/task 特判。

新 B 使用 RR 前端 + PegaFlow 默认 host lookup/replacement/隐式远端 fetch，不施加策略选定恢复上限，不调用自定义规划。**两 arm 仍共享 host-authority 测量 fixture，关闭不透明设备本地命中，因此不是完全不改动的生产 vLLM 系统比较**。这是需要单独验证的新基线，不能用旧 parity B 的性能替代。

MemServe/Mooncake 已有明确命名的策略适配和单元检查，未完整复现其系统，不计为已测基线。先超过新朴素同栈 B，再评估其他策略。

## 6. 如何做 CPU 三速率因果模拟

从仓库根目录，在单独 CPU 环境中提供 Python 3.10+、numpy、scipy、requests。无需 NPU、Torch、模型权重或 native 编译。不要安装/升级运行中容器的依赖来跑模拟。

```bash
PYTHONPATH=python python experiments/issue23_strategies/simulate_policy_feedback.py \
  --input-bundle experiments/issue23_handoff/simulation-inputs.json \
  --output /tmp/tailguard-screen-unique.json
```

输出路径必须不存在。一次运行 3 速率 × 均值/标定残差两类环境 × B/T。使用与实机相同的路由和 D 边界 solver、有限缓存动态回收、因果会话和四 FIFO，路由/边界/在线拟合实际 CPU 及 RPC/迁移进入虚拟时钟。环境阶段标签只来自隔离 32 标定，不逐请求偷看 95 个测试的真实服务时间；控制 RPC 是既有 380 个完成记录的聚合量，不是在线 oracle。

本次可复现重筛结果见 `evidence/handoff-rescreen-20261019.json.gz`：均值环境三档 p99 约 `100.72→41.96 / 87.77→35.36 / 93.05→37.83`，残差环境约 `100.46→40.16 / 86.55→34.80 / 98.42→34.80`。**只是模型条件下潜力，不是实机收益**。本次 fixture 来自已审计 detached 父版本，与原 10-16 phase-release 输入不同，不能冒充原报告重现；规划 CPU 因机器变化也不同。原 source02 模拟报告另存 `source02-original-screen.json.gz` 以便追溯。

主要精度缺口：native 来源顺序、增量发布/回收、条件同步尾部和硬件时变。能重放固定观测路径不代表能预测新策略排序。先在 2P2D 校正这些反馈，不外推 100P100D。

快速检查（pytest 仅测试所需）：

```bash
PYTHONPATH=python python -m pytest -q -o addopts='' \
  python/tests/test_strategies.py python/tests/test_strategy_resources.py \
  python/tests/test_strategy_serving_contract.py python/tests/test_strategy_mooncake.py \
  python/tests/test_strategy_residuals.py experiments/issue23_strategies/test_live_contract.py
python experiments/issue23_handoff/verify_handoff.py
```

交接时已运行三速率完整重筛、21 项契约检查和 Python compile；不声称重新做了 native/GPU 验收。`verify_handoff.py` 只验证本包哈希、请求/输出和汇总一致性，不替代历史双端完整审计。

## 7. 后续方向与避免重复的无效尝试

优先下一步：取回并审计任何遗留 job，确认没有在运行的旧作业；检查当前物理分配/显存/IP/磁盘/源码。完成新朴素 B 与 source02 的同分配、同初态、全开销三档对照，保留负收益。若只中档有潜力，按实际路径反馈继续优化，不承诺一定三档全赢。

机制定位优先补：真实 registered KV/pool 的 submit、sync、publication 和 gate 尝试/取得/释放子时钟，以及 first 前后 save 分段。检查完整 D 占用、失败交接先等待后回退、模型条件支持域和瞬时目标偏差；不把总端点预测误差全部叫排队误差。

已知无效/未证明方向：对恢复统一加罚、直接叠加各阶段 p99、把同工作量漂移归因网络、驻留字节 tie 奖励、长共享锁内扫描库存、预算内未完成评分仍强行动作、为了快慢架构保留无回执慢规划。多个尾部/geometry/affine/场景成本 CPU 候选不稳，未作为成果上传。少数实机版本倾向单 P 集中，说明局部估计/目标替代失真；不要用硬编码 50/50 配额掩盖，要预测被集中节点的完整占用和下游影响。

定时任务保持暂停，本次不启动实验。保留安全输出、generation/lease/in-flight/staging、唯一物理 allocation 计费和 SYNC_SAVE 发布屏障；不能用 cleanup-all 冒充选择性逐出。版本更新始终遵循：证据→可检验解释→公式/实现→因果模拟粗筛→有界实机→双端审计→反馈。

## 8. 证据与环境入口

`SOURCE-PROVENANCE.json` 记录 P 端核对、快照与基线 commit；`MANIFEST.sha256` 对应本包。原始权威工作区为 P 的 `/root/pegaflow-hust`，稳定 runtime 为 `/root/issue23-tools/pegaflow-joint-pipeline-01/.venv`，实际 vLLM/Ascend 来自 `/vllm-workspace`。主机/card/容器状态均不可凭旧编号假设。未上传密码、token、私钥、权重、虚拟环境或完整失败日志。
