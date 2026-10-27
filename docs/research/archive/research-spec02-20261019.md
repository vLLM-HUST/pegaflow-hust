# Tailguard：研究问题、数学建模与相关工作

更新：2026-10-19；版本：research-spec-02（交接版）。本文是持续维护的研究说明，不是已完成的创新性认证，也不表示以下全部建模项已部署。

本文初稿于 10-16 按原 PPT 的问题写法和公式组织整理，文献保留原三类划分。10-19 已按用户授权将 PPT 前三页内容与本文对齐，并保留原版式。既有实验结果与暂停的定时任务不变。首次整理时 PPT SHA256：`623b57bc077b20333319f0d0fa6278bccb06bf9030a249c8a99adc12da352a98`。

## 0. 先给结论

1. 原来的“跨节点流量突发造成 RDMA 拥塞，进而主导 TTFT p99”不能作为当前实验已经证实的问题。已归档数据支持的重点是**端点服务、状态恢复/保存同步、资源释放与跨请求排队**，不是持续的 RDMA 内部排队。
2. **P/D 分离、RDMA、两侧共享 KV 池、多轮前缀复用，均不能单独构成独特背景。** MemServe、Mooncake、DualPath、AMPD 已有很强重合；H2D、恢复/重算与多请求 I/O 竞争又与 Strata、CacheFlow、Bidaw 等重合。
3. 推荐收窄为：**无未来请求提示、有限节点侧主机 KV 池的 P/D 分离服务中，已缓存状态的跨阶段可执行性延迟，以及由此产生的 TTFT 尾部放大。** RDMA 是保留的系统条件，不再预设为唯一瓶颈。
4. 这仍是**待验证的差异化研究切口**，不是“没人做过”的结论。尤其要对照 2026 年的新工作；若现象只由本原型过粗的锁或同步缺陷造成，应先修复工程问题，不能包装成普遍的新研究问题。
5. 数学上保留原来的请求、P/D 节点、对象驻留、容量、方向带宽、因果策略与 p99 目标；主要修正**命中量的可行域、操作资源依赖、完整 decode 的释放时刻和关键路径计时**。三个分量时间是中间量，策略才是决策输出。

下面“文献已覆盖”均附原始来源；“我们的差异/待验证空白”是本次比较判断，不是论文作者的声明。

## 1. 研究问题：只描述客观问题，不写具体做法

### 1.1 推荐标题

**RDMA 互联的 P/D 分离服务中，有限主机 KV 缓存复用的跨阶段等待与 TTFT 尾部放大**

标题中的 RDMA 说明环境；“跨阶段等待”说明对象；“TTFT 尾部”说明关心的现象。标题不包含联合规划器、快慢动作、预测器、迁移算法等解决方案。

### 1.2 对应 PPT 第一面风格的问题正文

> 在 P/D 分离的多轮 LLM 服务中，prefill 和 decode 运行在不同节点。为容纳和复用历史状态，两侧有限的 CPU memory 共同保存 KV 缓存，并通过 RDMA 进行远端访问。一个前缀即使命中主机缓存，也要经过必要的数据搬运、状态发布和设备恢复，才能被当前请求实际使用。
>
> 当多个会话共享这些端点资源时，缓存命中与可执行时刻并不一致：前序请求的完整 decode、KV 恢复与保存同步，以及源端和目的端的资源释放，均可能推迟后续请求开始计算。即使 RDMA 传输较快且未出现显著网络排队，这些等待仍会沿 P/D 交接和会话依赖传播，使减少重计算或提高缓存命中率未必降低首 token 等待，甚至放大 TTFT 的尾部。

第二段是由当前系统观测支持、仍需跨实现检验的机制描述，不等于断言任意 P/D 服务都如此。**问题正文到此为止；不在这里接“因此我们设计某某算法”。**

### 1.3 必须与问题分开的研究问句

研究问句是：在上述场景下，什么情况下复用一个已缓存前缀会延长当前或其他请求的首 token 等待？端点状态可用性与资源释放怎样形成跨请求尾部放大，且该影响在扣除不了的真实控制、恢复、保存和迁移开销下是否仍然重要？

这是需要解释和验证的问题，不预先规定必须有慢规划、主动迁移或提高命中率。没有必要动作时，保持布局不变是合法结果。

### 1.4 场景限定及其合理性

| 限定 | 为什么保留 | 能否据此宣称独特 |
|---|---|---|
| 多个 P、D 实例独立排队，节点通过 RDMA 互联 | 需要研究源端、目的端及后续请求的交叉依赖，不只是一个请求的加载时间 | 不能；MemServe、Mooncake、DualPath、AMPD 已有相关背景 |
| 节点侧有限 CPU KV 池，HBM 为实际计算层 | 主机命中仍需恢复；驻留、暂存、在途引用会竞争容量 | 不能；已有分层缓存系统充分研究了这一背景 |
| 同模型、合法完整前缀、保持语义正确的状态复用 | 不把近似 token 丢弃、提示词重排或有损压缩的收益混入调度收益 | 是研究边界，不是创新点；输出完全一致还受数值与执行配置影响 |
| 多轮/agent 会话，未来请求内容与精确返回时间未知 | 在线决策不能利用测试未来；前一轮完整结束会影响后继请求 | 不能；AMPD、Continuum、PBKV、AgentServeSim 等已有相关处理 |
| 缓存公开可见与设备可执行之间有实际恢复、发布及释放步骤 | 区分“有这个 key”“有可用完整状态”“现在能执行” | 可作为机制研究焦点，但 Compositional Modeling 等已明确区分这些状态 |
| 观测到网络未主导的运行区间 | 避免把端点等待全部归因于网络；同一模型仍保留网络瓶颈情况 | 条件性切片，不是“网络永不拥塞”的假设或人为限流制造的新问题 |

不建议用“仅 Ascend 910B”“只允许一条请求”“必须使用我们的粗粒度锁”作为独特性限定。这些是实验平台或原型限制，不能代替真实系统问题。

## 2. 观测依据：哪些已知，哪些还不知道

本节来自项目归档，不来自相关工作的性能数字。精简交接包保留[阶段诊断结论](evidence/endpoint-drift-findings.txt)及[严格匹配与传输对齐数据](evidence/endpoint-drift-alignment.json.gz)。完整 22-arm 原始归档仍保留在历史工作区，不将本精简包冒充完整日志归档。

### 2.1 已观测事实

覆盖 22 个 arm、2090 条请求记录、4180 个 P/D 阶段，连接了 1070 次正式请求复制的回执与 native 日志。最近完整四段对照中：

| 项目 | B-before | B-after | 正确解释 |
|---|---:|---:|---|
| 正式请求 RDMA 字节 | 7.597 GB | 7.575 GB | 不是链路利用率 |
| fetch 完成等待累计 | 0.68995 s | 0.67058 s | 含完成观察，不是纯 DMA 时间 |
| RDMA batch 工作队列等待累计 | 4.185 ms | 5.104 ms | 不能用应用锁等待替代它 |
| fetch 等待 / 累计常规 TTFT | 0.02041% | 0.01716% | 是累计间隔之比，不能直接当可消除收益 |
| D1 同 44 请求、19.216 GB 的 H2D-task | 16.269 s | 32.959 s | 相同工作量仍有明显时变 |
| D0 严格同恢复与保存工作量的 48 请求：save RPC | 31.729 s | 234.201 s | 包含同步等，不能称纯 D2H 带宽变慢 |

最慢请求实例 `feal-differential-cryptanalysis:14`：两个 B 均 cache=0、H2D=0、保存字节和批数相同，save RPC 从 2.335 s 到 18.613 s；常规 TTFT 从 71.630 s 到 86.759 s。它说明尾部变化不能单独由恢复字节或 RDMA 解释。

另有交接尝试最终回退、没有 RDMA 字节，却与忙碌源 P 的 API 窗口大幅重叠。当前实现会先取得相关资源，再确认可复制完整包；**等待后不发生传输也是实际成本**。现有遥测还不能把这一窗口全部精确分成锁等待、库存 RPC 和失败检查。

### 2.2 证据支持的解释

- 完整 decode 和结束保存会影响资源释放；虽然发生在本请求首 token 之后，却能延迟其他请求的 TTFT。
- 同工作量服务时间变化经 FIFO、交接和会话依赖传播，能形成远大于单次搬运差异的尾部变化。
- 快 RDMA 不等于快恢复；主机到设备、发布/同步、源端与目的端就绪均是不同步骤。
- 高命中量不必然最优；重算也可能更便宜，但重算会增加计算、保存和后续占用，不能只给“恢复”单边加罚。

### 2.3 尚未证明的部分

- 没有连续 RNIC 拥塞、ECN/PFC、重传和端口利用率遥测，不能认证网络从不拥塞。
- 各方向应用层完整 fetch 并发受互斥约束；8 个因果会话与当前串行实例配置也限制实际压力。名义 Poisson 突发不等于真实并发网络突发。
- 尚未隔离 driver、设备 stream、allocator、NUMA、共享宿主干扰中的唯一根因；H2D-task 同步时间不是纯 DMA 时间。
- 原型的 source/destination gate、同步保存契约可能放大现象。要区分正确性必需依赖与可以消除的过度串行化。
- 95 个已暴露请求、单 seed、nearest-rank p99=max，不足以支持稳定显著性或生产网络的普遍结论。

因此，**“网络突发导致拥塞”改为待验证分支；“端点可执行性等待造成尾部放大”是现阶段更有证据的研究主线。** 这不是把 H2D 宣布成新的唯一根因。

## 3. 相关工作的组织、排序和核实标准

沿用 PPT 三类，不重新发明分类以避开竞争工作：

1. **A：KV-cache-aware 请求路由与放置。**
2. **B：跨节点 KV 缓存池与远程访问管理。**
3. **C：KV 获取、传输与重算优化。**

另外单列 D（P/D 架构、调度与资源释放）、E（建模、模拟与测量），用于解释本项目现在发现的问题；它们是补充，不替换前三类。论文可跨类，按主要贡献归档，避免重复计数。

相关度标记不是打分参数：

- **S0：背景高度重合或直接威胁问题/建模新颖性，必须优先精读。**
- **S1：关键机制或必须有的比较对象，不能以一句“没有考虑”略过。**
- **S2：基础、替代架构或不同语义目标，用于界定范围。**

核实深度：**F**=已核对原文相关章节；**A**=只核官方摘要/元数据，不据此断言正文没有某能力；**I**=官方实现/文档。F 不表示已复现论文，也不表示逐字通读全部附录。论文 venue 与预印本版本分开；不能把 under-review PDF 当已录用。

### 3.1 跨类别优先阅读顺序

| 优先级 | 工作 | 最直接的重合/风险 |
|---|---|---|
| 第一组：同背景 | MemServe；Mooncake；DualPath；AMPD | 已覆盖 P/D、共享或历史 KV、路由/交接；加入 RDMA 和多轮不足以区分 |
| 第二组：同瓶颈 | Strata；CacheFlow；When the KV Cache Is Slower Than the GPU；Bidaw | 已研究恢复/计算竞争、同步加载、缓存导致排队或尾部退化 |
| 第三组：同模型风险 | Compositional Performance Modeling；Calibrate, Then Route | 前者已有缓存可用性、容量与资源事件组合；后者直接揭示串行模拟错判和贪心分发偏斜 |
| 第四组：主要对照 | SYMPHONY；Preble；LBGR+RLT；LMetric；LMCache；Cake/HCache | 提示式状态管理、低开销路由、恢复/重算等不能遗漏 |

“第一组”之间不作虚假的精确强弱评分；它们分别覆盖不同侧面，均应在问题陈述前核查。

## 4. A 类：KV-cache-aware 请求路由与放置

以下先列更贴近当前研究的工作，再列可作为边界或基线的工作。

| ID / 相关度 / 核实 | 工作与来源 | 已覆盖的内容 | 对本项目的含义 |
|---|---|---|---|
| A01 / S0 / F | **AMPD：Efficient Multi-round LLM Inference over Disaggregated Serving**，2026；[arXiv v2，2026-07-21](https://arxiv.org/html/2602.14516v2) | 多轮增量 prefill、P/D 自适应执行位置、prefill 重排、部署规划；Dynamo/NIXL RDMA，历史 KV lazy read | “多轮＋PD＋RDMA”背景直接重合。区别必须落在有限主机池、状态可用性/释放和尾部机制，不是声称它未考虑调度 |
| A06 / S0–S1 / F | **Calibrate, Then Route: A Measured Study of Learned Request Routing for Disaggregated LLM Serving**，2026-09 预印本；[原文 §VI](https://arxiv.org/html/2609.16206v1) | 实测 PD 成本路由；报告贪心在稀缺资源下集中分发，模拟串行化夸大 HOL，校准后仍未复现策略排名 | 与我们“单节点偏斜＋模拟失真”高度相似；不是单靠换估计器就可宣称新问题。其测量为单节点 NIXL、3B 模型，正文明确跨节点 RDMA 为范围外 |
| A02 / S1 / F | **Preble**，ICLR 2025；[正式论文](https://openreview.net/pdf?id=meKEKDhdnx)，[会议页](https://proceedings.iclr.cc/paper_files/paper/2025/hash/5bc342f48de8264779952fac378f96dc-Abstract-Conference.html) | 分布式共享前缀调度、局部性与计算负载平衡、全局/本地层级调度，评估 p99 | 不能声称我们首次把缓存和负载联合。其主要单位是完整模型 worker，不等于本项目 P/D 两阶段主机恢复链 |
| A03 / S1 / A | **LMetric: Simple is Better—Multiplication May Be All You Need for LLM Request Scheduling**，2026；[原文入口](https://arxiv.org/abs/2603.15202) | 新 prefill token 数与当前 batch size 的乘法指标；分析失效条件 | 是避免魔法权重的直接参考和低开销对照；不能把“无调参”当我们的专属贡献。venue 不据二手介绍补写 |
| A04 / S1 / F | **Randomization Boosts KV Caching, Learning Balances Query Load: A Joint Perspective（LBGR+RLT）**，ICLR 2026；[正式 PDF](https://openreview.net/pdf?id=R7fv5NWfMm) | 随机前缀逐出、学习型负载感知路由、理论分析 | 已有在线学习与 cache/load 联合视角；是否覆盖特定主机恢复和发布路径要对公式及执行范围逐项比对，不能笼统说不考虑排队 |
| A05 / S1 / A | **Online Context Caching for Distributed Large Language Models Serving**，INFOCOM 2025；[IEEE](https://ieeexplore.ieee.org/document/11044599/)，DOI `10.1109/INFOCOM55648.2025.11044599` | 在线缓存放置与请求调度的时序耦合、优化/在线算法 | 已是联合放置与调度；本轮正文未取得，关于 RDMA、排队细节和理论假设均待核，不能用摘要缺词作否定证据 |
| A07 / S1 / A | **CacheRoute: Planned Prefix-Affinity Routing for Large-Scale LLM Serving**，2026-08 预印本；[原文入口](https://arxiv.org/abs/2608.19677) | 规划 warm-set/前缀亲和性与负载，报告有利与不利情形 | “局部性可能造成倾斜”已有讨论；应比较实际资源成本和动态可用性，而非强制 50/50 分发 |
| A08 / S1 / A | **Locality-aware Fair Scheduling in LLM Serving（DLPM、D²LPM）**，2025 预印本；[原文](https://arxiv.org/abs/2501.14312) | 局部/分布式局部性、负载与公平保证 | 可作调度边界；我们的主要目标是整体 p99，不能未经需求就加入结果拟合的公平阈值 |
| A09 / S1 / A | **NetKV: Network-Aware Decode Instance Selection for Disaggregated LLM Inference**，2026-06 预印本；[原文](https://arxiv.org/abs/2606.03910) | 网络成本 oracle 驱动 D 选择；拓扑/拥塞，64-GPU 模拟 | 原“网络感知 PD 路由”方向也已有直接工作；其摘要结果是模拟，不能写成已验证我们硬件上的 RDMA 瓶颈 |
| A10 / S1 / A | **GORGO: Maximizing KV-Cache Reuse While Minimizing Network Latency in Cross-Region LLM Load Balancing**，2026-02 预印本；[原文](https://arxiv.org/abs/2602.11688) | 跨地域网络、前缀局部性与负载的联合路由 | WAN 请求转发与本机 H2D/PD 交接不同，但“网络＋缓存＋负载联合”不是空白 |
| A11 / S1 / A | **QoS-Aware Prefix Orchestration for Geo-Distributed LLM-as-a-Service**，ICWS 2026；[IEEE](https://ieeexplore.ieee.org/abstract/document/11670114/) | 全局前缀路由、执行次序、保守分层命中边界，异构地域 PP | 已涉及边界正确性和次序协同；正文未取得，不宣称其无资源/同步模型 |

AMPD 的核实点：§4 在线路由/重排，§5 部署模型，§6 NIXL RDMA 与历史 lazy read，§7 实验。论文实验为 4 台服务器、每台 8 张 H20；不能照搬其绝对请求速率到我们的 4 张 910B。其目标以 SLO attainment/部署为主，但“我们用 p99”本身也不足以形成新颖性。

## 5. B 类：跨节点 KV 缓存池与远程访问管理

| ID / 相关度 / 核实 | 工作与来源 | 已覆盖的内容 | 对本项目的含义 |
|---|---|---|---|
| B01 / S0 / F | **MemServe: Flexible Mem Pool for Building Disaggregated LLM Serving with Caching**，2024 起的预印本；[原文](https://arxiv.org/html/2406.17565) | 弹性 GPU/CPU MemPool，P/D 历史缓存、D→P 复用、全局 prompt tree；§5.3 比较排队/执行和远端读取是否节省计算 | 是背景最接近的工作之一。其实验传输实现与原生 RDMA host pool 不同，不等于它的研究问题完全不同 |
| B02 / S0 / F | **Mooncake: Trading More Storage for Less Computation—A KVCache-centric Architecture for Serving LLM Chatbot**，FAST 2025；[官方页](https://www.usenix.org/conference/fast25/presentation/qin)，[论文](https://www.usenix.org/system/files/fast25-qin.pdf) | PD、分布式 DRAM/SSD KV 池、RDMA、Conductor；预测计算/排队/传输，考虑网络状态和拥塞发送方，热副本与负载 | 原 PPT“缺少统一缓存/调度/网络考虑”不成立；它还分解 DRAM→HBM 路径，不能把 H2D 首次加入模型当创新 |
| B03 / S0 / F | **DualPath: Breaking the Storage Bandwidth Bottleneck in Agentic LLM Inference**，2026；[原文 v2](https://arxiv.org/html/2602.21548v2) | 多轮 agent、PD、从 P/D 两侧读外部存储后 RDMA 回送；联合 PE/DE/读路径，涉及 H2D/D2H、PCIe/QoS 竞争 | “两侧资源协同”已存在。其外部 3FS 存储 NIC 瓶颈与节点侧有限 CPU 池不同，但不能把所有 I/O 争用声称为遗漏。未用二手 venue/title 替换此版本 |
| B04 / S0 / F | **SYMPHONY: Enabling Compute-Memory Disaggregation in LLM Serving Systems**，NSDI 2026；[官方页](https://www.usenix.org/conference/nsdi26/presentation/agarwal)，[论文](https://www.usenix.org/system/files/nsdi26-agarwal.pdf) | compute/state 分离、多轮工作负载、请求 advisory、分层缓存和协作 HBM 管理 | 无未来提示可与它的提示式路径区分，但它也讨论提示缺失，不能简单说“必须已知未来”；已核章节未确认 RDMA 具体传输栈 |
| B05 / S0–S1 / A+I | **LMCache: An Efficient KV Cache Layer for Enterprise-Scale LLM Inference**，2025；[技术报告](https://arxiv.org/abs/2510.09665)，[作者代码](https://github.com/LMCache/LMCache) | 多层 KV offload/reuse、引擎集成、PD 连接、批量传输和计算/I/O pipeline | 既是系统底座也是恢复路径对照；不能把接入 API 本身当新策略或完整论文基线 |
| B06 / S1 / F | **Bidaw: Enhancing Key-Value Caching for Interactive LLM Serving via Bidirectional Computation–Storage Awareness**，FAST 2026；[论文](https://www.usenix.org/system/files/fast26-hu-shipeng.pdf) | 计算与存储双向感知，load-aware 队列、分层缓存、复用/逐出、尾部和调度开销 | 已覆盖“命中不等于免费”。其异步写假设与本项目 SYNC_SAVE 安全契约不同；先核实必要同步依赖，不能为了区分而强制串行写 |
| B07 / S1 / F | **CachedAttention: Cost-Efficient Large Language Model Serving for Multi-turn Conversations**，ATC 2024；[官方论文](https://www.usenix.org/conference/atc24/presentation/gao-bin-cost) | 多轮历史分层驻留、layer prefetch、异步保存、调度协同 | 2024 年已有 H2D/保存路径优化；我们的贡献必须比“计入搬运时间”更具体 |
| B08 / S1 / A | **Continuum: Efficient and Robust Multi-Turn LLM Agent Scheduling with KV Cache Time-to-Live**，2025–2026；[原文，arXiv 2511.02230](https://arxiv.org/abs/2511.02230) | 工具间隙的状态保留、TTL、重载/重算与程序调度 | 多轮外部返回不确定已有研究；目标和执行范围与单请求 p99 区分，但不可称全无排队 |
| B09 / S1 / A+I | **KVFlow: Efficient Prefix Caching for Accelerating LLM-Based Multi-Agent Workflows**，NeurIPS 2025；[论文](https://arxiv.org/abs/2507.07400)，[作者实现](https://github.com/PanZaifeng/KVFlow) | Agent Step Graph、共享前缀、执行距离驱动逐出、CPU→GPU 后台预取 | 已知 workflow 信息与无提示在线场景不同；预取/H2D 不是我们的新机制 |
| B10 / S1 / A | **Efficient Serving for Dynamic Agent Workflows with Prediction-based KV-Cache Management（PBKV）**，2026；[原文](https://arxiv.org/abs/2605.06472) | 预测动态 agent 调用，保留/逐出/预取，考虑预测错误 | “动态多轮、没有完美未来”也不足以避开它；需核实际误差/资源模型 |
| B11 / S1 / F | **InferCept: Efficient Intercept Support for Augmented Large Language Model Inference**，ICML 2024；[论文](https://arxiv.org/pdf/2402.01869) | 外部调用中断时 preserve/swap/discard，控制恢复与冗余计算 | 多轮暂停后的状态取舍已有直接基础工作；不要误写成 ASPLOS |
| B12 / S1 / A | **TokenCake: A KV-Cache-centric Serving Framework for LLM-based Multi-Agent Applications**，2025 起；[原文](https://arxiv.org/abs/2510.18586) | Agent KV 的时间/空间协同、工具间隙 offload 与恢复 | 相关的不确定返回/驻留决策，完整 PD 端点范围需进一步核实 |
| B13 / S1 / A | **MARS: Efficient, Adaptive Co-Scheduling for Heterogeneous Agentic Systems**，2026；[原文](https://arxiv.org/abs/2604.26963) | GPU 推理与 CPU 工具耦合、准入/执行分离、关键路径及 KV 保留 | 多资源关键路径不是新概念；本项目不执行轨迹中的工具，不能把 replay 当作它的真实 GPU–CPU agent 实验 |
| B14 / S1 / A | **AugServe: Adaptive Request Scheduling for Augmented Large Language Model Inference Serving**，2025；[原文](https://arxiv.org/abs/2512.04013) | 增强式多轮推理的排队与有效吞吐调度 | 需要纳入多轮调度相关工作；是否覆盖具体分布式池不能从摘要判断 |
| B16 / S1 / A | **Fast Inference for Augmented Large Language Models（MARS）**，NeurIPS 2025；[正式论文页](https://proceedings.neurips.cc/paper_files/paper/2025/hash/679b0e02978404ee569ce2abfb8455cf-Abstract-Conference.html) | 内存感知调度结合 API 期间状态处理，预测性请求优先级 | 与 B13 的 2026 MARS 不是同一篇；早期 arXiv 2410.18248 使用 LAMPS 名称，不能混成一个版本 |
| B17 / S1 / F | **Agentix: An Efficient Serving Engine for LLM Agents as General Programs**，NSDI 2026；[官方页](https://www.usenix.org/conference/nsdi26/presentation/luo)，[预发表正文](https://www.usenix.org/system/files/conference/nsdi26/nsdi26spring_luo_prepub.pdf) | 程序/调用层 HOL，基于已完成服务的 non-clairvoyant 调度与局部性路由 | 无完整未来 DAG、跨轮累计阻塞已经被研究；其程序完成目标不等于本项目逐请求 p99 |
| B15 / S2 / A | **TraCT**，2025；[原文](https://arxiv.org/abs/2512.18194) | 机架 CXL 共享 KV 与分离计算 | 替代数据路径/硬件；“共享状态＋同步”不局限于 RDMA，但不能用 CXL 的收益冒充我们的平台 |

MemServe 重点核 §5.3：它已经用未完成工作估计队列，并比较远端额外传输与节省计算。Mooncake 重点核 §4.1、§4.2、§5.4：它不把网络视为恒定免费资源。DualPath 的外部存储架构包含独立计算/存储网络与每节点多 NIC；这种瓶颈结构不能不加区别地转移到当前实验。

## 6. C 类：KV 获取、传输与重算优化

| ID / 相关度 / 核实 | 工作与来源 | 已覆盖的内容 | 对本项目的含义 |
|---|---|---|---|
| C01 / S0 / F | **Strata: Hierarchical Context Caching for Long Context Language Model Serving**，OSDI 2026；[官方页](https://www.usenix.org/conference/osdi26/presentation/xie-zhiqiang)，[预印本正文](https://arxiv.org/html/2508.18572) | 小碎片搬运、加载阻塞、delay hit、GPU-assisted I/O、负载/批次与加载重叠 | “H2D 导致低利用率/排队、调度需感知恢复”已覆盖；已核预印本 §4.1 为 P/D co-location，不能等同独立 P/D 两端调度 |
| C02 / S0 / F | **CacheFlow: Efficient LLM Serving via Automated 3D-Parallel KV Cache Restoration**，2026；[v2，2026-09-26](https://arxiv.org/html/2604.25080v2) | token/layer/GPU 维度的恢复/重算；batch 内跨请求共享 I/O；hybrid 状态；尾部和规划开销 | “已有获取优化都只管单请求”已经不成立。其 batch/model-shard 内计划与独立 P/D 实例的全局驻留/释放不同，但仍是最强机制对照之一 |
| C03 / S0 / F | **When the KV Cache Is Slower Than the GPU: Scaling Remote KV Tiers for Context-Parallel Prefill**，2026-10-15 预印本；[正文](https://arxiv.org/html/2610.21500v1) | 远端命中慢于重算、同步 load 形成不稳定队列、固定命中开销、分片与异步加载 | 与我们现象极接近，不能遗漏。正文 §8 明确：主要 replay 是一个 prefill engine、无 decode handoff，多 engine 竞争是估算；不是完整多 P/D 主机池验证 |
| C04 / S1 / F | **ShadowServe: Interference-Free KV Cache Fetching for Distributed Prefix Caching**，2025 预印本；[论文](https://arxiv.org/abs/2509.16857)，[作者 PDF](https://xyxiang7.github.io/files/shadowserve_preprint.pdf) | fetch/解压与 CPU/GPU 推理干扰，SmartNIC 数据平面，低带宽情形 | “网络完成之外还有端点干扰”不是我们的首次发现；其 SmartNIC 与压缩路径不同，不能只拿 headline speedup 横比 |
| C05 / S1 / F | **Cake: Compute or Load KV Cache? Why Not Both?**，ICML 2025；[正式论文页](https://proceedings.mlr.press/v267/jin25d.html) | 双向并行加载/重算，自适应资源，和现有服务整合 | 加载/重算联合已成熟。注意它预存评估 KV，部分 I/O 带宽以延迟模拟控制；不要把该实验当真实拥塞证明。不是同名 CAKE token eviction |
| C06 / S1 / F | **HCache: Fast State Restoration in LLM Serving**，EuroSys 2025；[论文](https://chenyoumin1993.github.io/papers/eurosys25-hcache.pdf)，[arXiv](https://arxiv.org/abs/2410.05004) | 中间状态恢复、部分重算、存储组织和调度 | 状态表示/恢复路径对照；本项目主要研究未改状态表示下的端点队列与布局取舍 |
| C07 / S1 / A | **KVPR: Efficient LLM Inference with I/O-Aware KV Cache Partial Recomputation**，Findings of ACL 2025；[官方页](https://aclanthology.org/2025.findings-acl.997/) | CPU–GPU PCIe 瓶颈，partial activation/重算与并发传输，自动 profiler/scheduler/runtime | H2D 与部分重算的具体机制已经存在，不可遗漏 |
| C08 / S1 / A | **eLLM: High Throughput and Low Latency LLM Serving via Adaptive KV Caching**，EuroSys 2026；[ACM](https://doi.org/10.1145/3767295.3803570) | token-wise partial caching，batch/uncached ratio，层级计算通信重叠与 kernel fusion | 不是只从命中率考虑缓存；完整正文待补，不能宣称无共享资源分析 |
| C09 / S1 / A | **KVServe: Service-Aware KV Cache Compression for Communication-Efficient Disaggregated LLM Serving**，SIGCOMM 2026；[原文](https://arxiv.org/abs/2605.13734) | 服务感知压缩 profile、SLO/带宽在线适配及离线–在线差异 | 原 PPT 的网络获取类必须更新；压缩、质量约束与本项目不改变 KV 表示的边界不同 |
| C10 / S1 / A | **KVCodec: Efficient Remote KV Cache Reuse with GPU-native Video Codec**，SIGCOMM 2026；[原文](https://arxiv.org/abs/2602.09725) | GPU 原生 codec、远端取回/解码 pipeline、端点干扰 | 不能把“考虑恢复端代价”当空白；论文的 accuracy 保持不应自动解释为 KV 字节完全无损 |
| C11 / S1 / F | **CacheGen: KV Cache Compression and Streaming for Fast Large Language Model Serving**，SIGCOMM 2024；[正式论文](https://cs.stanford.edu/~keithw/sigcomm2024/sigcomm24-final1571-acmpaginated.pdf)，[作者代码](https://github.com/UChi-JCL/CacheGen) | 远端 KV 压缩/流式加载，随带宽适配，计算与传输分解 | 带宽受限恢复的基础对照；其压缩语义与精确前缀调度不同，不能据此否认其端到端优化 |
| C12 / S1 / A | **EdgeFlow: Efficient KV Cache Migration for Geo-Distributed LLM Inference in Collaborative Edge Computing**，ICDCS 2026；[IEEE](https://ieeexplore.ieee.org/abstract/document/11619197) | KV/activation 混合迁移；多节点迁移优化与近似算法 | **原“只优化一次获取/固定源目的”概括不成立。** 地域边缘带宽结构不同；正文未取得，更多排队细节未知 |
| C13 / S1 / A | **OrbitFlow: SLO-Aware Long-Context LLM Serving with Fine-Grained KV Cache Reconfiguration**，VLDB 2026；[原文](https://arxiv.org/abs/2601.10729) | 分层 KV 放置、CPU–GPU 搬运、内存波动、运行时反馈与延迟约束 | 资源/搬运联合已有研究，须对比不同调度粒度和目标 |
| C14 / S1 / A | **LayerKV: Optimizing Large Language Model Serving with Layer-wise KV Cache Management**，2024；[原文](https://arxiv.org/abs/2410.00428) | KV 内存分配导致排队，层粒度管理与 SLO 调度 | 排队不能只归于计算；venue 本轮未核，不补写录用 |
| C15 / S1 / A | **CacheOPT: Mitigating KV Cache Competition to Enhance User Experience in LLM Inference**，2025；[原文](https://arxiv.org/abs/2503.13773) | KV 竞争、输出长度不确定、容量保留/抢占、swap 与重算 | 已考虑内存竞争的尾部影响，不是只提高命中 |
| C16 / S1 / A | **Tutti: Making SSD-Backed KV Cache Practical for Long-Context LLM Serving**，2026；[原文](https://arxiv.org/abs/2605.03375) | GPU direct object I/O、异步存储、slack-aware 调度避免设备资源竞争 | 相关恢复干扰机制，但 SSD 直达架构不是我们当前 CPU pool 数据路径 |
| C17 / S2 / A | **CacheBlend: Fast Large Language Model Serving for RAG with Cached Knowledge Fusion**，EuroSys 2025；[原文](https://arxiv.org/abs/2405.16444) | 非前缀 RAG 复用、选择性重算/融合 | 语义/上下文处理不同，不和完整前缀 exact-output 直接混算 |
| C18 / S2 / A+I | **ContextPilot: Fast Long-Context Inference via Context Reuse**，MLSys 2026；[官方论文页](https://proceedings.mlsys.org/paper_files/paper/2026/hash/b0131b6ee02a00b03fc3320176fec8f5-Abstract-Conference.html) | 上下文索引、重排/去重/标注，增加复用 | 改变上下文组织的上游方法；本项目冻结原输入，不通过重排制造前缀 |
| C19 / S2 / A | **CacheTune**，2026；[原文](https://arxiv.org/abs/2605.24022) | 非前缀 token 重要性与硬件自适应恢复/重算 | 不同复用语义；作为边界而非主要基线 |
| C20 / S2 / A | **DroidSpeak**，NSDI 2026；[官方页](https://www.usenix.org/conference/nsdi26/presentation/liu-yuhan) | 不同微调模型间的状态共享与部分重算 | 模型不一致、质量权衡，非同模型合法前缀的直接场景 |
| C21 / S2 / A | **SparseServe**，2025；[原文](https://arxiv.org/abs/2509.24626) | sparse attention 分层缓存与 H2D/D2H pipeline | 更改 attention 执行范围，不是本项目 dense Qwen 同预算的直接对照 |
| C22 / S2 / A | **TENT: A Declarative Slice-Spraying Engine for Performant and Resilient Data Movement in Disaggregated LLM Serving**，2026；[原文](https://arxiv.org/abs/2604.00368) | 传输切片/多路径/可靠性 | 传输引擎层相关；不能把新传输机制的收益记成我们的在线缓存策略收益 |

最重要的修正：不能再把 C 类整体概括为“固定目标后只优化单请求传输”。CacheFlow 已联合分配 batch 内请求的计算与 I/O，EdgeFlow 处理多节点迁移，Cake 也讨论计算/I/O 可用资源变化。

## 7. D 类补充：P/D 架构、调度、占用与释放

| ID / 相关度 | 工作与来源 | 与当前问题的关系 |
|---|---|---|
| D01 / S1 | **DistServe**，OSDI 2024；[官方论文](https://www.usenix.org/conference/osdi24/presentation/zhong-yinmin) | PD 干扰、阶段资源和并行配置、带宽感知放置；TTFT/TPOT SLO 的基础，不能仅用自己的 parity 栈代替它 |
| D02 / S1 | **Splitwise: Efficient Generative LLM Inference Using Phase Splitting**，ISCA 2024；[原文](https://arxiv.org/abs/2311.18677) | 分阶段异构资源配置与 KV 交接；不要与同名边云 DRL 论文混淆 |
| D03 / S1 | **Llumnix**，OSDI 2024；[官方论文](https://www.usenix.org/conference/osdi24/presentation/sun-biao) | 运行时 rescheduling、live migration、碎片/抢占/负载与尾部；“迁移影响队列”已有明确研究 |
| D04 / S1 | **FlowKV: A Disaggregated Inference Framework with Low-Latency KV Cache Transfer and Load-Aware Scheduling**，2025；[正文](https://arxiv.org/html/2504.03775v1) | 分块传输调用/不连续布局开销、负载感知与弹性 PD；与压缩论文同名 FlowKV 无关 |
| D05 / S1 | **TetriInfer: Inference without Interference**，2024 起；[原文](https://arxiv.org/abs/2401.11181) | 混合下游任务的分离/调度与资源效率；venue 按后续正式记录补全，当前不凭记忆填写 |
| D06 / S1 | **P/D-Serve: Serving Disaggregated Large Language Model at Scale**，2024；[论文](https://arxiv.org/abs/2408.08147) | 规模化 PD 服务及传输/调度；与当前 Ascend 背景相关，但不能据平台相同认定模型成本相同 |
| D07 / S1 | **Sarathi-Serve**，OSDI 2024；[官方论文](https://www.usenix.org/conference/osdi24/presentation/agrawal) | chunked prefill、decode 优先/批处理及吞吐–时延；完整请求互斥不是硬件必然，必须检查我们是否人为夸大 HOL |
| D08 / S1 | **EcoServe**，OSDI 2026；[作者代码及说明](https://github.com/MachineLearningSystem/26OSDI-EcoServe) | 商品 GPU 上 PaDG/rolling activation，避免跨节点 KV 数据；说明传输结构本身可改变，不是 host pool 场景的直接同架构基线 |
| D09 / S1 | **Aegaeon**，SOSP 2025；[作者论文](https://ennanzhai.github.io/pub/sosp25-aegaeon.pdf) | 多模型 GPU 池和 token 粒度伸缩，细粒度 KV 同步；不能把粗锁改细本身当全新思想 |
| D11 / S1 | **FastServe: Iteration-Level Preemptive Scheduling for Large Language Model Inference**，NSDI 2026；[正式论文页](https://www.usenix.org/conference/nsdi26/presentation/wu-bingyang) | token 粒度抢占、主机状态 offload/upload、缓解 run-to-completion 的 HOL；本项目完整 decode gate 必须与这种可抢占形态区分 |
| D12 / S1 | **DéjàVu: KV-cache Streaming for Fast, Fault-tolerant Generative LLM Serving**，2024；[原文](https://arxiv.org/abs/2403.01876) | prompt/token 分离、流式 KV、microbatch swap、复制容错；通信/执行重叠不是新概念 |
| D13 / S1 | **MELL: Memory-Efficient Large Language Model Serving via Multi-GPU KV Cache Management**，2025；[原文](https://arxiv.org/abs/2501.06709) | 昂贵迁移与动态 KV 负载的在线管理；目标主要是减少所需 GPU/迁移，不等于我们的 p99 目标 |
| D14 / S1 | **JITServe: SLO-aware LLM Serving with Imprecise Request Information**，NSDI 2026；[官方页](https://www.usenix.org/conference/nsdi26/presentation/zhang-wei) | 不精确请求信息下的 SLO、批次与逐步细化；不能用“未知输出/依赖”宣称其他调度无法处理 |
| D10 / S2 | **vLLM/PagedAttention**，SOSP 2023；[论文](https://arxiv.org/abs/2309.06180)；**SGLang/RadixAttention**，NeurIPS 2024；[论文](https://arxiv.org/abs/2312.07104) | 分页、前缀和 iteration/continuous batching 基础。论文年份与当前实现功能分开；2023 早期预印本是时间窗外的必要背景 |

D 类除 D04 原文和 D09 同步相关章节外，本轮主要核摘要/官方说明；不是对全部架构的完整实现审计。

## 8. E 类补充：模型、模拟、真实测量与漂移

| ID / 相关度 / 核实 | 工作与来源 | 已有覆盖及限制 |
|---|---|---|
| E01 / S0 / F | **Compositional Performance Modeling of LLM Inference Clusters with Hierarchical KV Caching**，2026-10-11 预印本；[v1 正文](https://arxiv.org/html/2610.15366v1) | 分离服务成本与共享资源/KV 可用性、逻辑状态/实际分配/发布、事件组合，1P1D→3P1D。**DAG/离散事件＋驻留/可用性本身不是我们的新贡献**；应核条件成本、同步尾部和在线目标是否有实质新增 |
| E02 / S1 / A+I | **Vidur**，MLSys 2024；[正式论文页](https://proceedings.mlsys.org/paper_files/paper/2024/hash/b74a8de47d2b3c928360e0a011f48351-Abstract-Conference.html)，[实现](https://github.com/microsoft/vidur) | profile+仿真、大规模配置搜索；当前 canary 有缓存功能，不能只用 2024 版本否定最新实现 |
| E03 / S1 / A+I | **LLMServingSim 2.0: A Unified Simulator for Heterogeneous and Disaggregated LLM Serving Infrastructure**，2026；[原文](https://arxiv.org/abs/2602.23036)，[官方站](https://llmservingsim.ai/) | 异构/PD、硬件–运行时交互；模拟器不是只拟合单请求 token 时间 |
| E04 / S1 / A | **Frontier: Towards Comprehensive and Accurate LLM Inference Simulation**，2026，v2；[原文](https://arxiv.org/abs/2605.21312) | 角色化 PD/AFD、scheduler–batch–engine、stateful 请求；评估大规模推理，不应称本项目未来 100P100D 模拟独一无二 |
| E05 / S1 / A | **AgentServeSim: Serving-System Simulation and Policy Search for LLM Agent Programs**，2026，v3；[原文](https://arxiv.org/abs/2606.09613) | 前驱完成驱动后继释放、tool gap retention、dispatch、实机对照和自动策略搜索 |
| E06 / S1 / F | **Calibrate, Then Route**（交叉见 A06） | §VI 明确指出：改常数和加 batching 仍可复现不了策略排名，真实 backlog 不能直接送给在线估计器。正是我们模拟可信度需要面对的反例 |
| E07 / S1 / A | **KV Cache in the Wild: Characterizing and Optimizing KV Cache Reuse in Large Language Model Serving**，ATC 2025；[官方页](https://www.usenix.org/conference/atc25/presentation/wang-jiahao) | 真实复用分布与 workload 差异；独立请求也可能有共享前缀，不能为了复用只认多轮，亦不能补长/重复输入造优势 |
| E08 / S2 / A | **Exact Memory-Time Optimization for Prefix-Cached Language Model Serving**，2026-10-02；[原文](https://arxiv.org/abs/2610.02766) | 前缀依赖、驻留/timeout 的组合优化；摘要明确不是 GPU 推理时延测量，不替代端点排队模型 |
| E09 / S2 / A | **Shared KV Caching for Replicated 27B Inference: Correctness Failures and Performance Boundaries**，2026-09；[原文](https://arxiv.org/abs/2609.15021) | 共享池指针/stream 正确性、受控比较与性能边界；工程案例，不凭预印本认定一般理论或已同行评审 |

### 8.1 当前工业实现也不能遗漏

它们不是额外的论文录用，但会影响“现有系统有没有做”与可复现基线的判断：

| 实现 | 原始来源 | 需要核对的现实能力 |
|---|---|---|
| NVIDIA Dynamo KV Router | [官方路由说明](https://docs.dynamo.nvidia.com/dynamo/knowledge-base/modular-components/router/overview)，[作者仓库](https://github.com/ai-dynamo/dynamo) | 可复用 KV 状态、预计 active load、PD 路由；固定具体 release/commit 后才可比较 |
| llm-d Router / EPP | [官方架构](https://github.com/llm-d/llm-d-router/blob/main/docs/architecture.md)，[PD 协调](https://github.com/llm-d/llm-d-router/blob/main/docs/disaggregation.md) | 精确/近似前缀信息、负载、远端 source，不能当纯 round-robin |
| SGLang HiCache | [官方设计文档](https://docs.sglang.io/docs/advanced_features/hicache) | GPU/host/storage 分层与异步 I/O，具体 backend/阈值应版本化 |
| vLLM × Mooncake Store | [vLLM 项目原始说明，2026-05-06](https://vllm-project.github.io/2026/05/06/mooncake-store.html)，[Mooncake 原仓库](https://github.com/kvcache-ai/Mooncake) | agent 场景的分布式 DRAM/SSD、RDMA、KV 调度；论文版、当前 Store 与 Transfer Engine 分别标明 |

上述是 2026-10-16 查阅的在线文档，不能反向声称旧论文当时已实现当前全部功能。未安装、部署或运行这些新基线。

## 9. 核心工作逐项对照：不能用笼统“未考虑”避开重合

| 工作 | P/D 与历史/共享 KV | 恢复或端点资源代价 | 多请求/调度 | 与推荐问题的真实差别或未知项 |
|---|---|---|---|---|
| MemServe | 明确有 | 传输/重算成本 | 全局路由与队列 | 精细 H2D/save/释放 tail 如何组合需核；不能说无联合调度 |
| Mooncake | 明确有，RDMA | DRAM→HBM、网络状态 | Conductor/热副本/负载 | 端点时变同步与失败交接的尾部机制不是其主要展示点；不能说网络被忽略 |
| DualPath | 明确有，多轮 | 存储 I/O、H2D/D2H、PCIe | 联合 P/D/读路径 | 以外部存储 NIC 不平衡为核心；与有限节点侧 CPU 池有区别但非互不相关 |
| AMPD | 明确有，RDMA history | prefill/decode/transfer 模型 | 自适应路由与重排 | 增量 prefill 的执行位置/部署为核心；共享 CPU 池状态可执行性细节待进一步对照 |
| SYMPHONY | compute/state 分离 | 分层管理与预取 | advisory/优先级 | 提示式信息结构不同，但已考虑缺提示，不能靠无 hints 一句排除 |
| Strata | 已核 v1 为 co-location | 明确 CPU↔GPU/碎片/干扰 | batch、delay hit、bubble | 不同 P/D worker 之间的资源释放/源依赖不是其已核主问题 |
| CacheFlow | 多 GPU 模型执行；独立 PD 范围未确认 | 明确 compute/I/O 联合恢复 | batch-aware、多请求 | 不能声称无共享 I/O；需对比独立 PD 队列、动态驻留及跨阶段 release |
| Remote KV Slower Than GPU | 系统背景为 disaggregation | 同步加载、固定 engine 成本 | replay 排队 | 主要实验一个 prefill engine、无 D handoff；多 engine 争用未实测，不等于已解决我们的完整场景 |
| Bidaw | 分层交互缓存；独立 PD 未确认 | load/save 与存储 | 队列与逐出 | 异步写不在其关键路径是设计条件；不能强行给它添加我们同步实现的限制 |
| Compositional Modeling | PD/分层 KV | 资源、容量、状态可用性 | 事件、批次/队列 | 组合建模已有；特定同步尾部/代价时变与可证明在线决策才可能有新增 |
| Calibrate, Then Route | PD，单节点 NIXL 测量 | 校准/输出长度/压力 | 分发、真实与模拟对照 | 已发现贪心集中与仿真错排；本项目若只修估计器或强制均衡会高度重复 |

“未确认/待核”不是“不支持”。以上差别是从本轮已取得原文的研究范围判断，不能扩大成“论文完全没有这个机制”。

### 9.1 原 PPT 九项的逐一修正

| PPT 工作 | 本文入口 | 原概括处理 |
|---|---|---|
| Preble | A02 | 保留 cache/load 路由分类，撤回对全部相关工作“无排队/无联合”的概括 |
| Online Context Caching | A05 | 明确本来就联合 placement/scheduling；正文细节待核 |
| LBGR+RLT | A04 | 明确已有在线学习/负载与缓存，不把预测本身当新增 |
| Mooncake | B02 | 删除“未统一考虑网络状态”等过强判断 |
| SYMPHONY | B04 | 补充 advisory 的真实作用与缺提示范围 |
| MemServe | B01 | 升为最相关背景，不再当简单远端访问模块 |
| EdgeFlow | C12 | 修正：它处理多节点迁移，不是固定一次 fetch |
| CacheGen | C11 | 保留带宽/压缩流式路径；不拿表示改变与 exact reuse 直接等价 |
| KVServe | C09 | 补充在线服务约束与适配，不当作静态压缩参数 |

## 10. 怎样收窄，才不是人为“换个名字”

### 10.1 推荐研究切口与可证伪命题

推荐切口是：**在无提前请求提示的、有限节点侧 CPU KV 池的多 P/D 服务中，状态驻留、合法发布、设备恢复和资源释放并非同一时刻；它们的相互制约可能使网络未饱和时仍出现缓存相关的 TTFT 尾部放大。**

这不是新颖性结论。要形成贡献，至少应验证以下差别有实际意义：

1. **不只是带宽不足。** 同输入、同字节、同状态下，存在发布/释放时序或同步路径差异，能解释尾部变化；改变带宽不能自动消除这些等待。
2. **不只是单次加载慢。** 延迟通过忙源、目的恢复、完整 decode 及后继会话传播；一个请求的最短恢复方案可能增加其他请求尾部。
3. **不只是原型锁写错。** 消除不必要的共享锁以后，仍有物理资源或正确性必需依赖产生同类现象；否则贡献应定位为工程修复而非新调度理论。
4. **已有方法在相同条件下确实有不足。** 优先对照 MemServe/Mooncake 类路由、Strata/CacheFlow 类恢复和便宜 cache/load 指标；不能只打一个弱 parity baseline。
5. **新增模型可检验、可复用。** 不靠 task/rate 特判或事后真实服务时间；在后续数据/物理分配上仍能较好解释关键路径，而不是只重放训练 trace。

如果最接近工作在同条件下已经覆盖并解决这些点，则应继续收窄或调整方向。**“把所有现有模块联合起来”不是自动的独特性证明。**

### 10.2 不应再写的创新性表述

- “首次把 H2D、计算和 I/O 同时建模。”——Strata、CacheFlow、KVPR 等已覆盖相关内容。
- “现有工作不考虑多请求竞争/全局路由。”——Preble、MemServe、Mooncake、EdgeFlow、CacheFlow 构成反例。
- “现有 agent 系统都知道未来请求。”——Continuum、PBKV、AMPD 等不支持这种概括。
- “首次发现缓存导致 p99 变差或 GPU 低利用率。”——近期 Remote KV Slower Than GPU 已明确展示类似现象。
- “采用资源 DAG 或大规模离散事件模拟就是贡献。”——E01–E05 已有直接工作。
- “把 RDMA 加到背景就不撞车。”——Mooncake、AMPD、DualPath 等直接反例。
- “我们的网络没有拥塞，所以所有 PD 系统的网络都不重要。”——当前低并发/互斥及缺遥测不支持推广。

## 11. 数学建模：沿用 PPT 符号，仅修改必要公式

### 11.1 输入、决策、中间量、输出

| 项目 | 内容 |
|---|---|
| 输入 | 已到达请求及前缀依赖、当前队列/资源/库存、容量与带宽、已完成历史成本 |
| 决策 | 因果策略 \(\pi\)：P/D 实例、获取来源、恢复/重算边界及合法驻留/迁移动作 |
| 中间量 | \(H_{rh},W_{rh},S_{rh}^{\bar h\to h}\)、操作起止、first/full/release、\(T_r^{queue},T_r^{compute},T_r^{KV}\) |
| 目标 | 全体被评估请求的常规 TTFT p99；开销保持在实际关键路径中 |
| 输出 | 可执行的策略/决策 \(\pi\)，不是三个时间分量；近似在线算法不等于未知未来的全局最优解 |

### 11.2 原符号保留与最少补充

以下定义没有因问题修正而失效，保留：\(\mathcal R,r,L_r\)；\(\pi,\Pi_{causal},Q_{0.99}\)；\(h\in\{P,D\},\mathcal G_h,g_{rh}\)；\(o,s_o,m_o\)；\(a,x_{oa}(t),A_a(t),M_a^{CPU}\)；\(z_{rh},H^0_{rh}(g),H^{\bar h}_{rh}\)；\(H_{rh},W_{rh},S_{rh}^{\bar h\to h}\)；\(\xi_g,\mathrm{Wait},C\)；\(f,S_f,\tau_f,R_f(t),b_f(t;\pi)\)；\(B_{P\to D},B_{D\to P}\)；三个 \(T\) 分量。

只补充以下必要时间/状态符号：

| 新符号 | 定义 | 为什么需要 |
|---|---|---|
| \(a_r^{nom},a_r^{ready}\) | 名义 offered 时刻、请求内容和前序依赖满足后的入站时刻；与内存域 \(a\) 的下标区分 | 防止混用两个 TTFT 起点 |
| \(F_r^{first},F_r^{full}\) | 客户端实际首 token 与完整响应时刻 | first 不等于设备/锁可释放 |
| \(\rho_{v,k}\) | 操作 \(v\) 结束相关工作后，实际释放资源 \(k\) 的时刻 | 完整 decode、save/publication 或租约可能继续持有资源 |
| \(v,c_v,s_v,e_v,\mathcal K(v)\) | 原 \(\mathcal P_r\) 路径里的操作、条件服务时间、起止、所需资源 | 显式产生等待，而不是拟合一个排队罚分 |
| \(\mathcal H_{rh}(t;\pi)\) | 按前缀完整性、发布、容量与安全契约可以选择的恢复边界集合 | 不把全部远端命中强行当实际接受量 |
| \(\alpha\) | 物理 allocation 身份，仅当多个逻辑 key 共享一份实际内存时使用 | 避免重复计费或把逻辑删除当物理释放 |

\(A_a(t)\) 保留为原容量式中驻留对象之外的额外占用项，并在实现中明确其中是否已含暂存/在途分配；同一字节绝不能同时计入 \(A_a\) 和对象集合。

### 11.3 公式组（1）：目标及容量/网络约束

保持原来的“最小化 p99，受容量与带宽约束”格式：

\[
\boxed{
\min_{\pi\in\Pi_{causal}}
Q_{0.99}\!\left(\left\{TTFT_r(\pi)\right\}_{r\in\mathcal R}\right),
\qquad TTFT_r=F_r^{first}-a_r^{ready}.
}
\tag{1}
\]

在当前因果 replay 中：

\[
a_r^{ready}=\max\!\left\{a_r^{nom},F_{\mathrm{prev}(r)}^{full}\right\},
\quad
F_r^{first}-a_r^{nom}
=TTFT_r+\left(a_r^{ready}-a_r^{nom}\right).
\tag{1a}
\]

无前驱时忽略第二项；若真实服务的 API 提交时刻已记录，直接使用该实际入站时刻。式（1a）是本 replay 的会话释放规则，不是任意真实多轮服务的到达模型。它也意味着服务 TTFT 的分母起点可能随前轮策略改变，因此必须同时报告 nominal latency、会话完成时间与依赖等待，不能靠推迟 ready 制造“更快服务”。历史以 nominal 为主目标的协议不重标。

原容量式在对象彼此物理独立时保留：

\[
\sum_o m_o x_{oa}(t)+A_a(t)\le M_a^{CPU},\qquad \forall a,t.
\tag{1b}
\]

若 \(o\) 是共享 allocation 的逻辑 key，则用等价的物理计费表达替换求和：

\[
\sum_{\alpha\in\mathcal A_a^{resident}(t)\,\cup\,
\mathcal A_a^{staging}(t)\,\cup\,
\mathcal A_a^{inflight}(t)}
\mathrm{roundedBytes}(\alpha)+A_a^{other}(t)\le M_a^{CPU}.
\tag{1c}
\]

这里是集合并集，唯一身份只计一次；不可见但仍被引用的 allocation 不能直接扣掉。设备 HBM 容量也需同样的真实约束，不因为目标改成 TTFT 而忽略。

原方向带宽约束没有错，保留：

\[
\sum_{f\in\mathcal F_{P\to D}(t;\pi)} b_f(t;\pi)\le B_{P\to D},
\qquad
\sum_{f\in\mathcal F_{D\to P}(t;\pi)} b_f(t;\pi)\le B_{D\to P}.
\tag{1d}
\]

\[
\dot R_f(t)=-b_f(t;\pi),\quad R_f(\tau_f)=S_f,\quad
e_f=\inf\{t\ge\tau_f:R_f(t)=0\}.
\tag{1e}
\]

若存在共享 NIC/交换路径或 P→P、D→D，需要进一步用 \(\sum_{f:\ell\in path(f)}b_f\le B_\ell\) 表达真实链路 \(\ell\)。这是原两方向模型的扩展，不凭空制造 RDMA 拥塞。H2D/D2H 的互连、copy engine、内存控制器共享也是资源约束，但不能把它们的速率叫 RDMA 带宽。

### 11.4 公式组（2）：实际恢复量、重算量与跨节点字节

原式 \(H=H^0+z(H^{\bar h}-H^0)\) 仅适用于“本地或一个不短于本地的完整远端前缀，二选一”的受限模型。当前存在部分恢复、多个来源、保护/容量回退，因此替换为：

\[
\boxed{
H_{rh}\in\mathcal H_{rh}(t;\pi),\qquad
W_{rh}=L_r-H_{rh}.
}
\tag{2}
\]

集合中的边界必须满足 token/hash/模型 namespace、块与状态拓扑完整性，以及执行时真实可接受的条件。部分恢复是合法选择，不要求 \(H\) 永远最大；完整 hybrid/TP 状态不是“有 attention key 就算恢复”。可保留 \(z_{rh}\) 表示该阶段是否使用远端来源，但它不再强行决定一个最大前缀。

跨节点字节保持原来的“缺少对象之和”思路，明确所选来源和实际副本位置：

\[
S_{rh}^{\bar h\to h}
=\sum_{o\in\mathcal K_{rh}(H_{rh})}
s_o\,\mathbf1\{o\text{ 被选择从 }\bar h\text{ 复制到 }h\}.
\tag{2a}
\]

只有一个远端来源且目标缺少对象时，式（2a）退化回原 \(z\sum_{o\in\mathcal K\setminus\mathcal V}s_o\)。对象已在本机 CPU 不意味着已在设备：

\[
S_{rh}^{H2D}
=\sum_{o\in\mathcal K_{rh}(H_{rh})}
s_o\,\mathbf1\{o\text{ 尚未设备驻留且需经主机恢复}\}.
\tag{2b}
\]

RDMA、H2D、计算新增 KV 的 D2H/save 分别按真实路径计量，不将远端字节数直接当作 H2D 字节数。安全检查失败、缺完整包或资源变化导致回退时，已花掉的查询/资源等待/同步时间仍在关键路径；没有字节不等于没有成本。

“执行时实际接受量”用于事后审计。在线预测只能用决策时可见库存与发布状态；不能将 after-P 修订后的 cache 量拿来替换原 route 的预测输入。

### 11.5 公式组（3）：关键路径与资源释放，而非分量总和硬相加

仍保留原来的关键路径形式，但明确起止与资源依赖：

\[
\boxed{
TTFT_r(\pi)
=\max_{p\in\mathcal P_r(\pi)}\sum_{v\in p}c_v(\pi),
}
\tag{3}
\]

\(\mathcal P_r\) 是从 \(a_r^{ready}\) 到 \(F_r^{first}\) 的事件路径；包含因资源先后顺序产生的等待边与必要空隙。若节点同时包含等待和服务，就先拆成不重叠的区间，不能一边把等待放在边上、一边在节点时长再加一次。

原 \(\mathrm{Wait}_{h,g}(\xi_g;\pi)\) 和 \(C_{h,g}(L,H;\xi_g)\) 可保留为等待与条件计算中间量，但不能只用队长或静态每 token 时间决定全部开始时刻。对操作 \(v\)：

\[
u_v=\max\left\{release_v,\max_{w\in pred(v)} e_w\right\},
\quad
s_v=\mathrm{EarliestJointFeasible}
\left(u_v,\mathcal K(v),c_v,\text{resource calendars}\right),
\quad e_v=s_v+c_v.
\tag{3a}
\]

joint feasible 是所有必需资源上的共同可行区间，不是把每个资源的等待求和。服务成本依赖当时 batch、上下文、缓存布局和共享资源状态；若并发改变速度，日历和成本要相容更新，不能一边按互斥服务估计、一边假设完全重叠。

\[
\rho_{r,D}=\text{完整 decode、必要保存/发布及相关持有实际结束的时刻},
\qquad \rho_{r,D}\ge F_r^{first}.
\tag{3b}
\]

这里 \(\rho_{r,D}\) 是相关 D 资源的简写；不同资源有各自 \(\rho_{v,k}\)。它不必等于客户端 full response：异步 save 可能继续，某些 lease 也可能先行释放。只有实测契约允许时才简化为 \(F_r^{full}\)。本请求 first 后的 decode 不增加它自己的 TTFT，却能成为其他请求 first 路径的前驱。

典型依赖为：

\[
\text{lookup/reserve}
\to\text{可选远端复制/发布}
\to\text{目标 H2D}
\to\text{未缓存部分计算}
\to\text{首 token};
\qquad
\text{decode/save/release}\to\text{后续请求的可执行时刻}.
\tag{3c}
\]

实际保存可以与计算/传输重叠；何时阻挡 first、发布或 release 由真实完成依赖决定，不规定全部同步串行，也不假定所有 save 都免费异步。源/目的软件 gate 是当前实现的约束，不能误作任何硬件都必然独占；不必要的 gate 应在后续实验独立检查。

只有把**同一条已实现关键路径**划成互不重叠的等待、计算与 KV 区间后，才可写：

\[
TTFT_r=T_r^{queue}+T_r^{compute}+T_r^{KV}.
\tag{3d}
\]

控制平面实际执行/等待需明确归入相应区间，或者单列 control 分量；不能遗漏。并行分支、嵌套 restore/H2D-task、descriptor 累计时长、full-request save 不能直接相加。尤其不能写 \(p99(T)=p99(T^{queue})+p99(T^{compute})+p99(T^{KV})\)。

### 11.6 条件成本估计：公式由工作量决定，不设结果罚分

保留 \(C(L,H;\xi)\) 的含义。以 block-aligned dense attention 为例，合理的解释性特征可包括：

\[
U=L-H,\quad
A=\frac{L(L+1)-H(H+1)}2,\quad
C^{prefill}=\theta_0+\theta_U U+\theta_A A+\theta_K\left\lceil U/b\right\rceil.
\tag{4}
\]

这只是当前执行形态下的可校准近似，不是硬件普遍精确公式；batch、kernel、图模式改变时应重新验证支持域。完整 decode 占用还需上下文增长、输出长度和结束开销，而非只估 first。

\[
C^{load}=C^{lookup/reserve}+C^{descriptor/submit}
+C^{data\ movement/sync}+C^{publication},
\tag{4a}
\]

式（4a）仅在这些计时段实际互不重叠时作为分解；若只有一个包含 H2D 的 restore 总计时，就用该整体条件成本，不再另加 H2D。用纯 bytes/bandwidth 代替整个 load 会漏掉固定开销、布局和同步等待。保存同理，必须区分 first 前、first 后及资源释放的部分。

初始参数来自隔离标定，更新只用截至预测原点已完成的样本。运行中的超时样本是右删失，不能冒充已完成成本；外部未知负载不是可事后喂入的 oracle。固定 Qwen 布局里 bytes 与 descriptor 数近共线，不能宣称分别识别两种斜率。没有证据时不加“D0 × 3”“缓存超过 X 就惩罚”等魔法参数。

服务成本时变应作为需要验证的条件状态或预测误差，而不是把每个阶段的 p99 相加作为最终 p99。若引入不确定性场景，需要保留跨阶段、跨请求的相关性；固定惩罚倍数或事后重放真实未来都不能解决这一点。

### 11.7 正确性与可执行性约束仍保留

\[
\pi_t\text{ 只依赖 }\mathcal F_t
=\sigma(\text{已到达请求、当前可观测状态、已完成历史}),
\quad
\text{destination visible}(o)\Rightarrow
\text{copy/save complete}(o)\land\text{valid topology}(o).
\tag{5}
\]

容量在动作前可行；generation 在 commit 时新鲜；租约和在途源受保护；移动先得到完整目标副本再移除源；逻辑移除不冒充实际 allocator 回收。不要求跨机原子，也不声称单 mutex 就实现它。策略可以选择 no-op；是否采用快慢架构不属于问题定义或可行性公理。

全体请求的式（1）是研究目标，不等于只对当前 admitted-live 请求取 p99。历史 resource-DAG 版的 live 集合最多约 8 个会话，nearest-rank p99 实际为 max；已经 first、仍 decode 的请求又可能成为不可降低的目标下界。这种在线替代目标与最终分布的偏差要单独评估，不能以“用了 p99 字样”宣称严格实现了式（1）。95 个请求的 nearest-rank p99 也是最大值，应另报完整分布及重复测量，而非调整分位定义追求好看结果。

### 11.8 原公式保留/修改清单

| PPT 项目 | 处理 | 理由 |
|---|---|---|
| 请求、P/D 集合、长度、对象/副本、容量、因果策略、p99 | 保留 | 定义仍适用，不为改问题而重命名 |
| p99 优化框架 | 保留，明确 TTFT 起点 | 不混用名义 offered 与实际 ready |
| 独立对象容量和方向带宽 | 保留有效情形 | 共享 allocation/实际拓扑时扩展，不删除网络约束 |
| 二元远端/本地直接决定最大 H | 修改 | 不能表达部分恢复、完整包/容量回退 |
| Wait/C 中间量 | 保留并丰富 \(\xi\) 与资源事件 | 队列要来自完整服务/释放依赖 |
| 三分量总和 | 仅保留互斥关键路径分解 | 避免重复计算并行/嵌套时间 |
| 三部分 T 作为输出 | 改正 | T 是由策略产生的中间量，输出是策略/动作 |
| RDMA 为唯一拥塞资源 | 去掉该隐含假设 | 与当前证据不符，仍保留其可能成为瓶颈的分支 |

## 12. 差异化与实验证据还需要什么

本节是后续研究计划，不是本次启动命令；不修改当前实验。

### 12.1 最小机制验证

- 同物理分配、同输入与完整恢复/保存工作量，对齐 gate 尝试/取得/释放、lookup、reserve、submit、sync、publication、first/full，分清纯数据路径与软件等待。
- 对比原必要安全契约与去除不必要串行化后的路径；不能通过取消 SYNC_SAVE/租约保护造收益。
- 在相同有限缓存下检查低/高真实并发：同时记录 actual ingress、batch、网络 in-flight、RNIC 统计，而非只用 nominal rate 代表突发。
- 匹配 B→T→B 漂移控制，保留两 B 和负收益；同配置初始实际库存规范状态相等。未知外部干扰明确标注，不拿输出审计认证 clean hardware。
- 若简单便宜的 cache/load 路由已经接近最优，复杂规划的全部 CPU/RPC/迁移成本可能不值得；允许删掉无效慢路径。

### 12.2 基线优先顺序

1. **同栈机制对照**：固定 parity、least-load、prefix-aware、无慢规划、仅实际服务成本路由、完整候选。明确是同栈实现，不冠以其他论文名称。
2. **最接近系统/策略**：MemServe/Mooncake 类的 locality+queue+transfer；Preble、LMetric/LBGR 类低开销 cache/load 路由；Strata/CacheFlow/Cake 类恢复选择。只能按已核原文和可支持接口忠实实现；若只复现其中策略，标“strategy reproduction”，不说完整系统。
3. **端到端系统基线**：版本冻结的 vLLM/SGLang＋适当 KV backend、Mooncake/Dynamo/llm-d。相同 PD 角色、模型、输入、输出、容量和硬件预算；原生 colocated 多副本作为架构参照另报，不混成同 PD 策略消融。

不能把 Mooncake Transfer Engine 的接入当 FAST25 Conductor/Store 完整复现，不能把自己的 round-robin 栈叫“纯 vLLM”。缺硬件/backend 时先说明不可直接运行，不编造相同环境结果。

### 12.3 模拟必须保留的真实限制

需要独立表示：主机驻留/设备可用、H2D 与 save 的条件成本、完整 decode/资源释放、失败交接、共享 allocation/暂存、缓存动态演化、FIFO 或真实 batching、路由可观察信息，以及控制平面实际开销。

区分三个层次：实测；固定已观测 route/cache 的 trace-conditioned 记账；有策略反馈的反事实预测。第一种不能直接当第三种的 oracle。只重建端点或平均延迟不代表能复现策略排序和 p99；E01、E04、E05、A06 都应作为方法对照。当前 2P2D 尚未稳定验证，不外推 100P100D。

## 13. 调研范围、覆盖检查与待补事项

### 13.1 时间与检索方法

- 主范围：2023-10-16 至 2026-10-16 的公开论文/正式发表及最新可取得版本；2023 更早的 vLLM 等作为必要背景单列。CacheGen 2023 初稿虽略早于窗口，其 2024 正式论文在范围内。
- 起点：PPT 全部九项；沿 MemServe、Mooncake、DualPath、Strata、CacheFlow、AMPD 的相关工作/参考文献扩展，再查 2026 新稿和官方实现。
- 检索主题：`PD / prefill decode + multi-round + KV cache`；`distributed prefix cache + routing + queue`；`host GPU restoration + synchronization + tail`；`load recompute + shared I/O + batching`；`discrete event + KV availability + disaggregated simulation`。
- 核验来源：USENIX、ACM/IEEE 官方页面、ICLR/OpenReview、PMLR/MLSys、ACL Anthology、arXiv、作者论文页/官方仓库。二手文章只用于发现线索，不承担关键技术结论。
- 本文保留近邻、替代架构和模型工作，而不是只保留“看起来没有撞车”的论文。类别内按背景/机制相关度排序；同等级不作伪精确评分。

### 13.2 尚未完全解决的文献核实事项

| 优先级 | 事项 | 当前处理 |
|---|---|---|
| 最高 | Online Context Caching、EdgeFlow 的完整公式/执行范围 | 官方摘要已核，正文细节未知；不得以未知判否 |
| 最高 | AMPD、DualPath 与本项目有限 CPU 池及发布/释放契约的逐段差异 | AMPD v2、DualPath v2 关键章节已核；要主张具体缺失仍需对应正式版本和代码 |
| 最高 | Strata 正式 OSDI26 版相对 2025 arXiv v1 的变化 | venue/官方摘要已核，正文细节引用明确标 v1，不假定版本完全相同 |
| 最高 | CacheFlow 独立 P/D 实例范围、与全局 placement 的交叉 | v2 batch/3D 恢复已核；正文没有某关键词不等于证明不支持 |
| 高 | 新 2026-10-15 remote-tier 工作的后续代码/版本 | v1 §8 限制已核；单点固定开销/单轮结果不推广为所有模型阈值 |
| 高 | LMetric、TetriInfer、Tokencake 等正式 venue 与版本 | 无可靠原始录用记录的保持年份/预印本，不凭二手汇总填 venue |
| 高 | 现代 Dynamo、llm-d、HiCache、Mooncake 的实际可用接口 | 文档已核，待后续冻结 commit 做实施审计；本次未部署 |
| 持续 | 2026 新稿、已列论文的更新及引用扩展 | 按同一三分类追加，不删除不利于新颖性的工作 |

“不要遗漏”落实为：原九项全覆盖、最相关背景与机制主动扩展、未知项可追踪。**公开检索不能保证世界上没有未索引、未公开或刚发布的相同研究；本文不提供这种保证。** 新方向应靠对照和证据形成差别，不靠声称穷尽全部文献。

## 14. 维护记录

| 日期 / 版本 | 变化 | 未改变事项 |
|---|---|---|
| 2026-10-16 / research-spec-01 | 按原 PPT 写法重述客观问题；保留三类相关工作并补架构/模拟；修正恢复边界、关键路径、资源释放及物理容量公式；记录重合风险和未知项 | PPT、运行中源码/参数、原始实验、旧模型历史和暂停的定时任务 |

历史工作区另保留每版数学假说与反证，未将全部无效草案重复上传。本文是研究问题和文献对照入口，不反向修改旧实验目标或宣称尚未实现的公式已经上线；实测状态以本目录 README 和证据为准。
