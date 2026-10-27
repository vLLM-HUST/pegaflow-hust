# Tailguard：PCIe 与端点状态可用性的研究进展

更新：2026-10-26。检索范围以 2024–2026 年为主，重点复核 OSDI、FAST、NSDI、SIGCOMM、EuroSys 2026 与近期预印本。本文区分论文中的事实、我们的比较判断和待验证假说。文献重合分析不等于创新性认证。

## 1. 研究问题与已有证据

**问题：有限主机 KV 池的 P/D 分离服务中，主机命中后的跨阶段等待怎样放大 TTFT 尾部？**

在独立 prefill/decode 实例之间，合法 KV 前缀即使已经存在，也要经过远端获取、主机到设备恢复、必要的同步发布，以及端点资源释放，才可被请求实际使用。多个会话共享这些资源时，前序请求的完整 decode、保存与交接会推迟后续执行。高命中率、更少重算或更快 RDMA 不必然降低首 token 等待。

这个问题不预设采用快慢规划、主动迁移、某种预测器，也不预设 PCIe 或 RDMA 一定饱和。单纯将“RDMA”替换为“PCIe”不足以区分 Strata、Bidaw、CacheFlow 等已有工作。应检验**独立 P/D、有限端点池、无未来提示、精确状态复用、跨阶段资源释放依赖**这一组合下，哪些机制仍造成可观测的尾部损失。若原因只是原型的粗锁或错误同步，应作为工程缺陷修复。

已有真实证据：归档 bracket 中，RDMA fetch 等待累计约 0.69/0.67 秒，而同输入、同恢复工作量下 H2D task 和保存 RPC 可有数倍波动。这支持优先分析端点路径，不能证明网络从不阻塞，也不能证明 PCIe 链路已经饱和。H2D task 含描述符、后端和同步；保存 RPC 含提交、复制、同步及发布。嵌套计时不能直接相加。

当前 CPU 并行模型有四个实例、D 端迭代级批处理。它仍把同步 prefill 保存包含在 singleton first-service 拟合内。2026-10-26 三档保守并行回放中，D trace 标记为 compute 的总时间约 841–912 秒，restore 约 86–130 秒，末尾 save 约 7.9 秒。**compute 不是纯计算**，这些是模型输出，不是新 PCIe 测量。路由及边界规划另耗约 128–130 秒 CPU wall，已进入虚拟关键路径，是本轮先优化的明确开销。

## 2. 按原 PPT 分类、依相关度排序

阅读级别：F＝已读取本轮涉及的正文关键节；A＝官方/作者摘要，更多细节未核；H＝继承旧调研。硬件空缺保留未知，不用纸面 FLOPS 换算请求率。

### C. KV 获取、主机设备 I/O 与重算：当前最相关

| 工作 / 来源 / 阅读级别 | 已覆盖机制 | 对本研究的约束与可借鉴点 |
|---|---|---|
| **Strata，OSDI 2026，F**：[正式论文](https://www.usenix.org/system/files/osdi26-xie-zhiqiang.pdf) | 主机与 GPU 布局解耦、GPU-assisted I/O、delay hits、加载感知批调度与空隙填充。正式论文还讨论 I/O kernel 干扰及批量 DMA 取舍。 | 最重要的相邻工作之一。不能声称“现有调度不考虑加载”。我们要检验其机制在独立 P/D 端点的发布、完整 decode 和同步释放条件下是否足够。CUDA kernel 不能直接移植为 910B 已实现能力。 |
| **CacheFlow，2026-09-26 v2 预印本，F**：[正文](https://arxiv.org/html/2604.25080v2) | token/layer/GPU 三维恢复，跨请求 DP 共同选择重算与加载，在线采集成本。 | 已有批量共享 I/O 和恢复/重算联合优化。可借鉴 DP 的 Pareto 前沿和低成本规划；其重叠假设、层级操作权限与我们的整前缀执行接口不同。未核实会议录用。 |
| **Bidaw，FAST 2026，F**：[正式论文](https://www.usenix.org/system/files/fast26-hu-shipeng.pdf) | 按所在层与加载量调度，结合会话行为管理 DRAM/SSD；选择性保存历史状态。 | 已研究计算–存储双向协同。文中将写存储视为非关键路径；我们必须测量本实现同步写的实际依赖，不能把人为阻塞当普遍结论。 |
| **TurboBus，SIGCOMM 2026，A**：[官方议程](https://conferences.sigcomm.org/sigcomm/2026/program/papers/)，[DOI](https://doi.org/10.1145/3789240.3829130) | 通过 scale-up fabric 和 relay GPU 汇聚空闲 PCIe 路径，做细粒度双向分配与 size-aware 调度。 | **本轮新增、PCIe 方向必须覆盖。** 研究对象包括跨作业链路碎片化，不能把“某卡繁忙而别的卡空闲”自动当成请求路由错误。需有拓扑、授权中继和实际数据路径；本文未取得全文，不把第三方复现当作者代码。 |
| **eLLM，EuroSys 2026，A**：[作者摘要](https://cds-macau.github.io/publication/conference-paper/ellm/)，[DOI](https://doi.org/10.1145/3767295.3803570) | token-wise partial caching，动态 batch/重算比例，层级通信计算重叠与融合。 | 已有细粒度缓存–计算协同。不是只优化命中率。全文实验硬件与 SLO 细节待核。 |
| **KVPR，Findings ACL 2025，A/H**：[正式页](https://aclanthology.org/2025.findings-acl.997/)；**HCache，EuroSys 2025，H**：[论文](https://chenyoumin1993.github.io/papers/eurosys25-hcache.pdf) | partial recomputation、activation/状态恢复与 I/O 感知。 | 是恢复策略和表示层对照。我们目前不具备任意层级激活重建接口，不能只改模拟便声称实现同算法。 |
| **DirectKV，OSDI 2026，A**：[正式页](https://www.usenix.org/conference/osdi26/presentation/luo) | GH200/GB200 的 NVLink-C2C 上，GPU kernel 直接访问主机 KV，消除 staging buffer。 | 硬件边界很重要：它明确利用 NVLink-C2C 而非 PCIe。不能把收益数字直接套到 910B，亦不能假定当前平台具备这种 coherent access。 |
| **ECHO，OSDI 2026，A**：[正式页](https://www.usenix.org/conference/osdi26/presentation/liu-guangda) | native sparse attention，GPU graph 内的管理与无损预取、融合流水。 | 新增边界项。面向 native sparse 模型，不是 dense Qwen2.5 的同语义直接基线。 |
| **KVServe / KVCodec，SIGCOMM 2026，A/H**：[官方议程](https://conferences.sigcomm.org/sigcomm/2026/program/papers/) | 前者服务感知压缩选择及在线纠偏；后者 codec 与获取–解码–恢复流水。 | 都考虑端点成本，不能写成仅优化传输字节。保持 accuracy 不等于逐字节 KV 无损，比较时单独声明质量与输出门禁。 |

### B. 跨节点 KV 池与远程访问

| 工作 / 来源 / 阅读级别 | 已覆盖机制 | 与本研究关系 |
|---|---|---|
| **Mooncake，FAST 2025，H**：[正式页](https://www.usenix.org/conference/fast25/presentation/qin)；**MemServe，2024 预印本，H**：[原文](https://arxiv.org/abs/2406.17565) | 分布式主机 KV 池、缓存感知路由与驻留、传输与资源协调。 | 必须比较的系统背景。同栈“启发式策略”只复现策略思想时，不能冒充完整系统复现。 |
| **SYMPHONY，NSDI 2026，A/H**：[正式页](https://www.usenix.org/conference/nsdi26/presentation/agarwal) | compute–memory disaggregation 与会话状态管理。 | 无未来提示不是单独创新点。须比较状态获取与端点执行何时协调，不能概括为不考虑性能。 |
| **DualPath，SIGCOMM 2026，A/H**：[官方议程](https://conferences.sigcomm.org/sigcomm/2026/program/papers/)，[预印本](https://arxiv.org/abs/2602.21548) | 借助 decode 存储网络路径和 compute-network RDMA 分担存储读取，联合负载调度。 | **更新为已核实 SIGCOMM ’26。** 存储 NIC 瓶颈与我们短 RDMA 路径不完全相同，但独立 P/D 及来源选择已高度重合。 |
| **SARIA，2026-10-22 预印本，F**：[正文](https://arxiv.org/html/2610.31169v1) | 持久化感知 admission、统一混合来源前缀、DRAM/SSD reload 协调。 | **本轮新增。** 文中协调在双 SSD 条件也会退化，强调有效带宽而非纸面上限。可借鉴共享 ingress 建模；不能把 SSD/GDS 路径混成我们的 CPU-pool 恢复。 |
| **LMCache，2025 预印本，A/H**：[原文](https://arxiv.org/abs/2510.09665)；**Tutti，2026 预印本，H**：[原文](https://arxiv.org/abs/2605.03375) | 分层缓存层与 SSD I/O、直接恢复或 slack-aware 路径。 | 本轮未核实 LMCache 的新会议归属，保留预印本标签，不根据二手列表补写 MLSys。 |

### A. KV 感知请求路由与放置

| 工作 / 来源 / 阅读级别 | 已覆盖机制 | 与本研究关系 |
|---|---|---|
| **LMetric，OSDI 2026，A**：[正式页](https://www.usenix.org/conference/osdi26/presentation/zhang-dingyan) | 以新计算 token 数与当前 batch size 的乘积协调命中与负载，避免组合权重调参。 | **会议归属已核实。** 应作为低规划成本的强对照；其 score 不必准确预测秒数。不能只和 round-robin 比较就声称复杂模型必要。 |
| **Preble，ICLR 2025，H**：[原文](https://arxiv.org/abs/2407.00023) | 分布式前缀复用与请求调度。 | 主要路由对照，复用与均衡已是成熟问题。 |
| **Calibrate, Then Route，2026-09-14 预印本，F**：[正文](https://arxiv.org/html/2609.16206v1) | 边际完成时间、输出长度和 cache pressure；模拟后在实际 PD 集群校准比较。 | 说明模型失配可使评分退化成队长统计。应比较本算法的单阶段与端点误差，而不是只报告总体收益。真实集群与小模拟不能互换。 |
| **CacheRoute / GORGO / LBGR+RLT / Online Context Caching，H** | 旧目录保留逐项链接与阅读边界。 | 本轮未逐篇复核，不删除、不擅自修改其会议与机制结论。见下面完整继承目录。 |

### D. 建模、评估与非直接对照

**Compositional Performance Modeling of LLM Inference Clusters with Hierarchical KV Caching**，2026-10-11 预印本，F：[正文](https://arxiv.org/html/2610.15366v1)。它分离 local service、共享资源和 KV availability，用可组合事件模拟比较拓扑与策略。与本项目方法非常接近；不能把“独立服务拟合加资源日历”当未被研究的新意。借鉴其 component/cluster 两层误差验证，尤其 cache 可见与实际就绪的区分。

**Understanding Bottlenecks for Efficiently Serving LLM Inference With KV Offloading**，预印本，A：[原文](https://arxiv.org/abs/2601.19910)。检索页显示提交时间为 2025-12-16，编号与日期呈现不一致，引用保留 ID、不另推导日期。其瓶颈模型可作为带宽/计算交叉点的分析参考，其环境里的传输占比不是我们的测量。

保留 Orca、vLLM、Sarathi-Serve、DistServe、Splitwise、Llumnix、FastServe 等执行/PD 架构对照。CacheBlend、ContextPilot、ECHO、CacheSlide、稀疏/有损 KV 方法改变语义或计算路径，不能未经说明混成同预算 exact-prefix 策略对照。近期 MLSys 的 ContextPilot、FlexiCache 等分别属于输入组织或稀疏/压缩边界，并非自动适配 dense exact-output 场景。

### 继承目录与检索边界

旧完整研究目录保存为 [research-spec02-20261019.md](archive/research-spec02-20261019.md)，保留原 A/B/C 分类、所有链接与 F/A/I 阅读等级。该文件是历史快照，新增工作与会议修正以本文为准。检索过 OSDI/FAST/NSDI/SIGCOMM 2026 官方页、EuroSys 作者与 DOI，以及近期 arXiv；这不是全领域穷尽检索。全文未取得的工作明确列为 A，不根据第三方代码或博客推测论文实现。

## 3. 硬件与执行条件，先对齐再比较

| 工作 | 已核条件 | 不可直接外推的部分 |
|---|---|---|
| Strata 正式 OSDI ’26 §5.1 | 8×H200 节点、PCIe 5×16 每卡单向峰值 64GB/s；另有 8×H20+NVMe 与 GH200。8B/14B 与较大 TP 配置需逐项对齐 | CUDA I/O、NVLink、GH200 主机访问能力都与四卡 910B 不同。正式版有第三个 H20-storage 平台，不能只引用旧预印本两平台。 |
| Bidaw 正式 FAST ’26 §4 | 单 A800 80GB，200GB 主机内存，PCIe Gen4；四块 SATA SSD RAID5 约 1.5GB/s；含 Qwen-7B 等 | 论文明确其设置下约 30GB/s 的 PCIe 通常不是瓶颈，较慢 SSD 是瓶颈。因此不能把 Bidaw 的排队现象直接当成我们的 PCIe 拥塞证据。 |
| DirectKV | GH200/GB200 NVLink-C2C | 它专门绕开 PCIe，不能当作 PCIe 上零成本主机访问。 |
| SARIA §5.1 | RTX PRO 6000 Blackwell Server 96GB，双 EPYC 9534，256GB RAM，双 NVMe；另用 A30 做 admission/metadata | A30 部分是缩小结构、随机初始化模型；混合 DRAM/SSD 和单独 I/O 实验要分开。 |
| Calibrate, Then Route | 8×A40，vLLM/NIXL PD | 目标与负载、模型不同，不能按 GPU 数量直接套用请求率。 |
| 本项目已归档实机 / 当前 CPU | 历史 2P2D 四单卡 TP1 Qwen2.5-7B BF16；当前只有 P 端 CPU。D 并行是迭代级模型，尚无新实机校准 | 不宣称 CPU 批模型已经证明 910B 并行收益，也不按其他硬件峰值制造负载。 |

## 4. 对数学模型的具体修正

参数定义、因果策略与原容量约束保持。输出为可执行策略 \(\pi\)，各分量时间是中间量。常规指标为

\[
 TTFT_r=F_r^{first}-a_r^{ready},\qquad
 a_r^{ready}=\max(a_r^{nominal},F_{prev(r)}^{full}),\qquad
 \min_{\pi\in\Pi_{causal}}Q_{.99}(\{TTFT_r\}).
\]

同时报告 nominal→first、完整分布、最终 drain，不能扣掉规划或迁移。在线 admitted-pending 的尾部目标是全实验 p99 的代理，不能声称等于全局最优。

每个请求/批的操作 DAG 显式区分：库存与控制、RDMA 获取、主机描述符/提交、H2D、compute chunk、D2H、同步与 publish、decode iteration、最后 release。对于操作 \(v\)：

\[
 s_v=\mathrm{EarliestJointFeasible}\!\left(\max(a_v,\max_{u\prec v}e_u),\mathcal K_v,c_v\right),\quad
 e_v=s_v+c_v,\quad
 F_r^{first}=e_{first(r)},\quad \rho_r=e_{release(r)}\ge F_r^{first}.
\]

\(\mathcal K_v\) 包含实际需要的 CPU submit 线程、主机/NUMA 带宽域、PCIe 根端口/设备路径、DMA 或 GPU-assisted copy 计算资源、方向链路、HBM 与发布保护。**只有实测可重叠的资源才允许重叠**。当前未校准时保留独占设备 gate，不能借论文的 CUDA 重叠能力直接给本项目加速。

可校准的搬运服务式为

\[
 c_{io}=\alpha_{submit}N_{desc}+\alpha_{launch}N_{batch}
          +S/B_{eff}(topology,direction,concurrency)+c_{sync},
\]

各系数从隔离、已完成样本估计，且取非负。\(c_{sync}\) 若是等待前置操作完成，应表达为 DAG 依赖，不能又拟合成服务时间重复计算。若只有 aggregate task 计时，保留 aggregate 模型，不强行拟合不可识别的纯 DMA 与同步系数。也不把排队时间训练成每 token 计算成本。

共享路径需约束 \(\sum_f b_{f,\ell}(t)\le B_\ell\)。H2D 与 D2H 是否共享上限取决于拓扑及双工测量，不一律加总，也不无条件分成互不竞争的两条无限路径。完整 decode 与同步保存继续影响后续的 \(\rho_r\)，即使当前 TTFT 已经确定。

## 5. 本轮实现的优化假说与公平条件

首先处理可测的规划开销，保留 source05 的候选池、资源成本、batch/kernel、容量、精确前缀与 lease 修复。

候选路由 \(q\) 的乐观首 token 下界：

\[
 LB(q)=t-a^{ready}+W^{relaxed}_{P(q)}+C_P(q)+C_D^{first}(q).
\]

它忽略复制等待及 D 上其他工作，只保留同 P FIFO 之前的未完成 P 服务；这些约束的松弛不能增加完成时间。只有当当前 pending p99 等于 max 且 \(LB(q)>J_{incumbent}\) 时才剪枝；相等必须保留，以便比较完整 tail/release profile。更多 pending 请求使 p99 不等于 max 时停止这项剪枝，不能错误推广单请求下界。

最初候选沿原次序剪枝，并尝试对**同一前 64 个原始候选**按下界排序。实际高档诊断中，这个聚合 D 服务下界曾超过完整预测约 0.15 秒，因此该剪枝版本已拒绝，不能用其他回放通过替它背书。64 是继承的 CPU 搜索预算，不是按收益拟合的阈值。

下一版改用不剪枝的 decision-local memo：同一不可变库存、成本、token 进度快照只刷新一次工作链，完全相同的候选复用预测。24 请求 / 1536 候选与重新完整计算对照一致。闭环回放中 CPU 时间仍实测计入；因此路线与 cache 演化可能改变。它是正确性中性的计算复用，不是“三档性能必然改善”的保证。

再比较两种已就绪 prefill 排序。给定共同目标 \(J\)，请求期限为 \(d_r=a_r^{ready}+J\)。只按年龄时，排序等价于 \(a_r^{ready}\)；加入剩余工作时，松弛量为

\[
 \ell_r(t)=a_r^{ready}+J-t-\widehat C_r^{first,remaining}(t),\qquad
 \arg\min_r\ell_r=\arg\min_r(a_r^{ready}-\widehat C_r^{first,remaining}).
\]

共同的 \(J,t\) 消去，无需拟合混合权重。剩余工作只从已完成 chunk 进度与发出时成本预测，不能读取未来 world 完成标签。它只作用于已恢复、满足 HBM 准入的 prefill；decode、H2D/save barrier 与源/目的 gate 仍完整保留。动态优先级 crossing 限制日历合并步长，避免模拟跳过真正的排序切换。这是局部启发式，不是多资源下全局最优证明。

只按年龄的三档结果不足以升级，因此继续评估剩余松弛量。后者一轮的中档大收益还暴露了模拟器的资源仲裁不一致：执行循环按节点编号优先 P，预测却按请求次序排可执行操作。一条已完成 P 的 handoff 等待约 53.88 秒，其中源 P 上后续计算占约 53.08 秒；实际模拟链路复制仅约 0.0008 秒。这个**模型现象不是新实机 RDMA 测量**。需先统一仲裁，再重新比较；不能把人为不一致贡献的大收益归给算法。

本轮的保守修正让执行世界沿用预测日历的请求顺序争抢资源，同时保留两端设备 gate、方向链路、完整 decode、lease 和容量，未添加免费重叠。这是 CPU 模型一致性修正，不代表已修复原生服务器。未来应验证 host-to-host copy 是否真的需要整段持有源 NPU gate；若源缓存 lease 已足够，应缩短软件临界区，同时仍测量 host/NUMA/保存竞争，不能直接假定开销为零。

单纯改变轮询顺序还不能保证两端同时可用。最后一个候选为已就绪复制保留资源顺位：设其请求顺位为 \(o_v\)，资源 \(k\) 的保留顺位为

\[
 q_k(t)=\min\{o_v:v\text{ 是已就绪、尚未取得资源的复制}, k\in\mathcal K_v\}.
\]

未取得资源的新操作 \(u\) 仅在所有所需资源上满足 \(o_u\le q_k\) 时参与联合 acquire；空集合表示无约束。已经取得资源的操作不受新保留顺位抢占，仍须完成；无关端点继续工作。保留的是顺位，不提前持有部分 device gate。这是可解释的防插队约束，不以固定年龄阈值或 task/rate 特判触发。

该候选实际三档结果相对同轮统一仲裁的 memo 参考为 p99 **改善 13.6%、改善 5.4%、退化 9.3%**，因此未升级为最终策略。高档 D 接受前缀更多，模型 restore 总时间也从约 74.74 增至 102.61 秒；compute 总时间少约 17.65 秒，规划/边界 CPU wall 却多约 29.37 秒。这些区间有重叠，不能加总成 TTFT 差。最慢 git-multibranch:11 的 nominal 首 token 更早，常规 TTFT 却更长，说明会话 ready 内生必须分口径解释；不据此事后更换主指标。

本轮止于一组有完整数据支撑的局限：仍需校准 aggregate prefill 中的同步保存与逐 chunk 非线性成本，并检验未来发布/逐出导致的实际前缀变化；当前 detached calendar 不是完整的未来 cache-evolution oracle。继续为恢复时间加任意系数，或按每档挑最好版本，不能解决这些缺口。真实并行与 PCIe 阶段数据尚缺时，CPU 结果只作为候选筛选，不宣称已得到实机三档赢家。

这优化的是决策计算，不自动解决 PCIe 数据通路。后续若要做层级 DP 或 relay，应先具备实际层级接口或拓扑、独立成本测量，不能用模拟的免费重叠替代实现。精确的同快照结果一致性与闭环回放是不同验证：闭环里 CPU 时间变化会改变队列交错和缓存演化，因此即使候选池/目标不变，也不能承诺最终 TTFT 必然改善。

上述历史复现实验：95 请求、8 个完整自然会话、相同三个 Poisson 名义流和 seed、同一 32 请求隔离标定；各轮内部模型/容量/输出一致。只使用过去已完成信息。p99 在 95 样本下近似最大值，报告全部失败与负收益，不挑最好轮。当前模拟仍不能识别真实碎片化 I/O 或 batch kernel 的全部因素，不作为性能认证。

用户后续要求移除 2 GiB 设备 KV 限制：新 910B2 容量约束按可见 HBM、模型权重与显式 runtime reserve 推导，并用于所有比较组；实机采用自动 profile。详见 [容量模型、新三档结果与完整释放误差](910b2-memory-20261026.md)。旧 2 GiB 数据不改名、不与新组混配，主机池容量仍独立。

新的搜索成本优化使用事件进度形成首 token 分布的下界，不复用上文已失败的聚合服务下界。仅在下界严格劣于完整 incumbent 时提前停止候选模拟，并在同一冻结决策内复用排除证明。公式、精确机制对照和含实际 CPU 开销的三档结果见 [有界搜索迭代](bounded-search-20261026.md)。该优化保留原在线代理目标，尚未解决未来准入、缓存演化与闭环时序敏感性；不据此声称改进了模型的硬件精度。

2026-10-26 后续从具体 tail 的两个端点等待与静态缓存报价缺项入手，加入按已知操作推进的库存状态：\((L_{j+1},H_{j+1})=\Phi(L_j,H_j,e_j)\)。阶段使用可行的有效前缀而非路由命令；完整保存后才发布、按源实现回收，最后一个 owner 释放前保持保护。模型不读取未来请求或执行标签，也不假设 RDMA 拥塞。两轮三速率 CPU 对照中，第一轮 p99 全改善，反序确认低档退化 2.24%；详细公式、成本和失败证据见 [缓存演化修正](cache-evolution-20261026.md)。因此保留为候选，不据一次全赢升级默认策略。新增预测 wall 较高，下一步应先定位计算热点而不是加恢复惩罚。

热点分析随后确认租约保护刷新反复重建未变化的物理对象。保留不可变对象身份的等价优化通过 285 请求同控制成本完全一致对照，闭环规划耗时下降，但高档反序确认 p99 退化，详见 [稀疏保护更新](sparse-cache-20261026.md)。这说明减少计算量本身不等于解决尾部排队。

从失败请求进一步定位到控制完成约束：路由 CPU/RPC 结束前，新请求尚未进入 P FIFO；D 边界控制结束前，交接也不能争用源/目的 gate。若只把控制耗时加到最终响应时间，而不放入事件日历，预测可能错误地让交接抢在后续 P 计算之前。新增候选用同 arm 已完成 CPU 样本均值和原控制 RPC 常量估计

\[
 a_P=t+r_P+\overline c_P(t),\qquad
 a_{copy}=a_{Dslot}+r_D+\overline c_D(t),\qquad
 s_{copy}=\mathrm{EarliestJointFeasible}(a_{copy},\mathcal K_{copy},c_{copy}).
\]

均值只含控制完成时间不晚于当前决策的样本，空集取零 CPU；已发出的 D 控制使用已知开始和提交时长的剩余量。控制阶段不持有设备 gate，其他请求可继续占用设备。没有新增 task/rate 阈值或恢复惩罚系数，实际开销仍完整计费。该修正的首轮三速率仍有高档负收益，不能用公式更完整代替性能证据。详见 [控制完成日历及结果](control-release-20261026.md)。

## Completed-only control work model (2026-10-26)

The controller is a resource-release predecessor, not a post-hoc latency penalty.
`control-visible-work-01` estimates its CPU time with nonnegative least squares
on `[1, remaining event quanta, physical unit count × live requests]`, fitted
separately for route and D-boundary decisions from completed samples only. Before
identifiability it uses the past sample mean. The resulting duration shifts the
candidate P enqueue and D-control completion events in the resource calendar;
control itself owns no device gate. Full decode, H2D/restore, synchronous save,
copy ownership and physical cache evolution retain their existing constraints.

See [the model and all outcomes](control-work-20261026.md). Lower CPU prediction
error did not yield stable three-rate p99 gains. We therefore retain this as a
model branch and investigate decision timing, including stale D binding across
P queue/service, rather than fitting another rate-dependent restore penalty.

## Phase location and completed exclusive observations (2026-10-26)

Equal service totals do not identify interference. In the optional execution
world, retain the same calibration sample and the same aggregate first/tail
durations F/T while setting

```
r' = clip(r(x) + r_observed(s) - r(x_s), 0, F), for a nonzero accepted prefix
f' = clip(post_response_observed(s), 0, T)
first_body = F - r'; decode_body = T - f'
```

Only r'/f' occupy the exclusive restore/finish phases. These labels are isolated
stage proxies, not pure DMA/kernel measurements. They are hidden from the policy
until the corresponding owned event completes. See [model assumptions and the
negative high-rate result](phase-residual-20261026.md). In the controlled
fixed-input-routing diagnostic, phase placement changed p99 by ~0.928 seconds;
the much larger cross-world native difference also involved admission-sequence
round-robin feedback. We do not attribute that full difference to H2D.

For the next online estimator, completed batched API residence stays excluded.
An independently timed, exclusive D restore phase can instead update the
nonnegative affine physical model `r_n(x)=a_n+b_n*x*bytes_per_token`. Completed
exclusive finish phases update their sample mean. Start from the original
isolated calibration, use only completed same-arm intervals, subtract separately
charged controller CPU once, and leave singleton prefill/decode compute models
unchanged. The corrected forecast replaces the old phase component rather than
adding an extra latency penalty. Model maintenance also consumes measured CPU.
Serving deployment will need actual phase/ownership instrumentation before these
synthetic iteration observations can be treated as physical measurements.

## Acquired occupancy is a state constraint, not a new penalty (2026-10-26)

An already-running restore/save owns its device gate regardless of which new
route is being evaluated. For observed start s and current t, replace replaying
the whole phase by `E[S-(t-s) | S>t-s, completed history]`; preserve ownership
until this residual completes and apply the phase transition once. This fixes
both elapsed-work accounting and illegal overtaking by new copies. It does not
change total execution-world service, assume RDMA congestion, or treat a running
right-censored sample as completed. The explicit beyond-sample tail assumption,
evidence, limits and tests are recorded in [the candidate note](acquired-phase-20261026.md).
The previous cold worst request was predicted accurately; this repair is not
presented as a complete explanation or a guarantee of p99 improvement.

## Compute visitation and the first-token objective (2026-10-26)

The resource constraints alone do not determine within-device service order.
The retained scheduler reserves one token for each active decode before pending
prefill. Under the explicit zero-speedup scenario, that decode service directly
delays pending first tokens even with an accurate route forecast. The candidate
reverses the two compute visitation classes, retaining FIFO within each class,
the same 128-token budget and complete decode/save occupancy. In each iteration,
allocate `min(128, sum remaining eligible prefill tokens)` in prefill FIFO order;
allocate any remaining tokens to active decodes, at most one each. All subsequent release events still enter
the full DAG and the causal replay. There is no weight fitted to measured gain.

This local TTFT priority can delay decode completion, future slot availability
and causal-session successors; it is not a global p99 optimality result. Measure
ready-to-full completion and final drain alongside TTFT. The exact limited rule,
conservation test and full outcomes are in [the candidate note](pending-first-20261026.md).

The resulting release tradeoff must remain explicit. Define D slot occupancy
`N_D(t)=sum_r 1[a_r^(slot) <= t < c_r^(release)]`, where release follows all decode
and synchronous finish work. Slot admission is the earliest time after P
publication with both `N_D<K_D` and sufficient KV capacity; API acquisition also
waits for control/copy/device gates. These constraints were already in the
simulator. Prefill priority changes their future release times rather than
removing them. One candidate tail waited 12.681 s for a slot whose four holders
had already produced first tokens; actual control then took only 0.503 s. In a
reverse high-rate replay, ready-to-full p99 worsened 52.647→83.873 s despite lower
conventional TTFT p99. Therefore the model must retain full-release and nominal
latency reporting; a first-token objective alone is not a full-latency guarantee.

## Slot release as a predecessor of a known first token (2026-10-27)

The new `slot-release-first-01` branch targets the observed 12.681-second D-slot
wait; it does not infer RDMA saturation from that wait. Let Q_D(t) be the actual
FIFO of P-completed requests not yet admitted to D, and A_D(t) its slot holders.
Only when Q_D is nonempty, all K_D slots are held, and every holder is already in
decode, select the next release predecessor by

\[
 j^*=\arg\min_{j\in A_D(t)}\widehat R_j(t),\qquad
 \widehat R_j(t)=(O_j-d_j(t))\frac{\widehat T_{decode,j}}{O_j-1}
                  +\widehat T_{finish,j}.
\]

Here d_j is the observed output-token count. Decode and finish estimates use
the frozen calibration plus completed same-arm observations, not execution-world
durations. T_finish is the synchronous finish/save component; the already
completed restore is not charged again. For this branch the scheduling estimate
does not separately add incremental iteration CPU; actual iteration CPU remains
charged and its completed mean remains in the route calendar. This approximation
must be reported, not silently treated as a hardware timing identity.

Run one autoregressive token of j* at each idle compute boundary, reconsidering
the observed set. Running iterations and restore/save barriers are not preempted.
Once a slot is freed and a new prefill admitted, the parent's pending-first rule
applies again. No fixed decode quota, rate switch, request-name feature or forced
layout action is used. The same external D FIFO is supplied to the forecast and
execution schedulers; inspecting only the batcher's internal waiting list would
miss requests blocked before API admission.

Under the explicitly simulated zero batch-speedup assumption, concentrating
post-first service can advance the next release compared with round-robin
decode. That local argument does not prove global p99 improvement: other decode
holders and their causal-session successors can be delayed, physical cache
evolution and routes can change, and real continuous batching may have substantial
sharing benefits. Keep the parent immutable, measure all three rates with full
drain, and report both TTFT and complete-release distributions before promotion.

## Preserve shared work on a release-critical iteration (2026-10-27)

The release-only branch failed a deliberately optimistic sharing sensitivity.
With identical hardware assumptions in each arm, low/middle TTFT p99 increased
16.103→22.233 and 14.641→17.037 seconds when decode iteration work changed from
the sum to the maximum of singleton contributions. This is not a measured
910B2 batch-speedup estimate. It exposes dependence on a modeling assumption:
serially draining one holder can discard useful co-batched decode progress.

Use the existing execution-model function explicitly, with per-token estimates
c_j and a scenario sharing fraction b (default 0, sensitivity 1):

\[
 \widehat C_b(S)=(1-b)\sum_{j\in S}\widehat c_j+b\max_{j\in S}\widehat c_j.
\]

`release-preserving-cobatch-01` first chooses the same release predecessor j*.
Starting with S={j*}, visit other eligible decode holders in FIFO order and add
j only if C_b(S∪{j})≤C_b({j*}) and the original token budget permits it. Each
member still produces only one autoregressive token per iteration. No epsilon,
task/rate switch or fitted gain threshold is used. At b=0 with positive service,
this reduces to the prior release rule; at b=1 it retains peers no slower than
the selected token. The forecast and execution follow the same cohort rule.

The non-extension is a **prediction**, not a guarantee under unknown batch
kernels or phase noise. Both limiting scenarios are reported, not selected by
performance. Measured iteration/controller CPU is still charged separately;
non-extending model work does not mean free real planning. Full-output,
slot-release, lease, synchronous save and exclusive restore constraints persist.
Real batch timing and instrumentation are required before choosing a calibrated
sharing model or claiming NPU gains.

An offline check did not support making variance of this max function the main
explanation of the new regression: companion stretch summed to only 0.119
device-seconds in the middle case, while its p99 rose 4.354 seconds. The worst
request also lost 10752 accepted P-prefix tokens, spent 4.927 seconds more in P,
and 5.878 seconds more between P and D. Control CPU and causal cache evolution
changed simultaneously. Keep the distributional caveat, but do not add a fitted
"variance penalty" to explain a difference its measured magnitude cannot explain.
The full release/cache/controller feedback must be diagnosed jointly.


### Control-cost feedback is a state transition, not an additive correction

The 2026-10-27 release-cohort diagnosis requires distinguishing forecast work
from the physical release rule. For a decision state x_k, decision a_k and
completed-history cost model theta_k, write

    a_k = argmin_a J(Calendar(x_k, a; theta_k))
    C_k = measured_route_and_boundary_work(x_k, searched_calendars)
    x_(k+1) = F(x_k, a_k, C_k, completed_physical_operations)

Here x includes resource calendars, cache publication/leases, session dependency
and the ready queue. A reduction in C changes arrival interleaving and future
cache state; it is not valid to subtract aggregate control CPU from a measured
p99 or to promise monotone p99 improvement. Forecast release choices can alter
routing before any release-only choice actually executes. A fixed-control replay
is a mechanism diagnostic; a fully charged replay is necessary for net results.
The visible cost estimator must still use only completed samples, not the future
number of calendar events or actual future cache acceptance. No new gain threshold,
queue balancing quota or assumed RDMA congestion follows from this observation.
See `forecast-feedback-20261027.md` for aligned evidence and profiling provenance.


### Exact protection-delta implementation candidate

For physical allocation u on node n, keep

    protected(n,u) = intrinsic(n,u) OR (sum_owner lease(owner,n,u) > 0).

Between publications, only a zero/nonzero transition in that sum can change the
protection predicate. The candidate tracks this delta, while preserving owned-set
release, shared references and intrinsic guards. Any immutable node-tuple identity
change (copy, save, reclamation or replacement) invalidates the index and forces
full reconciliation; no hash-only equality substitutes for object identity.
This reduces repeated protection queries, not the physical resource requirement.
Immutable tuple reconstruction can still cost O(number of allocations), and
rebuilding the index adds work after publication. Therefore a profile hotspot
motivates a trial but does not prove lower total CPU or lower p99. The reference
algorithm and candidate require equal fixed-control trajectories before a fully
charged comparison is accepted. Sources, completed charged results and diagnostics are in
`forecast-feedback-20261027.md`.


### Separate exact implementation cost from endogenous search work

The delta-protection experiment has tested equal zero-control calendars but mixed
charged p99. Therefore define C_k = sum over searched calendars of c(state,option)
plus bookkeeping, rather than infer C_k solely from a candidate name. A change
in per-calendar c can change physical completion interleaving and the future
searched set. Paired copies of the same state/options estimate implementation
cost; charged causal replays estimate their net feedback. Neither substitutes for
the other. The middle-case boundary checks grew24.4% with CPU23.5%, so aggregate
CPU alone cannot establish that the new protection primitive became more costly.
No per-check fitted penalty is introduced; search size is only diagnostic here.


### Measured index maintenance and the exact scan follow-up

The paired identical-state profile did not support the intended CPU speedup:
route calendars cost3.15% more and boundary calendars1.76% more across the sampled
states. This is distinct from the larger changed search set in a charged replay.
The follow-up drops the physical-position index. For original immutable tuple U,
lease zero-crossing set Delta and last synchronized tuple U_prev:

    U is not U_prev: full retained protection reconciliation
    U is U_prev and Delta empty: no work
    otherwise: scan U, recompute protection only where identity belongs to Delta.

This preserves all protection semantics and avoids an unproven position-index
amortization. The scan and dirty-set maintenance remain charged, and full tuple
reconstruction is still required when bits change. No magic gain threshold,
physical-cost penalty, task/rate branch or balancing quota is added. Exact
zero-control equality precedes the fixed three-rate charged experiment; the
candidate is not a replacement default without evidence.

### Nonanticipative two-stage routing at observed P publication

The initial assignment is (p_i,d_i^0) at t_i. At tau_i = actual P publication,
the observable state S(tau_i) includes only current D slots/queues, progress,
published cache and completed cost observations. New requests arriving after t_i
can finish P first and consume the D release forecast at t_i; this is not an
RDMA congestion claim and cannot be fixed by assuming their future arrivals.

For an unacquired suffix, consider (d_i,b_i) in the two D destinations and feasible
published prefix boundaries. Keep p_i and all acquired operations immutable.
Predict readiness tau_i + RPC + estimated reselect CPU; this release owns no D
slot. Forecast the same full resource calendar and minimize the retained ordered
pending-first latency profile, then byte/boundary/stable-destination ties. Actual
enqueue occurs after measured reselect/RPC/commit cost, and admission revalidates
cache. The new CPU estimate uses completed calls only; zero initial CPU estimate
is uncertainty, not free actual execution. A structurally blocked destination
and an available alternative trigger evaluation, not mandatory movement.

This model separates an old route's stale release forecast from service cost
error. Offline alternate emptiness/wait is diagnostic, not an oracle counterfactual
benefit. A new assignment changes session timing and cache evolution; only a fully
charged causal replay can evaluate net p99. The current dirty-scan mixed outcomes
are retained, and a late-binding benefit has not yet been established.
### Progress-preserving optional decisions and a real no-op comparator

The late-decode01 feedback exposes a distinction between information acquisition
and mandatory dispatch work. Search may improve an unacquired D choice, but
making all paths wait for it removes the real no-op from the feasible set.
At time tau, let S(tau) contain only published cache, actual queues/progress and
completed cost observations. Let Phi be the retained ordered pending-first
profile (p99, ordered first latencies, then completion tie breakers).

    J_keep = Phi(Forecast(S(tau), unchanged original FIFO))
    J_move(d,b) = Phi(Forecast(S(tau), own D suffix released at tau+c_hat+RPC))
    choose a move only if J_move < J_keep.

The incumbent's release is its actual original enqueue, not tau+c_hat. Acquired
P/D operations and other FIFO order are immutable. c_hat uses completed calls
only; actual elapsed control is always charged. Under the next CPU candidate,
initial routing and optional advice share one serialized control lane. Its
availability advances by actual c+RPC+commit. Existing device execution can
continue, but newly arriving routing work waits for that lane. No free extra
CPU service or unconditional overlap speedup is assumed. Separate D boundary
control remains the retained, still hardware-unvalidated simulator assumption.

Advice is cancellable: acquisition on the original path invalidates it. At
completion, movement requires the target still be in its original FIFO and the
alternate currently have free admission with no older waiter. Cache is validated
again at actual D service. Cancellation does not refund CPU. Forecasted alternate
availability is not a reservation or a guarantee; its aging risk is explicit.
This separates nonblocking advice from scheduling correctness without adding a
balance quota, fitted benefit threshold, task branch or future service oracle.

### Cooperative control and service dispersion: new evidence boundary

Progress01 preserves D admission but computes all optional candidates before
cancelling obsolete advice. Let c_j be the measured cost of candidate j. The
control lane remains busy through sum(c_j)+RPC even if D acquires earlier.
The next candidate checks legality at each completed candidate boundary and
stops on acquired D or an actually eligible waiting initial route. Its charged
work is setup+sum(c_j actually evaluated)+RPC+commit; cancellation never refunds
work. This changes scheduling of computation, not the uninterrupted search
objective, candidate ordering or64-candidate ceiling. A single calendar is still
non-preemptible. Partial cancelled searches are not labels for full-search cost.

This is not claimed to explain all tails. In the completed six-case progress01
matrix, worst requests have zero direct optional-advice overlap before their
initial route. Their changed paths include P FIFO, cache acceptance and D restore.
Conditional restore diagnostics use ONLY completed calibration/arm observations
at the actual D-control origin, but condition on the post-hoc accepted prefix.
They measure model residuals, not an initial online routing forecast:

    e_i = R_i(actual accepted bytes) - r_hat(node,bytes;completed history).

On sharing1 high D1, total restore prediction is close to total work while a
critical9600-token restore is underestimated by2.437s. Point-estimate calendar
optimization need not optimize the tail of a calendar with dispersed service
times: tail(F(service)) is not F(point(service)). Residual scenarios conditioned
on node/operation and valid completed history are a hypothesis for later work,
not yet an implemented/validated policy. Do not add a constant penalty, use the
current test request's future residual, or claim measured RDMA congestion.

Late-decode01's default middle/high wins and sensitivity losses are retained.
Observed slots released during its blocked enqueue motivate this model change;
they are NOT a counterfactual recoverable-TTFT calculation. Pending CPU replay
must establish whether preserving progress compensates for extra shared-control
contention. A better no-op model does not itself prove a net p99 benefit.


### Snapshot epoch is distinct from controller admission

With measured setup cost c_setup, controller admission is t_c and forecasting
starts at t_f=t_c+c_setup. Acquired-phase observations, resource calendars and
completed-history cutoffs must all use t_f. The end-to-end charged interval
still begins at t_c. Passing t_c to a calendar built from S(t_f) violates the
resource contract; yielding01 was rejected for this exact bug. Independent02
fixes the epoch without relaxing that guard or removing setup cost.

### Recovery value changes while a P request waits (2026-10-27)

Yielding02's sharing-low critical path shows a 0.0391s history copy waiting about
6.5143s after a conservative P FIFO-ready bound. A later P request held its source
gate. The route quote was made before that later admission. Existing source/gate
constraints are necessary but do not make a previously chosen boundary permanent.
This is software endpoint access contention; no measured wire-bandwidth saturation
is established. Its importance is conditional: middle-rate source waits are small.

For a fixed state at actual unacquired P eligibility t, a physical comparison is

    F_remote = max(t, F_source_gate, F_destination_gate, F_direction_slot)
               + C_copy(missing_bytes) + R_H2D(prefix) + C_P(uncached,attention)
               + S_sync_finish
    F_local  = max(t + C_control, F_destination_gate)
               + R_H2D(local_prefix) + C_P(local_uncached,attention)
               + S_sync_finish.

These are INTERMEDIATE predicted completion times, not independent objectives or
necessarily disjoint additive measured timers. Completed-only models preserve
restore/sync cost definitions. C_P is the retained scheduled prefill path, which
already includes any per-chunk synchronous-save work; S_sync_finish denotes only
separately measured post-response completion work, never that nested time again.
Source residual uses observed live age conditioned
on completed issued/actual ratios, not the live operation's future completion.
Moving a request can change other resource/cache paths, so compare full pending
first-token profiles Phi(Forecast(S(t), choice)), rather than greedily minimizing
only F_remote/F_local. Keep is immediate; changed choices include controller cost.
The actual control cost is charged even if keep wins. A smaller current P cost
alone is not proof of smaller system p99 or future multi-turn latency.

The prefill-source-revalidation01 prototype limits re-evaluation to an unacquired
P head with an observed busy remote P source and a currently available requested
prefix. It compares unchanged recovery, current local prefix and zero-prefix
recomputation. Source/destination identity and P FIFO remain fixed. Decision work
uses the existing serialized control lane; the forecast puts a control operation
at the head of the P FIFO with no device resource, matching execution's queue
wait without letting later P requests overtake it. Commit preserves ownership and
actual admission rechecks cache. No forecast invents future requests or future
online revalidation outcomes. Input/rates/seed/physical budgets remain frozen.
This scoped change is under fully charged causal CPU evaluation, not a proven
tail improvement; use the latest research checkpoint for measured outcomes.

### Numerical resolution is not a latency-gain parameter

The first P-revalidation replay exposed a1-ULP reduction in the max predicted
latency overriding a worsened secondary profile. An independent two-stream
fixture reproduces endpoint changes under unrelated P timing when forecast
iterations are coalesced, but not when they are uncoalesced. There is no shared
resource in this fixture; the difference is numerical grouping/accumulation.

The model's real-valued objective is unchanged. Its implementation must certify
the sign of a difference at its numerical resolution; an unresolvable sign is
not evidence of strict improvement. A mathematical arithmetic/error-bound or
provenance solution is required before relying on such choices. Do not fit an
epsilon to these six outcomes, silently round reported TTFT, or claim all CPU
variability is numerical: measured controller-cost differences also alter actual
simulated interleaving and caches. A corrected calendar and control mechanism
diagnostics must precede another performance claim. The current candidate and
its negative results remain preserved, not reclassified as successes.

An opt-in implementation now uses exact dyadic event times: each supplied
binary64 duration is encoded in integer multiples of2^-1074, sums and repeat
products are exact, and endpoints are rounded once on output. This removes
grouping-dependent accumulation without introducing a fitted improvement
epsilon. Physical cost fits and public endpoint/profile rounding are NOT exact.
See numerical-calendar-20261027.md for source, scoped equivalence checks and the
separation between fixed-control mechanism replay and fully charged evaluation.

### Controller work participates in causal cache evolution

Let S_k be the observed, completed-history-only state at decision start t_k,
and g(S_k) the unchanged bounded forecast objective. If the measured controller
cost is C_k, its decision becomes available after the serialized control lane
at a_k=max(t_k, control_free)+C_k. Resource execution and later causal request
readiness depend on a_k; those times in turn affect resident owners, protection
and next state S_(k+1). Thus reducing computation while preserving g(S_k)
does not guarantee a monotonic decrease in final TTFT p99: the feedback path
can change route choices and cache availability at later decision states.

Numerical-choice05 profiling motivates replacing O(UV) repeated forecast
owner-list removal with expected O(U+V) counted-victim scanning. At fixed S_k
and identical C_k, require exactly the same retained owner order, bytes, leases,
reclaim events, forecast endpoints and decisions. At measured C_k, charge all
new work and evaluate the full causal replay; do not subtract saved CPU from
old TTFT as a policy prediction. No penalty weight, fitted gain threshold or
future-service oracle is introduced. See linear-reclaim-20261027.md for the
separate equivalence and net-performance evaluations.

The counted-victim implementation did not reliably reduce local computation.
The source-derived victim set has a stronger property: it is an ordered prefix
of eligible owners, not an arbitrary set. Exploit this invariant for an
identity-only survivor scan; retain general first-equal removal if the invariant
does not hold. This reduces the constant work without a fitted switching
threshold and leaves the resource/cache state transition unchanged. Evaluate
with the same completed-only cost model and full controller charges; the
measured mixed linear01 outcomes show why this is not a net-latency theorem.


### Distinguish dispatch forecast from revised control objective (2026-10-27)

A later D revision replaces the original endpoint prediction. Report residuals
against both origins, retaining their respective routes and cache commands.
The admitted-only objective J(t)=max_{i in pending(t)}(predicted_first_i-ready_i)
can intentionally lengthen one pending request while lowering J(t). That is
not a forecast failure; it also provides no theorem for later causal arrivals,
cache evolution or the final replay p99. The next no-decode-advice-01 ablation
measures the marginal effect of this optional revision and its control costs,
without a fitted penalty, future workload oracle or forced balancing rule.

### Optional control has an opportunity window (candidate, 2026-10-27)

The full-cost ON/OFF ablation was mixed, including zero-committed-move cases.
Thus optional planning cannot be valued by saved controller seconds alone.
For an unacquired D FIFO head with all slots in observed post-first decode,
estimate W=min_j[d_j(N_j-c_j)/(N_j-1)+s_j] from completed-only decode/sync fits
and observed completed token counts. When an iteration is already issued,
include its whole predicted non-preemptible work before applying remaining
counts. Never read its sampled execution duration or future finish time.
If another operation owns the device gate, mixed phases remain, or the head /
completed-control history is unavailable, W is unknown and the parent remains.

Optional advice can only commit before original acquisition. The candidate
therefore skips it when estimated W <= completed full-search mean CPU + RPC.
This is a break-even feasibility trigger, not a fitted reward threshold or a
guaranteed deadline. Future contention and service uncertainty can invalidate
the estimate. Full alternative routing still uses the resource/cache calendar
and pending-first objective; H2D/restore/sync/complete decode are not penalized
or removed. All trigger overhead is charged, and skips do not train search cost.
See advice-window-20261027.md for this unpromoted hypothesis and actual results.


### 2026-10-27: optional search uses acquisition opportunity, not isolated release

Observed queued source-copy gate work delays D release despite no external
owner at the observation instant. Use A_hat(q;S_t), the incumbent resource-DAG
FIFO acquisition time from current known work and completed-only costs.
After paying the keep-forecast/RPC, let C_remaining be the mean of completed
prior searches' remaining candidate CPU and commit CPU, excluding the sunk
first quantum/RPC. Cancel optional remainder when A_hat <= t+C_remaining.
A_hat is an intermediate prediction, not an optimization output or guarantee.
Unknown/cold state falls back; future arrivals are not used. The unchanged
pending-first objective still evaluates allowed route/boundary actions.
First forecast and trigger overhead remain in the measured causal timeline.
See advice-calendar-20261027.md; candidate pending full-cost evidence.

## Phase-order predecessor correction (2026-10-27)

See [ready-prefill-20261027.md](ready-prefill-20261027.md). Exclusive device capacity does not determine arbitration: a later restore previously preceded older ready prefill. New candidate changes only that eligible FIFO predecessor edge, with identical forecast/execution scheduler and full work retained. The two-operation exchange argument is local, not a global tail guarantee. Completed calendar-trigger results remain mixed; sharing failures with0 cancellations cannot be attributed to skips.

## Pending model correction: publication ownership (2026-10-27)

See [save-publication-20261027.md](save-publication-20261027.md). Separate published U from unpublished staging S and live ownership L; unique capacity U∪S∪L is not evictable read-cache capacity. Existing immediate-publication simulation can self-evict a fitting save. Counterexample reproduced; actual binary/failure path still to verify before corrected-world matrices. No retroactive performance claims.
