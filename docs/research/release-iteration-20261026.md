# 资源释放与恢复调度：2026-10-26 第二轮 CPU 迭代

本轮基于 `pcie-handoff-source-20261026-v2.tar.gz`（SHA256 `34bb5bbd15dc14a93073cce05825ac6eb5e56764686812aaf634f71108a4fdfb`）。只使用 yanbo-81 的 4 CPU 配额；自动任务仍 PAUSED，没有 NPU 推理或原生服务部署。前一轮 FairMemo、失败候选和归档全部保留。

## 不变的评价条件

95 请求、8 个完整自然会话，原三档 Poisson 名义率 0.2451617077 / 0.4903234154 / 0.9806468308，同 seed 20261014。同一隔离 32 请求标定、同初始库存、2P2D、D cap4、128 token budget、每实例 2GiB KV 和 2GiB host。shared-compute fraction=0，不假定批量计算量折扣。常规 TTFT=first−causal_ready 为主，nominal latency 另报。95 样本 nearest-rank p99=max。

每个候选与同轮 FairMemo 比较，同时保留 native RR 模拟。native 隐式取块与受控显式 copy 的软件门禁不同，故这是整个服务策略实现的模拟比较，不是仅改变评分函数的纯算法消融。原始执行成本和物理路径仍是粗粒度模型；CPU 输出不等于真实模型输出资格。

## 源码核查与第一项假说

归档实机控制器 `runtime/overlay/pegaflow/resource_runtime.py::fetch_prefix` 在源、目的两端 gates 下读取库存、构造 copy，再调用 `joint_runtime.py::copy`，后者取得方向锁。模型中的“设备先获取、方向后等待”有控制器依据，不是已经证明的硬件要求。主机数据 lease 能否替代整个源 NPU gate，仍需核对原生事务及实机并发，不能直接删除保护。

第一候选 joint-acquire-01 保留源/目的 gates、方向锁、所有服务时间和 allocation lease，只把获取改为三者同时可用才开始。定向反例证实：方向被占 3 秒时，原实现使另一 P 等到 9 秒后结束，新实现允许其在 1 秒结束；复制仍只能在方向空闲后执行。

完整三速率均值世界结果：FairMemo→候选 p99 为 33.1236→25.9508、38.4256→34.0713、37.6196→35.8841 秒。**不据此选择候选。** 三档参考组实际持 gate 等方向的时间均为 0。实际解除该等待没有得到证据；预测日历差异、实测规划耗时引起的闭环交错都可能改变路由和缓存。三档改变 PD 组合 51/54/38 个请求。这是重要的机制反证，不能把表面 21.7%/11.3%/4.6% 降幅包装成锁优化已奏效。

## 第二项假说：恢复不是无条件最高优先级

原 batcher 会在每次迭代前优先处理待恢复对象，可能让已经恢复且尚未首 token 的请求等待。recovery-priority-01 在 restore 和 prefill 间共用：

\[
 \ell_r=a_r^{ready}+J-t-\widehat C_r^{remaining},\qquad
 \arg\min_r\ell_r=\arg\min_r(a_r^{ready}-\widehat C_r^{remaining}).
\]

剩余量包含尚未完成的 restore 与 first body，使用当时已完成样本得出的 issued quote 和已观察 token 进度，绝不读取执行世界的残差服务时间。完整 decode、完成后的 save/释放、HBM 准入 FIFO 和源 gate 保留。save 优先完成以释放已保留内存，不把尚未保存的块提前回收。

三档 p99 为 33.5685→30.1333、32.3407→30.1459、27.3252→30.9196 秒：低中改善，高档退化 13.15%，不升级。高档实际延后 restore 1381 次迭代，机制确实触发；其他请求 restore 覆盖的等待累计 23.63→18.57 秒，但该统计是逐请求累计、可重叠，不能当集群 wall 节省。

高档同一个最慢 `feal-differential-cryptanalysis:11`：其他 compute 覆盖 12.09→9.68 秒，自己参与的 compute 0.64→8.12 秒；常规 TTFT 27.33→30.92 秒。全档 D cache 247808→198528 tokens，restore 92.91→74.07 秒而 compute 834.58→872.28 秒。compute 包含同步 prefill 保存，参与 batch 的区间也不是专属 kernel 时间。少等恢复不等于整条路径更快，局部优先级会改变路由、复用和批成员。

## 第三项修正：更简单的期限顺序，以及明确收费的迭代决策

候选 recovery-deadline-01 用同一首 token 目标 J 的期限 `d_r=a_ready+J`，局部排序简化成 earliest causal-ready。单机、所有任务已就绪、固定处理时间时 EDD 的最大 lateness 性质只是动机；内存、完整 decode 和跨节点依赖下不宣称全局 p99 最优。它避免用不准的 remaining-first 大小驱动局部切换，路由及恢复/重算评分仍保留 H2D、完整释放和资源日历。

三速率同轮四组：native、Timed FairMemo、Timed recovery-laxity、Timed recovery-deadline。每次有实际操作的 D 调度调用，其实测 CPU wall 加到该次设备提交前；同一规则用于所有组。后续预测只使用已经完成的调度样本平均值；合并 n 个预测迭代必须收费 n 次。空模拟轮询不是一次真实提交，不按轮询次数虚增服务成本。该修正不能证明真实 vLLM 并行调度的开销相同。

新增原 dispatch 预测、当时 prefix 命令与完成样本截止时间记录，用于对齐原预测与真实模拟首 token；after-P 边界及实际接受量不能替换原预测。结果另附，运行期间不改源码。

该第三轮随后在中档因已接受 prefix 与旧命令的 token 坐标混用而失败。已保存完整失败归档，不能从两档部分结果选择候选。定向反例修复和用户要求的 910B2 正常内存重跑见 [容量变更记录](910b2-memory-20261026.md)；新旧容量结果分开，不把旧失败重标为通过。

## 数学模型的保留与边界

每项操作仍满足

\[
 s_v=\mathrm{EarliestJointFeasible}(\max(a_v,\max_{u\prec v}e_u),\mathcal K_v),
 \quad e_v=s_v+c_v,
 \quad TTFT_r=e_{first(r)}-a_r^{ready}.
\]

完整 decode、同步发布和 release 是后续请求的资源依赖。取得某资源的顺位不等于实际持有；已开始操作不得被新预约抢占。不能把时间重叠分量相加成 TTFT，也不能在真实/模拟首 token 计时外再加一次规划 wall。

当前 prefill body 的同步保存与逐 chunk 注意力仍未独立识别，按总 body 线性分摊只是近似。H2D task/restore 包含后端和同步，不是纯 DMA。下一次实机应优先识别这些分量和源门禁的必要性，而不是继续给恢复时间乘惩罚系数。可用隔离标定残差作 CPU 敏感性分析，但它不能代替新的 PCIe/NUMA 并发测量。
