# 推理模拟器调研与实现改进

调研日期：2026-11-09。目标是支持在 workload、到达速率、P/D 数量、缓存容量及物理部署变化时，仍能解释性能变化的原因。核查材料包括作者论文、公开仓库及具体实现文件。下文区分已读源码的实现、论文报告的结果，以及对本项目的判断。

## 主要成果及其实现方法

| 项目 | 实现方法 | 对本项目的价值 |
|---|---|---|
| Vidur | 离散事件驱动；分层调度；MLP、attention、通信及 CPU overhead 分开采集和预测 | 将算子与系统事件分开，计算数据与网络拓扑数据分别复用 |
| SimDistServe | SimPy worker/queue；P、D 分别推进；由 token 数、上下文及 batch 特征估计耗时 | 保留可解释的轻量模型，按不同阶段选择特征 |
| Dooly / DoolySim | 按算子签名复用 profiling；模拟时使用真实 vLLM 调度对象，以预测耗时替代模型执行 | 让批处理、缓存分配和抢占语义尽量与执行引擎一致 |
| Frontier | scheduler–batch–engine 事件循环；P/D 等不同角色；配置相关的 KV 布局和可替换通信模型 | 明确模型结构、运行时状态和数据移动的依赖 |
| LLMServingSim 2.0 | 运行时决策、内存层级、硬件执行及互联共同推进；公开实现包含 prefix cache 与 memory model | 容量、复用、搬运和计算需要互相反馈 |
| AIConfigurator | 算子级性能数据库，按模型和部署组合算子成本，再搜索部署配置 | 把测量结果组织为可复用部件，而非每种部署重测整条端到端曲线 |

### Vidur：部件标定与系统调度分离

[论文](https://arxiv.org/abs/2405.05465)及[profiling 说明](https://github.com/microsoft/vidur/blob/abae7f63aa857300f5cdc6f5e0d27860cd24721b/docs/profiling.md)将 compute 与 network 数据分开：同一 GPU SKU 可以复用计算数据，但 NVLink 连接方式不同，应使用不同通信数据。它的预测器分别载入 MLP、attention 和 CPU overhead 数据，调度器决定每步 batch。

这适合我们已有的计算步标定思路。迁移到新部署时，优先补测发生变化的部件，同时验证整条请求路径。Vidur 的一些算子拟合以已知域内预测为目标；本项目还需要跨配置验证，因此继续保留按部署、上下文及会话分组的验证方法。

核查：[预测器源码](https://github.com/microsoft/vidur/blob/abae7f63aa857300f5cdc6f5e0d27860cd24721b/vidur/execution_time_predictor/sklearn_execution_time_predictor.py)。

### SimDistServe：轻量成本模型有用，但需要认清抽象边界

其 [time_estimator.py](https://github.com/LLMServe/DistServe/blob/82831f1604cc6b10bebd360f6c437a07790dde9f/simdistserve/estimators/time_estimator.py) 用 prefill token 总数、各请求长度的平方和构造 prefill 成本；decode 用上下文总量及 batch 大小，并区分大小 batch。worker 通过 SimPy timeout 推进时间。

这说明成本模型不必一开始就很复杂，特征应对应工作量。不过该版本的队列转发和成本估计不足以替代针对 CPU KV 池、RDMA 竞争及恢复慢尾的详细模型。我们保留现有缓存与异步恢复路径，借鉴其可解释的阶段划分。

核查：[worker](https://github.com/LLMServe/DistServe/blob/82831f1604cc6b10bebd360f6c437a07790dde9f/simdistserve/base/worker.py)、[scheduler](https://github.com/LLMServe/DistServe/blob/82831f1604cc6b10bebd360f6c437a07790dde9f/simdistserve/base/scheduler.py)。

### Dooly：优先避免调度语义偏差

[论文 v3](https://arxiv.org/abs/2605.07985v3)通过区分模型固定维度和请求变化维度，为可复用的算子签名建立测量数据库。DoolySim 的 [DoolySim 类](https://github.com/dooly-project/doolysim/blob/9efd9f6549595bb45814f5cff3cd3cd0cb70292a/doolysim/core/dooly_sim.py)实际取得 vLLM scheduler 并调用 `schedule()`，结合预测耗时和模拟输出推进状态。

已据此接入 vLLM 0.23 原生调度后端，保留 PegaFlow 与多节点资源模型，将分块 prefill、APC 和抢占交给安装环境的原生调度器。36 个真实模型请求、687 轮调度的逐步对照通过；具体配置、接口范围和耗时预测边界见[适配验收报告](native-vllm-scheduler-20261109.md)。

Dooly 报告的 TTFT MAPE 低于 5%、TPOT 低于 8%，对应论文中的硬件、引擎及 workload 范围，不能直接视为我们跨节点逐请求尾延迟的目标已达成。

### Frontier：KV 大小与传输路径必须有明确含义

[论文](https://arxiv.org/abs/2605.21312v2)将不同计算角色和运行时优化放进事件循环。核查其 [KV transfer predictor](https://github.com/NetX-lab/Frontier/blob/98b680a24f676b5381c334f49a9729ea45847ee0/frontier/kv_cache_transfer/analytical_kv_cache_transfer_predictor.py)发现：缓存大小按模型 family、层数、KV heads、head dimension 和 dtype 推导；dense K/V 与 MLA 布局分开处理。默认 analytical 路径仍采用固定延迟加字节量/带宽。

因此，模型结构相关的 KV 计算应当借鉴；默认分析型传输公式本身不等于已解决网络争用和 RDMA p99。它的[端到端校准工具](https://github.com/NetX-lab/Frontier/blob/98b680a24f676b5381c334f49a9729ea45847ee0/tests/comparison/calibration/e2e_metrics_gap.py)还明确区分请求身份、路由一致性以及各项耗时/吞吐指标，值得沿用。

### LLMServingSim 2.0 与 AIConfigurator：结构、数据和适用范围分开

[LLMServingSim 2.0 论文](https://arxiv.org/abs/2602.23036v2)将 batching、routing、offloading、memory 和互联放入共同运行时；其 [memory_model.py](https://github.com/senshwang-git/LLMServingSim2/blob/96b1ee004d4e3c9b941d79e0af2f6e739672228e/inference_serving/memory_model.py)按模型维度计算 KV 需求，并保留权重和 activation 空间。它的 ASTRA-Sim 路线适合更细的系统通信研究，但集成成本高于轻量资源模型。

[AIConfigurator](https://github.com/ai-dynamo/aiconfigurator/tree/6cd6f9686ab7de6e8fb28171d6e350ee0aa4ed39)按操作建立性能数据库，组合成本并搜索配置。其文档也说明框架适配、低速高吞吐区域及内存估计仍需实测核对。对我们的启发是按模型、dtype、算子形状、运行时版本及硬件保存能力数据，减少重复标定，同时保留独立端到端验收。

## 本轮已落实的实现

### 1. 用显式 KV 布局统一容量与传输量

此前相关路径默认每块 128 tokens、约 7MiB，绑定了 Qwen2.5-7B BF16。仅换模型名或拟合系数，不能同时修正缓存容量和字节量。

新增 `KVLayout`，支持显式 `bytes_per_block`，或使用每 worker 的层数、KV heads、head dimension 和 dtype 推导 dense attention 的 K/V 页大小。CPU eviction、NPU 页数、命中 token 数、网络 payload 和容量型恢复耗时使用同一布局。workload 记录 block token 数，防止 hash 粒度与 profile 不一致。

标准 dense 页的字节数为：

```text
block_tokens × layers_per_worker × kv_heads_per_worker × head_dim × dtype_bytes × 2
```

形状参数明确指单个 worker，避免直接用 TP 数量除 KV heads 而忽略复制。显式布局不代表已经支持 hybrid/recurrent 的多缓存组生命周期；新模型还需要相应计算和恢复 profile。

实现：[kv_layout.py](../../scripts/issue23/kv_layout.py)、[workload 构造](../../scripts/issue23/prepare_pd_workload.py)。

### 2. 用显式路径表达异构互联及中间瓶颈

新增网络 links/routes，允许同机 CPU-copy、不同方向 NIC 以及中间 fabric 使用不同带宽和启动延迟。多条流复用一个 link ID 就共享该资源；流到达和完成时重新分配 max-min 带宽。一个流先被其他慢链路限制时，多余带宽可以分给仍能提速的流。

同机不自动等于 NVLink。本项目当前远端缓存路径涉及 CPU 池，同机池间移动应描述为 CPU-copy；恢复至 NPU 仍是独立的 H2D 阶段。

输出记录每条传输的路径、transport、启动延迟和每条链路承载的字节数。这样可以区分“卡不够”“目标 NIC 满”“共享中间链路满”等不同结构。

实现：[pd_network.py](../../scripts/issue23/pd_network.py)、[流水线接入](../../scripts/issue23/pd_pipeline_simulator.py)。配置示例见 [PIPELINE_SIMULATION.md](../../scripts/issue23/PIPELINE_SIMULATION.md)。

## 验证与解释

本轮使用已有数据和 CPU 侧检查。72 项相关测试及修改文件的 Ruff 检查通过，CI 已接入这些模拟器检查。修改前后的 28 个历史条件、1,464 次请求回放，逐请求 TTFT/E2E 的最大变化为 0 秒，网络字节量与 D 重算量保持一致。这验证默认配置的兼容性，不能作为新增部署的真实精度验收。

新增检查覆盖：dense K/V 页尺寸、CPU/NPU 容量与 payload 一致性、64-token 页的 logits 后缀、独立路径、共享 fabric、不对称瓶颈剩余带宽分配、同机路径绕过 NIC、流中途到达及 16P16D 的状态排空。16P16D 检查是结构与资源记账验证，未建立该规模的真实预测误差。

新网络模型仍是流体近似，未实现包队列、ECN/PFC、QP 公平性，也未统一仲裁 H2D 与 RDMA 的共享 CPU-memory/DMA 流量。历史完整模型路径中的多秒恢复慢尾仍需原生运行时归因；本轮没有证据证明这一慢尾已被修复。

## 后续实施顺序

1. 使用已验收的 vLLM 0.23 调度后端，继续按计算、发布、网络与恢复部件校准耗时，并保留逐请求验证。
2. 给测量数据库增加模型/KV 布局、引擎版本、算子形状和物理路径身份，优先复用未变化的部件。
3. 有实测资源时，分别验证本地 CPU 池拷贝、跨节点池传输、H2D 恢复及其重叠；按会话和部署保留验证，报告逐请求误差及尾部分位数。
4. 研究重点进入交换机拥塞控制时，再接更细的网络 backend；当前先以可解释的共享资源模型验证瓶颈位置。

## 核查的源码版本

| 仓库 | 固定提交 |
|---|---|
| microsoft/vidur | `abae7f63aa857300f5cdc6f5e0d27860cd24721b` |
| LLMServe/DistServe | `82831f1604cc6b10bebd360f6c437a07790dde9f` |
| dooly-project/doolysim | `9efd9f6549595bb45814f5cff3cd3cd0cb70292a` |
| NetX-lab/Frontier | `98b680a24f676b5381c334f49a9729ea45847ee0` |
| senshwang-git/LLMServingSim2 | `96b1ee004d4e3c9b941d79e0af2f6e739672228e` |
| ai-dynamo/aiconfigurator | `6cd6f9686ab7de6e8fb28171d6e350ee0aa4ed39` |
