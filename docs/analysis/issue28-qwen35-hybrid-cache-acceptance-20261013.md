# Qwen3.5 跨节点混合缓存验收与输出差异定位

2026-10-13 在 hust-zsx-81 与 zsx-91 执行 Ascend TP=2 验收。
**整体验收未通过。** 首轮独立 checkpoint 为 9/9；补充发布边界和物理槽位防护后，
共享前缀的原生产顺序与反向生产顺序均为 7/9。易处理的保护已实现，软件回归
**332 passed**；完整输出一致性仍是后续工作门槛。

关键定位：解码产生的 checkpoint、以及较短 prefill 后继续计算的 checkpoint，
在本地 APC 真实命中时，也能产生与跨节点恢复完全相同的 32-token 差异。
相同输入 hash 不足以保证当前后端不同计算轨迹的状态和输出等价。
后续 [原生 APC 与算子诊断](issue28-native-apc-diagnosis-20261013.md) 已定位到
BF16 输出投影的批次形状数值差异，并验证局部修正能消除一类输出分叉；
decode 生成边界状态的完整正确性仍待解决。

实现基线为 `77be394a0a52089cc6877b0867077a040b772441`，工作分支为
`feature/pegaflow-hybrid-cache-adaptation`，服务与 binding 均为 **0.23.4**。

## 固定配置

| 项目 | 实际配置 |
| --- | --- |
| 机器 | 两节点，各 2 张 Ascend 910B2，每卡 64 GiB；容器内存限额各 64 GiB |
| 模型 | `/data/shared_upload/models/vspec/Qwen3.5-35B-A3B`，revision `59d61f3ce65a6d9863b86d2e96597125219dc754` |
| 权重 | 14 个 safetensors 分片全部匹配此前固定的 SHA256 清单 |
| vLLM / Ascend | `0fc695fc6d1d82e9a5ac6835ac8e4e1c83703665` / `5cb98caaadeff42b5b62b996e34bb2aaa29d20fd` |
| 执行 | BF16、TP=2、PP=DCP=PCP=1、eager、同步调度、MTP 关闭 |
| 缓存 | prefix caching 开启，mamba align，逻辑 B=2048；每 rank 2 GiB 设备缓存、2 GB engine pool |
| 请求 | 单请求，固定输入 token IDs，seed=17，temperature=0，top_p=1，ignore_eos=true，输出 32 tokens |
| 长度 | max_model_len=8192，max_num_batched_tokens=4096，max_num_seqs=4 |
| 源端 / 目标端 | 81 为 `save_only`；91 为 `read_write`；各两个 node-local engine，共用 MetaServer |
| 调度 | 相关主进程实测为 SCHED_OTHER、nice=0 |

输入由一条公开 FrontierScience 题目构造固定长度 token 序列，并增加边界前分叉。
该题目用于缓存正确性验收；本轮没有进行任务准确率或性能结论的评测。

91 上关闭 PegaFlow 接入，每组清空 APC 后重复两次。18 次对应输出 token IDs
全部一致。源端完整计算的输出也与这些基线一致。

每 rank 注册 **80 个平面**：20 个 attention K/V、60 个 conv/SSM。attention
为一个 storage group，recurrent 为三个 group。每个 2048-token attention 逻辑块
对应 16 个 128-token 物理行，每个 K/V 段为 1 MiB。

RoCE 使用 `mlx5_2` 的 IPv4 GID 5。1 MiB verbs 测试与实际 engine RDMA 取回
均成功。memlock 上限为 64 KiB，但容器具有 CAP_IPC_LOCK，实际完成了大块注册。

## 输出验收

共享轮次在请求之间清空设备 APC，保留 engine 已发布的数据。各组使用相同前缀
身份。反向轮次共用一个新的 salt，并反向执行相同的生产请求；各组之间仍然共享
缓存身份。

首轮独立轮次让每组分别使用源端／目标端一致的 salt，验证单次生产历史内的
恢复。其 9 组输出检查通过，其中 6 组有有效外部命中，1120 项 checksum 匹配
同一用例、边界 hash、物理 TP rank 和层的源端记录。该结果的范围为独立生产历史。

| 输入 tokens | 首轮共享 | 修复后原顺序 | 修复后反向顺序 | 原顺序首个 forward：已完成 / 本步计算 |
| --- | --- | --- | --- | ---: |
| 2047 | 一致 | 一致 | 一致 | 0 / 2047 |
| 2048 | 一致 | 一致 | 一致 | 0 / 2048 |
| 2049 | 不一致 | 一致 | 一致 | 2048 / 1 |
| 4095 | 不一致 | 一致 | 一致 | 2048 / 2047 |
| 4096 | 一致 | 不一致 | 不一致 | 2048 / 2048 |
| 4097 | 不一致 | 一致 | 不一致 | 4096 / 1 |
| 4097，边界前分叉 | 一致 | 一致 | 一致 | 4096 / 1 |
| 6145 | 不一致 | 不一致 | 一致 | 4096 / 2048 |
| 3073 | 一致 | 一致 | 一致 | 2048 / 1025 |

补充保留源端 APC 的 2047→2048→目标端 2049 用例通过，覆盖同 hash 的旧解码槽
与当前 prompt 槽同时存在的情况。长输入先生产的补充轮次，目标端 2049 和 6145
仍有差异，4097 一致。总计 22 个目标端检查中 16 个一致、6 个不一致。

当前 6145 用例有部分轮次仅恢复到 4096，而未恢复 6144；更严格的物理槽位选择
与 sparse checkpoint 生命周期之间的完整可用性还需要验收。

## 计算轨迹对照

最终本地 APC 对照在源端 `save_only` 模式下，通过验收包装器临时恢复本地 HMA
缓存索引。`prepare_request` 确认真实命中；这些请求没有经过跨节点装载。
包装器不修改 vLLM/Ascend 源文件。早期未恢复该索引的扩展记录只作为完整计算
对照，不能用于证明本地 APC 复用正确。

| 生产 → 目标输入 | 本地 APC 与完整计算基线 | 跨节点与完整计算基线 | 对照 |
| --- | --- | --- | --- |
| 2047，解码越过 2048 → 2049 | 不一致 | 不一致 | 两侧完整 32 个输出 IDs 相同；本地从 2048 开始计算 1 token |
| 2048 prefill → 2049 | 一致 | 一致 | 单个 prefill checkpoint 对照 |
| 2048 prefill → 4096 | 不一致 | 不一致 | 两侧完整 32 个输出 IDs 相同；本地从 2048 开始计算 2048 tokens |
| 4096 prefill → 4097 | 一致 | 一致 | 本地从 4096 开始计算 1 token |

这证明两个关键失败路径可以在本地状态复用中重现。固定版本的 Ascend GDN
prefill 使用 `chunk_gated_delta_rule`，decode 使用
`torch.ops._C_ascend.npu_recurrent_gated_delta_rule`；prefill 的分段路径也会改变
实际计算轨迹。上述结果尚不能单独判定具体算子的数学错误或数值误差来源。

## 数据来源与装载字节

首轮共享 4097 恢复中，第一个 attention 块的两 rank 共 40 个 K/V checksum
匹配早先的 2047-token 生产请求，而 4096 边界 recurrent 状态匹配后续
4096/4097-token 生产请求。不同计算历史在同一前缀 hash 下已经出现不同缓存字节。

修复后的两种顺序和补充用例共 **3160 项装载 checksum**，全部匹配同一边界 hash、
物理 rank 和层的已观测保存记录：2738 项匹配源端记录，422 项匹配目标端此前的
保存记录。后者严格按日志先保存、后装载的顺序核对。

目标端恢复短前缀后继续计算，会保存新的 checkpoint。它可能具有与源端完整
prefill 不同的计算历史，却使用相同 hash。后续查询可以命中这些本地数据。
engine 的 `write_path.rs` 对已驻留 key 的后续保存去重，保留先驻留的数据；当前
发现与恢复只验证输入 hash 和共同 group/shard 边界，未携带计算轨迹或统一快照
身份。跨 group 的来源一致性，以及目标端继续计算后的发布，都需要纳入设计。

首个 4097 冷目标恢复确实完成跨节点完整装载；此前测得两 rank 合计
148,275,200 bytes RDMA 取回。输出检查中的正命中均保留实际 forward 记录。
验证不能用完全重新计算代替正命中的输出一致性。

## 已完成的独立修复与检查

- 完整混合 checkpoint 由整个 forward 结束钩子统一提交，避免首层回调提前保存。
- 只发布 prompt 内完成的完整边界，阻止解码延长部分直接进入 prompt checkpoint 索引。
- 使用当前请求物理槽位及其 group hash 校验保存来源，避免按 hash 任取其他请求的旧槽。
- 增加 `PEGAFLOW_LOG_LEVEL`，并更新 namespace 长度契约的既有回归。
- 13 个定向 source-only Python 文件 **332 passed**；相关 Ruff、format、diff check 通过。
- Linux Ascend/RDMA core/server 检查、2 项 checkpoint 身份测试、release 服务及 binding 编译完成。
  首次构建脚本末尾的辅助包名查询失败；原生编译成功与运行版本 0.23.4 已分别核对。
- 源端和目标端各 8 个应保存的原顺序请求，均观察到本轮 rank 0/1 的 ACK；2047 没有
  prompt 内完整块，不要求 ACK。四个 engine 的终态 inflight bytes 为 0。源端 transfer
  lock 为 0；目标端该指标及失败类计数器未发出样本，记为 null。

两节点运行身份记录中的相关源码 SHA256 与当前实现核对。部署该实现应同时更新
节点与 binding，并重新启动 engine 清理先前发布的数据。

## 后续门槛与证据

下一步需要用状态和 logits 的定量误差区分后端计算问题，并为 checkpoint 定义
计算轨迹及统一来源的兼容性约束。单纯根据 token hash 合并各 group 的缓存，
不足以满足当前严格的输出 IDs 一致性验收。该门槛达成后再开展 #23 的正式性能实验。
graph、async、MTP、长上下文和并发生命周期压力也尚未覆盖。

公开证据：[固定输入、完整输出与核对脚本](issue28-qwen35-hybrid-cache-evidence-20261013/README.md)。
完整归档保存在实验平台共享目录，需要平台存储权限：
`/data/shared_upload/hust-zsx/hybrid-cache-acceptance-20261013/`。
所有响应、输入/输出 IDs、runtime identity、注册、保存、装载、ACK、复制槽位探针与
权重清单均保留。checksum 与槽位探针有同步开销，数据用于正确性诊断。

冻结记录及 SHA256：

- `final-evidence.tar.gz`：`974f10de0830e1073e51e96ecf56e3b649fba5a92cd8a2473edaa6d3d5ff6962`，首轮与独立恢复记录。
- `history-diagnosis-stage3.tar.gz`：`4e289aee48efdb15072b1d00a772395392069b9be309fe385814c7bc9d9fa46c`，解码与本地 APC 因果对照。
- `fixed-gate-stage4.tar.gz`：`73bf22ba4106684d0d5b0ff3f07e1b014c4823011cdb8c4042d6eef0fc7b9f33`，修复后顺序与来源验收。
- `diagnosis-final-stage5.tar.gz`：`1f1fb527c01398ee33f1013719869751d988176bd401e1a405e9573d1ef51ff4`，最终 prefill 对照、来源分析和停止进程记录。

后续 prefill 分段对照、停止进程记录及本地分析结果另外保存在证据目录。
`fixed-publication-summary.json` 记录 3160 项来源匹配；`native-prefill-split-diagnosis.json`
记录分段计算对照，`final-unit-tests.txt` 为最终软件回归原始输出。
两节点本次管理的进程已停止；终态 NPU 检查显示分配卡上无运行进程。
