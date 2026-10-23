# Qwen3.5 跨节点混合缓存代码审计

审计日期：2026-10-13。关联 [Issue #28](https://github.com/vLLM-HUST/pegaflow-hust/issues/28)。

本报告描述固定基线 `77be394` 的行为。复跑 `probes.py` 必须将 `--repo` 指向
该基线 checkout；其中断言用于复现基线缺陷。后续实现与服务器结果见
[实现报告](../issue28-qwen35-hybrid-cache-implementation-20261013.md)和
[验收报告](../issue28-qwen35-hybrid-cache-acceptance-20261013.md)。

**跨节点恢复需要同时解决 attention 物理块注册、recurrent checkpoint 的边界身份、远端稀疏 checkpoint 发现，以及恢复时的 lease 长度一致性。** 当前仓库已有 HMA 和多组状态传输基础，接下来应在这些实现上修复并验收。

本次完成源码审计、离线诊断、既有单测核验和后续验收设计。五组离线诊断调用当前连接器源码；硬件张量内容、跨节点模型恢复及 MTP/编译图组合仍需运行验证。

## 1. 审计基线与证据

| 对象 | 固定身份 |
| --- | --- |
| PegaFlow | `main@77be394a0a52089cc6877b0867077a040b772441`，包含已合并的 #34、#35 |
| 后续工作分支 | `feature/pegaflow-hybrid-cache-adaptation`，从该 main 创建 |
| 模型 | Qwen3.5-35B-A3B，BF16，revision `59d61f3ce65a6d9863b86d2e96597125219dc754` |
| 已归档运行的 vLLM | `0fc695fc6d1d82e9a5ac6835ac8e4e1c83703665` |
| 已归档运行的 vLLM-Ascend | `5cb98caaadeff42b5b62b996e34bb2aaa29d20fd` |

依赖源码按上述 commit 读取，URL、下载字节数和 SHA256 见 [sources.json](sources.json)。模型配置包含 40 个文本层：10 个 full attention 层、30 个 linear attention 层；全模型 KV heads=2、attention head_dim=256。依据为[固定 revision 的模型配置](https://huggingface.co/Qwen/Qwen3.5-35B-A3B/blob/59d61f3ce65a6d9863b86d2e96597125219dc754/config.json)。

离线诊断：[probes.py](probes.py)、[实际结果](probes.json)。单测范围、命令和数量见 [validation.json](validation.json)。诊断采用源码单测的 stub 环境，不生成 GPU/NPU 推理或 RDMA 性能数据。

历史实验身份和范围仍以 [10 月 12 日归档报告](https://github.com/vLLM-HUST/vllm-hust-benchmark/blob/aadff9107d2514ab8a319a5f6385a99671c98cfd/archive/mod-history-20261023/pegaflow/manifest.json)为准。该运行使用旧基线及归档补丁；本次诊断直接检查当前 main，二者的运行证据分别解释。

## 2. 已有路径及其边界

| 环节 | 当前代码行为 | 本次判断 |
| --- | --- | --- |
| 连接器声明 | `PegaKVConnector` 继承 `SupportsHMA` | HMA 接口已存在 |
| 分组及注册 | `CacheGroupLayout` 识别 MambaSpec，生成 storage group；worker 展开 attention K/V 和 recurrent state tensors | conv/SSM 可保留各自 allocation、shape 和 stride |
| 保存与发布 | 按 storage group 独立 seal；非零 group 对 hash 编码；seal 后进入 read cache 并发布 MetaServer | recurrent 数据保存、分组键和发布基础已存在 |
| attention 查询 | group 0 的连续前缀查询，可触发 SSD/RDMA 预取和 Loading/Ready | 可取远端 attention |
| recurrent 查询 | group > 0 从本机 resident read cache 返回稀疏 hit_positions 与 lease | 当前不会触发远端取回，也不会从 SSD 恢复 |
| 对齐 | 所有 recurrent group、所有 TP shard 共同持有同一个 checkpoint，才保留对应 attention 前缀 | 基本恢复约束已实现 |
| 恢复 | attention lease 装载前缀；recurrent membership lease 仅装载所选 checkpoint，其他位置为 None | 多 lease、多组恢复结构已存在 |
| 生命周期 | scheduler 处理 Ready 保留、请求身份变化和取消；worker 将 lease 交给 engine 消费 | 原 #34 修复可继续使用；HMA 特有边界需补齐 |

主要入口：

- [分组、checkpoint 对齐和 intent](https://github.com/vLLM-HUST/pegaflow-hust/blob/77be394a0a52089cc6877b0867077a040b772441/python/pegaflow/connector/common.py#L255)。
- [物理 tensor 展开](https://github.com/vLLM-HUST/pegaflow-hust/blob/77be394a0a52089cc6877b0867077a040b772441/python/pegaflow/connector/worker.py#L195)。
- [按组编码并 seal](https://github.com/vLLM-HUST/pegaflow-hust/blob/77be394a0a52089cc6877b0867077a040b772441/pegaflow-core/src/offload.rs#L626)、[seal 后发布](https://github.com/vLLM-HUST/pegaflow-hust/blob/77be394a0a52089cc6877b0867077a040b772441/pegaflow-core/src/storage/write_path.rs#L219)。
- [scheduler 的 hybrid reconcile](https://github.com/vLLM-HUST/pegaflow-hust/blob/77be394a0a52089cc6877b0867077a040b772441/python/pegaflow/connector/scheduler.py#L1104)、[worker 多组恢复](https://github.com/vLLM-HUST/pegaflow-hust/blob/77be394a0a52089cc6877b0867077a040b772441/python/pegaflow/connector/worker.py#L766)。

## 3. 需要修复的具体问题

### G1：non-MLA attention 的逻辑/物理块注册不一致

优先级 P0。当前源码行为已离线复现；历史运行的完整 tensor shape/stride 尚需补采。

`_infer_kv_cache_registration()` 对 non-MLA attention 固定返回 `physical_blocks_per_logical_block=1`，使用一行物理缓存的 stride 作为一个 PegaFlow block 的字节数。逻辑块聚合仅实现于 MLA 分支。[源码](https://github.com/vLLM-HUST/pegaflow-hust/blob/77be394a0a52089cc6877b0867077a040b772441/python/pegaflow/connector/worker.py#L93)。

固定版本的 Ascend attention backend 支持 128-token kernel block；hybrid model runner 会按 manager block / kernel block 的比例扩展行数，再分别生成 K/V tensors。[backend](https://github.com/vllm-project/vllm-ascend/blob/5cb98caaadeff42b5b62b996e34bb2aaa29d20fd/vllm_ascend/attention/attention_v1.py#L105)、[hybrid reshape](https://github.com/vllm-project/vllm-ascend/blob/5cb98caaadeff42b5b62b996e34bb2aaa29d20fd/vllm_ascend/worker/model_runner_v1.py#L4559)。

以 TP=2、BF16、单 rank 的一个 K 或 V tensor 为例：

| 指标 | 当前函数输出 | 2048-token 逻辑块应有值 |
| --- | ---: | ---: |
| 输入 shape | `[160, 128, 1, 256]` | 同一输入 |
| 一个逻辑块包含的物理行 | 1 | 16 |
| 注册 block 数 | 160 | 10 |
| 每 block 字节 | 65,536（64 KiB） | 1,048,576（1 MiB） |
| logical block_id=1 的字节偏移 | 65,536 | 1,048,576 |

这不仅涉及传输长度，也涉及 block_id 的寻址空间。应按具体 cache group 的 manager block size、backend kernel block size、tensor stride 和 segments 推导注册，保持 scheduler block IDs 与 engine block IDs 一致。recurrent state 的首维表示状态槽，不能套用 attention token 行聚合规则。

归档 rank 0 计划包含 8 个请求，每请求 2 个 hash、40 个 operation，每 operation 65,536 字节，合计 20 MiB，与历史报告相符。在上述 2048/128 布局、10 层 K/V 的假设下，完整 attention 应为每请求 40 MiB、8 请求 320 MiB。此项是布局推导，不能将它当作历史实际传输量；归档缺少逐 tensor shape/stride，尚不能完成历史逐字节寻址判定。

### G2：结束保存把运行状态和完整块 checkpoint 混用

优先级 P0。保存 metadata 的错误索引规则已复现，实际 state 内容需硬件验证。

`_consume_finished_hma_save()` 取 `saveable=floor(written/B)`，将 recurrent `group[saveable]` 与最后一个完整块的 hash 绑定，同时要求 recurrent table 长度至少 `saveable+1`。[源码](https://github.com/vLLM-HUST/pegaflow-hust/blob/77be394a0a52089cc6877b0867077a040b772441/python/pegaflow/connector/scheduler.py#L864)。

固定版本 vLLM 的 align 模式用 `(seq_len-1)//B` 选择 running state。Ascend GDN 从该 slot 读取和写回状态。[索引规则](https://github.com/vllm-project/vllm/blob/0fc695fc6d1d82e9a5ac6835ac8e4e1c83703665/vllm/v1/attention/backends/utils.py#L871)、[GDN metadata](https://github.com/vllm-project/vllm-ascend/blob/5cb98caaadeff42b5b62b996e34bb2aaa29d20fd/vllm_ascend/ops/gdn_attn_builder.py#L551)、[GDN state 更新](https://github.com/vllm-project/vllm-ascend/blob/5cb98caaadeff42b5b62b996e34bb2aaa29d20fd/vllm_ascend/ops/gdn.py#L386)。

- B=2048、written=4100 时，函数把 table index 2 的状态存到 4096-token 边界的 hash。index 2 是 4100-token running slot；4096-token 边界对应 index 1。是否仍有有效的边界 checkpoint，必须查已 cache 的块及其 hash 身份。
- B=2048、written=4096、MTP 关闭且 recurrent table 长度为 2 时，函数要求长度为 3，直接报 `final HMA block table is shorter than its hash prefix`。
- 问题发生于该结束保存分支有未保存完整块时；此前已保存完的请求不走这段保存逻辑。

此外，常规 `_local_cached_block_ids()` 要求待保存区间每个 hash 在所有 cache groups 同时存在。align recurrent checkpoint 本来可以稀疏，因此前面的缺失 checkpoint 会挡住后面的可用 checkpoint，以及本可独立保存的 dense attention。[源码](https://github.com/vLLM-HUST/pegaflow-hust/blob/77be394a0a52089cc6877b0867077a040b772441/python/pegaflow/connector/scheduler.py#L841)、[vLLM 稀疏状态管理](https://github.com/vllm-project/vllm/blob/0fc695fc6d1d82e9a5ac6835ac8e4e1c83703665/vllm/v1/core/single_type_kv_cache_manager.py#L955)。

修复原则：attention 独立保存完整块；recurrent 只发布具有准确 boundary/hash 身份、确实保存了该边界状态的 slot。running state 和 speculative state 必须各自核验。缺少边界 checkpoint 时按 miss 处理，保存完成前保留源块并完成 D2H。

### G3：recurrent 的远端稀疏发现和取回尚未接通

优先级 P1。协议、server、engine、Python client 四层均能确认。

group > 0 的 server 查询直接进入 `query_group_membership()`；engine 编码 group hash 后仅查询 resident read cache；Python client 明确要求立即 Ready，收到 Loading 会抛异常。[RPC 分支](https://github.com/vLLM-HUST/pegaflow-hust/blob/77be394a0a52089cc6877b0867077a040b772441/pegaflow-server/src/service.rs#L582)、[engine](https://github.com/vLLM-HUST/pegaflow-hust/blob/77be394a0a52089cc6877b0867077a040b772441/pegaflow-core/src/lib.rs#L574)、[Python client](https://github.com/vLLM-HUST/pegaflow-hust/blob/77be394a0a52089cc6877b0867077a040b772441/python/pegaflow/connector/tp_shards.py#L119)。

因此 source 节点存在 recurrent 状态、destination 节点缓存为空时，destination 即使取到了 attention，也会因缺共同 checkpoint 回退到全量 prefill。这与历史实验声明的实际路径一致。

MetaServer 现有查询以连续前缀为语义，遇到首个缺失 hash 即停止。如果 checkpoint 只存在位置 1，直接查询 `[h0,h1]` 得不到位置 1。[MetaServer](https://github.com/vLLM-HUST/pegaflow-hust/blob/77be394a0a52089cc6877b0867077a040b772441/pegaflow-metaserver/src/service.rs#L255)。

建议先做可验收的最小路径：在 attention 命中范围内，从右向左检查候选 checkpoint；将 group 编码后的单个 hash 交给现有 owner 查询和 RDMA fetch；选中所有 recurrent groups、所有 TP shards 均可恢复的同一候选。单 key 查询可复用现有 prefix API，批量稀疏 membership API 可作为后续性能优化。

需要同步扩展非零 group 的 Loading/Ready 处理，pending 身份至少包含 instance、request、group、hash 集和候选边界。保持命中位置与 lease 源块顺序一致，支持缓存驻留、远端取回、请求取消和有界 miss。QueryReady 必须在所选组完整 seal、传输完成并受 lease 保护后发出。

### G4：logits 限制缩短 HMA 命中后，attention lease 没有同步缩短

优先级 P1。离线已复现；真实 engine 有数量相等校验。

例：prompt 正好 4096 tokens，B=2048，attention lease 固定了 2 blocks，recurrent 在位置 0、1 均可恢复。为了留下最后一个 forward token，`_finish_cache_lookup()` 改为 2048-token 命中和 checkpoint=0，但仍保留原来的 2-block attention lease，也没有重新 query。[scheduler](https://github.com/vLLM-HUST/pegaflow-hust/blob/77be394a0a52089cc6877b0867077a040b772441/python/pegaflow/connector/scheduler.py#L352)。

后续 Python 检查的是修改后的 `probe.hit_blocks=1`；真正的 lease 源块仍为 2。engine 要求 lease block count 与 destination vector length 一致，因此会拒绝装载。[分配路径](https://github.com/vLLM-HUST/pegaflow-hust/blob/77be394a0a52089cc6877b0867077a040b772441/python/pegaflow/connector/scheduler.py#L477)、[engine 数量检查](https://github.com/vLLM-HUST/pegaflow-hust/blob/77be394a0a52089cc6877b0867077a040b772441/pegaflow-core/src/lib.rs#L761)。

应在边界缩短时重新获取准确长度的 attention lease，或保持原 lease 长度并为不装载的位置填写 None；选择一种统一契约后覆盖后续分配、worker 和 engine 路径。新 lease 获取失败时释放已获得的各组 lease，完整回退。

### G5：namespace 没有覆盖块布局与模型 revision

优先级 P1，影响多配置共享缓存及模型目录更新后的隔离。

`derive_namespace()` 已包含模型路径、dtype、TP/PP、head/layer 信息、HMA 和 DCP/PCP 等因子，但不包含 block size、模型 revision 或 recurrent shape/dtype 的完整布局指纹。[源码](https://github.com/vLLM-HUST/pegaflow-hust/blob/77be394a0a52089cc6877b0867077a040b772441/python/pegaflow/connector/common.py#L615)。

同一路径的 block_size=128 / 2048、revision=a / b 三个诊断配置均得到 `48313183`。缓存内容不兼容时，旧 block 有可能被同一 namespace 发现；slot 数校验无法完整证明字节布局与模型内容兼容。

应定义 source/destination 共同验证的 manifest：模型不可变 revision 或权重身份、group 类型和顺序、逻辑 block tokens、每层 segments/bytes/stride、conv/SSM dtype/shape、TP 分片。manifest 不兼容时必须在装载前拒绝复用。显式 namespace 覆盖配置也应验证布局。

### G6：membership 响应校验失败时，当前响应的 lease 未释放

优先级 P2，属于异常路径；正常 server 枚举位置的响应不触发此项。

`query_group_membership()` 先校验 Ready 的 positions，再把 lease 加入 results。当前响应校验异常时，catch 只释放先前 results；当前响应带回的 lease 被漏掉。[源码](https://github.com/vLLM-HUST/pegaflow-hust/blob/77be394a0a52089cc6877b0867077a040b772441/python/pegaflow/connector/tp_shards.py#L149)。

注入第二 shard 的越界 positions 后，第一 shard 的 lease 已释放，第二 shard 的 lease 未释放。engine 有 600 秒 TTL，故这是过期回收前的持有残留。[TTL](https://github.com/vLLM-HUST/pegaflow-hust/blob/77be394a0a52089cc6877b0867077a040b772441/pegaflow-core/src/lease.rs#L11)。

修复时先登记已经获得的 lease，再验证响应；统一覆盖 RPC 失败、无效响应、Loading、取消和身份变化。pending 转为异步后，这些分支应一并验收。

## 4. 需要运行验证的内容

以下是验收项目，而非本次已经确认的运行故障：

1. **实际布局**：逐 TP rank、逐层记录 cache group、manager/kernel block tokens、shape、stride、dtype、storage_offset、注册块数、每段字节和布局指纹；核对 block_id 的设备字节范围。
2. **checkpoint 内容与时刻**：明确 conv 和 SSM slot 所对应的已计算 token 边界；区分 cached checkpoint、running slot 和 speculative slots；记录 save 前后、load 后内容校验值。
3. **状态保护**：核实 source save ACK、seal/publication、RDMA source transfer lock、destination query lease、load completion 和块释放顺序。现有 HMA D2H 同步路径需在编译图及 callback 触发方式下验证。
4. **group 映射**：核对当前 Qwen3.5 的 hash group 是否映射到 storage group 0；代码把 storage group IDs 按 group 顺序分配，而 RPC 固定 group 0 为 prefix。对不满足该约束的布局应显式处理或拒绝。
5. **观测覆盖**：现有 checksum helper 使用 `kv_cache[block_id]` 读取单行，并向各层复用同一 block ID 列表。应改为按 group、logical block 范围和 recurrent 目标 slot 取样，才能覆盖 16 行 attention 及独立状态槽。[helper](https://github.com/vLLM-HUST/pegaflow-hust/blob/77be394a0a52089cc6877b0867077a040b772441/python/pegaflow/connector/worker.py#L1282)。

## 5. 实现与 PR 范围

先按以下依赖顺序推进实现：

1. 修复 G1 的注册/寻址、G2 的 checkpoint 身份和稀疏保存、G4 的 lease 长度；补充真实故障形状的源代码回归用例和布局观测。
2. 建立 G5 的兼容性契约，再接通 G3 的远端候选发现、取回和异步 reconcile；同时修复 G6 的异常清理。
3. 完成本地和跨节点正确性矩阵，记录实际支持的运行配置；之后进入 #23 正式实验准备。

该工作分支承载 #28 的连接器适配。可独立验收的基础修复可以单独形成并合并 PR；远端恢复 PR 必须提供真实的跨节点复用证据。每个 PR 的描述以实际改动和已经通过的验收为范围，#23 的性能分析使用通过正确性验证后的连接器版本。

若修改 QueryRequest/QueryReady 或注册协议，同步更新 proto、Rust service、PyO3、`.pyi` 和 Python 调度状态；遵循仓库严格版本握手和版本号一致性要求。

## 6. 正确性验收矩阵

第一阶段固定该模型 revision，文本请求，TP=2，DCP=PCP=PP=1，`mamba_cache_mode=align`，MTP 关闭，默认 `SCHED_OTHER/0`。先使用易于定位故障的执行配置，通过后分别加入编译图、async scheduling、MTP=2，最终核验 #28 要求的配置组合和 256K 上下文配置。

| 验收层 | 输入与操作 | 必须证明 |
| --- | --- | --- |
| 离线布局 | split attention K/V、独立 conv/SSM、offset/stride、多 group | 每个逻辑块的范围、字节和块数准确；recurrent 槽不按 token 行合并 |
| 离线边界 | B−1、B、B+1、2B−1、2B、2B+1；稀疏 checkpoint；末 token 限制 | hash、token 边界、state slot、lease 源块与 destination 一致 |
| 单机 TP=2 | 独立 baseline；保存后清除 vLLM 本地 APC；从 PegaFlow 恢复 | 输出 token IDs 一致；有效外部命中；attention 和 conv/SSM 内容一致 |
| 两节点各 TP=2 | P 预热，D 的 APC 和 engine 均冷；D 仅通过网络恢复 | 每 rank 的 attention、全部 recurrent group 实际取回；共同 checkpoint 命中 > 0；prefill 跳过对应 token 数 |
| 分叉前缀 | 开头相同但完整逻辑块结束前分叉的两条提示 | 累计 end hash 正确隔离；不存在错误命中；输出与各自 baseline 一致 |
| 异常及压力 | shard Loading/Ready 不齐、候选缺失、超时、取消、preemption、缓存驱逐、并发 | 有界 miss/失败；释放所有租约与 transfer locks；无半组恢复、重复取回或永久等待 |
| 配置组合 | graph、async、MTP=2 逐项加入，再运行目标组合 | speculative state、有效边界及输出保持正确；支持范围有对应证据 |

两节点验收需 destination 接到 **同一 checkpoint 的 attention + conv + SSM** 后才能计算有效命中。仅 output 相同、仅传输了 attention、或 D 仍执行全量 prefill，都不能满足跨节点复用门槛。

baseline 与恢复运行应固定输入 token IDs、sampling、并发和 batch 条件；先重复 baseline 确认输出稳定。若基础执行仍不稳定，先定位执行差异再判定缓存一致性。验收结果至少归档逐请求输出 token IDs、prefill token 数、group/slot/checkpoint 映射、传输字节、完成时刻和源/目标内容校验值。

## 7. 本次验证结果与下一项实现

既有六个测试文件共 **106 passed**，其中 hybrid reconcile 7 项、worker layout 15 项、TP pending 12 项、hash/block 调度 44 项、namespace 4 项、connector fault tolerance 24 项。覆盖现有基础行为；本次五组诊断补充了其未覆盖的失败条件。

本次未进行模型推理或 RDMA 验收，相关结论限定为源码行为、协议路径和离线 metadata/布局诊断。

下一项实现应从 **attention 逻辑块注册和 checkpoint 边界保存** 开始，并同步处理命中缩短后的 lease 契约。只有先保证写入的块和状态正确，接通远端取回才有可靠的源数据。
