# Qwen3.5 混合缓存实现与服务器验收

本次实现以 `77be394a0a52089cc6877b0867077a040b772441` 为基线，位于
`feature/pegaflow-hybrid-cache-adaptation`。目标是让 Qwen3.5 的 attention KV 与
GDN 的 conv/SSM 状态按同一完整 token 边界保存、发现和恢复。

代码与离线回归已完成。Linux 原生服务编译及两节点 TP=2 验收已执行。首轮独立
checkpoint 的 9 组用例通过；补充 prompt 边界与源槽位防护后，共享前缀的原生产
顺序和反向生产顺序均为 7/9 通过，整体验收仍未通过。诊断已复现原生 APC 的
计算轨迹差异，后续需要处理 checkpoint 的计算来源与复用一致性。
实际配置与证据见[服务器验收记录](issue28-qwen35-hybrid-cache-acceptance-20261013.md)。
此前的[源码审计](issue28-qwen35-hybrid-cache-audit-20261013/README.md)
保留基线行为及原始诊断记录。

## 已实现的行为

| 路径 | 实现 |
| --- | --- |
| attention 注册 | 将多个 kernel 行聚合为一个逻辑块；K/V 独立 tensor plane 明确按 blocks-first 寻址。逻辑 2048 / 物理 128 的 TP=2 BF16 单 head 布局，每个 K/V 段为 1 MiB、16 行。 |
| recurrent 注册 | conv 与 SSM 各自注册 state slot，保留各自 stride、dtype 和 offset。状态槽不进行 token 行聚合。 |
| 稀疏保存 | 只发布 prompt 内的完整边界；从请求自己的物理槽位验证带 group 的缓存 hash。各组可独立提交；缺失或已经保存的组使用 null sentinel。结束时使用最终 block table 校验 checkpoint。 |
| 源块保护 | 每批保存有独立 ticket；源块通过 block pool `touch` 增加引用。worker 成功复制后回报物理 `(PP, TP)` rank，scheduler 收齐该 ticket 的全部 rank 后通过 `free_blocks` 释放引用。重复和乱序 ACK 按 ticket 处理。 |
| 保存完成 | 混合缓存由整个 forward 的结束钩子同步完成 D2H；RPC 错误、异常及 block/hash 数量错误直接失败。无 forward 的结束步骤也会提交待保存 metadata。 |
| checkpoint 发现 | 先取本机全部 recurrent group/TP shard 的共同 checkpoint；缺失时在 attention 命中内从右向左逐个候选取回。候选不能覆盖所有 group/shard 时继续前移。 |
| backing 取回 | 非零 storage group 可通过单 hash 复用现有 SSD/RDMA prefetch。缺失为终态 miss；取回期间为 Loading。服务端 fetch 身份包含 instance、namespace、request、group 和 encoded hash。 |
| lease | 保留等待中的 attention 和已 Ready 的 recurrent shard；取消、错误和候选回退时释放。缩短命中时保留原 attention lease 长度，多余 destination 填 `None`。 |
| 兼容性 | attention hash group 固定映射 storage group 0。namespace 纳入模型身份、逻辑块大小、hash 算法、group/spec 布局、recurrent shape/dtype 与 speculative slots。混合缓存支持一个 attention 组加 recurrent 组，要求不可变模型身份、共同逻辑块大小及 DCP=PCP=1。 |
| 观测 | 注册日志记录 group、shape、stride、dtype、offset、逻辑块/字节/行数；checkpoint 日志记录保存边界及选择位置；可选 checksum 按实际 group 目标及完整逻辑块读取。 |

`group_id > 0` 的 query 契约为：`wait_for_full_prefix=false` 查询驻留 membership；
`true` 要求恰好一个 hash，启用 backing prefetch，返回 Loading 或终态 Ready。
proto 字段保持不变；语义与 namespace 已更新，因此 Rust workspace、Python project
及 Commitizen 版本统一为 **0.23.4**。所有节点和 Python binding 应一起重建。

保存 ticket 使用 vLLM 已有的 worker metadata 聚合通道。
已核对固定版本的 [base 接口](https://github.com/vllm-project/vllm/blob/0fc695fc6d1d82e9a5ac6835ac8e4e1c83703665/vllm/distributed/kv_transfer/kv_connector/v1/base.py)
和 [model runner mixin](https://github.com/vllm-project/vllm/blob/0fc695fc6d1d82e9a5ac6835ac8e4e1c83703665/vllm/v1/worker/kv_connector_model_runner_mixin.py)。
worker metadata 必须送回 scheduler 才能及时释放源块；该行为列入运行验收。

## 离线证据

13 个定向 Python 测试文件共 **332 passed**，覆盖新的混合缓存回归及既有
TP/hash/layout/namespace/fault tolerance/NPU/P/D 契约。
新增 Rust `checkpoint.rs` 的实际生产身份函数通过独立 `rustc --test` 的 **2 项测试**。
Ruff、修改的 Rust 文件 rustfmt 和 `git diff --check` 纳入本次检查。
检查状态及实现文件 SHA256 记录于[验证记录](issue28-qwen35-hybrid-cache-validation-20261013.json)。

Windows 上的 `cargo check` 曾因 `tikv-jemalloc-sys` 构建脚本无法启动 shell 而中止。
随后已在 Linux Ascend/RDMA 环境完成 core/server 类型检查、原生身份函数测试及
release 服务与 binding 构建。共享前缀输出一致性为当前验收阻断项。

Python 检查在 `python/` 下运行：

```sh
uv run --isolated --no-project --python 3.13 --with pytest --with numpy \
  --with 'requests>=2.26.0' pytest -q \
  tests/test_hybrid_connector.py tests/test_hybrid_reconcile.py \
  tests/test_worker_kv_layout.py tests/test_worker_kv_checksums.py \
  tests/test_tp_shard_pending.py tests/test_combine_hashes.py \
  tests/test_derive_namespace.py tests/test_connector_fault_tolerance.py \
  tests/test_npu_multi_instance_unit.py tests/test_unit_stubs.py \
  tests/pd/test_pd_connector_flow.py tests/pd/test_pd_connector_layout.py \
  tests/test_npu_connector_layer6.py
```

## 服务器验收配置

跨节点完整验收使用 **两台机器，各 2 张 NPU**，两侧各运行 TP=2。
每张卡的可用 HBM 应以模型装载检查为准；主机内存需覆盖 engine pool、编译和进程开销。
单机各项可先用一台双卡机器顺序运行。跨节点测试要求两侧同时在线。

固定运行身份：

- 模型：共享目录中的 `Qwen3.5-35B-A3B`，revision
  `59d61f3ce65a6d9863b86d2e96597125219dc754`；先核对权重和 tokenizer 的 manifest。
- vLLM：`0fc695fc6d1d82e9a5ac6835ac8e4e1c83703665`。
- vLLM-Ascend：`5cb98caaadeff42b5b62b996e34bb2aaa29d20fd`。
- 首轮：文本输入、BF16、TP=2、DCP=PCP=PP=1、逻辑 B=2048、align、MTP 关闭，
  单请求、固定 greedy sampling、固定输入 token IDs，默认操作系统调度优先级。
- 先测试 eager 与同步调度。graph、async、MTP=2、长上下文分别增加，并单独记录结果。

Linux 上先完成原生检查与构建：

```sh
cargo check -p pegaflow-core -p pegaflow-server --no-default-features --features ascend,rdma
cargo test -p pegaflow-core --lib --no-default-features --features ascend,rdma checkpoint::tests
cargo build --release -p pegaflow-server --no-default-features --features ascend,rdma
```

在兼容 torch_npu/vLLM 的已激活 Python 环境中，从 `python/` 重建 binding：

```sh
maturin develop --release --no-default-features --features ascend,rdma
```

部署时记录源码 diff、Cargo.lock、Python 环境、import 路径、实际启动参数和可执行文件
SHA256。验证 binding/server 均为 0.23.4。每节点两套 node-local engine 对应物理 TP rank
0/1，使用同一 MetaServer；四个 engine 的 RPC/HTTP/RDMA 端口均按宿主机现有服务分配。
核对实际网卡/GID、RoCE 连通性、memlock 和小块 RDMA 注册/传输。

connector extra config 至少包含：

```json
{
  "pegaflow.model_revision": "59d61f3ce65a6d9863b86d2e96597125219dc754",
  "pegaflow.mode": "read_write",
  "pegaflow.transfer_backend": "ascend_direct",
  "pegaflow.wait_for_full_prefix": true,
  "pegaflow.tp_shard_endpoints": ["http://NODE_IP:RANK0_PORT", "http://NODE_IP:RANK1_PORT"]
}
```

source 可用 `save_only`，destination 用 `read_write`。这是
`kv_connector_extra_config` 内的对象；外层使用 `PegaKVConnector`、`kv_both` 和
`pegaflow.connector`。两侧固定 `PYTHONHASHSEED`，启用 prefix caching/align 所需的
vLLM 设置。注册后必须核对日志中的真实逻辑 block tokens，而非仅依据 CLI 请求值。

## 执行与通过标准

1. **注册和基线**：核对所有 rank 的 attention K/V、全部 conv/SSM 注册；
   重复无 PegaFlow 基线并保存输出 token IDs。基线稳定后再比较恢复路径。
2. **单机恢复**：source 保存后，在新的 vLLM 实例或清空本地 APC 的实例恢复；
   测试 B−1、B、B+1、2B−1、2B、2B+1，以及前缀延伸和缩短。
3. **跨节点冷目标**：保留 source 的 seal/publication；destination 的 APC 和 engine
   memory cache 初始为空。完成 attention、conv 和 SSM 的网络取回，核对所有 shard
   选择相同边界、外部命中 token 数以及实际跳过的 prefill。
4. **分叉**：构造完整逻辑块结束前发生分叉的两个输入，分别与各自基线比较。
5. **生命周期**：检查 ticket/物理 rank ACK、源块引用归还；注入 shard 延迟、缺失
   候选、取消、驱逐、preemption 和并发，核对 lease/transfer lock 回收及有界失败。
6. **配置扩展**：按 eager → graph → async → MTP=2 → 长上下文逐项验证；每种配置
   单独形成输出与传输证据。通过后再开展 #23 的性能实验。

启用 `PEGA_DIAG_KV_CHECKSUM=1` 和 `PEGA_DIAG_KV_CHECKSUM_LAYERS=all` 核对
保存与装载的完整逻辑 attention 范围和实际 recurrent slot。日志中的 block ID 可不同，
应按 rank、layer、完整边界 hash 对齐内容。checksum 同步设备，会影响性能；正确性
验收后关闭再测时延。必要时启用 connector DEBUG 取得保存边界和 hash。

缓存复用通过标准需要同时满足：**输出 token IDs 与稳定基线一致、同一边界的所有
状态完整取回、外部有效命中大于零、对应 prefill 确实被跳过**。
B 及以下输入保留 forward token 后可自然零命中；它们用于验证边界行为。

归档逐请求输入/输出 token IDs、完整响应、prefill token 数、注册布局、checkpoint/hash、
ticket ACK、source seal、RDMA 字节与完成时刻、目标 load 状态和 checksum。
首次验收使用普通缓存路径；#23 cohort 门控与四臂实验另行验收其 recurrent fetch 关联。
候选查询当前为串行回退，最坏开销随 attention block 数和缓存组数增长，需在压力与
长上下文测试中评估。请求结束时仍有保护引用的批次由完成清理路径归还。
