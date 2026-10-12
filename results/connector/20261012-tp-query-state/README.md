# TP 查询状态与 ACK 验收

2026-10-12，在一台双 Ascend 910B2 机器上验收 PR #34 的独立连接器修复。运行代码提交为 `812f3a3af64d87805fd9b26d1a15b03f7500db3e`，模型为共享目录中的 Qwen3-0.6B BF16。模型文件及连接器源码的原始字节 SHA256 见 `runtime-identity.json`；`source-git-identity.json` 同时记录 LF 规范化后的运行文件及 Git blob SHA256，七个连接器文件均核对一致。

## 结果

- 源码单元回归：296 passed、23 skipped、11 deselected。
- 两次独立启动的 baseline：9/9 组输出一致。
- TP2 双 engine 正确性：仓库原有 4 项 E2E 断言通过，11/11 个缓存场景输出与 baseline 一致。
- 两个 engine 各保存 8 个块、58,720,256 bytes；各命中 3 个块、加载 22,020,096 bytes；RPC 和加载错误均为零。
- 两个 engine 的 effective TP rank 均为 0，物理 rank 的 ACK 留下 8 组 `.tp0` / `.tp1` 成对标记。
- 并发压力：仓库 warm-hit stress 断言通过，12 个并发请求全部完成；压力前、热命中及压力后的输出一致，KV 占用日志峰值 94.3%，RPC 和加载错误为零。

完整数值见 `summary.json`。`raw-evidence.tar.gz` 保存原始服务日志、客户端输出及 logprobs、pytest XML、两侧指标、ACK 文件和压力阶段的 vLLM 指标。

## 固定配置

正确性验收采用 TP=2、PP=1、max_model_len=4096、block_size=128、每 rank 2 GiB KV 容量、max_num_seqs=16、eager 执行。baseline 使用 NoopKVConnector；缓存阶段每个 rank 对接一个独立 PegaFlow engine，每个 engine 使用 1 GiB host pool、`ascend_direct` 传输；启用同步保存及 ACK。原始测试保持 temperature=0、seed=42、max_tokens=50、vLLM 中性 generation config，以及 `VLLM_BATCH_INVARIANT=1`。

并发压力使用同一模型和 TP2、一个 PegaFlow engine、max_model_len=2048、每 rank 256 MiB KV 容量、max_num_seqs=16。请求内容、12 路并发、8 路输出上限 96 / 4 路上限 32、冷/热/最终输出比较均来自仓库原有压力测试。

运行环境：Python 3.12.13、torch 2.10.0+cpu、torch-npu 2.10.0.post4、vLLM 0.23.0+empty、vLLM-Ascend 0.23.0、CANN 9.1.0。vLLM 与 vLLM-Ascend 的 Git 提交及干净状态见运行身份文件；Cargo 依赖冻结在本目录 `Cargo.lock`。

启动适配器保留 CANN 的 Python 路径，选择 `CUSTOM` 使 vLLM-Ascend 使用其注册的 attention 后端，并对 PegaFlow server 添加 `--disable-numa-affinity`。两份适配器仅调整平台启动、收集证据及增加分片检查，直接调用仓库测试中的输出、缓存活动和错误断言。

## 重跑

选择具备上述依赖及双 NPU 的环境，将仓库放在一个工作目录的 `source/` 子目录，并把本目录两份 `run_*_gate.py` 复制到该工作目录。使用 `.venv` 安装本地构建的 PegaFlow Ascend 原生模块，确保 `cargo`、`vllm` 来自对应的环境：

```bash
cp results/connector/20261012-tp-query-state/Cargo.lock Cargo.lock
cargo build --release --locked -p pegaflow-server --no-default-features \
  --features pegaflow-server/ascend,pegaflow-server/rdma
python -m maturin develop --release --locked --no-default-features \
  --features ascend,rdma --manifest-path python/Cargo.toml
```

从工作目录运行 `python run_sharded_gate.py`、`python run_stress_gate.py`。两份冻结适配器使用模型路径 `/data/shared_upload/models/Qwen/Qwen3-0.6B`、服务端口 28220/28221，输出分别写入 `sharded-evidence/`、`stress-evidence/`；运行前确认这些端口空闲，保留 CANN 环境和 Python 动态库搜索路径。

此次验收覆盖纯 attention 模型上的 TP 查询与 ACK 行为。跨节点 Ready/Loading 交错及取消路径另有单元回归；Qwen3.5 的跨节点 attention + recurrent 完整复用由 #28 的后续验收覆盖。
