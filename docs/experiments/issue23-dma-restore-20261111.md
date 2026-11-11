# DMA 预注册：缓存恢复优化与实验进度

2026-11-11，关联 [Issue #23](https://github.com/vLLM-HUST/pegaflow-hust/issues/23)。

**本地恢复对照中，同样的 3.773GiB 缓存从约 10 秒降至约 1 秒。真实模型四会话测试中，191 条请求从中途失败变为两次全部完成；共同成功的 148 条请求，平均首字等待减少约 47%～63%，p99 减少约 86%～88%。**

## 优化做了什么

问题发生在缓存已经取到 CPU 后，搬回 NPU 的阶段。原生恢复曾超过 120 秒，导致请求失败。

原来的 CPU 缓存池已经调用 `aclrtHostRegister(...ACL_HOST_REGISTER_MAPPED...)`。这次增加的是驱动提供的 `halHostRegister(...HOST_MEM_MAP_DMA...)`：申请两块 128MiB 中转内存，首次使用时做 DMA 注册，后续反复使用。每批把当前缓存复制进去，再搬回 NPU；复用前等待上一批读完，避免提前覆盖。原来的 RDMA 缓存池继续使用。

相关原实现见 [CPU 池](../../pegaflow-core/src/pinned_mem.rs)和 [Ascend 注册接口](../../pegaflow-core/src/device/ascend.rs)。

## 前置实验：同地址开关对照

11 月 11 日，在 91 的两张卡上分别固定进程、CPU 源地址、NPU 目标地址和源内容，恢复 161 块缓存，共 3.773GiB、15,456 个拷贝描述符。下表是同一块影子缓冲区“开 → 关 → 再开”的结果。

| 运行 | 预注册开启，stage-4 | 关闭，stage-5 | 重新开启，stage-6 |
| --- | ---: | ---: | ---: |
| `container1111-native-shadow-toggle-device1`，物理卡 7 | 0.786s | 9.372s | 0.798s |
| `container1111-native-shadow-crossover-device0`，物理卡 2 | 1.093s | 10.879s | 0.875s |

关闭到重新开启，耗时下降约 91.5% / 92.0%。恢复窗口包括当次注册/注销，首次整池申请和初始化复制另计。随后打乱顺序复测，91 的八组配对中七组预注册更快；约 12 倍是上表慢例的加速比。

此对照使用固定内容的整池影子缓冲区，用于归因；真实模型使用每批更新内容的双中转区。

## 真实模型：明确比较哪三次实验

共同设置：2P2D，81、91 各两张 Ascend 910B2；每卡一个实例、TP=1。模型为本地 `Qwen2.5-14B-Instruct-1M-plainrope`，BF16，graph/APC 开启，上下文上限 98,304，引擎并发 16，token budget 8,192，缓存块 128 tokens，每实例 CPU 池 24GiB。

输入为 SWE-Next revision `e378a60ddd7050fe9519a31a4d41d4872eeec6ac` 的四个完整会话：`swenext-1566/1662/2695/3517`，共 191 条请求。随机选择 D，名义 Poisson 3 req/s、seed 17；同一会话等待上一条完成，因此实际到达速率受会话依赖约束。输出按参考长度生成。使用请求自身的缓存流量、原速 RDMA。

时间均为北京时间：

| 编号 | 开始时间 | 完整运行名 | 完成情况 |
| --- | --- | --- | --- |
| A：原恢复路径 | 11-09 22:52:51 | `random-numa-4sessions-rps3-s17` | 148 成功、1 失败、42 取消 |
| B：双中转区 | 11-11 13:59:58 | `random-dma-stage-4sessions-rps3-s17` | 191 成功 |
| C：同条件复测 | 11-11 14:29:51 | `random-dma-repeat-4sessions-rps3-s17` | 191 成功 |

按会话、步骤、输入长度、输出目标及 P/D 分配匹配，共同成功的 148 条请求：

| TTFT | A | B | C |
| --- | ---: | ---: | ---: |
| 平均 | 4.385s | 1.643s | 2.307s |
| p50 | 0.641s | 0.698s | 0.624s |
| p95 | 15.932s | 6.300s | 11.237s |
| p99 | 105.140s | 13.131s | 14.341s |

TTFT 从满足会话依赖时起，计到首个输出 token，包含前端排队。改善主要在慢请求。

B、C 全部 191 条请求的平均 TTFT 分别为 1.794s / 2.586s，p99 为 18.331s / 28.139s；运行 699.11s / 849.31s，输出吞吐 30.28 / 24.92 tokens/s。这里与上一表的样本范围不同。

这次还完成固定 D、16-token 输出、六会话及 D 前端并发为 1 的对照，六组正式运行共成功 1,246 条。两个节点各做七轮源内容更新检查，合计 21,504 个描述符目标标记均正确。

上述模型前后对比跨日期，期间还调整了 P 本地存储和启动处理，属于整套实现的观测改善。六会话重负载仍出现 128 秒恢复，主要耗在事件等待及管理区段，另有缓存分配失败；后续需分开定位设备等待、计算竞争与缓存接纳。

## 本仓库中的代码

| 路径 | 用途 |
| --- | --- |
| [dma_staging.c](../../scripts/issue23/dma_restore_sources/dma_staging.c) | 真实模型使用的双中转区原型，拦截 H2D `aclrtMemcpyBatchAsync` |
| [shadow_dma_toggle.c](../../scripts/issue23/dma_restore_sources/shadow_dma_toggle.c) | 同地址、固定源内容的预注册开关对照 |
| [host_dma_cache.c](../../scripts/issue23/dma_restore_sources/host_dma_cache.c) | 独立的驱动缓冲区关闭/开启/开启/关闭对照 |
| [dma_restore_tools.py](../../scripts/issue23/dma_restore_tools.py) | 编译、显式加载实验库、清理条件间缓存 |
| [dma_restore_probe.py](../../scripts/issue23/dma_restore_probe.py) | 单卡原生 IPC 恢复对照及目标标记检查 |
| [dma_restore_replay.py](../../scripts/issue23/dma_restore_replay.py) | 完整会话 2P2D 重放、可见性等待、预算和逐请求分段 |
| [dma_restore_trace.py](../../scripts/issue23/dma_restore_trace.py) | 记录实际 vLLM 调度步、计算进度与队列 |
| [dma_restore_startup.py](../../scripts/issue23/dma_restore_startup.py) | 实测使用的可选启动 worker，回收闲置 Torch 主机锁页缓存 |
| [common.py](../../python/pegaflow/connector/common.py) | `pegaflow.model_identity` 支持不同物理路径的同一份模型共享缓存身份 |

C 文件来自上述实测版本；Python 入口整理为显式路径参数。原始执行副本与日志保存在下文的固定 benchmark 提交。新入口整理后的检查范围为 CPU 工具契约，硬件性能数字来自已完成的实验。

### 编译和单卡对照

需要 Linux、CANN 9.1.0 对应头文件和运行库、`ascend_hal.h`、编译器及 `numactl`。原生服务与 Python 扩展应由同一版本构建。将下列路径替换为实际目录，输出放在 checkout 之外：

```bash
export DMA_RUN=/absolute/external/dma-run
export CANN_INCLUDE=/path/to/cann/include
export DRIVER_INCLUDE=/path/to/driver/include
export NATIVE_BIN=/path/to/pegaflow-server-py

python scripts/issue23/dma_restore_tools.py build --kind staging \
  --include "$CANN_INCLUDE" --include "$DRIVER_INCLUDE" --output-dir "$DMA_RUN/build"
python scripts/issue23/dma_restore_tools.py build --kind shadow \
  --include "$CANN_INCLUDE" --include "$DRIVER_INCLUDE" --output-dir "$DMA_RUN/build"
python scripts/issue23/dma_restore_probe.py --output "$DMA_RUN/fixed-card0" \
  --binary "$NATIVE_BIN" --library "$DMA_RUN/build/shadow_dma_toggle.so" \
  --device 0 --numa 0 --counts 161 --policies 29,30,29 --all-tags
```

`--device` 为当前可见设备编号，`--numa` 按实际拓扑选择。默认约用 12GiB NPU 张量、5GiB CPU 池和另 5GiB 影子区。脚本记录耗时、标记、cgroup 和线程状态，结束时停止自己启动的服务。`--policies` 支持交错重复，`--layouts contiguous,reversed,random` 比较目标布局。影子库只适合此固定源内容实验。

独立拷贝探针用 `build --kind host-probe` 编译，另加 `--library-dir /path/to/cann/lib64`，运行 `host_dma_cache 0`；实际输出重定向到外部目录。

### 真实模型接入

在两个 D 的原生服务启动命令前显式加载双中转区，D0/D1 各用独立日志：

```bash
python scripts/issue23/dma_restore_tools.py exec \
  --library "$DMA_RUN/build/dma_staging.so" --log "$DMA_RUN/D0-dma.jsonl" \
  -- "$NATIVE_BIN" --devices 0 --pool-size 24gb \
  --addr 0.0.0.0:38752 --http-addr 0.0.0.0:38792 --enable-prometheus
```

这是包装示例，服务参数需补齐当前部署的 metaserver、RDMA 与 NUMA 配置。每个原生进程只服务一张 NPU，每次 H2D batch 总量上限 128MiB，中转区约占 256MiB/进程。缓冲区随进程退出释放；初始化和事件失败后的清理还需工程化。

P、D 使用相同且固定的 `pegaflow.model_revision`。如果权重在不同目录，另将 `pegaflow.model_identity` 设为双方相同标识；操作者应确保对应同一份权重。revision、精度和布局等因素继续隔离缓存。

### 完整会话重放

需要 Python 3.11+、httpx 和已部署的四个模型实例。P 设置 `PEGAFLOW_SYNC_SAVE_ON_FINISH=1`、`PEGAFLOW_SAVE_ACK_DIR=$DMA_RUN/visibility/P0`（P1 对应修改）；通知目录必须被重放器看见，控制和通知优先使用 P 本地盘。

轨迹为 gzip JSONL，每行含 `session`、`turn`、`prompt_tokens`、`prompt_token_ids`、`qwen_output_tokens`。原始工作负载和实际服务启动命令在 benchmark 的 P 原件包。将端点写入 `$DMA_RUN/config.json`：

```json
{
  "hosts": {"P": "P_CONTROL_HOST", "D": "D_CONTROL_HOST"},
  "served_model_name": "qwen14",
  "kv_bytes_per_token": 196608,
  "source_budget_bytes": 25769803776,
  "instances": [
    {"id": "P0", "role": "P", "model_port": 38680, "http_port": 38790},
    {"id": "P1", "role": "P", "model_port": 38681, "http_port": 38791},
    {"id": "D0", "role": "D", "model_port": 38682, "http_port": 38792},
    {"id": "D1", "role": "D", "model_port": 38683, "http_port": 38793}
  ]
}
```

```bash
python scripts/issue23/dma_restore_tools.py reset --config "$DMA_RUN/config.json"
python scripts/issue23/dma_restore_replay.py --root "$DMA_RUN" \
  --workload /external/workload.jsonl.gz --label random-dma-repeat \
  --rate 3 --routing random --seed 17 --sessions 4 \
  --d-pending-budget-gib 24 --d-inflight 16
```

`--sessions 4` 按最长输入选四个会话后保留其全部步骤；`0` 使用全部会话。P 每实例前端并发为 4，D 由 `--d-inflight` 控制；源预算保留至 D 首 token。每轮使用新标签，条件之间清理缓存。基线直接启动原生服务，优化组显式加载 staging 库。

详细调度遥测：`PYTHONPATH` 包含 `scripts/issue23`，设置 `SCHEDULING_TRACE_DIR`，并增加 `--scheduler-cls dma_restore_trace.TraceScheduler`。可选启动 worker 为 `dma_restore_startup.DMAStartupWorker`，设置 `PEGAFLOW_TELEMETRY_DIR`，使用实测 torch_npu/vLLM-Ascend 接口。实测软件为 vLLM / vLLM-Ascend 0.23.0、torch 2.10.0+cpu、torch_npu 2.10.0.post4。

原部署另采 native、连接器、NPU 和 cgroup 日志。重放器尝试从 P:38698 / D:38699 记录时钟偏差，端口可通过 `clock_ports` 指定；服务缺失时记录错误并继续，跨节点关联需要补齐。启动后可用 `--smoke` 检查四条路由，应逐条确认远端命中和实际恢复，再清理缓存正式测量；HTTP 200 只能说明请求完成。

## 完整数据和后续工作

原件、逐请求结果和图表统一保存在 benchmark，以下链接固定到上传的提交：

- [前后对比详细表及原件定位](https://github.com/vLLM-HUST/vllm-hust-benchmark/blob/472bde58203b385c8897419d67b75f92f4cb5811/reports/pegaflow-restore-causal-20261110/HANDOFF-20261111.md)
- [同地址开关与重复测量](https://github.com/vLLM-HUST/vllm-hust-benchmark/blob/d67aea6eb3eb0dee271a01b4d66acce8b65bd034/reports/pegaflow-restore-causal-20261110/CONTAINER-DMA-20261111.md)
- [六组真实模型实验及异常分析](https://github.com/vLLM-HUST/vllm-hust-benchmark/blob/d67aea6eb3eb0dee271a01b4d66acce8b65bd034/reports/pegaflow-restore-causal-20261110/MODEL-RERUN-20261111.md)
- [逐请求汇总、分段与原件清单目录](https://github.com/vLLM-HUST/vllm-hust-benchmark/tree/d67aea6eb3eb0dee271a01b4d66acce8b65bd034/reports/pegaflow-restore-causal-20261110)

下一步将中转区申请、事件生命周期和错误清理纳入原生 Ascend 后端，在相同负载内交错复测开关效果；重负载慢尾继续按阶段归因。
