# vLLM 0.23 原生调度器适配与验收

模拟器增加 `scheduler_backend=vllm`，每个 P、D 实例直接调用安装环境中的 vLLM
`Scheduler` 与 `KVCacheManager`。连续批处理、分块 prefill、APC、页分配和抢占由原生实现决定；
离散事件层继续管理 CPU 缓存发布、网络预取、H2D 恢复、计算耗时及完成通知。

## 验收结果

2026-11-09，在一个容器的两张 910B2 上运行 Qwen2.5-7B-Instruct BF16。模型目录为
`/data/shared_models/Qwen2.5-7B-Instruct`，模型 revision 为
`a09a35458c702b33eeacc393d103063234e8bc28`。vLLM/vLLM-Ascend 为 0.23.0；安装环境的
Scheduler 源码 SHA256 为
`9ae1491f57a6294e32416b93ef2ea68107fa0d65c3a4d7f745f1261012f735e9`。
该部署带有 full-ISL reservation 扩展，因此版本号之外还记录源码身份。

| 实测轨迹 | 请求数 | 调度轮数 | 对照结果 |
|---|---:|---:|---|
| 单卡真实模型：冷缓存、混合长度、共享前缀、4/8 请求并发 | 24 | 364 | 全部对齐 |
| 双卡本机 P/D：P 生成缓存 | 4 | 33 | 全部对齐 |
| 双卡本机 P/D：D 首次恢复及本地 APC 复用 | 8 | 290 | 全部对齐 |
| 合计 | 36 | 687 | token 分配、computed/output 数、状态、抢占数和物理页编号一致 |

模型使用 graph 模式（FULL_DECODE_ONLY）、TP=1、每实例 16GiB NPU KV、最多 16 序列、
每轮 2,048 tokens、128-token 页，关闭 async scheduling。双卡 P/D 使用同一原生服务的
16GiB CPU 池，卡间经过 CPU 缓存交接，未执行跨节点 RDMA。

P/D 的四种输入长度为 2,049、8,193、16,385、24,577 tokens。P 各生成一个 token；
D 每次生成 32 tokens。首次恢复命中 16、64、128、192 块缓存，每个请求只计算一个输入
后缀 token；第二次请求走本地 APC。D 的 290 轮包含等待恢复的空调度轮次。

对照方法是重放实际到达顺序、token 输入/输出和连接器查询/完成通知，然后检查每次原生
调度及更新后的状态。首版采集器未单独观察 HMA 的 finish 回调，重放该回调的延迟释放标志
由实测更新后的请求保留状态推导，结果中明确记录这一来源。查询、分配和收发完成通知有
直接观测记录；后续采集器也支持直接记录 HMA finish 回调。

另外，6 项原生运行时测试覆盖 prefill 分块与前缀命中、异步恢复屏障、小池抢占及排空、
1P1D/2P2D 发布交接、整块输入的 logits 后缀。72 项原有便携测试通过。

## 实现边界

适配器支持同步调度、TP=1、单 full-attention KV group，以及计算与拷贝重叠。请求无法放入
KV 池时明确报错。NPU 池中的 null page 由原生分配器保留，CPU/传输字节量仍使用模型的
`KVLayout`。页管理用的轻量 KV 元数据不分配张量，也不估计权重与 activation 内存。

模拟生成依照配置的输出长度提供合成 token；实际答案、EOS 和生成文本的后续复用需要真实
token。模拟仍需匹配模型、硬件与 graph 配置的耗时 profile。687 轮一致性验证证明的是调度
与状态适配，不能转换成 TTFT/TPOT 误差百分比；历史多秒恢复慢尾也需要独立标定。

使用方法见 [PIPELINE_SIMULATION.md](../../scripts/issue23/PIPELINE_SIMULATION.md)。原生后端
在提供的 Ascend 环境中验收；不执行模型内核的模拟仍依赖能导入的 vLLM 运行时。

证据入口：[benchmark 原始证据与报告](https://github.com/vLLM-HUST/vllm-hust-benchmark/tree/07a95f971f06c2d9a06a7a8bb912ae299b449005/reports/pegaflow-native-scheduler-20261109)。
