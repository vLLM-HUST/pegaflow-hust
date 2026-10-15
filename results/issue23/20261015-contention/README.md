# 2026-10-15：原速 RDMA 竞争四组实验

八个配对批次，每批十六条，四组各 128 条、共 512 次前景执行。使用 Qwen3.5-35B-A3B、BF16、TP2、两节点各两张 Ascend 910B2，完整混合缓存每条 148,275,200 字节。前景软件限速关闭，独立持续饱和 RC READ 背景；主测试用时 570.59 秒。

[完整条件、结果和结论边界](../../../docs/experiments/issue23-contention-pilot-20261015.md)；[阶段交付索引](../../../docs/experiments/issue23-stage-20261015.md)。

![四组结果](figures/four-arm-results.png)

![排队、CPU 和解码](figures/performance-attribution.png)

## 离线复核

从仓库根目录执行，只使用 Python 标准库：

```bash
python scripts/issue23/verify_contention.py \
  --root results/issue23/20261015-contention
```

复核逐文件 SHA-256、冻结操作和请求身份、完整载荷、1024 条首次模型工作记录、全部四组延迟分布、解码事件窗、QP 排队/服务、CPU 配额及端口计数。端到端 TTFT 使用客户端单调时钟，原生缓存阶段及模型工作使用 D 时间戳；SSE 事件窗按 P 客户端墙钟与 D 墙钟对齐。

`cohort-*/cohort.json.gz` 保存用例 token IDs、冻结计划与原始完成记录；每个 arm 的 `*.json.gz` 保存客户端响应、native observed 事件、观察窗口、后台周期报告和 decoder SSE 事件。`telemetry.json.gz` 保存复核所需的 D 资源样本、首次前景模型工作与已有解码事件时间。该文件是完整遥测的字段投影，来源文件和序列在首次工作记录中保留。

原始记录中的绝对 `result_dir` 仅供追溯。复核器按当前目录读取材料，迁移机器后也可运行。`pilot-analysis.json`、`pilot-context-analysis.json` 保存报告汇总；`calibration.json` 保存网络校准原始输出，包含未成功的工具模式尝试。

## 重新生成图片

需要 matplotlib、numpy 及中文字体；输出到新目录可保留原始图片身份：

```bash
python scripts/issue23/render_contention.py \
  --root results/issue23/20261015-contention --output /tmp/issue23-figures
```

脚本自动选择 Microsoft YaHei、Noto Sans CJK SC 或 Source Han Sans SC，也可通过 `--font /path/to/font.otf` 指定字体。原图同时提供 PNG、矢量 PDF 和 `figures/figure-data.csv`。

## 运行身份

`execution-identity.json` 记录依赖版本与原生制品 SHA-256。`execution-source.patch.gz` 保存启动目录的源码快照，相对基线和 LF 规范化后的 41 个文件 hash 见 `execution-source.json`。可在独立、干净的对应基线 checkout 中解压补丁并使用 `git apply` 重建该快照。

Python overlay 直接使用此启动目录；原生二进制和扩展复用了前一轮构建。41 个文件的 hash 验证覆盖快照重建，原生制品以单独记录的 hash 标识；归档未建立这些制品与整份启动快照之间的逐文件构建对应关系。合并版本的编译和硬件回归单独登记。

完整运行归档为 `pilot-01-evidence.tar.gz`，46,408,617 字节，SHA-256：

`b303269f4870b497e4c737cbb3fa88332d19f83999a94b4f9bceaf228e345018`

完整归档存放于实验共享目录 `/data/shared_upload/hust-zsx/issue23-contention-20261015`；本目录提供可公开、可独立复核的审查子集。完整资源/worker 日志及控制进程记录保留在该归档。公开清单按仓库 LF 文本及二进制字节计算。

## 结论范围

本轮支持持续链路竞争下集中缓存访问放大端点排队。网络特有的 TTFT 增量受首批高值影响，已有解码还受到 CPU 配额竞争；每组 128 条的 p99 是探索性观察。详细分析保留全部批次与提交时序偏差。后续工作由 Issue #23 继续跟踪。
