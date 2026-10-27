# P/D 缓存与服务流程模拟

本组工具在 CPU 上预测四个 P/D 实例的请求、缓存和传输时间线。输入为冻结的请求元数据、初始缓存、到达轨迹及组件标定；输出包括逐请求指标、缓存决策、计算步和传输记录。

三个模型分别回答不同问题：

| 入口                             | 模型范围                                                                    |
| -------------------------------- | --------------------------------------------------------------------------- |
| `pd_serial_simulator.py`         | 原配置串行基线，重新产生 CPU 缓存发布、回收与 D 恢复边界                    |
| `pd_capacity_batch_simulator.py` | 扩大 CPU/NPU 缓存、D 连续批处理，保留历史服务成本及 P 串行                  |
| `pd_serving_simulator.py`        | 图模式、P/D 连续批处理、NPU 前缀复用、异步 CPU 保存/RDMA/H2D 的目标服务流程 |

源代码及模型契约由 PegaFlow 维护。数值输入、历史测量、预测结果和图表保存在
[benchmark 阶段汇总](https://github.com/vLLM-HUST/vllm-hust-benchmark/tree/main/reports/pegaflow-simulation-progress-20261027)。
各报告保留执行源码快照及复现参数。原实验策略运行时保持冻结，通过导入使用其缓存和决策定义。

## 运行

将下列变量替换为对应 benchmark 报告的本地目录；输出路径应为新文件。

```sh
python scripts/issue23/pd_serial_simulator.py \
  --inputs "$SERIAL/inputs" --output /tmp/serial.json.gz

python scripts/issue23/pd_capacity_batch_simulator.py \
  --inputs "$CAPACITY/inputs" \
  --component-profile "$CAPACITY/inputs/component-profile.json.gz" \
  --host-gib 16 --npu-gib 16 --cap 4 --unbatchable 0.25 \
  --output /tmp/capacity.json.gz

python scripts/issue23/pd_serving_simulator.py \
  --inputs "$SERVING/inputs" --step-model "$SERVING/inputs/step-cost-model.json" \
  --components "$SERVING/inputs/components.json" \
  --p-tokens 8192 --d-tokens 2048 --max-sequences 128 \
  --output /tmp/serving.json.gz
```

三个变量分别对应 `pegaflow-pd-serial-validation-20261027`、
`pegaflow-pd-capacity-batching-20261027`、`pegaflow-pd-serving-scenario-20261027`。 Windows PowerShell
使用 `$SERIAL` 等变量，并将 `/tmp/` 改为可写的本地路径。

标准库足以执行预测。图表另需 Matplotlib。`report_pd_serial_validation.py`、 `report_pd_capacity_batching.py` 和
`report_pd_serving.py` 用于整理独立运行目录； `render_pd_serving_percentiles.py --root <serving-report>`
生成百分位曲线、CSV 和分段分析。 归档报告中的原件保持不变，新分析应在独立副本中运行。

```sh
python -m pytest -q scripts/issue23/test_pd_serial_simulator.py \
  scripts/issue23/test_pd_capacity_batch_simulator.py \
  scripts/issue23/test_pd_serving_simulator.py scripts/issue23/test_step_simulator.py
```

## 结果解释

原配置模型用于比较历史实测与重新决策的结果，目标请求的耗时和命中不作为预测标签。 缓存逐块发布时间及物理 owner 分组采用近似，控制开销来自此前记录的聚合。

容量模型保留原服务成本，D 使用共享 token 预算连续批处理；decode 的不可共享比例属于敏感性参数。 它与后续目标服务流程使用不同的成本和交接假设。

目标服务流程采用会话固定 P/D 路由、运行请求优先和等待队列 FCFS；当前未实现 Tailguard 优化调度处理组。 P 发布完成的可恢复块后 D 才恢复，GPU 活动页和 RDMA 源 CPU
块按生命周期保护。 计算成本来自单引擎 graph2048 标定，超过其实际 token/序列支持范围的计算步会计数为外推。 较大的配置上限不等于实际出现该规模的 batch。

CPU 保存/恢复使用独立组件测量，RDMA 使用历史 P→D 服务系数和 FIFO 队列。 完整连接器、CPU 竞争、拷贝与计算重叠及 packet 级拥塞仍需测量。 NPU/CPU
内存预算是参考硬件的估计，实际部署需重新 profiling。 模拟要求未标定抢占时会报错。

这些是离线研究工具和目标架构预测，实际 worker 仍需落实对应异步交接与锁粒度调整。 不同模型间的数值差额不能直接认定为已实现的运行时加速；优化调度收益需在相同服务条件下比较。
