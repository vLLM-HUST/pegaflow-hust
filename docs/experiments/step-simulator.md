# 单引擎调度模拟器

四实例 CPU 缓存、D 并发与目标服务流程的后续模型见 [P/D 服务模拟器](pd-serving-simulator.md)。

`scripts/issue23/step_simulator.py` 使用输入长度、输出预算、到达时间和前缀身份预测单引擎的逐步调度、KV 页占用及请求完成时间。计算耗时由独立标定的步成本模型给出。

实现使用 Python 标准库。运行不需要模型、NPU、vLLM 或 PegaFlow 服务。

```sh
python -m pytest -q scripts/issue23/test_step_simulator.py
python scripts/issue23/step_simulator.py --help
```

## 数据与复现

标定测量、预测输入、模拟结果及分析图保存在
[benchmark 实验目录](https://github.com/vLLM-HUST/vllm-hust-benchmark/tree/b5143f94bb1637a26075770db250e351b06e3024/reports/pegaflow-step-simulator-20261026)。
先将该目录取到本地，以下用 `$R` 表示其绝对路径。新分析输出应写到独立目录。

```sh
python scripts/issue23/step_simulator.py fit \
  --steps "$R/data/calibration-steps.jsonl.gz" \
  --metadata "$R/data/calibration-metadata.json" --output /tmp/step-cost.json
python scripts/issue23/step_simulator.py validate \
  --model /tmp/step-cost.json \
  --workload "$R/data/validation-95-shared-workload.json.gz" \
  --observed "$R/data/validation-95-shared-observed.json.gz" \
  --output /tmp/shared-validation.json
python scripts/issue23/verify_step_evidence.py --root "$R"
```

Windows PowerShell 将 `$R` 设置为本地目录，并将 `/tmp/` 替换为可写输出目录。

数值子集从原始日志导出使用 `prepare_step_evidence.py`；图表重建使用 `render_step_evidence.py --root "$R"`（另需
Matplotlib）；派生报告目录的清单使用 `index_step_evidence.py --root "$R"` 生成。原件及采集代码快照保持不变。

## 支持的行为

- 运行请求优先、chunked prefill、共享 token 预算及序列上限。
- KV 按步增长，前缀块引用计数、同一步内共享、空闲块淘汰、私有缓存域。
- 会话前后依赖，前序请求交付完成后才准入后续请求。
- `fcfs` 与 `shortest_remaining` 等待队列策略；后者按输入加输出 token 数排序，不抢占运行请求。
- token 或序列预算超出标定支持时，要求显式 `--allow-extrapolation` 并标记结果。
- 容量不足而需要抢占时明确报错；当前成本模型没有标定抢占成本。

本轮以 Qwen2.5-7B-Instruct BF16、单张 910B2、图模式、2048 token 预算、8 路序列和 16 GiB KV 标定。 32 个标定输入与 95
个验证输入按会话隔离。已查看的两组开发验证中，平均 TTFT 误差约为 −2.2% 和 −2.6%，整体完成时间误差约为 −1.3% 和 −1.8%。

验证覆盖 FCFS；新策略属于待验证预测。模型当前覆盖单引擎计算及 NPU 前缀缓存，不包含跨节点 RDMA、CPU 缓存传输或 P/D 调度。 纯 prefill 标定只有 2
步；改变工作负载和调度混合比例需要补充测量。本轮误差不构成未来预测的精度保证。
