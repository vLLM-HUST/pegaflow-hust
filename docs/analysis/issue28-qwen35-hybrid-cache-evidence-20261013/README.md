# Qwen3.5 TP=2 验收证据

本目录从 2026-10-13 冻结运行记录提取固定输入、95 条完整 32-token 输出，
以及 3160 条装载 checksum 的来源分类。总体验收状态为 **failed**。
实现、环境及完整判定见[验收报告](../issue28-qwen35-hybrid-cache-acceptance-20261013.md)。

| 文件 | 内容 |
| --- | --- |
| [cases.json](cases.json) | 九组实际输入 token IDs；由 FrontierScience 单题重复、截断并构造边界分叉 |
| [outputs.json](outputs.json) | 基线两次重复、独立恢复、修复后两种生产顺序、补充及本地 APC 对照的输出 IDs |
| [publications.json](publications.json) | 用索引表压缩的逐装载 rank/layer/hash/checksum 与匹配保存请求；字段顺序见 `columns` |
| [manifest.json](manifest.json) | 公开文件及提取来源 SHA256、冻结归档 SHA256 |
| [verify.py](verify.py) | 使用 Python 标准库重新计算输出比较、来源分类总数并检查文件身份 |

在仓库根目录运行：

```sh
python docs/analysis/issue28-qwen35-hybrid-cache-evidence-20261013/verify.py
```

默认退出 0 表示证据内部核对成功。输出会单独显示 `model_output_gate: failed`。
加入 `--require-model-correctness` 时，本轮证据会退出 1。
该命令分析已归档输出；重新运行模型仍需要[实现报告](../issue28-qwen35-hybrid-cache-implementation-20261013.md)
中的固定模型、依赖、运行配置和两节点 RDMA 环境。

原顺序为九组输入在 `cases.json` 中的顺序，源端每次清空 APC、保留 engine 缓存；
目标端先运行 `boundary-4097`，然后运行其他八组，各次清空 APC、保留 engine 缓存。
反向轮次使用新的共同 salt，源端顺序反转，目标端按 `cases.json` 顺序执行。
独立轮次每组使用不同 salt，两端该组相同，避免不同生产历史共享 key。
`outputs.json` 保留实际请求顺序及独立／对照的 salt；完整响应和启动记录在冻结归档中。

本地 APC 对照通过诊断包装器恢复本地 HMA 索引，在源端 `save_only` 下执行。
`first_forward` 为探针记录的已计算／本步计算 token 数，两个关键失败对照的
本地输出与跨节点输出全部 32 IDs 相同。当前还不能据此判定具体内核错误或数值差异。

checksum 分类可在本目录重新计数：2738 项匹配源端，422 项仅匹配目标端先前保存。
重新核对张量字节、日志时序和保存 ACK 需要完整 save/load/forward 日志。
完整归档保存在实验共享存储，访问需要该平台权限；归档哈希用于核对取到的文件身份。
公开提取包不包含完整模型、原始张量或全部运行日志。

输入用于边界和缓存正确性诊断，没有产生 FrontierScience 准确率或性能结论。
