# 混合缓存语义诊断证据

本目录对应[诊断收尾报告](../issue28-cache-semantics-20261013.md)。

- `summary.json`：固定环境、四次模型请求的完整输出 IDs 和真实命中、前缀身份核对摘要。
- `decode-semantic-replay.json`：两个 TP rank 的独立算子、FP64 递推与分块计算比较，
  以及 120 份 conv/SSM 恢复状态的逐位比较。
- `diagnostic-packet.zip`：固定输入、最终诊断脚本和 3160 项装载的完整前缀身份核对表。
- `SHA256SUMS`：本目录文件的 SHA256 清单。
- `verify.py`：使用标准库重新检查文件身份、命中、状态恢复及摘要数值约束。

在仓库根目录运行：

```sh
python docs/analysis/issue28-cache-semantics-20261013/verify.py
```

脚本核对归档事实。模型重新运行需要报告中的 Ascend 环境与模型；zip 中的启动脚本
使用实验共享路径。`audit_prefix_identity.py` 还需要此前跨节点保存日志，路径由
其 `ARCHIVE` 指定。重放张量及 W/U 中间值在实验平台共享存储，公开包不包含这些张量。

严格输出 token 一致性仍为 failed；在所记录配置和用例内未发现前缀、边界或复制
语义违规。数学参考和低精度过程模拟支持报告中的数值归因，未评估任务准确率。
