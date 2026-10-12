# Issue #23 attention 传输诊断记录

本目录保存 2026-10-12 四组诊断的固定输入、原始响应、trace、指标、运行身份和分析。实验范围为 `attention_transfer_diagnostic`，`hybrid_prefix_reuse=false`；D 侧仍完整预填充。结果解释见 [report.md](report.md)。

请求文件超过仓库单文件大小门槛，按原始字节 gzip 保存。先在临时结果目录解压 `prepared/smoke.json.gz`，再使用仓库 `scripts/issue23/analyze.py` 重算。解压后的 SHA256 必须等于 contract 的 smoke split SHA256。plan、原始 trace 和逐请求结果已经包含。

`runtime/issue23-pilot.patch` 是本轮基于 `ac1b2d3e943c27e9344a9dba9115ad31f4791d43` 实际运行的完整源码补丁，`runtime/Cargo.lock` 保存依赖锁。它包含连接器和实验两类改动，用于恢复本次运行身份；今后的运行另建结果目录并记录各分支的精确提交。

`runtime-snapshot/` 保存本轮本地保留的服务器编排脚本，包括原始节点地址和控制端点。控制凭据由运行环境提供，文件按白名单归档。后续实验入口需要把部署参数整理为显式配置。

原始压缩证据包 SHA256 为 `89ef821292fbcb7a14d04898f359bb6d7aaecb6c46c3a81c9ed09ed75afe4f16`。本目录展开保存相关证据，`manifest.json` 固化逐文件字节数和 SHA256。当前结果覆盖 8 道 MMLU-Pro 题目的四个实验臂。
