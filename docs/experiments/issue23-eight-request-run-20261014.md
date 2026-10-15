# Issue #23：八请求批次的配对缓存传输实验

## 执行配置

本轮使用 81、91 两节点，每节点两张 Ascend 910B2、8 核 CPU 额度与 64 GiB
容器内存。Qwen3.5-35B-A3B 使用 BF16、TP=2、eager/sync、seed=17，
每张 NPU 的 attention cache 预算 4 GiB，每节点 CPU pinned pool 4 GiB。
`max_num_seqs=16` 容纳八个前景请求与四个已有解码请求。OS 调度为默认策略。

源模型 revision 为 `59d61f3ce65a6d9863b86d2e96597125219dc754`。
冻结清单有 924 条 MMLU-Pro 与 100 条 FrontierScience，输入 4097 token、
复用 4096 token，前景输出 1 token；这是恢复机制与新请求 TTFT 的测量。
已有四条 decoder 提供 ITL/TPOT 观察。完整输出条件需独立运行和报告。

清单 SHA-256：
`1c3f91fca6149295c25eabfbb00c8acfcf5b71972f1a26e51703c67faa4fb1fa`。

每组计划 1024 条前景请求、128 个八请求 cohort；四组共 4096 次前景执行。
采用八个时间 block、每 block 十六个配对 cohort。每个 cohort 完整执行
RDMA集中、RDMA平滑、本地拷贝集中、本地拷贝平滑，四种平衡顺序轮换。
每个 arm 建立四条短 prompt、固定 seed 的新背景请求，前景开始前等待
每条背景至少八个 SSE 事件，测量结束后取消、等待设备缓存可安全重置。
全部 cohort 完成时共另启动 2048 条背景请求；源准备与功能资格请求另计。

## 共享传输服务与时序

每条完整混合缓存含全部四个 storage group、两个 TP rank，逻辑载荷
148,275,200 字节；每 cohort 为 1,186,201,600 字节。

- RDMA 采用跨 engine 全部 QP 共享的 1 Gb/s **软件注入预算**。
- 大描述符按不超过 1 MiB 分块，在额度等待中继续处理在途 CQ。
- 平滑在 Tg+0、1.2、2.4、3.6、4.8、6.0、7.2、8.4 秒获得提交资格。
- 集中在 Tg+4.2 秒统一获得资格，平均资格释放偏移相同。
- 两种 schedule 共用 blocking submit 路径，并独立等待、通知每个 bundle。
- 本地拷贝使用相同源内容、字节量和释放时序，网络预算不限制 CPU memcpy。

本实验缩放共享传输服务容量，测量提交排队、真实 RDMA 完成和恢复代价。
软件 pacing 不等同物理低速链路、交换机拥塞或 ECN/PFC 行为。
1 MiB 的初始发送额度会造成约 8 ms 的粒度差异，报告有效 payload 吞吐。
完成时间由实际 CQ 完成后通知，载荷必须与完整清单一致。

## 运行复用与测量边界

模型、engine、MetaServer 与 router 在整轮持续运行。每个 cohort 准备一次
源状态与 D 端隐藏镜像，四个 arm 复用同一份不可变状态；完成后更换 epoch
并回收占用。等待真实传输结束后才允许 epoch/额度变更。

arm 之间先结束背景、清理设备 APC 与可见 CPU cache，再启动相同背景。
背景请求使用设备内的状态，实验 opt-in 配置使其绕过 PegaFlow 外部查询
与保存，避免背景成为冻结清单以外的远端访问者。

每个 arm 的观察窗至少 35 秒，慢请求完成后再清理；这是独立配对 trial。
源准备和四种 arm 的切换在观察窗之外计时。它不是固定 35 秒到达的连续
开放负载，跨 cohort 的实际间隔另行记录。此设计首先检验八请求突发机制。

源服务冻结对象并保持地址有效；更换源快照前检查 serving lease 已释放。
每条测量前景必须出现完整的 eligible/posted/completed 事件，字节精确匹配。
提交跨度保留原 1 ms 标记，偏差样本保留并在统计中单列。

## 遥测与材料

主测量保存客户端 token ID/SSE、门控与完成事件、入队与服务日志、pacing
字节计数、H2D 描述符构建/API/同步时间、实际 model work 以及 1 Hz 主机
资源/Prometheus。npu-smi 放在环境核查中，主采样不开启周期 npu-smi。

Native 日志从 D 本地读取，只接收已以换行结束的 JSONL 行，再写一份
独立观测副本；生产者自己的日志文件持续保留。此方式支持运行中分析，
避免跨节点共享文件的可见性延迟造成缺失事件的误判。

共享运行目录：
`/data/shared_upload/hust-zsx/issue23-scaled-20261014/runs/main-01`。
每个完整配对 cohort 都写 `complete.json`，中断后跳过已完成配对；未完成
cohort 使用新 attempt 目录重跑，旧材料保留。所有进程按拥有的 PID/argv
与 process group 清理。控制凭据不属于归档材料。

定向检查已通过 13 个原生门控测试、共享预算测试、49 个工具测试。运行按
用户指令停止，已完成 28 个完整配对 cohort，每组 224 条、共 896 次前景
执行；另保存 32 次成功执行，其最后一个观察窗被中断。停止时结果记录见
`issue23-eight-request-results-20261014.md`，原定 1024 条/组的预算尚未完成。
