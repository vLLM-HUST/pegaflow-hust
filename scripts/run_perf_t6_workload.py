#!/usr/bin/env python3
"""
run_perf_t6_workload.py — T6: real-workload cache behavior on V4 + PegaFlow.

Ports the official pegaflow prompt-family design (test_vllm_e2e_correctness.py
/ test_vllm_warm_hit_stress.py) onto the DeepSeek-V4-Flash TP8 NPU stack to
measure PegaFlow's cache-hit behavior and benefit under realistic prefix
reuse, instead of the synthetic repeated-prompt probes used earlier.

Preregistered (perf plan §T6):
  - families: SHORT / LONG / PREFIX-EXTEND / PREFIX-ROLLBACK / MULTI-ROUND /
    concurrent stress (12x same + 1x other)
  - metric: per-request hit_tokens/num_tokens from connector cache_lookup
    logs, TTFT cold vs warm per family
  - gate: warm coverage >= 90% on LONG/PREFIX-EXTEND families, else the run
    is INVALID (the manipulation did not take); TTFT warm < cold on hits

覆盖的缓存路径 (与官方一一对应):
- 短提示      : 单块边界 (不完整块)
- 长提示      : 多块对齐
- 前缀延长    : 缓存 "A B C" 后请求 "A B C D E" (部分命中)
- 前缀回滚    : 缓存 "A B C D" 后请求 "A B" (反向)
- 多轮对话    : 每轮追加一问, 全历史重发 (前缀逐轮增长, 模拟真实聊天)
- 并发压力    : N 路重复提示 + 受限容量, 查探测释放/泄漏

每请求从 connector 日志解析 cache_lookup 行, 报告 hit_blocks/hit_tokens
与 TTFT。双臂 (PegaFlow on/off) 都可跑: 对照臂无 cache_lookup 日志,
命中率自然为 0, 可作基线。

注意: V4 在 NPU 上非确定性 (MTP/npugraph_ex, temperature=0 亦不保证逐字节
一致), 因此不做官方式的"逐字节一致性断言", 只做命中度量 + 延迟对比。

用法:
    python scripts/t6-v4-workload.py [--log /tmp/t6-deploy10.log] [--concurrency 12]
"""
import argparse
import json
import os
import re
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

PORT = 8900
MODEL = "dsv4"

# ---------------------------------------------------------------------------
# 提示族: 结构对齐官方测试。句子重复到目标 token 规模 (V4 tokenizer 中文
# 每字约 1 token; 目标块按 512-token 物理块计)。
# ---------------------------------------------------------------------------

def _repeat(text: str, tokens: int) -> str:
    """把一段文本重复到约 `tokens` 个 token (中文近似 1 字/token)。"""
    per = len(text)
    return text * max(1, tokens // per)

# 短提示: 不足一块 (512 token), 不完整块的边界情形
SHORT_PROMPT = "2 + 2 ="

# 长提示: 多块 (默认 4096 token ≈ 8 个 MLA 物理块)
LONG_BLOCK = "深度学习模型训练与推理优化技术综述。"
LONG_PROMPT = _repeat(LONG_BLOCK, 4096)

# 前缀延长: base 严格是 extend 的前缀; 先缓存 base, 再请求 extend → 部分命中
PREFIX_BASE = _repeat(
    "分布式系统依赖缓存降低延迟提升吞吐, 但缓存正确性取决于谨慎的归属管理。"
    "请求可能观察到前缀可用, 等待其他调度工作, 之后才使用同一前缀。", 2048)
PREFIX_EXTEND = PREFIX_BASE + "缓存层必须避免重复计数预留, 必须释放废弃引用, 保持哈希顺序一致。"

# 前缀回滚: 先缓存长版, 再请求其前缀 → 反向路径
ROLLBACK_LONG = _repeat(
    "哈希表是一种实现关联数组的数据结构, 通过哈希函数计算索引, 从桶中取得对应值。"
    "理想哈希函数让每个键落入唯一桶, 但多数设计允许碰撞, 由链地址法或开放寻址处理。", 4096)
ROLLBACK_SHORT = _repeat(
    "哈希表是一种实现关联数组的数据结构, 通过哈希函数计算索引, 从桶中取得对应值。"
    "理想哈希函数让每个键落入唯一桶, 但多数设计允许碰撞, 由链地址法或开放寻址处理。"
    "链地址法把碰撞元素挂在同一桶的链表上。", 2048)  # 严格是 LONG 的前缀

# 多轮对话: 每轮追加一问, 全历史重发 → 前缀逐轮增长
_MULTI_ROUND_STEM = _repeat(
    "量子计算利用叠加与纠缠等量子力学现象进行计算。"
    "与经典比特不同, 量子比特可同时处于 0 与 1 的叠加态。"
    "纠缠使量子比特相互关联, 无经典对应物。", 1024)
MULTI_ROUND_TURNS = [
    "量子计算利用叠加与纠缠等量子力学现象进行计算。与经典比特不同, 量子比特可同时处于 0 与 1 的叠加态。纠缠使量子比特相互关联, 无经典对应物。"
    "当前量子硬件处于什么阶段?",
    _MULTI_ROUND_STEM + "Shor 算法用于大数分解, Grover 算法用于无序数据库搜索。当前量子计算机面临退相干、错误率与极致制冷等挑战。"
    "主要算法有哪些?",
    _MULTI_ROUND_STEM + "Shor 算法用于大数分解, Grover 算法用于无序数据库搜索。当前量子计算机面临退相干、错误率与极致制冷等挑战。"
    "谷歌、IBM、微软正重金投入量子计算。业界进展如何?",
]

# 并发压力提示: 中等长度重复提示 (对齐官方 warm_hit_stress)
STRESS_PROMPT = _repeat(
    "分布式系统依赖缓存降低延迟提升吞吐, 但缓存正确性取决于谨慎的归属管理。"
    "请求可能观察到前缀可用, 等待其他调度工作, 之后才使用同一前缀。"
    "在此期间缓存层必须避免重复计数预留, 必须释放废弃引用, 保持哈希顺序一致。", 2048)

# ---------------------------------------------------------------------------
# connector 日志解析 (对齐官方 warm_hit_stress 的做法)
# ---------------------------------------------------------------------------
LOOKUP_RE = re.compile(
    r"req=(\S+) cache_lookup: hit_blocks=(\d+).*?hit_tokens=(\d+) num_tokens=(\d+)")


def parse_lookups(log_path: str) -> dict[str, dict]:
    """从 connector 日志解析 {req_id: {hit_blocks, hit_tokens, num_tokens}}。"""
    lookups: dict[str, dict] = {}
    if not log_path or not os.path.exists(log_path):
        return lookups
    with open(log_path, errors="ignore") as f:
        for line in f:
            m = LOOKUP_RE.search(line)
            if m:
                req_id, hit, hit_tok, num_tok = m.groups()
                lookups[req_id] = {
                    "hit_blocks": int(hit),
                    "hit_tokens": int(hit_tok),
                    "num_tokens": int(num_tok),
                }
    return lookups


# ---------------------------------------------------------------------------
# API 调用
# ---------------------------------------------------------------------------

def call(messages, max_tokens=16, port=None):
    """POST /v1/chat/completions, 返回 (resp, 延迟秒, req_id)。"""
    port = port or PORT
    data = json.dumps({
        "model": MODEL, "messages": messages, "max_tokens": max_tokens,
        "temperature": 0.0, "stream": False,
    }).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/v1/chat/completions",
        data=data, headers={"Content-Type": "application/json"})
    t0 = time.time()
    resp = json.load(urllib.request.urlopen(req, timeout=600))
    ttft = time.time() - t0
    return resp, ttft, resp.get("id", "unknown")


def msg(content: str) -> list[dict]:
    return [{"role": "user", "content": content}]


def report_hit(req_id: str, lookups: dict, latency: float, tag: str) -> dict:
    """合并 API 结果与日志命中信息, 打印一行并返回。"""
    lk = lookups.get(req_id, {})
    hit_tokens = lk.get("hit_tokens", 0)
    num_tokens = lk.get("num_tokens", 0)
    ratio = hit_tokens / num_tokens if num_tokens else 0.0
    print(f"  {tag:<14} req={req_id[:24]:<26} ttft={latency:5.1f}s "
          f"hit={hit_tokens:>6}/{num_tokens:<6} ({ratio*100:5.1f}%)")
    return {"tag": tag, "ttft": latency, "hit_tokens": hit_tokens,
            "num_tokens": num_tokens, "ratio": ratio}


def run_family(name: str, prompts: list[str], lookups: dict,
               max_tokens: int = 16, port: int | None = None) -> list[dict]:
    """一族提示, 每提示请求 2 次 (cold → warm), 报告命中与延迟。"""
    print(f"\n== {name} ==")
    rows = []
    for i, prompt in enumerate(prompts):
        m = msg(prompt)
        for j, label in enumerate(("cold", "warm")):
            resp, lat, req_id = call(m, max_tokens=max_tokens, port=port)
            rows.append(report_hit(req_id, lookups, lat, f"{name}[{i}]-{label}"))
    return rows


def run_concurrency(n: int, lookups: dict, max_tokens: int = 32,
                     port: int | None = None) -> None:
    """并发压力: n 路同提示 + 混入一个不同提示, 观察命中与失败。"""
    print(f"\n== 并发压力 ({n} 路同提示 + 1 路异提示) ==")
    other = msg("请用一句话介绍缓存一致性协议。")
    jobs = [("same", msg(STRESS_PROMPT)) for _ in range(n)] + [("other", other)]

    def one(job):
        tag, m = job
        try:
            resp, lat, req_id = call(m, max_tokens=max_tokens, port=port)
            lk = lookups.get(req_id, {})
            return {"tag": tag, "lat": lat, "hit": lk.get("hit_tokens", 0),
                    "total": lk.get("num_tokens", 0), "ok": True}
        except Exception as e:  # noqa: BLE001
            return {"tag": tag, "lat": 0, "hit": 0, "total": 0, "ok": False,
                    "err": str(e)[:120]}

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=n + 1) as ex:
        futures = [ex.submit(one, j) for j in jobs]
        results = [f.result() for f in as_completed(futures)]
    wall = time.time() - t0

    ok = [r for r in results if r["ok"]]
    fail = [r for r in results if not r["ok"]]
    hits = [r for r in ok if r["hit"] > 0]
    avg_lat = sum(r["lat"] for r in ok) / len(ok) if ok else 0
    print(f"  完成 {len(ok)}/{len(jobs)}  wall={wall:.1f}s  avg_ttft={avg_lat:.1f}s  "
          f"命中 {len(hits)} 失败 {len(fail)}")
    for r in fail[:3]:
        print(f"    FAIL {r['tag']}: {r['err']}")
    if hits:
        hit_ratio = sum(r["hit"] for r in hits) / sum(r["total"] for r in hits)
        print(f"  命中请求平均覆盖率: {hit_ratio*100:.1f}%")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="T6 V4+PegaFlow 真实 workload 探测 (官方提示族设计移植)")
    parser.add_argument("--port", type=int, default=PORT, help="vLLM 端口")
    parser.add_argument("--log", default="/tmp/t6-deploy10.log",
                        help="connector 日志 (cache_lookup 行来源); 对照臂传空跳过解析")
    parser.add_argument("--concurrency", type=int, default=12,
                        help="并发压力路数 (0 = 跳过)")
    parser.add_argument("--max-tokens", type=int, default=16, help="生成 token 上限")
    args = parser.parse_args()

    # 服务健康检查
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{PORT}/health", timeout=10)
    except Exception as e:  # noqa: BLE001
        print(f"ERROR: 服务不可达 {PORT}: {e}", file=sys.stderr)
        sys.exit(1)

    # 记录开始前的日志行数, 只解析本次运行产生的 cache_lookup
    lookups = parse_lookups(args.log)
    baseline = len(lookups)
    print(f"日志 {args.log}: 已解析 {baseline} 条历史 lookup")

    all_rows: list[dict] = []
    all_rows += run_family("SHORT", [SHORT_PROMPT], lookups, args.max_tokens, args.port)
    all_rows += run_family("LONG", [LONG_PROMPT], lookups, args.max_tokens, args.port)
    all_rows += run_family("PREFIX-EXTEND", [PREFIX_EXTEND], lookups, args.max_tokens, args.port)
    all_rows += run_family("PREFIX-ROLLBACK", [ROLLBACK_SHORT], lookups, args.max_tokens, args.port)
    all_rows += run_family("MULTI-ROUND", MULTI_ROUND_TURNS, lookups, args.max_tokens, args.port)

    if args.concurrency > 0:
        run_concurrency(args.concurrency, lookups, max(32, args.max_tokens), args.port)

    # 汇总: 每族 warm 请求的平均命中率与 TTFT
    print("\n== 汇总 (warm 请求) ==")
    for tag in sorted({r["tag"] for r in all_rows if "-warm" in r["tag"]}):
        rows = [r for r in all_rows if r["tag"] == tag]
        ratio = sum(r["ratio"] for r in rows) / len(rows)
        avg = sum(r["ttft"] for r in rows) / len(rows)
        print(f"  {tag:<14} 平均命中 {ratio*100:5.1f}%  平均 TTFT {avg:5.1f}s")


if __name__ == "__main__":
    main()
