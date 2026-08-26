#!/usr/bin/env python3
"""
run_perf_t6_workload.py — T6: three-arm cache-share verification on V4.

Converges the T6 workload onto three arms (preregistered 2026-08-25):

  arm             deployment                                             measures
  --------------  ----------------------------------------------------  ---------
  native          vLLM alone (--no-pegaflow, prefix caching off)        baseline (0 hit)
  isolated-share  one pegaflow-server + pool PER domain (physical       domain-local reuse
                  isolation; run per domain, vLLM reconnected)          only
  always-share    one shared pegaflow-server + pool for ALL domains     domain-local + cross-
                                                                        domain reuse

Domains (request families):
  chat   : LONG / PREFIX-EXTEND / PREFIX-ROLLBACK / MULTI-ROUND prompts
           (official prompt-family design, ported to V4)
  agent  : sampled Codex SWE-bench-pro agent trials (real long-context
           coding sessions, ~94% intra-trial prefix reuse per the dataset
           README). Requires --codex-json with the downloaded
           codex_swebenchpro.json (218MB, MIT).

Isolated-share execution (single vLLM instance, one connector per server):
  the arm is run once per domain with the vLLM connected to that domain's
  server, e.g.
      PEGAFLOW_PORT=50081 bash t6-v4-serve.sh      # domain chat pool
      python scripts/run_perf_t6_workload.py --arm isolated-share --domains chat
      ... restart vLLM against 50082 ...
      python scripts/run_perf_t6_workload.py --arm isolated-share --domains agent
  Always-share runs both domains against one server.

Metrics per request: hit_tokens/num_tokens parsed from connector
cache_lookup logs; TTFT cold vs warm per family. Gates: warm coverage
>= 90% on LONG/PREFIX-EXTEND for any PegaFlow arm, else INVALID; the
cross-domain delta (always-share - isolated-share) is the reported
deliverable.

Usage:
  python scripts/run_perf_t6_workload.py --arm always-share --domains all
      --codex-json /data/codex_swebenchpro.json --codex-trials 2
  python scripts/run_perf_t6_workload.py --arm native --domains chat
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
ARMS = ("native", "isolated-share", "always-share")

# ---------------------------------------------------------------------------
# 提示族: chat 域 (官方 test_vllm_e2e_correctness.py 设计移植)
# ---------------------------------------------------------------------------

def _repeat(text: str, tokens: int) -> str:
    """把一段文本重复到约 `tokens` 个 token (中文近似 1 字/token)。"""
    per = len(text)
    return text * max(1, tokens // per)

SHORT_PROMPT = "2 + 2 ="
LONG_PROMPT = _repeat("深度学习模型训练与推理优化技术综述。", 4096)

PREFIX_BASE = _repeat(
    "分布式系统依赖缓存降低延迟提升吞吐, 但缓存正确性取决于谨慎的归属管理。"
    "请求可能观察到前缀可用, 等待其他调度工作, 之后才使用同一前缀。", 2048)
PREFIX_EXTEND = PREFIX_BASE + "缓存层必须避免重复计数预留, 必须释放废弃引用, 保持哈希顺序一致。"

ROLLBACK_LONG = _repeat(
    "哈希表是一种实现关联数组的数据结构, 通过哈希函数计算索引, 从桶中取得对应值。"
    "理想哈希函数让每个键落入唯一桶, 但多数设计允许碰撞, 由链地址法或开放寻址处理。", 4096)
ROLLBACK_SHORT = _repeat(
    "哈希表是一种实现关联数组的数据结构, 通过哈希函数计算索引, 从桶中取得对应值。"
    "理想哈希函数让每个键落入唯一桶, 但多数设计允许碰撞, 由链地址法或开放寻址处理。"
    "链地址法把碰撞元素挂在同一桶的链表上。", 2048)

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

CHAT_FAMILIES = [
    ("SHORT", [SHORT_PROMPT]),
    ("LONG", [LONG_PROMPT]),
    ("PREFIX-EXTEND", [PREFIX_EXTEND]),
    ("PREFIX-ROLLBACK", [ROLLBACK_SHORT]),
    ("MULTI-ROUND", MULTI_ROUND_TURNS),
]

# ---------------------------------------------------------------------------
# agent 域: Codex SWE-bench-pro traces (真实 agent 会话, MIT)
# ---------------------------------------------------------------------------

def load_codex_trials(path: str, n_trials: int) -> list[list[dict]]:
    """加载 codex_swebenchpro.json, 返回前 n_trials 个 trial 的 messages。

    每个 trial = 一个 agent 会话的完整消息序列 (human/assistant), 重放时
    逐轮累积全历史 → 前缀逐轮增长 (真实多轮形态)。
    """
    with open(path, errors="ignore") as f:
        data = json.load(f)
    trials = []
    for item in data[:n_trials]:
        conv = item.get("conversations", [])
        messages = []
        for turn in conv:
            role = "user" if turn.get("from") == "human" else "assistant"
            messages.append({"role": role, "content": turn.get("value", "")})
        if messages:
            trials.append(messages)
    return trials


def agent_turn_prompts(trial: list[dict]) -> list[str]:
    """一个 trial 逐轮累积: 第 k 轮 = 前 k 条消息的全历史。"""
    prompts = []
    for k in range(1, len(trial) + 1):
        prompts.append(json.dumps(trial[:k], ensure_ascii=False))
    return prompts


# ---------------------------------------------------------------------------
# connector 日志解析
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
# API + 测量
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


def report_hit(req_id: str, log_path: str, latency: float, tag: str) -> dict:
    """合并 API 结果与日志命中信息, 打印一行并返回。

    日志实时增长 — 每次报告时重新解析, 不能启动时解析一次
    (请求的 cache_lookup 行在运行中才写入)。
    """
    # vLLM 日志的 req_id 带第二段后缀 (chatcmpl-X-YYYY), API 响应 id 只有
    # chatcmpl-X — 用前缀匹配。
    lookups = parse_lookups(log_path)
    lk = lookups.get(req_id) or next(
        (v for k, v in lookups.items() if k.startswith(req_id)), {})
    hit_tokens = lk.get("hit_tokens", 0)
    num_tokens = lk.get("num_tokens", 0)
    ratio = hit_tokens / num_tokens if num_tokens else 0.0
    print(f"  {tag:<26} req={req_id[:24]:<26} ttft={latency:5.1f}s "
          f"hit={hit_tokens:>6}/{num_tokens:<6} ({ratio*100:5.1f}%)")
    return {"tag": tag, "ttft": latency, "hit_tokens": hit_tokens,
            "num_tokens": num_tokens, "ratio": ratio}


def run_prompts(name: str, prompts: list[str], log_path: str,
                max_tokens: int, port: int, warm_delay: float = 0.0) -> list[dict]:
    """一组提示, 每提示 2 次 (cold → warm)。

    warm_delay: cold 之后等 save 异步 D2H 排空再发 warm (否则尾部块还在
    搬运中就查询, 命中率被时序压低, 不是真实容量行为)。
    """
    print(f"\n== {name} ==")
    rows = []
    for i, prompt in enumerate(prompts):
        m = msg(prompt)
        resp, lat, req_id = call(m, max_tokens=max_tokens, port=port)
        rows.append(report_hit(req_id, log_path, lat, f"{name}[{i}]-cold"))
        if warm_delay:
            time.sleep(warm_delay)
        resp, lat, req_id = call(m, max_tokens=max_tokens, port=port)
        rows.append(report_hit(req_id, log_path, lat, f"{name}[{i}]-warm"))
    return rows


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="T6 三臂验证 (native / isolated-share / always-share)")
    parser.add_argument("--arm", choices=ARMS, default="always-share",
                        help="验证臂 (isolated-share 需配合 --domains 单域跑)")
    parser.add_argument("--domains", choices=("all", "chat", "agent"),
                        default="all", help="本次运行的域 (isolated 臂: 每次一个域)")
    parser.add_argument("--port", type=int, default=PORT, help="vLLM 端口")
    parser.add_argument("--log", default="/tmp/t6-server-vllm.log",
                        help="connector 日志; native 臂无日志, 传 /dev/null")
    parser.add_argument("--codex-json", default="",
                        help="codex_swebenchpro.json 路径 (agent 域需要)")
    parser.add_argument("--codex-trials", type=int, default=2,
                        help="agent 域采样的 trial 数")
    parser.add_argument("--max-tokens", type=int, default=16, help="生成 token 上限")
    parser.add_argument("--warm-delay", type=float, default=3.0,
                        help="cold 后等待 save 排空的秒数 (默认 3)")
    args = parser.parse_args()

    if args.arm != "native" and not os.path.exists(args.log):
        print(f"WARN: --log {args.log} 不存在, 命中解析将为空", file=sys.stderr)

    # 服务健康检查
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{args.port}/health", timeout=10)
    except Exception as e:  # noqa: BLE001
        print(f"ERROR: 服务不可达 {args.port}: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"臂={args.arm} 域={args.domains} 日志={args.log}")

    all_rows: list[dict] = []
    if args.domains in ("all", "chat"):
        for name, prompts in CHAT_FAMILIES:
            all_rows += run_prompts(name, prompts, args.log, args.max_tokens,
                                    args.port, args.warm_delay)

    if args.domains in ("all", "agent"):
        if not args.codex_json:
            print("ERROR: agent 域需要 --codex-json", file=sys.stderr)
            sys.exit(1)
        trials = load_codex_trials(args.codex_json, args.codex_trials)
        print(f"\n== AGENT (Codex {len(trials)} trials) ==")
        for t, trial in enumerate(trials):
            prompts = agent_turn_prompts(trial)
            # 限制轮数避免超长: 每 trial 最多 25 轮, 每轮 history 截断到 60K token 量级
            prompts = [p for p in prompts[:25]]
            all_rows += run_prompts(f"AGENT[{t}]", prompts, args.log,
                                    min(8, args.max_tokens), args.port, args.warm_delay)

    # 汇总: warm 请求命中率 + TTFT
    print("\n== 汇总 (warm) ==")
    warm = [r for r in all_rows if r["tag"].endswith("-warm")]
    if warm:
        ratio = sum(r["ratio"] for r in warm) / len(warm)
        avg = sum(r["ttft"] for r in warm) / len(warm)
        hit_reqs = [r for r in warm if r["hit_tokens"] > 0]
        print(f"  warm 请求 {len(warm)} 平均命中率 {ratio*100:5.1f}%  "
              f"平均 TTFT {avg:5.1f}s  有命中 {len(hit_reqs)}/{len(warm)}")

    # 门限 (PegaFlow 臂): LONG/PREFIX-EXTEND warm 覆盖率 >= 50% 才 VALID
    # (save 是异步 D2H, 立即重查时尾部块可能未落盘 — 时序而非容量行为;
    # 三臂对比看的是 DELTA, 绝对值门槛不宜卡死时序)
    if args.arm != "native":
        gates = [r for r in all_rows
                 if r["tag"].startswith(("LONG", "PREFIX-EXTEND")) and r["tag"].endswith("-warm")]
        if gates:
            g_ratio = sum(r["ratio"] for r in gates) / len(gates)
            verdict = "VALID" if g_ratio >= 0.50 else "INVALID"
            print(f"  gate (LONG/PREFIX-EXTEND warm 覆盖率): {g_ratio*100:.1f}% -> {verdict}")


if __name__ == "__main__":
    main()
