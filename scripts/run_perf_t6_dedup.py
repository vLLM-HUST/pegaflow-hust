#!/usr/bin/env python3
"""
run_perf_t6_dedup.py — T6: MLA logical-KV TP-share dedup verification.

Verifies the SPATIAL dedup claim: under TP8 every rank holds its own copy of
the same logical KV; the PegaFlow pool stores ONE copy per (group, hash).
Saving 8x storage means a pool of size X behaves like 8X of per-rank GPU KV,
which shows up as hit rate (and TTFT) under pool-capacity pressure.

Three measurements per pool-capacity tier:
  1. dedup proof  : pool_used_bytes / cache_resident_bytes growth vs
                    cache_block_insertions — 8 ranks insert, pool keeps one
  2. hit rate     : fixed prefix set (Codex agent trials) replayed cold+warm
                    under pool capacity 16/8/4/2 GB -> eviction curve
  3. TTFT         : warm vs native baseline

Preregistered (perf plan §T6-dedup):
  - prefix set: N Codex trials x max-turns rounds (fixed across tiers)
  - tiers: --pool-sweep 16,8,4,2 (GB)
  - metric: pool_used_bytes delta ~= logical KV bytes (one copy, NOT 8x);
    insertions delta ~= 8x logical blocks; hit rate vs tier
  - gate: at the largest tier, pool_used_bytes delta within +-30% of one
    logical-KV copy (derived from save_bytes), else INVALID

Usage:
  python scripts/run_perf_t6_dedup.py --codex-json /data/codex_swebenchpro.json
      --codex-trials 2 --max-turns 12 --pool-sweep 16,8,4,2
"""

import argparse
import os
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_perf_t6_workload as W  # noqa: E402  (reuse prompts/call/parse)

SERVE = "/workspace/HUST/t6-v4-serve.sh"
METRICS_URL = "http://127.0.0.1:9091/metrics"
METRIC_NAMES = [
    "pegaflow_pool_used_bytes",
    "pegaflow_cache_block_insertions",
    "pegaflow_cache_resident_bytes",
    "pegaflow_cache_block_hits",
    "pegaflow_cache_block_misses",
    "pegaflow_cache_block_evictions",
    "pegaflow_save_bytes",
    "pegaflow_load_bytes",
]
METRIC_RE = re.compile(r"^(pegaflow_[a-z_]+?)(_total)?(\{[^}]*\})?\s+([0-9.eE+-]+)", re.MULTILINE)  # ^ 锚行首, 否则 finditer 只匹配第一行


def fetch_metrics() -> dict[str, float]:
    """从 server HTTP :9091 拉指标, 返回 {metric: 值}。"""
    out: dict[str, float] = {}
    try:
        text = urllib.request.urlopen(METRICS_URL, timeout=10).read().decode()
        for m in METRIC_RE.finditer(text):
            name, _suffix, _labels, value = m.groups()
            if name in METRIC_NAMES:
                out[name] = float(value)
    except Exception as e:  # noqa: BLE001
        print(f"WARN: metrics 拉取失败: {e}", file=sys.stderr)
    return out


def metric_delta(before: dict, after: dict, name: str) -> float:
    return after.get(name, 0.0) - before.get(name, 0.0)


def wait_ports_free(ports, timeout: float = 120.0) -> bool:
    """等端口完全释放 (含 TIME_WAIT — vLLM bind 不设 SO_REUSEADDR,
    刚杀的旧连接 TIME_WAIT ~60s 会挡下一次 bind, 这是 Address already in
    use 的真因)。"""
    import socket
    deadline = time.time() + timeout
    while time.time() < deadline:
        busy = []
        for p in ports:
            with socket.socket() as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    s.bind(("0.0.0.0", p))
                except OSError:
                    busy.append(p)
        if not busy:
            return True
        time.sleep(5)
    print(f"  [FAIL] 端口仍被占用: {busy}", file=sys.stderr)
    return False


def start_stack(pool_gb: int, log_path: str, port: int) -> subprocess.Popen:
    """起 pegaflow-server (pool 容量档) + vLLM (t6-v4-serve.sh)。"""
    if not wait_ports_free([port, 14579, 16666]):
        raise RuntimeError("ports not free")
    env = dict(os.environ)
    env["PEGAFLOW_POOL"] = f"{pool_gb}gb"
    # pegaflow-server 端口必须与 vLLM 端口不同 (8900 是 vLLM API)
    env["PEGAFLOW_PORT"] = env.get("PEGAFLOW_PORT", "50080")
    env["GMU"] = env.get("GMU", "0.85")
    log = open(log_path, "w")
    proc = subprocess.Popen(["bash", SERVE], env=env, stdout=log, stderr=subprocess.STDOUT)
    # 等待 vLLM startup (serve 脚本先探测显存, 需 ~1-2 分钟)
    deadline = time.time() + 900
    while time.time() < deadline:
        if proc.poll() is not None:
            # 共享机残留端口/显存竞态: 等 30s 重试一次
            print("  [RETRY] serve 脚本退出, 30s 后重试", file=sys.stderr)
            time.sleep(30)
            stop_stack()
            log = open(log_path, "w")
            proc = subprocess.Popen(["bash", SERVE], env=env, stdout=log,
                                    stderr=subprocess.STDOUT)
            deadline = time.time() + 900
            continue
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=5)
            return proc
        except Exception:  # noqa: BLE001
            time.sleep(10)
    raise RuntimeError("vLLM startup timeout")


def stop_stack() -> None:
    for pat in ("vllm serve", "VLLM::", "pegaflow-server"):
        subprocess.run(["pkill", "-9", "-f", pat], capture_output=True)
    time.sleep(4)


def run_tier(pool_gb: int, args) -> dict:
    """单个容量档: 起栈 -> 前缀集 cold+warm -> 指标增量。"""
    log_path = f"/tmp/t6-dedup-{pool_gb}gb.log"
    print(f"\n===== 档: 池 {pool_gb} GB =====")
    stop_stack()
    start_stack(pool_gb, log_path, args.port)
    print("  [OK] 栈起来")

    before = fetch_metrics()

    rows = []
    # 前缀集: Codex trials 逐轮增长 (每轮 cold+warm, warm-delay 排空 save)
    trials = W.load_codex_trials(args.codex_json, args.codex_trials)
    for t, trial in enumerate(trials):
        prompts = W.agent_turn_prompts(trial)[: args.max_turns]
        rows += W.run_prompts(
            f"DEDUP-AGENT[{t}]", prompts, log_path,
            min(8, args.max_tokens), args.port, args.warm_delay)
    time.sleep(3)  # 让指标计数落定
    after = fetch_metrics()

    # 汇总本档
    warm = [r for r in rows if r["tag"].endswith("-warm")]
    hit_ratio = sum(r["ratio"] for r in warm) / len(warm) if warm else 0.0
    ttft = sum(r["ttft"] for r in warm) / len(warm) if warm else 0.0
    result = {
        "pool_gb": pool_gb,
        "hit_ratio": hit_ratio,
        "ttft": ttft,
        "warm_reqs": len(warm),
        "used_bytes_delta": metric_delta(before, after, "pegaflow_pool_used_bytes"),
        "insertions_delta": metric_delta(before, after, "pegaflow_cache_block_insertions"),
        "resident_delta": metric_delta(before, after, "pegaflow_cache_resident_bytes"),
        "hits_delta": metric_delta(before, after, "pegaflow_cache_block_hits"),
        "misses_delta": metric_delta(before, after, "pegaflow_cache_block_misses"),
        "evictions_delta": metric_delta(before, after, "pegaflow_cache_block_evictions"),
        "save_bytes_delta": metric_delta(before, after, "pegaflow_save_bytes"),
        "load_bytes_delta": metric_delta(before, after, "pegaflow_load_bytes"),
    }
    print(f"  命中率 {hit_ratio*100:5.1f}%  TTFT {ttft:4.1f}s  "
          f"warm {len(warm)} 池占用 +{result['used_bytes_delta']/1048576:.0f}MB  "
          f"插入 {result['insertions_delta']:.0f} 命中 {result['hits_delta']:.0f} "
          f"驱逐 {result['evictions_delta']:.0f}")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="T6 MLA 逻辑 KV TP 共享去重验证 (池容量 sweep)")
    parser.add_argument("--codex-json", required=True, help="codex_swebenchpro.json 路径")
    parser.add_argument("--codex-trials", type=int, default=2, help="前缀集 trial 数")
    parser.add_argument("--max-turns", type=int, default=12, help="每 trial 轮数上限")
    parser.add_argument("--pool-sweep", default="16,8,4,2",
                        help="池容量档 (GB, 逗号分隔)")
    parser.add_argument("--port", type=int, default=8900, help="vLLM 端口")
    parser.add_argument("--max-tokens", type=int, default=8, help="生成 token 上限")
    parser.add_argument("--warm-delay", type=float, default=3.0,
                        help="cold 后等待 save 排空的秒数")
    args = parser.parse_args()

    tiers = [int(x) for x in args.pool_sweep.split(",")]
    results = []
    try:
        for gb in tiers:
            results.append(run_tier(gb, args))
    finally:
        stop_stack()

    # 汇总表
    print("\n===== T6 dedup 汇总 =====")
    print(f"{'池(GB)':>6} {'命中率':>7} {'TTFT':>5} {'池占用+MB':>9} "
          f"{'插入':>7} {'命中':>6} {'驱逐':>5} {'save+MB':>8} {'load+MB':>8}")
    for r in results:
        print(f"{r['pool_gb']:>6} {r['hit_ratio']*100:6.1f}% {r['ttft']:5.1f} "
              f"{r['used_bytes_delta']/1048576:>9.0f} {r['insertions_delta']:>7.0f} "
              f"{r['hits_delta']:>6.0f} {r['evictions_delta']:>5.0f} "
              f"{r['save_bytes_delta']/1048576:>8.0f} {r['load_bytes_delta']/1048576:>8.0f}")

    # gate: 最大档池占用 ≈ save_bytes (1:1)。save_bytes 是引擎去重过滤后的
    # 实际传输量 = 1 份逻辑 KV (第 2-8 rank 的重复 save 被 filter_new_hashes
    # 挡掉, 不计入)。无去重时 save_bytes 应 ≈ 8x 池占用。
    if results:
        top = results[0]
        saved = top["save_bytes_delta"]
        used = top["used_bytes_delta"]
        if saved > 0:
            ratio = used / saved if saved else 0
            verdict = "VALID" if 0.7 <= ratio <= 1.3 else "INVALID"
            print(f"gate: 池占用 {used/1048576:.0f}MB vs save_bytes {saved/1048576:.0f}MB "
                  f"(去重后 1:1) -> {ratio:.2f} -> {verdict}")
            print(f"dedup 因子: 8 rank 无去重预期 {saved*8/1048576:.0f}MB, "
                  f"实际存储 {used/1048576:.0f}MB = 1/{saved*8/max(used,1):.1f}")


if __name__ == "__main__":
    main()
