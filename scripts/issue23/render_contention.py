"""Render measured four-arm results and attribution checks as scientific figures."""

import argparse
import csv
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import font_manager
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(
    "--root",
    type=Path,
    default=Path(__file__).resolve().parents[2] / "results/issue23/20261015-contention",
)
parser.add_argument("--output", type=Path)
parser.add_argument(
    "--font", type=Path, help="Optional CJK font file for Chinese labels"
)
args = parser.parse_args()
ROOT = args.root
DATA = json.loads((ROOT / "pilot-analysis.json").read_text(encoding="utf-8"))
CONTEXT = json.loads((ROOT / "pilot-context-analysis.json").read_text(encoding="utf-8"))
OUT = args.output or ROOT / "figures"
OUT.mkdir(exist_ok=True)

if args.font:
    font_manager.fontManager.addfont(str(args.font))
else:
    for filename in ("msyh.ttc", "msyhbd.ttc"):
        font = Path("C:/Windows/Fonts") / filename
        if font.exists():
            font_manager.fontManager.addfont(str(font))
font_name = (
    font_manager.FontProperties(fname=str(args.font)).get_name()
    if args.font
    else next(
        (
            name
            for name in ("Microsoft YaHei", "Noto Sans CJK SC", "Source Han Sans SC")
            if any(font.name == name for font in font_manager.fontManager.ttflist)
        ),
        "DejaVu Sans",
    )
)
plt.rcParams.update(
    {
        "font.family": font_name,
        "font.size": 11,
        "axes.unicode_minus": False,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.spines.left": False,
        "axes.edgecolor": "#CBD5E1",
        "axes.labelcolor": "#475569",
        "text.color": "#172B45",
        "xtick.color": "#475569",
        "ytick.color": "#475569",
        "axes.titleweight": "bold",
        "axes.titlesize": 13,
        "savefig.facecolor": "white",
        "pdf.fonttype": 42,
    }
)
ARMS = ["rdma-burst", "rdma-smooth", "local_copy-burst", "local_copy-smooth"]
LABELS = ["RDMA\n集中", "RDMA\n平滑", "本地拷贝\n集中", "本地拷贝\n平滑"]
BURST, SMOOTH = "#DF7635", "#347AB6"
COLORS = [BURST, SMOOTH, BURST, SMOOTH]
X = np.arange(4)


def tidy(ax, title, ylabel, upper):
    ax.set_title(title, loc="left", pad=14)
    ax.set_ylabel(ylabel)
    ax.set_ylim(0, upper)
    ax.grid(axis="y", color="#E6EBF0", linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)
    ax.tick_params(axis="both", length=0, pad=7)


def labels(ax, bars, formatter, offset):
    for rect in bars:
        height = rect.get_height()
        ax.text(
            rect.get_x() + rect.get_width() / 2,
            height + offset,
            formatter(height),
            ha="center",
            va="bottom",
            fontsize=11,
            weight="bold",
        )


def title(fig, heading, subtitle):
    fig.text(0.055, 0.969, heading, fontsize=20, weight="bold", va="top")
    fig.text(0.055, 0.925, subtitle, fontsize=10.5, color="#64748B", va="top")


def save(fig, name):
    fig.savefig(OUT / (name + ".png"), dpi=160)
    fig.savefig(OUT / (name + ".pdf"))
    plt.close(fig)


fig, axes = plt.subplots(2, 2, figsize=(14.4, 9.2))
fig.subplots_adjust(
    left=0.075, right=0.97, bottom=0.15, top=0.83, hspace=0.49, wspace=0.24
)
title(
    fig,
    "集中 / 平滑 × RDMA / 本地拷贝：四组结果",
    "2026-10-15  ·  8 个配对批次 × 每批 16 条  ·  每组 128 条  ·  原速 RDMA + 持续饱和 READ 背景",
)
fig.legend(
    handles=[Patch(color=BURST, label="集中"), Patch(color=SMOOTH, label="平滑")],
    loc="upper right",
    bbox_to_anchor=(0.97, 0.965),
    frameon=False,
    ncol=2,
)

for ax, metric, stat, panel, upper, fmt in (
    (
        axes[0, 0],
        "ttft_seconds",
        "mean",
        "A  客户端 TTFT 均值",
        14,
        lambda v: f"{v:.2f}",
    ),
    (axes[0, 1], "ttft_seconds", "p99", "B  客户端 TTFT p99", 22, lambda v: f"{v:.2f}"),
    (
        axes[1, 0],
        "transfer_completion_ms",
        "p99",
        "C  缓存提交至完整完成 p99",
        2300,
        lambda v: f"{v:,.1f}",
    ),
):
    values = [DATA["arms"][arm][metric][stat] for arm in ARMS]
    bars = ax.bar(X, values, width=0.56, color=COLORS, zorder=3)
    tidy(ax, panel, "秒" if metric == "ttft_seconds" else "毫秒", upper)
    ax.set_xticks(X, LABELS)
    labels(ax, bars, fmt, upper * 0.025)

ax = axes[1, 1]
cohorts = DATA["cohort_means"]
batch = np.arange(1, len(cohorts) + 1)
rdma = [row["rdma-burst"] - row["rdma-smooth"] for row in cohorts]
local = [row["local_copy-burst"] - row["local_copy-smooth"] for row in cohorts]
ax.plot(batch, rdma, "o-", color="#172B45", label="RDMA", linewidth=2, markersize=5)
ax.plot(
    batch, local, "s--", color="#8C69A5", label="本地拷贝", linewidth=1.8, markersize=5
)
tidy(ax, "D  每批平均 TTFT：集中减平滑", "差值（秒）", 6)
ax.set_xlabel("配对批次")
ax.set_xticks(batch)
ax.axhline(0, color="#94A3B8", linewidth=0.8)
ax.legend(frameon=False, loc="upper right", ncol=2, fontsize=10)
ax.annotate(
    "首批 RDMA 差距 5.21 秒",
    xy=(1, rdma[0]),
    xytext=(2.3, 4.65),
    arrowprops={"arrowstyle": "-", "color": "#94A3B8"},
    fontsize=10,
)
fig.text(
    0.055,
    0.028,
    "缓存耗时含主机传输队列等待；TTFT 另含 P 端计算、整批等待和模型准入。p99 是 128 条请求的样本分位数。\n"
    "D 图每点代表一个配对批次；全部批次均保留，首批高值的具体成因尚未确定。",
    fontsize=9.5,
    color="#64748B",
    linespacing=1.65,
)
save(fig, "four-arm-results")

fig = plt.figure(figsize=(14.4, 8.2))
grid = fig.add_gridspec(
    2, 2, left=0.075, right=0.97, bottom=0.16, top=0.82, hspace=0.54, wspace=0.27
)
title(
    fig,
    "差异从哪里来：传输排队、CPU 配额与已有解码",
    "同一轮测量  ·  前景软件限速关闭  ·  后台周期报告均值约 89–90 Gb/s  ·  共同事件窗：整批缓存具备传输资格后的 4 秒",
)

ax = fig.add_subplot(grid[0, 0])
q, service = [], []
for arm in ARMS[:2]:
    row = DATA["transport_batch_metrics"][arm]
    q.append(row["queue_seconds"] / row["queue_batches"] * 1000)
    service.append(row["service_seconds"] / row["service_batches"] * 1000)
ax.barh([1, 0], q, color="#B95A44", height=0.43, label="会话队列等待", zorder=3)
ax.barh(
    [1, 0],
    service,
    left=q,
    color="#8299AA",
    height=0.43,
    label="服务（出队至完成）",
    zorder=3,
)
ax.set_yticks([1, 0], ["RDMA 集中", "RDMA 平滑"])
ax.set_xlim(0, 1150)
ax.set_ylim(-0.6, 1.6)
ax.set_title("A  原生 QP batch 的平均耗时", loc="left", pad=14)
ax.set_xlabel("毫秒 / QP batch")
ax.grid(axis="x", color="#E6EBF0", zorder=0)
ax.tick_params(length=0)
ax.text(
    q[0] / 2,
    1,
    f"排队 {q[0]:.1f}",
    va="center",
    ha="center",
    color="white",
    fontsize=10,
)
ax.text(
    q[0] + service[0] / 2,
    1,
    f"{service[0]:.1f}",
    va="center",
    ha="center",
    color="white",
    fontsize=10,
)
ax.text(170, 0, f"排队 {q[1]:.3f}；服务 {service[1]:.1f}", va="center", fontsize=10)
ax.legend(frameon=False, loc="upper right", ncol=2, fontsize=9)

ax = fig.add_subplot(grid[0, 1])
cpu = CONTEXT["cpu_event_window"]
values = [100 * cpu[arm]["throttled_period_fraction"]["mean"] for arm in ARMS]
bars = ax.bar(X, values, width=0.55, color=COLORS, zorder=3)
tidy(ax, "B  CPU 配额周期中发生限流的比例", "% 的配额周期", 65)
ax.set_xticks(X, LABELS)
for index, arm in enumerate(ARMS):
    ys = np.array(cpu[arm]["throttled_period_fraction"]["by_cohort"]) * 100
    ax.scatter(
        index + np.linspace(-0.14, 0.14, len(ys)),
        ys,
        s=15,
        color="#172B45",
        alpha=0.65,
        zorder=4,
    )
    ax.text(
        index,
        max(ys.max(), values[index]) + 3,
        f"{values[index]:.2f}%",
        ha="center",
        weight="bold",
        fontsize=10,
    )

ax = fig.add_subplot(grid[1, 0])
baseline = [DATA["phases"][arm]["sse_baseline_ms"]["p99"] for arm in ARMS]
event = [DATA["phases"][arm]["sse_event_window_ms"]["p99"] for arm in ARMS]
for x, b, e, color in zip(X, baseline, event, COLORS, strict=True):
    ax.plot([x, x], [b, e], color="#CBD5E1", linewidth=2, zorder=2)
    ax.scatter(x, b, color="#8B96A5", marker="o", s=38, zorder=4)
    ax.scatter(x, e, color=color, marker="D", s=44, zorder=4)
    ax.text(x, e + 4, f"{e:.1f}", ha="center", fontsize=10, weight="bold")
    ax.text(x + 0.1, b - 5, f"{b:.1f}", ha="left", fontsize=9, color="#64748B")
tidy(ax, "C  已有解码：客户端 SSE 消息间隔 p99", "毫秒", 135)
ax.set_xlim(-0.5, 3.6)
ax.set_xticks(X, LABELS)
ax.legend(
    handles=[
        Line2D(
            [], [], marker="o", linestyle="", color="#8B96A5", label="前景前 1 秒基线"
        ),
        Line2D([], [], marker="D", linestyle="", color="#172B45", label="共同事件窗"),
    ],
    frameon=False,
    loc="upper left",
    ncol=2,
    fontsize=8.5,
)

ax = fig.add_subplot(grid[1, 1])
interactions = DATA["network_interaction_seconds_by_cohort"]
bars = ax.bar(
    batch,
    interactions,
    width=0.58,
    color=["#A55D7A" if v >= 0 else "#7A98A4" for v in interactions],
    zorder=3,
)
ax.axhline(0, color="#94A3B8", linewidth=1)
ax.set_title("D  RDMA 相对本地的额外 TTFT 差距", loc="left", pad=14)
ax.set_ylabel("秒")
ax.set_xlabel("配对批次")
ax.set_xticks(batch)
ax.set_ylim(-1.9, 5)
ax.grid(axis="y", color="#E6EBF0", zorder=0)
ax.set_axisbelow(True)
ax.tick_params(length=0)
for rect, value in zip(bars, interactions, strict=True):
    ax.text(
        rect.get_x() + rect.get_width() / 2,
        value + (0.12 if value >= 0 else -0.13),
        f"{value:.2f}",
        ha="center",
        va="bottom" if value >= 0 else "top",
        fontsize=9,
    )
ax.text(
    0.97,
    0.92,
    "全 8 批均值 +0.379 秒\n去首批敏感性分析 −0.131 秒",
    transform=ax.transAxes,
    ha="right",
    va="top",
    fontsize=10,
    color="#64748B",
    linespacing=1.7,
)
fig.text(
    0.055,
    0.026,
    "A：QP batch 与逻辑请求的统计对象不同。B：点为各批次；限流周期比例不等于损失的墙钟时间比例。\n"
    "C：SSE 消息可含多个 token，因此不是严格 TPOT。D：(RDMA 集中−平滑)−(本地集中−平滑)，用于检查网络相关增量的稳定性。",
    fontsize=9.2,
    color="#64748B",
    linespacing=1.7,
)
save(fig, "performance-attribution")

with (OUT / "figure-data.csv").open("w", encoding="utf-8-sig", newline="") as stream:
    writer = csv.writer(stream)
    writer.writerow(
        [
            "arm",
            "requests",
            "ttft_mean_s",
            "ttft_p99_s",
            "cache_completion_p99_ms",
            "cpu_throttled_period_pct",
            "sse_baseline_p99_ms",
            "sse_event_p99_ms",
        ]
    )
    for i, arm in enumerate(ARMS):
        metrics = DATA["arms"][arm]
        writer.writerow(
            [
                arm,
                metrics["ttft_seconds"]["n"],
                metrics["ttft_seconds"]["mean"],
                metrics["ttft_seconds"]["p99"],
                metrics["transfer_completion_ms"]["p99"],
                values[i],
                baseline[i],
                event[i],
            ]
        )

print(
    json.dumps(
        {
            "figures": [str(p) for p in sorted(OUT.glob("*"))],
            "cohorts": DATA["complete_cohorts"],
        },
        ensure_ascii=False,
    )
)
