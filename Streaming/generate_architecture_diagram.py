"""Render system_architecture.png: a plain-language data-flow diagram of the
pipeline, for the project_overview.md write-up and non-technical stakeholders.

A one-off documentation asset, not part of the runtime pipeline -- run it by
hand whenever the architecture changes: `python scripts/generate_architecture_diagram.py`.
"""
from __future__ import annotations

import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

from src.streaming_pipeline.config import BASE_DIR

OUTPUT_PATH = BASE_DIR / "system_architecture.png"

_BOX_STYLE = "round,pad=0.4,rounding_size=0.08"
_COLORS = {
    "source": "#cfe8ff",
    "process": "#d9f2d0",
    "store": "#ffe6b3",
    "reject": "#ffd6d6",
    "observe": "#e6d9f7",
}


def _box(ax, xy, width, height, text, color, fontsize=10.5):
    x, y = xy
    box = FancyBboxPatch(
        (x, y), width, height,
        boxstyle=_BOX_STYLE, linewidth=1.2,
        edgecolor="#333333", facecolor=color,
    )
    ax.add_patch(box)
    ax.text(x + width / 2, y + height / 2, text, ha="center", va="center", fontsize=fontsize, wrap=True)
    return {"left": (x, y + height / 2), "right": (x + width, y + height / 2),
            "top": (x + width / 2, y + height), "bottom": (x + width / 2, y)}


def _arrow(ax, start, end, label="", color="#333333", connectionstyle="arc3,rad=0.0", label_offset=(0, 0.15)):
    arrow = FancyArrowPatch(
        start, end, arrowstyle="-|>", mutation_scale=16,
        linewidth=1.4, color=color, shrinkA=2, shrinkB=2,
        connectionstyle=connectionstyle,
    )
    ax.add_patch(arrow)
    if label:
        mid = ((start[0] + end[0]) / 2 + label_offset[0], (start[1] + end[1]) / 2 + label_offset[1])
        ax.text(mid[0], mid[1], label, ha="center", va="bottom", fontsize=9, color=color)


def build_figure():
    fig, ax = plt.subplots(figsize=(13, 8.5))
    ax.set_xlim(0, 13)
    ax.set_ylim(0, 8.5)
    ax.axis("off")
    ax.set_title(
        "How a Customer Click Becomes a Database Record\n"
        "Real-Time E-Commerce Event Pipeline",
        fontsize=15, fontweight="bold", pad=14,
    )

    top_y = 6.3
    generator = _box(ax, (0.3, top_y), 2.3, 1.3, "Event Generator\n(data_generator.py)\nsimulates shopper activity", _COLORS["source"])
    csv_files = _box(ax, (3.0, top_y), 2.1, 1.3, "New CSV file\nevery few seconds\n(data/incoming/)", _COLORS["source"])
    spark_read = _box(ax, (5.5, top_y), 2.3, 1.3, "Spark Structured\nStreaming\nwatches the folder", _COLORS["process"])
    clean = _box(ax, (8.2, top_y), 2.3, 1.3, "Clean & Convert\n(fix types,\ncatch bad data)", _COLORS["process"])

    postgres = _box(ax, (8.2, 4.1), 2.3, 1.3, "PostgreSQL\n\"events\" table\n(good records)", _COLORS["store"])
    rejected = _box(ax, (8.2, 1.9), 2.3, 1.3, "Rejected-rows file\n(data/rejected/)\nbad records, for review", _COLORS["reject"])

    metrics = _box(ax, (0.3, 0.4), 4.4, 1.7,
                    "Performance Tracking\nEvery batch's speed and row counts\nare logged to logs/batch_metrics.csv\nand logs/pipeline.log", _COLORS["observe"])

    _arrow(ax, generator["right"], csv_files["left"], "writes")
    _arrow(ax, csv_files["right"], spark_read["left"], "reads")
    _arrow(ax, spark_read["right"], clean["left"])
    _arrow(ax, clean["bottom"], postgres["top"], "valid rows", color="#1b7a1b")
    _arrow(
        ax, (clean["right"][0], clean["right"][1] - 0.35), (rejected["right"][0], rejected["right"][1] + 0.35),
        "invalid rows", color="#a83232", connectionstyle="arc3,rad=-0.35", label_offset=(0.75, 0),
    )
    _arrow(ax, (spark_read["bottom"][0], spark_read["bottom"][1] - 0.05), (metrics["right"][0] - 0.3, metrics["top"][1]), "progress\nevents", color="#6a3fa0")

    ax.text(
        0.3, 8.05,
        "Read left to right: fake shopper events are generated, streamed through cleaning,\n"
        "and split into a database table (good data) and a review file (bad data) -- with every\n"
        "step's speed recorded for the performance report.",
        fontsize=10, style="italic", color="#444444",
    )

    fig.tight_layout()
    return fig


def main() -> None:
    fig = build_figure()
    fig.savefig(OUTPUT_PATH, dpi=160)
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
