"""Render system_architecture.png: a plain-language data-flow diagram of the
pipeline, for the project_overview.md write-up and non-technical stakeholders.

A one-off documentation asset, not part of the runtime pipeline -- run it by
hand whenever the architecture changes: `python generate_architecture_diagram.py`.
"""
from __future__ import annotations

import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

from src.streaming_pipeline.config import BASE_DIR

OUTPUT_PATH = BASE_DIR / "system_architecture.png"

_BOX_STYLE = "round,pad=0.4,rounding_size=0.12"
_COLORS = {
    "source": "#cfe3fb",
    "process": "#d7f0d2",
    "store": "#fde3b8",
    "reject": "#f9d4d4",
    "observe": "#e5dbf7",
}
_EDGE_COLOR = "#4a4a4a"
_TEXT_COLOR = "#1f1f1f"


def _box(ax, xy, width, height, title, subtitle, color, title_size=11.5, subtitle_size=9.5):
    x, y = xy
    box = FancyBboxPatch(
        (x, y), width, height,
        boxstyle=_BOX_STYLE, linewidth=1.3,
        edgecolor=_EDGE_COLOR, facecolor=color,
    )
    box.set_joinstyle("round")
    box.set_clip_on(False)
    ax.add_patch(box)
    cx = x + width / 2
    ax.text(cx, y + height * 0.62, title, ha="center", va="center",
             fontsize=title_size, fontweight="bold", color=_TEXT_COLOR)
    ax.text(cx, y + height * 0.30, subtitle, ha="center", va="center",
             fontsize=subtitle_size, color=_TEXT_COLOR, linespacing=1.5)
    return {
        "left": (x, y + height / 2), "right": (x + width, y + height / 2),
        "top": (cx, y + height), "bottom": (cx, y),
        "top_right": (x + width, y + height), "bottom_right": (x + width, y),
    }


def _arrow(ax, start, end, label="", color=_EDGE_COLOR, connectionstyle="arc3,rad=0.0",
           label_pos=None, label_size=9.5):
    arrow = FancyArrowPatch(
        start, end, arrowstyle="-|>", mutation_scale=18,
        linewidth=1.6, color=color, shrinkA=3, shrinkB=3,
        connectionstyle=connectionstyle,
    )
    ax.add_patch(arrow)
    if label:
        if label_pos is None:
            label_pos = ((start[0] + end[0]) / 2, (start[1] + end[1]) / 2 + 0.18)
        ax.text(label_pos[0], label_pos[1], label, ha="center", va="bottom",
                 fontsize=label_size, color=color, fontweight="bold")


def build_figure():
    fig = plt.figure(figsize=(14, 9.5))
    ax = fig.add_axes((0.03, 0.03, 0.94, 0.80))
    ax.set_xlim(0, 13.6)
    ax.set_ylim(0, 8.6)
    ax.axis("off")

    fig.text(0.5, 0.965, "How a Customer Click Becomes a Database Record",
              ha="center", fontsize=17, fontweight="bold", color=_TEXT_COLOR)
    fig.text(0.5, 0.925, "Real-Time E-Commerce Event Pipeline -- architecture overview",
              ha="center", fontsize=11.5, color="#555555")
    fig.text(
        0.5, 0.875,
        "Read left to right: fake shopper events are generated, streamed through cleaning, and split into a\n"
        "database table (good data) and a review file (bad data) -- with every batch's speed and row counts recorded.",
        ha="center", va="top", fontsize=10, style="italic", color="#444444",
    )

    top_y = 6.55
    box_h = 1.35

    generator = _box(ax, (0.3, top_y), 2.5, box_h,
                      "Event Generator", "data_generator.py\nsimulates shopper activity",
                      _COLORS["source"])
    csv_files = _box(ax, (3.3, top_y), 2.3, box_h,
                      "New CSV file", "every few seconds\n(data/incoming/)",
                      _COLORS["source"])
    spark_read = _box(ax, (6.0, top_y), 2.5, box_h,
                       "Spark Structured\nStreaming", "watches the folder\nfor new files",
                       _COLORS["process"])
    clean = _box(ax, (9.0, top_y), 2.5, box_h,
                 "Clean & Convert", "fix types,\ncatch bad data",
                 _COLORS["process"])

    postgres = _box(ax, (9.0, 4.35), 2.5, box_h,
                     "PostgreSQL", '"events" table\n(good records)',
                     _COLORS["store"])
    rejected = _box(ax, (9.0, 2.15), 2.5, box_h,
                     "Rejected-rows file", "data/rejected/\nbad records, for review",
                     _COLORS["reject"])

    metrics = _box(ax, (0.3, 0.35), 8.2, 1.35,
                    "Performance Tracking",
                    "Every batch's duration and row counts (received / inserted / rejected)\n"
                    "are logged to logs/batch_metrics.csv, logs/batch_row_counts.csv, and logs/pipeline.log",
                    _COLORS["observe"], subtitle_size=9.5)

    _arrow(ax, generator["right"], csv_files["left"], "writes")
    _arrow(ax, csv_files["right"], spark_read["left"], "reads")
    _arrow(ax, spark_read["right"], clean["left"])
    _arrow(ax, clean["bottom"], postgres["top"], "valid rows", color="#1b7a1b")
    _arrow(
        ax, clean["bottom_right"], (rejected["right"][0], rejected["right"][1] + 0.35),
        "invalid rows", color="#a83232", connectionstyle="arc3,rad=-0.3",
        label_pos=(11.9, 3.35),
    )
    _arrow(
        ax, spark_read["bottom"], (spark_read["bottom"][0], metrics["top"][1]),
        "batch progress\nevents", color="#6a3fa0", label_pos=(7.55, 3.6),
    )

    return fig


def main() -> None:
    fig = build_figure()
    fig.savefig(OUTPUT_PATH, dpi=160)
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
