#!/usr/bin/env python3
"""Plots the results of scripts/p2p-convergence.py (needs matplotlib).

  ./scripts/p2p-plot.py [results.csv [plot.png]]

The simulations run at a higher rate than the 2 synchronizations per minute of
the assignment. Messages between local peers take about a millisecond, so the
time only depends on how often the peers synchronize, and the measured times
are scaled to what they would be at 2 per minute.
"""
import csv
import os
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ASSIGNMENT_RATE = 2.0   # synchronizations per minute

SERIES = "#2a78d6"
TEXT = "#52514e"
GRID = "#e4e3df"


def main():
    source = sys.argv[1] if len(sys.argv) > 1 else os.path.join(ROOT, "results/p2p-convergence.csv")
    target = sys.argv[2] if len(sys.argv) > 2 else os.path.splitext(source)[0] + ".png"

    minutes = defaultdict(list)   # N -> convergence times, in minutes at 2 syncs/minute
    tolerances = set()
    with open(source, newline="") as file:
        for row in csv.DictReader(file):
            scale = float(row["rate_per_minute"]) / ASSIGNMENT_RATE
            minutes[int(row["n"])].append(float(row["seconds"]) * scale / 60)
            tolerances.add(float(row["tolerance"]))

    sizes = sorted(minutes)
    means = [sum(minutes[n]) / len(minutes[n]) for n in sizes]

    figure, axes = plt.subplots(figsize=(7, 4.5))
    for n in sizes:
        axes.scatter([n] * len(minutes[n]), minutes[n], s=22, color=SERIES, alpha=0.35,
                     linewidths=0, label="one simulation" if n == sizes[0] else None)
    axes.plot(sizes, means, color=SERIES, linewidth=2, marker="o", markersize=6,
              label="mean of the simulations")

    tolerance = ", ".join("%g%%" % (100 * t) for t in sorted(tolerances))
    axes.set_title("Anti-entropy: time until every peer is within %s of 1/N" % tolerance,
                   loc="left", fontsize=11)
    axes.set_xlabel("N (number of peers)", color=TEXT)
    axes.set_ylabel("convergence time (minutes, at 2 syncs/minute per peer)", color=TEXT)
    axes.set_xticks(sizes)
    axes.set_ylim(bottom=0)
    axes.grid(axis="y", color=GRID, linewidth=0.8)
    axes.set_axisbelow(True)
    axes.tick_params(colors=TEXT, length=0)
    for side in ("top", "right", "left"):
        axes.spines[side].set_visible(False)
    axes.spines["bottom"].set_color(GRID)
    axes.legend(frameon=False, loc="upper left", labelcolor=TEXT)

    figure.tight_layout()
    figure.savefig(target, dpi=150)
    print("plot written to " + target)


if __name__ == "__main__":
    main()
