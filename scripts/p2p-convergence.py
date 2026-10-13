#!/usr/bin/env python3
"""Extra of exercise 2: convergence time of anti-entropy as a function of N.

For each network size N, builds a connected network of N peers with a random
topology (local processes on different ports), with one peer set to 1 and the
others to 0, and measures how long the peers take to get close to 1/N.

  ./scripts/p2p-convergence.py                     # default sizes
  ./scripts/p2p-convergence.py 5 10 20 --runs 3    # chosen sizes

The results are appended to a CSV file, one line per simulation, which
scripts/p2p-plot.py turns into a plot.
"""
import argparse
import csv
import os
import random
import re
import subprocess
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_PORT = 20000   # peer pN listens on BASE_PORT + N

# "[12:00:00.000] [p3] sync with localhost:20001: 0.500000 and 0.000000 -> 0.250000"
LINE = re.compile(r"\[[\d:.]+\] \[p(\d+)\] (.*)")
NEW_VALUE = re.compile(r"-> ([\d.]+)$")


def random_topology(n, rng):
    """Returns the edges (a, b), with a > b, of a random connected graph.

    Each peer is first linked to a random peer with a lower number. This gives a
    random tree, which is connected by construction. Then n // 2 random edges
    are added, so that the graph also has cycles.
    """
    edges = set()
    for peer in range(2, n + 1):
        edges.add((peer, rng.randint(1, peer - 1)))
    wanted = min(len(edges) + n // 2, n * (n - 1) // 2)
    while len(edges) < wanted:
        a, b = rng.sample(range(1, n + 1), 2)
        edges.add((max(a, b), min(a, b)))
    return sorted(edges)


def simulate(n, rate, tolerance, timeout, rng):
    """Runs one network of n peers; returns (edges, seconds until convergence).

    The time is None if the network did not converge before the timeout.
    """
    edges = random_topology(n, rng)
    one = rng.randint(1, n)
    values = {peer: 1.0 if peer == one else 0.0 for peer in range(1, n + 1)}

    # All peers write to the same pipe; each line says which peer wrote it.
    read_end, write_end = os.pipe()
    peers = []
    for peer in range(1, n + 1):
        # Peer a registers itself with peer b for each edge (a, b).
        contacts = ["localhost:%d" % (BASE_PORT + b) for a, b in edges if a == peer]
        peers.append(subprocess.Popen(
            ["java", "-Xmx32m", "-XX:+UseSerialGC", "-cp", "build", "ds.assignment.p2p.Peer", *contacts,
             "--port", str(BASE_PORT + peer), "--name", "p%d" % peer,
             "--value", str(values[peer]), "--rate", str(rate)],
            cwd=ROOT, stdout=write_end))
    os.close(write_end)

    def stop():
        for peer in peers:
            peer.kill()

    watchdog = threading.Timer(timeout, stop)
    watchdog.start()
    target = 1.0 / n
    registered = 0
    start = None
    elapsed = None
    try:
        with os.fdopen(read_end) as lines:
            for line in lines:
                match = LINE.match(line)
                if not match:
                    continue
                peer, message = int(match.group(1)), match.group(2)
                if message.startswith("registered with"):
                    # The clock starts when the last edge is in place.
                    registered += 1
                    if registered == len(edges):
                        start = time.monotonic()
                    continue
                new_value = NEW_VALUE.search(message)
                if not new_value:
                    continue
                values[peer] = float(new_value.group(1))
                if start is not None and all(
                        abs(value - target) <= tolerance * target for value in values.values()):
                    elapsed = time.monotonic() - start
                    break
    finally:
        watchdog.cancel()
        stop()
        for peer in peers:
            peer.wait()
    return len(edges), elapsed


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("sizes", nargs="*", type=int, default=[4, 8, 16, 24, 32, 48, 64],
                        help="network sizes to simulate (default: %(default)s)")
    parser.add_argument("--runs", type=int, default=5,
                        help="simulations per size, each with a new topology (default: %(default)s)")
    parser.add_argument("--rate", type=float, default=300,
                        help="synchronizations per minute of each peer (default: %(default)s)")
    parser.add_argument("--tolerance", type=float, default=0.01,
                        help="converged when every peer is within this fraction of 1/N "
                             "(default: %(default)s)")
    parser.add_argument("--timeout", type=float, default=600,
                        help="seconds after which a simulation is abandoned (default: %(default)s)")
    parser.add_argument("--seed", type=int, help="seed for the topologies")
    parser.add_argument("--output", default="results/p2p-convergence.csv",
                        help="CSV file the results are appended to (default: %(default)s)")
    args = parser.parse_args()

    subprocess.run(["./scripts/build.sh"], cwd=ROOT, check=True)
    rng = random.Random(args.seed)
    output = os.path.join(ROOT, args.output)
    os.makedirs(os.path.dirname(output), exist_ok=True)
    new_file = not os.path.exists(output)
    with open(output, "a", newline="") as file:
        results = csv.writer(file)
        if new_file:
            results.writerow(["n", "run", "edges", "rate_per_minute", "tolerance", "seconds"])
        for n in args.sizes:
            for run in range(1, args.runs + 1):
                edges, seconds = simulate(n, args.rate, args.tolerance, args.timeout, rng)
                if seconds is None:
                    print("N=%d run %d: no convergence in %g s" % (n, run, args.timeout))
                    continue
                print("N=%d run %d: %d edges, converged in %.1f s" % (n, run, edges, seconds))
                results.writerow([n, run, edges, args.rate, args.tolerance, "%.3f" % seconds])
                file.flush()


if __name__ == "__main__":
    main()
