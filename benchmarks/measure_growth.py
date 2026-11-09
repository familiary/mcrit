#!/usr/bin/env python3
"""Measure how a real corpus grows, so synthetic scaling can be honest rather than invented.

Two quantities decide whether a synthetic corpus behaves like a real one at scale:

  * Heaps' law exponent - distinct function signatures V(n) ~ K * n**beta over n samples.
    beta < 1 means new samples keep re-using code already in the corpus, which is what makes
    band posting lists lengthen and candidate sets grow. Guessing it would decide the very
    result the benchmark is supposed to measure, so it is fitted from real data.
  * functions-per-sample distribution - drawn from, rather than averaged, because it is
    heavily skewed and the tail is what produces slow queries.

Both are computed by repeatedly shuffling the real corpus's samples and accumulating.

Usage:
    python benchmarks/measure_growth.py --db bench_250 --json data/growth_250.json
"""

import argparse
import json
import math
import os
import random
from collections import defaultdict

import numpy as np
from pymongo import MongoClient


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", required=True)
    parser.add_argument("--mongo-host", default=os.environ.get("TEST_MONGODB", "127.0.0.1"))
    parser.add_argument("--mongo-port", type=int, default=27017)
    parser.add_argument("--trials", type=int, default=8, help="shuffles to average the growth curve over")
    parser.add_argument("--seed", type=int, default=23)
    parser.add_argument("--json", default="")
    args = parser.parse_args()

    database = MongoClient(args.mongo_host, args.mongo_port)[args.db]
    print("loading signatures per sample ...", flush=True)
    per_sample = defaultdict(list)
    for document in database.functions.find({"minhash": {"$ne": ""}}, {"minhash": 1, "sample_id": 1, "_id": 0}):
        # binary, or hex text in a corpus stored before; one form, so equal signatures compare equal
        minhash = document["minhash"]
        per_sample[document["sample_id"]].append(minhash.hex() if isinstance(minhash, bytes) else minhash)
    sample_ids = sorted(per_sample)
    print("%d samples, %d hashed functions" % (len(sample_ids), sum(len(v) for v in per_sample.values())), flush=True)

    sizes = sorted(len(per_sample[sample_id]) for sample_id in sample_ids)
    functions_per_sample = {
        "min": sizes[0],
        "p25": int(np.percentile(sizes, 25)),
        "p50": int(np.percentile(sizes, 50)),
        "p75": int(np.percentile(sizes, 75)),
        "p90": int(np.percentile(sizes, 90)),
        "p99": int(np.percentile(sizes, 99)),
        "max": sizes[-1],
        "mean": float(np.mean(sizes)),
    }
    print("functions per sample: %s" % json.dumps(functions_per_sample), flush=True)

    rng = random.Random(args.seed)
    curves = []
    for _trial in range(args.trials):
        order = list(sample_ids)
        rng.shuffle(order)
        seen = set()
        curve = []
        for position, sample_id in enumerate(order, start=1):
            seen.update(per_sample[sample_id])
            curve.append((position, len(seen)))
        curves.append(curve)

    # average V(n) across shuffles, then fit log V = log K + beta log n over the upper half,
    # where the curve is no longer dominated by the first few samples
    averaged = []
    for index in range(len(sample_ids)):
        position = curves[0][index][0]
        mean_distinct = float(np.mean([curve[index][1] for curve in curves]))
        averaged.append((position, mean_distinct))
    tail = [(n, v) for n, v in averaged if n >= max(2, len(sample_ids) // 2)]
    log_n = np.log([n for n, _ in tail])
    log_v = np.log([v for _, v in tail])
    beta, log_k = np.polyfit(log_n, log_v, 1)
    report = {
        "db": args.db,
        "num_samples": len(sample_ids),
        "num_hashed_functions": sum(len(v) for v in per_sample.values()),
        "functions_per_sample": functions_per_sample,
        "heaps_beta": float(beta),
        "heaps_K": float(math.exp(log_k)),
        "growth_curve": [(n, v) for n, v in averaged[:: max(1, len(averaged) // 40)]],
    }
    print("Heaps' law fit: V(n) = %.1f * n^%.4f" % (report["heaps_K"], beta), flush=True)
    for target in (1000, 10000, 100000, 1000000):
        print("  extrapolated distinct signatures at %8d samples: %12.0f" % (target, report["heaps_K"] * target**beta), flush=True)
    # what fraction of a *new* sample's functions are already in the corpus, at the observed end
    last_n, last_v = averaged[-1]
    print("at n=%d: %d distinct of %d hashed (%.1f%% re-use)" % (last_n, last_v, report["num_hashed_functions"], 100 * (1 - last_v / report["num_hashed_functions"])), flush=True)

    if args.json:
        with open(args.json, "w") as outfile:
            json.dump(report, outfile, indent=2)
        print("wrote %s" % args.json, flush=True)


if __name__ == "__main__":
    main()
