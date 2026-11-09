#!/usr/bin/env python3
"""Measure the structure of an indexed MCRIT corpus that determines 1-vs-N cost.

Three distributions decide how a 1-vs-N query scales, and none of them is visible from
sample or function counts alone:

  * band posting-list lengths - a query function's candidate set is the union of the
    posting lists its band hashes land in. If those lengths are Zipfian (a handful of
    library band hashes held by a large fraction of the corpus), candidate volume grows
    linearly with corpus size no matter how good the index is.
  * pichash posting-list lengths - same story for the exact-match path.
  * minhash signature multiplicity - how many functions share one identical signature.
    Identical signatures score identically against any query, so this is the amount of
    pairwise scoring that is provably redundant work.

Usage:
    python benchmarks/analyze_corpus.py --db bench_250 [--json out.json]
"""

import argparse
import json
import os
from collections import Counter

import numpy as np
from pymongo import MongoClient


def percentiles(values, points=(50, 90, 99, 99.9, 100)):
    if not len(values):
        return {}
    array = np.asarray(values, dtype=np.float64)
    return {"p%g" % point: float(np.percentile(array, point)) for point in points}


def describe(name, lengths):
    total = int(sum(lengths))
    report = {
        "count": len(lengths),
        "total_entries": total,
        "mean": float(total / len(lengths)) if lengths else 0.0,
        **percentiles(lengths),
    }
    print(
        "%-26s n=%-9d entries=%-11d mean=%8.2f  p50=%6.0f p90=%8.0f p99=%9.0f p99.9=%9.0f max=%9.0f"
        % (name, report["count"], total, report["mean"], report.get("p50", 0), report.get("p90", 0), report.get("p99", 0), report.get("p99.9", 0), report.get("p100", 0)),
        flush=True,
    )
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", required=True)
    parser.add_argument("--mongo-host", default=os.environ.get("TEST_MONGODB", "127.0.0.1"))
    parser.add_argument("--mongo-port", type=int, default=27017)
    parser.add_argument("--num-bands", type=int, default=20)
    parser.add_argument("--json", default="")
    args = parser.parse_args()

    database = MongoClient(args.mongo_host, args.mongo_port)[args.db]
    report = {"db": args.db}
    report["num_samples"] = database.samples.count_documents({})
    report["num_functions"] = database.functions.count_documents({})
    print("corpus: %d samples, %d functions" % (report["num_samples"], report["num_functions"]), flush=True)

    # --- band posting lists -------------------------------------------------------------
    band_reports = {}
    all_band_lengths = []
    for band_number in range(args.num_bands):
        collection = database["band_%d" % band_number]
        if not collection.estimated_document_count():
            continue
        lengths = [document["n"] for document in collection.aggregate([{"$project": {"n": {"$size": "$function_ids"}}}])]
        all_band_lengths.extend(lengths)
        band_reports["band_%d" % band_number] = {"count": len(lengths), "total_entries": int(sum(lengths)), "max": int(max(lengths)) if lengths else 0}
    report["bands"] = band_reports
    report["band_posting_lists"] = describe("band posting lists (all)", all_band_lengths)

    # --- pichash posting lists ----------------------------------------------------------
    pichash_counts = [
        document["n"]
        for document in database.functions.aggregate(
            [{"$match": {"_pichash": {"$ne": None}}}, {"$group": {"_id": "$_pichash", "n": {"$sum": 1}}}, {"$project": {"n": 1}}], allowDiskUse=True
        )
    ]
    report["pichash_posting_lists"] = describe("pichash posting lists", pichash_counts)

    # --- minhash signature multiplicity -------------------------------------------------
    signature_counter = Counter()
    num_hashed = 0
    for document in database.functions.find({"minhash": {"$ne": ""}}, {"minhash": 1, "_id": 0}):
        minhash = document.get("minhash")
        if not minhash:
            continue
        num_hashed += 1
        # minhashes are stored as binary, or as hex strings before; the identity that matters is
        # the byte sequence, and the hex form is a bijection of it, so count that
        signature_counter[minhash.hex() if isinstance(minhash, bytes) else minhash] += 1
    multiplicities = list(signature_counter.values())
    report["num_hashed_functions"] = num_hashed
    report["num_distinct_signatures"] = len(signature_counter)
    report["signature_multiplicity"] = describe("minhash signature groups", multiplicities)
    if len(signature_counter):
        report["dedup_factor"] = num_hashed / len(signature_counter)
        print(
            "hashed functions %d -> %d distinct signatures (dedup factor %.2fx; %.1f%% of pairwise scoring is provably redundant)"
            % (num_hashed, len(signature_counter), report["dedup_factor"], 100.0 * (1 - len(signature_counter) / max(1, num_hashed))),
            flush=True,
        )

    if args.json:
        with open(args.json, "w") as outfile:
            json.dump(report, outfile, indent=2)
        print("wrote %s" % args.json, flush=True)


if __name__ == "__main__":
    main()
