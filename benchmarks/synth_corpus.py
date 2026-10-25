#!/usr/bin/env python3
"""Grow an indexed MCRIT corpus to a target size with statistically faithful synthetic samples.

Malpedia is ~10k samples; the question is what happens at a million. Disassembling a million
samples is not possible, and replicating the same sample a million times would be worse than
useless - it would produce one enormous equivalence class and flatter *or* wreck the numbers
for reasons that have nothing to do with real corpora.

So synthetic samples are drawn from a process fitted to the real corpus:

  * functions per sample is drawn from the real empirical distribution (heavily skewed:
    measured p50=373, mean=750, max=9745 - the tail is what produces slow queries).
  * function signatures follow a Heaps-calibrated preferential-attachment urn seeded with the
    *real* signature population and its real frequencies. A signature already common in the
    corpus is likely to recur (this is what gives library code its long posting lists), and
    new signatures keep appearing at a rate set by the discount parameter d.
  * d is set to the Heaps' law exponent measured on the real corpus by
    benchmarks/measure_growth.py (V(n) ~ K * n**beta, measured beta=0.72), because for a
    Pitman-Yor urn the vocabulary grows as tokens**d. Matching the exponent is what makes
    the synthetic corpus's *skew* - the thing that drives candidate volume - grow the way a
    real one does, instead of the way an invented one would.

Band hashes come from the real storage object, so the index written here is the same index
MCRIT builds for itself and can be queried by the unmodified matcher.

Usage:
    python benchmarks/synth_corpus.py --source-db bench_250 --target-db scale_10k \
        --target-samples 10000 [--checkpoints 1000 2500 5000 10000] [--seed 23]
"""

import argparse
import json
import os
import random
import time

import numpy as np
from pymongo import InsertOne, MongoClient, UpdateOne

SYNTHETIC_FAMILY_ID = 90001


def load_population(database):
    """(signature hex strings, their frequencies, functions-per-sample sizes) from a real corpus."""
    frequencies = {}
    sizes = {}
    for document in database.functions.find({"minhash": {"$ne": ""}}, {"minhash": 1, "sample_id": 1, "_id": 0}):
        # binary, or hex text in a corpus stored before; this script works on the hex form
        minhash = document["minhash"]
        signature_hex = minhash.hex() if isinstance(minhash, bytes) else minhash
        frequencies[signature_hex] = frequencies.get(signature_hex, 0) + 1
        sizes[document["sample_id"]] = sizes.get(document["sample_id"], 0) + 1
    signatures = list(frequencies)
    counts = np.array([frequencies[signature] for signature in signatures], dtype=np.int64)
    return signatures, counts, np.array(sorted(sizes.values()), dtype=np.int64)


class HeapsUrn:
    """Signature generator that reproduces a measured corpus's growth and skew by construction.

    Two properties have to hold for a synthetic corpus to stress the index the way a real one
    does, and both are taken from measurement rather than chosen:

      * Heaps' law - the distinct-signature count must grow as V(t) = K * t**beta in tokens
        (measured beta = 0.72 on Malpedia). A new signature is minted at token t with
        probability dV/dt = K * beta * t**(beta-1), which *is* the law, differentiated. This
        is exact by construction, so the synthetic corpus cannot drift from the real growth
        rate the way a tuned-parameter urn can.
      * Zipf skew - an existing signature is drawn with probability proportional to how often
        it already occurs (preferential attachment). This is what gives library code the very
        long posting lists that dominate candidate volume, and it is seeded with the *real*
        population so the head of the distribution is real code, not an invention.

    Preferential attachment is implemented with a token array (each signature appears once per
    occurrence, drawn uniformly), which makes a draw O(1) and an update an append.
    """

    def __init__(self, signatures, counts, beta, rng, signature_bytes=64, seed_tokens=None):
        self._signatures = list(signatures)
        self._rng = rng
        self._numpy_rng = np.random.default_rng(rng.randrange(2**32))
        self._signature_bytes = signature_bytes
        self._beta = beta
        # token array: signature index repeated once per occurrence
        total_tokens = int(counts.sum())
        self._tokens = np.repeat(np.arange(len(signatures), dtype=np.int64), counts)
        self._num_tokens = total_tokens
        # calibrate K so the law passes exactly through the observed (tokens, distinct) point
        self._heaps_k = len(signatures) / (total_tokens**beta) if total_tokens else 1.0
        self._pending = []

    def _flush_pending(self):
        if self._pending:
            self._tokens = np.concatenate([self._tokens, np.array(self._pending, dtype=np.int64)])
            self._pending = []

    def draw(self):
        self._num_tokens += 1
        # probability that token t is a signature never seen before: dV/dt of V = K t**beta
        novel_probability = self._heaps_k * self._beta * (self._num_tokens ** (self._beta - 1.0))
        if self._rng.random() < novel_probability:
            signature = self._numpy_rng.integers(0, 256, size=self._signature_bytes, dtype=np.uint8).tobytes().hex()
            index = len(self._signatures)
            self._signatures.append(signature)
        else:
            if len(self._pending) > 200000:
                self._flush_pending()
            total = len(self._tokens) + len(self._pending)
            position = self._rng.randrange(total)
            index = int(self._tokens[position]) if position < len(self._tokens) else self._pending[position - len(self._tokens)]
            signature = self._signatures[index]
        self._pending.append(index)
        return signature

    @property
    def num_distinct(self):
        return len(self._signatures)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source-db", required=True, help="real corpus to fit the generator to")
    parser.add_argument("--target-db", required=True, help="database to grow (created if absent)")
    parser.add_argument("--target-samples", type=int, required=True)
    parser.add_argument("--heaps-beta", type=float, default=0.7247, help="measured Heaps exponent (benchmarks/measure_growth.py)")
    parser.add_argument("--chunk-samples", type=int, default=1000, help="samples per flush to mongo")
    parser.add_argument("--mongo-host", default=os.environ.get("TEST_MONGODB", "127.0.0.1"))
    parser.add_argument("--mongo-port", type=int, default=27017)
    parser.add_argument("--seed", type=int, default=23)
    parser.add_argument("--json", default="")
    args = parser.parse_args()

    client = MongoClient(args.mongo_host, args.mongo_port)
    source = client[args.source_db]
    target = client[args.target_db]

    import sys

    from mcrit.minhash.MinHash import MinHash

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from bench_matching import make_config  # reuse the one config builder

    config = make_config(args.target_db, args.mongo_host, str(args.mongo_port))
    from mcrit.storage.MongoDbStorage import MongoDbStorage

    storage = MongoDbStorage(config)
    signature_bits = config.MINHASH_CONFIG.MINHASH_SIGNATURE_BITS
    signature_length = config.MINHASH_CONFIG.MINHASH_SIGNATURE_LENGTH

    print("loading real population from '%s' ..." % args.source_db, flush=True)
    signatures, counts, sample_sizes = load_population(source)
    print("population: %d distinct signatures, %d function instances, %d samples" % (len(signatures), int(counts.sum()), len(sample_sizes)), flush=True)

    rng = random.Random(args.seed)
    urn = HeapsUrn(signatures, counts, args.heaps_beta, rng, signature_bytes=signature_length)

    existing_samples = target.samples.count_documents({})
    next_sample_id = (target.samples.find_one(sort=[("sample_id", -1)]) or {}).get("sample_id", -1) + 1
    next_function_id = (target.functions.find_one(sort=[("function_id", -1)]) or {}).get("function_id", -1) + 1
    to_generate = args.target_samples - existing_samples
    print("target db holds %d samples; generating %d more (ids from %d)" % (existing_samples, to_generate, next_sample_id), flush=True)
    if to_generate <= 0:
        return

    band_hash_cache = {}

    def band_hashes_for(signature_hex):
        """Band hashes for a signature, memoised - signatures repeat heavily by construction."""
        cached = band_hash_cache.get(signature_hex)
        if cached is None:
            minhash = MinHash(minhash_bytes=bytes.fromhex(signature_hex), minhash_bits=signature_bits)
            cached = sorted(storage.getBandHashesForMinHash(minhash).items())
            band_hash_cache[signature_hex] = cached
        return cached

    started = time.time()
    generated = 0
    while generated < to_generate:
        chunk = min(args.chunk_samples, to_generate - generated)
        function_documents = []
        sample_documents = []
        band_postings = {}  # (band_number, band_hash) -> [function_id]
        for _ in range(chunk):
            sample_id = next_sample_id
            next_sample_id += 1
            num_functions = int(sample_sizes[rng.randrange(len(sample_sizes))])
            for _function_index in range(num_functions):
                signature = urn.draw()
                function_id = next_function_id
                next_function_id += 1
                function_documents.append(
                    InsertOne(
                        {
                            "function_id": function_id,
                            "sample_id": sample_id,
                            "family_id": SYNTHETIC_FAMILY_ID,
                            "minhash": signature,
                            # a pichash derived from the signature keeps the exact-match path
                            # populated and correlated with the fuzzy one, as in real data
                            "_pichash": int(signature[:12], 16),
                            "num_instructions": 32,
                            "num_blocks": 4,
                            "offset": 0x400000 + 16 * function_id,
                            "function_name": "synthetic_%d" % function_id,
                            "function_labels": [],
                            "binweight": 128,
                            "architecture": "intel.32bit",
                            "minhash_shingle_composition": {},
                            "matches": [],
                        }
                    )
                )
                for band_number, band_hash in band_hashes_for(signature):
                    band_postings.setdefault((band_number, band_hash), []).append(function_id)
            sample_documents.append(
                InsertOne(
                    {
                        "sample_id": sample_id,
                        "family_id": SYNTHETIC_FAMILY_ID,
                        "family": "synthetic",
                        "version": "",
                        "component": "",
                        "is_library": False,
                        "filename": "synthetic_%d" % sample_id,
                        "sha256": "%064x" % rng.getrandbits(256),
                        "architecture": "intel",
                        "bitness": 32,
                        "base_addr": 0x400000,
                        "binary_size": 128 * num_functions,
                        "binweight": 128 * num_functions,
                        "smda_version": "4.6.0",
                        # SampleEntry.fromDict parses this with "%Y-%m-%dT%H-%M-%S" - dashes in
                        # the time part, not colons; an ISO-8601 string raises here
                        "timestamp": "2026-01-01T00-00-00",
                        "statistics": {
                            "num_functions": num_functions,
                            "num_recursive_functions": 0,
                            "num_leaf_functions": num_functions // 4,
                            "num_thunk_functions": 0,
                            "num_basic_blocks": 4 * num_functions,
                            "num_instructions": 32 * num_functions,
                            "num_api_calls": 0,
                            "num_function_calls": 2 * num_functions,
                            "num_failed_functions": 0,
                            "num_failed_instructions": 0,
                        },
                        "minhash_smda_version": "4.6.0",
                    }
                )
            )
        target.functions.bulk_write(function_documents, ordered=False)
        target.samples.bulk_write(sample_documents, ordered=False)
        by_band = {}
        for (band_number, band_hash), function_ids in band_postings.items():
            by_band.setdefault(band_number, []).append(UpdateOne({"band_hash": band_hash}, {"$push": {"function_ids": {"$each": function_ids}}}, upsert=True))
        for band_number, operations in by_band.items():
            collection = target["band_%d" % band_number]
            for offset in range(0, len(operations), 20000):
                collection.bulk_write(operations[offset : offset + 20000], ordered=False)
        generated += chunk
        elapsed = time.time() - started
        print(
            "%d/%d samples  %d functions  %d distinct signatures  %.1f samples/s" % (generated, to_generate, next_function_id, urn.num_distinct, generated / max(1e-9, elapsed)),
            flush=True,
        )

    # the band index needs its lookup index, exactly as MongoDbStorage creates it
    for band_number in range(config.STORAGE_CONFIG.STORAGE_NUM_BANDS):
        target["band_%d" % band_number].create_index("band_hash")
    target.functions.create_index("function_id")
    target.functions.create_index("sample_id")
    target.functions.create_index("_pichash")
    target.samples.create_index("sample_id")
    # keep the id counters and family statistics consistent with what was just written:
    # getStatus() reads the counters, and a later real insert would otherwise reuse ids
    target.counters.update_one({"name": "samples"}, {"$max": {"value": next_sample_id}}, upsert=True)
    target.counters.update_one({"name": "functions"}, {"$max": {"value": next_function_id}}, upsert=True)
    target.counters.update_one({"name": "families"}, {"$max": {"value": SYNTHETIC_FAMILY_ID + 1}}, upsert=True)
    num_synthetic_samples = target.samples.count_documents({"family_id": SYNTHETIC_FAMILY_ID})
    num_synthetic_functions = target.functions.count_documents({"family_id": SYNTHETIC_FAMILY_ID})
    target.families.update_one(
        {"family_id": SYNTHETIC_FAMILY_ID},
        {
            "$set": {
                "family_id": SYNTHETIC_FAMILY_ID,
                "family_name": "synthetic",
                "num_samples": num_synthetic_samples,
                "num_functions": num_synthetic_functions,
                "num_library_samples": 0,
            }
        },
        upsert=True,
    )
    print("done in %.0f s: %d samples, %d functions, %d distinct signatures" % (time.time() - started, args.target_samples, next_function_id, urn.num_distinct), flush=True)
    if args.json:
        with open(args.json, "w") as outfile:
            json.dump(
                {"target_db": args.target_db, "num_samples": args.target_samples, "num_functions": next_function_id, "num_distinct_signatures": urn.num_distinct}, outfile, indent=2
            )


if __name__ == "__main__":
    main()
