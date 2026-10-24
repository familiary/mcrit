#!/usr/bin/env python3
"""Read-only consistency check of a corpus, for data that older releases let in (#270).

Reports, without writing anything:

  - samples whose stored functions do not number what their statistics.num_functions says, and
    functions whose sample does not exist, or whose family is not their sample's;
  - functions holding a MinHash of another length than the configured signature has;
  - how many samples are sha256_verified true, false and null, and samples left with a claim or
    an unfinished replacement by an upload whose job died, or half deleted by a deletion that died;
  - what replacing a sample's analysis by its uploaded binary's staged under a staging id no
    sample records any more;
  - stored binaries whose sha256 is not their samples', or that belong to no sample;
  - families whose counters disagree with their samples and functions.

It scales with the corpus by leaving the work to MongoDB: one aggregation over the functions,
projected to the few fields it counts before grouping by sample, and projected scans of samples,
families and binary metadata. Disassembly is never read, and memory grows with the number of
samples, not of functions. Exits with 1 when it finds a problem, as migrate_xcfg_split's verify
mode does. What it reports can be fixed with POST /recompute_family_stats (family counters),
/repair_minhashes or a re-submission (MinHashes), another upload of the binary (an unfinished
replacement) or another deletion (an unfinished one); the rest needs a look at the samples named.

Usage:
    python -m mcrit.migrations.verify_corpus
    python -m mcrit.migrations.verify_corpus --db mcrit --out corpus_check.json
    python -m mcrit.migrations.verify_corpus --hash-binaries   # also hash the stored bytes
"""

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional, Tuple

import gridfs
from pymongo import MongoClient

BINARIES_BUCKET = "sample_binaries"
# as MongoDbStorage stages a replacement of a sample's analysis by its binary's
STAGING_COLLECTIONS = ("functions_staging", "xcfg_staging", "smda_extras_staging")


def minhash_hex_length(signature_length: int, signature_bits: int) -> int:
    """How many hex digits a stored MinHash has: one byte per entry up to 8 bits, else four."""
    return 2 * signature_length * (1 if signature_bits <= 8 else 4)


class Examples:
    """A count with the first few instances, so a corpus in bad shape still yields a small report."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.count = 0
        self.items: List[Any] = []

    def add(self, item: Any) -> None:
        self.count += 1
        if len(self.items) < self.limit:
            self.items.append(item)

    def toDict(self) -> Dict[str, Any]:
        return {"count": self.count, "examples": self.items}


def _count_functions(db, expected_minhash_length: int) -> List[Dict[str, Any]]:
    """Functions per (sample, family) with how many hold a MinHash of the wrong length: one pass."""
    # in hex digits: a minhash is stored as BSON binary since #256, and as hex text before
    minhash_length = {
        "$switch": {
            "branches": [
                {"case": {"$eq": [{"$type": "$minhash"}, "string"]}, "then": {"$strLenBytes": "$minhash"}},
                {"case": {"$eq": [{"$type": "$minhash"}, "binData"]}, "then": {"$multiply": [2, {"$binarySize": "$minhash"}]}},
            ],
            "default": 0,
        }
    }
    pipeline = [
        # projected first: nothing past these three fields leaves the storage engine's documents
        {"$project": {"_id": 0, "sample_id": 1, "family_id": 1, "minhash_length": minhash_length}},
        {
            "$group": {
                "_id": {"sample_id": "$sample_id", "family_id": "$family_id"},
                "num_functions": {"$sum": 1},
                "num_wrong_minhashes": {"$sum": {"$cond": [{"$and": [{"$gt": ["$minhash_length", 0]}, {"$ne": ["$minhash_length", expected_minhash_length]}]}, 1, 0]}},
            }
        },
    ]
    return list(db.functions.aggregate(pipeline, allowDiskUse=True))


def _hash_binary(bucket: gridfs.GridFS, file_id) -> str:
    sha256 = hashlib.sha256()
    stored = bucket.get(file_id)
    while chunk := stored.readchunk():
        sha256.update(chunk)
    return sha256.hexdigest()


def verify_corpus(db, signature_length: int, signature_bits: int, max_examples: int = 20, hash_binaries: bool = False) -> Dict[str, Any]:
    expected_minhash_length = minhash_hex_length(signature_length, signature_bits)
    # the staged documents before the samples: a job staging in between records its staging id
    # on its sample before it writes, so it is found recorded rather than reported orphaned
    staged_ids: List[Tuple[str, Any, int]] = []
    for collection in STAGING_COLLECTIONS:
        for row in db[collection].aggregate([{"$group": {"_id": "$staging_id", "num_documents": {"$sum": 1}}}, {"$sort": {"_id": 1}}]):
            staged_ids.append((collection, row["_id"], row["num_documents"]))
    # one small tuple per sample: (sha256, family_id, is_library, statistics.num_functions)
    samples: Dict[int, Tuple[str, int, bool, Optional[int]]] = {}
    by_verified = {"true": 0, "false": 0, "null": 0}
    pending_verification = Examples(max_examples)
    unfinished_replacements = Examples(max_examples)
    unfinished_deletions = Examples(max_examples)
    recorded_staging_ids = set()
    projection = {"_id": 0, "sample_id": 1, "sha256": 1, "family_id": 1, "is_library": 1, "statistics.num_functions": 1, "sha256_verified": 1, "verification": 1}
    for document in db.samples.find({}, projection):
        statistics = document.get("statistics") or {}
        samples[document["sample_id"]] = (document.get("sha256", ""), document.get("family_id", 0), bool(document.get("is_library")), statistics.get("num_functions"))
        verified = document.get("sha256_verified")
        by_verified["true" if verified is True else "false" if verified is False else "null"] += 1
        verification = document.get("verification")
        if verification:
            if verification.get("staging_id") is not None:
                recorded_staging_ids.add(verification["staging_id"])
            if verification.get("deleting"):
                # deleteSample claims a sample first; a sample still there was not deleted to the end
                unfinished_deletions.add(document["sample_id"])
                continue
            # a claim is normal while an upload's job runs; a swap begun is finished within seconds,
            # so one that stays is a job that died midway, finished by the next upload of the binary
            pending_verification.add({"sample_id": document["sample_id"], "claimed_at": verification.get("claimed_at")})
            if verification.get("swap"):
                unfinished_replacements.add(document["sample_id"])

    counted_functions: Dict[int, int] = {}
    functions_by_family: Dict[int, int] = {}
    wrong_minhashes = Examples(max_examples)
    orphaned_functions = Examples(max_examples)
    foreign_family_functions = Examples(max_examples)
    num_functions = 0
    num_wrong_minhashes = 0
    for row in _count_functions(db, expected_minhash_length):
        sample_id = row["_id"].get("sample_id")
        family_id = row["_id"].get("family_id")
        count = row["num_functions"]
        num_functions += count
        counted_functions[sample_id] = counted_functions.get(sample_id, 0) + count
        functions_by_family[family_id] = functions_by_family.get(family_id, 0) + count
        num_wrong_minhashes += row["num_wrong_minhashes"]
        if row["num_wrong_minhashes"]:
            wrong_minhashes.add({"sample_id": sample_id, "num_functions": row["num_wrong_minhashes"]})
        if sample_id not in samples:
            orphaned_functions.add({"sample_id": sample_id, "num_functions": count})
        elif samples[sample_id][1] != family_id:
            foreign_family_functions.add({"sample_id": sample_id, "sample_family_id": samples[sample_id][1], "function_family_id": family_id, "num_functions": count})
    function_count_mismatches = Examples(max_examples)
    for sample_id, (_, _, _, num_stated) in samples.items():
        num_stored = counted_functions.get(sample_id, 0)
        if num_stated != num_stored:
            function_count_mismatches.add({"sample_id": sample_id, "statistics_num_functions": num_stated, "stored_functions": num_stored})

    binary_mismatches = Examples(max_examples)
    binaries_without_sample = Examples(max_examples)
    num_binaries = 0
    bucket = gridfs.GridFS(db, collection=BINARIES_BUCKET)
    for document in db[f"{BINARIES_BUCKET}.files"].find({}, {"_id": 1, "metadata": 1}):
        num_binaries += 1
        metadata = document.get("metadata") or {}
        stated_sha256 = metadata.get("sha256")
        content_sha256 = _hash_binary(bucket, document["_id"]) if hash_binaries else None
        if content_sha256 is not None and stated_sha256 is not None and content_sha256 != stated_sha256:
            binary_mismatches.add({"file_id": str(document["_id"]), "metadata_sha256": stated_sha256, "content_sha256": content_sha256})
        sample_ids = metadata.get("sample_ids") or []
        if not sample_ids and stated_sha256 is not None:
            binaries_without_sample.add({"file_id": str(document["_id"]), "sha256": stated_sha256})
        for sample_id in sample_ids:
            if sample_id not in samples:
                binaries_without_sample.add({"file_id": str(document["_id"]), "sample_id": sample_id})
                continue
            binary_sha256 = content_sha256 or stated_sha256
            if binary_sha256 != samples[sample_id][0]:
                binary_mismatches.add({"sample_id": sample_id, "sample_sha256": samples[sample_id][0], "binary_sha256": binary_sha256})

    samples_by_family: Dict[int, List[int]] = {}
    for _, family_id, is_library, _ in samples.values():
        counts = samples_by_family.setdefault(family_id, [0, 0])
        counts[0] += 1
        counts[1] += int(is_library)
    family_mismatches = Examples(max_examples)
    known_family_ids = set()
    for document in db.families.find({}, {"_id": 0, "family_id": 1, "num_samples": 1, "num_functions": 1, "num_library_samples": 1}):
        family_id = document["family_id"]
        known_family_ids.add(family_id)
        num_samples, num_library_samples = samples_by_family.get(family_id, [0, 0])
        # as recomputeFamilyStats counts them
        actual = {"num_samples": num_samples, "num_functions": functions_by_family.get(family_id, 0), "num_library_samples": num_library_samples}
        stored = {key: document.get(key) for key in actual}
        if stored != actual:
            family_mismatches.add({"family_id": family_id, "stored": stored, "actual": actual})
    families_without_document = Examples(max_examples)
    for family_id in sorted(set(samples_by_family) - known_family_ids):
        families_without_document.add(family_id)

    # staged documents are discarded by whoever finds their staging id on the sample: the job, the
    # next upload of the binary, deleteSample. Under an id no sample records, nothing will
    orphaned_staging = Examples(max_examples)
    for collection, staging_id, num_documents in staged_ids:
        if staging_id not in recorded_staging_ids:
            orphaned_staging.add({"collection": collection, "staging_id": staging_id, "num_documents": num_documents})

    checks = {
        "function_count_mismatches": function_count_mismatches.toDict(),
        "functions_without_sample": orphaned_functions.toDict(),
        "functions_of_another_family": foreign_family_functions.toDict(),
        # counted by sample, as /repair_minhashes rehashes them
        "samples_with_wrong_length_minhashes": {**wrong_minhashes.toDict(), "num_functions": num_wrong_minhashes},
        "binary_sha256_mismatches": binary_mismatches.toDict(),
        "binaries_without_sample": binaries_without_sample.toDict(),
        "family_counter_mismatches": family_mismatches.toDict(),
        "families_without_document": families_without_document.toDict(),
        "unfinished_replacements": unfinished_replacements.toDict(),
        "unfinished_deletions": unfinished_deletions.toDict(),
        "orphaned_staging": orphaned_staging.toDict(),
    }
    problems = ["%s: %d" % (name, check["count"]) for name, check in checks.items() if check["count"]]
    return {
        "num_samples": len(samples),
        "num_functions": num_functions,
        "num_families": len(known_family_ids),
        "num_binaries": num_binaries,
        "binaries_hashed": hash_binaries,
        "expected_minhash_hex_length": expected_minhash_length,
        "samples_by_sha256_verified": by_verified,
        # informational: samples an upload's job is checking against their binary right now
        "pending_verifications": pending_verification.toDict(),
        "checks": checks,
        "problems": problems,
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only consistency check of an MCRIT corpus (#270).")
    parser.add_argument("--host", default=None, help="defaults to STORAGE_SERVER")
    parser.add_argument("--port", type=int, default=None, help="defaults to STORAGE_PORT")
    parser.add_argument("--db", default=None, help="defaults to STORAGE_MONGODB_DBNAME")
    parser.add_argument("--signature-length", type=int, default=None, help="defaults to MINHASH_SIGNATURE_LENGTH")
    parser.add_argument("--signature-bits", type=int, default=None, help="defaults to MINHASH_SIGNATURE_BITS")
    parser.add_argument("--max-examples", type=int, default=20, help="instances listed per check; the counts are complete")
    parser.add_argument("--hash-binaries", action="store_true", help="hash every stored binary instead of trusting its recorded sha256")
    parser.add_argument("--out", default=None)
    args = parser.parse_args(argv)
    if None in (args.host, args.port, args.db, args.signature_length, args.signature_bits):
        # only what was not given comes from the configuration
        from mcrit.config.McritConfig import McritConfig

        mcrit_config = McritConfig()
        storage_config = mcrit_config.STORAGE_CONFIG
        args.host = args.host if args.host is not None else storage_config.STORAGE_SERVER
        args.port = args.port if args.port is not None else int(storage_config.STORAGE_PORT)
        args.db = args.db if args.db is not None else storage_config.STORAGE_MONGODB_DBNAME
        args.signature_length = args.signature_length if args.signature_length is not None else mcrit_config.MINHASH_CONFIG.MINHASH_SIGNATURE_LENGTH
        args.signature_bits = args.signature_bits if args.signature_bits is not None else mcrit_config.MINHASH_CONFIG.MINHASH_SIGNATURE_BITS

    client = MongoClient("mongodb://%s:%d" % (args.host, args.port), connect=True)
    try:
        result: Dict[str, Any] = {"db": args.db, "utc": datetime.now(UTC).isoformat()}
        result.update(verify_corpus(client[args.db], args.signature_length, args.signature_bits, max_examples=args.max_examples, hash_binaries=args.hash_binaries))
    finally:
        client.close()
    print(json.dumps(result, indent=2, default=str))
    if args.out:
        with open(args.out, "w") as handle:
            json.dump(result, handle, indent=2, default=str)
    return 1 if result["problems"] else 0


if __name__ == "__main__":
    sys.exit(main())
