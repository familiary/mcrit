#!/usr/bin/env python3
"""Store pichashes zero-padded so that their hex order is their numeric order (#145).

`functions._pichash` and `functions._picblockhashes[].hash` (and the same fields of
`query_functions`) hold 64 bit values as hex strings. Written with `hex()`, they have a
variable width ("0x4d2"), so a range condition or a sort on them orders "0x99" after
"0x1000". Padding every value to 16 digits ("0x00000000000004d2") makes string order numeric
order; the storage then allows range operators and sorting by pichash, which the search
cursor needs to page a pichash-sorted function search.

Modes:
  pad     - the migration: rewrite every unpadded value in place, batched by function_id
            (resumable: progress is a keyset cursor in a state document, a killed run costs
            one batch). A final sweep catches documents the walk did not see, then the
            settings flag `pichash_padded` is set, which switches every reader and writer
            of the instance to the padded shape.
  verify  - count the values that are still unpadded and check the flag against them;
            exits 1 when the flag says padded but unpadded values remain.
  unpad   - rollback: rewrite every value with `hex()` again and clear the flag.

Stop the mcrit server and its workers before `pad`: while the flag is unset they keep
writing unpadded values, which `pad` would have to be re-run for (it is idempotent, and
`verify` reports leftovers). Storage reads the flag once per process, so restart them
afterwards. Until the flag is set, all readers accept both widths, so the instance keeps
answering correctly during the walk; only range and sort by pichash stay rejected.

Two derived indexes are keyed on the stored spelling and would silently disagree with the
rewritten functions: the PicHash count index (`pichash_counts`, MINHASH_PICHASH_MAX_MATCHES),
whose lookups would miss every count and drop every PicHash match, and the inverted
picblockhash index (`picblockhashes`, getUniqueBlocks), whose lookups would miss every block
and report blocks as unique that are not. `pad` and `unpad` mark both incomplete and drop them.
Both readers then fall back to the functions collection, correct but slow (and they warn),
until rebuildPicHashCountIndex and rebuildPicBlockHashIndex (GET /rebuild_picblockhash_index)
have run again; `verify` reports an index that is marked complete but holds the other width.

Usage:
    python -m mcrit.migrations.migrate_pichash_padding --mode pad
    python -m mcrit.migrations.migrate_pichash_padding --mode verify
    python -m mcrit.migrations.migrate_pichash_padding --mode unpad
"""

import argparse
import json
import sys
import time
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

from pymongo import ASCENDING, MongoClient, UpdateOne

from mcrit.config.McritConfig import McritConfig
from mcrit.storage.MongoDbStorage import PICHASH_HEX_DIGITS, MongoDbStorage, encode_pichash_value

STATE_COLLECTION = "pichash_padding_state"
PICHASH_COUNT_COLLECTION = MongoDbStorage._PICHASH_COUNT_COLLECTION
PICHASH_COUNT_SETTING = MongoDbStorage._PICHASH_COUNT_SETTING
PICBLOCKHASH_INDEX_COLLECTION = MongoDbStorage._PICBLOCKHASH_INDEX_COLLECTION
PICBLOCKHASH_INDEX_SETTING = MongoDbStorage._PICBLOCKHASH_INDEX_SETTING
# the derived indexes keyed on the stored spelling: (collection, key field, completeness flag)
DERIVED_INDEXES = (
    (PICHASH_COUNT_COLLECTION, "_pichash", PICHASH_COUNT_SETTING),
    (PICBLOCKHASH_INDEX_COLLECTION, "_id", PICBLOCKHASH_INDEX_SETTING),
)
COLLECTIONS = ("functions", "query_functions")
PROJECTION = {"function_id": 1, "_pichash": 1, "_picblockhashes.hash": 1, "_id": 0}


def build_mongo_uri(host: str, port: int, db_name: str, username: Optional[str], password: Optional[str], flags: Optional[str]) -> str:
    credentials = "%s:%s@" % (quote_plus(username), quote_plus(password)) if username and password else ""
    query = "?%s" % flags if flags else ""
    return "mongodb://%s%s:%d/%s%s" % (credentials, host, port, db_name, query)


def log(message):
    print("%s %s" % (datetime.now(UTC).strftime("%H:%M:%S"), message), flush=True)


def unpadded_query(prefix: str = "", field: str = "_pichash") -> Dict[str, Any]:
    """Values of fewer than 16 digits; anchored, so the index on the field serves it."""
    return {prefix + field: {"$regex": "^0x[0-9a-f]{1,%d}$" % (PICHASH_HEX_DIGITS - 1)}}


def leading_zero_query(field: str) -> Dict[str, Any]:
    """Padded values with a leading zero, the ones `hex()` never writes."""
    return {field: {"$regex": "^0x0[0-9a-f]{%d}$" % (PICHASH_HEX_DIGITS - 1)}}


def unpadded_block_query() -> Dict[str, Any]:
    return {"_picblockhashes.hash": {"$regex": "^0x[0-9a-f]{1,%d}$" % (PICHASH_HEX_DIGITS - 1)}}


def padded_query() -> Dict[str, Any]:
    return {"_pichash": {"$regex": "^0x[0-9a-f]{%d}$" % PICHASH_HEX_DIGITS}}


def padded_block_query() -> Dict[str, Any]:
    return {"_picblockhashes.hash": {"$regex": "^0x[0-9a-f]{%d}$" % PICHASH_HEX_DIGITS}}


def rewrite(document: Dict[str, Any], padded: bool) -> Optional[UpdateOne]:
    """The update that brings one document to the requested width, or None if it is there."""
    update: Dict[str, Any] = {}
    pichash = document.get("_pichash")
    if isinstance(pichash, str):
        wanted = encode_pichash_value(int(pichash, 16), padded)
        if wanted != pichash:
            update["_pichash"] = wanted
    block_entries = document.get("_picblockhashes")
    if block_entries:
        wanted_hashes = [encode_pichash_value(int(entry["hash"], 16), padded) if isinstance(entry.get("hash"), str) else entry.get("hash") for entry in block_entries]
        if wanted_hashes != [entry.get("hash") for entry in block_entries]:
            # positional per-element $set keeps offset/length untouched and the array order stable
            for position, wanted in enumerate(wanted_hashes):
                if wanted != block_entries[position].get("hash"):
                    update["_picblockhashes.%d.hash" % position] = wanted
    if not update:
        return None
    return UpdateOne({"function_id": document["function_id"]}, {"$set": update})


def get_state(db, key):
    return db[STATE_COLLECTION].find_one({"_id": key}) or {"_id": key, "last_function_id": None, "seen": 0, "rewritten": 0, "started_at": None}


def put_state(db, state):
    db[STATE_COLLECTION].replace_one({"_id": state["_id"]}, state, upsert=True)


def walk(db, collection_name: str, padded: bool, batch_size: int) -> Dict[str, Any]:
    mode = "pad" if padded else "unpad"
    state = get_state(db, "%s:%s" % (mode, collection_name))
    if state["started_at"] is None:
        state["started_at"] = datetime.now(UTC).isoformat()
    last_function_id = state["last_function_id"]
    seen, rewritten = state["seen"], state["rewritten"]
    started = time.perf_counter()
    log("%s %s from function_id > %s" % (mode, collection_name, last_function_id))
    while True:
        query = {} if last_function_id is None else {"function_id": {"$gt": last_function_id}}
        documents = list(db[collection_name].find(query, PROJECTION).sort("function_id", ASCENDING).limit(batch_size))
        if not documents:
            break
        updates = [update for update in (rewrite(document, padded) for document in documents) if update is not None]
        if updates:
            db[collection_name].bulk_write(updates, ordered=False)
        last_function_id = documents[-1]["function_id"]
        seen += len(documents)
        rewritten += len(updates)
        state.update({"last_function_id": last_function_id, "seen": seen, "rewritten": rewritten})
        put_state(db, state)
        if seen % (batch_size * 10) == 0:
            elapsed = time.perf_counter() - started
            log("  %d seen, %d rewritten, %.0f docs/s" % (seen, rewritten, seen / max(elapsed, 1e-9)))
    # documents the keyset walk did not see: written below the cursor by a server that was
    # still running, or left by an interrupted earlier run
    leftover_query = {"$or": [unpadded_query(), unpadded_block_query()]} if padded else {"$or": [padded_query(), padded_block_query()]}
    swept = 0
    while True:
        documents = list(db[collection_name].find(leftover_query, PROJECTION).limit(batch_size))
        updates = [update for update in (rewrite(document, padded) for document in documents) if update is not None]
        if not updates:
            break
        db[collection_name].bulk_write(updates, ordered=False)
        swept += len(updates)
    state["finished_at"] = datetime.now(UTC).isoformat()
    put_state(db, state)
    elapsed = time.perf_counter() - started
    log("done %s: %d seen, %d rewritten, %d swept, %.1f s" % (collection_name, seen, rewritten, swept, elapsed))
    return {"seen": seen, "rewritten": rewritten, "swept": swept, "seconds": elapsed}


def invalidate_derived_indexes(db) -> None:
    """Mark the indexes keyed on the stored spelling incomplete, then drop them.

    The walk has just changed that spelling. Left as they are, pichash_counts would miss every
    count, so the cutoff would drop every PicHash match, and the picblockhash index would miss
    every block, so getUniqueBlocks would report shared blocks as unique. Marked incomplete, both
    readers fall back to the functions collection and the write paths stop maintaining them. The
    flag goes first, so that no reader trusts an index that is half gone.
    """
    for collection_name, _, setting in DERIVED_INDEXES:
        db.settings.update_one({}, {"$set": {setting: False}}, upsert=True)
        db[collection_name].drop()
    log("pichash_counts and picblockhashes dropped; run rebuildPicHashCountIndex() and rebuildPicBlockHashIndex() to restore the fast paths")


def set_flag(db, padded: bool, invalidate: bool = True) -> None:
    if invalidate:
        invalidate_derived_indexes(db)
    db.settings.update_one({}, {"$set": {"pichash_padded": padded}}, upsert=True)
    # clear the walk state so a later run of the other mode starts from the top
    db[STATE_COLLECTION].delete_many({})
    log("settings.pichash_padded = %s" % padded)


def verify(db) -> Dict[str, Any]:
    settings = db.settings.find_one({}, {"pichash_padded": 1, "_id": 0}) or {}
    flag = bool(settings.get("pichash_padded", False))
    report: Dict[str, Any] = {"pichash_padded": flag, "collections": {}, "problems": []}
    for collection_name in COLLECTIONS:
        counts = {
            "documents": db[collection_name].count_documents({}),
            "unpadded_pichashes": db[collection_name].count_documents(unpadded_query()),
            "padded_pichashes": db[collection_name].count_documents(padded_query()),
            "documents_with_unpadded_blockhashes": db[collection_name].count_documents(unpadded_block_query()),
            "documents_with_padded_blockhashes": db[collection_name].count_documents(padded_block_query()),
        }
        report["collections"][collection_name] = counts
        # only one direction is a problem: a 16-digit value is legitimate under either encoding
        # (most 64 bit pichashes have no leading zero), an unpadded one only while the flag is unset
        if flag and (counts["unpadded_pichashes"] or counts["documents_with_unpadded_blockhashes"]):
            report["problems"].append("%s: flag says padded but unpadded values remain; re-run --mode pad" % collection_name)
    # A derived index is only read while it is marked complete, and then its keys have to be
    # spelled like the functions it was built from. Here a leading-zero value tells the widths
    # apart in both directions, since `hex()` never writes one.
    report["derived_indexes"] = {}
    for collection_name, field, setting in DERIVED_INDEXES:
        complete = bool((db.settings.find_one({}, {setting: 1, "_id": 0}) or {}).get(setting, False))
        wrong_width = unpadded_query(field=field) if flag else leading_zero_query(field)
        num_wrong_width = db[collection_name].count_documents(wrong_width)
        report["derived_indexes"][collection_name] = {"complete": complete, "keys_of_the_other_width": num_wrong_width}
        if complete and num_wrong_width:
            report["problems"].append("%s: marked complete but keyed on the other width; rebuild it" % collection_name)
    return report


def run(db, mode: str, batch_size: int = 2000) -> Dict[str, Any]:
    result: Dict[str, Any] = {"mode": mode, "utc": datetime.now(UTC).isoformat()}
    if mode == "verify":
        result.update(verify(db))
        return result
    padded = mode == "pad"
    flag_before = bool((db.settings.find_one({}) or {}).get("pichash_padded", False))
    for collection_name in COLLECTIONS:
        result[collection_name] = walk(db, collection_name, padded, batch_size)
    # a re-run that found nothing to rewrite (the counts carry over from an interrupted run, so
    # that one still counts) leaves the spelling, and the indexes built on it, as they were
    changed = flag_before != padded or any(result[name]["rewritten"] + result[name]["swept"] for name in COLLECTIONS)
    set_flag(db, padded, invalidate=changed)
    result.update(verify(db))
    return result


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["pad", "verify", "unpad"], required=True)
    storage_config = McritConfig().STORAGE_CONFIG
    parser.add_argument("--host", default=storage_config.STORAGE_SERVER)
    parser.add_argument("--port", type=int, default=int(storage_config.STORAGE_PORT))
    parser.add_argument("--db", default=storage_config.STORAGE_MONGODB_DBNAME)
    parser.add_argument("--uri", default=None, help="complete MongoDB URI; overrides host/port and the configured credentials and flags")
    parser.add_argument("--batch", type=int, default=2000)
    parser.add_argument("--out", default=None)
    args = parser.parse_args(argv)

    # the same credentials and connection flags MongoDbStorage connects with (STORAGE_MONGODB_*),
    # so a secured deployment needs nothing beyond its mcrit configuration
    uri = args.uri or build_mongo_uri(
        args.host, args.port, args.db, storage_config.STORAGE_MONGODB_USERNAME, storage_config.STORAGE_MONGODB_PASSWORD, storage_config.STORAGE_MONGODB_FLAGS
    )
    db = MongoClient(uri, connect=True)[args.db]
    result = run(db, args.mode, args.batch)
    print(json.dumps(result, indent=2, default=str))
    if args.out:
        with open(args.out, "w") as handle:
            json.dump(result, handle, indent=2, default=str)
    return 1 if result.get("problems") else 0


if __name__ == "__main__":
    sys.exit(main())
