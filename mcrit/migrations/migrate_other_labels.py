#!/usr/bin/env python3
"""#301 migration: derive `_other_labels` for the functions stored before it existed.

A plain function search term matches a function's name and the labels that differ from it, which
MongoDB keeps in the derived, indexed field `_other_labels`. Every write since #301 keeps it
current, and a database with at most MongoDbStorage._OTHER_LABELS_STARTUP_MAX labelled functions
is filled on startup. A larger one is filled by this script, while MCRIT keeps serving; until it
has run, a plain term finds a function by a label other than its name only when that function was
renamed or labelled since the upgrade.

Modes, sharing one code path so the dry run exercises the real thing:

  dry-run - read only: count the labelled functions and how many would change.
  apply   - write the field where it is missing or wrong. Resumable: progress is a keyset cursor
            (the last function_id done) kept in a state document, so a killed run costs only the
            batch in flight, and a finished run records the database as complete
            (settings.other_labels_complete), after which startup no longer checks. A re-run
            examines everything again and repairs what writes of an older MCRIT left behind. A
            function the update fails for (one whose document would grow past 16 MiB) is reported,
            exit status 1, and passed over; the database is then not recorded as complete, nor
            after a write concern error.
  verify  - read only: compare every labelled function's field with its labels and name, and
            check that no function without labels carries one.

Each batch reads the next functions in function_id order, the labelled ones only (the server walks
the function_id index and returns those, never the disassembly), and memory stays at one batch. A
function is updated from its document as it stands at the write (the same update MCRIT's own writes
run), so a rename or label added meanwhile is never overwritten with what the batch read.

Usage:
    python -m mcrit.migrations.migrate_other_labels --mode dry-run
    python -m mcrit.migrations.migrate_other_labels --mode apply
    python -m mcrit.migrations.migrate_other_labels --mode verify
"""

import argparse
import json
import sys
import time
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional

from pymongo import ASCENDING, MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

from mcrit.storage.FunctionEntry import OTHER_LABELS_FIELD, otherLabels
from mcrit.storage.MongoDbStorage import MongoDbStorage

# dropped by a finished run, and by MongoDbStorage.clearStorage with the functions it was for
STATE_COLLECTION = MongoDbStorage._OTHER_LABELS_MIGRATION_STATE
STATE_ID = "functions"
LABELLED = {"function_labels.function_label": {"$exists": True}}
# how many differing functions verify names in its report, and how many failed ones apply does
MAX_REPORTED_PROBLEMS = 20


def log(message):
    print("%s %s" % (datetime.now(UTC).strftime("%H:%M:%S"), message), flush=True)


def expected_other_labels(document: Dict[str, Any]) -> List[str]:
    labels = [label.get("function_label") for label in document.get("function_labels") or [] if isinstance(label, dict)]
    return otherLabels(document.get("function_name"), labels)


def is_complete(db) -> bool:
    settings_document = db.settings.find_one({}, {MongoDbStorage._OTHER_LABELS_SETTING: 1})
    return bool(settings_document and settings_document.get(MongoDbStorage._OTHER_LABELS_SETTING))


def labelled_batches(db, batch_size: int, last_function_id: Optional[int]):
    """The labelled functions after last_function_id, a batch at a time in function_id order."""
    projection = {"_id": 0, "function_id": 1, "function_name": 1, "function_labels.function_label": 1, OTHER_LABELS_FIELD: 1}
    # the index MCRIT creates on startup, should the database not have been opened by it yet
    db.functions.create_index("function_id")
    while True:
        query: Dict[str, Any] = dict(LABELLED)
        if last_function_id is not None:
            query["function_id"] = {"$gt": last_function_id}
        # keyset paging on the function_id index, which the hint keeps MongoDB on: planned freely,
        # it may pick the sparse label index and sort every labelled function for each batch
        documents = list(db.functions.find(query, projection).sort("function_id", ASCENDING).hint([("function_id", ASCENDING)]).limit(batch_size))
        if not documents:
            return
        yield documents
        last_function_id = documents[-1]["function_id"]


def migrate(db, mode: str, batch_size: int = 2000, limit: int = 0) -> Dict[str, Any]:
    """dry-run or apply; see the module docstring."""
    assert mode in ("dry-run", "apply")
    started = time.perf_counter()
    stored_state = db[STATE_COLLECTION].find_one({"_id": STATE_ID}) if mode == "apply" else None
    state: Dict[str, Any] = stored_state or {"_id": STATE_ID, "last_function_id": None, "examined": 0, "changed": 0, "started_at": datetime.now(UTC).isoformat()}
    state.setdefault("failed", 0)
    state.setdefault("failed_function_ids", [])
    state.setdefault("write_concern_errors", 0)
    if state["last_function_id"] is not None:
        log("resuming after function_id %d (%d examined, %d changed before)" % (state["last_function_id"], state["examined"], state["changed"]))
    examined = changed = 0
    finished = True
    for documents in labelled_batches(db, batch_size, state["last_function_id"]):
        updates = []
        updated_ids: List[int] = []
        for document in documents:
            if sorted(document.get(OTHER_LABELS_FIELD) or []) != sorted(expected_other_labels(document)):
                # derived from the document as it stands at the write, as MCRIT's writes do
                updates.append(UpdateOne({"function_id": document["function_id"]}, [MongoDbStorage._OTHER_LABELS_STAGE]))
                updated_ids.append(document["function_id"])
        failed: List[int] = []
        if updates and mode == "apply":
            try:
                db.functions.bulk_write(updates, ordered=False)
            except BulkWriteError as error:
                # unordered: the others are written; these are passed over, so a re-run gets past them too
                write_errors = error.details.get("writeErrors") or []
                failed = [updated_ids[write_error["index"]] for write_error in write_errors]
                if write_errors:
                    log("  could not update %d functions, e.g. %s: %s" % (len(failed), failed[:5], write_errors[0].get("errmsg", "")))
                # written, perhaps, but not as durably as asked: the run cannot vouch for this batch
                write_concern_errors = error.details.get("writeConcernErrors") or []
                if write_concern_errors:
                    state["write_concern_errors"] += len(write_concern_errors)
                    log("  %d write concern errors: %s" % (len(write_concern_errors), write_concern_errors[0].get("errmsg", "")))
            # every MCRIT process lists the other labels anew for its next plain search
            db.settings.update_one({}, {"$inc": {MongoDbStorage._OTHER_LABELS_VERSION_SETTING: 1}})
        examined += len(documents)
        changed += len(updates) - len(failed)
        if mode == "apply":
            state["failed"] += len(failed)
            state["failed_function_ids"] = (state["failed_function_ids"] + failed)[:MAX_REPORTED_PROBLEMS]
            state.update(
                {"last_function_id": documents[-1]["function_id"], "examined": state["examined"] + len(documents), "changed": state["changed"] + len(updates) - len(failed)}
            )
            db[STATE_COLLECTION].replace_one({"_id": STATE_ID}, state, upsert=True)
        if examined % (batch_size * 25) < len(documents):
            log(
                "  %d labelled functions examined, %d %s, %.0f/s"
                % (examined, changed, "changed" if mode == "apply" else "to change", examined / max(time.perf_counter() - started, 1e-9))
            )
        if limit and examined >= limit:
            log("stopping at --limit %d" % limit)
            finished = False
            break
    failures = {"failed": state["failed"], "failed_function_ids": state["failed_function_ids"], "write_concern_errors": state["write_concern_errors"]} if mode == "apply" else {}
    complete = mode == "apply" and finished and not state["failed"] and not state["write_concern_errors"]
    if mode == "apply" and finished:
        db[STATE_COLLECTION].drop()
        if complete:
            # every labelled function is done, and MCRIT's writes keep the field from here on
            db.settings.update_one({}, {"$set": {MongoDbStorage._OTHER_LABELS_SETTING: True}})
            log("finished: recorded settings.%s" % MongoDbStorage._OTHER_LABELS_SETTING)
        else:
            log(
                "finished, but %d functions could not be updated, e.g. %s, and %d writes met write concern errors: not recorded as complete"
                % (state["failed"], state["failed_function_ids"], state["write_concern_errors"])
            )
    elapsed = time.perf_counter() - started
    log("done: %d labelled functions examined, %d %s, %.1f s" % (examined, changed, "changed" if mode == "apply" else "would change", elapsed))
    return {"examined": examined, "changed": changed, "complete": complete, "seconds": elapsed, **failures}


def verify(db, batch_size: int = 2000) -> Dict[str, Any]:
    started = time.perf_counter()
    problems: List[str] = []
    examined = differing = 0
    for documents in labelled_batches(db, batch_size, None):
        for document in documents:
            stored, expected = sorted(document.get(OTHER_LABELS_FIELD) or []), sorted(expected_other_labels(document))
            if stored != expected:
                differing += 1
                if len(problems) < MAX_REPORTED_PROBLEMS:
                    problems.append("function %d: %s is %s, its labels and name give %s" % (document["function_id"], OTHER_LABELS_FIELD, stored, expected))
        examined += len(documents)
    # the field's sparse index holds exactly the functions that carry it
    stray = db.functions.count_documents({OTHER_LABELS_FIELD: {"$exists": True}, "function_labels.function_label": {"$exists": False}})
    if stray:
        problems.append("%d functions without labels carry %s" % (stray, OTHER_LABELS_FIELD))
    if differing > MAX_REPORTED_PROBLEMS:
        problems.append("... %d differing functions in all" % differing)
    return {"examined": examined, "differing": differing, "stray": stray, "complete": is_complete(db), "problems": problems, "seconds": time.perf_counter() - started}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=["dry-run", "apply", "verify"], required=True)
    parser.add_argument("--host", default=None, help="defaults to MCRIT's storage configuration")
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--db", default=None)
    parser.add_argument("--batch", type=int, default=2000)
    parser.add_argument("--limit", type=int, default=0, help="apply / dry-run: stop after N labelled functions (resumable)")
    parser.add_argument("--out", default=None)
    args = parser.parse_args(argv)
    if args.host is None or args.port is None or args.db is None:
        from mcrit.config.McritConfig import McritConfig

        storage_config = McritConfig().STORAGE_CONFIG
        args.host = args.host or storage_config.STORAGE_SERVER
        args.port = args.port or int(storage_config.STORAGE_PORT)
        args.db = args.db or storage_config.STORAGE_MONGODB_DBNAME

    db = MongoClient("mongodb://%s:%d" % (args.host, args.port), connect=True)[args.db]
    result: Dict[str, Any] = {"mode": args.mode, "db": args.db, "utc": datetime.now(UTC).isoformat()}
    if args.mode == "verify":
        result["functions"] = verify(db, args.batch)
    else:
        result["functions"] = migrate(db, args.mode, args.batch, args.limit)
    print(json.dumps(result, indent=2, default=str))
    if args.out:
        with open(args.out, "w") as handle:
            json.dump(result, handle, indent=2, default=str)
    if args.mode == "verify":
        return 1 if result["functions"]["problems"] else 0
    return 1 if result["functions"].get("failed") or result["functions"].get("write_concern_errors") else 0


if __name__ == "__main__":
    sys.exit(main())
