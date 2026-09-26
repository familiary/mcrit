import datetime
import json
import logging
import re
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from itertools import zip_longest
from operator import itemgetter
from typing import TYPE_CHECKING, Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple, Union

import numpy as np
from bson import encode as bson_encode
from packaging import version
from picblocks.blockhasher import BlockHasher
from pymongo import MongoClient, UpdateMany, UpdateOne
from pymongo.errors import BulkWriteError, DocumentTooLarge
from smda.common.BinaryInfo import BinaryInfo
from smda.common.SmdaFunction import SmdaFunction
from smda.SmdaConfig import SmdaConfig

from mcrit.index.SearchCursor import FullSearchCursor
from mcrit.index.SearchQueryTree import (
    AndNode,
    BaseVisitor,
    FilterSingleElementLists,
    NodeType,
    NotNode,
    OrNode,
    PropagateNot,
    SearchConditionNode,
    SearchFieldResolver,
)
from mcrit.libs.utility import decode_two_complement, encode_two_complement
from mcrit.minhash.MinHash import MinHash
from mcrit.storage.FamilyEntry import FamilyEntry
from mcrit.storage.FunctionEntry import FunctionEntry
from mcrit.storage.FunctionLabelEntry import FunctionLabelEntry
from mcrit.storage.MatchingCache import MatchingCache
from mcrit.storage.SampleEntry import SampleEntry
from mcrit.storage.StorageInterface import StorageInterface

LOGGER = logging.getLogger(__name__)
if MongoClient is None:
    LOGGER.warning("pymongo package import failed - MongoDB backend will not be available.")

if TYPE_CHECKING:  # pragma: no cover
    from pymongo.database import Database
    from smda.common.SmdaFunction import SmdaFunction
    from smda.common.SmdaReport import SmdaReport

    from mcrit.config.McritConfig import McritConfig


class MongoSearchTranspiler(BaseVisitor):
    """
    Converts a tree to a MongoDB query.
    The input tree MUST NOT contain Not or SearchTerm nodes.

    known_values maps a field to the complete list of distinct values it holds. A substring
    condition on such a field is evaluated against that list here and emitted as an $in / $nin
    of the values that match, which MongoDB answers from the index. The unanchored,
    case-insensitive regex it replaces cannot use index bounds and makes MongoDB examine every
    document of the collection when nothing matches (fkie-cad/mcritweb#76).
    """

    @staticmethod
    def _or_query(*conditions):
        if len(conditions) == 0:
            return {}
        if len(conditions) == 1:
            return conditions[0]
        return {"$or": conditions}

    @staticmethod
    def _and_query(*conditions):
        if len(conditions) == 0:
            return {}
        if len(conditions) == 1:
            return conditions[0]
        return {"$and": conditions}

    def visitAndNode(self, node: AndNode):
        visited_children = [self.visit(child) for child in node.children]
        return self._and_query(*visited_children)

    def visitOrNode(self, node: OrNode):
        visited_children = [self.visit(child) for child in node.children]
        return self._or_query(*visited_children)

    def __init__(self, known_values: Optional[Dict[str, List[Any]]] = None) -> None:
        super().__init__()
        self.known_values = known_values or {}

    def _substring_condition_from_known_values(self, node: SearchConditionNode) -> Optional[Dict[str, Any]]:
        # an empty search term matches everything, so the regex is left alone there
        if not node.operator.endswith("?") or node.field not in self.known_values or not node.value:
            return None
        pattern = re.compile(re.escape(node.value), re.IGNORECASE)
        matching = [value for value in self.known_values[node.field] if isinstance(value, str) and pattern.search(value)]
        if node.operator == "!?":
            return {node.field: {"$nin": matching}}
        return {node.field: {"$in": matching}}

    def visitSearchConditionNode(self, node: SearchConditionNode):
        condition_from_known_values = self._substring_condition_from_known_values(node)
        if condition_from_known_values is not None:
            return condition_from_known_values
        operator_to_mongo = {
            "<": "$lt",
            "<=": "$lte",
            ">": "$gt",
            ">=": "$gte",
            "=": None,
            "": None,
            "!=": "$ne",
            "?": None,
            "!?": "$not",
        }
        value = node.value
        if node.operator.endswith("?"):
            value = re.compile(re.escape(node.value), re.IGNORECASE)
        elif node.field in ("pichash", "offset") or node.field.endswith("_id") or "num_" in node.field:
            try:
                value = int(value, 0)
            except Exception:
                pass
        mongo_operator = operator_to_mongo[node.operator]
        if node.field == "pichash" and mongo_operator in ("$lt", "$lte", "$gt", "$gte"):
            # pichashes are stored as hex strings, on which range comparisons are not meaningful
            raise ValueError("Range operators are not supported for the field 'pichash'.")
        if mongo_operator is None:
            condition = {node.field: value}
        else:
            condition = {node.field: {mongo_operator: value}}
        if node.field == "pichash":
            if node.operator.endswith("?"):
                # a substring search is a regex, which cannot be hex-encoded - it is matched against
                # the stored representation instead, so only the field has to be renamed
                condition = {"_pichash": condition.pop("pichash")}
            else:
                MongoDbStorage._encodePichash(condition)
        return condition


def _hasSubstringCondition(tree: NodeType, field: str) -> bool:
    """True when the (resolved) tree holds a substring condition on the field."""
    if isinstance(tree, SearchConditionNode):
        return tree.field == field and tree.operator.endswith("?")
    if isinstance(tree, (AndNode, OrNode)):
        return any(_hasSubstringCondition(child, field) for child in tree.children)
    if isinstance(tree, NotNode):
        return _hasSubstringCondition(tree.child, field)
    return False


class MongoDbStorage(StorageInterface):
    _DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

    # A substring search on function_name is answered through the distinct names of the
    # collection when there are at most this many (fkie-cad/mcritweb#76); above the cap the search
    # falls back to the regex, unbounded as before. Listing them is a $group that stops one past
    # the cap: tens of milliseconds for 5,000 names over two million functions, but ~0.9 s to find
    # out that 11.6M functions carry more than 10,000 (314,144) names. That negative verdict is
    # therefore remembered per (collection, field) for _OVER_CAP_TTL seconds, see _getDistinctValues.
    _DISTINCT_VALUES_CAP = 10000
    # and at most this many bytes of them in total: the matching values go into one $in, which
    # must stay well inside MongoDB's 16 MiB command limit even when they are long mangled symbols
    _DISTINCT_VALUES_MAX_BYTES = 1 << 20
    # Only "over the cap" is cached, never the values: a corpus that grew past the cap stays past it,
    # so no write has to invalidate anything and each worker process may keep its own copy. Should
    # deletions bring it back under, the cost is the regex - today's behaviour - until the verdict
    # expires and the scan runs again.
    _OVER_CAP_TTL = 3600
    # Inverted index over picblockhashes: {_id: <block hash>, sample_ids: [<sample_id>, ...]}.
    # getUniqueBlocks only ever asks "does this block hash occur outside the requested samples?",
    # so the sample list is all it needs - deliberately not function ids or offsets, which would
    # multiply the document count by the ~5.6 blocks each function averages, and not family ids,
    # which would make a family reassignment invalidate the index for no gain.
    _PICBLOCKHASH_INDEX_COLLECTION = "picblockhashes"
    # settings flag: the index is only trusted when something has vouched that it is complete.
    # An index that is merely *present* is not safe to read - a half-filled one would report
    # blocks as unique that are not.
    _PICBLOCKHASH_INDEX_SETTING = "picblockhash_index_complete"
    # candidate hashes per $in when querying the index; the same slicing the band index uses
    _PICBLOCKHASH_INDEX_QUERY_SLICE = 20000

    # Maps a function_id back to the sample that holds it, without reading the function.
    # Candidate generation returns function ids; deciding *which samples* those belong to is
    # the whole of the shortlist stage, and asking the functions collection would mean one
    # lookup per candidate - the very cost the shortlist exists to avoid. Function ids are
    # allocated from a counter in one contiguous run per sample, so one [first, last] pair
    # per sample describes the whole mapping: ~40 B per sample, i.e. ~40 MB at a million
    # samples, and a searchsorted away from an answer.
    _FUNCTION_RANGE_COLLECTION = "function_ranges"
    # Same reasoning as the picblockhash flag: a partially filled range index would silently
    # attribute functions to the wrong sample, so it is only read once something vouches for it.
    _FUNCTION_RANGE_SETTING = "function_range_index_complete"
    # settings flag: band documents carry a df (posting-list length) that a (band_hash, df)
    # index can filter on. Only trusted once something has vouched for it, because a missing or
    # stale df would silently hide posting lists from candidate generation.
    _BAND_DF_SETTING = "band_df_index_complete"
    # How many corpus functions hold each pichash. MINHASH_PICHASH_MAX_MATCHES needs that count
    # to decide whether a hash is too common to be worth fetching, and counting it by grouping
    # over the functions collection touches one index entry per holder - which is exactly the
    # cost the cutoff exists to avoid, paid in full for every hash it then rejects. Measured on
    # a real corpus: pichash lookup still grew 1.80x for a 1.49x corpus increase *with* the
    # cutoff on. One small document per distinct pichash turns that into an indexed probe.
    _PICHASH_COUNT_COLLECTION = "pichash_counts"
    _PICHASH_COUNT_SETTING = "pichash_count_index_complete"
    # Count documents per insert during a rebuild. Independent of the partition size, which
    # bounds how much of the *functions* index one pass reads: a partition of pichash-index
    # keys collapses to as few as one count document, so tying the two together would make the
    # write batch follow how repetitive the corpus is rather than how large a write should be.
    _PICHASH_COUNT_WRITE_BATCH = 10000

    _database: Optional["Database"]

    def __init__(self, config: "McritConfig") -> None:
        super().__init__(config)  # sets config
        bucket_size = self._bandBucketSize()
        df_cutoff = int(getattr(self._storage_config, "STORAGE_BAND_DF_CUTOFF", 0) or 0)
        if bucket_size and df_cutoff > bucket_size:
            # only bucket 0 carries df, so a spilled hash whose total still fits under the cutoff
            # would be served as bucket 0 alone - a truncated posting list with no error
            raise ValueError(f"STORAGE_BAND_DF_CUTOFF ({df_cutoff}) must not exceed STORAGE_BAND_BUCKET_SIZE ({bucket_size}).")
        self.blockhasher = BlockHasher()
        self._database = None
        # guards the lazy initialisation in _getDb(); a threading.Lock is per-process, which is
        # what we want: forking servers (gunicorn) fork before the first request, so every child
        # inherits an unlocked copy and synchronises its own threads independently
        self._database_lock = threading.Lock()
        # (collection, field) -> time.monotonic() at which it was found over the distinct-values cap
        self._over_cap_since: Dict[Tuple[str, str], float] = {}

    def _getDb(self):
        # because of gunicorn and forking workers, we want to delay creation of MongoClient until actual usage and avoid it within __init__()
        if self._database is None:
            # double-checked locking: the server shares one storage object across all request
            # threads, and two threads arriving cold must not both construct a MongoClient (#109).
            # _initDb assigns self._database before ensuring indexes, so its re-entrant _getDb
            # calls take the fast path above and do not deadlock on this non-reentrant lock.
            with self._database_lock:
                if self._database is None:
                    self._initDb(
                        self._storage_config.STORAGE_SERVER,
                        self._storage_config.STORAGE_PORT,
                        self._storage_config.STORAGE_MONGODB_DBNAME,
                        self._storage_config.STORAGE_MONGODB_USERNAME,
                        self._storage_config.STORAGE_MONGODB_PASSWORD,
                        self._storage_config.STORAGE_MONGODB_FLAGS,
                    )
        return self._database

    def _initDb(self, server, port, db_name, username="", password="", flags=""):
        userpw_url = f"{username}:{password}@" if username and len(username) > 0 and password and len(password) > 0 else ""
        port_url = f":{port}" if port and len(port) > 0 else ""
        flags_url = f"?{flags}" if flags and len(flags) > 0 else ""

        mongo_uri = f"mongodb://{userpw_url}{server}{port_url}/{db_name}{flags_url}"

        self._database = MongoClient(mongo_uri, connect=False)[db_name]
        self._ensureIndexAndUnknownFamily()

    def hasInlineXcfgRemaining(self) -> Optional[bool]:
        """Whether the functions collection still carries inline disassembly (#137).

        True = pre-split shape (migrate_xcfg_split not run or not finished), False = migrated,
        None = could not be determined within the time budget. Surfaced through /status rather
        than probed in a constructor: the probe is a COLLSCAN that exits early exactly when
        things are broken and scans the whole collection when healthy, so it must never sit on
        the job path - SpawningWorker runs each job as a fresh process, which would pay the scan
        (and the WiredTiger cache churn of walking the collection) once per job.

        Decision order:
        1. the terminal marker migrate_xcfg_split writes when an inplace phase drains is O(1)
           and definitive for "migrated" - without it, the only honest answer to the healthy
           case would be a full scan (~33s post-split), which no /status poll should pay
        2. otherwise the bounded probe decides; it exits at the first hit in milliseconds
           precisely in the un-migrated case, where the answer is needed

        Only an inplace completion counts: copy mode migrating into a rehearsal target says
        nothing about this instance. Instances migrated by a script version older than the
        marker have no marker and fall through to the probe returning None (field omitted).
        """
        completed_marker = self._getDb()["c1_migration_state"].find_one({"_id": "inplace:functions:xcfg", "finished_at": {"$exists": True}}, {"_id": 1})
        if completed_marker:
            return False
        try:
            return self._getDb().functions.find_one({"_xcfg": {"$exists": True}}, {"_id": 1}, max_time_ms=5000) is not None
        except Exception:
            LOGGER.warning("Could not determine whether inline _xcfg remains (probe did not finish within 5000 ms).")
            return None

    def _ensureIndexAndUnknownFamily(self) -> None:
        if "settings" not in self._getDb().list_collection_names():
            self._getDb()["settings"].insert_one({"mcrit_db_id": str(uuid.uuid4()), "db_state": 0})
        # A database holding no functions has a trivially complete picblockhash index, so a fresh
        # instance - or one just cleared - maintains it from the first submit and never needs a
        # rebuild. An existing database carrying functions does *not* get the flag when it upgrades
        # into this code: it keeps using the scan until an operator rebuilds. Correctness first;
        # an index that is merely present would report blocks as unique that are not.
        # find_one is used rather than a count because this runs on every storage construction.
        # Same argument for the two indexes two-stage matching depends on, and the same
        # find_one rather than a count, since this runs on every storage construction. Both are
        # maintained from the first write - addSmdaReport records the sample's function id span,
        # and _updateBands keeps each posting list's df - so a database with no functions has
        # them trivially complete. Without this a fresh instance would maintain both correctly
        # and still take the fallback path forever, because nothing had vouched for them.
        is_empty = self._getDb()["functions"].find_one({}, {"_id": 1}) is None
        if not self._isPicBlockHashIndexComplete() and is_empty:
            self._setPicBlockHashIndexComplete(True)
        if is_empty:
            if not self.isFunctionRangeIndexComplete():
                self._setFunctionRangeIndexComplete(True)
            if not self.isBandDfIndexComplete():
                self._setBandDfIndexComplete(True)
            if not self.isPicHashCountIndexComplete():
                self._setPicHashCountIndexComplete(True)
        self._getDb()["samples"].create_index("sample_id")
        self._getDb()["samples"].create_index("sha256")
        self._getDb()["samples"].create_index("family_id")
        # the stale-minhash count on /status is a distinct plus a count over this field (#142)
        self._getDb()["samples"].create_index("minhash_smda_version")
        self._getDb()["families"].create_index("family_id")
        self._getDb()["families"].create_index("family_name")
        self._getDb()["functions"].create_index("function_id")
        self._getDb()["functions"].create_index("sample_id")
        self._getDb()["functions"].create_index("family_id")
        self._getDb()["functions"].create_index("function_name")
        self._getDb()["functions"].create_index("_pichash")
        self._getDb()["functions"].create_index("_picblockhashes.hash")
        # Searches sorted by a field other than the id (fkie-cad/mcritweb#59): the search cursor sorts by that
        # field and breaks ties by the id, in the same direction, so one compound index per
        # sortable field serves both directions by being walked backwards. Without it the
        # server sorts every filtered document in memory, which is where the reported 10x
        # goes. The fields are the ones the search results can be sorted by in MCRITweb.
        for field in ("family_name", "num_samples", "num_library_samples", "num_functions"):
            self._getDb()["families"].create_index([(field, 1), ("family_id", 1)])
        for field in ("family_id", "family", "filename", "version", "bitness", "sha256", "statistics.num_functions"):
            self._getDb()["samples"].create_index([(field, 1), ("sample_id", 1)])
        for field in ("family_id", "sample_id", "function_name", "offset", "num_instructions", "num_blocks"):
            self._getDb()["functions"].create_index([(field, 1), ("function_id", 1)])
        # stored without guarantee of existence
        self._getDb()["query_samples"].create_index("sample_id")
        self._getDb()["query_samples"].create_index("sha256")
        self._getDb()["query_functions"].create_index("function_id")
        self._getDb()["query_functions"].create_index("sample_id")
        # ensure that their counters are at least 1, so that they never contain items with sample_id/function_id 0
        # the name-only filter with $max is idempotent: it matches an existing counter instead of upserting a duplicate (#105)
        self._getDb().counters.update_one({"name": "query_samples"}, {"$max": {"value": 1}}, upsert=True)
        self._getDb().counters.update_one({"name": "query_functions"}, {"$max": {"value": 1}}, upsert=True)
        self._getDb()["matches"].create_index("match_id")
        self._getDb()["candidates"].create_index("function_id")
        # duplicates must be removed before the unique index can be built on databases polluted by #105
        self._removeCounterDuplicates()
        # counters.name must be unique so that _useCounterBulk can never increment a stale duplicate and re-issue ids;
        # older instances created this index without the constraint, in which case it has to be replaced
        counter_indexes = self._getDb()["counters"].index_information()
        if "name_1" in counter_indexes and not counter_indexes["name_1"].get("unique", False):
            self._getDb()["counters"].drop_index("name_1")
        self._getDb()["counters"].create_index("name", unique=True)
        self._getDb()["logs"].create_index("username")
        for band_id in range(self._storage_config.STORAGE_NUM_BANDS):
            self._getDb()["band_%d" % band_id].create_index("band_hash")
            self._getDb()["band_%d" % band_id].create_index([("band_hash", 1), ("df", 1)])
            self._getDb()["band_%d" % band_id].create_index([("band_hash", 1), ("bucket", 1)])
        # Add Family "" (family_id 0) if it is not already in storage. Two processes can bootstrap
        # the same database concurrently (e.g. server and worker), so instead of check-then-addFamily
        # (which would hand out two different family_ids for "") this uses a single upsert keyed on
        # family_id. First push the families counter past 0 so addFamily can never hand out id 0.
        self._getDb().counters.find_one_and_update(filter={"name": "families"}, update={"$max": {"value": 1}}, upsert=True)
        unknown_family = FamilyEntry(family_name="", family_id=0)
        upsert_result = self._getDb()["families"].update_one({"family_id": 0}, {"$setOnInsert": unknown_family.toDict()}, upsert=True)
        if upsert_result.upserted_id is not None:
            self._updateDbState()
        stored_unknown_family = self.getFamily(0)
        assert stored_unknown_family is not None and stored_unknown_family.family_name == ""

    def _removeCounterDuplicates(self) -> None:
        # one-off cleanup for #105: releases before 1.6.2 upserted a fresh {name, value: 1} document on every
        # process start once the counter had advanced - only the highest-valued document per name is the real counter.
        # Every name present is checked, not just the two that #105 duplicated: all counters are written by name-only
        # upserts (samples/functions/families here, "job" from mongoqueue, which shares this collection whenever queue
        # and storage use the same database - they do by default), so a concurrent first-touch can duplicate any of
        # them. A single leftover duplicate under any name would fail the unique index build below and take startup
        # down with it, so the cleanup has to cover the whole collection rather than a fixed list.
        for name in self._getDb().counters.distinct("name"):
            counter_documents = list(self._getDb().counters.find({"name": name}).sort("value", -1))
            if len(counter_documents) <= 1:
                continue
            duplicate_ids = [document["_id"] for document in counter_documents[1:]]
            delete_result = self._getDb().counters.delete_many({"_id": {"$in": duplicate_ids}})
            LOGGER.info("Removed %d duplicate counter documents for '%s' (#105).", delete_result.deleted_count, name)

    ###############################################################################
    # Generic database functionality and logging
    ###############################################################################

    def _getCurrentTimestamp(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.UTC).replace(tzinfo=None)

    def _convertTimestampToString(self, timestamp) -> str:
        return datetime.datetime.fromtimestamp(timestamp).strftime(self._DATETIME_FORMAT)

    def _convertDatetimeToString(self, dt: datetime.datetime) -> str:
        if isinstance(dt, datetime.datetime):
            return dt.strftime(self._DATETIME_FORMAT)

    def _convertStringToDatetime(self, date_string: str) -> datetime.datetime:
        if isinstance(date_string, str):
            return datetime.datetime.strptime(date_string, self._DATETIME_FORMAT)

    def _toBinary(self, obj):
        """Checks data to be inserted to the database for non-UTF8 strings and escapes these as BSON.Binary"""
        if isinstance(obj, str):
            return obj
            # return bson.binary.Binary(obj)
        elif isinstance(obj, dict):
            return {k: self._toBinary(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._toBinary(element) for element in obj]
        elif isinstance(obj, tuple):
            return tuple(self._toBinary(element) for element in obj)
        elif isinstance(obj, set):
            return [self._toBinary(element) for element in sorted(obj)]
        else:
            return obj

    def _dbInsert(self, collection: str, data: Dict):
        try:
            insert_result = self._getDb()[collection].insert_one(self._toBinary(data))
            if insert_result.acknowledged:
                return insert_result.inserted_id
            return None
        except Exception:
            self._dbLogError(
                'Database insert for collection "%s" failed.' % collection,
                details={"traceback": traceback.format_exc().split("\n")},
            )
            raise ValueError("Database insert failed.")

    # MongoDB refuses a document over this size; pymongo raises DocumentTooLarge for the whole
    # batch before anything is written (#42)
    _MAX_DOCUMENT_BYTES = 16 * 1024 * 1024
    # collections whose documents are droppable when oversized: a function without its stored
    # disassembly is still a function, one that cannot be inserted takes the whole sample down
    _DROPPABLE_WHEN_OVERSIZED = ("xcfg", "query_xcfg")

    @staticmethod
    def _describeDocument(document: Dict) -> Dict[str, Any]:
        keys = ("_id", "function_id", "sample_id", "family_id", "sha256", "offset")
        return {key: document[key] for key in keys if key in document}

    def _splitOversizedDocuments(self, documents: List[Dict]) -> Tuple[List[Dict], List[Tuple[Dict, int]]]:
        fitting, oversized = [], []
        for document in documents:
            size = len(bson_encode(document))
            if size > self._MAX_DOCUMENT_BYTES:
                oversized.append((document, size))
            else:
                fitting.append(document)
        return fitting, oversized

    @staticmethod
    def _isTooLargeError(error: Exception) -> bool:
        """pymongo rejects a clearly oversized document itself (DocumentTooLarge); one just over
        the limit reaches the server, which answers a write error (code 2 or 10334, "too large")."""
        if isinstance(error, DocumentTooLarge):
            return True
        if isinstance(error, BulkWriteError):
            return any(write_error.get("code") in (2, 10334) or "too large" in str(write_error.get("errmsg", "")) for write_error in error.details.get("writeErrors", []))
        return False

    def _dbInsertMany(self, collection: str, data: List["Dict"]):
        if len(data) == 0:
            return []
        documents = [self._toBinary(document) for document in data]
        # measure before the first wire batch goes out: once pymongo has sent earlier batches
        # and then hits an oversized document, DocumentTooLarge carries no nInserted, so the
        # committed documents could not be told apart from the rest (review of #42)
        if collection in self._DROPPABLE_WHEN_OVERSIZED:
            documents, oversized = self._splitOversizedDocuments(documents)
            if oversized:
                offenders = [{"document": self._describeDocument(document), "bytes": size} for document, size in oversized]
                self._dbLogError(
                    'Database insert_many for collection "%s": %d document(s) exceed the 16 MiB limit and are dropped.' % (collection, len(oversized)),
                    details={"oversized": offenders, "dropped": True},
                )
                LOGGER.warning("Dropping the disassembly of %d function(s) over MongoDB's 16 MiB document limit: %s", len(oversized), offenders)
                if not documents:
                    return []
        try:
            insert_result = self._getDb()[collection].insert_many(documents)
            if insert_result.acknowledged:
                return insert_result.inserted_ids
            return None
        except (DocumentTooLarge, BulkWriteError) as error:
            if not self._isTooLargeError(error):
                self._dbLogError('Database insert_many for collection "%s" failed.' % collection, details={"traceback": traceback.format_exc().split("\n")})
                raise ValueError("Database insert failed.")
            # an ordered insert stops at the first oversized document: what came before it is
            # in, the rest is not. Find out which documents are too large and say so, since
            # MongoDB's own message only carries a size (#42)
            inserted = documents[: error.details.get("nInserted", 0)] if isinstance(error, BulkWriteError) else []
            if "_id" in documents[0]:
                # whatever earlier wire batches committed is in; never retry those ids
                present = set(document["_id"] for document in self._getDb()[collection].find({"_id": {"$in": [document["_id"] for document in documents]}}, {"_id": 1}))
                inserted = [document for document in documents if document["_id"] in present]
            remaining = [document for document in documents if document not in inserted]
            fitting, oversized = self._splitOversizedDocuments(remaining)
            offenders = [{"document": self._describeDocument(document), "bytes": size} for document, size in oversized]
            self._dbLogError(
                'Database insert_many for collection "%s" failed: %d document(s) exceed the 16 MiB limit.' % (collection, len(oversized)),
                details={"oversized": offenders, "dropped": collection in self._DROPPABLE_WHEN_OVERSIZED},
            )
            if collection not in self._DROPPABLE_WHEN_OVERSIZED:
                raise ValueError(f"Database insert failed: {len(oversized)} document(s) for '{collection}' exceed MongoDB's 16 MiB document limit ({offenders})")
            LOGGER.warning("Dropping the disassembly of %d function(s) over MongoDB's 16 MiB document limit: %s", len(oversized), offenders)
            return [document["_id"] for document in inserted] + (self._dbInsertMany(collection, fitting) or [])
        except Exception:
            self._dbLogError(
                'Database insert_many for collection "%s" failed.' % collection,
                details={"traceback": traceback.format_exc().split("\n")},
            )
            raise ValueError("Database insert failed.")

    def _dbQuery(self, collection: str, query: Dict, find_one: bool = False):
        try:
            if find_one:
                return self._getDb()[collection].find_one(query)
            else:
                return self._getDb()[collection].find(query)
        except Exception:
            self._dbLogError(
                'Database query for collection "%s" failed.' % collection,
                details={"traceback": traceback.format_exc().split("\n")},
            )
            raise ValueError("Database query failed.")

    def dbLogEvent(self, event_msg, username=None, details=None):
        if details is None:
            details = {}
        self._dbLog("event", event_msg, username, details)

    def _dbLogError(self, error_msg, username=None, details=None):
        if details is None:
            details = {}
        self._dbLog("error", error_msg, username, details)

    def _dbLog(self, log_type, log_msg, log_username, log_details):
        if log_username is None:
            log_username = "mcrit/internal"
        record = {"ts": self._getCurrentTimestamp(), "%s_msg" % log_type: log_msg, "username": log_username, "%s_details" % log_type: log_details}
        self._getDb()[log_type].insert_one(self._toBinary(record))

    def _useCounterBulk(self, name: str, num_counts: int) -> Iterable[int]:
        assert num_counts >= 0
        if num_counts == 0:
            return []
        query_result = self._getDb().counters.find_one_and_update(filter={"name": name}, update={"$inc": {"value": num_counts}}, upsert=True)
        first_count = 0
        if query_result is not None:
            first_count = query_result["value"]
        return range(first_count, first_count + num_counts)

    def _useCounter(self, name: str) -> int:
        return next(iter(self._useCounterBulk(name, 1)))

    def _updateDbState(self):
        result = self._getDb().settings.find_one_and_update({}, {"$inc": {"db_state": 1}})
        result = self._getDb().settings.find_one_and_update({}, {"$set": {"db_timestamp": self._getCurrentTimestamp()}}, upsert=True)
        if result is None:
            raise Exception("Database does not have a db_state field")
        else:
            return result["db_state"]

    def _getDbState(self):
        result = self._getDb().settings.find_one({})
        if result is None:
            raise Exception("Database does not have a state field yet")
        elif "db_state" in result:
            return result["db_state"]

    def _getDbTimestamp(self):
        result = self._getDb().settings.find_one({})
        if result is None:
            raise Exception("Database does not have a state field yet")
        elif "db_timestamp" in result:
            return result["db_timestamp"]

    def updateDbCleanupTimestamp(self):
        result = self._getDb().settings.find_one_and_update({}, {"$inc": {"db_state": 1}})
        result = self._getDb().settings.find_one_and_update({}, {"$set": {"db_cleanup_timestamp": self._getCurrentTimestamp()}}, upsert=True)
        if result is None:
            raise Exception("Database does not have a db_state field")
        else:
            return result["db_cleanup_timestamp"]

    def getDbCleanupTimestamp(self):
        result = self._getDb().settings.find_one({})
        if result is None:
            raise Exception("Database does not have a state field yet")
        elif "db_cleanup_timestamp" in result:
            return result["db_cleanup_timestamp"]

    ###############################################################################
    # Conversion
    ###############################################################################

    @staticmethod
    def _encodeXcfg(function_dict: Dict, delete_old: bool = True) -> None:
        if "xcfg" in function_dict:
            function_dict["_xcfg"] = json.dumps(function_dict["xcfg"])
            if delete_old:
                del function_dict["xcfg"]

    @staticmethod
    def _decodeXcfg(function_dict: Dict, delete_old: bool = True) -> None:
        if "_xcfg" in function_dict:
            function_dict["xcfg"] = json.loads(function_dict["_xcfg"])
            if delete_old:
                del function_dict["_xcfg"]

    def _xcfgCollectionFor(self, function_id: int) -> str:
        """Disassembly lives beside the function collection it belongs to, not mixed into one
        collection keyed by sign (#137). Query functions carry negative ids."""
        return "query_xcfg" if function_id < 0 else "xcfg"

    def _extractXcfgDocuments(self, function_dicts: List[Dict]) -> Dict[str, List[Dict]]:
        """Move `_xcfg` out of function documents into standalone documents, keyed by
        collection name (#137).

        `_id` IS the function id: no secondary index is needed, the lookup is the primary key,
        and per-document overhead stays minimal. The function documents are mutated in place so
        the caller inserts them without the blob.
        """
        extracted = {}
        for function_dict in function_dicts:
            blob = function_dict.pop("_xcfg", None)
            if not blob:
                continue
            collection = self._xcfgCollectionFor(function_dict["function_id"])
            extracted.setdefault(collection, []).append({"_id": function_dict["function_id"], "_xcfg": blob})
        return extracted

    def _insertXcfgDocuments(self, function_dicts: List[Dict]) -> None:
        for collection, documents in self._extractXcfgDocuments(function_dicts).items():
            self._dbInsertMany(collection, documents)

    def _fetchXcfgBlobs(self, function_ids: List[int]) -> Dict[int, str]:
        """One `$in` per collection for a whole batch of ids - never one query per function,
        which is the N+1 shape #111 removed elsewhere."""
        blobs = {}
        for collection in ("xcfg", "query_xcfg"):
            ids = [function_id for function_id in function_ids if self._xcfgCollectionFor(function_id) == collection]
            if not ids:
                continue
            for document in self._getDb()[collection].find({"_id": {"$in": ids}}):
                blobs[document["_id"]] = document["_xcfg"]
        return blobs

    def _attachXcfgBlobs(self, function_documents: List[Dict]) -> None:
        """Put the split-out disassembly back onto function documents, one query per batch.

        Used by the readers that carried `_xcfg` before the split, so their callers keep seeing
        the same shape: minhash computation (Worker.calculateMinHashes) and the code-reference
        extraction in match reports both read FunctionEntry.xcfg. Missing blobs decode to `{}`,
        never None - see getFunctionById for why that distinction matters.

        An inline `_xcfg` is a fallback, not something to discard: this code ships before
        migrate_xcfg_split has run, so an un-migrated or half-migrated instance still carries
        blobs inside function documents. Overwriting those with `{}` would silently serve empty
        disassembly to minhash computation and match reports.
        """
        if not function_documents:
            return
        blobs = self._fetchXcfgBlobs([document["function_id"] for document in function_documents])
        for function_document in function_documents:
            function_document["_xcfg"] = blobs.get(function_document["function_id"]) or function_document.get("_xcfg") or "{}"

    def _deleteXcfgForFunctionIds(self, function_ids: List[int]) -> None:
        for collection in ("xcfg", "query_xcfg"):
            ids = [function_id for function_id in function_ids if self._xcfgCollectionFor(function_id) == collection]
            if ids:
                self._getDb()[collection].delete_many({"_id": {"$in": ids}})

    @staticmethod
    def _encodePichash(function_dict: Dict, delete_old: bool = True) -> None:
        if "pichash" in function_dict:
            value = function_dict["pichash"]
            # search conditions wrap the value in an operator dict, e.g. {"$ne": 0x1234}
            if isinstance(value, dict):
                function_dict["_pichash"] = {k: hex(v) if isinstance(v, int) else v for k, v in value.items()}
            else:
                function_dict["_pichash"] = hex(value)
            if delete_old:
                del function_dict["pichash"]
        if "picblockhashes" in function_dict:
            converted_entries = []
            for entry in function_dict["picblockhashes"]:
                converted_entry = dict(**entry)
                converted_entry["hash"] = hex(converted_entry["hash"])
                # use two-complement to convert unit64 to int64 and vice versa
                converted_entry["offset"] = encode_two_complement(converted_entry["offset"])
                converted_entries.append(converted_entry)
            function_dict["_picblockhashes"] = converted_entries
            if delete_old:
                del function_dict["picblockhashes"]

    @staticmethod
    def _decodePichash(function_dict: Dict, delete_old: bool = True) -> None:
        if "_pichash" in function_dict:
            value = function_dict["_pichash"]
            if isinstance(value, dict):
                function_dict["pichash"] = {k: int(v, 16) if isinstance(v, str) else v for k, v in value.items()}
            else:
                function_dict["pichash"] = int(value, 16)
            if delete_old:
                del function_dict["_pichash"]
        if "_picblockhashes" in function_dict:
            converted_entries = []
            for entry in function_dict["_picblockhashes"]:
                converted_entry = dict(**entry)
                converted_entry["hash"] = int(converted_entry["hash"], 16)
                # use two-complement to convert unit64 to int64 and vice versa
                converted_entry["offset"] = decode_two_complement(converted_entry["offset"])
                converted_entries.append(converted_entry)
            function_dict["picblockhashes"] = converted_entries
            if delete_old:
                del function_dict["_picblockhashes"]

    @staticmethod
    def _encodeFunction(function_dict: Dict, delete_old: bool = True) -> None:
        MongoDbStorage._encodePichash(function_dict, delete_old=delete_old)
        MongoDbStorage._encodeXcfg(function_dict, delete_old=delete_old)

    @staticmethod
    def _decodeFunction(function_dict: Dict, delete_old: bool = True) -> None:
        MongoDbStorage._decodePichash(function_dict, delete_old=delete_old)
        MongoDbStorage._decodeXcfg(function_dict, delete_old=delete_old)

    ###############################################################################
    # Interface
    ###############################################################################

    def getContent(self) -> Dict[str, Any]:
        raise NotImplementedError

    def setContent(self, content: Dict[str, Any]) -> None:
        raise NotImplementedError

    # NOTE this actually only gets used in MemoryStorage / MatchingCache
    def getMinHashByFunctionId(self, function_id: int) -> Optional[bytes]:
        function = self.getFunctionById(function_id)
        if function is None:
            return None
        return function.minhash

    # NOTE this actually only gets used in MemoryStorage / MatchingCache
    def getSampleIdByFunctionId(self, function_id: int) -> Optional[int]:
        if function_id < 0:
            function_document = self._getDb().query_functions.find_one({"function_id": function_id})
        else:
            function_document = self._getDb().functions.find_one({"function_id": function_id})
        if function_document is None:
            return None
        return function_document["sample_id"]

    def getSampleIdsByFunctionIds(self, function_ids: List[int]) -> Dict[int, int]:
        sample_ids = {}
        positive_function_ids = [function_id for function_id in set(function_ids) if function_id >= 0]
        negative_function_ids = [function_id for function_id in set(function_ids) if function_id < 0]
        for collection_name, collection_ids in (
            ("functions", positive_function_ids),
            ("query_functions", negative_function_ids),
        ):
            if not collection_ids:
                continue
            for sliced_ids in zip_longest(*[iter(collection_ids)] * 500000):
                query_function_ids = [function_id for function_id in sliced_ids if function_id is not None]
                for function_document in self._getDb()[collection_name].find(
                    {"function_id": {"$in": query_function_ids}},
                    {"_id": 0, "function_id": 1, "sample_id": 1},
                ):
                    sample_ids[function_document["function_id"]] = function_document["sample_id"]
        return sample_ids

    def deleteSample(self, sample_id: int) -> bool:
        sample_entry = self.getSampleById(sample_id)
        if sample_entry is None:
            return False
        if sample_id < 0:
            # remove functions
            query_function_ids = [document["function_id"] for document in self._getDb().query_functions.find({"sample_id": sample_id}, {"function_id": 1, "_id": 0})]
            self._deleteXcfgForFunctionIds(query_function_ids)
            self._getDb().query_functions.delete_many({"sample_id": sample_id})
            # remove sample
            self._getDb().query_samples.delete_one({"sample_id": sample_id})
            return True
        function_minhashes = self._getFunctionMinHashesBySampleId(sample_id)
        self._pullBandEntries(function_minhashes)
        # drop this sample from the picblockhash index while its functions still exist to be read
        self._removeSampleFromPicBlockHashIndex(sample_id)

        # remove functions
        self._deleteXcfgForFunctionIds([function_minhash["function_id"] for function_minhash in function_minhashes])
        num_functions_deleted = self._getDb().functions.delete_many({"sample_id": sample_id}).deleted_count
        # remove sample
        num_samples_deleted = self._getDb().samples.delete_one({"sample_id": sample_id}).deleted_count
        # update family stats by what was actually removed, not by what the sample claimed (#151)
        self._updateFamilyStats(sample_entry.family_id, -num_samples_deleted, -num_functions_deleted, -int(sample_entry.is_library and num_samples_deleted))
        self._deleteFamilyIfEmpty(sample_entry.family_id)
        self._updateDbState()
        return True

    def _deleteFamilyIfEmpty(self, family_id: int) -> None:
        # judged by the samples that exist, not by a counter that may have drifted (#151)
        if family_id != 0 and self._getDb().samples.count_documents({"family_id": family_id}, limit=1) == 0:
            self._getDb().families.delete_one({"family_id": family_id})

    def _ensureFamilyDocument(self, family_id: int, family_name: str) -> None:
        """Create the family document for an id that samples reference but no document describes.

        Imports carry family ids from the exporting instance; a sample whose family document is
        missing would otherwise have its statistics increments silently discarded (#151).
        """
        self._getDb().families.update_one(
            {"family_id": family_id},
            {"$setOnInsert": FamilyEntry(family_name=family_name, family_id=family_id).toDict()},
            upsert=True,
        )
        # the id came from elsewhere (an import): the counter must never hand it out again
        self._getDb().counters.update_one({"name": "families"}, {"$max": {"value": family_id + 1}}, upsert=True)

    def _pullBandEntries(self, function_minhashes: List[Dict[str, Any]]) -> int:
        """Take the given functions' minhashes out of the band index; how many had one."""
        # collect all band entries that need updating and pull all function_ids at once.
        # might need to batch this into slices of function_ids again
        minhashes_to_remove: Dict[int, Dict[int, List[int]]] = {band_number: {} for band_number in range(self._storage_config.STORAGE_NUM_BANDS)}
        num_hashed = 0
        for function_minhash in function_minhashes:
            minhash = self._getMinHashFromStorage(function_minhash)
            # remove minhash entries, if necessary
            if not minhash or not minhash.hasMinHash():
                continue
            num_hashed += 1
            band_hashes = self.getBandHashesForMinHash(minhash)
            for band_number, band_hash in sorted(band_hashes.items()):
                if band_hash not in minhashes_to_remove[band_number]:
                    minhashes_to_remove[band_number][band_hash] = []
                minhashes_to_remove[band_number][band_hash].append(function_minhash["function_id"])
        self._updateBands(minhashes_to_remove, method="pull")
        return num_hashed

    def deleteMinHashesForSample(self, sample_id: int) -> int:
        if not self.isSampleId(sample_id) or sample_id < 0:
            return 0
        function_minhashes = self._getFunctionMinHashesBySampleId(sample_id)
        num_hashed = self._pullBandEntries(function_minhashes)
        self._getDb().functions.update_many({"sample_id": sample_id, "minhash": {"$ne": ""}}, {"$set": {"minhash": "", "minhash_shingle_composition": {}}})
        self._getDb().samples.update_one({"sample_id": sample_id}, {"$unset": {"minhash_smda_version": ""}})
        return num_hashed

    def setMinHashVersionForSamples(self, smda_version: str, sample_ids: Optional[List[int]] = None) -> None:
        query = {} if sample_ids is None else {"sample_id": {"$in": list(sample_ids)}}
        self._getDb().samples.update_many(query, {"$set": {"minhash_smda_version": smda_version}})

    def _staleMinHashVersionQuery(self, threshold_version: str) -> Dict[str, Any]:
        """Samples carry few distinct recorded versions, so compare those instead of every document."""
        threshold = version.parse(threshold_version)
        recorded = self._getDb().samples.distinct("minhash_smda_version")
        stale_values = [value for value in recorded if self._isStaleMinHashVersion(value, threshold)]
        return {"$or": [{"minhash_smda_version": {"$exists": False}}, {"minhash_smda_version": {"$in": stale_values}}]}

    def getSamplesWithStaleMinHashes(self, threshold_version: str) -> List[int]:
        query = self._staleMinHashVersionQuery(threshold_version)
        return sorted(document["sample_id"] for document in self._getDb().samples.find(query, {"sample_id": 1, "_id": 0}))

    def countSamplesWithStaleMinHashes(self, threshold_version: str) -> int:
        return self._getDb().samples.count_documents(self._staleMinHashVersionQuery(threshold_version))

    def _updateFamilyStats(self, family_id, num_samples_inc, num_functions_inc, num_library_samples_inc):
        result = self._getDb().families.update_one(
            {"family_id": family_id},
            {
                "$inc": {
                    "num_samples": num_samples_inc,
                    "num_functions": num_functions_inc,
                    "num_library_samples": num_library_samples_inc,
                },
            },
        )
        if result.matched_count == 0:
            # an increment against a missing family is lost; say so instead of drifting quietly (#151)
            LOGGER.warning("Family %d has no document, its statistics update (%+d samples, %+d functions) was not applied.", family_id, num_samples_inc, num_functions_inc)

    def modifySample(self, sample_id: int, update_information: dict) -> bool:
        if not self.isSampleId(sample_id):
            return False
        sample_entry = self.getSampleById(sample_id)
        assert sample_entry is not None
        # statistics move by atomic increments: two concurrent relabels of samples in the same
        # family used to compute from the same pre-read document and lose one update (#151)
        is_library = sample_entry.is_library
        if "is_library" in update_information:
            new_is_library = bool(update_information["is_library"])
            # the counter follows the transition this write performs, not the value read
            # before it: two concurrent identical requests would otherwise both count
            transition = self._getDb().samples.update_one({"sample_id": sample_id, "is_library": {"$ne": new_is_library}}, {"$set": {"is_library": new_is_library}})
            if transition.modified_count:
                self._updateFamilyStats(sample_entry.family_id, 0, 0, 1 if new_is_library else -1)
            is_library = new_is_library
        if "family_name" in update_information:
            old_family_id = sample_entry.family_id
            family_name = update_information["family_name"]
            family_id = self.addFamily(family_name)
            if family_id != old_family_id:
                # gated on the family the sample was read in: of two concurrent moves only the
                # one that performs the transition adjusts the counters. The functions moved
                # are the ones still carrying the old family, whatever the sample's statistics say
                moved = self._getDb().samples.update_one({"sample_id": sample_id, "family_id": old_family_id}, {"$set": {"family_id": family_id, "family": family_name}})
                if moved.modified_count:
                    num_functions_moved = (
                        self._getDb().functions.update_many({"sample_id": sample_id, "family_id": old_family_id}, {"$set": {"family_id": family_id}}).modified_count
                    )
                    self._updateFamilyStats(old_family_id, -1, -num_functions_moved, -int(is_library))
                    self._updateFamilyStats(family_id, +1, num_functions_moved, int(is_library))
                    self._deleteFamilyIfEmpty(old_family_id)
            self._updateDbState()
        if "version" in update_information:
            self._getDb().samples.update_one({"sample_id": sample_id}, {"$set": {"version": update_information["version"]}})
        if "component" in update_information:
            self._getDb().samples.update_one({"sample_id": sample_id}, {"$set": {"component": update_information["component"]}})
        return True

    def modifyFunction(self, function_id: int, update_information: dict, username: Optional[str] = None) -> bool:
        if function_id < 0 or not self.isFunctionId(function_id):
            return False
        if "function_name" in update_information:
            function_name = update_information["function_name"]
            self._getDb().functions.update_one({"function_id": function_id}, {"$set": {"function_name": function_name}})
            if function_name:
                # the "not yet labelled so by this user" check is part of the update's filter,
                # so two concurrent identical requests cannot both push the label
                label_username = username or "anonymous"
                not_yet_labelled = {"function_id": function_id, "function_labels": {"$not": {"$elemMatch": {"username": label_username, "function_label": function_name}}}}
                self._getDb().functions.update_one(not_yet_labelled, {"$push": {"function_labels": FunctionLabelEntry(function_name, label_username).toDict()}})
        return True

    def modifyFamily(self, family_id: int, update_information: dict) -> bool:
        if not self.isFamilyId(family_id):
            return False
        old_family_info = self.getFamily(family_id)
        assert old_family_info is not None
        if "is_library" in update_information:
            self._getDb().samples.update_many({"family_id": family_id}, {"$set": {"is_library": update_information["is_library"]}})
            updated_count = old_family_info.num_samples if update_information["is_library"] else 0
            self._getDb().families.update_one({"family_id": family_id}, {"$set": {"num_library_samples": updated_count}})
            self._updateDbState()
        # the family's own name is not a rename: merging a family into itself deleted the document its
        # samples still point to, or for family 0 doubled its counters. Compared with the stored name
        # rather than looked up, because another family may carry the same name: recomputeFamilyStats
        # re-creates a missing family document under the name its samples carry
        if "family_name" in update_information and update_information["family_name"] != old_family_info.family_name:
            old_family_info = self.getFamily(family_id)
            family_name = update_information["family_name"]
            new_family_id = self.addFamily(family_name)
            new_family_info = self.getFamily(new_family_id)
            assert old_family_info is not None and new_family_info is not None
            new_num_samples = new_family_info.num_samples + old_family_info.num_samples
            new_num_functions = new_family_info.num_functions + old_family_info.num_functions
            new_num_lib_samples = new_family_info.num_library_samples + old_family_info.num_library_samples
            # update family_entry
            if family_id == 0:
                self._getDb().families.update_one({"family_id": 0}, {"$set": {"num_samples": 0, "num_functions": 0, "num_library_samples": 0}})
            else:
                self._getDb().families.delete_one({"family_id": family_id})
            self._getDb().families.update_one(
                {"family_id": new_family_id}, {"$set": {"num_samples": new_num_samples, "num_functions": new_num_functions, "num_library_samples": new_num_lib_samples}}
            )
            # update sample_entry and function_entries with new family information
            self._getDb().samples.update_many({"family_id": family_id}, {"$set": {"family_id": new_family_id, "family": family_name}})
            self._getDb().functions.update_many({"family_id": family_id}, {"$set": {"family_id": new_family_id}})
            self._updateDbState()
        return True

    def recomputeFamilyStats(self, progress_reporter=None) -> Dict[str, Any]:
        """Set every family's counters from the samples and functions that exist (#151).

        The per-family counters are denormalised and incremented on every write path; a lost
        update anywhere leaves them drifted, and /status sums them as the corpus size. This
        recomputes them with one aggregation over samples and one over functions, creates a
        document for any family id that samples reference without one, and reports what changed.
        """
        db = self._getDb()
        samples_by_family: Dict[int, Dict[str, Any]] = {}
        for row in db.samples.aggregate(
            [{"$group": {"_id": "$family_id", "num_samples": {"$sum": 1}, "num_library_samples": {"$sum": {"$cond": ["$is_library", 1, 0]}}, "family": {"$first": "$family"}}}]
        ):
            samples_by_family[row["_id"]] = row
        functions_by_family: Dict[int, int] = {}
        for row in db.functions.aggregate([{"$group": {"_id": "$family_id", "num_functions": {"$sum": 1}}}]):
            functions_by_family[row["_id"]] = row["num_functions"]
        report: Dict[str, Any] = {"num_families": 0, "num_families_corrected": 0, "num_families_created": 0, "corrections": {}}
        known_family_ids = {document["family_id"] for document in db.families.find({}, {"family_id": 1, "_id": 0})}
        for family_id, row in samples_by_family.items():
            if family_id not in known_family_ids:
                self._ensureFamilyDocument(family_id, str(row.get("family") or ""))
                report["num_families_created"] += 1
        # a counter is only replaced while it still holds what was read next to the aggregate:
        # a concurrent ingest, move or deletion that incremented it in between fails the filter,
        # and the family is aggregated again. Increments landing after the write are consistent
        # with it (their sample is not in the aggregate either way), so two passes converge.
        pending = {document["family_id"] for document in db.families.find({}, {"_id": 0, "family_id": 1})}
        for _ in range(3):
            if not pending:
                break
            retry = set()
            for family_document in db.families.find(
                {"family_id": {"$in": sorted(pending)}}, {"_id": 0, "family_id": 1, "num_samples": 1, "num_functions": 1, "num_library_samples": 1}
            ):
                family_id = family_document["family_id"]
                actual = {
                    "num_samples": samples_by_family.get(family_id, {}).get("num_samples", 0),
                    "num_functions": functions_by_family.get(family_id, 0),
                    "num_library_samples": samples_by_family.get(family_id, {}).get("num_library_samples", 0),
                }
                if family_id not in report["corrections"]:
                    report["num_families"] += 1
                stored = {key: family_document.get(key) for key in actual}
                if stored == actual:
                    continue
                written = db.families.update_one({"family_id": family_id, **stored}, {"$set": actual})
                if written.modified_count:
                    if family_id not in report["corrections"]:
                        report["num_families_corrected"] += 1
                    report["corrections"][family_id] = {"before": stored, "after": actual}
                else:
                    retry.add(family_id)
            if not retry:
                break
            # re-aggregate only the families a concurrent write touched
            samples_by_family.update(
                {
                    row["_id"]: row
                    for row in db.samples.aggregate(
                        [
                            {"$match": {"family_id": {"$in": sorted(retry)}}},
                            {
                                "$group": {
                                    "_id": "$family_id",
                                    "num_samples": {"$sum": 1},
                                    "num_library_samples": {"$sum": {"$cond": ["$is_library", 1, 0]}},
                                    "family": {"$first": "$family"},
                                }
                            },
                        ]
                    )
                }
            )
            functions_by_family.update(
                {
                    row["_id"]: row["num_functions"]
                    for row in db.functions.aggregate([{"$match": {"family_id": {"$in": sorted(retry)}}}, {"$group": {"_id": "$family_id", "num_functions": {"$sum": 1}}}])
                }
            )
            pending = retry
        return report

    def deleteFamily(self, family_id: int, keep_samples: bool = False) -> bool:
        family_entry = self.getFamily(family_id)
        if family_entry is None:
            return False
        sample_entries = self.getSamplesByFamilyId(family_id) or []
        if keep_samples:
            self._getDb().samples.update_many({"family_id": family_id}, {"$set": {"family_id": 0, "family": ""}})
            self._getDb().functions.update_many({"family_id": family_id}, {"$set": {"family_id": 0}})
            self._updateFamilyStats(0, family_entry.num_samples, family_entry.num_functions, family_entry.num_library_samples)
        else:
            for sample_entry in sample_entries:
                self.deleteSample(sample_entry.sample_id)
        # ensure we always have family_id 0 as empty family.
        if family_id == 0:
            self._getDb().families.update_one({"family_id": 0}, {"$set": {"num_samples": 0, "num_functions": 0, "num_library_samples": 0}})
        else:
            self._getDb().families.delete_one({"family_id": family_id})
        self._updateDbState()
        return True

    def getSamplesByFamilyId(self, family_id: int) -> Optional[List["SampleEntry"]]:
        if self.getFamily(family_id) is None:
            return None
        samples = self._getDb().samples.find({"family_id": family_id}, {"_id": 0})
        return [SampleEntry.fromDict(sample_document) for sample_document in samples]

    def getSamples(self, start_index: int, limit: int, is_query=False) -> List["SampleEntry"]:
        sample_entries = []
        if is_query:
            for sample_document in self._getDb().query_samples.find().skip(start_index).limit(limit):
                sample_entries.append(SampleEntry.fromDict(sample_document))
        else:
            for sample_document in self._getDb().samples.find().skip(start_index).limit(limit):
                sample_entries.append(SampleEntry.fromDict(sample_document))
        return sample_entries

    def clearStorage(self) -> None:
        # "xcfg"/"query_xcfg" hold the disassembly split out of the function documents (#137);
        # leaving them behind while the counters reset would collide on _id at the next insert
        # "picblockhashes" is the inverted block-hash index; leaving it behind would keep asserting
        # that hashes are held by samples that no longer exist, and getUniqueBlocks would believe it
        collections = ["samples", "families", "functions", "matches", "candidates", "counters", "query_samples", "query_functions", "xcfg", "query_xcfg", "picblockhashes"]
        for band_id in range(self._storage_config.STORAGE_NUM_BANDS):
            collections.append("band_%d" % band_id)
        for c in collections:
            self._getDb()[c].drop()
        # an emptied corpus is under every cap again; other processes simply wait out the TTL
        self._over_cap_since.clear()
        self._ensureIndexAndUnknownFamily()

    def getSampleBySha256(self, sha256: str, is_query=False) -> Optional["SampleEntry"]:
        target_sample = None
        if is_query:
            report_dict = self._getDb().query_samples.find_one({"sha256": sha256})
        else:
            report_dict = self._getDb().samples.find_one({"sha256": sha256})
        if report_dict:
            target_sample = SampleEntry.fromDict(report_dict)
        return target_sample

    def updateFunctionLabels(self, smda_report: "SmdaReport", username) -> Optional["SampleEntry"]:
        assert smda_report.sha256 is not None
        sample_entry = self.getSampleBySha256(smda_report.sha256)
        if not sample_entry:
            return None
        # check which functions in the SmdaReport have suitable function_names
        submitted_labels = {}
        for smda_function in smda_report.getFunctions():
            function_name = smda_function.function_name
            if function_name and not re.match("sub_[a-fA-F0-9]{1,16}", function_name):
                # use two-complement to convert unit64 to int64 and vice versa
                offset = encode_two_complement(smda_function.offset)
                submitted_labels[offset] = function_name
        # get the respective FunctionEntries and check if the label is novel
        sample_function_entries = {entry.offset: entry for entry in self.getFunctionsBySampleId(sample_entry.sample_id) or []}
        label_updates = []
        for label_offset, extracted_label in submitted_labels.items():
            is_new_label = False
            # we can only ever update labels if their offset exists in our DB
            if label_offset in sample_function_entries:
                is_new_label = True
                existing_labels = sample_function_entries[label_offset].function_labels
                for existing_label in existing_labels:
                    if existing_label.username == username and existing_label.function_label == extracted_label:
                        is_new_label = False
            # match by function_id or offset and add the label if it had not existed before.
            if is_new_label:
                new_function_entry_label = FunctionLabelEntry(extracted_label, username)
                update_command = {"$push": {"function_labels": new_function_entry_label.toDict()}}
                label_updates.append(UpdateOne({"function_id": sample_function_entries[label_offset].function_id}, update_command, upsert=True))
        if label_updates:
            self._getDb().functions.bulk_write(label_updates, ordered=False)

    def addSmdaReport(self, smda_report: "SmdaReport", isQuery=False) -> Optional["SampleEntry"]:
        assert smda_report.sha256 is not None
        sample_entry = None
        if isQuery:
            sample_entry = SampleEntry(smda_report, sample_id=-1 * self._useCounter("query_samples"), family_id=0)
            self._dbInsert("query_samples", sample_entry.toDict())
            function_ids = self._useCounterBulk("query_functions", smda_report.num_functions)
            function_dicts = []
            for function_id, smda_function in zip(function_ids, smda_report.getFunctions()):
                function_dicts.append(self._getFunctionDocument(sample_entry, smda_function, -1 * function_id))
            self._insertXcfgDocuments(function_dicts)
            self._dbInsertMany("query_functions", function_dicts)
        else:
            if not self.getSampleBySha256(smda_report.sha256):
                family_id = self.addFamily(smda_report.family or "")
                sample_entry = SampleEntry(smda_report, sample_id=self._useCounter("samples"), family_id=family_id)
                self._dbInsert("samples", sample_entry.toDict())
                function_ids = self._useCounterBulk("functions", smda_report.num_functions)
                function_dicts = []
                for function_id, smda_function in zip(function_ids, smda_report.getFunctions()):
                    function_dicts.append(self._getFunctionDocument(sample_entry, smda_function, function_id))
                self._insertXcfgDocuments(function_dicts)
                self._dbInsertMany("functions", function_dicts)
                self._addToPicBlockHashIndex(function_dicts)
                self._addToPicHashCounts(function_dicts)
                self._recordFunctionRange(sample_entry.sample_id, [document["function_id"] for document in function_dicts])
                self._updateFamilyStats(family_id, +1, sample_entry.statistics["num_functions"], int(sample_entry.is_library))
                self._updateDbState()
            else:
                LOGGER.warning("Sample %s already existed, skipping.", smda_report.sha256)
        return sample_entry

    def importSampleEntry(self, sample_entry: "SampleEntry") -> Optional["SampleEntry"]:
        if not self.getSampleBySha256(sample_entry.sha256):
            sample_id = self._useCounter("samples")
            sample_entry.sample_id = sample_id
            self._dbInsert("samples", sample_entry.toDict())
            self._ensureFamilyDocument(sample_entry.family_id, sample_entry.family)
            self._updateFamilyStats(sample_entry.family_id, +1, sample_entry.statistics["num_functions"], int(sample_entry.is_library))
            self._updateDbState()
        else:
            LOGGER.warning("Sample %s already existed, skipping.", sample_entry.sha256)
        return sample_entry

    def importFunctionEntry(self, function_entry: "FunctionEntry") -> Optional["FunctionEntry"]:
        function_entry.function_id = self._useCounter("functions")
        if function_entry.function_name and len(function_entry.function_labels) == 0:
            new_function_entry_label = FunctionLabelEntry(function_entry.function_name, "mcrit-import")
            function_entry.function_labels.append(new_function_entry_label)
        # add function to regular storage
        function_dict = function_entry.toDict()
        self._encodeFunction(function_dict)
        self._insertXcfgDocuments([function_dict])
        self._dbInsert("functions", function_dict)
        self._addToPicBlockHashIndex([function_dict])
        self._addToPicHashCounts([function_dict])
        return function_entry

    def importFunctionEntries(self, function_entries: List["FunctionEntry"]) -> Optional[List["FunctionEntry"]]:
        functions_as_dicts = []
        function_ids = self._useCounterBulk("functions", len(function_entries))
        for function_id, function_entry in zip(function_ids, function_entries):
            function_entry.function_id = function_id
            if function_entry.function_name and len(function_entry.function_labels) == 0:
                new_function_entry_label = FunctionLabelEntry(function_entry.function_name, "mcrit-import")
                function_entry.function_labels.append(new_function_entry_label)
            # add function to regular storage
            function_dict = function_entry.toDict()
            self._encodeFunction(function_dict)
            functions_as_dicts.append(function_dict)
        self._insertXcfgDocuments(functions_as_dicts)
        self._dbInsertMany("functions", functions_as_dicts)
        self._addToPicBlockHashIndex(functions_as_dicts)
        self._addToPicHashCounts(functions_as_dicts)
        return function_entries

    def getFunctionsBySampleId(self, sample_id: int) -> Optional[List["FunctionEntry"]]:
        if not self.isSampleId(sample_id):
            return None
        if sample_id < 0:
            function_dicts = list(self._getDb().query_functions.find({"sample_id": sample_id}, {"_id": 0}))
        else:
            function_dicts = list(self._getDb().functions.find({"sample_id": sample_id}, {"_id": 0}))
        functions = []
        self._attachXcfgBlobs(function_dicts)
        for f in function_dicts:
            self._decodeFunction(f)
            functions.append(FunctionEntry.fromDict(f))
        return functions

    def _getFunctionMinHashesBySampleId(self, sample_id: int) -> List[Dict[str, Any]]:
        field_selection = {"_id": 0, "function_id": 1, "minhash": 1}
        if sample_id < 0:
            return list(self._getDb().query_functions.find({"sample_id": sample_id}, field_selection))
        return list(self._getDb().functions.find({"sample_id": sample_id}, field_selection))

    def _getMinHashFromStorage(self, function_document: Dict[str, Any]) -> Optional["MinHash"]:
        minhash_hex = function_document.get("minhash")
        if not minhash_hex:
            return None
        return MinHash(
            function_id=function_document["function_id"],
            minhash_bytes=bytes.fromhex(minhash_hex),
            minhash_bits=self._minhash_config.MINHASH_SIGNATURE_BITS,
        )

    def getFunctionIdsBySampleId(self, sample_id: int) -> Optional[List["int"]]:
        function_ids = None
        if not self.isSampleId(sample_id):
            return function_ids
        function_ids = []
        if sample_id < 0:
            function_dicts = list(self._getDb().query_functions.find({"sample_id": sample_id}, {"_id": 0, "function_id": 1}))
        else:
            function_dicts = list(self._getDb().functions.find({"sample_id": sample_id}, {"_id": 0, "function_id": 1}))
        for f in function_dicts:
            function_ids.append(f["function_id"])
        return function_ids

    def getFunctions(self, start_index: int, limit: int) -> List["FunctionEntry"]:
        functions = []
        function_documents = list(self._getDb().functions.find().skip(start_index).limit(limit))
        self._attachXcfgBlobs(function_documents)
        for function_document in function_documents:
            self._decodeFunction(function_document)
            functions.append(FunctionEntry.fromDict(function_document))
        return functions

    def getSampleIds(self) -> List[int]:
        return self._getDb().samples.find().distinct("sample_id")

    def isSampleId(self, sample_id: int) -> bool:
        is_sample_id = None
        if sample_id < 0:
            is_sample_id = bool(self._getDb().query_samples.find_one({"sample_id": sample_id}))
        else:
            is_sample_id = bool(self._getDb().samples.find_one({"sample_id": sample_id}))
        return is_sample_id

    def getPicHashMatchesBySampleId(self, sample_id: int) -> Optional[Dict[int, Set[Tuple[int, int, int]]]]:
        if not self.isSampleId(sample_id):
            return None
        function_ids = list(
            map(
                itemgetter("function_id"),
                list(self._getDb().functions.find({"sample_id": sample_id}, {"function_id": 1, "_id": 0})),
            )
        )  # ["function_id"]
        return self.getPicHashMatchesByFunctionIds(function_ids)

    def getPicHashMatchesByFunctionId(self, function_id: int) -> Optional[Dict[int, Set[Tuple[int, int, int]]]]:
        query_result = self._getDb().functions.find_one({"function_id": function_id}, {"_id": 0, "_pichash": 1})
        if query_result is None or "_pichash" not in query_result:
            return None
        self._decodePichash(query_result, delete_old=False)
        encoded_pichash = query_result["_pichash"]
        decoded_pichash = query_result["pichash"]
        if decoded_pichash is None:
            return None

        sample_and_function_ids = set(
            map(
                lambda x: (x["family_id"], x["sample_id"], x["function_id"]),
                list(self._getDb().functions.find({"_pichash": encoded_pichash}, {"family_id": 1, "sample_id": 1, "function_id": 1, "_id": 0})),
            )
        )

        return {decoded_pichash: sample_and_function_ids}

    def getPicHashMatchesByFunctionIds(self, function_ids: List[int]) -> Dict[int, Set[Tuple[int, int, int]]]:
        # internal format!
        pichash_raw = self._getDb().functions.find({"function_id": {"$in": function_ids}}, {"_pichash": 1, "_id": 0})
        pichashes_list = list(pichash_raw)  # broken? TODO
        pichashes = {}
        encoded_to_decoded = {}
        for pichash in pichashes_list:
            # TODO what happens if pichash does not exist??
            self._decodePichash(pichash, delete_old=False)
            encoded_pichash = pichash["_pichash"]
            decoded_pichash = pichash["pichash"]
            if decoded_pichash in pichashes:
                continue
            pichashes[decoded_pichash] = set()
            encoded_to_decoded[encoded_pichash] = decoded_pichash
        # one $in over all distinct pichashes instead of one query per pichash (N+1, #111);
        # grouping client-side by the returned _pichash reproduces the per-query sets exactly
        if encoded_to_decoded:
            wanted = self._filterPicHashesByMatchCount(list(encoded_to_decoded))
            fields_to_fetch = {"_pichash": 1, "family_id": 1, "sample_id": 1, "function_id": 1, "_id": 0}
            for hit in self._getDb().functions.find({"_pichash": {"$in": wanted}}, fields_to_fetch):
                decoded_pichash = encoded_to_decoded[hit.get("_pichash")]
                pichashes[decoded_pichash].add((hit["family_id"], hit["sample_id"], hit["function_id"]))
        return pichashes

    def _filterPicHashesByMatchCount(self, encoded_pichashes: List[Any]) -> List[Any]:
        """Drop pichashes held by more than MINHASH_PICHASH_MAX_MATCHES functions.

        The counting pass groups on `_pichash`, which is indexed, so mongod answers it from the
        index instead of reading the documents - the expensive half is fetching family, sample
        and function id per hit, and that is what the cutoff avoids. Returns every hash
        unchanged when the knob is off, so the default path pays nothing for this.
        """
        cutoff = getattr(self._minhash_config, "MINHASH_PICHASH_MAX_MATCHES", 0)
        if cutoff <= 0 or not encoded_pichashes:
            return encoded_pichashes
        if self.isPicHashCountIndexComplete():
            # one indexed probe per queried hash, instead of one index entry per holder
            kept = [
                document["_pichash"]
                for document in self._getDb()[self._PICHASH_COUNT_COLLECTION].find({"_pichash": {"$in": encoded_pichashes}, "df": {"$lte": cutoff}}, {"_pichash": 1, "_id": 0})
            ]
        else:
            # no counts stored yet: fall back to counting, which is correct but pays the very
            # cost the cutoff is meant to avoid
            pipeline = [
                {"$match": {"_pichash": {"$in": encoded_pichashes}}},
                {"$group": {"_id": "$_pichash", "num_holders": {"$sum": 1}}},
                {"$match": {"num_holders": {"$lte": cutoff}}},
            ]
            kept = [group["_id"] for group in self._getDb().functions.aggregate(pipeline, allowDiskUse=True)]
        if len(kept) != len(encoded_pichashes):
            LOGGER.info("PicHash cutoff %d dropped %d of %d hashes as too common", cutoff, len(encoded_pichashes) - len(kept), len(encoded_pichashes))
        return kept

    def isFunctionId(self, function_id: int) -> bool:
        is_function_id = None
        if function_id < 0:
            is_function_id = bool(self._getDb().query_functions.find_one({"function_id": function_id}))
        else:
            is_function_id = bool(self._getDb().functions.find_one({"function_id": function_id}))
        return is_function_id

    def isFamilyId(self, family_id: int) -> bool:
        return bool(self._getDb().families.find_one({"family_id": family_id}))

    def addMinHash(self, minhash: "MinHash") -> bool:
        if minhash.function_id is None:
            return False
        is_query_function = minhash.function_id < 0
        collection = self._getDb().query_functions if is_query_function else self._getDb().functions
        set_command = {"$set": {"minhash": minhash.getMinHash().hex(), "minhash_shingle_composition": minhash.getComposition()}}
        if collection.find_one_and_update({"function_id": minhash.function_id}, set_command) is None:
            LOGGER.warning("addMinHash() - no function with id %d, skipping its minhash.", minhash.function_id)
            return False
        # query functions are not indexed into bands, they are only ever used as query input
        if not is_query_function:
            self._addMinHashToBands(minhash)
        return True

    def addMinHashes(self, minhashes: List["MinHash"]) -> None:
        if not minhashes:
            return
        for is_query_function in (False, True):
            relevant_minhashes = [minhash for minhash in minhashes if minhash.function_id is not None and (minhash.function_id < 0) == is_query_function]
            if not relevant_minhashes:
                continue
            collection = self._getDb().query_functions if is_query_function else self._getDb().functions
            function_ids = [minhash.function_id for minhash in relevant_minhashes]
            existing_ids = set(collection.distinct("function_id", {"function_id": {"$in": function_ids}}))
            function_updates = []
            stored_minhashes = []
            for minhash in relevant_minhashes:
                if minhash.function_id not in existing_ids:
                    LOGGER.warning("addMinHashes() - no function with id %d, skipping its minhash.", minhash.function_id)
                    continue
                set_command = {"$set": {"minhash": minhash.getMinHash().hex(), "minhash_shingle_composition": minhash.getComposition()}}
                function_updates.append(UpdateOne({"function_id": minhash.function_id}, set_command))
                stored_minhashes.append(minhash)
            if function_updates:
                collection.bulk_write(function_updates, ordered=False)
            if stored_minhashes and not is_query_function:
                self._addMinHashesToBands(stored_minhashes)

    # original implementation, already working
    def _addMinHashToBands(self, minhash: "MinHash") -> None:
        # reuse multi-insert function
        self._addMinHashesToBands([minhash])

    def _addMinHashesToBands(self, minhashes: List["MinHash"]) -> None:
        band_hashes = {number: {} for number in range(self._storage_config.STORAGE_NUM_BANDS)}
        for minhash in minhashes:
            if minhash.hasMinHash():
                for band_number, band_hash in self.getBandHashesForMinHash(minhash).items():
                    if band_hash not in band_hashes[band_number]:
                        band_hashes[band_number][band_hash] = []
                    band_hashes[band_number][band_hash].append(minhash.function_id)
        self._updateBands(band_hashes)

    # a band document whose posting list is empty (or missing) holds nothing a candidate
    # lookup could find; it is residue, not index (#149)
    _EMPTY_BAND_DOCUMENT = {"$or": [{"function_ids": {"$size": 0}}, {"function_ids": {"$exists": False}}]}

    def _bandBucketSize(self) -> int:
        """How many postings one band document may hold, or 0 for the single-document shape."""
        return max(0, int(getattr(self._storage_config, "STORAGE_BAND_BUCKET_SIZE", 0) or 0))

    def _updateBands(self, band_hashes: Dict[int, Dict[int, List[int]]], method="push") -> int:
        if method not in ["push", "pull"]:
            raise ValueError(f"MongoDbStorage._updateBands() can only do 'push' and 'pull', not '{method}'.")
        if method == "push" and self._bandBucketSize():
            return self._pushBandsBucketed(band_hashes)
        num_band_updates = 0
        for band_number, band_data in band_hashes.items():
            band_updates = []
            for band_hash, function_ids in band_data.items():
                if len(function_ids) < 1:
                    continue
                if method == "push":
                    # a band hash seen for the first time gets its document here. df counts the
                    # posting list so a candidate lookup can skip an over-long one from the
                    # index, without reading the list to measure it (#band-df)
                    band_updates.append(UpdateOne({"band_hash": band_hash}, {"$push": {"function_ids": {"$each": function_ids}}, "$inc": {"df": len(function_ids)}}, upsert=True))
                else:
                    # pulling from a band hash that has no document must not create one (#149), so
                    # no upsert; UpdateMany because under bucketing a hash spans several documents
                    band_updates.append(UpdateMany({"band_hash": band_hash}, {"$pull": {"function_ids": {"$in": function_ids}}}))
            if band_updates:
                collection = self._getDb()["band_%d" % band_number]
                collection.bulk_write(band_updates, ordered=False)
                if method == "pull":
                    # a posting list the pull emptied is removed, not kept as a tombstone; scoped to
                    # the hashes just touched, so it is one indexed delete per band (#149)
                    if self._bandBucketSize():
                        # bucket 0 carries df/tail/tail_n for the whole hash, so an emptied bucket 0
                        # is kept until the recompute shows no posting survives anywhere; only
                        # emptied buckets above it go now
                        collection.delete_many({"band_hash": {"$in": list(band_data)}, "bucket": {"$nin": [0, None]}, **self._EMPTY_BAND_DOCUMENT})
                        # under bucketing df on bucket 0 is the total across every bucket, so it
                        # cannot be restored per document - that would set each bucket's df to its
                        # own size and silently break the cutoff, which reads bucket 0 alone.
                        self._recomputeBandBookkeeping(collection, list(band_data))
                        collection.delete_many({"band_hash": {"$in": list(band_data)}, "df": 0, **self._EMPTY_BAND_DOCUMENT})
                    else:
                        collection.delete_many({"band_hash": {"$in": list(band_data)}, **self._EMPTY_BAND_DOCUMENT})
                        # $pull cannot report how many elements it removed, so df is restored from
                        # the list itself rather than decremented by a guess
                        collection.update_many(
                            {"band_hash": {"$in": list(band_data)}},
                            [{"$set": {"df": {"$size": {"$ifNull": ["$function_ids", []]}}}}],
                        )
            num_band_updates += len(band_updates)
        return num_band_updates

    def _pushBandsBucketed(self, band_hashes: Dict[int, Dict[int, List[int]]]) -> int:
        """Append postings, splitting a band hash across (band_hash, bucket) documents.

        A posting list is an array inside one document and MongoDB caps a document at 16 MB, so a
        band hash common enough to accumulate ~1.35M postings (~1.05M once ids need int64) stops
        being writable at all - the $push fails rather than slowing down, and indexing halts.
        Extrapolated from a 7,244-sample real corpus that wall sits near 270,000 samples, and no
        amount of sharding moves it, because a document cannot span shards.

        Shape: bucket 0 carries the bookkeeping for the whole hash - `df` (the total across every
        bucket, which is what STORAGE_BAND_DF_CUTOFF filters on), `tail` (the highest bucket in
        use) and `tail_n` (how full that one is). Buckets above 0 carry only their slice of the
        postings. Keeping df on bucket 0 alone means the cutoff filter and its (band_hash, df)
        index are unchanged: a hash under the cutoff is far below one bucket's worth, so it never
        spills, and a hash that did spill has a df that rejects it anyway.

        Buckets fill in order rather than by hashing the function id, because the common case is a
        short posting list: modulo placement would scatter a 200-entry list across 200 documents
        and make the cheap case expensive. Filling in order keeps every under-cutoff hash in a
        single document, exactly as before.

        Placement needs to know where the tail is, which costs one indexed read per batch per
        band - projected to exclude function_ids, so it never pays for the arrays it is placing
        next to.
        """
        bucket_size = self._bandBucketSize()
        num_band_updates = 0
        for band_number, band_data in band_hashes.items():
            wanted = [band_hash for band_hash, function_ids in band_data.items() if len(function_ids) >= 1]
            if not wanted:
                continue
            collection = self._getDb()["band_%d" % band_number]
            # current tail per hash. Documents written before bucketing was switched on have no
            # `bucket` field; they are bucket 0 by definition, and tail_n falls back to df.
            state = {}
            for document in collection.find(
                {"band_hash": {"$in": wanted}, "bucket": {"$in": [0, None]}},
                {"_id": 0, "band_hash": 1, "df": 1, "tail": 1, "tail_n": 1},
            ):
                df = int(document.get("df") or 0)
                state[document["band_hash"]] = {
                    "df": df,
                    "tail": int(document.get("tail") or 0),
                    "tail_n": int(document["tail_n"]) if document.get("tail_n") is not None else df,
                }
            band_updates = []
            for band_hash in wanted:
                function_ids = list(band_data[band_hash])
                current = state.get(band_hash, {"df": 0, "tail": 0, "tail_n": 0})
                tail, tail_n = current["tail"], current["tail_n"]
                offset = 0
                touched_bucket_zero = False
                while offset < len(function_ids):
                    if tail_n >= bucket_size:
                        tail += 1
                        tail_n = 0
                    space = bucket_size - tail_n
                    slice_ids = function_ids[offset : offset + space]
                    update = {"$push": {"function_ids": {"$each": slice_ids}}}
                    if tail == 0:
                        # bucket 0 carries the whole hash's bookkeeping, so its own write does both
                        touched_bucket_zero = True
                        update["$inc"] = {"df": len(function_ids)}
                    band_updates.append(UpdateOne({"band_hash": band_hash, "bucket": tail}, update, upsert=True))
                    tail_n += len(slice_ids)
                    offset += len(slice_ids)
                # tail/tail_n are owned by this trailing write alone, so it and a bucket-0 $push in
                # the same unordered bulk touch disjoint fields and their order does not matter
                trailing = {"$set": {"tail": tail, "tail_n": tail_n}}
                if not touched_bucket_zero:
                    # the postings all landed above bucket 0, so its df needs this write too
                    trailing["$inc"] = {"df": len(function_ids)}
                # upsert only when no bucket-0 $push precedes it, so one batch never carries two
                # upserts for the same new document
                band_updates.append(UpdateOne({"band_hash": band_hash, "bucket": 0}, trailing, upsert=not touched_bucket_zero))
            if band_updates:
                collection.bulk_write(band_updates, ordered=False)
                num_band_updates += len(band_updates)
        return num_band_updates

    def _recomputeBandBookkeeping(self, collection, band_hashes: List[int]) -> None:
        """Restore bucket 0's df/tail/tail_n for the given hashes from the buckets that remain.

        Needed after a pull, which can empty a bucket anywhere in the chain and leaves the
        counters describing a state that no longer exists. Scoped to the hashes just touched, so
        the cost follows the deletion rather than the corpus.

        A deletion can also leave a gap - bucket 2 surviving while bucket 1 emptied and was
        removed - so `tail` is taken as the highest bucket that still exists rather than derived
        from df, and `tail_n` from that bucket's own length. Placement then appends to a
        part-filled tail, which is correct if untidy; buckets are a cap, not a promise of packing.
        """
        if not band_hashes:
            return
        totals = {}
        for row in collection.aggregate(
            [
                {"$match": {"band_hash": {"$in": band_hashes}}},
                {"$project": {"band_hash": 1, "bucket": {"$ifNull": ["$bucket", 0]}, "n": {"$size": {"$ifNull": ["$function_ids", []]}}}},
                {"$group": {"_id": "$band_hash", "df": {"$sum": "$n"}, "tail": {"$max": "$bucket"}, "buckets": {"$push": {"bucket": "$bucket", "n": "$n"}}}},
            ]
        ):
            tail = int(row["tail"] or 0)
            tail_n = 0
            for entry in row["buckets"]:
                if int(entry["bucket"] or 0) == tail:
                    tail_n = int(entry["n"])
                    break
            totals[row["_id"]] = (int(row["df"]), tail, tail_n)
        if not totals:
            return
        # upsert while postings survive: bucket 0 holds the only copy of these counters, and a hash
        # whose bucket 0 is missing (e.g. removed before this was fixed) would otherwise stay
        # invisible to the cutoff and restart placement at bucket 0 on the next push
        updates = [
            UpdateOne({"band_hash": band_hash, "bucket": 0}, {"$set": {"df": df, "tail": tail, "tail_n": tail_n}}, upsert=df > 0)
            for band_hash, (df, tail, tail_n) in totals.items()
        ]
        collection.bulk_write(updates, ordered=False)

    def purgeEmptyBandDocuments(self) -> int:
        """Remove the empty band documents that deletions left behind before #149; returns how many."""
        removed = 0
        for band_number in range(self._storage_config.STORAGE_NUM_BANDS):
            # a bucketed hash's bucket 0 can be empty yet still carry df/tail/tail_n for postings in
            # higher buckets; that is bookkeeping, not residue
            residue = {"$and": [self._EMPTY_BAND_DOCUMENT, {"$nor": [{"bucket": 0, "df": {"$gt": 0}}]}]}
            removed += self._getDb()["band_%d" % band_number].delete_many(residue).deleted_count
        return removed

    def _collectBandHashTargets(self, function_id_to_minhash: Dict[int, "MinHash"]):
        """band_number -> set(band_hash) to query, and band_number -> band_hash -> query function_ids."""
        target_band_hashes_per_band = {band_number: set() for band_number in range(self._storage_config.STORAGE_NUM_BANDS)}
        band_hash_to_function_ids = {band_number: {} for band_number in range(self._storage_config.STORAGE_NUM_BANDS)}
        for function_id, minhash in function_id_to_minhash.items():
            if not minhash.hasMinHash():
                continue
            band_hashes = self.getBandHashesForMinHash(minhash)
            for band_number, band_hash in sorted(band_hashes.items()):
                target_band_hashes_per_band[band_number].add(band_hash)
                if band_hash not in band_hash_to_function_ids[band_number]:
                    band_hash_to_function_ids[band_number][band_hash] = set()
                band_hash_to_function_ids[band_number][band_hash].add(function_id)
        return target_band_hashes_per_band, band_hash_to_function_ids

    def _bandDfCutoff(self, band_df_cutoff: Optional[int]) -> int:
        """The df cutoff a lookup applies: the one its job asked for, else STORAGE_BAND_DF_CUTOFF (#217).

        A job's cutoff is held to what __init__ holds the configured one to: only bucket 0 carries
        df, so a cutoff above the bucket size would serve a spilled hash as bucket 0 alone.
        """
        if band_df_cutoff is None:
            return getattr(self._storage_config, "STORAGE_BAND_DF_CUTOFF", 0)
        bucket_size = self._bandBucketSize()
        if bucket_size and band_df_cutoff > bucket_size:
            raise ValueError(f"band_df_cutoff ({band_df_cutoff}) must not exceed STORAGE_BAND_BUCKET_SIZE ({bucket_size}).")
        return band_df_cutoff

    def _bandLookupPipeline(self, band_hashes: List[int], band_df_cutoff: Optional[int] = None) -> List[Dict[str, Any]]:
        """Aggregation returning the wanted band documents, dropping over-long posting lists.

        The cutoff is applied server-side rather than after the fetch on purpose: the cost of a
        stopword band hash is dominated by shipping and BSON-decoding a posting list with
        millions of entries, so a client-side check would pay almost the whole price before
        discarding it.
        """
        cutoff = self._bandDfCutoff(band_df_cutoff)
        if cutoff <= 0:
            return [{"$match": {"band_hash": {"$in": band_hashes}}}]
        if self.isBandDfIndexComplete():
            # the whole point of storing df: with a (band_hash, df) index mongod decides from
            # the index entry alone, so an over-long posting list is never read. Filtering on
            # $size instead still reads every document to measure it, which measured no faster
            # than not filtering at all.
            return [{"$match": {"band_hash": {"$in": band_hashes}, "df": {"$lte": cutoff}}}]
        # no trustworthy df yet: fall back to measuring the list, which is correct but only
        # saves the transfer, not the read
        # Only bucket 0 is measured. Under bucketing a hash over the cutoff is split, and each of
        # its buckets on its own could be shorter than the cutoff - measuring per document would
        # admit exactly the posting lists the cutoff exists to reject. Bucket 0 of such a hash is
        # full (one bucket's worth, far above any sane cutoff), so testing it alone is correct,
        # and a hash that never spilled has all of its postings there anyway.
        return [
            # a hash that has spilled (its bucket 0 names a tail above 0) holds more than bucket 0,
            # which is full or, after pulls, not the whole list: never served from bucket 0 alone,
            # even when the cutoff equals the bucket size and that one document would pass $size
            {"$match": {"band_hash": {"$in": band_hashes}, "bucket": {"$in": [0, None]}, "tail": {"$not": {"$gt": 0}}}},
            {"$match": {"$expr": {"$lte": [{"$size": {"$ifNull": ["$function_ids", []]}}, cutoff]}}},
        ]

    def _getCandidatesForMinHashesNumpy(self, function_id_to_minhash: Dict[int, "MinHash"], band_matches_required=1, as_arrays=False, band_df_cutoff=None):
        """Variant C: accumulate band hits as int64 arrays instead of dict[qid][cid] -> count.

        Semantically identical to the dict version (np.unique(..., return_counts=True) counts
        repeated ids exactly like the += 1 loop did), but it never builds the per-pair Python
        dict, and it stores one array per posting list rather than one entry per pair - the
        posting list is *shared* by reference between all query functions that hit that band
        hash, so the inner O(pairs) Python loop disappears as well.
        """
        target_band_hashes_per_band, band_hash_to_function_ids = self._collectBandHashTargets(function_id_to_minhash)
        hit_chunks = {}
        for band_number, band_hashes in target_band_hashes_per_band.items():
            cursor = self._getDb()["band_%d" % band_number].aggregate(self._bandLookupPipeline(list(band_hashes), band_df_cutoff))
            for hit in cursor:
                reference_function_ids = band_hash_to_function_ids[band_number][hit["band_hash"]]
                # int64, not int32: function ids come from a counter that never reuses an id, so they
                # pass 2**31 - 1 within a few million samples. numpy 2 then raises OverflowError, and
                # numpy 1.x silently wraps the id onto a different function.
                posting_list = np.array(hit["function_ids"], dtype=np.int64)
                for function_id in reference_function_ids:
                    if function_id not in hit_chunks:
                        hit_chunks[function_id] = [posting_list]
                    else:
                        hit_chunks[function_id].append(posting_list)
        # reduce candidates based on banding requirements
        valid_candidates = {}
        for function_id in list(hit_chunks):
            chunks = hit_chunks.pop(function_id)
            all_hits = chunks[0] if len(chunks) == 1 else np.concatenate(chunks)
            del chunks
            if band_matches_required <= 1:
                surviving = np.unique(all_hits)
            else:
                unique_ids, counts = np.unique(all_hits, return_counts=True)
                surviving = unique_ids[counts >= band_matches_required]
            if surviving.size:
                valid_candidates[function_id] = surviving.astype(np.int64, copy=False) if as_arrays else set(surviving.tolist())
        return valid_candidates

    def getCandidateArraysForMinHashes(self, function_id_to_minhash: Dict[int, "MinHash"], band_matches_required=1, band_df_cutoff=None) -> Dict[int, "np.ndarray"]:
        """Candidates as sorted int64 arrays rather than sets.

        The accumulation already produces arrays and then boxes them into Python sets; a caller
        that wants to keep working in numpy (the shortlist stage does) would immediately unbox
        them again. Measured on a 361k-pair query against 10k samples, that round trip alone was
        most of a second.
        """
        return self._getCandidatesForMinHashesNumpy(function_id_to_minhash, band_matches_required=band_matches_required, as_arrays=True, band_df_cutoff=band_df_cutoff)

    def getCandidatesForMinHashes(self, function_id_to_minhash: Dict[int, "MinHash"], band_matches_required=1, band_df_cutoff=None) -> Dict[int, Set[int]]:
        if getattr(self._storage_config, "STORAGE_CANDIDATE_ACCUMULATION", "dict") == "numpy":
            return self._getCandidatesForMinHashesNumpy(function_id_to_minhash, band_matches_required=band_matches_required, band_df_cutoff=band_df_cutoff)
        candidates = {}
        target_band_hashes_per_band, band_hash_to_function_ids = self._collectBandHashTargets(function_id_to_minhash)
        for band_number, band_hashes in target_band_hashes_per_band.items():
            cursor = self._getDb()["band_%d" % band_number].aggregate(self._bandLookupPipeline(list(band_hashes), band_df_cutoff))
            for hit in cursor:
                reference_function_ids = band_hash_to_function_ids[band_number][hit["band_hash"]]
                for function_id in reference_function_ids:
                    if function_id not in candidates:
                        candidates[function_id] = {}
                    for hit_function_id in hit["function_ids"]:
                        if hit_function_id not in candidates[function_id]:
                            candidates[function_id][hit_function_id] = 0
                        candidates[function_id][hit_function_id] += 1
        # reduce candidates based on banding requirements
        valid_candidates = {}
        for function_id, hit_counters in candidates.items():
            for other_id, count in hit_counters.items():
                if count >= band_matches_required:
                    if function_id not in valid_candidates:
                        valid_candidates[function_id] = set()
                    valid_candidates[function_id].add(other_id)
        return valid_candidates

    def getCandidatesForMinHash(self, minhash: "MinHash", band_matches_required=1, band_df_cutoff=None) -> Set[int]:
        if not minhash.hasMinHash():
            return set()
        candidates = {}
        band_hashes = self.getBandHashesForMinHash(minhash)
        for band_number, band_hash in sorted(band_hashes.items()):
            band_documents = list(self._getDb()["band_%d" % band_number].aggregate(self._bandLookupPipeline([band_hash], band_df_cutoff)))
            band_document = band_documents[0] if band_documents else None
            if band_document:
                for function_id in band_document["function_ids"]:
                    if function_id not in candidates:
                        candidates[function_id] = 0
                    candidates[function_id] += 1
        # reduce candidates based on banding requirements
        valid_candidates = set([])
        for function_id, hit_count in candidates.items():
            if hit_count >= band_matches_required:
                valid_candidates.add(function_id)
        return valid_candidates

    @staticmethod
    def _contiguousRuns(function_ids: List[int]) -> List[Tuple[int, int]]:
        """[(first, last)] for each run of consecutive ids in the given set."""
        runs: List[Tuple[int, int]] = []
        ordered = sorted(set(function_ids))
        if not ordered:
            return runs
        run_start = previous = ordered[0]
        for function_id in ordered[1:]:
            if function_id != previous + 1:
                runs.append((run_start, previous))
                run_start = function_id
            previous = function_id
        runs.append((run_start, previous))
        return runs

    def _recordFunctionRange(self, sample_id: int, function_ids: List[int]) -> None:
        """Record which function ids a sample owns, as one document per contiguous run.

        Ids normally arrive as one dense run per sample - they come from a single bulk counter
        reservation - but nothing guarantees it: an import can add functions to an existing
        sample later, and concurrent writers interleave their reservations. Storing a span per
        run rather than a single [first, last] makes the index exact for any layout instead of
        correct only for the common one, and the lookup is a searchsorted over spans either
        way, so the generality costs nothing at query time.
        """
        if not function_ids:
            return
        operations = [
            UpdateOne(
                {"sample_id": sample_id, "first_function_id": first_function_id},
                {"$set": {"sample_id": sample_id, "first_function_id": first_function_id, "last_function_id": last_function_id}},
                upsert=True,
            )
            for first_function_id, last_function_id in self._contiguousRuns(function_ids)
        ]
        if operations:
            self._getDb()[self._FUNCTION_RANGE_COLLECTION].bulk_write(operations, ordered=False)

    def isFunctionRangeIndexComplete(self) -> bool:
        settings_document = self._getDb().settings.find_one({}, {self._FUNCTION_RANGE_SETTING: 1})
        return bool(settings_document and settings_document.get(self._FUNCTION_RANGE_SETTING))

    def _setFunctionRangeIndexComplete(self, is_complete: bool) -> None:
        self._getDb().settings.update_one({}, {"$set": {self._FUNCTION_RANGE_SETTING: bool(is_complete)}})

    def rebuildFunctionRangeIndex(self, progress_reporter=None) -> int:
        """Build the function-range index from the functions collection; returns samples covered.

        One ordered scan, emitting a span whenever the sample changes or the ids jump. Exact for
        any id layout, where grouping on min/max per sample would be correct only when every
        sample owns one dense run - and a wrong span misattributes another sample's functions,
        which is a worse outcome than a slower rebuild.
        """
        collection = self._getDb()[self._FUNCTION_RANGE_COLLECTION]
        # Mark incomplete *before* emptying it. The flag is what readers consult to decide
        # whether the index may be trusted, so leaving it true across a rebuild advertises a
        # complete index over a half-filled collection - and a shortlist built on that would
        # silently attribute functions to no sample at all.
        self._setFunctionRangeIndexComplete(False)
        collection.delete_many({})
        # indexed before the upserts for the same reason as the pichash counts: each upsert
        # matches on (sample_id, first_function_id), and an unindexed match scans the collection
        collection.create_index("sample_id")
        collection.create_index("first_function_id")
        operations = []
        num_spans = 0
        samples_seen = set()
        state = {"sample_id": None, "run_start": None, "previous": None}

        def flush_span():
            nonlocal num_spans
            if state["sample_id"] is None:
                return
            operations.append(
                UpdateOne(
                    {"sample_id": state["sample_id"], "first_function_id": state["run_start"]},
                    {"$set": {"sample_id": state["sample_id"], "first_function_id": state["run_start"], "last_function_id": state["previous"]}},
                    upsert=True,
                )
            )
            num_spans += 1

        cursor = self._getDb().functions.find({}, {"_id": 0, "function_id": 1, "sample_id": 1}).sort("function_id", 1).batch_size(10000)
        for document in cursor:
            function_id, sample_id = document["function_id"], document["sample_id"]
            samples_seen.add(sample_id)
            if state["sample_id"] == sample_id and state["previous"] is not None and function_id == state["previous"] + 1:
                state["previous"] = function_id
                continue
            flush_span()
            state["sample_id"], state["run_start"], state["previous"] = sample_id, function_id, function_id
            if len(operations) >= 10000:
                collection.bulk_write(operations, ordered=False)
                operations = []
                if progress_reporter is not None:
                    progress_reporter.step()
        flush_span()
        if operations:
            collection.bulk_write(operations, ordered=False)
        self._setFunctionRangeIndexComplete(True)
        LOGGER.info("Function range index rebuilt: %d spans over %d samples.", num_spans, len(samples_seen))
        return len(samples_seen)

    def _getFunctionRangeLookup(self):
        """(first_function_id array, sample_id array) for searchsorted, cached per db_state.

        Cached because a matching job asks for it once per batch and it only changes when the
        corpus does; keyed on db_state so a stale map can never outlive the write that
        invalidated it.
        """
        db_state = self._getDbState()
        cached = getattr(self, "_function_range_cache", None)
        if cached is not None and cached[0] == db_state:
            return cached[1], cached[2]
        documents = list(
            self._getDb()[self._FUNCTION_RANGE_COLLECTION].find({}, {"_id": 0, "sample_id": 1, "first_function_id": 1, "last_function_id": 1}).sort("first_function_id", 1)
        )
        firsts = np.fromiter((document["first_function_id"] for document in documents), dtype=np.int64, count=len(documents))
        lasts = np.fromiter((document["last_function_id"] for document in documents), dtype=np.int64, count=len(documents))
        sample_ids = np.fromiter((document["sample_id"] for document in documents), dtype=np.int64, count=len(documents))
        self._function_range_cache = (db_state, (firsts, lasts), sample_ids)
        return (firsts, lasts), sample_ids

    def getSampleFunctionCounts(self, sample_ids: Optional[Iterable[int]] = None) -> Dict[int, int]:
        """sample_id -> how many functions it owns, from the range index (no functions read).

        `sample_ids` restricts the answer to those samples, and matters more than it looks: the
        caller in the matching path only needs counts for samples that received a vote - at most
        a few thousand - and building the whole-corpus map for that is the one place the query
        path still scaled with corpus size. At the sizes measured here it is invisible (5,034
        entries), but the map is one entry per contiguous run of function ids, so at 10^9 samples
        it is a 10^9-entry dict built per matching job. Passing the ids turns it into an indexed
        lookup of what is actually needed.

        A sample can own several runs when its function ids are not contiguous, so the sizes are
        summed per sample rather than assigned.
        """
        if sample_ids is None:
            (firsts, lasts), all_sample_ids = self._getFunctionRangeLookup()
            if not firsts.size:
                return {}
            sizes = (lasts - firsts + 1).tolist()
            counts: Dict[int, int] = {}
            for sample_id, size in zip(all_sample_ids.tolist(), sizes):
                counts[sample_id] = counts.get(sample_id, 0) + size
            return counts
        wanted = [int(sample_id) for sample_id in sample_ids]
        if not wanted:
            return {}
        counts = {}
        for document in self._getDb()[self._FUNCTION_RANGE_COLLECTION].find(
            {"sample_id": {"$in": wanted}}, {"_id": 0, "sample_id": 1, "first_function_id": 1, "last_function_id": 1}
        ):
            size = document["last_function_id"] - document["first_function_id"] + 1
            counts[document["sample_id"]] = counts.get(document["sample_id"], 0) + size
        return counts

    def getSampleIdsForFunctionIdArray(self, function_ids: "np.ndarray") -> "np.ndarray":
        """Sample id per function id, or -1 where the range index cannot answer.

        Vectorised on purpose: this runs over every candidate a query produced, and a Python
        loop there would cost more than the lookup it replaces.
        """
        (firsts, lasts), sample_ids = self._getFunctionRangeLookup()
        if not firsts.size:
            return np.full(function_ids.shape, -1, dtype=np.int64)
        positions = np.searchsorted(firsts, function_ids, side="right") - 1
        result = np.full(function_ids.shape, -1, dtype=np.int64)
        valid = positions >= 0
        candidate_positions = positions[valid]
        # a function id past its span's end belongs to no sample the index knows about
        inside = function_ids[valid] <= lasts[candidate_positions]
        resolved = np.where(inside, sample_ids[candidate_positions], -1)
        result[valid] = resolved
        return result

    def isBandDfIndexComplete(self) -> bool:
        settings_document = self._getDb().settings.find_one({}, {self._BAND_DF_SETTING: 1})
        return bool(settings_document and settings_document.get(self._BAND_DF_SETTING))

    def _setBandDfIndexComplete(self, is_complete: bool) -> None:
        self._getDb().settings.update_one({}, {"$set": {self._BAND_DF_SETTING: bool(is_complete)}})

    def rebuildBandDfIndex(self, progress_reporter=None) -> int:
        """Set df on every band document and index (band_hash, df); returns documents updated.

        Needed once on a database built before df existed. Until it has run, a configured
        cutoff still applies - it just cannot skip the read, so it saves less.
        """
        bucket_size = self._bandBucketSize()
        num_updated = 0
        for band_number in range(self._storage_config.STORAGE_NUM_BANDS):
            collection = self._getDb()["band_%d" % band_number]
            if bucket_size:
                num_updated += self._rebuildBandBookkeepingBucketed(collection)
            else:
                result = collection.update_many({}, [{"$set": {"df": {"$size": {"$ifNull": ["$function_ids", []]}}}}])
                num_updated += result.modified_count
            collection.create_index([("band_hash", 1), ("df", 1)])
            collection.create_index([("band_hash", 1), ("bucket", 1)])
            if progress_reporter is not None:
                progress_reporter.step()
        self._setBandDfIndexComplete(True)
        LOGGER.info("Band df index rebuilt over %d documents.", num_updated)
        return num_updated

    def _rebuildBandBookkeepingBucketed(self, collection) -> int:
        """Set df/tail/tail_n on bucket 0 for every hash in one band collection; returns hashes done.

        Doubles as the migration onto the bucketed shape. A collection written before bucketing
        was switched on has one document per hash and no `bucket` field, which the bucketed
        upsert filter `{band_hash, bucket: 0}` would not match - it would insert a *second*
        document for the hash and split the posting list invisibly. Stamping `bucket: 0` here is
        what makes those documents addressable, so this has to run after enabling the knob and
        before the next write.

        Writes in batches rather than one bulk_write over the whole collection, because the
        rebuild is the one operation whose cost does follow corpus size and a single batch of
        millions of updates is how an offline rebuild turns into an outage.
        """
        num_hashes = 0
        pending = []
        for row in collection.aggregate(
            [
                {"$project": {"band_hash": 1, "bucket": {"$ifNull": ["$bucket", 0]}, "n": {"$size": {"$ifNull": ["$function_ids", []]}}}},
                {"$group": {"_id": "$band_hash", "df": {"$sum": "$n"}, "tail": {"$max": "$bucket"}, "buckets": {"$push": {"bucket": "$bucket", "n": "$n"}}}},
            ],
            allowDiskUse=True,
        ):
            tail = int(row["tail"] or 0)
            tail_n = 0
            for entry in row["buckets"]:
                if int(entry["bucket"] or 0) == tail:
                    tail_n = int(entry["n"])
                    break
            pending.append(UpdateOne({"band_hash": row["_id"], "bucket": {"$in": [0, None]}}, {"$set": {"bucket": 0, "df": int(row["df"]), "tail": tail, "tail_n": tail_n}}))
            if len(pending) >= 5000:
                collection.bulk_write(pending, ordered=False)
                num_hashes += len(pending)
                pending = []
        if pending:
            collection.bulk_write(pending, ordered=False)
            num_hashes += len(pending)
        return num_hashes

    def isPicHashCountIndexComplete(self) -> bool:
        settings_document = self._getDb().settings.find_one({}, {self._PICHASH_COUNT_SETTING: 1})
        return bool(settings_document and settings_document.get(self._PICHASH_COUNT_SETTING))

    def _setPicHashCountIndexComplete(self, is_complete: bool) -> None:
        self._getDb().settings.update_one({}, {"$set": {self._PICHASH_COUNT_SETTING: bool(is_complete)}})

    def _addToPicHashCounts(self, function_documents: List[Dict]) -> None:
        """Count the pichashes these functions carry, one $inc per distinct hash."""
        if not self.isPicHashCountIndexComplete():
            return
        increments: Dict[Any, int] = {}
        for function_document in function_documents:
            encoded_pichash = function_document.get("_pichash")
            if encoded_pichash is not None:
                increments[encoded_pichash] = increments.get(encoded_pichash, 0) + 1
        if not increments:
            return
        operations = [UpdateOne({"_pichash": encoded}, {"$inc": {"df": count}}, upsert=True) for encoded, count in increments.items()]
        self._getDb()[self._PICHASH_COUNT_COLLECTION].bulk_write(operations, ordered=False)

    def rebuildPicHashCountIndex(self, progress_reporter=None) -> int:
        """Count holders per pichash from the functions collection; returns distinct hashes.

        Two implementations live below. STORAGE_REBUILD_PARTITION_SIZE selects between them: 0,
        the default, keeps the single-`$group` original, and a positive value switches to the
        partitioned scan. Both are kept rather than one replacing the other, so that they can be
        measured against each other and so a corpus that violates the partitioned path's
        precondition in a way its postcondition misses still has a way back.
        """
        partition_size = int(getattr(self._storage_config, "STORAGE_REBUILD_PARTITION_SIZE", 0) or 0)
        if partition_size <= 0:
            return self._rebuildPicHashCountIndexGrouped(progress_reporter=progress_reporter)
        return self._rebuildPicHashCountIndexPartitioned(partition_size, progress_reporter=progress_reporter)

    def _rebuildPicHashCountIndexGrouped(self, progress_reporter=None) -> int:
        """The original rebuild, still the default: one `$group` over every pichash, then upserts.

        Both halves hold state shaped like the corpus. The `$group` is blocking and its
        accumulator table holds one entry per *distinct* pichash; past
        `internalDocumentSourceGroupMaxMemoryBytes` (100 MB by default) it spills to disk and the
        rebuild pays external merge I/O on top of the scan. The upserts then arrive in the
        group's output order rather than key order, so each dirties a random page of an index
        that is itself growing. STORAGE_REBUILD_PARTITION_SIZE opts out of both - what each one
        costs is measured on the `research/scaling-notes` branch under docs/scaling/.
        """
        collection = self._getDb()[self._PICHASH_COUNT_COLLECTION]
        # same reasoning as the range index: a hash with no count document is *excluded* by the
        # indexed filter, so a rebuild that left the flag true would drop exact matches for
        # every hash it had not yet counted
        self._setPicHashCountIndexComplete(False)
        collection.delete_many({})
        # Create the index *before* the upserts, not after. Every upsert matches on _pichash, so
        # without it each one is a collection scan against everything written so far - quadratic,
        # and measured at ~35 upserts/s where an indexed rebuild does thousands.
        collection.create_index([("_pichash", 1), ("df", 1)])
        pipeline = [{"$match": {"_pichash": {"$ne": None}}}, {"$group": {"_id": "$_pichash", "df": {"$sum": 1}}}]
        operations = []
        num_hashes = 0
        for group in self._getDb().functions.aggregate(pipeline, allowDiskUse=True):
            operations.append(UpdateOne({"_pichash": group["_id"]}, {"$set": {"_pichash": group["_id"], "df": group["df"]}}, upsert=True))
            num_hashes += 1
            if len(operations) >= 10000:
                collection.bulk_write(operations, ordered=False)
                operations = []
                if progress_reporter is not None:
                    progress_reporter.step()
        if operations:
            collection.bulk_write(operations, ordered=False)
        self._setPicHashCountIndexComplete(True)
        LOGGER.info("PicHash count index rebuilt over %d distinct hashes.", num_hashes)
        return num_hashes

    def _iteratePicHashRuns(self, partition_size: int) -> Iterator[Tuple[Any, int]]:
        """Yield (encoded pichash, holders) for every non-null pichash, in index order.

        The whole point of the partitioned rebuild. `_pichash` is indexed and the projection is
        covered (verified with explain: PROJECTION_COVERED over IXSCAN `_pichash_1`), so the
        scan walks the index rather than the documents, and it arrives *sorted*. Equal hashes
        are therefore adjacent, which is what lets the counting be a run length held in two
        local variables instead of a hash table the size of the vocabulary.

        Each partition is an independent `find` of at most `partition_size` index keys - bounded
        work, bounded memory, no long-lived cursor to time out on a multi-hour rebuild - resumed
        by a keyset bound on the last key seen. A run cut by the partition boundary is *not*
        emitted; the next partition restarts inclusively at its key and counts it from the
        beginning, so a boundary can never split a count.

        The one case that needs care is a hash held by more than `partition_size` functions: the
        partition is then a single run, restarting inclusively would not advance, and the loop
        would not terminate. That run is counted with an indexed `count_documents` instead - one
        COUNT_SCAN over its own contiguous index range - and the bound then moves past it.

        This pages with `$gte`/`$gt`, which MongoDB brackets by BSON type, so it would silently
        stop at the end of the string bracket if a corpus held pichashes of another type.
        `_encodePichash` only ever writes `hex()`, i.e. a string, and the caller verifies the
        total against an independent count rather than trusting that.
        """
        functions = self._getDb().functions
        condition: Dict[str, Any] = {"$ne": None}
        while True:
            cursor = functions.find({"_pichash": condition}, {"_id": 0, "_pichash": 1}).sort("_pichash", 1).limit(partition_size)
            run_key: Any = None
            run_length = 0
            num_keys = 0
            num_runs = 0
            for function_document in cursor:
                encoded_pichash = function_document["_pichash"]
                num_keys += 1
                if encoded_pichash != run_key:
                    if run_key is not None:
                        yield run_key, run_length
                    run_key, run_length, num_runs = encoded_pichash, 0, num_runs + 1
                run_length += 1
            if run_key is None:
                return
            if num_keys < partition_size:
                # the cursor ended on its own, so the trailing run is complete as well
                yield run_key, run_length
                return
            if num_runs == 1:
                yield run_key, functions.count_documents({"_pichash": run_key})
                condition = {"$gt": run_key}
            else:
                condition = {"$gte": run_key}

    def _rebuildPicHashCountIndexPartitioned(self, partition_size: int, progress_reporter=None) -> int:
        """Rebuild the counts from a partitioned, sorted index scan; returns distinct hashes.

        Writes with `insert_many` rather than upserts: the collection was just emptied and the
        flag is false, which stops `_addToPicHashCounts` from writing concurrently, so every
        write is known to be an insert and the upsert's match is pure overhead. The keys arrive
        ascending, so the index fills at its right edge instead of being dirtied at random.

        The result is checked, not assumed: the counted holders must equal an independent count
        of the functions carrying a pichash. If they disagree - a pichash of an unexpected BSON
        type, or a writer that ran during the rebuild - the counts are discarded and the grouped
        rebuild runs instead, so a violated precondition costs time and not correctness.
        """
        collection = self._getDb()[self._PICHASH_COUNT_COLLECTION]
        self._setPicHashCountIndexComplete(False)
        collection.delete_many({})
        collection.create_index([("_pichash", 1), ("df", 1)])
        documents = []
        num_hashes = 0
        num_holders = 0
        for encoded_pichash, holders in self._iteratePicHashRuns(partition_size):
            documents.append({"_pichash": encoded_pichash, "df": holders})
            num_hashes += 1
            num_holders += holders
            if len(documents) >= self._PICHASH_COUNT_WRITE_BATCH:
                collection.insert_many(documents, ordered=False)
                documents = []
                if progress_reporter is not None:
                    progress_reporter.step()
        if documents:
            collection.insert_many(documents, ordered=False)
        expected_holders = self._getDb().functions.count_documents({"_pichash": {"$ne": None}})
        if num_holders != expected_holders:
            LOGGER.warning(
                "Partitioned pichash count rebuild saw %d holders where the functions collection has %d - falling back to the grouped rebuild.",
                num_holders,
                expected_holders,
            )
            return self._rebuildPicHashCountIndexGrouped(progress_reporter=progress_reporter)
        self._setPicHashCountIndexComplete(True)
        LOGGER.info("PicHash count index rebuilt over %d distinct hashes (%d holders, partition size %d).", num_hashes, num_holders, partition_size)
        return num_hashes

    def _getCacheDataForFunctionIds(self, function_ids: List[int]) -> Dict:
        cache_data = {}
        sample_ids = {}
        sample_to_func_ids = {}
        minhashes = {}
        interned_signatures: Dict[bytes, bytes] = {}
        # process this in batches as the number of function_ids can be exceedingly large, pushing beyond Mongo's 16M limit
        positive_function_ids = [function_id for function_id in set(function_ids) if function_id >= 0]
        negative_function_ids = [function_id for function_id in set(function_ids) if function_id < 0]
        # This call is latency-bound, not bandwidth-bound: the projection returns 186 B per
        # document but WiredTiger reads the whole ~4 kB record to produce it, so a single
        # sequential cursor leaves mongod's I/O parallelism unused. Measured on 500k ids:
        # 1 thread 12.96 s, 8 threads 1.90 s (6.81x) for a byte-identical result. Threads, not
        # processes - pymongo releases the GIL for socket I/O and BSON decode.
        fetch_threads = max(1, getattr(self._storage_config, "STORAGE_CACHE_FETCH_THREADS", 1))
        slice_size = max(1, getattr(self._storage_config, "STORAGE_CACHE_FETCH_SLICE_SIZE", 500000))
        if fetch_threads > 1:
            # the two knobs are not independent: with the 500k default almost every batch is a
            # single slice and the pool is never built, so the thread count silently does
            # nothing (measured: 1.09 effective workers on bumblebee). Derive it instead.
            # aim for several slices per worker rather than exactly one: equal-sized slices
            # do not take equal time, and with one slice each a single straggler leaves the
            # other workers idle. Measured on citadel (824k ids, 8 threads): one slice per
            # worker 65.2 s, four 46.4 s.
            # Several slices per worker, because equal-sized slices do not take equal time.
            # But floor it: with fix 01 the per-batch delta shrinks steadily, and dividing a
            # small delta by 32 produces dozens of tiny round trips whose per-query overhead
            # costs more than the concurrency saves.
            widest = max(len(positive_function_ids), len(negative_function_ids))
            target = -(-widest // (fetch_threads * 4))
            slice_size = max(10000, min(slice_size, target))
        for collection_name, collection_ids in (
            ("functions", positive_function_ids),
            ("query_functions", negative_function_ids),
        ):
            if not collection_ids:
                continue
            slices = [[function_id for function_id in sliced_ids if function_id is not None] for sliced_ids in zip_longest(*[iter(collection_ids)] * slice_size)]
            if fetch_threads > 1 and len(slices) > 1:
                with ThreadPoolExecutor(max_workers=min(fetch_threads, len(slices))) as pool:
                    # map preserves input order, so the resulting dicts are built in the same
                    # order a sequential fetch would build them
                    decoded_slices = list(pool.map(lambda ids: self._fetchCacheSlice(collection_name, ids), slices))
            else:
                decoded_slices = [self._fetchCacheSlice(collection_name, ids) for ids in slices]
            for slice_rows, slice_signatures in decoded_slices:
                # One decoded signature object per distinct signature, shared by every function
                # that carries it. The slices deduplicate within themselves while decoding; this
                # merges their tables into one so the sharing also spans slices, collections and
                # fetch threads. The per-function work left is a list index, where it used to be
                # a hex decode of the full signature.
                shared_signatures = [interned_signatures.setdefault(signature, signature) for signature in slice_signatures]
                for function_id, sample_id, signature_index in slice_rows:
                    minhashes[function_id] = shared_signatures[signature_index]
                    sample_ids[function_id] = sample_id
                    if sample_id not in sample_to_func_ids:
                        sample_to_func_ids[sample_id] = set()
                    sample_to_func_ids[sample_id].add(function_id)
        cache_data["func_id_to_minhash"] = minhashes
        cache_data["func_id_to_sample_id"] = sample_ids
        cache_data["sample_id_to_func_ids"] = sample_to_func_ids
        # the dedup factor is the one number that explains how much of this fetch was
        # redundant, and it is corpus-shaped: it grows with the corpus (fitted Heaps' law puts
        # it near 24x at a million samples), so a deployment can see its own instead of
        # inheriting the measured one
        if minhashes:
            LOGGER.info(
                "MatchingCache fetch: %d functions over %d distinct signatures (%.2fx)",
                len(minhashes),
                len(interned_signatures),
                len(minhashes) / max(1, len(interned_signatures)),
            )
        return cache_data

    def _fetchCacheSlice(self, collection_name: str, query_function_ids: List[int]) -> Tuple[List[Tuple[int, int, int]], List[bytes]]:
        """One $in query's worth of the slice, deduplicated by signature.

        Returns `(rows, signatures)`, where each row is `(function_id, sample_id,
        signature_index)` and `signatures[signature_index]` is the decoded MinHash. Candidate
        sets are heavily duplicated - measured 2.46x distinct-signature dedup on 257 real
        Malpedia samples, and the fitted Heaps' law puts that near 24x at a million - so
        decoding per function decodes the same signature over and over. Indexing into a table
        of the distinct ones is exactly equivalent (the decode is a pure function of the stored
        hex string) and does the decode once per distinct signature instead of once per
        candidate function.

        The documents still have to be read one per function: the row also carries `sample_id`,
        which is genuinely per-function, and nothing stored lets the fetch ask for "the distinct
        signatures of these function ids" without a signature-keyed index that does not exist.

        Kept free of shared state so it can be run from a thread pool; merging the per-slice
        tables into one happens in the calling thread, in input order.
        """
        rows: List[Tuple[int, int, int]] = []
        signatures: List[bytes] = []
        index_by_hex: Dict[str, int] = {}
        for function_document in self._getDb()[collection_name].find(
            {"function_id": {"$in": query_function_ids}},
            {"_id": 0, "sample_id": 1, "minhash": 1, "function_id": 1},
        ):
            hex_minhash = function_document["minhash"]
            signature_index = index_by_hex.get(hex_minhash)
            if signature_index is None:
                signature_index = len(signatures)
                index_by_hex[hex_minhash] = signature_index
                signatures.append(bytes.fromhex(hex_minhash))
            rows.append((function_document["function_id"], function_document["sample_id"], signature_index))
        return rows, signatures

    def deleteXcfgForSampleId(self, sample_id: int) -> None:
        function_ids = [document["function_id"] for document in self._getDb().functions.find({"sample_id": sample_id}, {"function_id": 1, "_id": 0})]
        self._deleteXcfgForFunctionIds(function_ids)

    def deleteXcfgData(self) -> None:
        # dropping the collection reclaims the space, where blanking the field in place did not:
        # STORAGE_DROP_DISASSEMBLY now genuinely shrinks the instance (#137)
        self._getDb()["xcfg"].drop()

    # TODO move to storage interface, remove this from memory storage?
    def getLibraryInfoForSampleId(self, sample_id: int) -> Optional[Dict[str, str]]:
        sample_entry = self.getSampleById(sample_id)
        if sample_entry is None:
            return None
        if sample_entry.is_library:
            return {"family": sample_entry.family, "version": sample_entry.version}
        return None

    def isPicHash(self, pichash: int) -> bool:
        query = {"pichash": pichash}
        self._encodePichash(query)
        return self._getDb().functions.find_one(query) is not None

    def getMatchesForPicHash(self, pichash: int) -> Set[Tuple[int, int, int]]:
        query = {"pichash": pichash}
        self._encodePichash(query)
        return set(
            map(
                lambda x: (x["family_id"], x["sample_id"], x["function_id"]),
                list(self._getDb().functions.find(query, {"family_id": 1, "function_id": 1, "sample_id": 1, "_id": 0})),
            )
        )

    def getMatchesForPicBlockHash(self, picblockhash: int) -> Set[Tuple[int, int, int, int]]:
        query = {"_picblockhashes.hash": hex(picblockhash)}
        result = self._getDb().functions.aggregate(
            [
                {"$match": query},
                {"$unwind": "$_picblockhashes"},
                {"$match": query},
                {"$project": {"_id": 0, "function_id": 1, "family_id": 1, "sample_id": 1, "offset": "$_picblockhashes.offset"}},
            ]
        )
        return set(
            map(
                # use two-complement to convert unit64 to int64 and vice versa
                lambda x: (x["family_id"], x["sample_id"], x["function_id"], decode_two_complement(x["offset"])),
                result,
            )
        )

    ########################## 'old' implementations below

    def _getFunctionDocument(self, sample_entry: "SampleEntry", smda_function: "SmdaFunction", function_id: int) -> Dict:
        """
        Converts a SmdaFunction to a dict ready for insertion into MongoDb.
        Picblockhashes will be added to the function.
        The function_id has to be explicitly provided.
        """
        function_entry = FunctionEntry(sample_entry, smda_function, function_id)
        # calculate block hashes and add separately
        image_lower = sample_entry.base_addr
        image_upper = image_lower + sample_entry.binary_size
        picblockhashes = []
        for hash_entry in self.blockhasher.getBlockhashesForFunction(smda_function, image_lower, image_upper, hash_size=8):
            for block_entry in hash_entry["offset_tuples"]:
                block_entry["hash"] = hash_entry["hash"]
                picblockhashes.append(block_entry)
        function_entry.picblockhashes = picblockhashes
        # convert for persistance
        function_dict = function_entry.toDict()
        self._encodeFunction(function_dict)
        return function_dict

    def createMatchingCache(self, function_ids: List[int], allow_self_return: bool = False, previous: Optional[MatchingCache] = None) -> MatchingCache:
        if not getattr(self._storage_config, "STORAGE_MATCHING_CACHE_PERSIST", False):
            cache_data = self._getCacheDataForFunctionIds(function_ids)
            return MatchingCache(cache_data)
        return self._createIncrementalMatchingCache(function_ids, previous)

    def _createIncrementalMatchingCache(self, function_ids: List[int], previous: Optional[MatchingCache] = None) -> MatchingCache:
        """Serve a batch from a job-scoped cache, fetching only what is not already held.

        The per-batch rebuild re-fetches the same candidate signatures many times over
        (measured 1.54x-12.21x on real samples). Retaining them across the batches of one job
        turns total fetches into the distinct union, and decouples the batch size from I/O
        cost entirely.

        The cache is owned by the calling matcher, which passes it back in as `previous` on
        every batch after the first - the storage object holds NO reference to it. One
        matcher instance is one job (worker) or one request (server), so concurrent matchers
        on a shared storage can never see each other's entries, evict each other's rows, or
        overwrite each other's query minhashes (the function_id = -1 collision of #110).

        Only entries fetched from storage are evictable (tracked in cache._evictable).
        Entries injected by the query matchers via addFunctionEntriesToCache are left alone,
        and the whole cache dies with the matcher that owns it, so nothing leaks between jobs.
        """
        requested = set(function_ids)
        cache = previous
        if cache is None:
            cache = MatchingCache({"func_id_to_minhash": {}, "func_id_to_sample_id": {}, "sample_id_to_func_ids": {}})
        held = cache._func_id_to_minhash
        missing = [function_id for function_id in requested if function_id not in held]

        max_entries = getattr(self._storage_config, "STORAGE_MATCHING_CACHE_MAX_ENTRIES", 0)
        if max_entries:
            overflow = len(held) + len(missing) - max_entries
            if overflow > 0:
                self._evictFromMatchingCache(cache, overflow, protected=requested)

        if missing:
            cache_data = self._getCacheDataForFunctionIds(missing)
            fetched_sample_ids = cache_data["func_id_to_sample_id"]
            for function_id, minhash in cache_data["func_id_to_minhash"].items():
                cache._setFunctionEntry(function_id, fetched_sample_ids[function_id], minhash)
                cache._evictable[function_id] = None

        # refresh recency for everything this batch needs, so eviction is least-recently-needed
        for function_id in requested:
            if function_id in cache._evictable:
                cache._evictable.move_to_end(function_id)
        return cache

    def _evictFromMatchingCache(self, cache: MatchingCache, count: int, protected: Set[int]) -> int:
        evicted = 0
        for function_id in list(cache._evictable):
            if evicted >= count:
                break
            if function_id in protected:
                continue
            sample_id = cache._func_id_to_sample_id.pop(function_id, None)
            cache._func_id_to_minhash.pop(function_id, None)
            if sample_id is not None and sample_id in cache._sample_id_to_func_ids:
                cache._sample_id_to_func_ids[sample_id].discard(function_id)
                if not cache._sample_id_to_func_ids[sample_id]:
                    del cache._sample_id_to_func_ids[sample_id]
            del cache._evictable[function_id]
            evicted += 1
        if evicted:
            cache.invalidateSignatureMatrix()
            # the ceiling is otherwise silent when it binds, which has already caused wrong
            # performance readings - make every eviction visible
            LOGGER.info(
                "MatchingCache eviction: evicted %d of %d requested, %d entries retained (ceiling %d)",
                evicted,
                count,
                len(cache._func_id_to_minhash),
                getattr(self._storage_config, "STORAGE_MATCHING_CACHE_MAX_ENTRIES", 0),
            )
        return evicted

    def clearMatchingCache(self) -> None:
        # the matching cache is owned by the matcher that created it and dies with that
        # matcher; the storage object holds no reference to it, so there is nothing to clear
        return

    def getFamilyId(self, family_name: str) -> Optional[int]:
        family_document = self._getDb().families.find_one({"family_name": family_name})
        if family_document is None:
            return None
        return family_document["family_id"]

    def getFamilyIds(self) -> List[int]:
        return self._getDb().families.find().distinct("family_id")

    def addFamily(self, family_name: str) -> int:
        family_document = self._getDb().families.find_one({"family_name": family_name})
        if family_document is not None:
            return family_document["family_id"]
        family_id = self._useCounter("families")
        family_entry = FamilyEntry(family_name=family_name, family_id=family_id)
        self._dbInsert("families", family_entry.toDict())
        self._updateDbState()
        return family_id

    def getFamily(self, family_id: int) -> Optional[FamilyEntry]:
        family_document = self._getDb().families.find_one({"family_id": family_id})
        if family_document is None:
            return None
        return FamilyEntry.fromDict(family_document)

    def getFamilyEntriesByIds(self, family_ids: List[int]) -> Dict[int, "FamilyEntry"]:
        """One $in query instead of one find_one per family.

        Ids without a family document are simply absent from the result, matching
        getFamily's None for them.
        """
        entries: Dict[int, FamilyEntry] = {}
        for family_document in self._getDb().families.find({"family_id": {"$in": family_ids}}, {"_id": 0}):
            entry = FamilyEntry.fromDict(family_document)
            entries[entry.family_id] = entry
        return entries

    def getFunctionById(self, function_id: int, with_xcfg=False) -> Optional["FunctionEntry"]:
        field_selection = {"_id": 0}
        if function_id < 0:
            function_document = self._getDb().query_functions.find_one({"function_id": function_id}, field_selection)
        else:
            function_document = self._getDb().functions.find_one({"function_id": function_id}, field_selection)
        if not function_document:
            return None
        if with_xcfg:
            # the blob lives in its own collection now (#137); inject it so _decodeFunction
            # and FunctionEntry see exactly the shape they saw before the split. An inline
            # `_xcfg` (pre-migration document, see _attachXcfgBlobs) is a fallback, not
            # something to overwrite with an empty dict.
            # "{}" when no blob is stored, so the caller still gets an empty dict rather than
            # None: xcfg is None means "not requested", {} means "disassembly dropped". Before
            # the split that distinction came from the field being blanked in place instead of
            # removed, and it is part of the contract (tests/testStorage.py).
            function_document["_xcfg"] = self._fetchXcfgBlobs([function_id]).get(function_id) or function_document.get("_xcfg") or "{}"
        self._decodeFunction(function_document)
        return FunctionEntry.fromDict(function_document)

    def getSampleById(self, sample_id: int) -> Optional["SampleEntry"]:
        if sample_id < 0:
            sample_document = self._getDb().query_samples.find_one({"sample_id": sample_id}, {"_id": 0})
        else:
            sample_document = self._getDb().samples.find_one({"sample_id": sample_id}, {"_id": 0})
        if not sample_document:
            return None
        return SampleEntry.fromDict(sample_document)

    def getSampleEntriesByIds(self, sample_ids: List[int]) -> Dict[int, "SampleEntry"]:
        """One $in per collection instead of one find_one per sample (N+1, #111).

        Ids without a sample document are simply absent from the result, matching
        getSampleById's None for them.
        """
        entries: Dict[int, SampleEntry] = {}
        query_ids = [sample_id for sample_id in sample_ids if sample_id < 0]
        corpus_ids = [sample_id for sample_id in sample_ids if sample_id >= 0]
        for collection_name, collection_ids in (("query_samples", query_ids), ("samples", corpus_ids)):
            if not collection_ids:
                continue
            for sample_document in self._getDb()[collection_name].find({"sample_id": {"$in": collection_ids}}, {"_id": 0}):
                entry = SampleEntry.fromDict(sample_document)
                entries[entry.sample_id] = entry
        return entries

    # TODO add types
    def getStats(self, with_pichash=True) -> Dict:
        # we will have to work around using .aggregate(), as a collection with unique pic hashes will easily exceed 16M
        # https://stackoverflow.com/questions/20348093/mongodb-aggregation-how-to-get-total-records-count
        # None signals "not computed", so that consumers can distinguish it from an actual count of 0
        num_unique_pichashes = None
        if with_pichash:
            num_unique_pichashes = 0
            for result in self._getDb().functions.aggregate([{"$group": {"_id": "$_pichash"}}, {"$count": "Total"}]):
                num_unique_pichashes = result["Total"]
        # use family statistics to derive relevant values
        stats = {
            "db_state": self._getDbState(),
            "db_timestamp": self._convertDatetimeToString(self._getDbTimestamp()),
            "num_families": 0,
            "num_samples": 0,
            "num_query_samples": self._getDb().query_samples.estimated_document_count(),
            "num_query_functions": self._getDb().query_functions.estimated_document_count(),
            "num_functions": 0,
            "num_bands": self._storage_config.STORAGE_NUM_BANDS,
            "num_pichashes": num_unique_pichashes,
        }
        for family_document in self._getDb().families.find():
            stats["num_families"] += 1
            stats["num_samples"] += family_document["num_samples"]
            stats["num_functions"] += family_document["num_functions"]
        return stats

    def _hashableSizeQuery(self) -> Optional[Dict]:
        """Query fragment for "this function is large enough to receive a MinHash".

        Mirrors StorageInterface.isHashableBySize. Returns None when no threshold is active, in
        which case nothing is hashable and the caller should not query at all.
        """
        clauses = []
        if self._minhash_config.MINHASH_FN_MIN_BLOCKS:
            clauses.append({"num_blocks": {"$gt": self._minhash_config.MINHASH_FN_MIN_BLOCKS}})
        if self._minhash_config.MINHASH_FN_MIN_INS:
            clauses.append({"num_instructions": {"$gt": self._minhash_config.MINHASH_FN_MIN_INS}})
        if not clauses:
            return None
        return clauses[0] if len(clauses) == 1 else {"$or": clauses}

    def getUnhashedFunctions(self, function_ids: Optional[List[int]] = None, only_function_ids=False) -> List[Union[int, "FunctionEntry"]]:
        # Select in the database rather than in Python. Both filters matter on a large corpus:
        # "minhash is empty" is trivially indexable work the server should not ship to the client,
        # and the size filter keeps functions that can never BE hashed out of the result. Without
        # it, every caller re-fetches the disassembly of every sub-threshold function to rediscover
        # it is too small - on an 11.6M-function corpus that was 4.16M of the 10.01M functions with
        # an empty minhash, on every invocation, which is enough to stall a repair and exhaust the
        # memory of the process pool that consumes the result.
        hashable_query = self._hashableSizeQuery()
        if hashable_query is None:
            return []
        search_query = {"minhash": "", **hashable_query}
        if function_ids is not None:
            search_query["function_id"] = {"$in": list(function_ids)}
        # only_function_ids wants exactly one field; projecting it keeps the scan from dragging
        # every other field of every matching document across the wire for nothing.
        projection = {"function_id": 1, "_id": 0} if only_function_ids else {"_id": 0}
        unhashed_functions = []
        pending_documents = []
        for function_document in self._getDb().functions.find(search_query, projection):
            if only_function_ids:
                unhashed_functions.append(function_document["function_id"])
            else:
                pending_documents.append(function_document)
        # this feeds minhash computation, which needs the disassembly
        self._attachXcfgBlobs(pending_documents)
        for function_document in pending_documents:
            self._decodeFunction(function_document)
            unhashed_functions.append(FunctionEntry.fromDict(function_document))
        return unhashed_functions

    def rebuildMinhashBandIndex(self, progress_reporter=None):
        # Drop every band_<n> present, not just the ones the current config expects. An explicit
        # projection makes changing the band count easy, and dropping only band_0..NUM_BANDS-1
        # would leave the surplus collections behind holding entries under the previous
        # projection. Matching never reads them - it only iterates band_0..NUM_BANDS-1 - so they
        # would sit there consuming disk until someone raised the band count again and started
        # reading stale keys.
        existing_bands = [name for name in self._getDb().list_collection_names() if re.match(r"^band_\d+$", name)]
        for c in existing_bands:
            self._getDb()[c].drop()
        # recreate collections and their indices
        collections = []
        for band_id in range(self._storage_config.STORAGE_NUM_BANDS):
            collections.append("band_%d" % band_id)
        for c in collections:
            self._getDb()[c].create_index("band_hash")
            # the df cutoff reads (band_hash, df); without this the rebuilt bands would carry a
            # correct df - _updateBands maintains it by $inc from the first write - that no index
            # could serve, so STORAGE_BAND_DF_CUTOFF would silently fall back to scanning
            self._getDb()[c].create_index([("band_hash", 1), ("df", 1)])
            # bucketed writes upsert on (band_hash, bucket); without this the upsert scans the
            # hash's buckets and, worse, two concurrent writers can both miss and insert
            self._getDb()[c].create_index([("band_hash", 1), ("bucket", 1)])
        # re-add minhashes in batches
        total_functions = self._getDb().functions.count_documents(filter={})
        minhash_functions = 0
        batch_size = self._minhash_config.MINHASH_BAND_REBUILD_WORK_PACKAGE_SIZE
        if progress_reporter:
            progress_reporter.set_total((total_functions // batch_size) + 1)
        # keyset paging on the indexed function_id, as skip() would walk and discard all skipped documents on every batch
        last_function_id = None
        while True:
            batch_query = {} if last_function_id is None else {"function_id": {"$gt": last_function_id}}
            function_documents = list(self._getDb().functions.find(batch_query, {"function_id": 1, "minhash": 1, "_id": 0}).sort("function_id", 1).limit(batch_size))
            if not function_documents:
                break
            last_function_id = function_documents[-1]["function_id"]
            minhashes = []
            for function_document in function_documents:
                if function_document["minhash"]:
                    function_id = function_document["function_id"]
                    minhash_bytes = bytes.fromhex(function_document["minhash"])
                    minhash_obj = MinHash(function_id, minhash_bytes, minhash_bits=self._minhash_config.MINHASH_SIGNATURE_BITS)
                    minhashes.append(minhash_obj)
            self._addMinHashesToBands(minhashes)
            minhash_functions += len(minhashes)
            if progress_reporter:
                progress_reporter.step()
        return {"minhash_functions_indexed": minhash_functions}

    def deleteAllMinHashes(self, progress_reporter=None):
        # delete all minhashes
        self._getDb().functions.update_many({}, {"$set": {"minhash": ""}})
        # reset bands
        collections = []
        for band_id in range(self._storage_config.STORAGE_NUM_BANDS):
            collections.append("band_%d" % band_id)
        for c in collections:
            self._getDb()[c].drop()
            self._getDb()[c].create_index("band_hash")
        LOGGER.info("Dropped all Minhashes and created a fresh banding index.")
        return

    def recalculateAllPicHashes(self, progress_reporter=None):
        # get current SMDA version
        smda_config = SmdaConfig()
        smda_version = smda_config.VERSION
        smda_downward_compatibility = getattr(smda_config, "ESCAPER_DOWNWARD_COMPATIBILITY", None)
        if smda_downward_compatibility is None:
            LOGGER.warning("SMDA downward compatibility version unknown, using current SMDA version as threshold...")
            smda_downward_compatibility = smda_version
        compatibility_threshold = version.parse(smda_downward_compatibility)
        # get samples where recalculation is necessary
        samples_to_be_updated = {}
        for sample_document in self._getDb().samples.find({}, {"sample_id": 1, "smda_version": 1, "architecture": 1, "base_addr": 1, "binary_size": 1, "bitness": 1, "_id": 0}):
            report_version = sample_document["smda_version"]
            if report_version.startswith("MCRIT4IDA"):
                report_version = report_version.rsplit(" ", 1)[-1]
            if version.parse(report_version) < compatibility_threshold:
                samples_to_be_updated[sample_document["sample_id"]] = sample_document
        # reprocess functions on a per sample level
        total_samples = len(samples_to_be_updated)
        if progress_reporter:
            progress_reporter.set_total((total_samples))
        functions_updatable = 0
        functions_updated = 0
        picblockhashes_updatable = 0
        picblockhashes_updated = 0
        xcfg_missing = 0
        for sample_id, sample_info in samples_to_be_updated.items():
            pic_hash_updates = []
            sample_function_documents = list(self._getDb().functions.find({"sample_id": sample_id}, {"function_id": 1, "_pichash": 1, "_picblockhashes": 1, "_xcfg": 1, "_id": 0}))
            # one $in for the whole sample rather than a lookup per function (#137); inline
            # `_xcfg` is the fallback for pre-migration documents (see _attachXcfgBlobs)
            sample_xcfg_blobs = self._fetchXcfgBlobs([document["function_id"] for document in sample_function_documents])
            for function_document in sample_function_documents:
                function_document["_xcfg"] = sample_xcfg_blobs.get(function_document["function_id"]) or function_document.get("_xcfg") or ""
                update_document = {}
                functions_updatable += 1
                # create all relevant objects
                binary_info = BinaryInfo(b"")
                binary_info.architecture = sample_info["architecture"]
                binary_info.base_addr = sample_info["base_addr"]
                binary_info.binary_size = sample_info["binary_size"]
                binary_info.bitness = sample_info["bitness"]
                if "_xcfg" not in function_document or not function_document["_xcfg"]:
                    xcfg_missing += 1
                    continue
                smda_xcfg = json.loads(function_document["_xcfg"])
                old_pichash = int(function_document["_pichash"], 16)
                old_blockhashes = []
                if "_picblockhashes" in function_document:
                    for blockhash in function_document["_picblockhashes"]:
                        blockhash["hash"] = int(blockhash["hash"], 16)
                        old_blockhashes.append(blockhash)
                    picblockhashes_updatable += len(old_blockhashes)
                smda_function = SmdaFunction.fromDict(smda_xcfg, binary_info=binary_info)
                new_pichash = smda_function.getPicHash(binary_info)
                if old_pichash != new_pichash:
                    functions_updated += 1
                    update_document["pichash"] = new_pichash
                # check if any blockhashes changed and possibly stage for update
                picblockhashes = []
                for hash_entry in self.blockhasher.getBlockhashesForFunction(smda_function, binary_info.base_addr, binary_info.base_addr + binary_info.binary_size, hash_size=8):
                    for block_entry in hash_entry["offset_tuples"]:
                        block_entry["hash"] = hash_entry["hash"]
                        picblockhashes.append(block_entry)
                set_old = set([(pbh["offset"], pbh["hash"]) for pbh in old_blockhashes])
                set_new = set([(pbh["offset"], pbh["hash"]) for pbh in picblockhashes])
                if len(set_new) != len(set_old.intersection(set_new)):
                    picblockhashes_updated += len(picblockhashes)
                    update_document["picblockhashes"] = picblockhashes
                # prepare single function entry update
                if update_document:
                    self._encodePichash(update_document)
                    update_command = {"$set": update_document}
                    pic_hash_updates.append(UpdateOne({"function_id": function_document["function_id"]}, update_command, upsert=True))
            # batch insert updates for function_entries
            if pic_hash_updates:
                self._getDb().samples.update_one({"sample_id": sample_id}, {"$set": {"smda_version": smda_version}})
                self._getDb().functions.bulk_write(pic_hash_updates, ordered=False)
            if progress_reporter:
                progress_reporter.step()
        self._getDb().command("reIndex", "functions")
        LOGGER.info(
            f"Found {total_samples} outdated samples, {functions_updated}/{functions_updatable} PicHashes and {picblockhashes_updated}/{picblockhashes_updatable} PicBlockHashes were updated."
        )
        if xcfg_missing:
            LOGGER.warning(f"{xcfg_missing} functions could not be updated as there was not CFG available.")
        if picblockhashes_updated:
            # block hashes were rewritten in place, so the inverted index no longer describes the
            # corpus. Marking it incomplete makes getUniqueBlocks fall back to the scan - slow but
            # correct - until rebuildPicBlockHashIndex runs. Updating it incrementally here would
            # mean diffing old against new hashes per function, which is what the rebuild does
            # anyway and does in one pass.
            self._setPicBlockHashIndexComplete(False)
            LOGGER.warning("picblockhash index invalidated by the recalculation - run rebuildPicBlockHashIndex().")
        return {
            "outdated_samples": total_samples,
            "functions_updatable": functions_updatable,
            "functions_updated": functions_updated,
            "picblockhashes_updatable": picblockhashes_updatable,
            "picblockhashes_updated": picblockhashes_updated,
            "xcfg_missing": xcfg_missing,
        }

    ##### picblockhash inverted index #####
    #
    # getUniqueBlocks used to answer "is this block hash unique to the requested samples?" by
    # reading every function in the database that has picblockhashes - 9.1M documents and 51.4M
    # block entries on a Malpedia-sized instance, ~92 s per call, and that cost does not depend on
    # how many samples were asked about. The inverted index answers the same question by reading
    # one document per candidate hash.
    #
    # Why not the existing `_picblockhashes.hash` index instead: it works, but the $in fan-out
    # pulls whole function documents, so it degrades as the request grows - measured 12.7x for one
    # sample but only 2.3x for five, where this index is 407x and 37x. The multi-sample case is the
    # one that matters, since a sample *set* is what the feature is for.

    def _picBlockHashesOfDocuments(self, function_documents: List[Dict]) -> List[str]:
        """The distinct block hashes carried by already-encoded function documents."""
        hashes = set()
        for function_document in function_documents:
            for block_entry in function_document.get("_picblockhashes") or []:
                hashes.add(block_entry["hash"])
        return sorted(hashes)

    def _isPicBlockHashIndexComplete(self) -> bool:
        settings_document = self._getDb().settings.find_one({}, {self._PICBLOCKHASH_INDEX_SETTING: 1})
        return bool(settings_document and settings_document.get(self._PICBLOCKHASH_INDEX_SETTING))

    def _setPicBlockHashIndexComplete(self, is_complete: bool) -> None:
        self._getDb().settings.update_one({}, {"$set": {self._PICBLOCKHASH_INDEX_SETTING: bool(is_complete)}})

    def _addToPicBlockHashIndex(self, function_documents: List[Dict]) -> int:
        """Record, for every block hash these functions carry, that their sample holds it.

        $addToSet with upsert is deliberate: it has set semantics, so a retried or replayed insert
        converges on the same state instead of drifting. That is the difference between this and
        the counters in #151 ($inc, where a lost or repeated update is permanent) - it is what
        makes maintaining this index on the write path defensible rather than another thing that
        silently goes wrong.
        """
        if not self._isPicBlockHashIndexComplete():
            # nothing reads the index while it is not trusted, so there is nothing to keep current;
            # rebuildPicBlockHashIndex() will pick these functions up when it runs
            return 0
        updates = []
        for sample_id in sorted({document["sample_id"] for document in function_documents}):
            of_sample = [document for document in function_documents if document["sample_id"] == sample_id]
            updates.extend(UpdateOne({"_id": block_hash}, {"$addToSet": {"sample_ids": sample_id}}, upsert=True) for block_hash in self._picBlockHashesOfDocuments(of_sample))
        if updates:
            self._getDb()[self._PICBLOCKHASH_INDEX_COLLECTION].bulk_write(updates, ordered=False)
        return len(updates)

    def _removeSampleFromPicBlockHashIndex(self, sample_id: int) -> int:
        """Drop a sample from the posting lists of the block hashes it holds.

        Scoped by _id rather than by a query on sample_ids: the sample's own functions are one
        indexed read, whereas finding its hashes through the index would need a multikey index on
        sample_ids costing 0.24 GB over 12.1M documents. Must therefore run *before* the sample's
        function documents are deleted.

        Emptied documents are deleted rather than left behind. An empty posting list would in fact
        be read correctly here - it means "held by nobody", so the candidate survives - but #149 is
        the same residue in the band index and there is no reason to repeat it.
        """
        if not self._isPicBlockHashIndexComplete():
            return 0
        hashes = self._picBlockHashesOfDocuments(
            list(
                self._getDb().functions.find(
                    {"sample_id": sample_id, "_picblockhashes": {"$exists": True, "$ne": []}},
                    {"_picblockhashes": 1, "_id": 0},
                )
            )
        )
        if not hashes:
            return 0
        collection = self._getDb()[self._PICBLOCKHASH_INDEX_COLLECTION]
        collection.update_many({"_id": {"$in": hashes}}, {"$pull": {"sample_ids": sample_id}})
        collection.delete_many({"_id": {"$in": hashes}, "sample_ids": []})
        return len(hashes)

    def rebuildPicBlockHashIndex(self, progress_reporter=None) -> int:
        """Rebuild the picblockhash index from the functions collection and mark it trustworthy.

        This is the repair path, not the construction path - the write hooks keep the index current
        in normal operation. It exists because every denormalised structure in this storage needs a
        way to be reconstructed when it is wrong (cf. #142, #149, #151), and because
        recalculateAllPicHashes rewrites block hashes wholesale and invalidates it by design.
        """
        pipeline = [
            {"$match": {"_picblockhashes": {"$exists": True, "$ne": []}}},
            {"$unwind": "$_picblockhashes"},
            {"$group": {"_id": "$_picblockhashes.hash", "sample_ids": {"$addToSet": "$sample_id"}}},
            {"$out": self._PICBLOCKHASH_INDEX_COLLECTION},
        ]
        self._getDb().functions.aggregate(pipeline, allowDiskUse=True)
        self._setPicBlockHashIndexComplete(True)
        num_hashes = self._getDb()[self._PICBLOCKHASH_INDEX_COLLECTION].count_documents({})
        LOGGER.info("Rebuilt picblockhash index over %d distinct block hashes.", num_hashes)
        return num_hashes

    def _reduceToUniqueBlocksUsingIndex(self, candidate_picblockhashes: Dict, sample_ids: List[int]) -> None:
        """Drop every candidate the index shows in a sample outside the request."""
        requested_sample_ids = set(sample_ids)
        collection = self._getDb()[self._PICBLOCKHASH_INDEX_COLLECTION]
        candidate_hashes = list(candidate_picblockhashes)
        for offset in range(0, len(candidate_hashes), self._PICBLOCKHASH_INDEX_QUERY_SLICE):
            hash_slice = candidate_hashes[offset : offset + self._PICBLOCKHASH_INDEX_QUERY_SLICE]
            for document in collection.find({"_id": {"$in": hash_slice}}, {"sample_ids": 1}):
                if any(sample_id not in requested_sample_ids for sample_id in document["sample_ids"]):
                    candidate_picblockhashes.pop(document["_id"], None)

    def _reduceToUniqueBlocksByScan(self, candidate_picblockhashes: Dict, sample_ids: List[int], progress_reporter=None) -> None:
        """The pre-index elimination: read every function that has block hashes.

        Kept as the fallback for an instance whose index has not been built yet, so that upgrading
        never silently changes results - it only stays slow until rebuildPicBlockHashIndex runs.
        """
        if progress_reporter is not None:
            progress_reporter.set_total(self._getDb().functions.count_documents(filter={}))
        for entry in self._getDb().functions.find({"_picblockhashes": {"$exists": True, "$ne": []}}, {"sample_id": 1, "_picblockhashes": 1, "_id": 0}):
            if progress_reporter is not None:
                progress_reporter.step()
            if entry["sample_id"] not in sample_ids:
                for block_entry in entry["_picblockhashes"]:
                    candidate_picblockhashes.pop(block_entry["hash"], None)

    def getUniqueBlocks(self, sample_ids: List[int], progress_reporter=None) -> Dict:
        # query once to get all blocks from the functions of our samples
        block_statistics: Dict[str, Any] = {
            "by_sample_id": {sample_id: {"sample_id": sample_id, "total_blocks": 0, "characteristic_blocks": 0, "unique_blocks": 0} for sample_id in sample_ids},
            "unique_blocks_overall": 0,
            "num_samples": len(sample_ids),
        }
        candidate_picblockhashes: Dict[int, Dict[str, Any]] = {}
        for entry in self._getDb().functions.find(
            {"sample_id": {"$in": sample_ids}, "_picblockhashes": {"$exists": True, "$ne": []}}, {"function_id": 1, "sample_id": 1, "_picblockhashes": 1, "_id": 0}
        ):
            sample_id = entry["sample_id"]
            for block_entry in entry["_picblockhashes"]:
                block_hash = block_entry["hash"]
                if block_hash not in candidate_picblockhashes:
                    candidate_picblockhashes[block_hash] = {
                        "samples": set(),
                        "length": block_entry["length"],
                        "function_id": entry["function_id"],
                        "sample_id": sample_id,
                        "offset": decode_two_complement(block_entry["offset"]),
                        "instructions": [],
                        "escaped_sequence": "",
                        "score": 0,
                    }
                candidate_picblockhashes[block_hash]["samples"].add(sample_id)
        # update statistics based on candidates
        for picblockhash, entry in candidate_picblockhashes.items():
            for sample_id in entry["samples"]:
                block_statistics["by_sample_id"][sample_id]["total_blocks"] += 1
        LOGGER.info(f"Found {len(candidate_picblockhashes)} candidate picblock hashes")
        # remove those that are not unique
        if self._isPicBlockHashIndexComplete():
            self._reduceToUniqueBlocksUsingIndex(candidate_picblockhashes, sample_ids)
        else:
            LOGGER.warning(
                "picblockhash index is not built - falling back to a full scan of the functions collection. "
                "Run rebuildPicBlockHashIndex() (Worker.rebuildPicBlockHashIndex) to enable the indexed path."
            )
            self._reduceToUniqueBlocksByScan(candidate_picblockhashes, sample_ids, progress_reporter=progress_reporter)
        # update statistics again after having reduced to results
        for picblockhash, entry in candidate_picblockhashes.items():
            if len(entry["samples"]) == 1:
                single_sample_id = list(entry["samples"])[0]
                block_statistics["by_sample_id"][single_sample_id]["unique_blocks"] += 1
            for sample_id in entry["samples"]:
                block_statistics["by_sample_id"][sample_id]["characteristic_blocks"] += 1
        block_statistics["unique_blocks_overall"] = len(candidate_picblockhashes)
        LOGGER.info(f"Reduced to {len(candidate_picblockhashes)} unique picblock hashes")
        # we are basically finished when we reached this step, so set progress to 100%
        if progress_reporter is not None:
            progress_reporter.set_total(1)
            progress_reporter.step()
        # iterate over candidates by function_id and extract instructions
        function_id_to_block_offsets = {}
        for picblockhash, entry in candidate_picblockhashes.items():
            candidate_picblockhashes[picblockhash]["samples"] = sorted(list(entry["samples"]))
            # we calculate the score for this block as 80% of how well it covers the samples and 20% how far its size is away from an "ideal" signature block
            sample_score = 100.0 * len(entry["samples"]) / len(sample_ids)
            length_score = 100.0
            if entry["length"] < 7:
                length_score = 100.0 - (100.0 * (7 - entry["length"]) / 7)
            elif entry["length"] > 10:
                length_score = 100.0 * (1 / (entry["length"] - 10))
            candidate_picblockhashes[picblockhash]["score"] = 0.8 * sample_score + 0.2 * length_score
            if entry["function_id"] not in function_id_to_block_offsets:
                function_id_to_block_offsets[entry["function_id"]] = []
            function_id_to_block_offsets[entry["function_id"]].append((entry["offset"], picblockhash))
        block_function_ids = list(function_id_to_block_offsets.keys())
        block_xcfg_blobs = self._fetchXcfgBlobs(block_function_ids)
        for entry in self._getDb().functions.find({"function_id": {"$in": block_function_ids}}, {"function_id": 1, "_xcfg": 1, "_id": 0}):
            function_id = entry["function_id"]
            # inline `_xcfg` is the fallback for pre-migration documents (see _attachXcfgBlobs)
            entry["_xcfg"] = block_xcfg_blobs.get(function_id) or entry.get("_xcfg") or "{}"
            self._decodeXcfg(entry)
            for block_offset, picblockhash in function_id_to_block_offsets[function_id]:
                candidate_picblockhashes[picblockhash]["instructions"] = entry["xcfg"]["blocks"][str(block_offset)]
        LOGGER.info(f"Instructions for {len(candidate_picblockhashes)} blocks extracted.")
        return {"statistics": block_statistics, "unique_blocks": candidate_picblockhashes}

    ##### helpers for search ######

    # pichashes are stored hex-encoded, in a differently named field ("_pichash") and with a
    # variable width, so sorting on them would neither order numerically nor let the search cursor
    # page - its tree compares the sort field with a range operator, which the transpiler rejects.
    # Zero-padding the stored value would lift this and the range rejection alike (#145).
    _UNSORTABLE_FIELDS = ("pichash",)

    @staticmethod
    def _assert_sortable_fields(full_cursor: Optional[FullSearchCursor]) -> None:
        if full_cursor is None:
            return
        for field in full_cursor.sort_fields:
            if field in MongoDbStorage._UNSORTABLE_FIELDS:
                raise ValueError(f"Sorting by the field '{field}' is not supported by the MongoDB backend.")

    @staticmethod
    def _get_sort_list_from_cursor(full_cursor: Optional[FullSearchCursor]):
        MongoDbStorage._assert_sortable_fields(full_cursor)
        if full_cursor is None:
            return None
        is_backward_search = not full_cursor.is_forward_search
        sort_list = [(key, 1 if direction ^ is_backward_search else -1) for key, direction in full_cursor.sort_by_list]
        return sort_list

    def _getDistinctValues(self, collection: str, field: str) -> Optional[List[Any]]:
        """All distinct values of the field, or None when there are more than _DISTINCT_VALUES_CAP
        of them or they exceed _DISTINCT_VALUES_MAX_BYTES in total. A None is remembered for
        _OVER_CAP_TTL seconds, so a large corpus pays for the capped scan once, not per search."""
        found_over_cap = self._over_cap_since.get((collection, field))
        if found_over_cap is not None and time.monotonic() - found_over_cap < self._OVER_CAP_TTL:
            return None
        values = self._scanDistinctValues(collection, field)
        if values is None:
            self._over_cap_since[(collection, field)] = time.monotonic()
        return values

    def _scanDistinctValues(self, collection: str, field: str) -> Optional[List[Any]]:
        pipeline = [{"$group": {"_id": "$" + field}}, {"$limit": self._DISTINCT_VALUES_CAP + 1}]
        values = []
        total_bytes = 0
        for document in self._getDb()[collection].aggregate(pipeline):
            value = document["_id"]
            if isinstance(value, str):
                total_bytes += len(value.encode("utf-8"))
                if total_bytes > self._DISTINCT_VALUES_MAX_BYTES:
                    return None
            values.append(value)
        if len(values) > self._DISTINCT_VALUES_CAP:
            return None
        return values

    def _get_search_query(
        self, search_fields: List[str], search_tree: NodeType, cursor: Optional[FullSearchCursor], conditional_search_fields=None, distinct_fields: Optional[Dict[str, str]] = None
    ):
        """
        distinct_fields maps a field to its collection: a substring condition on it is rewritten to
        the distinct values that match, when the collection has few enough of them (see
        MongoSearchTranspiler.known_values).
        """
        # checked here as well as when the sort list is built, because this runs first: paging by an
        # unsortable field would otherwise fail in the transpiler, blaming the range operator that
        # the cursor tree happens to use instead of naming the field that cannot be sorted by
        self._assert_sortable_fields(cursor)
        if cursor is not None:
            full_tree = AndNode([search_tree, cursor.toTree()])
        else:
            full_tree = search_tree
        full_tree = SearchFieldResolver(search_fields, conditional_search_fields=conditional_search_fields).visit(full_tree)
        full_tree = FilterSingleElementLists().visit(full_tree)
        full_tree = PropagateNot().visit(full_tree)
        known_values = {}
        for field, collection in (distinct_fields or {}).items():
            if _hasSubstringCondition(full_tree, field):
                values = self._getDistinctValues(collection, field)
                if values is not None:
                    known_values[field] = values
        query = MongoSearchTranspiler(known_values).visit(full_tree)
        return query

    ##### search ####

    def findFamilyByString(self, search_tree: NodeType, cursor: Optional[FullSearchCursor] = None, max_num_results: int = 100) -> Dict[int, "FamilyEntry"]:
        result_dict = {}
        search_fields = ["family_name"]
        query = self._get_search_query(search_fields, search_tree, cursor)
        sort_list = self._get_sort_list_from_cursor(cursor)
        for family_document in self._getDb().families.find(query, {"_id": 0}, sort=sort_list, limit=max_num_results):
            entry = FamilyEntry.fromDict(family_document)
            result_dict[family_document["family_id"]] = entry
        return result_dict

    def findSampleByString(self, search_tree: NodeType, cursor: Optional[FullSearchCursor] = None, max_num_results: int = 100) -> Dict[int, "SampleEntry"]:
        result_dict = {}
        search_fields = [
            "filename",
            "family",
            "component",
            "version",
        ]
        conditional_field = ("sha256", lambda search_term: len(search_term) >= 3)
        query = self._get_search_query(search_fields, search_tree, cursor, conditional_search_fields=[conditional_field])
        sort_list = self._get_sort_list_from_cursor(cursor)
        for sample_document in self._getDb().samples.find(query, {"_id": 0}, sort=sort_list, limit=max_num_results):
            entry = SampleEntry.fromDict(sample_document)
            result_dict[entry.sample_id] = entry
        return result_dict

    def findFunctionByString(self, search_tree: NodeType, cursor: Optional[FullSearchCursor] = None, max_num_results: int = 100) -> Dict[int, "FunctionEntry"]:
        result_dict = {}
        # TODO also search through function labels once we have implemented them
        search_fields = ["function_name"]
        query = self._get_search_query(search_fields, search_tree, cursor, distinct_fields={"function_name": "functions"})
        sort_list = self._get_sort_list_from_cursor(cursor)
        for function_document in self._getDb().functions.find(query, {"_id": 0}, sort=sort_list, limit=max_num_results):
            self._decodeFunction(function_document)
            entry = FunctionEntry.fromDict(function_document)
            result_dict[entry.function_id] = entry
        return result_dict
