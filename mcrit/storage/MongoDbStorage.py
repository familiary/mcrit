import contextlib
import datetime
import functools
import hashlib
import json
import logging
import re
import threading
import time
import traceback
import uuid
from collections import Counter, OrderedDict
from concurrent.futures import ThreadPoolExecutor
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version
from itertools import zip_longest
from operator import itemgetter
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, Iterator, List, Optional, Set, Tuple, Union

import gridfs
import numpy as np
from bson import ObjectId
from bson import encode as bson_encode
from gridfs.errors import FileExists
from packaging import version
from picblocks.blockhasher import BlockHasher
from pymongo import MongoClient, ReturnDocument, UpdateMany, UpdateOne
from pymongo.errors import BulkWriteError, DocumentTooLarge, DuplicateKeyError, ExecutionTimeout, OperationFailure, PyMongoError
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
from mcrit.libs.tags import MAX_TAGS_PER_ENTITY, TagLimitError, checkTagEntity, normalizeTags, tagLimitMessage
from mcrit.libs.utility import decode_two_complement, encode_two_complement
from mcrit.minhash.MinHash import MinHash
from mcrit.minhash.MinHasher import MINHASH_SHINGLER_REVISION, SHINGLER_REVISION_SINCE
from mcrit.storage.FamilyEntry import FamilyEntry
from mcrit.storage.FunctionEntry import OTHER_LABELS_FIELD, FunctionEntry, checkSubmittedPicHashes, otherLabels, picBlockHashEntries, smdaFunctionFromXcfg
from mcrit.storage.FunctionLabelEntry import FunctionLabelEntry
from mcrit.storage.MatchingCache import MatchingCache
from mcrit.storage.SampleEntry import SampleEntry
from mcrit.storage.StorageInterface import BandDfSkips, BinaryStream, StorageInterface, isSha256
from mcrit.storage.UniqueBlocksResult import UniqueBlocksProgressReporter, blockScore

LOGGER = logging.getLogger(__name__)

# the picblocks this process hashes blocks with, recorded on every sample it stores (#240)
try:
    PICBLOCKS_VERSION = package_version("picblocks")
except PackageNotFoundError:
    # an install without package metadata; pyproject.toml requires picblocks 2.1.0 or newer
    PICBLOCKS_VERSION = "2.1.0"
# the first picblocks that escapes a block by its own architecture's rules; before, every block was
# escaped as Intel code, so non-Intel block hashes stored by an older one have to be recomputed
PICBLOCKS_ARCHITECTURE_AWARE = "2.1.0"
if MongoClient is None:
    LOGGER.warning("pymongo package import failed - MongoDB backend will not be available.")

if TYPE_CHECKING:  # pragma: no cover
    from pymongo.database import Database
    from smda.common.SmdaFunction import SmdaFunction
    from smda.common.SmdaReport import SmdaReport

    from mcrit.config.McritConfig import McritConfig


# pichashes and picblockhashes are 64 bit values stored as hex strings. Written zero-padded to
# 16 digits, their string order is their numeric order, which is what range conditions and
# cursor paging by pichash need (#145). Instances created before this carry variable-width
# values ("0x4d2") until migrate_pichash_padding has run; the settings flag "pichash_padded"
# says which shape an instance holds, and every reader below honours both until it is set.
PICHASH_HEX_DIGITS = 16
PICHASH_MAX_VALUE = (1 << (4 * PICHASH_HEX_DIGITS)) - 1


def encode_pichash_value(value: int, padded: bool) -> str:
    return "0x%0*x" % (PICHASH_HEX_DIGITS, value) if padded else hex(value)


def pichash_value_variants(value: int) -> List[str]:
    """Both encodings a value may be stored under while an instance is not (yet) padded."""
    return sorted({hex(value), encode_pichash_value(value, padded=True)})


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
        visited_children = [self.visit(child) for child in self._dedupe(node.children)]
        return self._and_query(*visited_children)

    def visitOrNode(self, node: OrNode):
        visited_children = [self.visit(child) for child in self._dedupe(node.children)]
        return self._or_query(*visited_children)

    # how many (value, term) pairs one query may test against its known values, about 0.4 s of
    # Python on 50-character labels (0.75 us a test): the terms past that are left to the regex
    _MAX_VALUE_TESTS = 500_000

    def __init__(self, known_values: Optional[Dict[str, List[Any]]] = None, pichash_padded: bool = False) -> None:
        super().__init__()
        self.known_values = known_values or {}
        # whether the instance stores pichashes zero-padded (#145), which decides how a pichash
        # condition is encoded and whether range operators on it are allowed
        self.pichash_padded = pichash_padded
        # the $in / $nin lists of one query share _DISTINCT_VALUES_MAX_BYTES, so a query stays well
        # inside MongoDB's 16 MiB however many terms it has; a term past the budget is a regex
        self._list_bytes_left = MongoDbStorage._DISTINCT_VALUES_MAX_BYTES
        self._value_tests_left = self._MAX_VALUE_TESTS
        self._matching: Dict[Tuple[str, str, str], Optional[List[str]]] = {}
        # the known fields of which a condition was left to the regex all the same
        self.unlisted_fields: Set[str] = set()

    @staticmethod
    def _dedupe(children: List[NodeType]) -> List[NodeType]:
        # "a b a" is "a b" in both an and and an or: the same condition is transpiled once
        seen = set()
        unique = []
        for child in children:
            if isinstance(child, SearchConditionNode):
                key = (child.field, child.operator, repr(child.value))
                if key in seen:
                    continue
                seen.add(key)
            unique.append(child)
        return unique

    @staticmethod
    def _may_match(term: str, value: str) -> bool:
        # A superset of what MongoDB's case-insensitive regex matches: its folding (PCRE's) differs
        # from Python's for a few characters, such as the dotted capital I, which Python's re folds
        # to "i" and PCRE does not. The $regex the $in is paired with decides. Both fold ASCII alike,
        # and nearly every label is ASCII: that test alone is a third of the cost (#301).
        if term.isascii() and value.isascii():
            return term.lower() in value.lower()
        return term.lower() in value.lower() or term.upper() in value.upper() or term.casefold() in value.casefold()

    @staticmethod
    def _is_exact(field: str, operator: str) -> bool:
        # A "?" on the labels is answered by the probe (the regex) or through the listing, so the two
        # have to agree exactly. The names keep the $in of what Python's re matches, which is all
        # their (bounded) index scan needs.
        return operator == "?" and field in MongoDbStorage._DISTINCT_VALUES_IN_ARRAYS

    def _matching_values(self, field: str, operator: str, term: str) -> Optional[List[str]]:
        key = (field, operator, term)
        if key not in self._matching:
            self._matching[key] = self._scan_matching_values(field, operator, term)
        return self._matching[key]

    def _scan_matching_values(self, field: str, operator: str, term: str) -> Optional[List[str]]:
        values = self.known_values[field]
        if len(values) > self._value_tests_left:
            return None
        self._value_tests_left -= len(values)
        pattern = re.compile(re.escape(term), re.IGNORECASE)
        if self._is_exact(field, operator):
            matching = [value for value in values if isinstance(value, str) and (pattern.search(value) or self._may_match(term, value))]
        else:
            matching = [value for value in values if isinstance(value, str) and pattern.search(value)]
        size = sum(len(value.encode("utf-8")) for value in matching)
        if size > self._list_bytes_left:
            return None
        self._list_bytes_left -= size
        return matching

    def _substring_condition_from_known_values(self, node: SearchConditionNode) -> Optional[Dict[str, Any]]:
        # an empty search term matches everything, so the regex is left alone there
        if node.operator not in ("?", "!?") or node.field not in self.known_values or not node.value:
            return None
        if node.operator == "!?" and node.field in MongoDbStorage._DISTINCT_VALUES_IN_ARRAYS:
            # a negated label term stays the regex: its $nin would examine every document all the
            # same, and could not be paired with the regex to keep MongoDB's case folding
            return None
        matching = self._matching_values(node.field, node.operator, node.value)
        if matching is None:
            self.unlisted_fields.add(node.field)
            return None
        if node.operator == "!?":
            return {node.field: {"$nin": matching}}
        if not self._is_exact(node.field, node.operator):
            return {node.field: {"$in": matching}}
        # the $in of every value the regex may match bounds the index scan, and the regex itself
        # keeps exactly MongoDB's matches, as the probe and the unlisted search find them
        return {node.field: {"$in": matching, "$regex": re.compile(re.escape(node.value), re.IGNORECASE)}}

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
        elif node.isNumeric():
            value = node.numericValue()
            # offsets are stored as signed 64 bit values, an offset of 2**63 and above wrapped around
            if node.field == "offset" and value is not None and value >= 2**63:
                if isinstance(node.value, str) and node.operator in ("<", "<=", ">", ">="):
                    # wrapped, it would compare below every other offset; a search cursor's own
                    # conditions follow that order, which is the order MongoDB sorts offsets in
                    raise ValueError("Range operators on 'offset' are only supported below 2**63.")
                value = encode_two_complement(value)
        elif isinstance(value, int) and not isinstance(value, bool) and not -(2**63) <= value < 2**63:
            # a search cursor may carry up to 2**64 for an offset, and that much reaches a text field's
            # condition too when a client crafts one; pymongo would raise OverflowError, a 500
            raise ValueError(f"The value for '{node.field}' does not fit into 64 bits.")
        mongo_operator = operator_to_mongo[node.operator]
        if node.field == "pichash" and mongo_operator in ("$lt", "$lte", "$gt", "$gte"):
            if not self.pichash_padded:
                # on variable-width hex strings a range comparison answers a plausible but wrong set
                raise ValueError("Range operators on the field 'pichash' need zero-padded pichashes; run migrate_pichash_padding first.")
            if not isinstance(value, int) or not 0 <= value <= PICHASH_MAX_VALUE:
                # a bound outside the 64 bit domain formats wider than 16 digits and would not compare
                raise ValueError("A pichash bound must be an integer between 0 and 0xffffffffffffffff.")
        if mongo_operator is None:
            condition = {node.field: value}
        else:
            condition = {node.field: {mongo_operator: value}}
        if node.field == "pichash":
            if node.operator.endswith("?"):
                # a substring search is a regex, which cannot be hex-encoded - it is matched against
                # the stored representation instead, so only the field has to be renamed
                condition = {"_pichash": condition.pop("pichash")}
            elif value is None:
                # a search cursor pages past functions without a pichash with "= None", which matches
                # a missing field in any encoding, so it is renamed only
                condition = {"_pichash": condition.pop("pichash")}
            elif self.pichash_padded or not isinstance(value, int):
                MongoDbStorage._encodePichash(condition, padded=self.pichash_padded)
            elif mongo_operator is None:
                # an instance that is not padded may hold either width (a migration in flight)
                condition = {"_pichash": {"$in": pichash_value_variants(value)}}
            else:
                condition = {"_pichash": {"$nin": pichash_value_variants(value)}}
        return condition


def _conditionFields(tree: NodeType) -> Set[str]:
    """The fields the (resolved) tree has conditions on."""
    if isinstance(tree, SearchConditionNode):
        return {tree.field}
    if isinstance(tree, (AndNode, OrNode)):
        return set().union(*(_conditionFields(child) for child in tree.children))
    if isinstance(tree, NotNode):
        return _conditionFields(tree.child)
    return set()


def _fieldConditions(tree: NodeType, field: str) -> List[SearchConditionNode]:
    """The (resolved) tree's conditions on the field."""
    if isinstance(tree, SearchConditionNode):
        return [tree] if tree.field == field else []
    if isinstance(tree, (AndNode, OrNode)):
        return [condition for child in tree.children for condition in _fieldConditions(child, field)]
    if isinstance(tree, NotNode):
        return _fieldConditions(tree.child, field)
    return []


def _substringTerms(tree: NodeType, field: str, operator: str) -> Set[str]:
    """The distinct terms of the (resolved) tree's substring conditions on the field."""
    if isinstance(tree, SearchConditionNode):
        return {tree.value} if tree.field == field and tree.operator == operator else set()
    if isinstance(tree, (AndNode, OrNode)):
        return set().union(*(_substringTerms(child, field, operator) for child in tree.children))
    if isinstance(tree, NotNode):
        return _substringTerms(tree.child, field, operator)
    return set()


def _foldOtherLabels(tree: NodeType) -> Union[NodeType, bool]:
    """The (resolved) tree as it holds for a function without OTHER_LABELS_FIELD: its conditions on
    that field folded into the constants they are for a missing field, True or False when the
    whole tree is one. A regex or a comparison never matches a missing field, a negated one always
    does, as in MongoDB."""
    if isinstance(tree, SearchConditionNode):
        if tree.field == OTHER_LABELS_FIELD:
            return tree.operator in ("!?", "!=")
        return tree
    if isinstance(tree, (AndNode, OrNode)):
        is_and = isinstance(tree, AndNode)
        children = []
        for child in tree.children:
            folded = _foldOtherLabels(child)
            if isinstance(folded, bool):
                if folded != is_and:
                    # a False in an and, a True in an or decides it
                    return folded
                continue
            children.append(folded)
        if not children:
            return is_and
        if len(children) == 1:
            return children[0]
        return AndNode(children) if is_and else OrNode(children)
    return tree


def _sortValue(value: Any) -> Tuple[int, Any]:
    """MongoDB's order of the values a function search sorts by: a missing value or null first,
    then numbers, then strings (compared by code point, as MongoDB compares their UTF-8 bytes)."""
    if value is None:
        return (0, 0)
    if isinstance(value, bool):
        return (3, value)
    if isinstance(value, (int, float)):
        return (1, value)
    if isinstance(value, str):
        return (2, value)
    return (4, str(value))


def _sortedDocuments(documents: List[Dict], sort_list: Optional[List[Tuple[str, int]]]) -> List[Dict]:
    """The documents in the order MongoDB sorts them by the sort list, which ends in a unique field."""
    if not sort_list:
        return documents

    def compare(first: Dict, second: Dict) -> int:
        for field, direction in sort_list:
            first_value, second_value = _sortValue(first.get(field)), _sortValue(second.get(field))
            if first_value != second_value:
                return direction if first_value > second_value else -direction
        return 0

    return sorted(documents, key=functools.cmp_to_key(compare))


def _hasSubstringCondition(tree: NodeType, field: str, operators: Tuple[str, ...] = ("?", "!?")) -> bool:
    """True when the (resolved) tree holds a substring condition on the field."""
    if isinstance(tree, SearchConditionNode):
        return tree.field == field and tree.operator in operators
    if isinstance(tree, (AndNode, OrNode)):
        return any(_hasSubstringCondition(child, field, operators) for child in tree.children)
    if isinstance(tree, NotNode):
        return _hasSubstringCondition(tree.child, field, operators)
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
    # Fields inside an array of documents (the function labels, searched as
    # "function_labels.function_label:?term") are listed with the distinct command instead: a $group
    # would group by the whole array, and the $unwind it needs first reads every document, where
    # distinct is answered from the multikey index alone (DISTINCT_SCAN). It cannot stop at the cap,
    # and these fields are not held to it either: the regex they would fall back to cannot be
    # checked against the keys of a multikey index, so it fetches every labelled function, and
    # MongoDB preferred walking all of them by function_id (a search without results over a million
    # functions: 1.4 s, every document examined). Listing 150,000 distinct labels there took 0.6 s,
    # and the listing is paid on every search with such a term, never cached: 36-42 ms for 7,500
    # labels, 130-190 ms for 48,600 over 250,000 functions. Its bounds are the search time limit and
    # the 16 MiB a distinct can answer, past which the labels fall back to the regex after all; the
    # $in stays within _DISTINCT_VALUES_MAX_BYTES because the transpiler only uses the matching
    # values up to that size.
    # The labels that differ from a function's name (#301), which a plain term matches besides the
    # name, are listed the same way: a few hundred values on a real corpus, listed in about a
    # millisecond from their sparse index.
    _DISTINCT_VALUES_IN_ARRAYS = ("function_labels.function_label", OTHER_LABELS_FIELD)
    # A search of nothing but label conditions with a "?" among them is first tried as the regex
    # walking the sort order, for this long: a term that many labels match fills its page within a
    # few milliseconds that way (5-35 ms over 250,000 functions), where listing the labels costs
    # 30-250 ms. Only a probe that runs out lists them, and the probe's and the listing's time count
    # against the search time limit, so a rare term costs this much more than before, never more
    # than the limit.
    _LABEL_PROBE_TIME_MS = 50
    # an inline `_xcfg` (a corpus migrate_xcfg_split has not run on) is left on the server: a search
    # result carries no disassembly, as it never did once the blobs were split out
    _FUNCTION_SEARCH_PROJECTION = {"_id": 0, "_xcfg": 0, OTHER_LABELS_FIELD: 0}
    # The update stage that derives OTHER_LABELS_FIELD from the document it updates (#301): the
    # distinct label strings other than function_name, the field removed when there are none, so its
    # sparse index holds only renamed or user-labelled functions. Every write that changes a
    # function's labels or its name runs it in the same update, so the field follows the document
    # as written, however writes interleave; an insert sets it from FunctionEntry's otherLabels.
    _OTHER_LABELS_STAGE = {
        "$set": {
            OTHER_LABELS_FIELD: {
                "$let": {
                    "vars": {
                        "others": {
                            "$setDifference": [
                                {"$filter": {"input": {"$ifNull": ["$function_labels.function_label", []]}, "cond": {"$eq": [{"$type": "$$this"}, "string"]}}},
                                [{"$ifNull": ["$function_name", None]}],
                            ]
                        }
                    },
                    "in": {"$cond": [{"$gt": [{"$size": "$$others"}, 0]}, "$$others", "$$REMOVE"]},
                }
            }
        }
    }
    # settings flag: every labelled function carries OTHER_LABELS_FIELD as its labels and name say.
    # Set for an empty database, by the startup backfill and by migrate_other_labels; until then a
    # plain term finds a function by a label other than its name only once a write touched it.
    _OTHER_LABELS_SETTING = "other_labels_complete"
    # A database upgrading into #301 fills OTHER_LABELS_FIELD on startup when it has at most this many
    # labelled functions: one server-side update over them, 0.74 s for 87,700 of 250,000 functions
    # here. A larger corpus is left to migrate_other_labels, and startup logs that it is due.
    _OTHER_LABELS_STARTUP_MAX = 100_000
    # where mcrit.migrations.migrate_other_labels keeps its progress
    _OTHER_LABELS_MIGRATION_STATE = "other_labels_migration_state"
    # settings counter: bumped after every write that can change which other labels exist (a rename,
    # a label, an insert of functions with other labels, a deletion of functions, the fill and the
    # migration), so that each process lists them again only after one, in whichever process it was
    _OTHER_LABELS_VERSION_SETTING = "other_labels_version"
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
    # function documents given their minhash, or band hashes given their postings, per bulk_write
    _MINHASH_UPDATE_BATCH = 10000
    # What replacing a reported sample's analysis stages (#270): functions, disassembly and extras
    # under a staging id the sample records, apart from the collections every reader uses, so that
    # no matching, search or export sees them before they are swapped in under the sample's id.
    _STAGING_FUNCTIONS = "functions_staging"
    _STAGING_XCFG = "xcfg_staging"
    _STAGING_SMDA_EXTRAS = "smda_extras_staging"
    _STAGING_COLLECTIONS = (_STAGING_FUNCTIONS, _STAGING_XCFG, _STAGING_SMDA_EXTRAS)

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
        self._pichash_padded: Optional[bool] = None
        # guards the lazy initialisation in _getDb(); a threading.Lock is per-process, which is
        # what we want: forking servers (gunicorn) fork before the first request, so every child
        # inherits an unlocked copy and synchronises its own threads independently
        self._database_lock = threading.Lock()
        # (collection, field) -> time.monotonic() at which it was found over the distinct-values cap
        self._over_cap_since: Dict[Tuple[str, str], float] = {}
        # (collection, field) -> (how many distinct values its last listing found, time.monotonic()
        # of it), for the fields of _DISTINCT_VALUES_IN_ARRAYS: a search with too many terms to test
        # against that many values skips the listing for _OVER_CAP_TTL seconds
        self._distinct_count_since: Dict[Tuple[str, str], Tuple[int, float]] = {}
        # (settings.other_labels_version it was listed at, the distinct other labels): a plain term
        # reads the version and lists them only when it moved (#301)
        self._other_labels_listing: Optional[Tuple[int, List[Any]]] = None
        # time.monotonic() at which they could not be listed: not tried again for _OVER_CAP_TTL
        # seconds, whatever writes there are meanwhile
        self._other_labels_unlisted_since: Optional[float] = None
        # (term as matched) -> (the version of the listing, the other labels of it a "?" with the
        # term may match), least recently used first, so a repeated term skips testing every label;
        # dropped as a whole when the version moves (_otherLabelsMatchingTerm). The lock guards it:
        # the server shares this object across its request threads (#109).
        self._other_label_matches: OrderedDict[str, Tuple[int, List[str]]] = OrderedDict()
        self._other_label_matches_version: Optional[int] = None
        self._other_label_matches_lock = threading.Lock()

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

    @staticmethod
    def buildMongoUri(server, port, db_name, username="", password="", flags="") -> str:
        """The URI MCRIT connects with; migrate_minhash_binary and migrate_pichash_padding use it too, so they reach the same database.

        Credentials go in as configured, so a password holding `@`, `:` or `/` has to be configured
        percent-encoded already. An empty port leaves it out, which is how a host list is given.
        """
        userpw_url = f"{username}:{password}@" if username and password else ""
        port_url = f":{port}" if port else ""
        flags_url = f"?{flags}" if flags else ""
        return f"mongodb://{userpw_url}{server}{port_url}/{db_name}{flags_url}"

    def _initDb(self, server, port, db_name, username="", password="", flags=""):
        mongo_uri = self.buildMongoUri(server, port, db_name, username, password, flags)

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
            # a fresh instance stores pichashes zero-padded from the start (#145)
            self._getDb()["settings"].insert_one({"mcrit_db_id": str(uuid.uuid4()), "db_state": 0, "pichash_padded": True})
            self._pichash_padded = None
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
                # counts left untrusted with no function behind them - an import whose counting
                # failed after earlier samples were counted, then rolled back - would be trusted
                # again with the flag, and counted twice by the next import of those samples
                self._getDb()[self._PICHASH_COUNT_COLLECTION].delete_many({})
                self._setPicHashCountIndexComplete(True)
            if not self.isOtherLabelsComplete():
                self._setOtherLabelsComplete(True)
        self._getDb()["samples"].create_index("sample_id")
        self._getDb()["samples"].create_index("sha256")
        self._getDb()["samples"].create_index("family_id")
        # the stale-minhash count on /status is a distinct plus a count over this field (#142)
        self._getDb()["samples"].create_index("minhash_smda_version")
        # and the stale-picblockhash count over these two, which skips Intel samples in the index (#240)
        self._getDb()["samples"].create_index([("architecture", 1), ("picblockhash_version", 1)])
        # and over these two for its shingler revision clause (#238)
        self._getDb()["samples"].create_index([("architecture", 1), ("minhash_shingler_revision", 1)])
        # and the stale-pichash count over these two, the report's version standing in where no stamp exists (#249)
        self._getDb()["samples"].create_index([("pichash_smda_version", 1), ("smda_version", 1)])
        # and the distinct over the reports' versions it builds that count from
        self._getDb()["samples"].create_index("smda_version")
        # the samples /complete_minhashes finishes; sparse, as only samples still pending carry it
        self._getDb()["samples"].create_index("minhash_pending.smda_version", sparse=True)
        # and the count of samples the running smda found impossible to rehash; sparse, as few are, and
        # so the planner cannot pick it for the $ne both stale queries carry, which would read it whole
        self._getDb()["samples"].create_index("pichash_unrehashable_smda_version", sparse=True)
        self._getDb()["families"].create_index("family_id")
        self._ensureUniqueFamilyNames()
        self._getDb()["functions"].create_index("function_id")
        self._getDb()["functions"].create_index("sample_id")
        self._getDb()["functions"].create_index("family_id")
        self._getDb()["functions"].create_index("function_name")
        # the qualified label search (findFunctionByString); sparse, as only named or labelled functions carry
        # labels, and a query for label values never asks for the functions without one
        self._getDb()["functions"].create_index("function_labels.function_label", sparse=True)
        # the labels that differ from the name, which a plain term matches as well (#301); sparse, as
        # only renamed or user-labelled functions have any, typically a few hundred
        self._getDb()["functions"].create_index(OTHER_LABELS_FIELD, sparse=True)
        self._getDb()["functions"].create_index("_pichash")
        self._getDb()["functions"].create_index("_picblockhashes.hash")
        # tags (#53): multikey, so that tags:x in a search is an index lookup. The functions index
        # is sparse and function documents leave the field out while it is empty (see
        # _encodeFunction), so it holds the tagged functions only instead of one entry per function
        # of the corpus. Its first build on an existing corpus still reads every function once, but
        # adds nothing, since no document carries the field yet.
        self._getDb()["families"].create_index("tags")
        self._getDb()["samples"].create_index("tags")
        self._getDb()["functions"].create_index("tags", sparse=True)
        # Searches sorted by a field other than the id (fkie-cad/mcritweb#59): the search cursor sorts by that
        # field and breaks ties by the id, in the same direction, so one compound index per
        # sortable field serves both directions by being walked backwards. Without it the
        # server sorts every filtered document in memory, which is where the reported 10x
        # goes. The fields are the ones the search results can be sorted by in MCRITweb.
        for field in ("family_name", "num_samples", "num_library_samples", "num_functions"):
            self._getDb()["families"].create_index([(field, 1), ("family_id", 1)])
        for field in ("family_id", "family", "filename", "version", "bitness", "sha256", "statistics.num_functions"):
            self._getDb()["samples"].create_index([(field, 1), ("sample_id", 1)])
        # The pichash sort (allowed once the instance stores pichashes zero-padded, #145) sorts on the stored "_pichash".
        for field in ("family_id", "sample_id", "function_name", "offset", "num_instructions", "num_blocks", "_pichash"):
            self._getDb()["functions"].create_index([(field, 1), ("function_id", 1)])
        # the pichash counts and the function ranges, with the specs their rebuilds create. Adds
        # upsert into both by these keys, the MINHASH_PICHASH_MAX_MATCHES cutoff reads the counts by
        # them and deleteSample removes from both by them (#261). Only the rebuilds created them
        # before, so a fresh or cleared database, which never needed one, scanned on every write.
        self._getDb()[self._PICHASH_COUNT_COLLECTION].create_index([("_pichash", 1), ("df", 1)])
        self._getDb()[self._FUNCTION_RANGE_COLLECTION].create_index("sample_id")
        self._getDb()[self._FUNCTION_RANGE_COLLECTION].create_index("first_function_id")
        # stored without guarantee of existence
        self._getDb()["query_samples"].create_index("sample_id")
        self._getDb()["query_samples"].create_index("sha256")
        self._getDb()["query_functions"].create_index("function_id")
        self._getDb()["query_functions"].create_index("sample_id")
        # the SMDA report's extras, stored apart from the sample documents (#94)
        for collection in ("smda_extras", "query_smda_extras"):
            self._getDb()[collection].create_index([("sample_id", 1), ("chunk", 1)], unique=True)
        # what a replacement of a sample's analysis staged is found and discarded by its staging id (#270)
        for collection in self._STAGING_COLLECTIONS:
            self._getDb()[collection].create_index("staging_id")
        # stored binaries are keyed by content: one file per sha256, naming every sample it belongs to (#95).
        # Partial, so that a file retired for deletion (its sha256 cleared) does not hold the key.
        self._getDb()[self._BINARIES_BUCKET + ".files"].create_index("metadata.sha256", unique=True, partialFilterExpression={"metadata.sha256": {"$type": "string"}})
        self._getDb()[self._BINARIES_BUCKET + ".files"].create_index("metadata.sample_ids")
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
        # last, and never fatal: a failed fill (_ensureOtherLabels) leaves the flag unset rather
        # than this process without the setup above
        try:
            self._ensureOtherLabels()
        except Exception:
            LOGGER.exception("Could not derive the labels other than their name of the labelled functions (#301); see migrate_other_labels.")

    def isOtherLabelsComplete(self) -> bool:
        """Whether every labelled function carries OTHER_LABELS_FIELD (#301): a plain search term
        finds a function by a label other than its name only where it does."""
        settings_document = self._getDb().settings.find_one({}, {self._OTHER_LABELS_SETTING: 1})
        return bool(settings_document and settings_document.get(self._OTHER_LABELS_SETTING))

    def _setOtherLabelsComplete(self, is_complete: bool) -> None:
        self._getDb().settings.update_one({}, {"$set": {self._OTHER_LABELS_SETTING: bool(is_complete)}})

    def _otherLabelsVersion(self) -> int:
        settings_document = self._getDb().settings.find_one({}, {self._OTHER_LABELS_VERSION_SETTING: 1})
        return int((settings_document or {}).get(self._OTHER_LABELS_VERSION_SETTING, 0))

    def _bumpOtherLabelsVersionFor(self, function_documents: List[Dict]) -> None:
        """After inserting these functions: a bump only when one of them has other labels."""
        if any(OTHER_LABELS_FIELD in document for document in function_documents):
            self._bumpOtherLabelsVersion()

    def _bumpOtherLabelsVersion(self) -> None:
        """After a write that can change which other labels exist: every process lists them anew."""
        self._getDb().settings.update_one({}, {"$inc": {self._OTHER_LABELS_VERSION_SETTING: 1}})

    # database name -> time.monotonic() at which this process found it due for migrate_other_labels:
    # it neither counts nor says so again for _OVER_CAP_TTL seconds
    _other_labels_due_since: Dict[str, float] = {}
    # settings field holding when a process last found the database due, so that the processes
    # SpawningWorker starts for each job skip the count and the warning as well
    _OTHER_LABELS_DUE_SETTING = "other_labels_due_at"

    def _ensureOtherLabels(self) -> None:
        """Fill OTHER_LABELS_FIELD on a database upgrading into #301, when that is cheap.

        Runs on every storage construction, so once the flag is set it costs one read of the
        settings. Before, it counts the labelled functions up to _OTHER_LABELS_STARTUP_MAX through
        their sparse index (about 60 ms at the cap) and fills the field with one server-side update
        when there are no more, which is idempotent, so processes starting together may each run it.
        A larger corpus is left to migrate_other_labels: startup would take seconds to minutes. That
        verdict is kept for _OVER_CAP_TTL seconds, in this process and in the settings, so neither a
        restart nor a job SpawningWorker starts pays the count again or repeats the warning.
        """
        settings_document = self._getDb().settings.find_one({}, {self._OTHER_LABELS_SETTING: 1, self._OTHER_LABELS_DUE_SETTING: 1}) or {}
        if settings_document.get(self._OTHER_LABELS_SETTING):
            return
        db_name = self._getDb().name
        due_since = self._other_labels_due_since.get(db_name)
        if due_since is not None and time.monotonic() - due_since < self._OVER_CAP_TTL:
            return
        now = self._getCurrentTimestamp()
        due_at = settings_document.get(self._OTHER_LABELS_DUE_SETTING)
        if isinstance(due_at, datetime.datetime) and now - due_at < datetime.timedelta(seconds=self._OVER_CAP_TTL):
            # another process found it due a moment ago, and said so
            self._other_labels_due_since[db_name] = time.monotonic()
            return
        labelled = {"function_labels.function_label": {"$exists": True}}
        num_labelled = self._getDb().functions.count_documents(labelled, limit=self._OTHER_LABELS_STARTUP_MAX + 1)
        if num_labelled > self._OTHER_LABELS_STARTUP_MAX:
            if self._noteOtherLabelsDue(db_name, due_at, now):
                LOGGER.warning(
                    "More than %d functions carry labels: a plain search term finds a function by a label other than its name only once "
                    "python -m mcrit.migrations.migrate_other_labels has run on this database (#301). Until then it finds only "
                    "the functions renamed or labelled since this upgrade; the qualified field function_labels.function_label finds all.",
                    self._OTHER_LABELS_STARTUP_MAX,
                )
            return
        started = time.monotonic()
        try:
            result = self._getDb().functions.update_many(labelled, [self._OTHER_LABELS_STAGE])
        except PyMongoError as error:
            # such as one function whose document cannot grow (10334): the functions written before
            # it keep the field, the others are the migration's, and no start tries again for a while
            self._bumpOtherLabelsVersion()
            if self._noteOtherLabelsDue(db_name, due_at, now):
                LOGGER.error(
                    "Could not derive the labels other than their name of the labelled functions (#301): %s. Run "
                    "python -m mcrit.migrations.migrate_other_labels --mode apply, which passes over and reports the functions it cannot update.",
                    error,
                )
            return
        self._bumpOtherLabelsVersion()
        self._setOtherLabelsComplete(True)
        LOGGER.info(
            "Derived the labels other than their name for %d of %d labelled functions in %.1f s (#301).", result.modified_count, result.matched_count, time.monotonic() - started
        )

    def _noteOtherLabelsDue(self, db_name: str, due_at: Any, now: datetime.datetime) -> bool:
        """Records that the database is due for migrate_other_labels, in this process and in the
        settings; True for the one process of those starting together that is to say so."""
        self._other_labels_due_since[db_name] = time.monotonic()
        noted = self._getDb().settings.update_one(
            {"$or": [{self._OTHER_LABELS_DUE_SETTING: due_at}, {self._OTHER_LABELS_DUE_SETTING: {"$exists": False}}]}, {"$set": {self._OTHER_LABELS_DUE_SETTING: now}}
        )
        return bool(noted.modified_count)

    def _ensureUniqueFamilyNames(self) -> None:
        """Index family_name as unique, so that ensureFamily's upsert tells atomically whether it created
        a family (#299). A database that holds a name twice already cannot have the index: its duplicate
        names are logged as an error and nothing is merged. ensureFamily works the same there, but two
        writers upserting one new name at the same moment can then both insert it."""
        families = self._getDb()["families"]
        indexes = families.index_information()
        duplicates = []
        if not indexes.get("family_name_1", {}).get("unique"):
            duplicates = self._findDuplicateFamilyNames()
            if not duplicates:
                # older instances created it without the constraint. A concurrent start may replace it
                # too, and a name added twice meanwhile fails the build: judged by what is there after
                if "family_name_1" in indexes:
                    with contextlib.suppress(OperationFailure):
                        families.drop_index("family_name_1")
                with contextlib.suppress(OperationFailure):
                    families.create_index("family_name", unique=True)
                indexes = families.index_information()
        if not indexes.get("family_name_1", {}).get("unique"):
            if "family_name_1" not in indexes:
                families.create_index("family_name")
            LOGGER.error(
                "Family names are not unique, so adding a family cannot check atomically whether it exists. Merge or rename the families named %s.",
                duplicates or self._findDuplicateFamilyNames(),
            )

    def _findDuplicateFamilyNames(self) -> List[str]:
        pipeline = [{"$group": {"_id": "$family_name", "count": {"$sum": 1}}}, {"$match": {"count": {"$gt": 1}}}, {"$sort": {"_id": 1}}]
        return [row["_id"] for row in self._getDb()["families"].aggregate(pipeline)]

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
    _DROPPABLE_WHEN_OVERSIZED = ("xcfg", "query_xcfg", "xcfg_staging")

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

    def _fetchXcfgBlobs(self, function_ids: List[int], progress_reporter=None) -> Dict[int, str]:
        """One `$in` per collection for a whole batch of ids - never one query per function,
        which is the N+1 shape #111 removed elsewhere."""
        blobs = {}
        for collection in ("xcfg", "query_xcfg"):
            ids = [function_id for function_id in function_ids if self._xcfgCollectionFor(function_id) == collection]
            if not ids:
                continue
            for document in self._getDb()[collection].find({"_id": {"$in": ids}}):
                blobs[document["_id"]] = document["_xcfg"]
                if progress_reporter is not None:
                    progress_reporter.step()
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

    # the SMDA report's top-level extras (#94) - whole-binary data references and xmetadata -
    # grow with the binary and have no cap, so they never go into the sample document, which
    # every sample listing and job result loads. They are a JSON blob split over documents of
    # at most this many characters (ASCII, as json.dumps escapes), well under the 16 MiB limit
    _SMDA_EXTRAS_CHUNK_CHARS = 8 * 1024 * 1024

    @staticmethod
    def _smdaExtrasCollectionFor(sample_id: int) -> str:
        return "query_smda_extras" if sample_id < 0 else "smda_extras"

    def _insertSmdaExtras(self, sample_id: int, smda_extras: Dict[str, Any]) -> None:
        blob = json.dumps(smda_extras)
        size = self._SMDA_EXTRAS_CHUNK_CHARS
        pieces = [blob[start : start + size] for start in range(0, len(blob), size)] or [""]
        documents = [{"sample_id": sample_id, "chunk": index, "num_chunks": len(pieces), "_extras": piece} for index, piece in enumerate(pieces)]
        # clear chunks an earlier, interrupted store left under this id, or they collide on the unique index
        self._deleteSmdaExtras(sample_id)
        self._dbInsertMany(self._smdaExtrasCollectionFor(sample_id), documents)

    def getSmdaExtras(self, sample_id: int) -> Optional[Dict[str, Any]]:
        documents = list(self._getDb()[self._smdaExtrasCollectionFor(sample_id)].find({"sample_id": sample_id}, {"_id": 0}).sort("chunk", 1))
        if not documents:
            return None
        if len(documents) != documents[0]["num_chunks"]:
            LOGGER.warning("SMDA extras of sample %d are incomplete: %d of %d chunks present.", sample_id, len(documents), documents[0]["num_chunks"])
            return None
        return json.loads("".join(document["_extras"] for document in documents))

    def _deleteSmdaExtras(self, sample_id: int) -> None:
        self._getDb()[self._smdaExtrasCollectionFor(sample_id)].delete_many({"sample_id": sample_id})

    @staticmethod
    def _encodePichash(function_dict: Dict, delete_old: bool = True, padded: bool = False) -> None:
        if "pichash" in function_dict:
            value = function_dict["pichash"]
            # search conditions wrap the value in an operator dict, e.g. {"$ne": 0x1234}
            if isinstance(value, dict):
                function_dict["_pichash"] = {k: encode_pichash_value(v, padded) if isinstance(v, int) else v for k, v in value.items()}
            else:
                function_dict["_pichash"] = encode_pichash_value(value, padded)
            if delete_old:
                del function_dict["pichash"]
        if "picblockhashes" in function_dict:
            converted_entries = []
            for entry in function_dict["picblockhashes"]:
                converted_entry = dict(**entry)
                converted_entry["hash"] = encode_pichash_value(converted_entry["hash"], padded)
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

    def _encodeFunction(self, function_dict: Dict, delete_old: bool = True) -> None:
        MongoDbStorage._encodePichash(function_dict, delete_old=delete_old, padded=self.isPichashPadded())
        MongoDbStorage._encodeXcfg(function_dict, delete_old=delete_old)
        MongoDbStorage._encodeMinHash(function_dict)
        # an untagged function stores no tags field, which keeps it out of the sparse tags index;
        # FunctionEntry.toDict leaves an empty list out already, this covers a dict built otherwise.
        # FunctionEntry.fromDict reads the missing field as no tags (#53)
        if "tags" in function_dict and not function_dict["tags"]:
            del function_dict["tags"]
        MongoDbStorage._encodeOtherLabels(function_dict)

    def isPichashPadded(self) -> bool:
        """Whether this instance stores pichashes zero-padded (#145); read once per process."""
        if self._pichash_padded is None:
            settings = self._getDb().settings.find_one({}, {"pichash_padded": 1, "_id": 0}) or {}
            self._pichash_padded = bool(settings.get("pichash_padded", False))
        return self._pichash_padded

    def _pichashLookupCondition(self, field: str, value: int) -> Dict[str, Any]:
        if self.isPichashPadded():
            return {field: encode_pichash_value(value, padded=True)}
        return {field: {"$in": pichash_value_variants(value)}}

    @staticmethod
    def _decodeFunction(function_dict: Dict, delete_old: bool = True) -> None:
        MongoDbStorage._decodePichash(function_dict, delete_old=delete_old)
        MongoDbStorage._decodeXcfg(function_dict, delete_old=delete_old)
        MongoDbStorage._decodeMinHash(function_dict)
        # storage-only, derived from the labels: never part of a FunctionEntry
        function_dict.pop(OTHER_LABELS_FIELD, None)

    # A minhash is stored as BSON binary: half the bytes of the hex text it used to be stored as,
    # which is 5.7% of `functions` uncompressed and an estimated 12.9% after snappy on a
    # 7,244-sample corpus with 64.8% of its functions hashed.
    # Hex written before is still read, and "" stays the marker of a function not hashed yet, so
    # the queries for unhashed functions do not change. migrate_minhash_binary converts old data.

    @staticmethod
    def _minHashForStorage(minhash_bytes: bytes):
        return bytes(minhash_bytes) if minhash_bytes else ""

    @staticmethod
    def _minHashFromStorage(stored_value) -> bytes:
        if not stored_value:
            return b""
        if isinstance(stored_value, str):
            return bytes.fromhex(stored_value)
        return bytes(stored_value)

    @staticmethod
    def _encodeMinHash(function_dict: Dict) -> None:
        # FunctionEntry.toDict carries hex, as exports and the REST API keep doing
        if isinstance(function_dict.get("minhash"), str):
            function_dict["minhash"] = MongoDbStorage._minHashForStorage(bytes.fromhex(function_dict["minhash"]))

    @staticmethod
    def _decodeMinHash(function_dict: Dict) -> None:
        # back to the hex FunctionEntry.fromDict reads, whichever form the document holds
        if "minhash" in function_dict and not isinstance(function_dict["minhash"], str):
            function_dict["minhash"] = MongoDbStorage._minHashFromStorage(function_dict["minhash"]).hex()

    @staticmethod
    def _encodeOtherLabels(function_dict: Dict) -> None:
        """Derive OTHER_LABELS_FIELD for a function document about to be inserted (#301)."""
        labels = [label.get("function_label") for label in function_dict.get("function_labels") or [] if isinstance(label, dict)]
        others = otherLabels(function_dict.get("function_name"), labels)
        if others:
            function_dict[OTHER_LABELS_FIELD] = others
        else:
            function_dict.pop(OTHER_LABELS_FIELD, None)

    @classmethod
    def _pushLabelUpdate(cls, label: FunctionLabelEntry) -> List[Dict]:
        """An update appending the label, as $push does, and deriving OTHER_LABELS_FIELD anew."""
        appended = {"$concatArrays": [{"$ifNull": ["$function_labels", []]}, [{"$literal": label.toDict()}]]}
        return [{"$set": {"function_labels": appended}}, cls._OTHER_LABELS_STAGE]

    @classmethod
    def _addLabelsUpdate(cls, labels: List[Dict]) -> List[Dict]:
        """An update adding the labels not there yet, as $addToSet with $each does, and deriving
        OTHER_LABELS_FIELD anew."""
        added = {
            "$reduce": {
                "input": {"$literal": labels},
                "initialValue": {"$ifNull": ["$function_labels", []]},
                "in": {"$cond": [{"$in": ["$$this", "$$value"]}, "$$value", {"$concatArrays": ["$$value", ["$$this"]]}]},
            }
        }
        return [{"$set": {"function_labels": added}}, cls._OTHER_LABELS_STAGE]

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
            self._deleteSmdaExtras(sample_id)
            # remove sample
            self._getDb().query_samples.delete_one({"sample_id": sample_id})
            return True
        db = self._getDb()
        # the claim on the sample is taken first: a replacement of its analysis under way (#270)
        # stops at its next step, and what it recorded tells which of its writes are counted
        claimed = db.samples.find_one_and_update(
            {"sample_id": sample_id},
            {"$set": {"verification.token": f"deleteSample:{uuid.uuid4()}", "verification.deleting": True, "verification.claimed_at": datetime.datetime.now(datetime.UTC)}},
            projection={"_id": 0, "family_id": 1, "is_library": 1, "verification": 1},
        )
        if claimed is None:
            return False
        # the family and library flag the counters are decremented by are read with the claim, not
        # before it: a replacement moves the sample's family only while it holds the claim
        family_id = claimed.get("family_id", 0)
        is_library = bool(claimed.get("is_library"))
        verification = claimed.get("verification") or {}
        swap = verification.get("swap") or {}
        function_id_range = verification.get("staging_function_ids")
        if verification.get("staging_id") is not None:
            self._discardStaging(verification["staging_id"])
        # the functions a replacement wrote under the sample, if any, apart from the old ones:
        # they are in the counters only as far as the replacement recorded counting them
        new_functions: Optional[Dict[str, Any]] = None
        old_functions: Dict[str, Any] = {"sample_id": sample_id}
        if function_id_range:
            new_functions = {"sample_id": sample_id, "function_id": {"$gte": function_id_range[0], "$lte": function_id_range[1]}}
            old_functions = {"sample_id": sample_id, "function_id": {"$not": {"$gte": function_id_range[0], "$lte": function_id_range[1]}}}
        function_minhashes = self._getFunctionMinHashesBySampleId(sample_id)
        self._pullBandEntries(function_minhashes)
        # drop this sample from the picblockhash index while its functions still exist to be read
        self._removeSampleFromPicBlockHashIndex(sample_id)
        # and from the pichash counts and the function ranges, which addSmdaReport maintains as
        # well; left in, a deleted and re-added sample was counted twice (#261). The counts are
        # taken out at most once per sample: the queue runs a failed delete job again and two
        # deletes of one sample can overlap, and a second decrement would leave other samples'
        # hashes undercounted or without a count document, which drops their exact matches
        # under MINHASH_PICHASH_MAX_MATCHES. A delete that dies after the claim but before the
        # decrement leaves the counts too high, as before this fix, until rebuildPicHashCountIndex
        # runs. One that dies after the decrement leaves them too low, since the sample's functions
        # are still stored but no longer counted, until a retry completes the delete.
        claim = db.samples.update_one({"sample_id": sample_id, "pichash_counts_removed": {"$ne": True}}, {"$set": {"pichash_counts_removed": True}})
        if claim.modified_count:
            if not swap.get("old_pichashes_uncounted"):
                self._removeFromPicHashCounts(old_functions)
            if new_functions is not None and swap.get("new_pichashes_counted"):
                self._removeFromPicHashCounts(new_functions)
        db[self._FUNCTION_RANGE_COLLECTION].delete_many({"sample_id": sample_id})

        # remove functions
        self._deleteXcfgForFunctionIds([function_minhash["function_id"] for function_minhash in function_minhashes])
        # update family stats by what was actually removed, not by what the sample claimed (#151)
        num_functions_deleted = 0
        if new_functions is None:
            num_functions_deleted = db.functions.delete_many({"sample_id": sample_id}).deleted_count
        else:
            # each by the family it is in, which a replacement under way may have left mixed
            self._deleteFunctionsCounted(old_functions)
            if swap.get("new_counted"):
                self._deleteFunctionsCounted(new_functions)
            else:
                db.functions.delete_many(new_functions)
        self._bumpOtherLabelsVersion()
        # remove sample
        # the report's extras before the sample document, as for a query sample: an interrupted
        # deletion leaves a sample to delete again, never extras nothing refers to (#94)
        self._deleteSmdaExtras(sample_id)
        num_samples_deleted = db.samples.delete_one({"sample_id": sample_id}).deleted_count
        if function_id_range:
            # a write of the replacement under way when the claim was taken can land after the
            # sweep above; the replacement removes what lands later still, once it sees the sample gone
            self._purgeDeletedSample(sample_id, function_id_range)
        # the raw submission goes with the sample it belongs to (#95)
        self.deleteSampleBinary(sample_id)
        self._updateFamilyStats(family_id, -num_samples_deleted, -num_functions_deleted, -int(is_library and num_samples_deleted))
        self._deleteFamilyIfEmpty(family_id)
        self._updateDbState()
        return True

    ###############################################################################
    # Replacing a reported sample's analysis by the disassembly of its binary (#270)
    ###############################################################################

    # The fields of a sample document a replacement takes from the binary's disassembly. The others
    # are what the sample says about itself, which the swap keeps or takes from the upload.
    _ANALYSIS_FIELDS = ("architecture", "base_addr", "binary_size", "binweight", "bitness", "sha256", "smda_version", "statistics", "timestamp")

    def getFunctionPicHashesBySampleId(self, sample_id: int) -> Dict[int, int]:
        pichashes = {}
        if sample_id < 0:
            return pichashes
        for document in self._getDb().functions.find({"sample_id": sample_id}, {"_id": 0, "offset": 1, "_pichash": 1}):
            encoded_pichash = document.get("_pichash")
            pichashes[decode_two_complement(document["offset"])] = int(encoded_pichash, 16) if isinstance(encoded_pichash, str) else 0
        return pichashes

    @staticmethod
    def _expiredClaimTime() -> datetime.datetime:
        """What a claim is dated to that any upload may take over at once."""
        return datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)

    def _holdsClaim(self, sample_id: int, token: str) -> bool:
        return self._getDb().samples.find_one({"sample_id": sample_id, "verification.token": token}, {"_id": 1}) is not None

    def _discardStaging(self, staging_id: int) -> None:
        db = self._getDb()
        for collection in self._STAGING_COLLECTIONS:
            db[collection].delete_many({"staging_id": staging_id})

    def getSampleVerificationClaim(self, sample_id: int) -> Optional[Dict[str, Any]]:
        document = self._getDb().samples.find_one({"sample_id": sample_id, "verification.token": {"$exists": True}}, {"_id": 0, "verification": 1})
        if document is None:
            return None
        verification = document["verification"]
        claimed_at = verification.get("claimed_at")
        return {
            "token": verification["token"],
            "job_id": verification.get("job_id"),
            # stored without a zone, as UTC
            "claimed_at": claimed_at.replace(tzinfo=datetime.UTC).timestamp() if claimed_at else 0.0,
            "swap_started": "swap" in verification,
        }

    def claimSampleVerification(self, sample_id: int, token: str, job_id: Optional[str], stale_after: float, dead_token: Optional[str] = None) -> str:
        db = self._getDb()
        now = datetime.datetime.now(datetime.UTC)
        takeover: List[Dict[str, Any]] = [
            {"verification.token": {"$exists": False}},
            {"verification.token": token},
            {"verification.claimed_at": {"$lt": now - datetime.timedelta(seconds=stale_after)}},
        ]
        if dead_token is not None:
            takeover.append({"verification.token": dead_token})
        # never from deleteSample, which takes the claim to stop a replacement under way
        claimable = {"sample_id": sample_id, "sha256_verified": {"$ne": True}, "verification.deleting": {"$ne": True}, "$or": takeover}
        # only the claim itself is replaced: what the job before staged or began to swap in stays
        # recorded beside it, so that a takeover dying in turn leaves it recorded as well
        previous = db.samples.find_one_and_update(
            claimable, {"$set": {"verification.token": token, "verification.job_id": job_id, "verification.claimed_at": now}}, projection={"_id": 0, "verification": 1}
        )
        if previous is None:
            sample_document = db.samples.find_one({"sample_id": sample_id}, {"_id": 0, "sha256_verified": 1})
            if sample_document is None:
                return "missing"
            return "verified" if sample_document.get("sha256_verified") is True else "busy"
        verification = previous.get("verification") or {}
        if "swap" in verification:
            # the old analysis may be partly gone already: the only way on is forward
            LOGGER.warning("Sample %d: an earlier job began to replace its analysis; finishing it (#270).", sample_id)
            return "resume"
        if verification.get("staging_id") is not None:
            self._discardStaging(verification["staging_id"])
            db.samples.update_one(
                {"sample_id": sample_id, "verification.token": token},
                {"$unset": {"verification.staging_id": "", "verification.staging_function_ids": "", "verification.staged": ""}},
            )
        return "claimed"

    def renewSampleVerification(self, sample_id: int, token: str) -> bool:
        renewed = self._getDb().samples.update_one(
            {"sample_id": sample_id, "verification.token": token}, {"$set": {"verification.claimed_at": datetime.datetime.now(datetime.UTC)}}
        )
        return bool(renewed.matched_count)

    def releaseSampleVerification(self, sample_id: int, token: str) -> None:
        db = self._getDb()
        held = {"sample_id": sample_id, "verification.token": token}
        document = db.samples.find_one(held, {"_id": 0, "verification": 1})
        if document is None:
            return
        verification = document["verification"]
        if "swap" in verification:
            # past the point of no return: the swap stays recorded and the claim expired, so that
            # the next upload of this binary finishes it straight away
            db.samples.update_one(held, {"$set": {"verification.claimed_at": self._expiredClaimTime(), "verification.job_id": None}})
            return
        if verification.get("staging_id") is not None:
            self._discardStaging(verification["staging_id"])
        db.samples.update_one(held, {"$unset": {"verification": ""}})

    def stageSampleAnalysis(self, sample_id: int, smda_report: "SmdaReport", token: str) -> bool:
        db = self._getDb()
        holds = {"sample_id": sample_id, "verification.token": token, "verification.swap": {"$exists": False}}
        sample_document = db.samples.find_one(holds, {"_id": 0, "sha256": 1})
        if sample_document is None:
            return False
        assert smda_report.sha256 == sample_document["sha256"]
        smda_functions = list(smda_report.getFunctions())
        staging_id = self._useCounter("staging")
        function_ids = list(self._useCounterBulk("functions", len(smda_functions)))
        # recorded before anything is written, so that whoever finds it - this job on a failure,
        # the next claimant, deleteSample - discards what was staged
        staging = {"verification.staging_id": staging_id, "verification.staging_function_ids": [function_ids[0], function_ids[-1]] if function_ids else None}
        if not db.samples.update_one(holds, {"$set": staging}).matched_count:
            return False
        holds_staging = {**holds, "verification.staging_id": staging_id}
        staged_entry = SampleEntry(smda_report, sample_id=sample_id, family_id=0)
        try:
            function_documents = []
            xcfg_documents = []
            for function_id, smda_function in zip(function_ids, smda_functions):
                function_document = self._getFunctionDocument(staged_entry, smda_function, function_id)
                xcfg_blob = function_document.pop("_xcfg", None)
                if xcfg_blob:
                    xcfg_documents.append({"_id": function_id, "staging_id": staging_id, "_xcfg": xcfg_blob})
                function_document["staging_id"] = staging_id
                function_documents.append(function_document)
            self._dbInsertMany(self._STAGING_XCFG, xcfg_documents)
            self._dbInsertMany(self._STAGING_FUNCTIONS, function_documents)
            blob = json.dumps(SampleEntry.smdaExtrasOf(smda_report))
            size = self._SMDA_EXTRAS_CHUNK_CHARS
            pieces = [blob[start : start + size] for start in range(0, len(blob), size)] or [""]
            self._dbInsertMany(
                self._STAGING_SMDA_EXTRAS, [{"staging_id": staging_id, "chunk": index, "num_chunks": len(pieces), "_extras": piece} for index, piece in enumerate(pieces)]
            )
            analysis = {field: value for field, value in staged_entry.toDict().items() if field in self._ANALYSIS_FIELDS}
            staged = db.samples.update_one(holds_staging, {"$set": {"verification.staged": analysis}}).matched_count
        except Exception:
            self._discardStaging(staging_id)
            db.samples.update_one(holds_staging, {"$unset": {"verification.staging_id": "", "verification.staging_function_ids": ""}})
            raise
        if not staged:
            # the claim went elsewhere meanwhile, and whoever took it may have discarded the
            # staging before this job had written all of it
            self._discardStaging(staging_id)
            return False
        return True

    def _abandonSwap(self, sample_id: int, staging_id: int, function_id_range: Optional[List[int]], written_documents: List[Dict[str, Any]]) -> Dict[str, Any]:
        """A swap that lost its claim stops here, removing what it wrote that no one else will.
        written_documents are the new functions it indexed, whose index entries it may have written
        after deleteSample read the functions it removes them by."""
        verification = self._getDb().samples.find_one({"sample_id": sample_id}, {"_id": 0, "verification": 1})
        if verification is None:
            # deleted meanwhile: deleteSample removed what it found, this removes what was
            # written after, so that nothing is left under the sample id
            self._purgeDeletedSample(sample_id, function_id_range, written_documents)
            self._discardStaging(staging_id)
            LOGGER.info("Sample %d was deleted while its analysis was being replaced.", sample_id)
            return {"status": "sample_deleted"}
        if (verification.get("verification") or {}).get("staging_id") != staging_id:
            # the claimant after this job dropped its staging, or finished its swap already
            self._discardStaging(staging_id)
        LOGGER.info("Sample %d: another job took over replacing its analysis.", sample_id)
        return {"status": "claim_lost"}

    def _purgeDeletedSample(self, sample_id: int, function_id_range: Optional[List[int]], written_documents: Optional[List[Dict[str, Any]]] = None) -> None:
        """Remove whatever is still stored under a deleted sample's id, with its index entries.

        Only for a sample whose document is gone: a write under way when deleteSample ran can land
        after it. Counters are left alone, deleteSample settles them from what the swap recorded.
        """
        db = self._getDb()
        function_documents = list(db.functions.find({"sample_id": sample_id}, {"_id": 0, "function_id": 1, "_picblockhashes": 1}))
        self._pullSampleFromPicBlockHashes(sample_id, self._picBlockHashesOfDocuments(function_documents + list(written_documents or [])))
        self._deleteXcfgForFunctionIds([document["function_id"] for document in function_documents])
        if function_id_range:
            # the staged disassembly merged in, whether or not its function document was
            db.xcfg.delete_many({"_id": {"$gte": function_id_range[0], "$lte": function_id_range[1]}})
        db.functions.delete_many({"sample_id": sample_id})
        self._bumpOtherLabelsVersion()
        db[self._FUNCTION_RANGE_COLLECTION].delete_many({"sample_id": sample_id})
        self._deleteSmdaExtras(sample_id)

    def _isBeingDeleted(self, sample_id: int) -> bool:
        document = self._getDb().samples.find_one({"sample_id": sample_id}, {"_id": 0, "verification.deleting": 1})
        return document is None or bool((document.get("verification") or {}).get("deleting"))

    def _markSwapStep(self, sample_id: int, token: str, step: str, value: Any = True) -> bool:
        """Record a step of a swap as done, once: True only for the call that recorded it."""
        recorded = self._getDb().samples.update_one(
            {"sample_id": sample_id, "verification.token": token, f"verification.swap.{step}": {"$exists": False}}, {"$set": {f"verification.swap.{step}": value}}
        )
        return bool(recorded.modified_count)

    def swapSampleAnalysis(self, sample_id: int, token: str, filename: Optional[str] = None, family: Optional[str] = None, version: Optional[str] = None) -> Dict[str, Any]:
        db = self._getDb()
        holds = {"sample_id": sample_id, "verification.token": token, "verification.staged": {"$exists": True}}
        document = db.samples.find_one(holds, {"_id": 0, "base_addr": 1, "family": 1, "family_id": 1, "verification": 1})
        if document is None:
            # nothing written by this call; what was staged went with the claim or the sample
            return {"status": "claim_lost" if self.isSampleId(sample_id) else "sample_deleted"}
        staging_id = document["verification"]["staging_id"]
        function_id_range = document["verification"].get("staging_function_ids")
        staged_analysis = document["verification"]["staged"]
        new_base_addr = decode_two_complement(staged_analysis["base_addr"])

        new_documents: List[Dict[str, Any]] = []

        def lost() -> Dict[str, Any]:
            return self._abandonSwap(sample_id, staging_id, function_id_range, new_documents)

        # 1. the point of no return: from here on the swap is finished, by this job or the next
        # claimant. What the upload says about the sample is recorded with it, and applied only
        # after it, so that a job losing its claim before leaves the sample as it was
        self._markSwapStep(sample_id, token, "started", {"old_base_addr": document["base_addr"], "upload": {"family": family, "filename": filename, "version": version}})
        if function_id_range:
            first_function_id, last_function_id = function_id_range
            new_functions: Dict[str, Any] = {"sample_id": sample_id, "function_id": {"$gte": first_function_id, "$lte": last_function_id}}
            old_functions: Dict[str, Any] = {"sample_id": sample_id, "function_id": {"$not": {"$gte": first_function_id, "$lte": last_function_id}}}
        else:
            new_functions = {"sample_id": sample_id, "function_id": {"$in": []}}
            old_functions = {"sample_id": sample_id}
        swap_document = db.samples.find_one(holds, {"_id": 0, "verification.swap": 1})
        if swap_document is None:
            return lost()
        swap = swap_document["verification"]["swap"]
        # a resuming upload's own metadata wins, what the job before it recorded stands in
        recorded_upload = swap["started"].get("upload") or {}
        family = family if family is not None else recorded_upload.get("family")
        filename = filename if filename is not None else recorded_upload.get("filename")
        version = version if version is not None else recorded_upload.get("version")
        if family is not None:
            # moved to as modifySample does it, functions and counters alike; a no-op once done
            self.modifySample(sample_id, {"family_name": family})
        family_document = db.samples.find_one(holds, {"_id": 0, "family_id": 1})
        if family_document is None:
            return lost()
        # the family the new functions are written in, fixed once, so that a job finishing the
        # swap writes and counts them in the same one
        self._markSwapStep(sample_id, token, "family_id", family_document["family_id"])
        family_document = db.samples.find_one(holds, {"_id": 0, "verification.swap.family_id": 1})
        if family_document is None:
            return lost()
        family_id = family_document["verification"]["swap"]["family_id"]
        # 2. the new functions under the sample id, from where no reader looked. A reader sees
        # both function sets under the sample from here until step 4 removes the old ones
        db[self._STAGING_FUNCTIONS].aggregate(
            [
                {"$match": {"staging_id": staging_id}},
                {"$set": {"sample_id": sample_id, "family_id": family_id}},
                {"$unset": "staging_id"},
                {"$merge": {"into": "functions", "on": "_id", "whenMatched": "keepExisting", "whenNotMatched": "insert"}},
            ]
        )
        # staged as addSmdaReport writes functions, OTHER_LABELS_FIELD and all (#301)
        self._bumpOtherLabelsVersion()
        db[self._STAGING_XCFG].aggregate(
            [{"$match": {"staging_id": staging_id}}, {"$unset": "staging_id"}, {"$merge": {"into": "xcfg", "on": "_id", "whenMatched": "keepExisting", "whenNotMatched": "insert"}}]
        )
        if not self._holdsClaim(sample_id, token):
            return lost()
        new_documents.extend(db.functions.find(new_functions, {"_id": 0, "function_id": 1, "sample_id": 1, "offset": 1, "_pichash": 1, "_picblockhashes": 1}))
        # counted once each, in the family they were written in: a move by modifySample in
        # between moves them out of it with their count
        if self._markSwapStep(sample_id, token, "new_counted"):
            self._updateFamilyStats(family_id, 0, len(new_documents), 0)
        # the pichash counts in two marks, around the increments: deleteSample takes out only
        # counts recorded as added, and a job that lost its claim to it after adding them takes
        # them out itself. Both would race on removing the count documents a decrement empties
        counted_here = self._markSwapStep(sample_id, token, "new_pichashes_counting")
        if counted_here:
            try:
                self._addToPicHashCounts(new_documents)
            except Exception:
                # not counted after all, as far as this job knows: the next claimant counts them.
                # An unordered add may have landed in part, which that would count twice, so the
                # counts are not trusted until rebuilt either way
                self._setPicHashCountIndexComplete(False)
                db.samples.update_one({"sample_id": sample_id, "verification.token": token}, {"$unset": {"verification.swap.new_pichashes_counting": ""}})
                raise
        elif "new_pichashes_counting" in swap and "new_pichashes_counted" not in swap:
            # a job died adding them, and an unordered add may have landed in part: the counts
            # are not trusted until rebuilt, rather than left too low for the cutoff to read
            LOGGER.warning("Sample %d: pichash counts of a replacement may be partial; marking the count index for a rebuild (#270).", sample_id)
            self._setPicHashCountIndexComplete(False)
        if not self._markSwapStep(sample_id, token, "new_pichashes_counted") and not self._holdsClaim(sample_id, token):
            if counted_here and self._isBeingDeleted(sample_id):
                self._uncountPicHashes(new_documents)
            return lost()
        self._addToPicBlockHashIndex(new_documents)
        self._recordFunctionRange(sample_id, [new_document["function_id"] for new_document in new_documents])
        staged_extras = list(db[self._STAGING_SMDA_EXTRAS].find({"staging_id": staging_id}, {"_id": 0, "staging_id": 0}).sort("chunk", 1))
        if not self._holdsClaim(sample_id, token):
            return lost()
        if staged_extras:
            self._deleteSmdaExtras(sample_id)
            self._dbInsertMany("smda_extras", [{**extras_document, "sample_id": sample_id} for extras_document in staged_extras])
        if not self._holdsClaim(sample_id, token):
            return lost()
        # all of it is under the sample now; the rest of the swap reads it from there
        self._discardStaging(staging_id)
        # 3. labels, by offset from the base address, as they are right before the old functions go
        if "labels" not in swap:
            old_base_addr = decode_two_complement(swap["started"]["old_base_addr"])
            new_function_ids = {decode_two_complement(new_document["offset"]) - new_base_addr: new_document["function_id"] for new_document in new_documents}
            labels_by_offset = {
                decode_two_complement(old_document["offset"]) - old_base_addr: old_document["function_labels"]
                for old_document in db.functions.find({**old_functions, "function_labels.0": {"$exists": True}}, {"_id": 0, "offset": 1, "function_labels": 1})
            }
            carried = [(new_function_ids[offset], labels) for offset, labels in labels_by_offset.items() if offset in new_function_ids]
            labels = {
                "old_num_functions": db.functions.count_documents(old_functions),
                "labels_carried": sum(len(function_labels) for _, function_labels in carried),
                "labels_dropped": sum(len(function_labels) for offset, function_labels in labels_by_offset.items() if offset not in new_function_ids),
            }
            if not self._holdsClaim(sample_id, token):
                return lost()
            if carried:
                # added to, not set: a label put on a new function meanwhile stays. The labels
                # other than the name follow in the same update (#301)
                db.functions.bulk_write(
                    [UpdateOne({"function_id": function_id}, self._addLabelsUpdate(function_labels)) for function_id, function_labels in carried], ordered=False
                )
                self._bumpOtherLabelsVersion()
            self._markSwapStep(sample_id, token, "labels", labels)
            swap["labels"] = labels
        # 4. the old functions, with what is derived from them; read before their pichash counts
        # are claimed, so that exactly what was there is uncounted
        if not self._holdsClaim(sample_id, token):
            return lost()
        old_documents = list(db.functions.find(old_functions, {"_id": 0, "function_id": 1, "minhash": 1, "_pichash": 1, "_picblockhashes": 1}))
        if self._markSwapStep(sample_id, token, "old_pichashes_uncounted"):
            self._uncountPicHashes(old_documents)
        old_block_hashes = set(self._picBlockHashesOfDocuments(old_documents)) - set(self._picBlockHashesOfDocuments(new_documents))
        self._pullSampleFromPicBlockHashes(sample_id, sorted(old_block_hashes))
        self._pullBandEntries(old_documents)
        if function_id_range:
            db[self._FUNCTION_RANGE_COLLECTION].delete_many(
                {"sample_id": sample_id, "$or": [{"first_function_id": {"$lt": function_id_range[0]}}, {"first_function_id": {"$gt": function_id_range[1]}}]}
            )
        else:
            db[self._FUNCTION_RANGE_COLLECTION].delete_many({"sample_id": sample_id})
        self._deleteXcfgForFunctionIds([old_document["function_id"] for old_document in old_documents])
        self._deleteFunctionsCounted(old_functions)
        self._bumpOtherLabelsVersion()
        # 5. the sample takes the new analysis, and is verified
        sample_update = {**staged_analysis, "sha256_verified": True, "pichash_smda_version": SmdaConfig().VERSION, "picblockhash_version": PICBLOCKS_VERSION}
        if filename is not None:
            sample_update["filename"] = filename
        if version is not None:
            sample_update["version"] = version
        finished = db.samples.update_one(
            holds,
            {
                "$set": sample_update,
                # the new functions are unhashed, and nothing is known yet about rehashing them
                "$unset": {"verification": "", "minhash_smda_version": "", "minhash_shingler_revision": "", "pichash_unrehashable_smda_version": ""},
            },
        )
        if not finished.matched_count:
            return lost()
        # 6. functions written in a family the sample has left meanwhile follow it
        self._moveFunctionsToSampleFamily(sample_id)
        self._updateDbState()
        return {"status": "replaced", "new_num_functions": len(new_documents), **swap["labels"]}

    def _deleteFunctionsCounted(self, function_filter: Dict[str, Any]) -> int:
        """Delete the functions function_filter selects, uncounting each from the family it is in."""
        db = self._getDb()
        num_deleted = 0
        # by family as stored when deleted, so that a concurrent family move counts each once
        for _ in range(3):
            family_ids = db.functions.distinct("family_id", function_filter)
            if not family_ids:
                break
            for family_id in family_ids:
                num_family_deleted = db.functions.delete_many({**function_filter, "family_id": family_id}).deleted_count
                if num_family_deleted:
                    self._updateFamilyStats(family_id, 0, -num_family_deleted, 0)
                    num_deleted += num_family_deleted
        return num_deleted

    def _moveFunctionsToSampleFamily(self, sample_id: int) -> None:
        db = self._getDb()
        sample_document = db.samples.find_one({"sample_id": sample_id}, {"_id": 0, "family_id": 1})
        if sample_document is None:
            return
        family_id = sample_document["family_id"]
        for other_family_id in db.functions.distinct("family_id", {"sample_id": sample_id, "family_id": {"$ne": family_id}}):
            num_moved = db.functions.update_many({"sample_id": sample_id, "family_id": other_family_id}, {"$set": {"family_id": family_id}}).modified_count
            if num_moved:
                self._updateFamilyStats(other_family_id, 0, -num_moved, 0)
                self._updateFamilyStats(family_id, 0, num_moved, 0)
            self._deleteFamilyIfEmpty(other_family_id)

    def _deleteFamilyIfEmpty(self, family_id: int) -> None:
        # judged by the samples that exist, not by a counter that may have drifted (#151)
        if family_id != 0 and self._getDb().samples.count_documents({"family_id": family_id}, limit=1) == 0:
            self._getDb().families.delete_one({"family_id": family_id})

    def _ensureFamilyDocument(self, family_id: int, family_name: str) -> int:
        """Create the family document for an id that samples reference but no document describes, and
        answer the family id they are filed under then.

        Imports carry family ids from the exporting instance; a sample whose family document is
        missing would otherwise have its statistics increments silently discarded (#151). Writers call
        this once they have filed a sample, so that a failed import removing the family in between
        (#299) does not leave it without one. When another family carries the name by now, the samples
        and functions filed under the id are re-filed onto that one: a name is one family.
        """
        while True:
            try:
                upserted = self._getDb().families.update_one(
                    {"family_id": family_id},
                    {"$setOnInsert": FamilyEntry(family_name=family_name, family_id=family_id).toDict()},
                    upsert=True,
                )
            except DuplicateKeyError:
                other_family_id = self.getFamilyId(family_name)
                # deleted again since: the name is free once more
                if other_family_id is None:
                    continue
                self._getDb().samples.update_many({"family_id": family_id}, {"$set": {"family_id": other_family_id}})
                self._getDb().functions.update_many({"family_id": family_id}, {"$set": {"family_id": other_family_id}})
                LOGGER.warning("Family %d has no document, and its name %r is family %d's: its samples and functions are filed there.", family_id, family_name, other_family_id)
                self._updateDbState()
                return other_family_id
            if upserted.upserted_id is not None:
                # the id may have come from elsewhere (an import): the counter must never hand it out again
                self._getDb().counters.update_one({"name": "families"}, {"$max": {"value": family_id + 1}}, upsert=True)
            return family_id

    def _pullBandEntries(self, function_minhashes: List[Dict[str, Any]]) -> int:
        """Take the given functions' minhashes out of the band index; how many had one."""
        # collect all band entries that need updating and pull all function_ids at once.
        # might need to batch this into slices of function_ids again
        minhashes_to_remove: Dict[int, Dict[int, List[int]]] = {band_number: {} for band_number in range(self._storage_config.STORAGE_NUM_BANDS)}
        num_hashed = 0
        for function_minhash in function_minhashes:
            minhash_bytes = self._minHashFromStorage(function_minhash.get("minhash"))
            # remove minhash entries, if necessary
            if not minhash_bytes:
                continue
            num_hashed += 1
            # judged by its bytes before it is read: reading or banding one that cannot have been
            # banded raised, on every attempt to delete its sample or repair its minhashes
            if not self._hasBandableLength(minhash_bytes):
                LOGGER.warning("Function %d holds a minhash of %d bytes, which cannot have band entries.", function_minhash["function_id"], len(minhash_bytes))
                continue
            minhash = MinHash(function_id=function_minhash["function_id"], minhash_bytes=minhash_bytes, minhash_bits=self._minhash_config.MINHASH_SIGNATURE_BITS)
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
        self._getDb().samples.update_one({"sample_id": sample_id}, {"$unset": {"minhash_smda_version": "", "minhash_shingler_revision": ""}})
        return num_hashed

    def setMinHashVersionForSamples(
        self, smda_version: str, sample_ids: Optional[List[int]] = None, except_sample_ids: Optional[Iterable[int]] = None, finish_pending: bool = False
    ) -> None:
        query: Dict[str, Any] = {} if sample_ids is None else {"sample_id": {"$in": list(sample_ids)}}
        if except_sample_ids:
            query = {"$and": [query, {"sample_id": {"$nin": list(except_sample_ids)}}]}
        update: Dict[str, Any] = {"$set": {"minhash_smda_version": smda_version, "minhash_shingler_revision": MINHASH_SHINGLER_REVISION}}
        if finish_pending:
            update["$unset"] = {"minhash_pending": ""}
        self._getDb().samples.update_many(query, update)
        if not finish_pending:
            # still pending, now as hashed by this smda and shingler revision
            pending = {"minhash_pending.smda_version": smda_version, "minhash_pending.shingler_revision": MINHASH_SHINGLER_REVISION}
            self._getDb().samples.update_many({"$and": [query, {"minhash_pending": {"$exists": True}}]}, {"$set": pending})

    def getSamplesWithPendingMinHashes(self, threshold_version: str) -> List[int]:
        # few distinct versions are ever pending, so compare those rather than every document
        threshold = version.parse(threshold_version)
        has_marker = {"minhash_pending.smda_version": {"$exists": True}}
        recorded = self._getDb().samples.distinct("minhash_pending.smda_version", has_marker)
        compatible = [value for value in recorded if not self._isStaleMinHashVersion(value, threshold)]
        if not compatible:
            return []
        query = {"minhash_pending.smda_version": {"$in": compatible}, "minhash_pending.shingler_revision": MINHASH_SHINGLER_REVISION}
        return sorted(document["sample_id"] for document in self._getDb().samples.find(query, {"sample_id": 1, "_id": 0}))

    # the first query for a sample's candidates is this small: before hashing, the first candidate
    # already settles the sample; after it, there are none
    _UNHASHED_PROBE_SIZE = 16
    _UNHASHED_CHECK_BATCH_SIZE = 1000

    def getSamplesWithUnhashedFunctions(self, sample_ids: List[int]) -> Set[int]:
        hashable_query = self._hashableSizeQuery()
        if hashable_query is None:
            return set()
        unhashed_sample_ids = set()
        for sample_id in sample_ids:
            if sample_id < 0:
                continue
            # one sample at a time, on the sample_id index
            query = {"sample_id": sample_id, "minhash": "", **hashable_query}
            probe = [document["function_id"] for document in self._getDb().functions.find(query, {"_id": 0, "function_id": 1}).limit(self._UNHASHED_PROBE_SIZE)]
            if self._hasStoredXcfg(probe):
                unhashed_sample_ids.add(sample_id)
                continue
            if len(probe) < self._UNHASHED_PROBE_SIZE:
                continue
            with self._getDb().functions.find(query, {"_id": 0, "function_id": 1}).batch_size(self._UNHASHED_CHECK_BATCH_SIZE) as cursor:
                for function_ids in self._batchedIds(cursor, self._UNHASHED_CHECK_BATCH_SIZE):
                    if self._hasStoredXcfg(function_ids):
                        unhashed_sample_ids.add(sample_id)
                        break
        return unhashed_sample_ids

    def _hasStoredXcfg(self, function_ids: List[int]) -> bool:
        """Whether any of these (non-query) functions has disassembly in the xcfg collection, where
        "{}" means none, as _attachXcfgBlobs reads it. A pending sample was stored by a release that
        keeps disassembly there only, so an inline _xcfg (#137) is not looked for."""
        if not function_ids:
            return False
        return self._getDb().xcfg.find_one({"_id": {"$in": function_ids}, "_xcfg": {"$nin": ["", "{}", None]}}, {"_id": 1}) is not None

    def _staleMinHashVersionQuery(self, threshold_version: str) -> Dict[str, Any]:
        """Samples carry few distinct recorded versions, so compare those instead of every document."""
        threshold = version.parse(threshold_version)
        recorded = self._getDb().samples.distinct("minhash_smda_version")
        stale_values = [value for value in recorded if self._isStaleMinHashVersion(value, threshold)]
        stale_revisions = [
            {"architecture": architecture, "$or": [{"minhash_shingler_revision": {"$exists": False}}, {"minhash_shingler_revision": {"$lt": since}}]}
            for architecture, since in SHINGLER_REVISION_SINCE.items()
        ]
        return {"$or": [{"minhash_smda_version": {"$exists": False}}, {"minhash_smda_version": {"$in": stale_values}}, *stale_revisions]}

    def getSamplesWithStaleMinHashes(self, threshold_version: str) -> List[int]:
        query = self._staleMinHashVersionQuery(threshold_version)
        return sorted(document["sample_id"] for document in self._getDb().samples.find(query, {"sample_id": 1, "_id": 0}))

    def countSamplesWithStaleMinHashes(self, threshold_version: str) -> int:
        return self._getDb().samples.count_documents(self._staleMinHashVersionQuery(threshold_version))

    def _stalePicBlockHashQuery(self) -> Dict[str, Any]:
        """Samples of an architecture other than Intel whose block hashes a picblocks before
        PICBLOCKS_ARCHITECTURE_AWARE computed, or an unrecorded one - it escaped every block as Intel
        code (#240). Intel blocks hash the same either way, and a sample of unknown architecture has
        no code to hash."""
        threshold = version.parse(PICBLOCKS_ARCHITECTURE_AWARE)
        recorded = self._getDb().samples.distinct("picblockhash_version")
        stale_values = [value for value in recorded if self._isStaleMinHashVersion(value, threshold)]
        return {
            "architecture": {"$nin": ["intel", ""]},
            "$or": [{"picblockhash_version": {"$exists": False}}, {"picblockhash_version": {"$in": stale_values}}],
            **self._notUnrehashableQuery(),
        }

    @staticmethod
    def _notUnrehashableQuery() -> Dict[str, Any]:
        """Leaves out samples the running smda already found missing disassembly, which a rerun
        cannot rehash either; a marker of an older smda is retried once (#249)."""
        return {"pichash_unrehashable_smda_version": {"$ne": SmdaConfig().VERSION}}

    def countSamplesWithUnrehashablePicHashes(self) -> Optional[int]:
        return self._getDb().samples.count_documents({"pichash_unrehashable_smda_version": SmdaConfig().VERSION})

    def countSamplesWithStalePicBlockHashes(self) -> Optional[int]:
        return self._getDb().samples.count_documents(self._stalePicBlockHashQuery())

    def _stalePicHashQuery(self, threshold_version: str) -> Dict[str, Any]:
        """Samples whose PicHashes an smda older than the threshold escaped, as recorded by
        recalculateAllPicHashes, or - where it never stamped one - as the report's smda says (#249).
        Like the minhash query, this compares the few distinct recorded values, not every document."""
        threshold = version.parse(threshold_version)
        stamped = self._getDb().samples.distinct("pichash_smda_version")
        # distinct answers null for unstamped (or null-stamped) documents, which the other clauses decide
        stale_stamps = [value for value in stamped if value is not None and self._isStaleMinHashVersion(value, threshold)]
        reported = self._getDb().samples.distinct("smda_version")
        stale_reports = [value for value in reported if self._isStaleMinHashVersion(self._stripReportVersionPrefix(value), threshold)]
        return {
            "$or": [
                {"pichash_smda_version": {"$in": stale_stamps}},
                {"pichash_smda_version": None, "smda_version": {"$in": stale_reports}},
                # a report without any smda version, which distinct need not answer as null
                {"pichash_smda_version": None, "smda_version": None},
            ],
            **self._notUnrehashableQuery(),
        }

    def countSamplesWithStalePicHashes(self) -> Optional[int]:
        return self._getDb().samples.count_documents(self._stalePicHashQuery(self._getPicHashCompatibilityThreshold()))

    @staticmethod
    def _getPicHashCompatibilityThreshold() -> str:
        smda_config = SmdaConfig()
        smda_downward_compatibility = getattr(smda_config, "ESCAPER_DOWNWARD_COMPATIBILITY", None)
        if smda_downward_compatibility is None:
            LOGGER.warning("SMDA downward compatibility version unknown, using current SMDA version as threshold...")
            smda_downward_compatibility = smda_config.VERSION
        return smda_downward_compatibility

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
                    family_id = self._ensureFamilyDocument(family_id, family_name)
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
            # the labels that differ from the name follow the rename in the same update (#301); a
            # name is a literal here, where a "$" would otherwise make it a field path
            rename = [{"$set": {"function_name": {"$literal": function_name}}}, self._OTHER_LABELS_STAGE]
            self._getDb().functions.update_one({"function_id": function_id}, rename)
            if function_name:
                # the "not yet labelled so by this user" check is part of the update's filter,
                # so two concurrent identical requests cannot both push the label
                label_username = username or "anonymous"
                not_yet_labelled = {"function_id": function_id, "function_labels": {"$not": {"$elemMatch": {"username": label_username, "function_label": function_name}}}}
                self._getDb().functions.update_one(not_yet_labelled, self._pushLabelUpdate(FunctionLabelEntry(function_name, label_username)))
            self._bumpOtherLabelsVersion()
        return True

    def modifyFamily(self, family_id: int, update_information: dict) -> bool:
        if not self.isFamilyId(family_id):
            return False
        old_family_info = self.getFamily(family_id)
        assert old_family_info is not None
        if "actors" in update_information:
            self._getDb().families.update_one({"family_id": family_id}, {"$set": {"actors": FamilyEntry.normalizeActors(update_information["actors"])}})
        if "is_library" in update_information:
            self._getDb().samples.update_many({"family_id": family_id}, {"$set": {"is_library": update_information["is_library"]}})
            updated_count = old_family_info.num_samples if update_information["is_library"] else 0
            self._getDb().families.update_one({"family_id": family_id}, {"$set": {"num_library_samples": updated_count}})
            self._updateDbState()
        # the family's own name is not a rename: merging a family into itself deleted the document its
        # samples still point to, or for family 0 doubled its counters. Compared with the stored name
        # rather than looked up, because another family may carry the same name in a database from
        # before family names were unique (#299)
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
            # the attribution moves with the samples: a rename onto an existing family merges
            # both actor lists (a review of #57 caught the rename dropping them), up to MAX_ACTORS
            # as an import does - uncapped, renaming many families into one grew it without bound
            merged_actors, num_dropped = FamilyEntry.mergeActors(new_family_info.actors, old_family_info.actors)
            if num_dropped:
                LOGGER.warning(
                    "Family %d would be attributed to more than %d actors, dropping %d actors of merged family %d.", new_family_id, FamilyEntry.MAX_ACTORS, num_dropped, family_id
                )
            family_update: Dict[str, Any] = {
                "$set": {"num_samples": new_num_samples, "num_functions": new_num_functions, "num_library_samples": new_num_lib_samples, "actors": merged_actors}
            }
            # and so do its tags (#53), added to the target's rather than replacing them, so that
            # a tag added to the target meanwhile is not lost. Not capped at MAX_TAGS_PER_ENTITY
            # (see mcrit.libs.tags)
            if old_family_info.tags:
                family_update["$addToSet"] = {"tags": {"$each": list(old_family_info.tags)}}
            self._getDb().families.update_one({"family_id": new_family_id}, family_update)
            # update sample_entry and function_entries with new family information
            self._getDb().samples.update_many({"family_id": family_id}, {"$set": {"family_id": new_family_id, "family": family_name}})
            self._getDb().functions.update_many({"family_id": family_id}, {"$set": {"family_id": new_family_id}})
            self._updateDbState()
        return True

    # the collection and id field that hold each kind of taggable entity (#53)
    _TAG_COLLECTIONS = {"family": ("families", "family_id"), "sample": ("samples", "sample_id"), "function": ("functions", "function_id")}

    def _updateTags(self, entity: str, entity_id: int, update: Dict[str, Any], condition: Optional[Dict[str, Any]] = None) -> Optional[List[str]]:
        collection, id_field = self._TAG_COLLECTIONS[checkTagEntity(entity)]
        # query samples and functions (negative ids) live in collections of their own and carry no tags
        if entity != "family" and entity_id < 0:
            return None
        document = self._getDb()[collection].find_one_and_update(
            {id_field: entity_id, **(condition or {})}, update, projection={"_id": 0, "tags": 1}, return_document=ReturnDocument.AFTER
        )
        if document is None:
            if condition is None:
                return None
            # no such entity, or one the condition refused
            existing = self._getDb()[collection].find_one({id_field: entity_id}, projection={"_id": 0, "tags": 1})
            if existing is None:
                return None
            raise TagLimitError(tagLimitMessage(entity, entity_id, len(existing.get("tags") or [])))
        tags = list(document.get("tags") or [])
        if entity == "function" and "tags" in document and not tags:
            # the last tag is gone: drop the field again, to keep the function out of the sparse index.
            # Conditional on the array still being empty, so that a concurrent addTags is not undone
            self._getDb()[collection].update_one({id_field: entity_id, "tags": {"$size": 0}}, {"$unset": {"tags": ""}})
        return tags

    def addTags(self, entity: str, entity_id: int, tags: List[str]) -> Optional[List[str]]:
        tags = normalizeTags(tags)
        # The cap is part of the filter, so that checking it and adding the tags are one atomic
        # write: a concurrent addTags cannot slip in between and take the entity past it. The
        # filter counts the union of the stored and the new tags, exactly what $addToSet leaves,
        # so re-adding tags an entity at the cap carries already is not refused. $literal keeps a
        # tag from being read as an expression; the tag rule already refuses a leading "$".
        within_cap = {"$expr": {"$lte": [{"$size": {"$setUnion": [{"$ifNull": ["$tags", []]}, {"$literal": tags}]}}, MAX_TAGS_PER_ENTITY]}}
        return self._updateTags(entity, entity_id, {"$addToSet": {"tags": {"$each": tags}}}, condition=within_cap)

    def removeTags(self, entity: str, entity_id: int, tags: List[str]) -> Optional[List[str]]:
        tags = normalizeTags(tags)
        return self._updateTags(entity, entity_id, {"$pull": {"tags": {"$in": tags}}})

    def getTagCounts(self, entity: str) -> Dict[str, int]:
        collection, _ = self._TAG_COLLECTIONS[checkTagEntity(entity)]
        # for functions the $exists filter is answered from the sparse tags index, which holds the
        # tagged functions only, so the count does not read the whole collection
        pipeline = [
            {"$match": {"tags": {"$exists": True}}},
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}},
        ]
        # the $group holds one entry per distinct tag; allowDiskUse lets a large vocabulary spill past
        # MongoDB's 100 MB stage limit, which before 6.0 is an error by default
        return {document["_id"]: document["count"] for document in self._getDb()[collection].aggregate(pipeline, allowDiskUse=True)}

    # query ids are handed out from a counter and stored negated, so a smaller id is a newer
    # record; deletes and the $in/$nin arguments are kept to this many ids per command
    _ORPHAN_BATCH_SIZE = 5000

    def deleteOrphanedQueryData(self) -> Dict[str, int]:
        """Delete the query functions no query sample refers to and the query disassembly no
        query function refers to. Both are left behind when a deletion or an insert is
        interrupted halfway, and a query job can be deleted without its sample (#68).

        Safe next to a query being inserted on another worker. A query is inserted as its
        sample, then the disassembly of every function, then the functions. Each boundary is
        taken first, and only records older than it (a larger, i.e. less negative, id) are
        judged: a query whose ids are handed out afterwards is never looked at. For the
        functions that is enough, as their sample is written before them. The disassembly is
        written before its function, so it is judged only once no query sample is still short
        of its functions - see _aQueryInsertMayBeInFlight.

        No single command carries the whole collection. The sample ids come from an
        aggregation cursor rather than distinct(), which answers with one document and fails
        past MongoDB's 16 MiB limit, and every `$in` below is one batch wide.
        """
        db = self._getDb()
        # both boundaries before anything is judged
        newest_function = db.query_functions.find_one({}, {"function_id": 1, "_id": 0}, sort=[("function_id", 1)])
        newest_xcfg = db.query_xcfg.find_one({}, {"_id": 1}, sort=[("_id", 1)])
        num_functions_deleted = 0
        if newest_function is not None:
            num_functions_deleted = self._deleteQueryFunctionsWithoutASample(newest_function["function_id"])
        num_xcfg_deleted = 0
        if newest_xcfg is not None:
            num_xcfg_deleted = self._deleteQueryXcfgWithoutAFunction(newest_xcfg["_id"])
        return {"query_functions": num_functions_deleted, "query_xcfg": num_xcfg_deleted}

    def _judgedQuerySampleIds(self, function_boundary: int) -> Iterable[List[int]]:
        """The distinct sample ids of the query functions old enough to judge, one batch at a
        time. $group over a cursor, because distinct() would answer with a single document."""
        batch: List[int] = []
        for group in self._getDb().query_functions.aggregate(
            [{"$match": {"function_id": {"$gte": function_boundary}}}, {"$group": {"_id": "$sample_id"}}],
            allowDiskUse=True,
        ):
            batch.append(group["_id"])
            if len(batch) >= self._ORPHAN_BATCH_SIZE:
                yield batch
                batch = []
        if batch:
            yield batch

    def _deleteQueryFunctionsWithoutASample(self, function_boundary: int) -> int:
        """The samples are looked up per batch rather than snapshotted up front. A query
        sample is written before its functions, so a sample read after the boundary is at
        worst newer than the function referring to it - reading later can only find more
        samples, never fewer, and a sample deleted meanwhile takes its functions with it."""
        db = self._getDb()
        deleted = 0
        for sample_ids in self._judgedQuerySampleIds(function_boundary):
            known = {document["sample_id"] for document in db.query_samples.find({"sample_id": {"$in": sample_ids}}, {"sample_id": 1, "_id": 0})}
            orphans = [sample_id for sample_id in sample_ids if sample_id not in known]
            if orphans:
                deleted += db.query_functions.delete_many({"sample_id": {"$in": orphans}, "function_id": {"$gte": function_boundary}}).deleted_count
        return deleted

    def _aQueryInsertMayBeInFlight(self) -> bool:
        """Whether some query sample does not have all of its functions yet.

        Such a sample is an insert still running, or one that died between its disassembly and
        its functions. Either way the disassembly of its missing functions is not an orphan
        while the sample exists: the first will still write the functions, and the second's is
        collected in the run that deletes the sample. A disassembly document does not name its
        sample, so while any sample is short, none is judged.

        addSmdaReport records on the query sample how many functions it is going to write. A
        query sample written before that has no count and is taken as possibly in flight, which
        holds the judgment back until the last of them has expired.

        Checked after the boundary is taken: a disassembly old enough to judge had its sample
        written before that, so the sample is seen here, unless it was deleted - and then its
        disassembly is an orphan."""
        db = self._getDb()
        batch: List[Dict[str, Any]] = []

        def batch_is_short(samples: List[Dict[str, Any]]) -> bool:
            if any(not isinstance(sample.get("num_query_functions"), int) for sample in samples):
                return True
            counts = {
                group["_id"]: group["n"]
                for group in db.query_functions.aggregate(
                    [{"$match": {"sample_id": {"$in": [sample["sample_id"] for sample in samples]}}}, {"$group": {"_id": "$sample_id", "n": {"$sum": 1}}}]
                )
            }
            return any(counts.get(sample["sample_id"], 0) < sample["num_query_functions"] for sample in samples)

        for sample in db.query_samples.find({}, {"sample_id": 1, "num_query_functions": 1, "_id": 0}).batch_size(self._ORPHAN_BATCH_SIZE):
            batch.append(sample)
            if len(batch) >= self._ORPHAN_BATCH_SIZE:
                if batch_is_short(batch):
                    return True
                batch = []
        return bool(batch) and batch_is_short(batch)

    def _deleteQueryXcfgWithoutAFunction(self, xcfg_boundary: int) -> int:
        """The boundary is the newest disassembly document, which also covers the case where no
        query function exists at all - an insert interrupted before its first function leaves
        exactly that, and once the queries around it are deleted query_functions is empty."""
        if self._aQueryInsertMayBeInFlight():
            LOGGER.info("A query sample is still short of its functions; the query disassembly is judged in a later cleanup.")
            return 0
        db = self._getDb()
        deleted = 0
        last_id = None
        while True:
            query = {"_id": {"$gte": xcfg_boundary}} if last_id is None else {"_id": {"$gt": last_id}}
            batch = [document["_id"] for document in db.query_xcfg.find(query, {"_id": 1}).sort("_id", 1).limit(self._ORPHAN_BATCH_SIZE)]
            if not batch:
                break
            referenced = set(db.query_functions.distinct("function_id", {"function_id": {"$in": batch}}))
            orphans = [function_id for function_id in batch if function_id not in referenced]
            if orphans:
                deleted += db.query_xcfg.delete_many({"_id": {"$in": orphans}}).deleted_count
            last_id = batch[-1]
        return deleted

    # the collections the query cleanup deletes from, and so the ones compactQueryCollections reclaims
    QUERY_COLLECTIONS = ("query_samples", "query_functions", "query_xcfg", "query_smda_extras")
    # where the job queue keeps the results of the query jobs the cleanup deletes
    QUEUE_GRIDFS_COLLECTIONS = ("fs.files", "fs.chunks")

    def _sharesDatabaseWithQueue(self) -> bool:
        """Whether the job queue's GridFS lives in this database - as it does when both keep the
        default server, port and database name."""
        queue_config = getattr(self._config, "QUEUE_CONFIG", None)
        if queue_config is None or getattr(queue_config, "QUEUE_METHOD", None) != "mongodb":
            return False
        return (str(queue_config.QUEUE_SERVER), str(queue_config.QUEUE_PORT), queue_config.QUEUE_MONGODB_DBNAME) == (
            str(self._storage_config.STORAGE_SERVER),
            str(self._storage_config.STORAGE_PORT),
            self._storage_config.STORAGE_MONGODB_DBNAME,
        )

    def compactQueryCollections(self) -> Dict[str, Any]:
        """Run MongoDB's compact on the collections the query cleanup deletes from.

        The cleanup deletes query jobs too, and with them their results in the queue's GridFS. When
        the queue shares this database, fs.files and fs.chunks are compacted as well; when it keeps
        its own, this handle cannot reach them and leaves them alone.

        compact needs the compact privilege on the database; a refusal is reported per
        collection rather than raised, since the cleanup itself has already succeeded."""
        db = self._getDb()
        outcome: Dict[str, Any] = {}
        collections = self.QUERY_COLLECTIONS + (self.QUEUE_GRIDFS_COLLECTIONS if self._sharesDatabaseWithQueue() else ())
        for collection in collections:
            try:
                result = db.command("compact", collection)
                outcome[collection] = {"ok": result.get("ok"), "bytesFreed": result.get("bytesFreed")}
            except Exception as error:
                LOGGER.warning("compact of %s was refused: %s", collection, error)
                outcome[collection] = {"ok": 0, "error": str(error)}
        return outcome

    # raw submitted binaries live in their own GridFS bucket (#95), one file per distinct content:
    # metadata.sha256 is the key and metadata.sample_ids lists the samples the binary belongs to.
    # Keyed by content rather than by sample, so that anything else holding the same bytes - the
    # file parameters of queue jobs, say - can refer to the same file without a data migration.
    _BINARIES_BUCKET = "sample_binaries"

    def _getBinaries(self) -> "gridfs.GridFS":
        return gridfs.GridFS(self._getDb(), collection=self._BINARIES_BUCKET)

    def _getBinaryFiles(self):
        return self._getDb()[f"{self._BINARIES_BUCKET}.files"]

    def storeSampleBinary(self, sample_id: int, binary: bytes) -> bool:
        if not self.isSampleId(sample_id):
            return False
        binary = bytes(binary)
        sha256 = hashlib.sha256(binary).hexdigest()
        # a sample has one binary: let go of any other content it was linked to before
        for stored in self._getBinaryFiles().find({"metadata.sample_ids": sample_id, "metadata.sha256": {"$ne": sha256}}, {"_id": 1}):
            self._releaseBinaryFile(stored["_id"], sample_id)
        while not self._linkBinaryFile(sha256, sample_id):
            file_id = ObjectId()
            try:
                self._getBinaries().put(binary, _id=file_id, metadata={"sha256": sha256, "sample_ids": [sample_id], "size": len(binary)})
                break
            except FileExists:
                # another submission of the same bytes stored them first (the unique index on
                # metadata.sha256 refused this copy); GridFS leaves the chunks it had already
                # written behind, so they go here, and the loop links to the file that won
                self._getDb()[f"{self._BINARIES_BUCKET}.chunks"].delete_many({"files_id": file_id})
        # a sample deleted meanwhile would otherwise leave its id on the file for good: deleteSample
        # removes the sample before its binaries, so whichever of the two runs second cleans up
        if not self.isSampleId(sample_id):
            self.deleteSampleBinary(sample_id)
            return False
        return True

    def _linkBinaryFile(self, sha256: str, sample_id: int) -> bool:
        """Add the sample to the file already holding these bytes; False when no file holds them."""
        return self._getBinaryFiles().update_one({"metadata.sha256": sha256}, {"$addToSet": {"metadata.sample_ids": sample_id}}).matched_count > 0

    def _releaseBinaryFile(self, file_id, sample_id: int) -> bool:
        """Take the sample off the file, and delete the file once no sample is left on it.

        The file is retired first - its sha256 cleared in the same update that checks no sample is
        left - so a submission of the same bytes arriving in between cannot link to it: it finds no
        file under that hash and stores a fresh one instead of losing its binary to this deletion."""
        self._getBinaryFiles().update_one({"_id": file_id}, {"$pull": {"metadata.sample_ids": sample_id}})
        # only a file still carrying its hash is retired, so of two releasers exactly one deletes it
        retired = self._getBinaryFiles().find_one_and_update(
            {"_id": file_id, "metadata.sample_ids": [], "metadata.sha256": {"$type": "string"}}, {"$set": {"metadata.sha256": None}}
        )
        if retired is None:
            return False
        # through GridFS, which takes the chunks along with the file document
        self._getBinaries().delete(file_id)
        return True

    def getSampleBinary(self, sample_id: int) -> Optional[bytes]:
        stored = self._getBinaries().find_one({"metadata.sample_ids": sample_id})
        return stored.read() if stored is not None else None

    def hasSampleBinary(self, sample_id: int) -> bool:
        """Whether a binary is stored, without fetching a single chunk of it. GridFS keeps the
        file's metadata in `.files` and its bytes in `.chunks`, so this reads one small
        document where getSampleBinary() would stream the whole file to answer the same
        question - which is what the resubmission path in Worker.addBinarySample was doing."""
        return self._getBinaryFiles().find_one({"metadata.sample_ids": sample_id}, {"_id": 1}) is not None

    def openSampleBinary(self, sample_id: int) -> Optional[BinaryStream]:
        """The stored binary as a GridOut, which reads chunk by chunk, so serving it never
        holds the whole file in memory."""
        return self._getBinaries().find_one({"metadata.sample_ids": sample_id})

    def deleteSampleBinary(self, sample_id: int) -> bool:
        """Take the sample off its binary; the binary itself is deleted only when no other sample
        still refers to it. True when the sample had one."""
        had_binary = False
        for stored in self._getBinaryFiles().find({"metadata.sample_ids": sample_id}, {"_id": 1}):
            self._releaseBinaryFile(stored["_id"], sample_id)
            had_binary = True
        return had_binary

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
        report: Dict[str, Any] = {"num_families": 0, "num_families_corrected": 0, "num_families_created": 0, "num_families_merged": 0, "corrections": {}}
        known_family_ids = {document["family_id"] for document in db.families.find({}, {"family_id": 1, "_id": 0})}
        for family_id, row in list(samples_by_family.items()):
            if family_id in known_family_ids:
                continue
            filed_family_id = self._ensureFamilyDocument(family_id, str(row.get("family") or ""))
            if filed_family_id == family_id:
                report["num_families_created"] += 1
                continue
            # its name is another family's, which its samples and functions are filed under now
            report["num_families_merged"] += 1
            del samples_by_family[family_id]
            merged_into = samples_by_family.setdefault(filed_family_id, {"num_samples": 0, "num_library_samples": 0})
            merged_into["num_samples"] += row["num_samples"]
            merged_into["num_library_samples"] += row["num_library_samples"]
            functions_by_family[filed_family_id] = functions_by_family.get(filed_family_id, 0) + functions_by_family.pop(family_id, 0)
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
        # leaving them behind while the counters reset would collide on _id at the next insert;
        # "smda_extras"/"query_smda_extras" are keyed by sample id in the same way (#94)
        # "picblockhashes" is the inverted block-hash index; leaving it behind would keep asserting
        # that hashes are held by samples that no longer exist, and getUniqueBlocks would believe it
        # the "sample_binaries" GridFS bucket holds the raw submissions (#95)
        # the pichash counts and the function ranges go for the same reason as the picblockhash
        # index (#261): _ensureIndexAndUnknownFamily vouches for both on the emptied database, so
        # left behind, the counts would count every re-added function twice and the ranges would
        # map the reset function ids to samples that no longer exist
        collections = [
            "samples",
            "families",
            "functions",
            "matches",
            "candidates",
            "counters",
            "query_samples",
            "query_functions",
            "xcfg",
            "query_xcfg",
            "smda_extras",
            "query_smda_extras",
            "picblockhashes",
            *self._STAGING_COLLECTIONS,
            "sample_binaries.files",
            "sample_binaries.chunks",
            self._PICHASH_COUNT_COLLECTION,
            self._FUNCTION_RANGE_COLLECTION,
            self._OTHER_LABELS_MIGRATION_STATE,
        ]
        for band_id in range(self._storage_config.STORAGE_NUM_BANDS):
            collections.append("band_%d" % band_id)
        for c in collections:
            self._getDb()[c].drop()
        # an emptied corpus is under every cap again; other processes simply wait out the TTL
        self._over_cap_since.clear()
        self._distinct_count_since.clear()
        self._other_labels_unlisted_since = None
        # the settings outlive a clear: other processes learn of it from the version
        self._bumpOtherLabelsVersion()
        self._ensureIndexAndUnknownFamily()

    # sha256 values per $in query: 64 characters each keeps a query well below the 16 MB BSON limit
    _PRESENT_SHA256S_CHUNK = 10000

    def getPresentSha256s(self, sha256s: Iterable[str]) -> Set[str]:
        # only what can be a sha256: anything else matches no sample, and a long one would take the
        # $in query beyond the 16 MB BSON limit
        candidates = list({sha256 for sha256 in sha256s if isSha256(sha256)})
        present = set()
        for start in range(0, len(candidates), self._PRESENT_SHA256S_CHUNK):
            chunk = candidates[start : start + self._PRESENT_SHA256S_CHUNK]
            present.update(document["sha256"] for document in self._getDb().samples.find({"sha256": {"$in": chunk}}, {"_id": 0, "sha256": 1}))
        return present

    def getSampleBySha256(self, sha256: str, is_query=False) -> Optional["SampleEntry"]:
        target_sample = None
        if not isinstance(sha256, str):
            # an object would be read as a query operator
            return None
        if is_query:
            report_dict = self._getDb().query_samples.find_one({"sha256": sha256})
        else:
            report_dict = self._getDb().samples.find_one({"sha256": sha256})
        if report_dict:
            target_sample = SampleEntry.fromDict(report_dict)
        return target_sample

    def updateFunctionLabels(self, smda_report: "SmdaReport", username, checked: bool = False) -> Optional["SampleEntry"]:
        assert smda_report.sha256 is not None
        sample_entry = self.getSampleBySha256(smda_report.sha256)
        if not sample_entry:
            return None
        # check which functions in the SmdaReport have suitable function_names
        submitted_labels = {}
        for smda_function in smda_report.getFunctions():
            label = FunctionLabelEntry.labelFromReport(smda_function.function_name, checked=checked)
            if label is not None and smda_function.offset is not None:
                # use two-complement to convert unit64 to int64 and vice versa
                offset = encode_two_complement(smda_function.offset)
                submitted_labels[offset] = label
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
                update_command = self._pushLabelUpdate(new_function_entry_label)
                # as modifyFunction does: the "not yet labelled so by this user" check is part of the
                # filter, and a function deleted meanwhile is not recreated as a bare label document
                not_yet_labelled = {
                    "function_id": sample_function_entries[label_offset].function_id,
                    "function_labels": {"$not": {"$elemMatch": {"username": username, "function_label": extracted_label}}},
                }
                label_updates.append(UpdateOne(not_yet_labelled, update_command))
        if label_updates:
            self._getDb().functions.bulk_write(label_updates, ordered=False)
            self._bumpOtherLabelsVersion()

    def addSmdaReport(self, smda_report: "SmdaReport", isQuery=False, sha256_verified=False) -> Optional["SampleEntry"]:
        assert smda_report.sha256 is not None
        # the sample is written before its functions are encoded, so this has to come first
        checkSubmittedPicHashes(smda_report)
        sample_entry = None
        if isQuery:
            sample_entry = SampleEntry(smda_report, sample_id=-1 * self._useCounter("query_samples"), family_id=0)
            sample_document = sample_entry.toDict()
            # how many functions this insert is about to write, so that deleteOrphanedQueryData
            # can tell an insert still in flight - whose disassembly precedes its functions -
            # from a finished one (#68)
            sample_document["num_query_functions"] = smda_report.num_functions
            self._dbInsert("query_samples", sample_document)
            self._insertSmdaExtras(sample_entry.sample_id, SampleEntry.smdaExtrasOf(smda_report))
            function_ids = self._useCounterBulk("query_functions", smda_report.num_functions)
            function_dicts = []
            for function_id, smda_function in zip(function_ids, smda_report.getFunctions()):
                function_dicts.append(self._getFunctionDocument(sample_entry, smda_function, -1 * function_id))
            self._insertXcfgDocuments(function_dicts)
            self._dbInsertMany("query_functions", function_dicts)
        else:
            if not self.getSampleBySha256(smda_report.sha256):
                family_id = self.addFamily(smda_report.family or "")
                sample_entry = SampleEntry(smda_report, sample_id=self._useCounter("samples"), family_id=family_id, sha256_verified=sha256_verified)
                # its hashes are computed right here, by the running smda and picblocks
                self._dbInsert("samples", {**sample_entry.toDict(), "pichash_smda_version": SmdaConfig().VERSION, "picblockhash_version": PICBLOCKS_VERSION})
                function_ids = []
                try:
                    # filed: the family must still have its document, which a failed import may have
                    # removed since addFamily (#299); its functions follow the family it is filed under
                    sample_entry.family_id = family_id = self._ensureFamilyDocument(family_id, smda_report.family or "")
                    self._insertSmdaExtras(sample_entry.sample_id, SampleEntry.smdaExtrasOf(smda_report))
                    function_ids = self._useCounterBulk("functions", smda_report.num_functions)
                    function_dicts = []
                    for function_id, smda_function in zip(function_ids, smda_report.getFunctions()):
                        function_dicts.append(self._getFunctionDocument(sample_entry, smda_function, function_id))
                    self._insertXcfgDocuments(function_dicts)
                    self._dbInsertMany("functions", function_dicts)
                    self._bumpOtherLabelsVersionFor(function_dicts)
                except Exception:
                    # a sample whose family, extras or functions could not all be written would stay
                    # without them, and
                    # every resubmission of the report would be answered that it exists already
                    self._discardFunctions(function_ids)
                    self._deleteSmdaExtras(sample_entry.sample_id)
                    self._getDb().samples.delete_one({"sample_id": sample_entry.sample_id})
                    raise
                self._addToPicBlockHashIndex(function_dicts)
                self._addToPicHashCounts(function_dicts)
                self._recordFunctionRange(sample_entry.sample_id, [document["function_id"] for document in function_dicts])
                # the functions stored, as deleteSample counts them: the report's statistics are its own claim
                self._updateFamilyStats(family_id, +1, len(function_dicts), int(sample_entry.is_library))
                # only once its functions are all written: finishing the sample earlier would record
                # it as hashed and drop disassembly that is still to be hashed
                pending = {"smda_version": SmdaConfig().VERSION, "shingler_revision": MINHASH_SHINGLER_REVISION}
                self._getDb().samples.update_one({"sample_id": sample_entry.sample_id}, {"$set": {"minhash_pending": pending}})
                self._updateDbState()
            else:
                LOGGER.warning("Sample %s already existed, skipping.", smda_report.sha256)
        return sample_entry

    def importSampleEntry(self, sample_entry: "SampleEntry") -> Optional["SampleEntry"]:
        if not self.getSampleBySha256(sample_entry.sha256):
            sample_id = self._useCounter("samples")
            sample_entry.sample_id = sample_id
            # whatever the export says: this instance has never seen the sample's bytes (#270)
            sample_entry.sha256_verified = False
            self._dbInsert("samples", sample_entry.toDict())
            # the caller learns of the sample only once this returns, so one that fails after the
            # insert - a family counter that overflows, say - is removed here, or it would stay. Its
            # functions are counted as they are imported, not by the statistics the export states
            is_counted = False
            try:
                sample_entry.family_id = self._ensureFamilyDocument(sample_entry.family_id, sample_entry.family)
                self._updateFamilyStats(sample_entry.family_id, +1, 0, int(sample_entry.is_library))
                is_counted = True
                self._updateDbState()
            except Exception:
                try:
                    if is_counted:
                        self.discardImportedSample(sample_entry)
                    else:
                        self._getDb().samples.delete_one({"sample_id": sample_id, "sha256": sample_entry.sha256})
                except Exception:
                    LOGGER.exception("Could not remove sample %s, whose import failed.", sample_entry.sha256)
                raise
            return sample_entry
        # not this entry's sample: the entry still carries the id it had in the export, which here may
        # well be another sample's, so the caller must not attach the entry's functions to its sample_id
        LOGGER.warning("Sample %s already existed, skipping.", sample_entry.sha256)
        return None

    def importFunctionEntry(self, function_entry: "FunctionEntry") -> Optional["FunctionEntry"]:
        function_entry.function_id = self._useCounter("functions")
        if function_entry.function_name and len(function_entry.function_labels) == 0:
            new_function_entry_label = FunctionLabelEntry(function_entry.function_name, "mcrit-import")
            function_entry.function_labels.append(new_function_entry_label)
        # add function to regular storage
        function_dict = function_entry.toDict()
        self._encodeFunction(function_dict)
        self._insertAndCountFunctions([function_dict], lambda: self._dbInsert("functions", function_dict))
        self._bumpOtherLabelsVersionFor([function_dict])
        self._addToPicBlockHashIndex([function_dict])
        self._recordFunctionRange(function_entry.sample_id, [function_entry.function_id])
        self._updateDbState()
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
        self._insertAndCountFunctions(functions_as_dicts, lambda: self._dbInsertMany("functions", functions_as_dicts))
        self._bumpOtherLabelsVersionFor(functions_as_dicts)
        self._addToPicBlockHashIndex(functions_as_dicts)
        # as addSmdaReport does: without a range, a shortlisted match never attributes these
        # functions to their sample, and their minhash matches are dropped
        function_ids_by_sample: Dict[int, List[int]] = {}
        for function_dict in functions_as_dicts:
            function_ids_by_sample.setdefault(function_dict["sample_id"], []).append(function_dict["function_id"])
        for sample_id, sample_function_ids in function_ids_by_sample.items():
            self._recordFunctionRange(sample_id, sample_function_ids)
        # the range lookup is cached per db state: one built while these were being written must not outlive them
        self._updateDbState()
        return function_entries

    def _insertAndCountFunctions(self, function_dicts: List[Dict], insert_functions: Callable[[], Any]) -> None:
        """Store imported functions with insert_functions, then count them: their pichashes, and their
        families' function counters.

        discardImportedSample takes both counts of the functions it finds stored out again, so every
        function it can find has to be counted in both by then. An insert that fails removes what it
        wrote; should that fail as well, or the pichash counting itself, functions may be stored
        without their pichashes being counted, and the count index is marked incomplete instead: the
        discard then leaves the counts alone, and rebuildPicHashCountIndex counts what is stored.
        Functions whose pichashes could not be counted are removed again as well, since no family has
        counted them yet, and the discard would take them off their family all the same. A family
        counter that fails - one that would overflow, say - takes back the functions with their
        pichash counts, so that the discard finds none of them, and once they are gone the families
        counted before it. Should removing the functions fail, those families stay raised for the
        discard to take off what it finds, and the count index is marked incomplete.
        """
        function_ids = [function_dict["function_id"] for function_dict in function_dicts]
        may_be_uncounted = True
        is_stored = False
        try:
            try:
                self._insertXcfgDocuments(function_dicts)
                insert_functions()
            except Exception:
                # none of them, rather than the ones before the document that failed
                self._discardFunctions(function_ids)
                may_be_uncounted = False
                raise
            is_stored = True
            self._addToPicHashCounts(function_dicts)
            may_be_uncounted = False
        except Exception:
            if may_be_uncounted:
                self._markPicHashCountsUntrusted()
            if is_stored:
                # their families are not counted yet, so the discard must not find them
                try:
                    self._discardFunctions(function_ids)
                except Exception:
                    LOGGER.exception("Could not remove the functions of a failed import, their families may be counted short until recomputed.")
            raise
        counted_families = []
        try:
            for family_id, num_functions in Counter(function_dict["family_id"] for function_dict in function_dicts).items():
                self._updateFamilyStats(family_id, 0, num_functions, 0)
                counted_families.append((family_id, num_functions))
        except Exception:
            # the functions go first: only once none of them is left for the discard to find may the
            # families counted so far be taken back, or the discard would take them off a second time
            try:
                self._uncountPicHashes(function_dicts)
                self._discardFunctions(function_ids)
            except Exception:
                # stored, or counted, without the other: only a rebuild can tell. The families counted
                # so far stay raised, for the discard to take off what it finds of theirs
                LOGGER.exception("Could not remove the functions of a failed import, their pichash counts are left to a rebuild.")
                self._markPicHashCountsUntrusted()
            else:
                for family_id, num_functions in counted_families:
                    try:
                        self._updateFamilyStats(family_id, 0, -num_functions, 0)
                    except Exception:
                        LOGGER.exception("Could not take the functions of a failed import off family %d.", family_id)
            raise

    def _markPicHashCountsUntrusted(self) -> None:
        try:
            self._setPicHashCountIndexComplete(False)
        except Exception:
            LOGGER.exception("Could not mark the pichash counts incomplete, the imported functions may be stored uncounted.")

    def _discardFunctions(self, function_ids: Iterable[int]) -> None:
        """Remove what a failed insert of these functions may have written before it failed."""
        function_ids = list(function_ids)
        self._deleteXcfgForFunctionIds(function_ids)
        for offset in range(0, len(function_ids), 10000):
            self._getDb().functions.delete_many({"function_id": {"$in": function_ids[offset : offset + 10000]}})
        self._bumpOtherLabelsVersion()

    def discardImportedSample(self, sample_entry: "SampleEntry") -> None:
        sample_id = sample_entry.sample_id
        # only the sample this entry was imported as: an entry importSampleEntry skipped still
        # carries the id it had elsewhere, which here may well be another sample's
        if self._getDb().samples.find_one({"sample_id": sample_id, "sha256": sample_entry.sha256}, {"_id": 1}) is None:
            return
        # the indexes are read from the functions, so before they go, as in deleteSample: the band
        # entries - pulling those a failed addMinHashes never pushed changes nothing - the
        # picblockhash index, and the pichash counts, taken out at most once. The import counts its
        # functions right after the insert, and marks the count index incomplete where they may not
        # be (_insertAndCountFunctions), so every function found here was counted or is not taken
        # out. An import writes sample by sample, so one that fails later takes back samples whose
        # functions were stored and counted
        function_minhashes = self._getFunctionMinHashesBySampleId(sample_id)
        self._pullBandEntries(function_minhashes)
        self._removeSampleFromPicBlockHashIndex(sample_id)
        claim = self._getDb().samples.update_one(
            {"sample_id": sample_id, "sha256": sample_entry.sha256, "pichash_counts_removed": {"$ne": True}}, {"$set": {"pichash_counts_removed": True}}
        )
        if claim.modified_count:
            self._removeSampleFromPicHashCounts(sample_id)
        # and its function ranges: left behind, they would attribute the function ids to a sample
        # that is gone
        self._getDb()[self._FUNCTION_RANGE_COLLECTION].delete_many({"sample_id": sample_id})
        function_ids = [function_minhash["function_id"] for function_minhash in function_minhashes]
        self._discardFunctions(function_ids)
        num_samples_deleted = self._getDb().samples.delete_one({"sample_id": sample_id, "sha256": sample_entry.sha256}).deleted_count
        if num_samples_deleted or function_ids:
            # the mirror of the increments of importSampleEntry and of the functions as they were stored
            # (not of its statistics); the family stays, since it may have been there before the import,
            # with actors of its own
            self._updateFamilyStats(sample_entry.family_id, -num_samples_deleted, -len(function_ids), -num_samples_deleted * int(sample_entry.is_library))
        self._updateDbState()

    def discardImportedFamily(self, family_id: int) -> bool:
        if family_id == 0 or self._getDb().samples.find_one({"family_id": family_id}, {"_id": 1}) is not None:
            return False
        family_document = self._getDb().families.find_one_and_delete({"family_id": family_id, "num_samples": {"$lte": 0}})
        if family_document is None:
            return False
        # Another writer may have filed a sample under the family since the lookup above. One that
        # did so before the lookup below is found by it, and one after it has filed by the time it
        # ensures the family document itself, as every writer does: either way the document is created
        # again. Its counters may then miss that writer's sample until recomputeFamilyStats runs.
        if self._getDb().samples.find_one({"family_id": family_id}, {"_id": 1}) is not None:
            self._ensureFamilyDocument(family_id, family_document["family_name"])
            return False
        self._updateDbState()
        return True

    def getFunctionsBySampleId(self, sample_id: int, with_xcfg: bool = True) -> Optional[List["FunctionEntry"]]:
        if not self.isSampleId(sample_id):
            return None
        # inline `_xcfg` of pre-migration documents is left out along with the separate blobs
        projection = {"_id": 0} if with_xcfg else {"_id": 0, "_xcfg": 0}
        if sample_id < 0:
            function_dicts = list(self._getDb().query_functions.find({"sample_id": sample_id}, projection))
        else:
            function_dicts = list(self._getDb().functions.find({"sample_id": sample_id}, projection))
        return self._toFunctionEntries(function_dicts, with_xcfg)

    def getFunctionsBySampleIdInBatches(self, sample_id: int, batch_size: int = 1000) -> Iterator[List["FunctionEntry"]]:
        if not self.isSampleId(sample_id):
            return
        collection = self._getDb().query_functions if sample_id < 0 else self._getDb().functions
        function_dicts = []
        for function_dict in collection.find({"sample_id": sample_id}, {"_id": 0}, batch_size=batch_size):
            function_dicts.append(function_dict)
            if len(function_dicts) >= batch_size:
                yield self._toFunctionEntries(function_dicts)
                function_dicts = []
        if function_dicts:
            yield self._toFunctionEntries(function_dicts)

    def _toFunctionEntries(self, function_dicts: List[Dict], with_xcfg: bool = True) -> List["FunctionEntry"]:
        functions = []
        if with_xcfg:
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
        decoded_pichash = query_result["pichash"]
        # 0 is what a function without a PicHash is stored with (FunctionEntry), not a hash it shares
        if not decoded_pichash:
            return None

        sample_and_function_ids = set(
            map(
                lambda x: (x["family_id"], x["sample_id"], x["function_id"]),
                list(self._getDb().functions.find(self._pichashLookupCondition("_pichash", decoded_pichash), {"family_id": 1, "sample_id": 1, "function_id": 1, "_id": 0})),
            )
        )

        return {decoded_pichash: sample_and_function_ids}

    def getPicHashMatchesByFunctionIds(self, function_ids: List[int]) -> Dict[int, Set[Tuple[int, int, int]]]:
        # internal format!
        pichash_raw = self._getDb().functions.find({"function_id": {"$in": function_ids}}, {"_pichash": 1, "_id": 0})
        pichashes_list = list(pichash_raw)
        pichashes = {}
        encoded_to_decoded = {}
        for pichash in pichashes_list:
            # a function document without one has no pichash to match, as in the single-id lookup
            if "_pichash" not in pichash:
                continue
            self._decodePichash(pichash, delete_old=False)
            encoded_pichash = pichash["_pichash"]
            decoded_pichash = pichash["pichash"]
            # 0 is what a function without a PicHash is stored with: matching it would pair every
            # such function with every other one as an exact match, as MemoryStorage never does
            if not decoded_pichash or decoded_pichash in pichashes:
                continue
            pichashes[decoded_pichash] = set()
            encoded_to_decoded[encoded_pichash] = decoded_pichash
        # one $in over all distinct pichashes instead of one query per pichash (N+1, #111);
        # grouping client-side by the returned _pichash reproduces the per-query sets exactly
        if encoded_to_decoded:
            # while the instance is not padded, either spelling of a value may be stored (#145)
            lookup = set(encoded_to_decoded)
            if not self.isPichashPadded():
                for decoded_pichash in list(encoded_to_decoded.values()):
                    for variant in pichash_value_variants(decoded_pichash):
                        encoded_to_decoded[variant] = decoded_pichash
                        lookup.add(variant)
            wanted = self._filterPicHashesByMatchCount(sorted(lookup))
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
            counts = {
                document["_pichash"]: document["df"]
                for document in self._getDb()[self._PICHASH_COUNT_COLLECTION].find({"_pichash": {"$in": encoded_pichashes}}, {"_pichash": 1, "df": 1, "_id": 0})
            }
        else:
            # no counts stored yet: fall back to counting, which is correct but pays the very
            # cost the cutoff is meant to avoid
            LOGGER.warning("PicHash count index incomplete, counting holders instead; run MongoDbStorage.rebuildPicHashCountIndex() to restore the fast path.")
            pipeline = [
                {"$match": {"_pichash": {"$in": encoded_pichashes}}},
                {"$group": {"_id": "$_pichash", "num_holders": {"$sum": 1}}},
            ]
            counts = {group["_id"]: group["num_holders"] for group in self._getDb().functions.aggregate(pipeline, allowDiskUse=True)}
        # Until migrate_pichash_padding has run, one value may be stored in both spellings
        # ("0x4d2" and its zero-padded form), each counted separately (#145). Sum over the
        # spellings of a value and keep or drop them together, so that a value over the cutoff
        # cannot pass by being split. A spelling nobody holds counts 0.
        totals: Dict[int, int] = {}
        for encoded_pichash in set(encoded_pichashes):
            value = int(encoded_pichash, 16)
            totals[value] = totals.get(value, 0) + counts.get(encoded_pichash, 0)
        kept = [encoded_pichash for encoded_pichash in encoded_pichashes if totals[int(encoded_pichash, 16)] <= cutoff]
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

    def _minHashUpdate(self, minhash: "MinHash", keep_composition: bool) -> Dict[str, Any]:
        """the fields addMinHash(es) sets on the function's document"""
        update: Dict[str, Any] = {"minhash": self._minHashForStorage(minhash.getMinHash())}
        if not keep_composition:
            update["minhash_shingle_composition"] = minhash.getComposition()
        return {"$set": update}

    def addMinHash(self, minhash: "MinHash", keep_composition: bool = False) -> bool:
        if minhash.function_id is None:
            return False
        is_query_function = minhash.function_id < 0
        collection = self._getDb().query_functions if is_query_function else self._getDb().functions
        if collection.find_one_and_update({"function_id": minhash.function_id}, self._minHashUpdate(minhash, keep_composition)) is None:
            LOGGER.warning("addMinHash() - no function with id %d, skipping its minhash.", minhash.function_id)
            return False
        # query functions are not indexed into bands, they are only ever used as query input
        if not is_query_function:
            self._addMinHashToBands(minhash)
        return True

    def addMinHashes(self, minhashes: List["MinHash"], keep_composition: bool = False) -> None:
        if not minhashes:
            return
        for is_query_function in (False, True):
            relevant_minhashes = [minhash for minhash in minhashes if minhash.function_id is not None and (minhash.function_id < 0) == is_query_function]
            if not relevant_minhashes:
                continue
            collection = self._getDb().query_functions if is_query_function else self._getDb().functions
            function_ids = [minhash.function_id for minhash in relevant_minhashes]
            # looked up a slice at a time: the ids of an import of a million functions, as query
            # and as answer, would each exceed MongoDB's 16 MB document limit
            existing_ids = set()
            for offset in range(0, len(function_ids), self._MINHASH_UPDATE_BATCH):
                existing_ids.update(collection.distinct("function_id", {"function_id": {"$in": function_ids[offset : offset + self._MINHASH_UPDATE_BATCH]}}))
            stored_minhashes = []
            for minhash in relevant_minhashes:
                if minhash.function_id not in existing_ids:
                    LOGGER.warning("addMinHashes() - no function with id %d, skipping its minhash.", minhash.function_id)
                    continue
                stored_minhashes.append(minhash)
            # the updates built and written a slice at a time: those of an import's every function
            # held about 0.6 kB each at once
            for offset in range(0, len(stored_minhashes), self._MINHASH_UPDATE_BATCH):
                function_updates = [
                    UpdateOne({"function_id": minhash.function_id}, self._minHashUpdate(minhash, keep_composition))
                    for minhash in stored_minhashes[offset : offset + self._MINHASH_UPDATE_BATCH]
                ]
                collection.bulk_write(function_updates, ordered=False)
            if stored_minhashes and not is_query_function:
                self._addMinHashesToBands(stored_minhashes)

    # original implementation, already working
    def _addMinHashToBands(self, minhash: "MinHash") -> None:
        # reuse multi-insert function
        self._addMinHashesToBands([minhash])

    def _addMinHashesToBands(self, minhashes: List["MinHash"]) -> None:
        hashed = [minhash for minhash in minhashes if minhash.hasMinHash()]
        if not hashed:
            return
        # every band hash of every minhash in one matrix, then the posting lists of one band at a time:
        # those of all bands at once held about 1.3 kB per function, which made banding an import of
        # 210,000 functions with distinct minhashes peak at 1.8 GB. The updates are the same either way
        band_numbers = list(self.getBandHashesForMinHash(hashed[0]))
        band_hash_matrix = np.empty((len(hashed), len(band_numbers)), dtype=np.int64)
        for row, minhash in enumerate(hashed):
            band_hash_matrix[row] = list(self.getBandHashesForMinHash(minhash).values())
        function_ids = [minhash.function_id for minhash in hashed]
        for column, band_number in enumerate(band_numbers):
            band_data = {}
            for band_hash, function_id in zip(band_hash_matrix[:, column].tolist(), function_ids):
                band_data.setdefault(band_hash, []).append(function_id)
            # and their updates a slice of band hashes at a time; each hash is written whole, by one update
            band_items = list(band_data.items())
            del band_data
            for offset in range(0, len(band_items), self._MINHASH_UPDATE_BATCH):
                self._updateBands({band_number: dict(band_items[offset : offset + self._MINHASH_UPDATE_BATCH])})

    # a band document whose posting list is empty (or missing) holds nothing a candidate
    # lookup could find; it is residue, not index (#149)
    _EMPTY_BAND_DOCUMENT = {"$or": [{"function_ids": {"$size": 0}}, {"function_ids": {"$exists": False}}]}

    def _bandBucketSize(self) -> int:
        """How many postings one band document may hold, or 0 for the single-document shape."""
        return max(0, int(getattr(self._storage_config, "STORAGE_BAND_BUCKET_SIZE", 0) or 0))

    # what mongod answers a $push that would take a document past 16 MiB: 17419 ("Resulting document
    # after update is larger than 16777216"), 10334 (BSONObjectTooLarge) on some versions
    _DOCUMENT_FULL_CODES = (10334, 17419)

    @classmethod
    def _isFullBandDocumentError(cls, error: BulkWriteError) -> bool:
        """Whether every write the bulk refused was refused for growing a document past the cap."""
        write_errors = error.details.get("writeErrors", [])
        return bool(write_errors) and not error.details.get("writeConcernErrors") and all(write_error.get("code") in cls._DOCUMENT_FULL_CODES for write_error in write_errors)

    def _dropPostingsForFullBandDocuments(self, band_number: int, error: BulkWriteError) -> None:
        """Leave out the postings of band hashes whose single document is full, and say so.

        Under STORAGE_BAND_BUCKET_SIZE = 0 a band hash is one document, which takes no further posting
        once it nears MongoDB's 16 MiB cap - about 1.35 million function ids. Copies of one function
        share all their band hashes, so submitting a function over and over fills its documents. The
        bulk is unordered, so every other posting of the batch is in and only the refused ones (and
        their df) are missing. Raising here instead failed the sample at the first full band and
        skipped every band after it, for all of its functions, and the job's retry - finding the
        minhashes already stored - then stamped the sample as indexed. STORAGE_BAND_BUCKET_SIZE lifts
        the cap.
        """
        full_hashes = [write_error.get("op", {}).get("q", {}).get("band_hash") for write_error in error.details["writeErrors"]]
        message = "Band %d: %d band hash(es) at the 16 MiB document cap take no further postings; set STORAGE_BAND_BUCKET_SIZE to split them." % (band_number, len(full_hashes))
        self._dbLogError(message, details={"band_number": band_number, "band_hashes": full_hashes[:100]})
        LOGGER.warning("%s Hashes: %s", message, full_hashes[:10])

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
                try:
                    collection.bulk_write(band_updates, ordered=False)
                except BulkWriteError as error:
                    if method != "push" or not self._isFullBandDocumentError(error):
                        raise
                    self._dropPostingsForFullBandDocuments(band_number, error)
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
            UpdateOne(
                {"band_hash": band_hash, "bucket": 0},
                {"$set": {"df": df, "tail": tail, "tail_n": tail_n}, "$setOnInsert": {"function_ids": []}},
                upsert=df > 0,
            )
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

    def _collectBandHashTargets(self, function_id_to_minhash: Dict[int, "MinHash"], skips: Optional[BandDfSkips] = None):
        """band_number -> set(band_hash) to query, and band_number -> band_hash -> query function_ids."""
        target_band_hashes_per_band = {band_number: set() for band_number in range(self._storage_config.STORAGE_NUM_BANDS)}
        band_hash_to_function_ids = {band_number: {} for band_number in range(self._storage_config.STORAGE_NUM_BANDS)}
        for function_id, minhash in function_id_to_minhash.items():
            if not minhash.hasMinHash():
                continue
            band_hashes = self.getBandHashesForMinHash(minhash)
            if skips is not None:
                skips.addLookups(function_id, len(band_hashes))
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

    def _bandLookupPipeline(
        self, band_hashes: List[int], band_df_cutoff: Optional[int] = None, df_index_complete: Optional[bool] = None, skipped_from: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Aggregation returning the wanted band documents, dropping over-long posting lists.

        The cutoff is applied server-side rather than after the fetch on purpose: the cost of a
        stopword band hash is dominated by shipping and BSON-decoding a posting list with
        millions of entries, so a client-side check would pay almost the whole price before
        discarding it. `df_index_complete` passes in the flag a caller already read, so a lookup
        over all bands reads the settings document once rather than once per band.

        With `skipped_from` (the band collection's name) and the df index trusted, the hashes the
        cutoff skips come back as well, as {band_hash, df} without a posting list (#201): a
        $unionWith in the same command, answered from the (band_hash, df) index entries alone, so
        no skipped band document is read and no extra round trip is made.
        """
        cutoff = self._bandDfCutoff(band_df_cutoff)
        if cutoff <= 0:
            return [{"$match": {"band_hash": {"$in": band_hashes}}}]
        if df_index_complete is None:
            df_index_complete = self.isBandDfIndexComplete()
        if df_index_complete:
            # the whole point of storing df: with a (band_hash, df) index mongod decides from
            # the index entry alone, so an over-long posting list is never read. Filtering on
            # $size instead still reads every document to measure it, which measured no faster
            # than not filtering at all.
            # no bucket predicate here: with one, mongod prefers the (band_hash, bucket) index and
            # reads every over-cutoff bucket 0 before discarding it. _bandLookupHits drops the stray
            # upper bucket a df could admit instead.
            pipeline: List[Dict[str, Any]] = [{"$match": {"band_hash": {"$in": band_hashes}, "df": {"$lte": cutoff}}}]
            if skipped_from is not None:
                # band_hash and df only, so the scan is covered: an over-cutoff posting list stays unread
                skipped = [{"$match": {"band_hash": {"$in": band_hashes}, "df": {"$gt": cutoff}}}, {"$project": {"_id": 0, "band_hash": 1, "df": 1}}]
                pipeline.append({"$unionWith": {"coll": skipped_from, "pipeline": skipped}})
            return pipeline
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

    def _bandLookupPlan(self, band_df_cutoff: Optional[int] = None) -> Tuple[int, bool, bool]:
        """(cutoff, df index complete, lookup reads bucket 0 alone), decided once per lookup call.

        The settings document is read once here rather than once per band. The lookup matches on
        df, which only bucket 0 of a bucketed hash carries, when a cutoff is set and the df index
        is trusted. The cutoff is the job's effective one, not the configured one: deciding from
        the configured cutoff would fetch the upper buckets of a hash the job's pipeline already
        returned whole, or skip them for one it matched on df alone.
        """
        cutoff = self._bandDfCutoff(band_df_cutoff)
        df_index_complete = cutoff > 0 and self.isBandDfIndexComplete()
        return cutoff, df_index_complete, bool(self._bandBucketSize()) and df_index_complete

    def _bandLookupHits(self, band_number: int, band_hashes: List[int], plan: Tuple[int, bool, bool], on_skipped=None):
        """The band documents a candidate lookup reads: every posting of every hash the cutoff admits.

        `on_skipped(band_hash, df)`, when given, is called once for each wanted hash the cutoff
        skipped (#201); that needs the df index, without which the lookup cannot tell a skipped
        hash from one the corpus does not hold, and nothing is reported.

        The df-indexed pipeline matches bucket 0 alone. For a hash that never spilled that is all
        of it, and a hash that spilled has a df the cutoff rejects - unless pulls shrank it back
        under the cutoff while its surviving postings sit in buckets above 0. Its bucket 0 then
        names a tail above 0, and those buckets are fetched as well; otherwise the lookup would
        quietly treat the hash as one without candidates. Only deletions produce such a hash, so
        the extra, indexed query is normally never made.
        """
        cutoff, df_index_complete, bucket_zero_only = plan
        collection = self._getDb()["band_%d" % band_number]
        shrunk = []
        count_skipped = on_skipped is not None and cutoff > 0 and df_index_complete
        skipped_from = collection.name if count_skipped else None
        skipped = set()
        for hit in collection.aggregate(self._bandLookupPipeline(band_hashes, cutoff, df_index_complete, skipped_from=skipped_from)):
            # only the $unionWith returns a df over the cutoff, and never with a posting list
            if count_skipped and (hit.get("df") or 0) > cutoff and "function_ids" not in hit:
                # once per hash, should a stray df on an upper bucket match as well
                if hit["band_hash"] not in skipped:
                    skipped.add(hit["band_hash"])
                    on_skipped(hit["band_hash"], hit["df"])
                continue
            if bucket_zero_only and (hit.get("bucket") or 0) != 0:
                # df belongs on bucket 0 alone; one found on a bucket above it - only switching
                # bucketing back off, which is unsupported, stamps one - must not admit that bucket
                continue
            yield hit
            if bucket_zero_only and (hit.get("tail") or 0) > 0:
                shrunk.append(hit["band_hash"])
        if shrunk:
            yield from collection.find({"band_hash": {"$in": shrunk}, "bucket": {"$gt": 0}}, {"_id": 0, "band_hash": 1, "function_ids": 1})

    def _skipCounter(self, skips: Optional[BandDfSkips], plan: Tuple[int, bool, bool], band_hash_to_function_ids: Dict[int, Set[int]]):
        """The on_skipped callback for one band's lookup, counting into `skips`; None when not counting."""
        if skips is None:
            return None
        if not getattr(self._storage_config, "STORAGE_BAND_DF_COUNT_SKIPS", True):
            skips.markUnmeasured("disabled")
            return None
        cutoff, df_index_complete, _ = plan
        if cutoff > 0 and not df_index_complete:
            skips.markUnmeasured("band_df_index_incomplete")
            return None
        return lambda band_hash, df: skips.addSkipped(band_hash_to_function_ids[band_hash], df)

    def _getCandidatesForMinHashesNumpy(self, function_id_to_minhash: Dict[int, "MinHash"], band_matches_required=1, as_arrays=False, band_df_cutoff=None, skips=None):
        """Variant C: accumulate band hits as int64 arrays instead of dict[qid][cid] -> count.

        Semantically identical to the dict version (np.unique(..., return_counts=True) counts
        repeated ids exactly like the += 1 loop did), but it never builds the per-pair Python
        dict, and it stores one array per posting list rather than one entry per pair - the
        posting list is *shared* by reference between all query functions that hit that band
        hash, so the inner O(pairs) Python loop disappears as well.
        """
        target_band_hashes_per_band, band_hash_to_function_ids = self._collectBandHashTargets(function_id_to_minhash, skips)
        hit_chunks = {}
        plan = self._bandLookupPlan(band_df_cutoff)
        for band_number, band_hashes in target_band_hashes_per_band.items():
            on_skipped = self._skipCounter(skips, plan, band_hash_to_function_ids[band_number])
            for hit in self._bandLookupHits(band_number, list(band_hashes), plan, on_skipped):
                reference_function_ids = band_hash_to_function_ids[band_number][hit["band_hash"]]
                # int64, not int32: function ids come from a counter that never reuses an id, so they
                # pass 2**31 - 1 within a few million samples. numpy 2 then raises OverflowError, and
                # numpy 1.x silently wraps the id onto a different function.
                posting_list = np.array(hit.get("function_ids") or [], dtype=np.int64)
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

    def getCandidateArraysForMinHashes(
        self, function_id_to_minhash: Dict[int, "MinHash"], band_matches_required=1, band_df_cutoff=None, skips: Optional[BandDfSkips] = None
    ) -> Dict[int, "np.ndarray"]:
        """Candidates as sorted int64 arrays rather than sets.

        The accumulation already produces arrays and then boxes them into Python sets; a caller
        that wants to keep working in numpy (the shortlist stage does) would immediately unbox
        them again. Measured on a 361k-pair query against 10k samples, that round trip alone was
        most of a second.
        """
        return self._getCandidatesForMinHashesNumpy(function_id_to_minhash, band_matches_required=band_matches_required, as_arrays=True, band_df_cutoff=band_df_cutoff, skips=skips)

    def getCandidatesForMinHashes(
        self, function_id_to_minhash: Dict[int, "MinHash"], band_matches_required=1, band_df_cutoff=None, skips: Optional[BandDfSkips] = None
    ) -> Dict[int, Set[int]]:
        if getattr(self._storage_config, "STORAGE_CANDIDATE_ACCUMULATION", "dict") == "numpy":
            return self._getCandidatesForMinHashesNumpy(function_id_to_minhash, band_matches_required=band_matches_required, band_df_cutoff=band_df_cutoff, skips=skips)
        candidates = {}
        target_band_hashes_per_band, band_hash_to_function_ids = self._collectBandHashTargets(function_id_to_minhash, skips)
        plan = self._bandLookupPlan(band_df_cutoff)
        for band_number, band_hashes in target_band_hashes_per_band.items():
            on_skipped = self._skipCounter(skips, plan, band_hash_to_function_ids[band_number])
            for hit in self._bandLookupHits(band_number, list(band_hashes), plan, on_skipped):
                reference_function_ids = band_hash_to_function_ids[band_number][hit["band_hash"]]
                for function_id in reference_function_ids:
                    if function_id not in candidates:
                        candidates[function_id] = {}
                    for hit_function_id in hit.get("function_ids") or []:
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

    def getCandidatesForMinHash(self, minhash: "MinHash", band_matches_required=1, band_df_cutoff=None, skips: Optional[BandDfSkips] = None) -> Set[int]:
        if not minhash.hasMinHash():
            return set()
        candidates = {}
        band_hashes = self.getBandHashesForMinHash(minhash)
        if skips is not None:
            skips.addLookups(0, len(band_hashes))
        plan = self._bandLookupPlan(band_df_cutoff)
        for band_number, band_hash in sorted(band_hashes.items()):
            on_skipped = self._skipCounter(skips, plan, {band_hash: {0}})
            # every document, not the first: under bucketing a hash's postings span several
            for band_document in self._bandLookupHits(band_number, [band_hash], plan, on_skipped):
                # a bucket 0 recreated to hold a hash's bookkeeping can carry no posting list
                for function_id in band_document.get("function_ids") or []:
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

    # _bandLookupPipeline filters on df, so this backend's candidate lookup does skip what the
    # coverage report counts
    APPLIES_BAND_DF_CUTOFF = True

    def _bandDfIndexName(self, band_number: int) -> Optional[str]:
        """The name of the (band_hash, df) index on one band collection, or None if it has none."""
        for name, info in self._getDb()["band_%d" % band_number].index_information().items():
            if [tuple(key) for key in info.get("key", [])] == [("band_hash", 1), ("df", 1)]:
                return name
        return None

    def _bandDfUnavailableReason(self) -> Optional[str]:
        # Refused rather than measured by $size: without a trusted df the only way to count is to
        # read every band document in full - on a 7,244-sample corpus that is 111.8M postings - which
        # is the very read the (band_hash, df) index exists to avoid, and the rebuild that fixes it
        # is the one the cutoff needs anyway to skip from the index.
        reason = super()._bandDfUnavailableReason()
        if reason is not None:
            return reason
        missing = [band_number for band_number in range(self._storage_config.STORAGE_NUM_BANDS) if self._bandDfIndexName(band_number) is None]
        if missing:
            return (
                f"Band collections {missing} have no (band_hash, df) index, so what STORAGE_BAND_DF_CUTOFF skips cannot be "
                "counted from the index alone. Run the band df rebuild (GET /rebuild_band_df_index), which creates it, "
                "then request the coverage again."
            )
        return None

    @staticmethod
    def _bandDfCountPipeline(thresholds: List[int]) -> List[Dict[str, Any]]:
        """The aggregation _countBandDf runs per band: one $group over df and nothing else.

        Only bucket 0 of a hash carries df (the total across its buckets; the buckets above it carry
        none), so a document with df > 0 is exactly one band hash and its df is that hash's whole
        posting-list length. Counting those documents and summing their df therefore needs no
        grouping by band_hash: one running total per band, whose memory does not grow with the
        number of hashes and so never needs allowDiskUse. A document without df (a bucket above 0)
        or with df 0 (an empty one) adds nothing to any of the counts.
        """
        with_df = {"$gt": ["$df", 0]}
        over_accumulators: Dict[str, Any] = {}
        for threshold in thresholds:
            over = {"$gt": ["$df", threshold]}
            over_accumulators["hashes_over_%d" % threshold] = {"$sum": {"$cond": [over, 1, 0]}}
            over_accumulators["postings_over_%d" % threshold] = {"$sum": {"$cond": [over, "$df", 0]}}
        return [
            {
                "$group": {
                    "_id": None,
                    "band_hashes": {"$sum": {"$cond": [with_df, 1, 0]}},
                    "postings": {"$sum": {"$cond": [with_df, "$df", 0]}},
                    "max_df": {"$max": "$df"},
                    **over_accumulators,
                }
            },
        ]

    def _countBandDf(self, band_number: int, thresholds: List[int]) -> Dict[str, Any]:
        """Count one band's posting lists from the (band_hash, df) index alone.

        The pipeline only reads df, which the hinted index carries, so the plan is a covered index
        scan: no band document - and no posting list - is fetched. On a 7,244-sample corpus
        (MongoDB 7.0) a single $group of this shape took 39.3 s for all 20 bands, 1.1 to 2 s per
        band.

        Postings in buckets above 0 of a hash whose bucket 0 is missing carry no df and are not
        counted; with the cutoff on they are never served either. rebuild_band_df_index recreates
        the missing bucket 0 from the buckets that remain.
        """
        collection = self._getDb()["band_%d" % band_number]
        rows = list(collection.aggregate(self._bandDfCountPipeline(thresholds), hint=self._bandDfIndexName(band_number)))
        row = rows[0] if rows else {}
        return {
            "band_hashes": int(row.get("band_hashes") or 0),
            "postings": int(row.get("postings") or 0),
            "max_df": int(row.get("max_df") or 0),
            "over": {threshold: [int(row.get("hashes_over_%d" % threshold) or 0), int(row.get("postings_over_%d" % threshold) or 0)] for threshold in thresholds},
        }

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
        before the next write. It also recreates a hash's bucket 0 where that is missing while
        buckets above it survive, since bucket 0 holds the hash's only df.

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
            # upsert while postings survive, as _recomputeBandBookkeeping does: a hash whose bucket 0
            # is missing carries no df anywhere, so without it the cutoff never serves its postings
            # and the coverage report does not count them. A recreated bucket 0 gets an empty posting
            # list, since every lookup reads function_ids off each document it returns
            df = int(row["df"])
            pending.append(
                UpdateOne(
                    {"band_hash": row["_id"], "bucket": {"$in": [0, None]}},
                    {"$set": {"bucket": 0, "df": df, "tail": tail, "tail_n": tail_n}, "$setOnInsert": {"function_ids": []}},
                    upsert=df > 0,
                )
            )
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

    def _removeSampleFromPicHashCounts(self, sample_id: int) -> None:
        """Take a sample's functions out of the pichash counts; runs before they are deleted.

        The mirror of _addToPicHashCounts: one negative $inc per distinct hash. A hash that no
        function holds any more loses its document, as a rebuild would never have written it. The
        delete matches `df <= 0` per document, so a count raised again by a concurrent insert in
        between is left alone.
        """
        self._removeFromPicHashCounts({"sample_id": sample_id})

    def _removeFromPicHashCounts(self, function_filter: Dict[str, Any]) -> None:
        """_removeSampleFromPicHashCounts for the functions function_filter selects."""
        if not self.isPicHashCountIndexComplete():
            return
        self._uncountPicHashes(self._getDb().functions.find({**function_filter, "_pichash": {"$ne": None}}, {"_pichash": 1, "_id": 0}))

    def _uncountPicHashes(self, function_documents: Iterable[Dict[str, Any]]) -> None:
        """Take the pichashes these function documents carry out of the counts."""
        if not self.isPicHashCountIndexComplete():
            return
        decrements: Dict[Any, int] = {}
        for function_document in function_documents:
            encoded_pichash = function_document.get("_pichash")
            if encoded_pichash is None:
                continue
            decrements[encoded_pichash] = decrements.get(encoded_pichash, 0) + 1
        if not decrements:
            return
        collection = self._getDb()[self._PICHASH_COUNT_COLLECTION]
        collection.bulk_write([UpdateOne({"_pichash": encoded}, {"$inc": {"df": -count}}) for encoded, count in decrements.items()], ordered=False)
        encoded_pichashes = list(decrements)
        for offset in range(0, len(encoded_pichashes), self._PICHASH_COUNT_WRITE_BATCH):
            collection.delete_many({"_pichash": {"$in": encoded_pichashes[offset : offset + self._PICHASH_COUNT_WRITE_BATCH]}, "df": {"$lte": 0}})

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
        `_encodePichash` only ever writes a hex string - zero-padded, or `hex()` on an instance
        not yet migrated (#145), whose two spellings of a value are then separate runs, counted
        separately and summed again by the cutoff - and the caller verifies the total against an
        independent count rather than trusting that.
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
        value) and does the decode once per distinct signature instead of once per
        candidate function.

        The documents still have to be read one per function: the row also carries `sample_id`,
        which is genuinely per-function, and nothing stored lets the fetch ask for "the distinct
        signatures of these function ids" without a signature-keyed index that does not exist.

        Kept free of shared state so it can be run from a thread pool; merging the per-slice
        tables into one happens in the calling thread, in input order.
        """
        rows: List[Tuple[int, int, int]] = []
        signatures: List[bytes] = []
        index_by_stored: Dict[Any, int] = {}
        for function_document in self._getDb()[collection_name].find(
            {"function_id": {"$in": query_function_ids}},
            {"_id": 0, "sample_id": 1, "minhash": 1, "function_id": 1},
        ):
            # keyed by the stored value: during a migration one signature can be held as hex by one
            # function and as binary by another, which only costs a duplicate row in the table
            stored_minhash = function_document["minhash"]
            signature_index = index_by_stored.get(stored_minhash)
            if signature_index is None:
                signature_index = len(signatures)
                index_by_stored[stored_minhash] = signature_index
                signatures.append(self._minHashFromStorage(stored_minhash))
            rows.append((function_document["function_id"], function_document["sample_id"], signature_index))
        return rows, signatures

    def deleteXcfgForSampleId(self, sample_id: int) -> None:
        function_ids = [document["function_id"] for document in self._getDb().functions.find({"sample_id": sample_id}, {"function_id": 1, "_id": 0})]
        self._deleteXcfgForFunctionIds(function_ids)

    def deleteXcfgData(self) -> None:
        # dropping the collection reclaims the space, where blanking the field in place did not:
        # STORAGE_DROP_DISASSEMBLY now genuinely shrinks the instance (#137)
        self._getDb()["xcfg"].drop()

    def getLibraryInfoForSampleId(self, sample_id: int) -> Optional[Dict[str, str]]:
        sample_entry = self.getSampleById(sample_id)
        if sample_entry is None:
            return None
        if sample_entry.is_library:
            return {"family": sample_entry.family, "version": sample_entry.version}
        return None

    def isPicHash(self, pichash: int) -> bool:
        # 0 is what a function without a PicHash is stored with, not a hash (as in MemoryStorage)
        if not pichash:
            return False
        query = self._pichashLookupCondition("_pichash", pichash)
        return self._getDb().functions.find_one(query, {"_id": 1}) is not None

    def getMatchesForPicHash(self, pichash: int) -> Set[Tuple[int, int, int]]:
        if not pichash:
            return set()
        query = self._pichashLookupCondition("_pichash", pichash)
        return set(
            map(
                lambda x: (x["family_id"], x["sample_id"], x["function_id"]),
                list(self._getDb().functions.find(query, {"family_id": 1, "function_id": 1, "sample_id": 1, "_id": 0})),
            )
        )

    def getMatchesForPicHashes(self, pichashes: List[int]) -> Dict[int, Set[Tuple[int, int, int]]]:
        # padded on a padded instance; otherwise either spelling of a value may be stored (#145)
        padded = self.isPichashPadded()
        encoded_to_decoded = {}
        for pichash in set(pichashes):
            if not pichash:
                continue
            for encoded in [encode_pichash_value(pichash, padded=True)] if padded else pichash_value_variants(pichash):
                encoded_to_decoded[encoded] = pichash
        matches: Dict[int, Set[Tuple[int, int, int]]] = {}
        if encoded_to_decoded:
            wanted = self._filterPicHashesByMatchCount(sorted(encoded_to_decoded))
            fields_to_fetch = {"_pichash": 1, "family_id": 1, "sample_id": 1, "function_id": 1, "_id": 0}
            for hit in self._getDb().functions.find({"_pichash": {"$in": wanted}}, fields_to_fetch):
                matches.setdefault(encoded_to_decoded[hit["_pichash"]], set()).add((hit["family_id"], hit["sample_id"], hit["function_id"]))
        return matches

    def getMatchesForPicBlockHash(self, picblockhash: int) -> Set[Tuple[int, int, int, int]]:
        query = self._pichashLookupCondition("_picblockhashes.hash", picblockhash)
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
        function_entry.picblockhashes = picBlockHashEntries(self.blockhasher, smda_function, image_lower, image_upper)
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

    def getFamilyId(self, family_name: str) -> Optional[int]:
        family_document = self._getDb().families.find_one({"family_name": family_name})
        if family_document is None:
            return None
        return family_document["family_id"]

    def getFamilyIds(self) -> List[int]:
        return self._getDb().families.find().distinct("family_id")

    def addFamily(self, family_name: str) -> int:
        return self.ensureFamily(family_name)[0]

    def ensureFamily(self, family_name: str) -> Tuple[int, bool]:
        families = self._getDb().families
        family_document = families.find_one({"family_name": family_name}, {"family_id": 1, "_id": 0})
        if family_document is not None:
            return family_document["family_id"], False
        family_id = self._useCounter("families")
        family_entry = FamilyEntry(family_name=family_name, family_id=family_id)
        while True:
            try:
                # None when it inserted, which the unique index on family_name makes atomic
                family_document = families.find_one_and_update({"family_name": family_name}, {"$setOnInsert": family_entry.toDict()}, {"family_id": 1, "_id": 0}, upsert=True)
                if family_document is None:
                    break
            except DuplicateKeyError:
                # a concurrent upsert of the name won; MongoDB retries most of these itself
                family_document = families.find_one({"family_name": family_name}, {"family_id": 1, "_id": 0})
                if family_document is None:
                    # and its family is gone again since
                    continue
            # another writer added the name since the lookup, and the id drawn stays unused
            return family_document["family_id"], False
        try:
            self._updateDbState()
        except Exception:
            # the caller learns of the family only once this returns
            with contextlib.suppress(Exception):
                self.discardImportedFamily(family_id)
            raise
        return family_id, True

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

    def getFunctionsByIds(self, function_ids: List[int]) -> Dict[int, "FunctionEntry"]:
        function_entries = {}
        # query functions (negative ids) live in their own collection
        for collection, ids in (
            (self._getDb().functions, [function_id for function_id in function_ids if function_id >= 0]),
            (self._getDb().query_functions, [function_id for function_id in function_ids if function_id < 0]),
        ):
            if not ids:
                continue
            for function_document in collection.find({"function_id": {"$in": ids}}, {"_id": 0}):
                self._decodeFunction(function_document)
                function_entry = FunctionEntry.fromDict(function_document)
                function_entries[function_entry.function_id] = function_entry
        return function_entries

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

    def _countDistinctPicHashes(self) -> int:
        """How many distinct pichashes the stored functions carry.

        While the pichash count index is complete it holds one document per distinct hash, so
        counting those is the answer, and it reads one small document per hash instead of grouping
        every function. Only documents with `df > 0` are counted: a sample delete decrements first
        and removes the documents that reach 0 afterwards (_removeSampleFromPicHashCounts), so a
        `df <= 0` document is a hash no function holds any more - in between, or for good when
        the delete died between the two steps. Otherwise the count falls back to the grouping.
        """
        if self.isPicHashCountIndexComplete():
            return self._getDb()[self._PICHASH_COUNT_COLLECTION].count_documents({"df": {"$gt": 0}})
        # a collection with unique pic hashes will easily exceed 16M, hence the $count rather than
        # a list of groups; a function without a pichash is not a hash, as in the count index
        num_unique_pichashes = 0
        pipeline = [{"$match": {"_pichash": {"$ne": None}}}, {"$group": {"_id": "$_pichash"}}, {"$count": "Total"}]
        for result in self._getDb().functions.aggregate(pipeline, allowDiskUse=True):
            num_unique_pichashes = result["Total"]
        return num_unique_pichashes

    def getStats(self, with_pichash: bool = True) -> Dict[str, Union[int, str, None]]:
        # None signals "not computed", so that consumers can distinguish it from an actual count of 0
        num_unique_pichashes = None
        if with_pichash:
            num_unique_pichashes = self._countDistinctPicHashes()
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
            "pichash_padded": self.isPichashPadded(),
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
        # every band is gone by now, so a record that cannot be banded must not end the rebuild - it
        # failed at that batch, leaving it and every later one out of the index, on every run. A
        # function document without a minhash field (an upsert racing a deletion) has nothing to
        # band, and a minhash of another length than the configured signatures (imports stored such
        # unchecked) does not fit the projection, so both are left out
        signature_bytes = self._minhash_config.MINHASH_SIGNATURE_LENGTH * (1 if self._minhash_config.MINHASH_SIGNATURE_BITS <= 8 else 4)
        num_skipped = 0
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
                minhash_bytes = self._minHashFromStorage(function_document.get("minhash"))
                if "minhash" not in function_document:
                    LOGGER.warning("Function %d has no minhash field, leaving it out of the band index.", function_document["function_id"])
                if not minhash_bytes:
                    continue
                if len(minhash_bytes) != signature_bytes:
                    num_skipped += 1
                    continue
                minhash_obj = MinHash(function_document["function_id"], minhash_bytes, minhash_bits=self._minhash_config.MINHASH_SIGNATURE_BITS)
                minhashes.append(minhash_obj)
            self._addMinHashesToBands(minhashes)
            minhash_functions += len(minhashes)
            if progress_reporter:
                progress_reporter.step()
        if num_skipped:
            LOGGER.warning("Left %d functions out of the band index, their minhashes are not %d bytes long; recalculating minhashes indexes them.", num_skipped, signature_bytes)
        return {"minhash_functions_indexed": minhash_functions}

    def _resetBands(self) -> None:
        for band_id in range(self._storage_config.STORAGE_NUM_BANDS):
            self._getDb()["band_%d" % band_id].drop()
            # the same indexes _initDb and rebuildMinhashBandIndex create: the df cutoff and the
            # bucketed writes need theirs from the first write on, not from the next restart
            self._getDb()["band_%d" % band_id].create_index("band_hash")
            self._getDb()["band_%d" % band_id].create_index([("band_hash", 1), ("df", 1)])
            self._getDb()["band_%d" % band_id].create_index([("band_hash", 1), ("bucket", 1)])

    def deleteAllMinHashes(self, progress_reporter=None):
        # delete all minhashes
        self._getDb().functions.update_many({}, {"$set": {"minhash": ""}})
        # reset bands
        self._resetBands()
        LOGGER.info("Dropped all Minhashes and created a fresh banding index.")
        return

    def deleteRehashableMinHashes(self, progress_reporter=None) -> Set[int]:
        self._resetBands()
        functions = self._getDb().functions
        kept_sample_ids: Set[int] = set()
        num_dropped = num_kept = num_unbandable = 0
        signature_bytes = self._minhash_config.MINHASH_SIGNATURE_LENGTH * (1 if self._minhash_config.MINHASH_SIGNATURE_BITS <= 8 else 4)
        # streamed in function_id order rather than listed first: a corpus of ten million functions
        # would otherwise hold every id at once. Each batch only changes documents already read.
        hashed = functions.find({"minhash": {"$nin": ["", None]}}, {"function_id": 1, "_id": 0}).sort("function_id", 1).batch_size(10000)
        for batch in self._batchedIds(hashed, 10000):
            # disassembly lives in the xcfg collection, or inline in a function not yet migrated by
            # migrate_xcfg_split; an empty one is what STORAGE_DROP_DISASSEMBLY leaves
            with_disassembly = {document["_id"] for document in self._getDb().xcfg.find({"_id": {"$in": batch}, "_xcfg": {"$nin": ["", "{}", None]}}, {"_id": 1})}
            with_disassembly.update(
                document["function_id"]
                for document in functions.find({"function_id": {"$in": batch}, "_xcfg": {"$exists": True, "$nin": ["", "{}", None]}}, {"function_id": 1, "_id": 0})
            )
            rehashable = [function_id for function_id in batch if function_id in with_disassembly]
            if rehashable:
                num_dropped += functions.update_many({"function_id": {"$in": rehashable}}, {"$set": {"minhash": ""}}).modified_count
            kept = [function_id for function_id in batch if function_id not in with_disassembly]
            if kept:
                kept_documents = list(functions.find({"function_id": {"$in": kept}}, {"function_id": 1, "sample_id": 1, "minhash": 1, "_id": 0}))
                # as in rebuildMinhashBandIndex, one of another length than the configured signature
                # does not fit the band projection: it stays stored, but is left out of the bands
                kept_minhashes = []
                for document in kept_documents:
                    minhash_bytes = self._minHashFromStorage(document.get("minhash"))
                    if len(minhash_bytes) != signature_bytes:
                        num_unbandable += 1
                        continue
                    kept_minhashes.append(MinHash(document["function_id"], minhash_bytes, minhash_bits=self._minhash_config.MINHASH_SIGNATURE_BITS))
                self._addMinHashesToBands(kept_minhashes)
                kept_sample_ids.update(document["sample_id"] for document in kept_documents)
                num_kept += len(kept_minhashes)
        LOGGER.info(
            "Dropped %d minhashes to recalculate and created a fresh banding index; kept %d of functions without disassembly in %d samples.",
            num_dropped,
            num_kept,
            len(kept_sample_ids),
        )
        if num_unbandable:
            LOGGER.warning("Left %d kept minhashes out of the band index, they are not %d bytes long.", num_unbandable, signature_bytes)
        return kept_sample_ids

    @staticmethod
    def _batchedIds(documents, size: int) -> Iterator[List[int]]:
        batch: List[int] = []
        for document in documents:
            batch.append(document["function_id"])
            if len(batch) == size:
                yield batch
                batch = []
        if batch:
            yield batch

    def recalculateAllPicHashes(self, progress_reporter=None):
        # get current SMDA version
        smda_version = SmdaConfig().VERSION
        # get samples where recalculation is necessary: those not yet stamped as rehashed by a
        # compatible smda, rather than those whose report is old, which every run would pick again (#249)
        samples_to_be_updated = {}
        for sample_document in self._getDb().samples.find(
            self._stalePicHashQuery(self._getPicHashCompatibilityThreshold()),
            {"sample_id": 1, "smda_version": 1, "architecture": 1, "base_addr": 1, "binary_size": 1, "bitness": 1, "_id": 0},
        ):
            samples_to_be_updated[sample_document["sample_id"]] = sample_document
        # and those whose block hashes a picblocks escaped as Intel code while they are not (#240)
        num_stale_picblockhash_samples = 0
        for sample_document in self._getDb().samples.find(
            self._stalePicBlockHashQuery(), {"sample_id": 1, "smda_version": 1, "architecture": 1, "base_addr": 1, "binary_size": 1, "bitness": 1, "_id": 0}
        ):
            num_stale_picblockhash_samples += 1
            if sample_document["sample_id"] not in samples_to_be_updated:
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
        samples_skipped = 0
        picblockhash_index_invalidated = False
        pichash_count_index_invalidated = False
        for sample_id, sample_info in samples_to_be_updated.items():
            pic_hash_updates = []
            sample_xcfg_missing = 0
            picblockhashes_updated_before = picblockhashes_updated
            functions_updated_before = functions_updated
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
                    sample_xcfg_missing += 1
                    continue
                # rebuilt and rehashed before anything is counted: a function stored as {} is skipped,
                # and its old block hashes are not updatable
                try:
                    smda_function = smdaFunctionFromXcfg(json.loads(function_document["_xcfg"]), binary_info)
                    if smda_function is not None:
                        old_pichash = int(function_document["_pichash"], 16)
                        old_blockhashes = [{**blockhash, "hash": int(blockhash["hash"], 16)} for blockhash in function_document.get("_picblockhashes", [])]
                        new_pichash = smda_function.getPicHash(binary_info)
                        picblockhashes = picBlockHashEntries(self.blockhasher, smda_function, binary_info.base_addr, binary_info.base_addr + binary_info.binary_size)
                except Exception as error:
                    # an imported function holds whatever disassembly and hashes, and its sample whatever
                    # architecture, base address and size, the export stated: one that cannot be rehashed
                    # would fail the job on every run, so it is skipped like one without disassembly
                    LOGGER.warning("Function %d of sample %d cannot be rehashed from its stored disassembly and is skipped: %s", function_document["function_id"], sample_id, error)
                    smda_function = None
                if smda_function is None:
                    # stored as {} - as unrehashable as a missing blob, so the sample must not be stamped
                    xcfg_missing += 1
                    sample_xcfg_missing += 1
                    continue
                picblockhashes_updatable += len(old_blockhashes)
                if old_pichash != new_pichash:
                    functions_updated += 1
                    update_document["pichash"] = new_pichash
                # check if any blockhashes changed and possibly stage for update
                set_old = set([(pbh["offset"], pbh["hash"]) for pbh in old_blockhashes])
                set_new = set([(pbh["offset"], pbh["hash"]) for pbh in picblockhashes])
                if len(set_new) != len(set_old.intersection(set_new)):
                    picblockhashes_updated += len(picblockhashes)
                    update_document["picblockhashes"] = picblockhashes
                # prepare single function entry update
                if update_document:
                    self._encodePichash(update_document, padded=self.isPichashPadded())
                    update_command = {"$set": update_document}
                    pic_hash_updates.append(UpdateOne({"function_id": function_document["function_id"]}, update_command, upsert=True))
            # batch insert updates for function_entries
            if pic_hash_updates:
                if picblockhashes_updated > picblockhashes_updated_before and not picblockhash_index_invalidated:
                    # block hashes are about to be rewritten in place, so the inverted index stops
                    # describing the corpus. Marking it incomplete first makes getUniqueBlocks fall
                    # back to the scan - slow but correct - until rebuildPicBlockHashIndex runs, even
                    # if this run does not finish. Updating it incrementally here would mean diffing
                    # old against new hashes per function, which is what the rebuild does anyway.
                    self._setPicBlockHashIndexComplete(False)
                    picblockhash_index_invalidated = True
                if functions_updated > functions_updated_before and not pichash_count_index_invalidated:
                    # the same for the pichash counts, which are keyed by the hash: left complete,
                    # they would go on counting the old hashes and miss the new ones, which the
                    # cutoff then drops, and a later deleteSample would decrement the new ones,
                    # taking them away from other samples holding them (#261). Until rebuildPicHashCountIndex runs, the MINHASH_PICHASH_MAX_MATCHES
                    # cutoff counts from the functions collection, slower but correct.
                    self._setPicHashCountIndexComplete(False)
                    pichash_count_index_invalidated = True
                self._getDb().functions.bulk_write(pic_hash_updates, ordered=False)
            # only a sample whose every function was rehashed holds hashes of this smda and picblocks,
            # whether or not any of them changed; smda_version keeps naming the smda of the report
            # one that was not is marked, so neither selection picks it again until smda changes
            if sample_xcfg_missing:
                samples_skipped += 1
                self._getDb().samples.update_one({"sample_id": sample_id}, {"$set": {"pichash_unrehashable_smda_version": smda_version}})
            else:
                self._getDb().samples.update_one(
                    {"sample_id": sample_id},
                    {"$set": {"pichash_smda_version": smda_version, "picblockhash_version": PICBLOCKS_VERSION}, "$unset": {"pichash_unrehashable_smda_version": ""}},
                )
            if progress_reporter:
                progress_reporter.step()
        self._getDb().command("reIndex", "functions")
        LOGGER.info(
            f"Found {total_samples} outdated samples, {functions_updated}/{functions_updatable} PicHashes and {picblockhashes_updated}/{picblockhashes_updatable} PicBlockHashes were updated."
        )
        if xcfg_missing:
            LOGGER.warning(
                f"{xcfg_missing} functions in {samples_skipped} samples could not be updated as no CFG was available for them, these samples are marked unrehashable until smda changes."
            )
        if picblockhash_index_invalidated:
            LOGGER.warning("picblockhash index invalidated by the recalculation - run rebuildPicBlockHashIndex().")
        if pichash_count_index_invalidated:
            LOGGER.warning("pichash count index invalidated by the recalculation - run rebuildPicHashCountIndex().")
        return {
            "outdated_samples": total_samples,
            "stale_picblockhash_samples": num_stale_picblockhash_samples,
            "functions_updatable": functions_updatable,
            "functions_updated": functions_updated,
            "picblockhashes_updatable": picblockhashes_updatable,
            "picblockhashes_updated": picblockhashes_updated,
            "xcfg_missing": xcfg_missing,
            "samples_skipped_xcfg_missing": samples_skipped,
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
        # grouped in one pass: filtering the documents once per sample was quadratic in a batch of
        # many samples (#300), which importFunctionEntries may still be handed
        documents_by_sample: Dict[int, List[Dict]] = {}
        for document in function_documents:
            documents_by_sample.setdefault(document["sample_id"], []).append(document)
        updates = []
        for sample_id in sorted(documents_by_sample):
            for block_hash in self._picBlockHashesOfDocuments(documents_by_sample[sample_id]):
                updates.append(UpdateOne({"_id": block_hash}, {"$addToSet": {"sample_ids": sample_id}}, upsert=True))
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
        return self._pullSampleFromPicBlockHashes(sample_id, hashes)

    def _pullSampleFromPicBlockHashes(self, sample_id: int, hashes: List[str]) -> int:
        """Drop a sample from the posting lists of the given block hashes, and the emptied lists."""
        if not hashes or not self._isPicBlockHashIndexComplete():
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

    @staticmethod
    def _unpaddedBlockHash(stored_hash: str) -> str:
        """The one spelling getUniqueBlocks keys a block hash by on an instance not (yet) padded.

        Such an instance may hold a value in both spellings while migrate_pichash_padding is
        underway (#145), and a candidate is dropped by looking its key up, so both have to land on
        the same key. `hex()` is the one its values had before the migration started. Only a
        padded value with a leading zero is spelled differently, and the scan fallback calls this
        once per block entry of the corpus, so every other value is returned without parsing it.
        """
        if stored_hash[2:3] != "0":
            return stored_hash
        return hex(int(stored_hash, 16))

    def _reduceToUniqueBlocksUsingIndex(self, candidate_picblockhashes: Dict, sample_ids: List[int], progress: UniqueBlocksProgressReporter) -> None:
        """Drop every candidate the index shows in a sample outside the request, a step of <progress> per candidate."""
        requested_sample_ids = set(sample_ids)
        collection = self._getDb()[self._PICBLOCKHASH_INDEX_COLLECTION]
        padded = self.isPichashPadded()
        candidate_hashes = list(candidate_picblockhashes)
        for offset in range(0, len(candidate_hashes), self._PICBLOCKHASH_INDEX_QUERY_SLICE):
            hash_slice = candidate_hashes[offset : offset + self._PICBLOCKHASH_INDEX_QUERY_SLICE]
            if not padded:
                # the index is keyed on the stored spelling, which may be either width here
                hash_slice = sorted({variant for block_hash in hash_slice for variant in pichash_value_variants(int(block_hash, 16))})
            for document in collection.find({"_id": {"$in": hash_slice}}, {"sample_ids": 1}):
                if any(sample_id not in requested_sample_ids for sample_id in document["sample_ids"]):
                    candidate_picblockhashes.pop(document["_id"] if padded else self._unpaddedBlockHash(document["_id"]), None)
            progress.step(len(hash_slice))

    def _reduceToUniqueBlocksByScan(self, candidate_picblockhashes: Dict, sample_ids: List[int], progress: UniqueBlocksProgressReporter) -> None:
        """The pre-index elimination: read every function that has block hashes, a step of <progress> per function.

        Kept as the fallback for an instance whose index has not been built yet, so that upgrading
        never silently changes results - it only stays slow until rebuildPicBlockHashIndex runs.
        """
        padded = self.isPichashPadded()
        for entry in self._getDb().functions.find({"_picblockhashes": {"$exists": True, "$ne": []}}, {"sample_id": 1, "_picblockhashes": 1, "_id": 0}):
            progress.step()
            if entry["sample_id"] not in sample_ids:
                for block_entry in entry["_picblockhashes"]:
                    candidate_picblockhashes.pop(block_entry["hash"] if padded else self._unpaddedBlockHash(block_entry["hash"]), None)

    def getUniqueBlocks(self, sample_ids: List[int], progress_reporter=None) -> Dict:
        progress = UniqueBlocksProgressReporter.wrap(progress_reporter)
        # query once to get all blocks from the functions of our samples
        block_statistics: Dict[str, Any] = {
            "by_sample_id": {sample_id: {"sample_id": sample_id, "total_blocks": 0, "characteristic_blocks": 0, "unique_blocks": 0} for sample_id in sample_ids},
            "unique_blocks_overall": 0,
            "num_samples": len(sample_ids),
        }
        candidate_picblockhashes: Dict[str, Dict[str, Any]] = {}
        # keyed by the stored spelling, which on an instance not (yet) padded is normalised so
        # that a value stored in both widths during a migration stays one block (#145)
        padded = self.isPichashPadded()
        # expect every function of the samples; those without blocks take no step, and the next
        # start_phase fills what they leave
        num_functions = sum(
            document.get("statistics", {}).get("num_functions", 0)
            for document in self._getDb().samples.find({"sample_id": {"$in": sample_ids}}, {"statistics.num_functions": 1, "_id": 0})
        )
        progress.start_phase("collect", num_functions)
        for entry in self._getDb().functions.find(
            {"sample_id": {"$in": sample_ids}, "_picblockhashes": {"$exists": True, "$ne": []}}, {"function_id": 1, "sample_id": 1, "_picblockhashes": 1, "_id": 0}
        ):
            sample_id = entry["sample_id"]
            for block_entry in entry["_picblockhashes"]:
                block_hash = block_entry["hash"] if padded else self._unpaddedBlockHash(block_entry["hash"])
                candidate = candidate_picblockhashes.get(block_hash)
                # a block in several functions is represented by the lowest function_id, as
                # MemoryStorage has it, whatever order find() answers in (#283)
                if candidate is None or entry["function_id"] < candidate["function_id"]:
                    candidate_picblockhashes[block_hash] = {
                        "samples": candidate["samples"] if candidate is not None else set(),
                        "length": block_entry["length"],
                        "function_id": entry["function_id"],
                        "sample_id": sample_id,
                        "offset": decode_two_complement(block_entry["offset"]),
                        "instructions": [],
                        "escaped_sequence": "",
                        "score": 0,
                    }
                candidate_picblockhashes[block_hash]["samples"].add(sample_id)
            progress.step()
        # update statistics based on candidates
        for picblockhash, entry in candidate_picblockhashes.items():
            for sample_id in entry["samples"]:
                block_statistics["by_sample_id"][sample_id]["total_blocks"] += 1
        LOGGER.info(f"Found {len(candidate_picblockhashes)} candidate picblock hashes")
        # remove those that are not unique
        if self._isPicBlockHashIndexComplete():
            progress.start_phase("eliminate", len(candidate_picblockhashes))
            self._reduceToUniqueBlocksUsingIndex(candidate_picblockhashes, sample_ids, progress)
        else:
            # the scan steps once per function with blocks; counting those would read the collection
            # once more, so the phase expects the collection's size, as its metadata estimates it
            progress.start_phase("eliminate", self._getDb().functions.estimated_document_count())
            LOGGER.warning(
                "picblockhash index is not built - falling back to a full scan of the functions collection. "
                "Run rebuildPicBlockHashIndex() (Worker.rebuildPicBlockHashIndex) to enable the indexed path."
            )
            self._reduceToUniqueBlocksByScan(candidate_picblockhashes, sample_ids, progress)
        # update statistics again after having reduced to results
        for picblockhash, entry in candidate_picblockhashes.items():
            if len(entry["samples"]) == 1:
                single_sample_id = list(entry["samples"])[0]
                block_statistics["by_sample_id"][single_sample_id]["unique_blocks"] += 1
            for sample_id in entry["samples"]:
                block_statistics["by_sample_id"][sample_id]["characteristic_blocks"] += 1
        block_statistics["unique_blocks_overall"] = len(candidate_picblockhashes)
        LOGGER.info(f"Reduced to {len(candidate_picblockhashes)} unique picblock hashes")
        # iterate over candidates by function_id and extract instructions
        function_id_to_block_offsets = {}
        progress.start_phase("score", len(candidate_picblockhashes))
        for picblockhash, entry in candidate_picblockhashes.items():
            candidate_picblockhashes[picblockhash]["samples"] = sorted(list(entry["samples"]))
            # we calculate the score for this block as 80% of how well it covers the samples and 20% how far its size is away from an "ideal" signature block
            candidate_picblockhashes[picblockhash]["score"] = blockScore(len(entry["samples"]), len(sample_ids), entry["length"])
            if entry["function_id"] not in function_id_to_block_offsets:
                function_id_to_block_offsets[entry["function_id"]] = []
            function_id_to_block_offsets[entry["function_id"]].append((entry["offset"], picblockhash))
            progress.step()
        block_function_ids = list(function_id_to_block_offsets.keys())
        # a step per xcfg blob fetched and per function read
        progress.start_phase("extract", 2 * len(block_function_ids))
        block_xcfg_blobs = self._fetchXcfgBlobs(block_function_ids, progress_reporter=progress)
        for entry in self._getDb().functions.find({"function_id": {"$in": block_function_ids}}, {"function_id": 1, "_xcfg": 1, "_id": 0}):
            function_id = entry["function_id"]
            # inline `_xcfg` is the fallback for pre-migration documents (see _attachXcfgBlobs)
            entry["_xcfg"] = block_xcfg_blobs.get(function_id) or entry.get("_xcfg") or "{}"
            self._decodeXcfg(entry)
            # a function whose disassembly was dropped (STORAGE_DROP_DISASSEMBLY, #42) decodes to {}:
            # its blocks keep no instructions rather than failing the whole job, as in MemoryStorage
            blocks = entry["xcfg"].get("blocks") or {}
            for block_offset, picblockhash in function_id_to_block_offsets[function_id]:
                block_instructions = blocks.get(str(block_offset))
                if block_instructions is None:
                    continue
                candidate_picblockhashes[picblockhash]["instructions"] = block_instructions
            progress.step()
        LOGGER.info(f"Instructions for {len(candidate_picblockhashes)} blocks extracted.")
        # called directly rather than by Worker.getUniqueBlocks, which finishes its own
        if progress is not progress_reporter:
            progress.finish()
        return {"statistics": block_statistics, "unique_blocks": candidate_picblockhashes}

    ##### helpers for search ######

    # pichashes are stored hex-encoded in a differently named field ("_pichash"). Sorting on them
    # orders numerically only once the instance stores them zero-padded, and the search cursor
    # pages by comparing the sort field with a range operator, which the transpiler only allows
    # then (#145); an instance that has not been migrated keeps rejecting the sort by name.
    _SORT_FIELD_TO_STORED = {"pichash": "_pichash"}

    def _assert_sortable_fields(self, full_cursor: Optional[FullSearchCursor]) -> None:
        if full_cursor is None or self.isPichashPadded():
            return
        for field in full_cursor.sort_fields:
            if field == "pichash":
                raise ValueError("Sorting by the field 'pichash' needs zero-padded pichashes; run migrate_pichash_padding first.")

    def _get_sort_list_from_cursor(self, full_cursor: Optional[FullSearchCursor]):
        self._assert_sortable_fields(full_cursor)
        if full_cursor is None:
            return None
        is_backward_search = not full_cursor.is_forward_search
        sort_list = [(self._SORT_FIELD_TO_STORED.get(key, key), 1 if direction ^ is_backward_search else -1) for key, direction in full_cursor.sort_by_list]
        return sort_list

    def _getDistinctValues(self, collection: str, field: str, max_time_ms: Optional[int] = None) -> Optional[List[Any]]:
        """All distinct values of the field, or None when there are more than _DISTINCT_VALUES_CAP
        of them, they exceed _DISTINCT_VALUES_MAX_BYTES in total or the scan runs out of the search time
        limit (STORAGE_MONGODB_SEARCH_MAX_TIME_MS); a field of _DISTINCT_VALUES_IN_ARRAYS only stops
        at the time limit and the 16 MiB a distinct can answer. A None is remembered for
        _OVER_CAP_TTL seconds, so a large corpus pays for the capped scan once, not per search.
        max_time_ms replaces the search time limit for a scan that has only part of it; running out
        of that is the same verdict: the listing is too slow for the limit searches have."""
        if field == OTHER_LABELS_FIELD:
            return self._getOtherLabels(max_time_ms)
        if self._isOverCap(collection, field):
            return None
        try:
            values = self._scanDistinctValues(collection, field, max_time_ms or self._getSearchMaxTimeMS())
        except ExecutionTimeout:
            # the same verdict as too many values: the search matches the names in the database instead
            values = None
        if values is None:
            self._over_cap_since[(collection, field)] = time.monotonic()
        elif field in self._DISTINCT_VALUES_IN_ARRAYS:
            self._distinct_count_since[(collection, field)] = (len(values), time.monotonic())
        return values

    def _getOtherLabels(self, max_time_ms: Optional[int] = None) -> Optional[List[Any]]:
        """The distinct other labels (#301), listed again only when settings.other_labels_version
        moved since this process last listed them: a search reads one small document instead of
        listing them (2 ms for 500, 55 ms for 22,000). None - the search is then done without them -
        when they exceed the 16 MiB a distinct answers, the time limit or the values a search may
        test (MongoSearchTranspiler._MAX_VALUE_TESTS), a verdict kept for _OVER_CAP_TTL seconds
        whatever the version does, as each try may cost the whole time limit. Their size is not
        capped otherwise: the $in and $nin a search builds hold only the ones its terms match, which
        _matchingOtherLabels bounds.

        The version is read before the listing, so a write that lands meanwhile has the next search
        list again; one that lands after the listing but before its bump goes unseen by searches
        until the bump, as it would without the cache.
        """
        unlisted_since = self._other_labels_unlisted_since
        if unlisted_since is not None and time.monotonic() - unlisted_since < self._OVER_CAP_TTL:
            return None
        version = self._otherLabelsVersion()
        held = self._other_labels_listing
        if held is not None and held[0] == version:
            return held[1]
        try:
            values = self._scanDistinctValues("functions", OTHER_LABELS_FIELD, max_time_ms or self._getSearchMaxTimeMS())
        except ExecutionTimeout:
            values = None
        if values is None or len(values) > MongoSearchTranspiler._MAX_VALUE_TESTS:
            self._other_labels_unlisted_since = time.monotonic()
            return None
        self._other_labels_unlisted_since = None
        # a thread that read an older version than another one's listing leaves that in place
        held = self._other_labels_listing
        if held is None or held[0] < version:
            self._other_labels_listing = (version, values)
        return values

    def _lastDistinctCount(self, collection: str, field: str) -> Optional[int]:
        count, since = self._distinct_count_since.get((collection, field), (None, 0.0))
        return count if count is not None and time.monotonic() - since < self._OVER_CAP_TTL else None

    def _isOverCap(self, collection: str, field: str) -> bool:
        found_over_cap = self._over_cap_since.get((collection, field))
        return found_over_cap is not None and time.monotonic() - found_over_cap < self._OVER_CAP_TTL

    def _scanDistinctValues(self, collection: str, field: str, max_time_ms: Optional[int]) -> Optional[List[Any]]:
        if field in self._DISTINCT_VALUES_IN_ARRAYS:
            try:
                return self._getDb()[collection].distinct(field, maxTimeMS=max_time_ms or 0)
            except OperationFailure as failure:
                # 17217: "distinct too big, 16mb cap"
                if failure.code == 17217:
                    return None
                raise
        pipeline = [{"$group": {"_id": "$" + field}}, {"$limit": self._DISTINCT_VALUES_CAP + 1}]
        values = []
        total_bytes = 0
        for document in self._getDb()[collection].aggregate(pipeline, maxTimeMS=max_time_ms or 0):
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
        full_tree = self._resolve_search_tree(search_fields, search_tree, cursor, conditional_search_fields)
        known_values = self._list_known_values(full_tree, distinct_fields or {})
        query = MongoSearchTranspiler(known_values, pichash_padded=self.isPichashPadded()).visit(full_tree)
        return query

    def _resolve_search_tree(self, search_fields: List[str], search_tree: NodeType, cursor: Optional[FullSearchCursor], conditional_search_fields=None) -> NodeType:
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
        return PropagateNot().visit(full_tree)

    def _list_known_values(self, full_tree: NodeType, distinct_fields: Dict[str, str], max_time_ms: Optional[int] = None) -> Dict[str, List[Any]]:
        """Lists each field once, however many conditions of the tree it serves."""
        known_values = {}
        for field, collection in distinct_fields.items():
            # the labels are listed for a "?" only: their listing is paid on every search, and the $nin a
            # "!?" would become needs every document examined just like the regex it replaces
            operators = ("?",) if field in self._DISTINCT_VALUES_IN_ARRAYS else ("?", "!?")
            if _hasSubstringCondition(full_tree, field, operators):
                values = self._getDistinctValues(collection, field, max_time_ms)
                if values is not None:
                    known_values[field] = values
        return known_values

    ##### search ####

    def _getSearchMaxTimeMS(self) -> Optional[int]:
        """The time limit of a search query in milliseconds, None for no limit. Read with a default, as a
        config module replaced by an older copy does not name the setting."""
        return int(getattr(self._storage_config, "STORAGE_MONGODB_SEARCH_MAX_TIME_MS", 30000) or 0) or None

    @contextlib.contextmanager
    def _searchTimeLimit(self):
        """Answers a search that ran out of its time limit, or whose query grew past what MongoDB
        accepts, as a ValueError, which the REST layer turns into a 400."""
        try:
            yield
        except ExecutionTimeout as timeout:
            raise ValueError("The search took too long and was stopped. Narrow the search term.") from timeout
        except DocumentTooLarge as too_large:
            raise ValueError("The search is too large. Use fewer search terms.") from too_large

    def findFamilyByString(self, search_tree: NodeType, cursor: Optional[FullSearchCursor] = None, max_num_results: int = 100) -> Dict[int, "FamilyEntry"]:
        result_dict = {}
        with self._searchTimeLimit():
            search_fields = ["family_name"]
            query = self._get_search_query(search_fields, search_tree, cursor)
            sort_list = self._get_sort_list_from_cursor(cursor)
            for family_document in self._getDb().families.find(query, {"_id": 0}, sort=sort_list, limit=max_num_results, max_time_ms=self._getSearchMaxTimeMS()):
                entry = FamilyEntry.fromDict(family_document)
                result_dict[family_document["family_id"]] = entry
        return result_dict

    def findSampleByString(self, search_tree: NodeType, cursor: Optional[FullSearchCursor] = None, max_num_results: int = 100) -> Dict[int, "SampleEntry"]:
        result_dict = {}
        with self._searchTimeLimit():
            search_fields = [
                "filename",
                "family",
                "component",
                "version",
            ]
            conditional_field = ("sha256", lambda search_term: len(search_term) >= 3)
            query = self._get_search_query(search_fields, search_tree, cursor, conditional_search_fields=[conditional_field])
            sort_list = self._get_sort_list_from_cursor(cursor)
            for sample_document in self._getDb().samples.find(query, {"_id": 0}, sort=sort_list, limit=max_num_results, max_time_ms=self._getSearchMaxTimeMS()):
                entry = SampleEntry.fromDict(sample_document)
                result_dict[entry.sample_id] = entry
        return result_dict

    @staticmethod
    def _remainingMS(deadline: Optional[float]) -> Optional[int]:
        """What is left of a search's time limit, None for no limit; a search past it is stopped."""
        if deadline is None:
            return None
        remaining_ms = round((deadline - time.monotonic()) * 1000)
        if remaining_ms <= 0:
            raise ExecutionTimeout("the search used up its time limit")
        return remaining_ms

    def _canProbe(self, sort_list, max_time_ms: Optional[int]) -> bool:
        if not sort_list or self._isOverCap("functions", "function_labels.function_label"):
            return False
        return max_time_ms is None or max_time_ms > self._LABEL_PROBE_TIME_MS

    def _probeFunctionSearch(self, query: Dict, sort_list, max_num_results: int, max_time_ms: Optional[int]) -> Optional[List[Dict]]:
        """The page the regex finds within _LABEL_PROBE_TIME_MS, None when it does not, or when there
        is no point in a probe: the labels are over the cap, so the search is the regex anyway, or
        the search's own limit leaves no room for it.

        The probe walks the index of the sort order - (field, function_id) for every sortable field -
        by hint: left to plan it, MongoDB first tries the label index, whose regex fetches every
        labelled function, and a probe that runs out while planning never caches the plan, so every
        later probe would run out the same way (~400 ms of planning against ~20 ms of walking).
        """
        if not self._canProbe(sort_list, max_time_ms):
            return None
        hint = [(field, 1) for field, _ in sort_list]
        try:
            return list(
                self._getDb().functions.find(query, self._FUNCTION_SEARCH_PROJECTION, sort=sort_list, limit=max_num_results, max_time_ms=self._LABEL_PROBE_TIME_MS, hint=hint)
            )
        except OperationFailure:
            # out of time - or a database without the index to hint, which the listing serves as well
            return None

    def _findFunctionsByLabels(self, full_tree: NodeType, sort_list, max_num_results: int) -> List[Dict]:
        """A search of label conditions alone: the regex is probed, then the labels are listed. Both
        count against the search time limit, and the final query gets what is left of it."""
        max_time_ms = self._getSearchMaxTimeMS()
        deadline = None if max_time_ms is None else time.monotonic() + max_time_ms / 1000
        if self._canProbe(sort_list, max_time_ms):
            function_documents = self._probeFunctionSearch(MongoSearchTranspiler(pichash_padded=self.isPichashPadded()).visit(full_tree), sort_list, max_num_results, max_time_ms)
            if function_documents is not None:
                return function_documents
        field = "function_labels.function_label"
        # too many terms to test against every label: the transpiler would leave them to the regex
        value_tests = len(_substringTerms(full_tree, field, "?"))
        last_count = self._lastDistinctCount("functions", field)
        if last_count is not None and value_tests * last_count > MongoSearchTranspiler._MAX_VALUE_TESTS:
            known_values = {}
        else:
            # the listing gets half of what is left, so that the query keeps the other half even when
            # the listing runs out of its share: then the search is the regex, as it is for the next
            # _OVER_CAP_TTL seconds
            remaining_ms = self._remainingMS(deadline)
            listing_ms = None if remaining_ms is None else max(1, remaining_ms // 2)
            known_values = self._list_known_values(full_tree, {field: "functions"}, listing_ms)
            if value_tests * len(known_values.get(field, [])) > MongoSearchTranspiler._MAX_VALUE_TESTS:
                known_values = {}
        transpiler = MongoSearchTranspiler(known_values, pichash_padded=self.isPichashPadded())
        query = transpiler.visit(full_tree)
        if transpiler.unlisted_fields:
            # Some terms were left to the regex (past the query's byte budget): in an $or with listed
            # ones, MongoDB would scan the label index once per regex (3.3 s against 0.9 s for 400 terms
            # over 50,000 functions). The regex alone walks the documents once.
            query = MongoSearchTranspiler(pichash_padded=self.isPichashPadded()).visit(full_tree)
        return list(self._getDb().functions.find(query, self._FUNCTION_SEARCH_PROJECTION, sort=sort_list, limit=max_num_results, max_time_ms=self._remainingMS(deadline)))

    # what a plain search term matches: the name, and the labels that differ from it (#301)
    _PLAIN_FUNCTION_SEARCH_FIELDS = ["function_name", OTHER_LABELS_FIELD]

    def _findFunctionsByNameOrOtherLabel(self, full_tree: NodeType, sort_list, max_num_results: int) -> List[Dict]:
        """A search with a plain term, which matches function_name or OTHER_LABELS_FIELD (#301).

        As one query, $or[name, other labels] costs a sort by name its bounded walk of the
        (function_name, function_id) index: the name branch can no longer deliver the order, so
        MongoDB reads every name the term matches and sorts them (4.7 -> 98-167 ms for a common term
        over 250,000 functions). Instead the functions are searched in two disjoint parts:

        - those whose other labels the terms cannot match - nearly all, as most have none - by the
          name query as before: the conditions on the field fold into what they are for a missing
          field, and a $nin of the other labels the terms match keeps the rest out;
        - those with such an other label by the whole tree, through the field's sparse index,
          bounded to these labels by the hint and an $in.

        The page is the first max_num_results of both pages merged in the sort order, as the $or
        would give it. The other labels are listed for that once per change to them in any process
        (_getOtherLabels); when they cannot be (past the time limit or 16 MiB), or a condition on the field is not a
        substring one, the second part is every function with other labels.
        """
        max_time_ms = self._getSearchMaxTimeMS()
        # one listing per field for both queries: the names (#76), and the other labels for the $in
        # that bounds a "?" on them to the matching keys of their index (#302)
        known_values = self._list_known_values(full_tree, {"function_name": "functions", OTHER_LABELS_FIELD: "functions"})
        matching_other_labels = self._matchingOtherLabels(full_tree, known_values)
        if matching_other_labels is None:
            apart: Dict[str, Any] = {"$exists": True}
            not_apart: Optional[Dict[str, Any]] = {OTHER_LABELS_FIELD: {"$exists": False}}
        else:
            apart = {"$in": matching_other_labels}
            not_apart = {OTHER_LABELS_FIELD: {"$nin": matching_other_labels}} if matching_other_labels else None
        function_documents: List[Dict] = []
        tree_without_other_labels = _foldOtherLabels(full_tree)
        if tree_without_other_labels is not False:
            conditions = [] if tree_without_other_labels is True else [MongoSearchTranspiler(known_values, pichash_padded=self.isPichashPadded()).visit(tree_without_other_labels)]
            conditions += [not_apart] if not_apart else []
            query = conditions[0] if len(conditions) == 1 else {"$and": conditions} if conditions else {}
            function_documents.extend(self._getDb().functions.find(query, self._FUNCTION_SEARCH_PROJECTION, sort=sort_list, limit=max_num_results, max_time_ms=max_time_ms))
        if matching_other_labels != []:
            # The hint keeps MongoDB on the sparse index, whatever the other conditions would make it
            # try. The few functions it reads are matched by the regex on the names rather than an $in
            # of every name a common term matches, thousands the query would carry for nothing; the
            # other labels are matched among the ones found to match.
            other_labels_listed = known_values.get(OTHER_LABELS_FIELD) if matching_other_labels is None else matching_other_labels
            transpiler = MongoSearchTranspiler({} if other_labels_listed is None else {OTHER_LABELS_FIELD: other_labels_listed}, pichash_padded=self.isPichashPadded())
            query = {"$and": [transpiler.visit(full_tree), {OTHER_LABELS_FIELD: apart}]}
            # only their sort keys at first: a function document is large (100 of them take 2.6 ms
            # to fetch), and the merge keeps few of them as a rule
            key_projection = {"_id": 0, "function_id": 1, **{field: 1 for field, _ in sort_list or []}}
            keys = list(self._getDb().functions.find(query, key_projection, sort=sort_list, limit=max_num_results, max_time_ms=max_time_ms, hint=[(OTHER_LABELS_FIELD, 1)]))
            if keys:
                keyed = {id(key) for key in keys}
                page = _sortedDocuments(function_documents + keys, sort_list)[:max_num_results]
                wanted = [document["function_id"] for document in page if id(document) in keyed]
                fetched = {}
                if wanted:
                    found = self._getDb().functions.find({"function_id": {"$in": wanted}}, self._FUNCTION_SEARCH_PROJECTION, max_time_ms=max_time_ms)
                    fetched = {document["function_id"]: document for document in found}
                function_documents = []
                for document in page:
                    if id(document) in keyed:
                        # a function deleted in between is left out
                        document = fetched.get(document["function_id"])
                    if document is not None:
                        function_documents.append(document)
        return function_documents[:max_num_results]

    def _matchingOtherLabels(self, full_tree: NodeType, known_values: Dict[str, List[Any]]) -> Optional[List[str]]:
        """The listed other labels that the tree's terms on OTHER_LABELS_FIELD may match, a superset of
        what MongoDB's regex matches (MongoSearchTranspiler._may_match); a function whose other labels
        are none of them meets every such condition as a function without the field does. None when
        that cannot be told: a condition on the field that is not a substring one, the labels not
        listed, or more of them than one query takes."""
        conditions = _fieldConditions(full_tree, OTHER_LABELS_FIELD)
        if any(condition.operator not in ("?", "!?") or not isinstance(condition.value, str) for condition in conditions):
            return None
        values = known_values.get(OTHER_LABELS_FIELD)
        if values is None and not _hasSubstringCondition(full_tree, OTHER_LABELS_FIELD, ("?",)):
            # listed for a "?" only (_list_known_values), whose None stands; a "!?" alone lists them here
            values = self._getDistinctValues("functions", OTHER_LABELS_FIELD)
        terms = {condition.value for condition in conditions}
        if values is None or len(values) * len(terms) > MongoSearchTranspiler._MAX_VALUE_TESTS:
            return None
        if len(terms) == 1:
            matching = self._otherLabelsMatchingTerm(next(iter(terms)), values)
        else:
            # a label several terms match is listed once
            matching = list(dict.fromkeys(value for term in sorted(terms) for value in self._otherLabelsMatchingTerm(term, values)))
        if sum(len(value.encode("utf-8")) for value in matching) > self._DISTINCT_VALUES_MAX_BYTES:
            return None
        return matching

    # how many terms' matches a process keeps per listing of the other labels
    _OTHER_LABEL_MATCHES_MAX = 256

    def _otherLabelsMatchingTerm(self, term: str, values: List[Any]) -> List[str]:
        """The other labels among values that a "?" with term may match, kept per term for as long as
        values is the listing _getOtherLabels holds: a search repeating a term reads the counter and
        nothing else. An ASCII term matches the same labels in any case (the regex ignores it, and
        _may_match folds an ASCII term to the same strings), so it is kept lowercase.

        The labels are tested outside the lock, and kept only when values is still the held listing
        then: a thread holding a listing another one has replaced meanwhile keeps nothing."""
        key = term.lower() if term.isascii() else term
        listing = self._other_labels_listing
        if listing is None or listing[1] is not values:
            return self._scanOtherLabelsMatchingTerm(key, values)
        version = listing[0]
        with self._other_label_matches_lock:
            kept = self._other_label_matches.get(key)
            if kept is not None and kept[0] == version:
                self._other_label_matches.move_to_end(key)
                return kept[1]
        matching = self._scanOtherLabelsMatchingTerm(key, values)
        with self._other_label_matches_lock:
            if self._other_labels_listing is listing:
                if self._other_label_matches_version != version:
                    self._other_label_matches.clear()
                    self._other_label_matches_version = version
                self._other_label_matches[key] = (version, matching)
                self._other_label_matches.move_to_end(key)
                while len(self._other_label_matches) > self._OTHER_LABEL_MATCHES_MAX:
                    self._other_label_matches.popitem(last=False)
        return matching

    @staticmethod
    def _scanOtherLabelsMatchingTerm(term: str, values: List[Any]) -> List[str]:
        # matched as the transpiler lists a "?" on the labels (#302): the regex, or _may_match for
        # what MongoDB's case folding may match besides, which for ASCII is the same
        pattern = re.compile(re.escape(term), re.IGNORECASE)
        if term.isascii():
            return [
                value
                for value in values
                if isinstance(value, str) and (pattern.search(value) is not None or (not value.isascii() and MongoSearchTranspiler._may_match(term, value)))
            ]
        return [value for value in values if isinstance(value, str) and (pattern.search(value) is not None or MongoSearchTranspiler._may_match(term, value))]

    def findFunctionByString(self, search_tree: NodeType, cursor: Optional[FullSearchCursor] = None, max_num_results: int = 100) -> Dict[int, "FunctionEntry"]:
        result_dict = {}
        with self._searchTimeLimit():
            # a plain term matches the name and the labels that differ from it (#301); every label is
            # searched with the qualified field "function_labels.function_label"
            full_tree = self._resolve_search_tree(self._PLAIN_FUNCTION_SEARCH_FIELDS, search_tree, cursor)
            sort_list = self._get_sort_list_from_cursor(cursor)
            search_fields = _conditionFields(self._resolve_search_tree(self._PLAIN_FUNCTION_SEARCH_FIELDS, search_tree, None))
            if search_fields == {"function_labels.function_label"} and _hasSubstringCondition(full_tree, "function_labels.function_label", ("?",)):
                function_documents = self._findFunctionsByLabels(full_tree, sort_list, max_num_results)
            elif OTHER_LABELS_FIELD in search_fields:
                function_documents = self._findFunctionsByNameOrOtherLabel(full_tree, sort_list, max_num_results)
            else:
                # any other search is the query it always was: a hint or a listing would cost a
                # condition on another field the plan MongoDB picks for it
                query = self._get_search_query(self._PLAIN_FUNCTION_SEARCH_FIELDS, search_tree, cursor, distinct_fields={"function_name": "functions"})
                function_documents = self._getDb().functions.find(
                    query, self._FUNCTION_SEARCH_PROJECTION, sort=sort_list, limit=max_num_results, max_time_ms=self._getSearchMaxTimeMS()
                )
            for function_document in function_documents:
                self._decodeFunction(function_document)
                entry = FunctionEntry.fromDict(function_document)
                result_dict[entry.function_id] = entry
        return result_dict
