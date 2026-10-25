#!/usr/bin/env python3
import json
import logging
import re
from datetime import UTC, datetime, timedelta
from typing import Dict, List, Optional

from packaging.version import Version
from smda.common.SmdaReport import SmdaReport
from smda.SmdaConfig import SmdaConfig

import mcrit.matchers.MatcherFlags as MatcherFlags
from mcrit.config.McritConfig import McritConfig
from mcrit.config.MinHashConfig import MinHashConfig
from mcrit.config.ShinglerConfig import ShinglerConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MatchingParameters import applyMatchingPreset, resolveMatchingParams
from mcrit.index.SearchCursor import FullSearchCursor, MinimalSearchCursor, sortSpec
from mcrit.index.SearchQueryParser import SearchQueryParser
from mcrit.libs.tags import MAX_TAGS_PER_ENTITY, TagLimitError, normalizeTags
from mcrit.libs.utility import MAX_DECOMPRESSION_RATIO, MIN_IMPORT_DECOMPRESSED_SIZE, compress_encode, compress_encode_fragments, decompress_decode
from mcrit.matchers.MatcherQueryFunction import MatcherQueryFunction
from mcrit.minhash.EscaperFingerprint import FINGERPRINT_UNAVAILABLE, getEscaperFingerprint, getEscaperFingerprints
from mcrit.minhash.MinHash import MinHash
from mcrit.minhash.MinHasher import MinHasher
from mcrit.queue.QueueFactory import QueueFactory
from mcrit.queue.QueueRemoteCalls import QueueRemoteCaller
from mcrit.storage.FamilyEntry import FamilyEntry
from mcrit.storage.FunctionEntry import FunctionEntry
from mcrit.storage.MatchedFunctionEntry import MatchedFunctionEntry
from mcrit.storage.SampleEntry import TIMESTAMP_FORMAT, SampleEntry
from mcrit.storage.StorageFactory import StorageFactory
from mcrit.Worker import Worker

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

# the smda that this process will escape with; recorded alongside the escaper fingerprint so an
# export or a status response says which smda produced the minhashes it describes
SMDA_VERSION = SmdaConfig().VERSION
# Exports newer than this floor (PEP 440 ordering) are eligible when their shingler and MinHash
# configuration hashes match. Raise it only with a change that makes older objects incompatible.
MINIMUM_IMPORT_VERSION = Version("0.0.0")

# A cross compare of n samples makes n child jobs - with sample_group_only each of them reads the
# n-1 other samples - and n*n matrices in its combine step, and its id list was bounded by nothing
# but the length of a URL.
MAX_CROSS_COMPARE_SAMPLES = 1000


#### Helper methods for nested sort_by in search


def _get_field_once(object, field):
    try:
        return getattr(object, field)
    except Exception:
        pass
    try:
        return object[field]
    except Exception:
        pass


def _get_cursor_value(object, field):
    """A sort field's value as a cursor carries it: JSON, and comparable to the stored value. A
    sample's timestamp is a datetime on the entry but stored as text, so the cursor holds the text."""
    value = _get_field(object, field)
    if isinstance(value, datetime):
        return value.strftime(TIMESTAMP_FORMAT)
    return value


def _get_field(object, field):
    dot_index = field.find(".")
    if dot_index >= 0:
        first_field = field[:dot_index]
        remaining_fields = field[dot_index + 1 :]
        return _get_field(_get_field_once(object, first_field), remaining_fields)
    else:
        return _get_field_once(object, field)


class ImportRejected(ValueError):
    """An export addImportData refuses before storing anything of it: one of another version or
    configuration, or one it cannot read. POST /import answers it with 400, while any other error
    of an import is the server's."""


# MongoDB stores an integer as signed 64 bit and fails the insert of a larger one, or of a key that
# holds a NUL, after part of an import is written, where memory storage keeps them; so an import
# holds both to that (#300). Hashes and offsets are unsigned 64-bit values, which MongoDB storage
# writes as hex text and in two's complement respectively.
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
_UINT64_LIMIT = 2**64


def _holdsUnstorable(value) -> bool:
    """Whether value is, or holds in its lists and mappings, an integer beyond signed 64 bit or a key
    with a NUL in it"""
    if isinstance(value, int):
        return not _INT64_MIN <= value <= _INT64_MAX
    if isinstance(value, dict):
        return any("\x00" in key or _holdsUnstorable(item) for key, item in value.items())
    if isinstance(value, list):
        return any(_holdsUnstorable(item) for item in value)
    return False


def _isUint64(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 0 <= value < _UINT64_LIMIT


def _areStorableBlockHashes(picblockhashes) -> bool:
    """Whether picblockhashes is a list of mappings that each hold an unsigned 64-bit hash and offset,
    and nothing else _holdsUnstorable refuses; run per block, so the common case is checked inline"""
    if not isinstance(picblockhashes, list):
        return False
    for block_entry in picblockhashes:
        if not isinstance(block_entry, dict) or "hash" not in block_entry or "offset" not in block_entry:
            return False
        for key, value in block_entry.items():
            is_unsigned = key == "hash" or key == "offset"
            if "\x00" in key:
                return False
            if type(value) is int:
                if not (0 <= value < _UINT64_LIMIT if is_unsigned else _INT64_MIN <= value <= _INT64_MAX):
                    return False
            elif is_unsigned or _holdsUnstorable(value):
                return False
    return True


class MinHashIndex(QueueRemoteCaller(Worker)):
    # a search answers at most this many results per page; limit=-1 asked the storage for limit + 1 = 0
    # results, which MongoDB's find() reads as no limit, so one search could load every function
    MAX_SEARCH_RESULTS = 1000

    def __init__(self, config=None, base_url=None):
        if config is None:
            config = McritConfig()
        self.config = config
        self._storage_config = config.STORAGE_CONFIG
        self._minhash_config = config.MINHASH_CONFIG
        self._shingler_config = config.SHINGLER_CONFIG
        self._queue_config = config.QUEUE_CONFIG
        self.minhasher = MinHasher(self._minhash_config, self._shingler_config)
        self._storage = StorageFactory.getStorage(config)
        self._cleanup_delta = timedelta(seconds=int(self._storage_config.STORAGE_MONGODB_CLEANUP_DELTA))
        # config.QUEUE_CONFIG.QUEUE_METHOD = QueueFactory.QUEUE_METHOD_FAKE
        queue = QueueFactory().getQueue(config, storage=self._storage, consumer_id="index")
        self.search_query_parser = SearchQueryParser()
        super().__init__(queue)

    def _getLastCleanup(self):
        try:
            timestamp = self._storage.getDbCleanupTimestamp()
            if not timestamp:
                timestamp = self._storage.updateDbCleanupTimestamp()
            return timestamp
        except Exception as e:
            LOGGER.error(f"Failed getting last cleanup: {e}, this should no automatically fix itself with the next request.")

    def _cleanupCallback(self):
        if not self._storage_config.STORAGE_MONGODB_ENABLE_CLEANUP:
            return
        last_timestamp = self._getLastCleanup()
        if last_timestamp:
            # the storage stamps the last cleanup in UTC
            now = datetime.now(UTC).replace(tzinfo=None)
            if last_timestamp + self._cleanup_delta < now:
                LOGGER.info("Scheduling a cleanup for query samples.")
                self.doDbCleanup(force_recalculation=True)
                self._storage.updateDbCleanupTimestamp()
        else:
            LOGGER.info("Couldn't determine last db cleanup time, this should no automatically fix itself with the next request.")
            # Should we do cleanup anyway in such cases?

    def _indexCallback(self):
        """called whenever other functionality in MinHashIndex is used, intended for scheduling maintainance jobs etc."""
        self._cleanupCallback()

    #### STORAGE IO ####
    def getStorage(self):
        """Get an interface to the storage"""
        # use this as a callback in all other functions to trigger functionality
        self._indexCallback()
        return self._storage

    def getStorageData(self):
        """Warning: This is intended for local debugging runs - storage may become huge"""
        storage = self.getStorage()
        results = {
            "config": {
                "minhash_config": self._minhash_config.toDict(),
                "shingler_config": self._shingler_config.toDict(),
                "storage_config": self._storage_config.toDict(),
                "queue_config": self._queue_config.toDict(),
            },
            "stats": storage.getStats(),
            "storage": storage.getContent(),
        }
        return results

    def setStorageData(self, storage_data):
        storage = self.getStorage()
        self._minhash_config = MinHashConfig.fromDict(storage_data["config"]["minhash_config"])
        self._shingler_config = ShinglerConfig.fromDict(storage_data["config"]["shingler_config"])
        self._storage_config = StorageConfig.fromDict(storage_data["config"]["storage_config"])
        self._queue_config = StorageConfig.fromDict(storage_data["config"]["queue_config"])
        mcrit_config = McritConfig()
        mcrit_config.MINHASH_CONFIG = self._minhash_config
        mcrit_config.SHINGLER_CONFIG = self._shingler_config
        mcrit_config.STORAGE_CONFIG = self._storage_config
        mcrit_config.QUEUE_CONFIG = self._queue_config
        # reinitialize
        storage = StorageFactory.getStorage(mcrit_config)
        storage.setContent(storage_data["storage"])

    def getExportData(self, sample_ids=None, compress_data=False):
        return self._collectExportData(sample_ids, compress_data, serialize=False)

    def getExportJson(self, sample_ids=None, compress_data=False) -> List[bytes]:
        """json.dumps(getExportData()), in pieces that joined are that text (GET /export sends them as they are).

        Function entries are read from storage one batch at a time and serialised one by one, and every entry's text -
        sample and function entries, family names and actors - is counted against STORAGE_MAX_EXPORT_SIZE before
        it is kept. Only that text is kept, so the export holds what the cap counts plus one batch; a compressed export
        counts a sample's text while it holds it for compression. getExportData() keeps Python objects instead,
        several times the size the cap counts."""
        exported_data = self._collectExportData(sample_ids, compress_data, serialize=True)
        fragments = [b"{"]
        for position, (key, value) in enumerate(exported_data.items()):
            fragments.append(b"%s%s: " % (b", " if position else b"", json.dumps(key).encode()))
            if key not in ("family_actors", "sample_entries", "function_entries"):
                fragments.append(json.dumps(value).encode())
                continue
            # these hold the entries' text fragments already
            fragments.append(b"{")
            for entry_position, (entry_key, entry_fragments) in enumerate(value.items()):
                fragments.extend([b", " if entry_position else b"", json.dumps(str(entry_key)).encode(), b": ", *entry_fragments])
            fragments.append(b"}")
        fragments.append(b"}")
        return fragments

    @staticmethod
    def _serializeExportFunctions(storage, sample_id, compress_data, count):
        """A sample's function entries as JSON text fragments, read and serialised one batch at a time, each counted
        (count) before it is kept. Compressed, the text is counted while it is held and then replaced by the compressed
        text, which is what stays."""
        fragments = [b"{"]
        num_functions = 0
        for batch in storage.getFunctionsBySampleIdInBatches(sample_id):
            for function_entry in batch:
                fragment = b"%s%s: %s" % (b", " if num_functions else b"", json.dumps(str(function_entry.function_id)).encode(), json.dumps(function_entry.toDict()).encode())
                count(len(fragment))
                fragments.append(fragment)
                num_functions += 1
        fragments.append(b"}")
        count(2)
        if compress_data:
            compressed = json.dumps(compress_encode_fragments(fragments)).encode()
            count(len(compressed))
            count(-sum(len(fragment) for fragment in fragments))
            fragments = [compressed]
        return fragments, num_functions

    def _collectExportData(self, sample_ids, compress_data, serialize):
        storage = self.getStorage()
        exported_data = {
            "content": {"is_compressed": compress_data, "num_families": 0, "num_samples": 0, "num_functions": 0},
            # config hashes need to be identical in order for minhashes to be reusable
            "config": {
                "version": self.config.VERSION,
                "shingler": self._shingler_config.getConfigHash(),
                "minhash": self._minhash_config.getConfigHash(),
                # minhashes derive from smda's escaped instructions, so the escaper is an input
                # just like the seeds above - see mcrit/minhash/EscaperFingerprint.py. Persisted
                # per architecture, so widening the probe later cannot invalidate exports that
                # are already in the wild.
                "escaper": getEscaperFingerprints(),
                "smda_version": SMDA_VERSION,
            },
            # family mapping is needed for function_entries as they only contain family_ids
            "family_mapping": {},
            # organize by sha256 to simplify avoiding redundancy during import
            "sample_entries": {},
            # organize by sha256 as well, to easily access functions of a sample
            "function_entries": {},
        }
        family_mapping = {}
        exported_sample_entries = {}
        exported_function_entries = {}
        export_size = 0

        def count(num_bytes):
            # the JSON text of every entry, checked before the entry is kept; serialize keeps only that text, and
            # then the count is what the export holds
            nonlocal export_size
            export_size += num_bytes
            if export_size > self._storage_config.STORAGE_MAX_EXPORT_SIZE:
                raise MemoryError("Export running beyond the allocated maximum, aborting operation.")

        # iterate over sample_ids and transform respective data
        for sample_id in storage.getSampleIds():
            if sample_ids and sample_id not in sample_ids:
                continue
            sample_entry = storage.getSampleById(sample_id)
            if sample_entry.family_id not in family_mapping:
                count(len(json.dumps(sample_entry.family)))
            family_mapping[sample_entry.family_id] = sample_entry.family
            sample_dict = sample_entry.toDict()
            sample_json = json.dumps(sample_dict)
            count(len(sample_json))
            if serialize:
                exported_sample_entries[sample_entry.sha256] = [sample_json.encode()]
                exported_function_entries[sample_entry.sha256], num_functions = self._serializeExportFunctions(storage, sample_id, compress_data, count)
            else:
                function_entries = storage.getFunctionsBySampleId(sample_id) or []
                functions_dict = {function_entry.function_id: function_entry.toDict() for function_entry in function_entries}
                functions_value = compress_encode(json.dumps(functions_dict)) if compress_data else functions_dict
                count(len(json.dumps(functions_value)))
                exported_sample_entries[sample_entry.sha256] = sample_dict
                exported_function_entries[sample_entry.sha256] = functions_value
                num_functions = len(function_entries)
            exported_data["content"]["num_samples"] += 1
            exported_data["content"]["num_functions"] += num_functions
        exported_data["content"]["num_families"] = len(family_mapping)
        exported_data["family_mapping"] = family_mapping
        # the family attributes beyond the name (#57); an importer without this key ignores it
        exported_data["family_actors"] = {}
        # and so are their tags (#53); a sample's and a function's travel in its own entry
        exported_data["family_tags"] = {}
        for family_id in family_mapping:
            family_entry = storage.getFamily(family_id)
            if family_entry is not None and family_entry.actors:
                actors_json = json.dumps(list(family_entry.actors))
                count(len(actors_json))
                exported_data["family_actors"][family_id] = [actors_json.encode()] if serialize else list(family_entry.actors)
            if family_entry is not None and family_entry.tags:
                count(len(json.dumps(list(family_entry.tags))))
                exported_data["family_tags"][family_id] = list(family_entry.tags)
        exported_data["sample_entries"] = exported_sample_entries
        exported_data["function_entries"] = exported_function_entries
        return exported_data

    @staticmethod
    def _statedNumFunctions(sample_entry_dict) -> int:
        """The functions an exported sample entry states it has, if it does: a sample submitted without statistics holds none."""
        statistics = sample_entry_dict.get("statistics")
        num_functions = statistics.get("num_functions") if isinstance(statistics, dict) else None
        return num_functions if isinstance(num_functions, int) and not isinstance(num_functions, bool) and num_functions >= 0 else 0

    def _prepareImportedSamples(self, export_data, present_sha256s, signature_bytes, inflation_budget):
        """Read and check every sample of an export - inflate and parse the content of those not in
        present_sha256s, up to inflation_budget bytes in all, build their sample and function entries
        and check that the storage can hold them - and the families they are filed under, without
        touching the storage. The entries are checked and let go one sample at a time: the write pass
        reads each sample again with _prepareImportedSample, so memory holds one sample's functions,
        not the export's. Returns the family names and the family actors by family_id, a (sha256,
        number of functions) per new sample, and the number of samples and of functions skipped as
        present."""
        # family ids parsed once here, so that the families are written from what was checked
        family_names = {}
        for family_id, family_name in export_data["family_mapping"].items():
            # null is how an export carries the family of a sample reported without one: the default family
            family_name = "" if family_name is None else family_name
            if not isinstance(family_name, str):
                raise ImportRejected(f"export names family {family_id!r} by something other than text")
            # "7" and "07" are one family id, which cannot carry two names
            if family_names.setdefault(int(family_id), family_name) != family_name:
                raise ImportRejected(f"export names family {int(family_id)} twice, differently")
        family_actors = {}
        for family_id, actors in (export_data.get("family_actors") or {}).items():
            if actors is not None and not isinstance(actors, list):
                raise ImportRejected(f"export holds actors of family {family_id!r} as something other than a list")
            # the actors of "7" and "07" are both family 7's, and merged as such
            family_actors.setdefault(int(family_id), []).extend(actors or [])
        num_minhashes_dropped = 0
        num_samples_skipped = 0
        num_functions_skipped = 0
        samples_to_import = []
        for sample_sha256, sample_entry_dict in export_data["sample_entries"].items():
            # read for every sample, present or not: the import report counts the functions, and the
            # escaper comparison collects and sorts the architectures. None, or none at all, is an
            # architecture SMDA could not tell, as older versions stored it
            # only reported back: the family statistics count the functions imported, not this claim (#265)
            num_functions = self._statedNumFunctions(sample_entry_dict)
            if sample_entry_dict.get("architecture") is not None and not isinstance(sample_entry_dict["architecture"], str):
                raise ImportRejected(f"sample {sample_sha256} states its architecture as something other than text")
            # skip samples that are present already
            if sample_sha256 in present_sha256s:
                LOGGER.info(f"Sample with SHA256 {sample_sha256} already present in database, skipping...")
                num_samples_skipped += 1
                num_functions_skipped += num_functions
                continue
            _, _, num_dropped, num_inflated = self._prepareImportedSample(export_data, sample_sha256, family_names, signature_bytes, inflation_budget)
            inflation_budget -= num_inflated
            num_minhashes_dropped += num_dropped
            samples_to_import.append((sample_sha256, num_functions))
        if num_minhashes_dropped:
            LOGGER.warning("Dropped %d imported minhashes that are not %d bytes long, the next minhash repair recalculates them.", num_minhashes_dropped, signature_bytes)
        return family_names, family_actors, samples_to_import, num_samples_skipped, num_functions_skipped

    def _prepareImportedSample(self, export_data, sample_sha256, family_names, signature_bytes, inflation_budget):
        """Inflate and parse the content of one sample of an export, up to inflation_budget bytes, build
        its sample and function entries and check that the storage can hold them; the entries keep the
        export's family_id. Returns the sample entry, the function entries, the number of minhashes
        dropped for their length and the number of bytes the content inflated to."""
        sample_entry_dict = export_data["sample_entries"][sample_sha256]
        # exported as a bool; the family statistics count it as 0 or 1
        if sample_entry_dict["is_library"] not in (True, False):
            raise ImportRejected(f"sample {sample_sha256} states whether it is a library as something other than true or false")
        function_entry_dicts = export_data["function_entries"].get(sample_sha256)
        # decompress function_entries if necessary
        num_inflated = 0
        if function_entry_dicts is not None and export_data["content"]["is_compressed"]:
            inflated = decompress_decode(function_entry_dicts, max_size=inflation_budget)
            num_inflated = len(inflated)
            function_entry_dicts = json.loads(inflated)
            del inflated
        sample_entry = SampleEntry.fromDict(sample_entry_dict)
        # an export is data from elsewhere: a tag this instance would not accept is dropped (#53)
        sample_entry.tags = normalizeTags(sample_entry.tags, drop_invalid=True)[:MAX_TAGS_PER_ENTITY]
        if sample_entry.family_id not in family_names:
            raise ImportRejected(f"sample {sample_sha256} names family {sample_entry.family_id}, which the export does not hold")
        # the fields stored as they are read; family_id and sample_id are this instance's, and the
        # base address is written in two's complement
        if isinstance(sample_entry.base_addr, int) and not 0 <= sample_entry.base_addr < _UINT64_LIMIT:
            raise ImportRejected(f"sample {sample_sha256} states a base address beyond 64 bit")
        for value in (
            sample_entry.architecture,
            sample_entry.binary_size,
            sample_entry.binweight,
            sample_entry.bitness,
            sample_entry.component,
            sample_entry.family,
            sample_entry.filename,
            sample_entry.smda_version,
            sample_entry.statistics,
            sample_entry.version,
        ):
            if _holdsUnstorable(value):
                raise ImportRejected(f"sample {sample_sha256} holds a number beyond 64 bit or a key with a NUL")
        # a sample without a family is stored with "" as local ones are, not with null
        if sample_entry.family is None:
            sample_entry.family = family_names[sample_entry.family_id]
        num_minhashes_dropped = 0
        function_entries = []
        for function_entry_dict in (function_entry_dicts or {}).values():
            function_entry = FunctionEntry.fromDict(function_entry_dict)
            function_entry.tags = normalizeTags(function_entry.tags, drop_invalid=True)[:MAX_TAGS_PER_ENTITY]
            # the storage keys its pichash index by it
            # null as FunctionEntry stores a function without one
            if function_entry.pichash is None:
                function_entry.pichash = 0
            if not _isUint64(function_entry.pichash):
                raise ImportRejected(f"sample {sample_sha256} holds a function whose pichash is no 64-bit number")
            if not _areStorableBlockHashes(function_entry.picblockhashes):
                raise ImportRejected(f"sample {sample_sha256} holds a function whose picblockhashes are no list of 64-bit hashes and offsets")
            if isinstance(function_entry.offset, int) and not 0 <= function_entry.offset < _UINT64_LIMIT:
                raise ImportRejected(f"sample {sample_sha256} holds a function whose offset is beyond 64 bit")
            # the other fields stored as they are read; MongoDB storage writes the disassembly as JSON text
            for value in (
                function_entry.architecture,
                function_entry.binweight,
                function_entry.function_name,
                function_entry.matches,
                function_entry.minhash_shingle_composition,
                function_entry.num_blocks,
                function_entry.num_instructions,
            ):
                if _holdsUnstorable(value):
                    raise ImportRejected(f"sample {sample_sha256} holds a function with a number beyond 64 bit or a key with a NUL")
            for label in function_entry.function_labels:
                if _holdsUnstorable(label.function_label) or _holdsUnstorable(label.username):
                    raise ImportRejected(f"sample {sample_sha256} holds a function label with a number beyond 64 bit or a key with a NUL")
            if function_entry.minhash and len(function_entry.minhash) != signature_bytes:
                function_entry.minhash = b""
                function_entry.minhash_shingle_composition = {}
                num_minhashes_dropped += 1
            function_entries.append(function_entry)
        return sample_entry, function_entries, num_minhashes_dropped, num_inflated

    def addImportData(self, export_data, import_size=None):
        """Store an export, all of it or, should storing fail, nothing (#299). import_size is the size
        of the export as it was sent, which bounds what its content may inflate to; without it, the
        size of its compressed content stands in."""
        storage = self.getStorage()
        import_report = {
            "num_samples_imported": 0,
            "num_samples_skipped": 0,
            "num_functions_imported": 0,
            "num_functions_skipped": 0,
            "num_families_imported": 0,
            "num_families_skipped": 0,
            "escaper_mismatch": False,
        }
        try:
            export_version = Version(export_data["config"]["version"])
        # InvalidVersion is a ValueError, and so is the error for a number of more than 4300 digits
        except (ValueError, TypeError, KeyError) as error:
            raise ImportRejected("export has no valid version") from error
        if export_version <= MINIMUM_IMPORT_VERSION:
            raise ImportRejected(f"export version {export_version} is older than this instance imports")
        if export_data["config"].get("shingler") != self._shingler_config.getConfigHash():
            raise ImportRejected("the export's shingler configuration differs from this instance's")
        if export_data["config"].get("minhash") != self._minhash_config.getConfigHash():
            raise ImportRejected("the export's MinHash configuration differs from this instance's")
        for section in ("content", "family_mapping", "sample_entries", "function_entries"):
            if not isinstance(export_data.get(section), dict):
                raise ImportRejected(f"export has no {section} mapping")
        # imported minhashes are stored and banded as they are, so each has to be one this instance
        # could have computed: one of another length is banded beside the code it was copied from
        # and then fails every matching job that draws it as a candidate, or does not fit the band
        # projection at all. It is dropped rather than failing the import - imported samples record
        # no minhash version, so the next minhash repair rehashes them from their disassembly
        signature_bytes = self._minhash_config.MINHASH_SIGNATURE_LENGTH * (1 if self._minhash_config.MINHASH_SIGNATURE_BITS <= 8 else 4)
        if import_size is None:
            import_size = sum(len(content) for content in export_data["function_entries"].values() if isinstance(content, str))
        inflation_budget = max(MAX_DECOMPRESSION_RATIO * import_size, MIN_IMPORT_DECOMPRESSED_SIZE)
        # looked up apart from reading the export, so that an error of the storage is not taken for one of the export
        present_sha256s = storage.getPresentSha256s(export_data["sample_entries"])
        # a first pass reads everything the export holds - inflates and parses each sample's content,
        # builds its sample and function entries - before anything is stored, so that an import that
        # is refused or malformed stores nothing, not the samples before the one that failed. It keeps
        # none of the entries; the write pass below reads each sample once more
        try:
            family_names, family_actors, samples_to_import, num_samples_skipped, num_functions_skipped = self._prepareImportedSamples(
                export_data, present_sha256s, signature_bytes, inflation_budget
            )
        except ImportRejected:
            raise
        except (KeyError, TypeError, ValueError, AttributeError, RecursionError) as error:
            # nothing is stored yet: whatever the export lacks or holds in another shape is its fault,
            # content nested too deeply to parse included. Catching this broadly holds only as long as
            # the pass makes no storage calls - an error of the storage would be taken for the export's
            raise ImportRejected(f"export is malformed ({type(error).__name__}: {error})") from error
        import_report["num_samples_skipped"] += num_samples_skipped
        import_report["num_functions_skipped"] += num_functions_skipped
        # An escaper mismatch does not make the data unusable - most functions are unaffected by a
        # given escaper change - so this warns instead of refusing. The imported minhashes were
        # computed from a different escaped representation than this instance produces, so matching
        # between imported and locally indexed functions will under-report for the affected share.
        # The escaper field is a per-architecture mapping (see mcrit/minhash/EscaperFingerprint.py);
        # comparing the intersection of keys means adding probe architectures later cannot make
        # older exports mismatch on architectures they do not carry.
        exported_escapers = export_data["config"].get("escaper")
        local_escapers = getEscaperFingerprints()
        if exported_escapers is None:
            # legacy exports carry no escaper field at all - nothing to compare
            pass
        elif not isinstance(exported_escapers, dict):
            LOGGER.warning(
                "Unexpected escaper provenance format in export (%r), cannot compare. Imported minhashes may or may not share this instance's escaping behaviour.",
                exported_escapers,
            )
        else:
            shared_architectures = set(local_escapers).intersection(exported_escapers)
            # only the architectures the export holds samples of: a change in how smda escapes
            # another one leaves these minhashes as comparable as before
            exported_samples = [sample_entry for sample_entry in export_data.get("sample_entries", {}).values() if isinstance(sample_entry, dict)]
            exported_architectures = {sample_entry.get("architecture") for sample_entry in exported_samples if sample_entry.get("architecture")}
            if exported_samples:
                # samples of an unknown architecture only (SMDA could not disassemble them) hold no
                # minhashes any escaper produced, so there is then nothing to compare at all
                shared_architectures &= exported_architectures
            shared_architectures = sorted(shared_architectures)
            if exported_samples and not exported_architectures:
                pass
            elif not shared_architectures:
                LOGGER.warning(
                    "Export carries no escaper fingerprint for the architectures of its samples (samples: %s, export fingerprints: %s, local: %s). Imported minhashes may or may not share this instance's escaping behaviour.",
                    sorted(exported_architectures) or "unknown",
                    sorted(exported_escapers),
                    sorted(local_escapers),
                )
            else:
                incomparable = [architecture for architecture in shared_architectures if FINGERPRINT_UNAVAILABLE in (exported_escapers[architecture], local_escapers[architecture])]
                if incomparable:
                    # different from a legacy export: provenance is knowable in principle but not
                    # here, so say why the comparison was skipped rather than passing silently
                    LOGGER.warning(
                        "Cannot compare escaper provenance for %s: fingerprint unavailable on export or locally.",
                        incomparable,
                    )
                mismatched = [
                    architecture
                    for architecture in shared_architectures
                    if FINGERPRINT_UNAVAILABLE not in (exported_escapers[architecture], local_escapers[architecture])
                    and exported_escapers[architecture] != local_escapers[architecture]
                ]
                if mismatched:
                    import_report["escaper_mismatch"] = True
                    unchanged = [architecture for architecture in shared_architectures if architecture not in mismatched]
                    LOGGER.warning(
                        "Importing data escaped by a different smda for %s: export %s (smda %s), this instance %s (smda %s). "
                        "Imported minhashes are not fully comparable to locally computed ones%s; consider recalculating minhashes.",
                        mismatched,
                        {architecture: exported_escapers[architecture] for architecture in mismatched},
                        export_data["config"].get("smda_version", "unknown"),
                        {architecture: local_escapers[architecture] for architecture in mismatched},
                        SMDA_VERSION,
                        "" if not unchanged else " (unchanged for %s)" % unchanged,
                    )
        # families are added only now, so that a refused export does not leave its families behind
        # create a dictionary for pointing family_ids as contained in the export to family_ids as used in this instance
        family_id_remapping = {}
        created_family_ids = []
        index = 0
        num_functions_added = 0
        # the minhashes are banded once every function is stored, as before; until then only their
        # function ids and bytes are kept, about 100 bytes per function, a small part of its entry
        minhashes_to_import = []
        imported_sample_entries = []
        # everything written from here on is removed again should a write fail - a function insert
        # MongoDB refuses, say. Each sample is stored with its functions before the next is read, so a
        # failure at any sample leaves the ones before it stored, counted and indexed, which a
        # corrected import would skip as present, and families left without samples would stay behind
        # empty: discardImportedSample and discardImportedFamily take all of them back
        try:
            for exported_family_id, family_name in family_names.items():
                remapped_family_id, is_created = storage.ensureFamily(family_name)
                if is_created:
                    import_report["num_families_imported"] += 1
                    created_family_ids.append(remapped_family_id)
                else:
                    import_report["num_families_skipped"] += 1
                family_id_remapping[exported_family_id] = remapped_family_id
            LOGGER.info("Family remapping created: %d families, %d samples.", len(family_id_remapping), len(export_data["sample_entries"]))
            for sample_sha256, num_functions in samples_to_import:
                index += 1
                # read again, as the first pass did: it kept no more than the sha256 to bound memory. The
                # same content inflates to the same size, so this pass draws on a budget of its own, as
                # large as the first pass's, and cannot exceed it where the first did not
                sample_entry, function_entries, _, num_inflated = self._prepareImportedSample(export_data, sample_sha256, family_names, signature_bytes, inflation_budget)
                inflation_budget -= num_inflated
                sample_entry.family_id = family_id_remapping[sample_entry.family_id]
                # add sample_entry to storage and receive new sample_id; None when its sha256 is known
                # here after all - stated apart from its key, or added by another writer since the check
                # above. Its functions must not follow: they would land on the sample_id the export
                # carries, and as it is not this import's, it is not removed should the import fail later
                remapped_sample_entry = storage.importSampleEntry(sample_entry) if sample_entry.sha256 == sample_sha256 else None
                if remapped_sample_entry is None:
                    LOGGER.warning("Sample with SHA256 %s was not added (entry states SHA256 %r), skipping it and its functions...", sample_sha256, sample_entry.sha256)
                    import_report["num_samples_skipped"] += 1
                    import_report["num_functions_skipped"] += num_functions
                    del sample_entry, function_entries
                    continue
                import_report["num_samples_imported"] += 1
                imported_sample_entries.append(remapped_sample_entry)
                # adjusting family_id and sample_id of the functions
                function_entry = None
                for function_entry in function_entries:
                    function_entry.sample_id = remapped_sample_entry.sample_id
                    function_entry.family_id = remapped_sample_entry.family_id
                import_report["num_functions_imported"] += len(function_entries)
                adjusted_function_entries = storage.importFunctionEntries(function_entries) if function_entries else []
                num_functions_added += len(adjusted_function_entries)
                # we can only index once our imported function_entries had their function_id adjusted to our DB
                hashed = [function_entry for function_entry in adjusted_function_entries if function_entry.minhash]
                if hashed:
                    minhashes_to_import.append(([function_entry.function_id for function_entry in hashed], [function_entry.minhash for function_entry in hashed]))
                LOGGER.info("Sample %d with SHA256 %s added...", index, sample_sha256)
                # let go of this sample's entries before the next is read, and the last one's before banding
                del sample_entry, function_entries, function_entry, adjusted_function_entries, hashed
            LOGGER.info(f"{num_functions_added} functions added")
            # ensure that their minhashes / pichashes are added to the respective indices. Delaying minhash
            # insertion speeds up the procedure dramatically, and so does doing it in one call: a band hash
            # shared across samples is one update for all of them, where banding sample by sample took 2.5
            # times as long. It is the one step that still grows with the export, by 1.1-1.4 kB a function.
            # Should it fail, the samples are taken back with whatever band entries were written
            minhashes = [
                MinHash(function_id, minhash, minhash_bits=self._minhash_config.MINHASH_SIGNATURE_BITS)
                for function_ids, minhash_bytes in minhashes_to_import
                for function_id, minhash in zip(function_ids, minhash_bytes)
            ]
            del minhashes_to_import
            # rebuilt from their bytes, these carry no shingle composition: the one each function was
            # stored with above stays, where writing theirs would empty it
            storage.addMinHashes(minhashes, keep_composition=True)
            LOGGER.info(f"{len(minhashes)} Minhashes added")
        except Exception:
            LOGGER.error("Import failed, removing the %d samples and %d families it had added.", len(imported_sample_entries), len(created_family_ids))
            # one that cannot be removed must not hide why the import failed
            for imported_sample_entry in imported_sample_entries:
                try:
                    storage.discardImportedSample(imported_sample_entry)
                except Exception:
                    LOGGER.exception("Could not remove sample %s of the failed import.", imported_sample_entry.sha256)
            # only while no sample is filed under one, this import's or another writer's
            for family_id in created_family_ids:
                try:
                    storage.discardImportedFamily(family_id)
                except Exception:
                    LOGGER.exception("Could not remove family %d of the failed import.", family_id)
            raise
        # actors of imported families are merged into what this instance already knows (#57), once the
        # import is complete: they are metadata on top of it, so one that cannot be stored is logged and
        # the import still succeeds - a failure here neither undoes it nor leaves it half done
        try:
            for exported_family_id, actors in family_actors.items():
                remapped_family_id = family_id_remapping.get(exported_family_id)
                local_family = storage.getFamily(remapped_family_id) if remapped_family_id is not None else None
                # held to what the API accepts: an export is data from elsewhere
                valid_actors = [actor for actor in actors if FamilyEntry.isValidActor(actor)]
                if len(valid_actors) != len(actors):
                    LOGGER.warning("Dropping %d invalid actor name(s) of imported family %s.", len(actors) - len(valid_actors), exported_family_id)
                actors = FamilyEntry.normalizeActors(valid_actors)
                if local_family is not None and actors:
                    # and to the API's cap, as tags are below: the first that fit are added
                    merged, num_dropped = FamilyEntry.mergeActors(local_family.actors, actors)
                    if num_dropped:
                        LOGGER.warning(
                            "Family %d would be attributed to more than %d actors, dropping %d imported actors.", remapped_family_id, FamilyEntry.MAX_ACTORS, num_dropped
                        )
                    if merged != local_family.actors:
                        storage.modifyFamily(remapped_family_id, {"actors": merged})
        except Exception:
            LOGGER.exception("Could not merge the actors of the imported families, the import itself is complete.")
        # tags of imported families likewise (#53). An export is data from elsewhere, so a tag
        # this instance would not accept is dropped rather than failing the whole import, and so
        # are those that would take a family past MAX_TAGS_PER_ENTITY: the first that fit are added
        try:
            family_tags = export_data.get("family_tags") or {}
            for exported_family_id, tags in family_tags.items() if isinstance(family_tags, dict) else []:
                remapped_family_id = family_id_remapping.get(int(exported_family_id))
                local_family = storage.getFamily(remapped_family_id) if remapped_family_id is not None else None
                tags = normalizeTags(tags, drop_invalid=True)
                if local_family is None or not tags:
                    continue
                known_tags = set(local_family.tags)
                new_tags = [tag for tag in tags if tag not in known_tags]
                fitting_tags = new_tags[: max(0, MAX_TAGS_PER_ENTITY - len(local_family.tags))]
                if len(fitting_tags) < len(new_tags):
                    LOGGER.warning(
                        "Family %d would carry more than %d tags, dropping %d imported tags.", remapped_family_id, MAX_TAGS_PER_ENTITY, len(new_tags) - len(fitting_tags)
                    )
                if fitting_tags:
                    try:
                        storage.addTags("family", remapped_family_id, fitting_tags)
                    except TagLimitError:
                        LOGGER.warning("Family %d was tagged meanwhile and reached %d tags, dropping its imported tags.", remapped_family_id, MAX_TAGS_PER_ENTITY)
        except Exception:
            LOGGER.exception("Could not merge the tags of the imported families, the import itself is complete.")
        return import_report

    def respawn(self):
        """Get an interface to the storage"""
        storage = self.getStorage()
        self.queue.clear()
        return storage.clearStorage()

    #### REDIRECTED TO WORKER ####
    """
    def updateMinHashes(self, function_ids, finish_samples=False):
    def rebuildIndex(self):
    def recomputeFamilyStats(self):
    def deleteOrphanedQueueFiles(self, dry_run=False):
    def recalculatePicHashes(self):
    def recalculateMinHashes(self):
    def repairMinHashes(self):
    def getMatchesForReport(self, report):
    def getMatchesForSmdaReport(self, report_json, minhash_threshold=None):
    def getMatchesForMappedBinary(self, binary, base_address, minhash_threshold=None):
    def getMatchesForUnmappedBinary(self, binary, minhash_threshold=None):
    def getMatchesForSample(self, sample_id, minhash_threshold=None):
    def getMatchesForSampleVs(self, sample_id, other_sample_id, minhash_threshold=None):
    def getMatchesForSampleVsGroup(self, sample_id, other_sample_ids, minhash_threshold=None):
    def getAggregatedMatchesForSample(self, sample_id, minhash_threshold=None):
    def getUniqueBlocks(self, sample_ids):
    def addBinarySample(self, binary, is_dump, bitness, base_address,):
    # these need to be jobs to ensure database consistency
    def deleteSample(self, sample_id):
    def deleteFamily(self, family_id, keep_samples=False):
    def modifyFamily(self, family_id, update_information):
    def modifySample(self, sample_id, update_information):
    """

    #### NOT REDIRECTED ####
    def addReport(self, smda_report, calculate_hashes=True, calculate_matches=False, username=None):
        # checked before the lookup, where MongoDB would read an object as a query operator
        if not SampleEntry.isValidSha256(smda_report.sha256):
            raise ValueError("the report's sha256 is not 64 hex digits")
        for smda_function in smda_report.getFunctions():
            # None is a function without a PicHash (stored as 0); anything else is stored through hex()
            pic_hash = smda_function.pic_hash
            if pic_hash is not None and (not isinstance(pic_hash, int) or isinstance(pic_hash, bool) or not 0 <= pic_hash < 2**64):
                raise ValueError(f"the pic_hash of function at {smda_function.offset} is not an unsigned 64-bit integer")
        storage = self.getStorage()
        sample_entry = storage.getSampleBySha256(smda_report.sha256)
        if sample_entry:
            # the sample may well be someone else's: its labels are held to what a rename could set
            self._storage.updateFunctionLabels(smda_report, username, checked=True)
            return {"existed": True, "sample_info": sample_entry.toDict()}
        sample_entry = storage.addSmdaReport(smda_report)
        if not sample_entry:
            return None
        LOGGER.info("Added %s", sample_entry)
        # ensure that original function_names are also added as labels
        storage.updateFunctionLabels(smda_report, username)
        function_entries = storage.getFunctionsBySampleId(sample_entry.sample_id)
        LOGGER.info("Added %d function entries.", len(function_entries))
        job_id = None
        if calculate_hashes:
            job_id = self.updateMinHashesForSample(sample_entry.sample_id, username=username)
        return {"existed": False, "sample_info": sample_entry.toDict(), "job_id": job_id}

    def addReportJson(self, report_json, calculate_hashes=True, calculate_matches=False, username=None):
        report = SmdaReport.fromDict(report_json)
        return self.addReport(report, calculate_hashes=calculate_hashes, calculate_matches=calculate_matches, username=username)

    def addReportFile(self, report_filepath, calculate_hashes=True, calculate_matches=False):
        with open(report_filepath) as fin:
            report_json = json.load(fin)
        report = SmdaReport.fromDict(report_json)
        return self.addReport(report, calculate_hashes=calculate_hashes, calculate_matches=calculate_matches)

    def modifyFunction(self, function_id: int, update_information: dict, username: Optional[str] = None) -> bool:
        # unlike modifySample/modifyFamily this touches one document and no statistics, so it
        # is answered synchronously instead of as a job (fkie-cad/mcritweb#72)
        return self.getStorage().modifyFunction(function_id, update_information, username=username)

    # tags (#53) touch one document and no statistics, so like modifyFunction they are answered
    # synchronously rather than as a job
    def addTags(self, entity: str, entity_id: int, tags: List[str]) -> Optional[List[str]]:
        return self.getStorage().addTags(entity, entity_id, tags)

    def removeTags(self, entity: str, entity_id: int, tags: List[str]) -> Optional[List[str]]:
        return self.getStorage().removeTags(entity, entity_id, tags)

    def getTagCounts(self, entity: str) -> Dict[str, int]:
        return self.getStorage().getTagCounts(entity)

    #### MATCHING JOBS ####
    # Each submits its job with every matching knob resolved to the value the job runs with, so
    # the job's arguments - its cache key - hold those values rather than the absence of the ones
    # a caller left out (#217). The server resolves them as well; resolving again is a no-op.

    def _submitMatchingJob(self, method_name, job_args, knobs, job_options, with_shortlist=True):
        # None is "not set", as everywhere else, and passes
        if not with_shortlist and any(job_options.get(name) is not None for name in ("shortlist_size", "shortlist_unavailable")):
            # refused here rather than stored as an argument the worker method does not take,
            # which would fail the job on every attempt
            raise TypeError(f"{method_name} takes no shortlist: it is restricted to the samples it names.")
        if not with_shortlist:
            job_options = {name: value for name, value in job_options.items() if name not in ("shortlist_size", "shortlist_unavailable")}
        # a preset is expanded into knob values, never stored: the job is keyed on what it runs with
        preset = job_options.pop("preset", None)
        if preset is not None:
            knobs = applyMatchingPreset(knobs, preset, with_shortlist=with_shortlist, config=self.config)
        resolved = resolveMatchingParams(knobs, self.config, storage=self._storage, with_shortlist=with_shortlist)
        return getattr(super(), method_name)(*job_args, **resolved, **job_options)

    def getMatchesForSample(
        self, sample_id, minhash_threshold=None, pichash_size=None, band_matches_required=None, shortlist_size=None, band_df_cutoff=None, shortlist_unavailable=None, **job_options
    ):
        knobs = dict(
            minhash_threshold=minhash_threshold,
            pichash_size=pichash_size,
            band_matches_required=band_matches_required,
            shortlist_size=shortlist_size,
            band_df_cutoff=band_df_cutoff,
            shortlist_unavailable=shortlist_unavailable,
        )
        return self._submitMatchingJob("getMatchesForSample", (sample_id,), knobs, job_options)

    def getMatchesForSmdaReport(
        self,
        report_json,
        minhash_threshold=None,
        pichash_size=None,
        band_matches_required=None,
        shortlist_size=None,
        band_df_cutoff=None,
        shortlist_unavailable=None,
        **job_options,
    ):
        knobs = dict(
            minhash_threshold=minhash_threshold,
            pichash_size=pichash_size,
            band_matches_required=band_matches_required,
            shortlist_size=shortlist_size,
            band_df_cutoff=band_df_cutoff,
            shortlist_unavailable=shortlist_unavailable,
        )
        return self._submitMatchingJob("getMatchesForSmdaReport", (report_json,), knobs, job_options)

    def getMatchesForMappedBinary(
        self,
        binary,
        base_address,
        minhash_threshold=None,
        pichash_size=None,
        band_matches_required=None,
        shortlist_size=None,
        band_df_cutoff=None,
        shortlist_unavailable=None,
        **job_options,
    ):
        knobs = dict(
            minhash_threshold=minhash_threshold,
            pichash_size=pichash_size,
            band_matches_required=band_matches_required,
            shortlist_size=shortlist_size,
            band_df_cutoff=band_df_cutoff,
            shortlist_unavailable=shortlist_unavailable,
        )
        return self._submitMatchingJob("getMatchesForMappedBinary", (binary, base_address), knobs, job_options)

    def getMatchesForUnmappedBinary(
        self, binary, minhash_threshold=None, pichash_size=None, band_matches_required=None, shortlist_size=None, band_df_cutoff=None, shortlist_unavailable=None, **job_options
    ):
        knobs = dict(
            minhash_threshold=minhash_threshold,
            pichash_size=pichash_size,
            band_matches_required=band_matches_required,
            shortlist_size=shortlist_size,
            band_df_cutoff=band_df_cutoff,
            shortlist_unavailable=shortlist_unavailable,
        )
        return self._submitMatchingJob("getMatchesForUnmappedBinary", (binary,), knobs, job_options)

    def getMatchesForSampleVs(self, sample_id, other_sample_id, minhash_threshold=None, pichash_size=None, band_matches_required=None, band_df_cutoff=None, **job_options):
        # restricted to the sample it names: no shortlist
        knobs = dict(minhash_threshold=minhash_threshold, pichash_size=pichash_size, band_matches_required=band_matches_required, band_df_cutoff=band_df_cutoff)
        return self._submitMatchingJob("getMatchesForSampleVs", (sample_id, other_sample_id), knobs, job_options, with_shortlist=False)

    def getMatchesForSampleVsGroup(self, sample_id, other_sample_ids, minhash_threshold=None, pichash_size=None, band_matches_required=None, band_df_cutoff=None, **job_options):
        # restricted to the samples it names: no shortlist
        knobs = dict(minhash_threshold=minhash_threshold, pichash_size=pichash_size, band_matches_required=band_matches_required, band_df_cutoff=band_df_cutoff)
        return self._submitMatchingJob("getMatchesForSampleVsGroup", (sample_id, other_sample_ids), knobs, job_options, with_shortlist=False)

    def getMatchesCross(self, sample_ids: List[int], sample_group_only=False, force_recalculation=False, username=None, **params):
        sample_to_job_id = {}
        # a cross compare reads the named samples out of each child's report, so a shortlist -
        # ranked over the whole corpus - could only drop some of them: vs-group children take
        # none, and 1-vs-corpus children are asked for none rather than the configured one. One a
        # caller asks for is refused, as on the vs routes, rather than quietly dropped
        if params.get("shortlist_size") is not None:
            raise TypeError("getMatchesCross takes no shortlist: it reads the samples it names out of each report.")
        params.pop("shortlist_size", None)
        params.pop("shortlist_unavailable", None)
        if not sample_group_only:
            params["shortlist_size"] = 0
        # a sample named twice is compared once: each vs-group child loads every other sample named,
        # so a repeated id made it load that sample's functions once per repetition
        sample_ids = list(dict.fromkeys(sample_ids))
        if len(sample_ids) > MAX_CROSS_COMPARE_SAMPLES:
            raise ValueError(f"A cross compare takes at most {MAX_CROSS_COMPARE_SAMPLES} samples.")
        for id in sample_ids:
            if sample_group_only:
                job_id = self.getMatchesForSampleVsGroup(id, [sid for sid in sample_ids if sid != id], force_recalculation=force_recalculation, username=username, **params)
            else:
                job_id = self.getMatchesForSample(id, force_recalculation=force_recalculation, username=username, **params)
            sample_to_job_id[id] = job_id
        return self.combineMatchesToCross(sample_to_job_id, await_jobs=[*sample_to_job_id.values()], force_recalculation=force_recalculation, username=username)

    def getMatchesForSmdaFunction(
        self,
        smda_report_with_function: SmdaReport,
        minhash_threshold=None,
        pichash_size=None,
        band_matches_required=None,
        exclude_self_matches=False,
        shortlist_size=None,
        band_df_cutoff=None,
        shortlist_unavailable=None,
        force_recalculation=False,
        preset=None,
    ):
        # force_recalculation is accepted for the query parameter's sake and has nothing to do:
        # a function query runs in the request and is never cached
        if preset is not None:
            names = ("minhash_threshold", "pichash_size", "band_matches_required", "shortlist_size", "band_df_cutoff")
            knobs = applyMatchingPreset(dict(zip(names, (minhash_threshold, pichash_size, band_matches_required, shortlist_size, band_df_cutoff))), preset, config=self.config)
            minhash_threshold, pichash_size, band_matches_required, shortlist_size, band_df_cutoff = (knobs[name] for name in names)
        # convert function to FunctionEntry
        smda_report = SmdaReport.fromDict(smda_report_with_function)
        assert smda_report is not None and smda_report.xcfg is not None and smda_report.sha256 is not None
        function_offset = None
        if len(smda_report.xcfg) != 1:
            raise ValueError("SmdaReport has to contain exactly one function.")
        function_offset = int([k for k in smda_report.xcfg.keys()][0])
        matcher = MatcherQueryFunction(
            self,
            minhash_threshold=minhash_threshold,
            pichash_size=pichash_size,
            band_matches_required=band_matches_required,
            exclude_self_matches=exclude_self_matches,
            shortlist_size=shortlist_size,
            band_df_cutoff=band_df_cutoff,
            shortlist_unavailable=shortlist_unavailable,
        )
        # run Matcher for a single function
        match_report = matcher.getMatchesForSmdaFunction(smda_report)
        function_identifier = f"{smda_report.sha256[:8]}@0x{function_offset:x}"
        match_report["info"]["job"]["parameters"] = f"getMatchesForSmdaFunction({function_identifier})"
        # return completed MatchingReport
        return match_report

    def getMatchesFunctionVs(self, function_id_a: int, function_id_b: int):
        storage = self.getStorage()
        function_entry_a = storage.getFunctionById(function_id_a, with_xcfg=True)
        function_entry_b = storage.getFunctionById(function_id_b, with_xcfg=True)
        if function_entry_a is None or function_entry_b is None:
            return
        sample_entry_a = storage.getSampleById(function_entry_a.sample_id)
        sample_entry_b = storage.getSampleById(function_entry_b.sample_id)
        if sample_entry_a is None or sample_entry_b is None:
            # a function whose sample document is gone (#270) cannot be reported, as in matching
            return
        minhash_a = function_entry_a.getMinHash(minhash_bits=self._minhash_config.MINHASH_SIGNATURE_BITS)
        minhash_b = function_entry_b.getMinHash(minhash_bits=self._minhash_config.MINHASH_SIGNATURE_BITS)

        score = None
        if minhash_a.minhash and minhash_b.minhash:
            score = MinHash.calculateMinHashScore(minhash_a.minhash, minhash_b.minhash, minhash_bits=self._minhash_config.MINHASH_SIGNATURE_BITS)
        match_flags = 0
        match_flags += MatcherFlags.IS_MINHASH_FLAG if score is not None and score >= self._minhash_config.MINHASH_MATCHING_THRESHOLD else 0
        # a PicHash of 0 is a function without one, not a hash two functions can share
        match_flags += MatcherFlags.IS_PICHASH_FLAG if function_entry_a.pichash and function_entry_a.pichash == function_entry_b.pichash else 0
        match_flags += MatcherFlags.IS_LIBRARY_FLAG if sample_entry_b.is_library else 0
        match_tuple = [function_entry_b.family_id, function_entry_b.sample_id, function_entry_b.function_id, score, match_flags]
        result = {
            "function_entry_a": function_entry_a.toDict(),
            "function_entry_b": function_entry_b.toDict(),
            "sample_entry_a": sample_entry_a.toDict(),
            "sample_entry_b": sample_entry_b.toDict(),
            "match_entry": MatchedFunctionEntry(function_id_a, function_entry_a.binweight, function_entry_a.offset, match_tuple).toDict(),
        }
        return result

    #### SIMPLE LOOKUPS ####
    def getFamily(self, family_id):
        return self.getStorage().getFamily(family_id)

    def getFamiliesByIds(self, family_ids: List[int]) -> Dict[int, FamilyEntry]:
        return self.getStorage().getFamilyEntriesByIds(family_ids)

    def getFunctionsBySampleId(self, sample_id):
        return self.getStorage().getFunctionsBySampleId(sample_id)

    def getSmdaReportForSample(self, sample_id):
        return self.getStorage().getSmdaReportForSample(sample_id)

    def getSampleBinary(self, sample_id):
        return self.getStorage().getSampleBinary(sample_id)

    def openSampleBinary(self, sample_id):
        return self.getStorage().openSampleBinary(sample_id)

    def isServingSampleBinaries(self) -> bool:
        """Whether GET /samples/{id}/binary may hand out stored binaries (STORAGE_SERVE_SUBMITTED_BINARIES)."""
        return bool(self._storage_config.STORAGE_SERVE_SUBMITTED_BINARIES)

    def isFunctionId(self, function_id):
        return self.getStorage().isFunctionId(function_id)

    def isSampleId(self, sample_id):
        return self.getStorage().isSampleId(sample_id)

    def getFunctionById(self, function_id, with_xcfg=False):
        return self.getStorage().getFunctionById(function_id, with_xcfg=with_xcfg)

    def getFunctionsByIds(self, function_ids):
        return self.getStorage().getFunctionsByIds(function_ids)

    def getFunctions(self, start_index, limit):
        return self.getStorage().getFunctions(start_index, limit)

    def getSamplesByFamilyId(self, family_id):
        return self.getStorage().getSamplesByFamilyId(family_id)

    def getSampleById(self, sample_id):
        return self.getStorage().getSampleById(sample_id)

    def getSamplesByIds(self, sample_ids: List[int]) -> Dict[int, SampleEntry]:
        return self.getStorage().getSampleEntriesByIds(sample_ids)

    def getSamples(self, start_index, limit):
        return self.getStorage().getSamples(start_index, limit)

    def getFamilies(self) -> Dict[int, FamilyEntry]:
        storage = self.getStorage()
        family_overview = {}
        for family_id in storage.getFamilyIds():
            family_overview[family_id] = storage.getFamily(family_id)
        return family_overview

    def getFunctionGraph(self, function_id):
        # NOTE: Does not work at the moment, as xcfg might be deleted.
        raise NotImplementedError
        function_entry = self._storage.getFunctionById(function_id)
        function_info = {"cfg": function_entry.xcfg}
        return function_info

    def getAllSampleInfos(self):
        infos = []
        for sample_id in sorted(self.getStorage().getSampleIds()):
            infos.append(self.getSampleById(sample_id))
        return infos

    def getStatus(self, with_pichash=True):
        storage = self.getStorage()
        storage_stats = storage.getStats(with_pichash=with_pichash)
        status = {
            "status": {
                "db_state": storage_stats["db_state"],
                "db_timestamp": storage_stats["db_timestamp"],
                "storage_type": self._storage_config.STORAGE_METHOD,
                "num_bands": storage_stats["num_bands"],
                "num_samples": storage_stats["num_samples"],
                "num_families": storage_stats["num_families"],
                "num_functions": storage_stats["num_functions"],
                "num_pichashes": storage_stats["num_pichashes"],
                # False on an instance created before pichashes were stored zero-padded and not yet
                # migrated (migrate_pichash_padding); range conditions and sorting by pichash need True (#145)
                "pichash_padded": storage_stats.get("pichash_padded", True),
                # minhashes are only comparable across functions escaped by the same smda; see
                # mcrit/minhash/EscaperFingerprint.py for why this is worth surfacing
                "smda_version": SMDA_VERSION,
                "escaper_fingerprint": getEscaperFingerprint(),
                # per architecture, as exports record them (#93); the field above stays Intel's
                "escaper_fingerprints": getEscaperFingerprints(),
            }
        }
        # operator signal for a half-migrated instance (#137): the inline fallback keeps serving
        # correct, but the split's space reclamation only happens once migrate_xcfg_split ran.
        # Every storage answers this - the interface default is None ("not applicable") - so the
        # field is simply omitted when a backend cannot determine it.
        inline_xcfg_remaining = storage.hasInlineXcfgRemaining()
        if inline_xcfg_remaining is not None:
            status["status"]["inline_xcfg_remaining"] = inline_xcfg_remaining
        # how many samples' minhashes an older escaper produced; repairMinHashes rehashes them (#142)
        threshold = Worker.getMinHashCompatibilityThreshold()
        status["status"]["minhash_compatibility_threshold"] = threshold
        status["status"]["num_samples_with_stale_minhashes"] = storage.countSamplesWithStaleMinHashes(threshold)
        # how many non-Intel samples' block hashes were escaped as Intel code; recalculatePicHashes redoes them (#240)
        num_stale_picblockhash_samples = storage.countSamplesWithStalePicBlockHashes()
        if num_stale_picblockhash_samples is not None:
            status["status"]["num_samples_with_stale_picblockhashes"] = num_stale_picblockhash_samples
        # how many samples' PicHashes an older escaper produced and recalculatePicHashes has not yet redone (#249)
        num_stale_pichash_samples = storage.countSamplesWithStalePicHashes()
        if num_stale_pichash_samples is not None:
            status["status"]["num_samples_with_stale_pichashes"] = num_stale_pichash_samples
        # and how many of those it cannot redo, as some function's disassembly is gone (#249)
        num_unrehashable_samples = storage.countSamplesWithUnrehashablePicHashes()
        if num_unrehashable_samples is not None:
            status["status"]["num_samples_pichash_unrehashable"] = num_unrehashable_samples
        return status

    def getVersion(self):
        return {"version": self.config.VERSION}

    def getMatchesForPicHash(self, pichash):
        return self.getStorage().getMatchesForPicHash(pichash)

    def getMatchesForPicBlockHash(self, picblockhash):
        return self.getStorage().getMatchesForPicBlockHash(picblockhash)

    def getSampleBySha256(self, sample_sha256):
        return self.getStorage().getSampleBySha256(sample_sha256)

    #### SEARCH ####

    # When performing an initial search, the cursor should be set to None.
    # Search results are of the following form:
    # {
    #     "search_results": {
    #         id1: found_entry1,
    #         id2: found_entry2,
    #         ...
    #     },
    #     "cursor": {
    #         "forward": forward cursor,
    #         "backward": backward cursor,
    #     }
    # }
    # To get further results, perform a search using the forward cursor.
    # To get back to the previous search results, use the backward cursor.
    # If no further or previous results are available, the forward or backward cursor will be None.
    #
    # IMPORTANT: A cursor shall only be used in combination with the same
    # search_term and sort_by_list that were used when the cursor was returned from mcrit.
    # If those parameters are altered, mcrit's behavior is undefined.

    def _getSearchResultTemplate(self, search_function, search_term, sort_by_list, cursor_str, limit):
        assert isinstance(search_term, str)
        # a page at a time: no positive limit, or one beyond the page size, answers a full page
        if not 0 < limit <= self.MAX_SEARCH_RESULTS:
            limit = self.MAX_SEARCH_RESULTS

        sort_fields = [sort_info[0] for sort_info in sort_by_list]

        is_backward_search = False
        cursor = MinimalSearchCursor.fromStr(cursor_str)

        full_cursor = FullSearchCursor(cursor, sort_by_list)
        is_backward_search = not full_cursor.is_forward_search

        parsed_search = self.search_query_parser.parse(search_term)
        try:
            search_results_objects = search_function(parsed_search, cursor=full_cursor, max_num_results=limit + 1)
        except TypeError as mismatched_cursor:
            # the in-memory search compares the cursor's values with the entries' and fails on a cursor
            # that was issued for another sort field; MongoDB answers such a cursor with an arbitrary page
            if cursor is None:
                raise
            raise ValueError("The search cursor does not match the requested sort order.") from mismatched_cursor

        # Find last last_element_key, which is used for the forward cursor
        search_results_keys = list(search_results_objects.keys())
        if len(search_results_objects) >= limit + 1:
            search_results_objects.pop(search_results_keys[-1])
            search_results_keys.pop()
            last_element_key = search_results_keys[-1]
        else:
            last_element_key = None

        forward_cursor_str = None
        # `is not None`, not truthiness: the key is an id, and a page whose last entry is
        # id 0 (function 0, sample 0, the unknown family 0) used to end the listing there
        if last_element_key is not None:
            last_result = search_results_objects[last_element_key]
            forward_cursor = MinimalSearchCursor()
            forward_cursor.is_forward_search = True ^ is_backward_search  # switch for backward search, because of swap
            forward_cursor.record_values = [_get_cursor_value(last_result, field) for field in sort_fields]
            forward_cursor.sort_spec = sortSpec(sort_by_list)
            forward_cursor_str = forward_cursor.toStr()

        backward_cursor_str = None
        if len(search_results_objects) > 0 and cursor is not None:
            first_result = search_results_objects[search_results_keys[0]]
            backward_cursor = MinimalSearchCursor()
            backward_cursor.is_forward_search = False ^ is_backward_search  # switch for backward search, because of swap
            backward_cursor.record_values = [_get_cursor_value(first_result, field) for field in sort_fields]
            backward_cursor.sort_spec = sortSpec(sort_by_list)
            backward_cursor_str = backward_cursor.toStr()

        if is_backward_search:
            # reverse order
            backward_cursor_str, forward_cursor_str = forward_cursor_str, backward_cursor_str
            search_results = {k: v.toDict() for k, v in reversed(search_results_objects.items())}
        else:
            search_results = {k: v.toDict() for k, v in search_results_objects.items()}

        return {
            "search_results": search_results,
            "cursor": {
                "forward": forward_cursor_str,
                "backward": backward_cursor_str,
            },
        }

    def _get_sort_data(self, standard_sort, sort_by, is_ascending):
        if sort_by == standard_sort or sort_by is None:
            sort_by_list = [
                (standard_sort, is_ascending),
            ]
        else:
            # the tie-break follows the direction of the sort field: the order stays total and
            # deterministic, and a single compound index on (field, id) then serves the
            # descending order as well, walked backwards (fkie-cad/mcritweb#59)
            sort_by_list = [
                (sort_by, is_ascending),
                (standard_sort, is_ascending),
            ]
        return sort_by_list

    def getFamilySearchResults(self, search_term, sort_by="family_id", is_ascending=True, cursor=None, limit=100):
        storage = self.getStorage()
        term_as_int = None
        id_match = None
        try:
            if search_term.startswith("0x"):
                term_as_int = int(search_term, 16)
            else:
                term_as_int = int(search_term)
            if term_as_int <= 0xFFFFFFFF:
                if storage.isFamilyId(term_as_int):
                    id_match = storage.getFamily(term_as_int).toDict()
            else:
                LOGGER.warning("Can only handle family/sample/function IDs up to 0xFFFFFFFF.")
        except Exception:
            pass

        if sort_by not in (
            None,
            "family_id",
            "family_name",
            "num_samples",
            "num_library_samples",
            "num_functions",
        ):
            raise ValueError(f"Sorting family search results by the field '{sort_by}' is not supported.")

        sort_data = self._get_sort_data("family_id", sort_by, is_ascending)
        result = self._getSearchResultTemplate(
            storage.findFamilyByString,
            search_term,
            sort_data,
            cursor,
            limit,
        )
        result["id_match"] = id_match
        return result

    def getFunctionSearchResults(self, search_term, sort_by="function_id", is_ascending=True, cursor=None, limit=100):
        storage = self.getStorage()
        term_as_int = None
        id_match = None
        try:
            if search_term.startswith("0x"):
                term_as_int = int(search_term, 16)
            else:
                term_as_int = int(search_term)
            if term_as_int <= 0xFFFFFFFF:
                if storage.isFunctionId(term_as_int):
                    id_match = storage.getFunctionById(term_as_int).toDict()
            else:
                LOGGER.warning("Can only handle family/sample/function IDs up to 0xFFFFFFFF.")
        except Exception:
            pass

        if sort_by not in (
            None,
            "function_id",
            "sample_id",
            "family_id",
            "pichash",
            "function_name",
            "offset",
            "num_instructions",
            "num_blocks",
        ):
            raise ValueError(f"Sorting function search results by the field '{sort_by}' is not supported.")

        sort_data = self._get_sort_data("function_id", sort_by, is_ascending)
        result = self._getSearchResultTemplate(
            storage.findFunctionByString,
            search_term,
            sort_data,
            cursor,
            limit,
        )
        result["id_match"] = id_match
        return result

    def getSampleSearchResults(self, search_term, sort_by="sample_id", is_ascending=True, cursor=None, limit=100):
        storage = self.getStorage()
        term_as_int = None
        id_match = None
        try:
            if search_term.startswith("0x"):
                term_as_int = int(search_term, 16)
            else:
                term_as_int = int(search_term)
            if term_as_int <= 0xFFFFFFFF:
                if storage.isSampleId(term_as_int):
                    id_match = storage.getSampleById(term_as_int).toDict()
            else:
                LOGGER.warning("Can only handle family/sample/function IDs up to 0xFFFFFFFF.")
        except Exception:
            pass

        sha_match = None
        if re.match("^[a-fA-F0-9]{64}$", search_term) is not None:
            # both storages answer an unknown hash with None (#158)
            sample_entry = storage.getSampleBySha256(search_term)
            if sample_entry is not None:
                sha_match = sample_entry.toDict()

        if sort_by not in (
            None,
            "filename",
            "sample_id",
            "family_id",
            "family",
            "architecture",
            "base_addr",
            "binary_size",
            "binweight",
            "bitness",
            "component",
            "is_library",
            "sha256",
            "timestamp",
            "version",
            "statistics.num_functions",
        ):
            raise ValueError(f"Sorting sample search results by the field '{sort_by}' is not supported.")

        sort_data = self._get_sort_data("sample_id", sort_by, is_ascending)
        result = self._getSearchResultTemplate(
            storage.findSampleByString,
            search_term,
            sort_data,
            cursor,
            limit,
        )
        result["id_match"] = id_match
        result["sha_match"] = sha_match
        return result

    ##### CONFIG CHANGES ####
    def updateMinHashThreshold(self, threshold):
        self.config.MINHASH_CONFIG.MINHASH_MATCHING_THRESHOLD = threshold
        self._minhash_config.MINHASH_MATCHING_THRESHOLD = threshold

    def updatePicHashSize(self, size):
        self.config.MINHASH_CONFIG.PICHASH_SIZE = size
        self._minhash_config.PICHASH_SIZE = size

    def updateMinHasherConfig(self, config):
        self._storage_config = config.STORAGE_CONFIG
        self._minhash_config = config.MINHASH_CONFIG
        self._shingler_config = config.SHINGLER_CONFIG
