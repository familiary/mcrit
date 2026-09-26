#!/usr/bin/env python3
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from smda.common.SmdaReport import SmdaReport
from smda.SmdaConfig import SmdaConfig

import mcrit.matchers.MatcherFlags as MatcherFlags
from mcrit.config.McritConfig import McritConfig
from mcrit.config.MinHashConfig import MinHashConfig
from mcrit.config.ShinglerConfig import ShinglerConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.SearchCursor import FullSearchCursor, MinimalSearchCursor
from mcrit.index.SearchQueryParser import SearchQueryParser
from mcrit.libs.utility import compress_encode, decompress_decode
from mcrit.matchers.MatcherQueryFunction import MatcherQueryFunction
from mcrit.minhash.EscaperFingerprint import FINGERPRINT_UNAVAILABLE, getEscaperFingerprint, getEscaperFingerprints
from mcrit.minhash.MinHash import MinHash
from mcrit.minhash.MinHasher import MinHasher
from mcrit.queue.QueueFactory import QueueFactory
from mcrit.queue.QueueRemoteCalls import QueueRemoteCaller
from mcrit.storage.FamilyEntry import FamilyEntry
from mcrit.storage.FunctionEntry import FunctionEntry
from mcrit.storage.MatchedFunctionEntry import MatchedFunctionEntry
from mcrit.storage.SampleEntry import SampleEntry
from mcrit.storage.StorageFactory import StorageFactory
from mcrit.Worker import Worker

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

# the smda that this process will escape with; recorded alongside the escaper fingerprint so an
# export or a status response says which smda produced the minhashes it describes
SMDA_VERSION = SmdaConfig().VERSION


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


def _get_field(object, field):
    dot_index = field.find(".")
    if dot_index >= 0:
        first_field = field[:dot_index]
        remaining_fields = field[dot_index + 1 :]
        return _get_field(_get_field_once(object, first_field), remaining_fields)
    else:
        return _get_field_once(object, field)


class MinHashIndex(QueueRemoteCaller(Worker)):
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
            now = datetime.now()
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
        # iterate over sample_ids and transform respective data
        for sample_id in storage.getSampleIds():
            if sample_ids and sample_id not in sample_ids:
                continue
            sample_entry = storage.getSampleById(sample_id)
            family_mapping[sample_entry.family_id] = sample_entry.family
            exported_sample_entries[sample_entry.sha256] = sample_entry.toDict()
            function_entries = storage.getFunctionsBySampleId(sample_id)
            functions_dict = {function_entry.function_id: function_entry.toDict() for function_entry in function_entries}
            exported_function_entries[sample_entry.sha256] = functions_dict
            if compress_data:
                compressed_encoded = compress_encode(json.dumps(functions_dict))
                exported_function_entries[sample_entry.sha256] = compressed_encoded
                export_size += len(compressed_encoded)
            else:
                export_size += len(json.dumps(functions_dict))
            exported_data["content"]["num_samples"] += 1
            exported_data["content"]["num_functions"] += len(function_entries)
            if export_size > self._storage_config.STORAGE_MAX_EXPORT_SIZE:
                raise MemoryError("Export running beyond the allocated maximum, aborting operation.")
        exported_data["content"]["num_families"] = len(family_mapping)
        exported_data["family_mapping"] = family_mapping
        exported_data["sample_entries"] = exported_sample_entries
        exported_data["function_entries"] = exported_function_entries
        return exported_data

    def addImportData(self, export_data):
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
        # TODO adjust minimum required MCRIT version if we ever extend in a way that objects become incompatible
        # TODO do not compare version as string
        if export_data["config"]["version"] <= "0.0.0":
            LOGGER.error("Cannot import data, version of export is incompatible / too old.")
            return
        if export_data["config"]["shingler"] != self._shingler_config.getConfigHash():
            LOGGER.error("Cannot import data, shingler configuration hash is incompatible.")
            return
        if export_data["config"]["minhash"] != self._minhash_config.getConfigHash():
            LOGGER.error("Cannot import data, MinHash configuration hash is incompatible.")
            return
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
        is_compressed = export_data["content"]["is_compressed"]
        # create a dictionary for pointing family_ids as contained in the export to family_ids as used in this instance
        family_id_remapping = {}
        max_family_id_in_storage = max(storage.getFamilyIds())
        for exported_family_id, exported_family in export_data["family_mapping"].items():
            exported_family_id = int(exported_family_id)
            remapped_family_id = storage.addFamily(exported_family)
            if remapped_family_id > max_family_id_in_storage:
                import_report["num_families_imported"] += 1
                max_family_id_in_storage = remapped_family_id
            else:
                import_report["num_families_skipped"] += 1
            family_id_remapping[exported_family_id] = remapped_family_id
        LOGGER.info("Family remapping created: %d families, %d samples.", len(family_id_remapping), len(export_data["sample_entries"]))
        # iterate samples
        index = 0
        function_entries_to_import = []
        for sample_sha256, sample_entry_dict in export_data["sample_entries"].items():
            index += 1
            # check if sample is already present, and skip if yes
            if storage.getSampleBySha256(sample_sha256):
                LOGGER.info(f"Sample with SHA256 {sample_sha256} already present in database, skipping...")
                import_report["num_samples_skipped"] += 1
                import_report["num_functions_skipped"] += sample_entry_dict["statistics"]["num_functions"]
                continue
            import_report["num_samples_imported"] += 1
            sample_entry = SampleEntry.fromDict(sample_entry_dict)
            # adjust family_id in sample_entry using our remapping
            sample_entry.family_id = family_id_remapping[sample_entry.family_id]
            # add sample_entry to storage and receive new sample_id
            remapped_sample_entry = storage.importSampleEntry(sample_entry)
            if sample_sha256 in export_data["function_entries"]:
                function_entries = export_data["function_entries"][sample_sha256]
                # decompress function_entries if necessary
                if is_compressed:
                    function_entries = json.loads(decompress_decode(function_entries))
                # iterate functions and add them, adjusting family_id and sample_id
                for old_function_id, function_entry_dict in function_entries.items():
                    function_entry = FunctionEntry.fromDict(function_entry_dict)
                    function_entry.sample_id = remapped_sample_entry.sample_id
                    function_entry.family_id = remapped_sample_entry.family_id
                    function_entries_to_import.append(function_entry)
                    import_report["num_functions_imported"] += 1
            LOGGER.info("Sample %d with SHA256 %s added...", index, sample_sha256)

        adjusted_function_entries = storage.importFunctionEntries(function_entries_to_import)
        LOGGER.info(f"{len(adjusted_function_entries)} functions added")
        # we can only start indexing once our imported function_entries had their function_id adjusted to our DB
        minhashes_to_import = []
        for function_entry in adjusted_function_entries:
            # delaying minhash insertion speeds up the procedure dramatically
            if function_entry.minhash:
                minhash = MinHash(function_entry.function_id, function_entry.minhash, minhash_bits=self._minhash_config.MINHASH_SIGNATURE_BITS)
                minhashes_to_import.append(minhash)
        # ensure that their minhashes / pichashes are added to the respective indices
        storage.addMinHashes(minhashes_to_import)
        LOGGER.info(f"{len(minhashes_to_import)} Minhashes added")
        return import_report

    def respawn(self):
        """Get an interface to the storage"""
        storage = self.getStorage()
        self.queue.clear()
        return storage.clearStorage()

    #### REDIRECTED TO WORKER ####
    """
    def updateMinHashes(self, function_ids):
    def rebuildIndex(self):
    def recomputeFamilyStats(self):
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
        storage = self.getStorage()
        sample_entry = storage.getSampleBySha256(smda_report.sha256)
        if sample_entry:
            self._storage.updateFunctionLabels(smda_report, username)
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

    def getMatchesCross(self, sample_ids: List[int], sample_group_only=False, force_recalculation=False, username=None, **params):
        sample_to_job_id = {}
        for id in sample_ids:
            if sample_group_only:
                job_id = self.getMatchesForSampleVsGroup(id, [sid for sid in sample_ids if sid != id], force_recalculation=force_recalculation, username=username, **params)
            else:
                job_id = self.getMatchesForSample(id, force_recalculation=force_recalculation, username=username, **params)
            sample_to_job_id[id] = job_id
        return self.combineMatchesToCross(sample_to_job_id, await_jobs=[*sample_to_job_id.values()], force_recalculation=force_recalculation, username=username)

    def getMatchesForSmdaFunction(self, smda_report_with_function: SmdaReport, minhash_threshold=None, pichash_size=None, band_matches_required=None, exclude_self_matches=False):
        # convert function to FunctionEntry
        smda_report = SmdaReport.fromDict(smda_report_with_function)
        assert smda_report is not None and smda_report.xcfg is not None and smda_report.sha256 is not None
        function_offset = None
        if len(smda_report.xcfg) != 1:
            raise ValueError("SmdaReport has to contain exactly one function.")
        function_offset = int([k for k in smda_report.xcfg.keys()][0])
        matcher = MatcherQueryFunction(self, minhash_threshold=None, pichash_size=None, band_matches_required=band_matches_required, exclude_self_matches=False)
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
        minhash_a = function_entry_a.getMinHash(minhash_bits=self._minhash_config.MINHASH_SIGNATURE_BITS)
        minhash_b = function_entry_b.getMinHash(minhash_bits=self._minhash_config.MINHASH_SIGNATURE_BITS)

        score = None
        if minhash_a.minhash and minhash_b.minhash:
            score = MinHash.calculateMinHashScore(minhash_a.minhash, minhash_b.minhash, minhash_bits=self._minhash_config.MINHASH_SIGNATURE_BITS)
        match_flags = 0
        match_flags += MatcherFlags.IS_MINHASH_FLAG if score is not None and score >= self._minhash_config.MINHASH_MATCHING_THRESHOLD else 0
        match_flags += MatcherFlags.IS_PICHASH_FLAG if function_entry_a.pichash == function_entry_b.pichash else 0
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

    def isFunctionId(self, function_id):
        return self.getStorage().isFunctionId(function_id)

    def isSampleId(self, sample_id):
        return self.getStorage().isSampleId(sample_id)

    def getFunctionById(self, function_id, with_xcfg=False):
        return self.getStorage().getFunctionById(function_id, with_xcfg=with_xcfg)

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

        sort_fields = [sort_info[0] for sort_info in sort_by_list]

        is_backward_search = False
        cursor = MinimalSearchCursor.fromStr(cursor_str)

        full_cursor = FullSearchCursor(cursor, sort_by_list)
        is_backward_search = not full_cursor.is_forward_search

        parsed_search = self.search_query_parser.parse(search_term)
        search_results_objects = search_function(parsed_search, cursor=full_cursor, max_num_results=limit + 1)

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
            forward_cursor.record_values = [_get_field(last_result, field) for field in sort_fields]
            forward_cursor_str = forward_cursor.toStr()

        backward_cursor_str = None
        if len(search_results_objects) > 0 and cursor is not None:
            first_result = search_results_objects[search_results_keys[0]]
            backward_cursor = MinimalSearchCursor()
            backward_cursor.is_forward_search = False ^ is_backward_search  # switch for backward search, because of swap
            backward_cursor.record_values = [_get_field(first_result, field) for field in sort_fields]
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
            "function_id",
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
