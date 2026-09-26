#!/usr/bin/env python3

import hashlib
import logging
import os
import uuid
from datetime import datetime, timedelta
from itertools import zip_longest
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import tqdm
from smda.common.BinaryInfo import BinaryInfo
from smda.common.SmdaFunction import SmdaFunction
from smda.common.SmdaInstruction import SmdaInstruction
from smda.common.SmdaReport import SmdaReport
from smda.Disassembler import Disassembler
from smda.SmdaConfig import SmdaConfig

from mcrit.config.McritConfig import McritConfig
from mcrit.config.MinHashConfig import MinHashConfig
from mcrit.config.QueueConfig import QueueConfig
from mcrit.config.ShinglerConfig import ShinglerConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.libs.parallel import create_process_pool
from mcrit.matchers.MatcherCross import MatcherCross
from mcrit.matchers.MatcherQuery import MatcherQuery
from mcrit.matchers.MatcherSample import MatcherSample
from mcrit.matchers.MatcherVs import MatcherVs
from mcrit.matchers.MatcherVsGroup import MatcherVsGroup
from mcrit.minhash.MinHasher import MinHasher
from mcrit.queue.LocalQueue import Job
from mcrit.queue.QueueFactory import QueueFactory
from mcrit.queue.QueueRemoteCalls import NoProgressReporter, QueueRemoteCallee, Remote
from mcrit.storage.SampleEntry import SampleEntry
from mcrit.storage.StorageFactory import StorageFactory

if TYPE_CHECKING:
    from mcrit.storage.StorageInterface import StorageInterface

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

# Declared by the jobs whose result is a report computed from the corpus (matching, cross
# compares, unique blocks) and recorded in their job descriptors, so a repeated request is only
# answered from a job computed by the same results version (#241). Bump it in any change that
# alters what such a report holds for the same corpus and parameters - jobs made before are
# then recomputed on their next request instead of being handed out again.
RESULTS_VERSION = 1


class Worker(QueueRemoteCallee):
    def __init__(self, queue=None, config=None, storage: Optional["StorageInterface"] = None, profiling=False):
        self._worker_id = f"Worker-{uuid.uuid4()}"
        LOGGER.info(f"Starting as worker: {self._worker_id}")
        if config is None:
            config = McritConfig()

        if not queue:
            queue = QueueFactory().getQueue(config, consumer_id=self._worker_id)

        if profiling:
            print("[!] Running as profiled application.")
            profiling_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "profiler"))
            os.makedirs(profiling_path, exist_ok=True)
        else:
            profiling_path = None
        super().__init__(queue, profiling_path)

        self.config = config
        self._storage_config = config.STORAGE_CONFIG
        self._minhash_config = config.MINHASH_CONFIG
        self._shingler_config = config.SHINGLER_CONFIG
        self._queue_config = config.QUEUE_CONFIG
        self.minhasher = MinHasher(config.MINHASH_CONFIG, config.SHINGLER_CONFIG)
        if storage:
            self._storage = storage
        else:
            self._storage = StorageFactory.getStorage(config)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        # TODO unregister our worker_id from all in-progress jobs found in the queue
        self.queue.unregisterWorker()
        self.queue.release_all_jobs()

    #### STORAGE IO ####
    def getStorage(self):
        """Get an interface to the storage"""
        return self._storage

    def getStorageData(self):
        """Warning: This is intended for local debugging runs - storage may become huge"""
        results = {
            "config": {
                "minhash_config": self._minhash_config.toDict(),
                "shingler_config": self._shingler_config.toDict(),
                "storage_config": self._storage_config.toDict(),
                "queue_config": self._queue_config.toDict(),
            },
            "stats": self._storage.getStats(),
            "storage": self._storage.getContent(),
        }
        return results

    def setStorageData(self, storage_data):
        self._minhash_config = MinHashConfig.fromDict(storage_data["config"]["minhash_config"])
        self._shingler_config = ShinglerConfig.fromDict(storage_data["config"]["shingler_config"])
        self._storage_config = StorageConfig.fromDict(storage_data["config"]["storage_config"])
        self._queue_config = QueueConfig.fromDict(storage_data["config"]["queue_config"])
        mcrit_config = McritConfig()
        mcrit_config.MINHASH_CONFIG = self._minhash_config
        mcrit_config.SHINGLER_CONFIG = self._shingler_config
        mcrit_config.STORAGE_CONFIG = self._storage_config
        mcrit_config.QUEUE_CONFIG = self._queue_config
        # reinitialize
        self.minhasher = MinHasher(self._minhash_config, self._shingler_config)
        self._storage = StorageFactory.getStorage(mcrit_config)
        self._storage.setContent(storage_data["storage"])

    #### REDIRECTED FROM INDEX: MAIN WORKER FUNKTIONALITY ###

    @Remote(progress=True)
    def modifyFamily(self, family_id, update_information, progress_reporter=NoProgressReporter()):
        return self._storage.modifyFamily(family_id, update_information)

    @Remote(progress=True)
    def deleteFamily(self, family_id, keep_samples=False, progress_reporter=NoProgressReporter()):
        return self._storage.deleteFamily(family_id, keep_samples=keep_samples)

    @Remote(progress=True)
    def modifySample(self, sample_id, update_information, progress_reporter=NoProgressReporter()):
        return self._storage.modifySample(sample_id, update_information)

    @Remote(progress=True)
    def deleteSample(self, sample_id, progress_reporter=NoProgressReporter()):
        return self._storage.deleteSample(sample_id)

    def _addReport(self, smda_report, calculate_hashes=True, calculate_matches=False) -> Optional["SampleEntry"]:
        sample_entry = self._storage.getSampleBySha256(smda_report.sha256)
        if sample_entry:
            LOGGER.info("Sample is already present in database: %s", sample_entry)
            return sample_entry
        sample_entry = self._storage.addSmdaReport(smda_report)
        if not sample_entry:
            return None
        LOGGER.info("Added %s", sample_entry)
        function_entries = self._storage.getFunctionsBySampleId(sample_entry.sample_id) or []
        LOGGER.info("Added %d function entries.", len(function_entries))
        if calculate_hashes:
            self.updateMinHashesForSample(sample_entry.sample_id)
        return sample_entry

    # Reports PROGRESS
    @Remote(progress=True, file_locations=[0])
    def addBinarySample(self, binary, filename, family, version, is_dump, base_address, bitness, progress_reporter=NoProgressReporter()):
        binary_sha256 = hashlib.sha256(binary).hexdigest()
        sample_entry = self._storage.getSampleBySha256(binary_sha256)
        if sample_entry:
            LOGGER.info("Sample is already known with ID: %d", sample_entry.sample_id)
            return {"sample_info": sample_entry.toDict()}
        config = SmdaConfig()
        SMDA_REPORT = None
        DISASSEMBLER = Disassembler(config)
        LOGGER.info("Disassembling...")
        if is_dump:
            SMDA_REPORT = DISASSEMBLER.disassembleBuffer(binary, base_addr=base_address, bitness=bitness)
        else:
            SMDA_REPORT = DISASSEMBLER.disassembleUnmappedBuffer(binary)
        if filename is not None:
            SMDA_REPORT.filename = filename
        if family is not None:
            SMDA_REPORT.family = family
        if version is not None:
            SMDA_REPORT.version = version
        sample_entry = self._addReport(SMDA_REPORT)
        LOGGER.info("Disassembled and indexed sample: %s", sample_entry)
        if sample_entry is not None:
            return {"sample_info": sample_entry.toDict()}
        else:
            return None

    # Reports PROGRESS
    @Remote(progress=True)
    def doDbCleanup(self, progress_reporter=NoProgressReporter()):
        now = datetime.now()
        delta = timedelta(seconds=self._storage_config.STORAGE_MONGODB_CLEANUP_TTL)
        time_cutoff = now - delta
        LOGGER.info("Fetching data from the queues.")
        unmapped_finished = self.getQueueData(0, 0, method="getMatchesForUnmappedBinary", state="finished")
        mapped_finished = self.getQueueData(0, 0, method="getMatchesForMappedBinary", state="finished")
        unmapped_failed = self.getQueueData(0, 0, method="getMatchesForUnmappedBinary", state="failed")
        mapped_failed = self.getQueueData(0, 0, method="getMatchesForMappedBinary", state="failed")
        smda_finished = self.getQueueData(0, 0, method="getMatchesForSmdaReport", state="finished")
        protected_sample_ids = set([])
        samples_to_be_deleted = {}
        jobs_to_be_deleted = []
        LOGGER.info(
            f"Collected all data from the queues, now iterating {len(unmapped_finished) + len(mapped_finished) + len(unmapped_failed) + len(mapped_failed) + len(smda_finished)} items."
        )
        # first iterate and collect all potentially stale sample_entries by their submission/processing timestamp
        for sample_entry in self._storage.getSamples(start_index=0, limit=0, is_query=True):
            if sample_entry.timestamp is None:
                LOGGER.warning(f"Found query_samples entry without timestamp: {sample_entry}")
                continue
            if sample_entry.sha256 not in samples_to_be_deleted:
                samples_to_be_deleted[sample_entry.sha256] = []
            if sample_entry.timestamp < time_cutoff:
                samples_to_be_deleted[sample_entry.sha256].append(sample_entry)

        for job_collection in [unmapped_finished, mapped_finished]:
            for job_dict in job_collection:
                job = Job(job_dict, None)
                result = self.getResultForJob(job.job_id)
                reference_sample_entry = SampleEntry.fromDict(result["info"]["sample"])
                # we keep those query samples that have been submitted since the cutoff
                if job.finished_at > time_cutoff:
                    protected_sample_ids.add(reference_sample_entry.sample_id)
                else:
                    jobs_to_be_deleted.append(job)
                    if reference_sample_entry.sha256 not in samples_to_be_deleted:
                        samples_to_be_deleted[reference_sample_entry.sha256] = []
                    samples_to_be_deleted[reference_sample_entry.sha256].append(reference_sample_entry)
        for failed_job_collection in [unmapped_failed, mapped_failed]:
            for failed_job_dict in failed_job_collection:
                job = Job(failed_job_dict, None)
                result = self.getResultForJob(job.job_id)
                reference_sample_entry = SampleEntry.fromDict(result["info"]["sample"])
                if job.started_at > time_cutoff:
                    protected_sample_ids.add(reference_sample_entry.sample_id)
                else:
                    jobs_to_be_deleted.append(job)
                    if reference_sample_entry.sha256 not in samples_to_be_deleted:
                        samples_to_be_deleted[reference_sample_entry.sha256] = []
                    samples_to_be_deleted[reference_sample_entry.sha256].append(reference_sample_entry)
        LOGGER.info("Decoding SMDA reports for SHA256 hashes.")
        for job_dict in smda_finished:
            job = Job(job_dict, None)
            result = self.getResultForJob(job.job_id)
            reference_sample_entry = SampleEntry.fromDict(result["info"]["sample"])
            # we keep those query samples that have been submitted since the cutoff
            if job.finished_at > time_cutoff:
                protected_sample_ids.add(reference_sample_entry.sample_id)
            else:
                jobs_to_be_deleted.append(job)
                if reference_sample_entry.sha256 not in samples_to_be_deleted:
                    samples_to_be_deleted[reference_sample_entry.sha256] = []
                samples_to_be_deleted[reference_sample_entry.sha256].append(reference_sample_entry)
        LOGGER.info(f"Found {len(samples_to_be_deleted)} query samples that can be deleted")
        progress_reporter.set_total(len(samples_to_be_deleted))
        for sample_sha256, sample_entries in samples_to_be_deleted.items():
            LOGGER.info(f"Deleting {sample_entry.sample_id}.")
            for sample_id in set([sample_entry.sample_id for sample_entry in sample_entries]):
                if sample_id not in protected_sample_ids:
                    self._storage.deleteSample(sample_id)
            progress_reporter.step()
        # now remove the respective data also from the queue, which also deletes the results from GridFS
        LOGGER.info(f"Found {len(jobs_to_be_deleted)} query jobs that can be deleted.")
        for job in jobs_to_be_deleted:
            self.queue.delete_job(job.job_id)

    # Reports PROGRESS
    @Remote(progress=True)
    def rebuildIndex(self, progress_reporter=NoProgressReporter()):
        return self._storage.rebuildMinhashBandIndex(progress_reporter=progress_reporter)

    @Remote()
    def recomputeFamilyStats(self):
        return self._storage.recomputeFamilyStats()

    # Reports PROGRESS
    @Remote(progress=True)
    def rebuildPicBlockHashIndex(self, progress_reporter=NoProgressReporter()):
        return self._storage.rebuildPicBlockHashIndex(progress_reporter=progress_reporter)

    # Reports PROGRESS
    @Remote(progress=True)
    def rebuildFunctionRangeIndex(self, progress_reporter=NoProgressReporter()):
        return self._storage.rebuildFunctionRangeIndex(progress_reporter=progress_reporter)

    # Reports PROGRESS
    @Remote(progress=True)
    def rebuildBandDfIndex(self, progress_reporter=NoProgressReporter()):
        return self._storage.rebuildBandDfIndex(progress_reporter=progress_reporter)

    # Reports PROGRESS
    @Remote(progress=True)
    def recalculatePicHashes(self, progress_reporter=NoProgressReporter()):
        return self._storage.recalculateAllPicHashes(progress_reporter=progress_reporter)

    # Reports PROGRESS
    @Remote(progress=True)
    def recalculateMinHashes(self, progress_reporter=NoProgressReporter()):
        self._storage.deleteAllMinHashes(progress_reporter=progress_reporter)
        num_updated = self.updateMinHashes(None, progress_reporter=progress_reporter)
        self._storage.setMinHashVersionForSamples(SmdaConfig().VERSION)
        return num_updated

    @staticmethod
    def getMinHashCompatibilityThreshold() -> str:
        """The oldest smda whose escaper produces the same minhashes as the running one."""
        smda_config = SmdaConfig()
        return getattr(smda_config, "ESCAPER_DOWNWARD_COMPATIBILITY", None) or smda_config.VERSION

    # Reports PROGRESS
    @Remote(progress=True)
    def repairMinHashes(self, progress_reporter=NoProgressReporter()):
        """Rehash only the samples whose minhashes an older smda escaper produced (#142).

        recalculateMinHashes drops every band collection and rehashes everything; this walks
        the samples whose recorded minhash smda version is older than the escaper compatibility
        threshold (or unrecorded), pulls their band entries, rehashes them and records the
        running version, so the index stays serving throughout and a killed run costs one sample.
        """
        threshold = self.getMinHashCompatibilityThreshold()
        stale_sample_ids = self._storage.getSamplesWithStaleMinHashes(threshold)
        LOGGER.info("Repairing MinHashes: %d samples are stale against escaper compatibility %s.", len(stale_sample_ids), threshold)
        progress_reporter.set_total(len(stale_sample_ids))
        report = {
            "compatibility_threshold": threshold,
            "smda_version": SmdaConfig().VERSION,
            "num_samples_stale": len(stale_sample_ids),
            "num_samples_repaired": 0,
            "num_functions_dropped": 0,
            "num_functions_rehashed": 0,
        }
        report["num_samples_skipped"] = 0
        for sample_id in stale_sample_ids:
            # hash first, drop second: a sample whose disassembly is gone (STORAGE_DROP_DISASSEMBLY)
            # cannot be rehashed, and its old minhashes are still better than none
            function_entries = self._storage.getFunctionsBySampleId(sample_id) or []
            hashable = [function_entry for function_entry in function_entries if function_entry.xcfg]
            minhashes = self.calculateMinHashes(hashable) if hashable else []
            if not minhashes:
                LOGGER.warning("Repairing MinHashes: sample %d has no disassembly to rehash from, keeping its minhashes.", sample_id)
                report["num_samples_skipped"] += 1
                progress_reporter.step()
                continue
            report["num_functions_dropped"] += self._storage.deleteMinHashesForSample(sample_id)
            self._storage.addMinHashes(minhashes)
            self._storage.setMinHashVersionForSamples(SmdaConfig().VERSION, [sample_id])
            report["num_functions_rehashed"] += len(minhashes)
            report["num_samples_repaired"] += 1
            progress_reporter.step()
        return report

    # Reports PROGRESS
    @Remote(progress=True)
    def updateMinHashes(self, function_ids, progress_reporter=NoProgressReporter()):
        """Find unhashed functions in storage and calculate their MinHashes, optionally filter by function_ids or get function_entries passed directly"""
        # Counts every MinHash written, across every batch. It has to be initialised before the
        # loops: when there is nothing left to hash the loop body never runs, and returning
        # len(minhashes) then raised UnboundLocalError - so finishing with no work to do failed
        # exactly like a crash. Accumulating also fixes what the return value means. It used to
        # be the size of the *last* batch, which silently under-reports any run longer than one
        # workpack, while every caller reads it as a total ("num_updated", and 0 for a sample
        # with no functions).
        num_updated = 0
        if function_ids is None:
            # calculate all missing MinHashes in batches.
            unhashed_function_ids = self._storage.getUnhashedFunctions(None, only_function_ids=True)
            # to up to 10.000 function per batch
            LOGGER.info("Updating MinHashes: %d function entries have no MinHash yet.", len(unhashed_function_ids))
            total_batches = len(unhashed_function_ids) // self.config.MINHASH_CONFIG.MINHASH_GENERATION_WORKPACK_SIZE + (
                1 if len(unhashed_function_ids) % self.config.MINHASH_CONFIG.MINHASH_GENERATION_WORKPACK_SIZE else 0
            )
            progress_reporter.set_total(total_batches)
            for sliced_ids in zip_longest(*[iter(unhashed_function_ids)] * self.config.MINHASH_CONFIG.MINHASH_GENERATION_WORKPACK_SIZE):
                sliced_ids = [fid for fid in sliced_ids if fid is not None]
                unhashed_functions = self._storage.getUnhashedFunctions([fid for fid in sliced_ids if isinstance(fid, int)])
                minhashes = self.calculateMinHashes(unhashed_functions, progress_reporter=progress_reporter)
                if minhashes:
                    self._storage.addMinHashes(minhashes)
                    num_updated += len(minhashes)
                    LOGGER.info("Updated minhashes for %d function entries.", len(minhashes))
                progress_reporter.step()
        else:
            LOGGER.info("Updating MinHashes: %d function entries considered.", len(function_ids))
            total_batches = len(function_ids) // self.config.MINHASH_CONFIG.MINHASH_GENERATION_WORKPACK_SIZE + (
                1 if len(function_ids) % self.config.MINHASH_CONFIG.MINHASH_GENERATION_WORKPACK_SIZE else 0
            )
            progress_reporter.set_total(total_batches)
            for sliced_ids in zip_longest(*[iter(function_ids)] * self.config.MINHASH_CONFIG.MINHASH_GENERATION_WORKPACK_SIZE):
                sliced_ids = [fid for fid in sliced_ids if fid is not None]
                unhashed_functions = self._storage.getUnhashedFunctions([fid for fid in sliced_ids if isinstance(fid, int)])
                LOGGER.info("Updating MinHashes: %d function entries have no MinHash yet.", len(unhashed_functions))
                minhashes = self.calculateMinHashes(unhashed_functions, progress_reporter=progress_reporter)
                if minhashes:
                    self._storage.addMinHashes(minhashes)
                    num_updated += len(minhashes)
                    LOGGER.info("Updated minhashes for %d function entries.", len(minhashes))
                progress_reporter.step()
        # TODO if we do deferred calculation for a batch of minhashes, we might have to clear them here or address this where else updateMinHashes is used
        return num_updated

    # Reports PROGRESS
    @Remote(progress=True)
    def updateMinHashesForSample(self, sample_id, progress_reporter=NoProgressReporter()):
        """Find unhashed functions in storage and calculate their MinHashes, optionally filter by function_ids or get function_entries passed directly"""
        function_entries = self._storage.getFunctionsBySampleId(sample_id)
        if function_entries:
            update_result = self.updateMinHashes([fe.function_id for fe in function_entries], progress_reporter=progress_reporter)
        else:
            LOGGER.info("Sample %d did not have any functions, proceeding.", sample_id)
            update_result = 0
        # which escaper produced them, so a later smda can tell whether they are stale (#142)
        self._storage.setMinHashVersionForSamples(SmdaConfig().VERSION, [sample_id])
        if not function_entries:
            return 0
        if self.config.STORAGE_CONFIG.STORAGE_DROP_DISASSEMBLY:
            self._storage.deleteXcfgForSampleId(sample_id)
        return update_result

    # Reports PROGRESS
    @Remote(progress=True, results_version=RESULTS_VERSION)
    def getUniqueBlocks(self, sample_ids, family_id=None, covers_required=10, min_instructions=0, progress_reporter=NoProgressReporter()):
        """Collect the blocks unique to <sample_ids> and greedily pick a multi-set cover of them.

        The cover is a k-of-n construction: <covers_required> is that k, i.e. how many selected
        blocks every sample must be reached by. Lower values yield a smaller rule on weaker
        evidence. <min_instructions> drops shorter blocks before the selection runs, so the cover is
        chosen from blocks a caller would keep rather than from ones it discards afterwards - a
        two-instruction block is not worth a YARA string. Both are clamped to sane values.

        NOTE: result["yara_rule"] is the cover's selection as a list of picblockhash strings, not
        YARA source - the byte patterns live per block in unique_blocks[hash]["escaped_sequence"].

        statistics reports how far the cover got: "num_samples_covered" counts the samples reached
        by at least covers_required selected blocks, and "yara_covers" is the number of selected
        blocks covering the least covered sample, i.e. the k the cover actually achieved. It also
        echoes the two parameters, so a result says what shaped it, and "blocks_considered" records
        how many blocks survived min_instructions while the counts above describe everything found.
        """
        # TODO we could propagate this progress reporter into the storage function for more fine grained progress tracking
        progress_reporter.set_total(1)
        covers_required = max(1, int(covers_required))
        min_instructions = max(0, int(min_instructions))
        blocks_result_dict = self._storage.getUniqueBlocks(sample_ids, progress_reporter=progress_reporter)
        blocks_result_dict["statistics"]["has_yara_rule"] = False
        blocks_result_dict["statistics"]["yara_covers"] = 0
        blocks_result_dict["statistics"]["has_complete_yara_rule"] = False
        if min_instructions:
            blocks_result_dict["unique_blocks"] = {block_hash: entry for block_hash, entry in blocks_result_dict["unique_blocks"].items() if entry["length"] >= min_instructions}
        unique_blocks = blocks_result_dict["unique_blocks"]
        blocks_result_dict["statistics"]["covers_required"] = covers_required
        blocks_result_dict["statistics"]["min_instructions"] = min_instructions
        blocks_result_dict["statistics"]["blocks_considered"] = len(unique_blocks)
        # greedily produce a multi set cover of picblockhashes for sample_ids, i.e. a YARA rule :)
        yara_rule = []
        sample_coverage = {sample_id: 0 for sample_id in sample_ids}
        samples_covered = set()
        while True:
            # calculate block_scores as how much benefit they bring, i.e. how many uncovered samples they can cover at once
            block_candidates = []
            for block_hash, entry in unique_blocks.items():
                sample_ids_coverable = set(entry["samples"]).difference(samples_covered)
                if sample_ids_coverable and block_hash not in yara_rule:
                    candidate = {"block_hash": block_hash, "coverable": sample_ids_coverable, "value": len(sample_ids_coverable), "score": entry["score"]}
                    block_candidates.append(candidate)
            # check if we are done yet, successful or not
            if len(samples_covered) == len(sample_ids):
                # an empty request covers its zero samples vacuously - do not report a rule for it
                blocks_result_dict["statistics"]["has_yara_rule"] = bool(yara_rule)
                blocks_result_dict["statistics"]["has_complete_yara_rule"] = bool(yara_rule)
                break
            if len(block_candidates) == 0:
                if len(yara_rule) > 0:
                    blocks_result_dict["statistics"]["has_yara_rule"] = True
                break
            # if not, choose the best block
            block_candidates.sort(key=lambda i: (i["value"], i["score"]))
            selected_block: Dict[str, Any] = block_candidates.pop()
            yara_rule.append(selected_block["block_hash"])
            # and update counters
            for sample_id in selected_block["coverable"]:
                sample_coverage[sample_id] += 1
            samples_covered = set([sample_id for sample_id, count in sample_coverage.items() if count >= covers_required])
        blocks_result_dict["statistics"]["num_samples_covered"] = len(samples_covered)
        # how many selected blocks reach the least covered sample - reported as 0 before (#144)
        blocks_result_dict["statistics"]["yara_covers"] = min(sample_coverage.values()) if sample_coverage else 0
        blocks_result_dict["yara_rule"] = yara_rule
        # enrich with escaped sequences
        sample_addr_borders = {}
        sample_escaper = {}
        for sample_id in sample_ids:
            sample_entry = self._storage.getSampleById(sample_id)
            assert sample_entry is not None
            sample_addr_borders[sample_id] = {"lower": sample_entry.base_addr, "upper": sample_entry.base_addr + sample_entry.binary_size}
            sample_escaper[sample_id] = SmdaFunction.getInstructionEscaper(sample_entry.architecture)
        for block_hash, entry in unique_blocks.items():
            sample_id = entry["sample_id"]
            escaped_sequences = []
            for instruction in entry["instructions"]:
                smda_instruction = SmdaInstruction(instruction)
                escaped_sequences.append(
                    smda_instruction.getEscapedBinary(
                        sample_escaper[sample_id],
                        escape_intraprocedural_jumps=True,
                        lower_addr=sample_addr_borders[sample_id]["lower"],
                        upper_addr=sample_addr_borders[sample_id]["upper"],
                    )
                )
            unique_blocks[block_hash]["escaped_sequence"] = " ".join(escaped_sequences)
        blocks_result_dict["unique_blocks"] = unique_blocks
        progress_reporter.step()
        return blocks_result_dict

    # Reports PROGRESS
    @Remote(progress=True, json_locations=[0], results_version=RESULTS_VERSION)
    def getMatchesForSmdaReport(self, report_json, minhash_threshold=None, pichash_size=None, band_matches_required=None, progress_reporter=NoProgressReporter()):
        matcher = MatcherQuery(
            self, minhash_threshold=minhash_threshold, pichash_size=pichash_size, band_matches_required=band_matches_required, progress_reporter=progress_reporter
        )
        smda_report = SmdaReport.fromDict(report_json)
        match_report = matcher.getMatchesForSmdaReport(smda_report)
        return match_report

    # Reports PROGRESS
    @Remote(progress=True, file_locations=[0], results_version=RESULTS_VERSION)
    def getMatchesForMappedBinary(self, binary, base_address, minhash_threshold=None, pichash_size=None, band_matches_required=None, progress_reporter=NoProgressReporter()):
        config = SmdaConfig()
        SMDA_REPORT = None
        DISASSEMBLER = Disassembler(config)
        SMDA_REPORT = DISASSEMBLER.disassembleBuffer(binary, base_address)
        matcher = MatcherQuery(
            self, minhash_threshold=minhash_threshold, pichash_size=pichash_size, band_matches_required=band_matches_required, progress_reporter=progress_reporter
        )
        match_report = matcher.getMatchesForSmdaReport(SMDA_REPORT)
        return match_report

    # Reports PROGRESS
    @Remote(progress=True, file_locations=[0], results_version=RESULTS_VERSION)
    def getMatchesForUnmappedBinary(self, binary, minhash_threshold=None, pichash_size=None, band_matches_required=None, progress_reporter=NoProgressReporter()):
        config = SmdaConfig()
        SMDA_REPORT = None
        DISASSEMBLER = Disassembler(config)
        SMDA_REPORT = DISASSEMBLER.disassembleUnmappedBuffer(binary)
        matcher = MatcherQuery(
            self, minhash_threshold=minhash_threshold, pichash_size=pichash_size, band_matches_required=band_matches_required, progress_reporter=progress_reporter
        )
        match_report = matcher.getMatchesForSmdaReport(SMDA_REPORT)
        return match_report

    # Reports PROGRESS
    @Remote(progress=True, results_version=RESULTS_VERSION)
    def getMatchesForSample(self, sample_id, minhash_threshold=None, pichash_size=None, band_matches_required=None, progress_reporter=NoProgressReporter()):
        matcher = MatcherSample(
            self, minhash_threshold=minhash_threshold, pichash_size=pichash_size, band_matches_required=band_matches_required, progress_reporter=progress_reporter
        )
        match_report = matcher.getMatchesForSample(sample_id)
        return match_report

    # Reports PROGRESS
    @Remote(progress=True, results_version=RESULTS_VERSION)
    def getMatchesForSampleVs(
        self,
        sample_id,
        other_sample_id,
        minhash_threshold=None,
        pichash_size=None,
        band_matches_required=None,
        progress_reporter=NoProgressReporter(),
    ):
        matcher = MatcherVs(self, minhash_threshold=minhash_threshold, pichash_size=pichash_size, band_matches_required=band_matches_required, progress_reporter=progress_reporter)
        match_report = matcher.getMatchesForSample(sample_id, other_sample_id)
        return match_report

    # Reports PROGRESS
    @Remote(progress=True, results_version=RESULTS_VERSION)
    def getMatchesForSampleVsGroup(
        self, sample_id, other_sample_ids: List[int], minhash_threshold=None, pichash_size=None, band_matches_required=None, progress_reporter=NoProgressReporter()
    ):
        matcher = MatcherVsGroup(
            self, minhash_threshold=minhash_threshold, pichash_size=pichash_size, band_matches_required=band_matches_required, progress_reporter=progress_reporter
        )
        match_report = matcher.getMatchesForSample(sample_id, other_sample_ids)
        return match_report

    @Remote(results_version=RESULTS_VERSION)
    def combineMatchesToCross(self, sample_to_job_id):
        child_results = []
        for job_id in sample_to_job_id.values():
            result = self.getResultForJob(job_id)
            if result is None:
                raise ValueError("Cannot evaluate Cross Compare. A child job failed.")
            # here we strip the results to sample level only, as function level matches are
            # not required and may easily blow up memory
            result["matches"].pop("functions")
            child_results.append(result)
        return MatcherCross().create_result(child_results)

    def _groupItems(self, items, packsize=500):
        packed_items = []
        for lower_index in range(0, len(items) + packsize, packsize):
            sliced = items[lower_index : lower_index + packsize]
            if sliced:
                packed_items.append(sliced)
        return packed_items

    #### used by Worker(updateMinHashes) and MatcherQuery #####

    # Reports PROGRESS
    def calculateMinHashes(self, function_entries, progress_reporter=NoProgressReporter()):
        minhashes = []
        smda_functions = []
        LOGGER.info("Calculating MinHashes: hashing for %d function entries requested.", len(function_entries))
        for func in function_entries:
            binary_info = BinaryInfo(b"")
            binary_info.architecture = func.architecture
            smda_functions.append((func.function_id, SmdaFunction.fromDict(func.xcfg, binary_info=binary_info)))
        # filter down to functions that fulfill size requirements
        smda_functions = [(function_id, smda_function) for function_id, smda_function in smda_functions if self.minhasher.isMinHashableFunction(smda_function)]
        LOGGER.info("Calculating MinHashes: %d function entries are indexable.", len(smda_functions))
        is_stepping = False
        if smda_functions:
            if self._minhash_config.MINHASH_POOL_INDEXING:
                packed_smda_functions = self._groupItems(smda_functions)
                if not progress_reporter.has_total_set():
                    progress_reporter.set_total(len(packed_smda_functions))
                    is_stepping = True
                with create_process_pool() as pool:
                    for result in tqdm.tqdm(
                        pool.imap_unordered(self.minhasher.calculateMinHashesFromStorage, packed_smda_functions),
                        total=len(packed_smda_functions),
                    ):
                        minhashes.extend(result)
                        if is_stepping:
                            progress_reporter.step()
            else:
                if not progress_reporter.has_total_set():
                    progress_reporter.set_total(len(smda_functions))
                    is_stepping = True
                for smda_function in tqdm.tqdm(smda_functions, total=len(smda_functions)):
                    minhashes.append(self.minhasher.calculateMinHashFromStorage(smda_function))
                    if is_stepping:
                        progress_reporter.step()
            LOGGER.info("Calculated minhashes for %d function entries!", len(minhashes))
        return minhashes

    ##### CONFIG CHANGES ####
    def updateMinHashThreshold(self, threshold):
        self.config.MINHASH_CONFIG.MINHASH_MATCHING_THRESHOLD = threshold
        self._minhash_config.MINHASH_MATCHING_THRESHOLD = threshold
        self.minhasher._minhash_config.MINHASH_MATCHING_THRESHOLD = threshold

    def updatePicHashSize(self, size):
        self.config.MINHASH_CONFIG.PICHASH_SIZE = size
        self._minhash_config.PICHASH_SIZE = size
        self.minhasher._minhash_config.PICHASH_SIZE = size

    def updateMinHasherConfig(self, config):
        self._storage_config = config.STORAGE_CONFIG
        self._minhash_config = config.MINHASH_CONFIG
        self._shingler_config = config.SHINGLER_CONFIG
        self.minhasher = MinHasher(config.MINHASH_CONFIG, config.SHINGLER_CONFIG)
