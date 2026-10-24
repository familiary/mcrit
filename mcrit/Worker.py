#!/usr/bin/env python3

import hashlib
import logging
import os
import signal
import threading
import uuid
from datetime import UTC, datetime, timedelta
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
from mcrit.minhash.MinHasher import MINHASH_SHINGLER_REVISION, MinHasher
from mcrit.queue.LocalQueue import Job
from mcrit.queue.QueueFactory import QueueFactory
from mcrit.queue.QueueRemoteCalls import NoProgressReporter, QueueRemoteCallee, Remote, UncacheableResult
from mcrit.storage.FunctionEntry import smdaFunctionFromXcfg
from mcrit.storage.SampleEntry import SampleEntry
from mcrit.storage.StorageFactory import StorageFactory
from mcrit.storage.UniqueBlocksResult import UniqueBlocksProgressReporter, blockHashAsInt, blockScore, isGenericBlock, selectBlockCover

if TYPE_CHECKING:
    from mcrit.storage.StorageInterface import StorageInterface

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

# Declared by the jobs whose result is a report computed from the corpus (matching, cross
# compares, unique blocks) and recorded in their job descriptors, so a repeated request is only
# answered from a job computed by the same results version (#241). Bump it in any change that
# alters what such a report holds for the same corpus and parameters - jobs made before are
# then recomputed on their next request instead of being handed out again.
RESULTS_VERSION = 3


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
        self.queue.unregisterWorker()
        self.queue.release_all_jobs()

    def run(self):
        # jobs run in this process while the heartbeat reports it alive, so nothing else would reclaim
        # one that does not end; a SpawningWorker bounds its job processes instead
        # a QueueConfig from before this setting (docker-mcrit ships its own) has none
        self._job_timeout = getattr(self._queue_config, "QUEUE_WORKER_JOB_TIMEOUT", 3600)
        if self._job_timeout > 0 and (not hasattr(signal, "setitimer") or threading.current_thread() is not threading.main_thread()):
            LOGGER.warning("QUEUE_WORKER_JOB_TIMEOUT needs SIGALRM in the main thread; this worker runs jobs without a time limit.")
            self._job_timeout = 0
        super().run()

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

    def _addReport(self, smda_report, calculate_hashes=True, calculate_matches=False, sha256_verified=False) -> Optional["SampleEntry"]:
        sample_entry = self._storage.getSampleBySha256(smda_report.sha256)
        if sample_entry:
            LOGGER.info("Sample is already present in database: %s", sample_entry)
            return sample_entry
        sample_entry = self._storage.addSmdaReport(smda_report, sha256_verified=sha256_verified)
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
        job_id = self._jobIdOf(progress_reporter)
        if sample_entry and sample_entry.sha256_verified is not True:
            # indexed from a report that only states this hash: replaced by the bytes' disassembly (#270)
            return self._replaceReportedSample(sample_entry, binary, binary_sha256, filename, family, version, is_dump, base_address, bitness, job_id=job_id)
        if sample_entry:
            return self._knownSampleResult(sample_entry, binary)
        return self._indexNewBinary(binary, binary_sha256, filename, family, version, is_dump, base_address, bitness, job_id=job_id)

    @staticmethod
    def _jobIdOf(progress_reporter) -> Optional[str]:
        """The queue job a call runs as, which a remote call's progress reporter is made for."""
        job = getattr(progress_reporter, "_job", None)
        return str(job.job_id) if job is not None else None

    def _knownSampleResult(self, sample_entry: "SampleEntry", binary: bytes) -> Dict[str, Any]:
        LOGGER.info("Sample is already known with ID: %d", sample_entry.sample_id)
        result: Dict[str, Any] = {"sample_info": sample_entry.toDict()}
        if self._storage_config.STORAGE_KEEP_SUBMITTED_BINARIES and not self._storage.hasSampleBinary(sample_entry.sample_id):
            # a retried job (the sample was committed, storing the binary was not) or a
            # sample submitted before binaries were kept: complete it here
            result["binary_stored"] = self._storage.storeSampleBinary(sample_entry.sample_id, binary)
        return result

    @staticmethod
    def _disassemble(binary, filename, family, version, is_dump, base_address, bitness) -> SmdaReport:
        LOGGER.info("Disassembling...")
        disassembler = Disassembler(SmdaConfig())
        if is_dump:
            smda_report = disassembler.disassembleBuffer(binary, base_addr=base_address, bitness=bitness)
        else:
            smda_report = disassembler.disassembleUnmappedBuffer(binary)
        if filename is not None:
            smda_report.filename = filename
        if family is not None:
            smda_report.family = family
        if version is not None:
            smda_report.version = version
        return smda_report

    def _indexNewBinary(self, binary, binary_sha256, filename, family, version, is_dump, base_address, bitness, job_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        smda_report = self._disassemble(binary, filename, family, version, is_dump, base_address, bitness)
        # smda hashes the buffer it was given; the sample is only as verified as that hash is the upload's
        sample_entry = self._addReport(smda_report, sha256_verified=smda_report.sha256 == binary_sha256)
        LOGGER.info("Disassembled and indexed sample: %s", sample_entry)
        if sample_entry is None:
            return None
        if sample_entry.sha256_verified is not True and smda_report.sha256 == binary_sha256:
            # a report submitted under this hash won the race with this job: replace it as well
            return self._replaceReportedSample(
                sample_entry, binary, binary_sha256, filename, family, version, is_dump, base_address, bitness, smda_report=smda_report, job_id=job_id
            )
        result: Dict[str, Any] = {"sample_info": sample_entry.toDict()}
        if self._storage_config.STORAGE_KEEP_SUBMITTED_BINARIES:
            # the raw submission, for whatever wants the bytes later (#95)
            result["binary_stored"] = self._storage.storeSampleBinary(sample_entry.sample_id, binary)
        return result

    @staticmethod
    def compareWithStoredFunctions(stored_pichashes: Dict[int, int], stored_base_addr: int, smda_report: SmdaReport) -> Dict[str, Any]:
        """How far a binary's disassembly agrees with a stored sample's functions (#270): those at
        the same offset from the base address with the same PicHash, as a share of the larger of
        the two function sets. For information only, the upload replaces the sample either way.
        One pass over the binary's functions against the stored ones by offset."""
        stored_by_offset = {offset - stored_base_addr: pichash for offset, pichash in stored_pichashes.items()}
        binary_base_addr = smda_report.base_addr or 0
        num_matched = 0
        num_binary_functions = 0
        for smda_function in smda_report.getFunctions():
            num_binary_functions += 1
            if smda_function.pic_hash and stored_by_offset.get((smda_function.offset or 0) - binary_base_addr) == smda_function.pic_hash:
                num_matched += 1
        num_larger = max(num_binary_functions, len(stored_by_offset))
        return {
            "agreement": num_matched / num_larger if num_larger else 1.0,
            "matched_functions": num_matched,
            "binary_functions": num_binary_functions,
            "stored_functions": len(stored_by_offset),
        }

    def _claimReportedSample(self, sample_id: int, token: str, job_id: Optional[str]) -> str:
        """Claim a sample for replacing its analysis, taking over the claim of a job that ended."""
        stale_after = getattr(self._queue_config, "QUEUE_VERIFICATION_CLAIM_TIMEOUT", 60 * 60)
        claim = self._storage.claimSampleVerification(sample_id, token, job_id, stale_after)
        if claim != "busy":
            return claim
        held = self._storage.getSampleVerificationClaim(sample_id)
        if held is None:
            # released in between
            return self._storage.claimSampleVerification(sample_id, token, job_id, stale_after)
        if self._isClaimingJobOver(held.get("job_id"), job_id):
            return self._storage.claimSampleVerification(sample_id, token, job_id, stale_after, dead_token=held["token"])
        return "busy"

    def _isClaimingJobOver(self, claiming_job_id: Optional[str], own_job_id: Optional[str]) -> bool:
        """Whether the queue job that holds a claim no longer runs: finished, failed, or released
        for another attempt. Without a job id to look up, only the claim's timeout tells."""
        if claiming_job_id is None:
            return False
        if claiming_job_id == own_job_id:
            # an earlier attempt of this very job
            return True
        try:
            job = self.queue.get_job(claiming_job_id)
        except Exception:
            LOGGER.warning("Could not look up job %s holding a sample claim.", claiming_job_id, exc_info=True)
            return False
        return job is None or bool(job.is_finished) or bool(job.is_failed) or not job.locked_by

    def _replaceReportedSample(
        self,
        sample_entry: "SampleEntry",
        binary,
        binary_sha256,
        filename,
        family,
        version,
        is_dump,
        base_address,
        bitness,
        smda_report: Optional[SmdaReport] = None,
        job_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Replace the analysis of a sample indexed from a submitted report by its binary's (#270).

        A report only states the hash of the bytes it was made from. Once the bytes are uploaded,
        their disassembly takes the place of the reported analysis under the same sample_id, so
        that sha256_verified vouches for the analysis being MCRIT's own of those bytes. The labels
        of functions found at the same offset from the base address are carried over. The family,
        version and filename the upload names win, otherwise the sample keeps its own, as it holds
        them when the swap ends, and so it does its is_library and component. How far the report
        agreed with the disassembly is part of the result, for information.
        """
        sample_id = sample_entry.sample_id
        token = f"{self._worker_id}:{uuid.uuid4()}"
        claim = self._claimReportedSample(sample_id, token, job_id)
        if claim == "missing":
            # deleted since it was looked up: the upload is a new sample after all
            return self._indexNewBinary(binary, binary_sha256, filename, family, version, is_dump, base_address, bitness, job_id=job_id)
        if claim == "verified":
            current = self._storage.getSampleById(sample_id)
            if current is None:
                return self._indexNewBinary(binary, binary_sha256, filename, family, version, is_dump, base_address, bitness, job_id=job_id)
            return self._knownSampleResult(current, binary)
        if claim == "busy":
            LOGGER.info("Sample %d is being replaced by another job.", sample_id)
            return {"sample_info": sample_entry.toDict(), "verification": {"status": "in_progress"}}
        verification: Dict[str, Any] = {}
        try:
            if claim == "resume":
                # a job that died swapping had the bytes disassembled already: finish its swap
                # with what this upload says about the sample
                verification["resumed"] = True
            else:
                if smda_report is None:
                    smda_report = self._disassemble(binary, filename, family, version, is_dump, base_address, bitness)
                if smda_report.sha256 != binary_sha256:
                    self._storage.releaseSampleVerification(sample_id, token)
                    return {"sample_info": sample_entry.toDict(), "verification": {"status": "unverified", "reason": "the disassembly hashes other bytes than the upload"}}
                if not self._storage.renewSampleVerification(sample_id, token):
                    return self._unreplacedResult(sample_id, verification)
                current = self._storage.getSampleById(sample_id)
                stored_base_addr = current.base_addr if current is not None else 0
                verification.update(self.compareWithStoredFunctions(self._storage.getFunctionPicHashesBySampleId(sample_id), stored_base_addr, smda_report))
                if not self._storage.stageSampleAnalysis(sample_id, smda_report, token) or not self._storage.renewSampleVerification(sample_id, token):
                    return self._unreplacedResult(sample_id, verification)
            verification.update(self._storage.swapSampleAnalysis(sample_id, token, filename=filename, family=family, version=version))
        except Exception:
            self._storage.releaseSampleVerification(sample_id, token)
            raise
        if verification["status"] != "replaced":
            return self._unreplacedResult(sample_id, verification)
        LOGGER.info("Sample %d replaced by the disassembly of its binary: %s", sample_id, verification)
        self.updateMinHashesForSample(sample_id)
        return self._verificationResult(sample_id, binary, verification)

    def _unreplacedResult(self, sample_id: int, verification: Dict[str, Any]) -> Dict[str, Any]:
        """The result of a replacement that stopped, its claim gone to deleteSample or another job."""
        sample_entry = self._storage.getSampleById(sample_id)
        if "status" not in verification or verification["status"] == "replaced":
            verification["status"] = "claim_lost" if sample_entry is not None else "sample_deleted"
        LOGGER.info("Sample %d was not replaced: %s", sample_id, verification["status"])
        return {"sample_info": sample_entry.toDict() if sample_entry is not None else None, "verification": {**verification, "sample_id": sample_id}}

    def _verificationResult(self, sample_id: int, binary: bytes, verification: Dict[str, Any]) -> Dict[str, Any]:
        sample_entry = self._storage.getSampleById(sample_id)
        if sample_entry is None:
            # deleted right after its swap
            return self._unreplacedResult(sample_id, {**verification, "status": "sample_deleted"})
        result: Dict[str, Any] = {"sample_info": sample_entry.toDict(), "verification": verification}
        if self._storage_config.STORAGE_KEEP_SUBMITTED_BINARIES:
            result["binary_stored"] = self._storage.storeSampleBinary(sample_id, binary)
        return result

    QUERY_JOB_METHODS = ("getMatchesForUnmappedBinary", "getMatchesForMappedBinary", "getMatchesForSmdaReport")

    def _querySampleOfJob(self, job: Job) -> Optional["SampleEntry"]:
        """The query sample a query job produced, or None when the job left no result (it
        failed before matching, or was terminated) - such a job must not take the cleanup down."""
        result = self.getResultForJob(job.job_id)
        if not result or "info" not in result or not result["info"].get("sample"):
            return None
        return SampleEntry.fromDict(result["info"]["sample"])

    # Reports PROGRESS
    @Remote(progress=True)
    def doDbCleanup(self, progress_reporter=NoProgressReporter()) -> Dict[str, Any]:
        """Delete query samples and query jobs older than STORAGE_MONGODB_CLEANUP_TTL, then the
        query functions and disassembly no query sample refers to any more, and optionally
        compact the collections they lived in (#68)."""
        now = datetime.now()
        delta = timedelta(seconds=self._storage_config.STORAGE_MONGODB_CLEANUP_TTL)
        # the queue stamps jobs in local time, while smda stamps a sample in UTC
        time_cutoff = now - delta
        sample_time_cutoff = datetime.now(UTC).replace(tzinfo=None) - delta
        LOGGER.info("Fetching data from the queues.")
        query_jobs = []
        for method in self.QUERY_JOB_METHODS:
            for state in ("finished", "failed"):
                query_jobs.extend((state, Job(job_dict, None)) for job_dict in self.getQueueData(0, 0, method=method, state=state))
        protected_sample_ids = set([])
        samples_to_be_deleted = {}
        jobs_to_be_deleted = []
        LOGGER.info(f"Collected all data from the queues, now iterating {len(query_jobs)} items.")
        # first iterate and collect all potentially stale sample_entries by their submission/processing timestamp
        for sample_entry in self._storage.getSamples(start_index=0, limit=0, is_query=True):
            if sample_entry.timestamp is None:
                LOGGER.warning(f"Found query_samples entry without timestamp: {sample_entry}")
                continue
            if sample_entry.sha256 not in samples_to_be_deleted:
                samples_to_be_deleted[sample_entry.sha256] = []
            # smda run in-process stamps an aware timestamp, which the memory backend keeps as is,
            # while MongoDB stores the UTC wall time without the zone; compare it the way MongoDB keeps it
            timestamp = sample_entry.timestamp
            if timestamp.tzinfo is not None:
                timestamp = timestamp.astimezone(UTC).replace(tzinfo=None)
            if timestamp < sample_time_cutoff:
                samples_to_be_deleted[sample_entry.sha256].append(sample_entry)
        for state, job in query_jobs:
            # a finished job is dated by when it finished, a failed one by when it last ran
            job_timestamp = job.finished_at if state == "finished" else job.started_at
            is_recent = job_timestamp is not None and job_timestamp > time_cutoff
            reference_sample_entry = self._querySampleOfJob(job)
            if reference_sample_entry is None:
                # nothing to protect or to collect; an old job without a result just goes
                if not is_recent:
                    jobs_to_be_deleted.append(job)
                continue
            # we keep those query samples that have been submitted since the cutoff
            if is_recent:
                protected_sample_ids.add(reference_sample_entry.sample_id)
            else:
                jobs_to_be_deleted.append(job)
                if reference_sample_entry.sha256 not in samples_to_be_deleted:
                    samples_to_be_deleted[reference_sample_entry.sha256] = []
                samples_to_be_deleted[reference_sample_entry.sha256].append(reference_sample_entry)
        LOGGER.info(f"Found {len(samples_to_be_deleted)} query samples that can be deleted")
        progress_reporter.set_total(len(samples_to_be_deleted))
        num_samples_deleted = 0
        for sample_sha256, sample_entries in samples_to_be_deleted.items():
            for sample_id in set([sample_entry.sample_id for sample_entry in sample_entries]):
                if sample_id not in protected_sample_ids:
                    LOGGER.info(f"Deleting query sample {sample_id} ({sample_sha256}).")
                    if self._storage.deleteSample(sample_id):
                        num_samples_deleted += 1
            progress_reporter.step()
        # now remove the respective data also from the queue, which also deletes the results from GridFS
        LOGGER.info(f"Found {len(jobs_to_be_deleted)} query jobs that can be deleted.")
        for job in jobs_to_be_deleted:
            self.queue.delete_job(job.job_id)
        # whatever a deleted or half-deleted query sample left behind (#68)
        orphans = self._storage.deleteOrphanedQueryData()
        LOGGER.info(f"Deleted orphaned query data: {orphans}")
        report: Dict[str, Any] = {"num_query_samples_deleted": num_samples_deleted, "num_query_jobs_deleted": len(jobs_to_be_deleted), "orphans": orphans}
        if self._storage_config.STORAGE_MONGODB_COMPACT_AFTER_CLEANUP:
            report["compacted"] = self._storage.compactQueryCollections()
        return report

    # Reports PROGRESS
    @Remote(progress=True)
    def rebuildIndex(self, progress_reporter=NoProgressReporter()):
        return self._storage.rebuildMinhashBandIndex(progress_reporter=progress_reporter)

    @Remote()
    def recomputeFamilyStats(self):
        return self._storage.recomputeFamilyStats()

    @Remote()
    def deleteOrphanedQueueFiles(self, dry_run=False):
        return self.queue.delete_orphaned_files(dry_run=dry_run)

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
    def getBandDfCutoffCoverage(self, band_df_cutoff=None, progress_reporter=NoProgressReporter()):
        """What STORAGE_BAND_DF_CUTOFF (or band_df_cutoff) skips, per band and in total (#201).

        A job rather than a request because it scans the (band_hash, df) index of every band: on a
        7,244-sample corpus (MongoDB 7.0) a single index-only $group of exactly this shape took
        39.3 s for all 20 bands, about 1.1 to 2 s per band. It is kept off the query path because a
        per-lookup count would roughly double each band lookup's index work.
        """
        return self._storage.getBandDfCutoffCoverage(band_df_cutoff=band_df_cutoff, progress_reporter=progress_reporter)

    # Reports PROGRESS
    @Remote(progress=True)
    def recalculatePicHashes(self, progress_reporter=NoProgressReporter()):
        return self._storage.recalculateAllPicHashes(progress_reporter=progress_reporter)

    # Reports PROGRESS
    @Remote(progress=True)
    def recalculateMinHashes(self, progress_reporter=NoProgressReporter()):
        # functions whose disassembly is gone keep their minhashes: this used to drop every minhash
        # first and lose theirs for good, and then stamp their samples as current anyway
        kept_sample_ids = self._storage.deleteRehashableMinHashes(progress_reporter=progress_reporter)
        failed_function_ids = []
        num_updated = self._updateMinHashes(None, progress_reporter, failed_function_ids)
        # a sample with a function that could not be hashed is not recorded as current either, so
        # that a repair tries it again
        unhashable_sample_ids = set()
        for function_id in failed_function_ids:
            function_entry = self._storage.getFunctionById(function_id)
            if function_entry is not None:
                unhashable_sample_ids.add(function_entry.sample_id)
        if unhashable_sample_ids:
            LOGGER.warning("Recalculating MinHashes: %d samples have functions that could not be hashed and are not recorded as current.", len(unhashable_sample_ids))
        self._storage.setMinHashVersionForSamples(SmdaConfig().VERSION, except_sample_ids=kept_sample_ids | unhashable_sample_ids)
        return num_updated

    @staticmethod
    def getMinHashCompatibilityThreshold() -> str:
        """The oldest smda whose escaper produces the same minhashes as the running one."""
        smda_config = SmdaConfig()
        return getattr(smda_config, "ESCAPER_DOWNWARD_COMPATIBILITY", None) or smda_config.VERSION

    # Reports PROGRESS
    @Remote(progress=True)
    def repairMinHashes(self, progress_reporter=NoProgressReporter()):
        """Rehash only the samples whose minhashes an older smda escaper produced (#142), or that
        were computed before a shingler changed for their architecture (#238).

        recalculateMinHashes drops every band collection and rehashes everything; this walks
        the samples whose recorded minhash smda version is older than the escaper compatibility
        threshold (or unrecorded), or whose shingler revision is older than
        SHINGLER_REVISION_SINCE names for their architecture, pulls their band entries, rehashes
        them and records the running version, so the index stays serving throughout and a killed
        run costs one sample.
        """
        threshold = self.getMinHashCompatibilityThreshold()
        stale_sample_ids = self._storage.getSamplesWithStaleMinHashes(threshold)
        LOGGER.info(
            "Repairing MinHashes: %d samples are stale against escaper compatibility %s or shingler revision %d.", len(stale_sample_ids), threshold, MINHASH_SHINGLER_REVISION
        )
        progress_reporter.set_total(len(stale_sample_ids))
        report = {
            "compatibility_threshold": threshold,
            "smda_version": SmdaConfig().VERSION,
            "shingler_revision": MINHASH_SHINGLER_REVISION,
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
            if function_entries and not hashable:
                LOGGER.warning("Repairing MinHashes: sample %d has no disassembly to rehash from, keeping its minhashes.", sample_id)
                report["num_samples_skipped"] += 1
                progress_reporter.step()
                continue
            # a sample without functions large enough to hash is repaired too: it simply holds no
            # minhashes afterwards, and is recorded as current like any other (#238)
            failed_function_ids = []
            minhashes = self.calculateMinHashes(hashable, failed_function_ids=failed_function_ids) if hashable else []
            if failed_function_ids:
                # dropping the sample's minhashes would drop those of the functions that could not be
                # hashed again, and recording it as current would stop anything from trying them
                LOGGER.warning("Repairing MinHashes: %d functions of sample %d cannot be hashed, keeping its minhashes and leaving it stale.", len(failed_function_ids), sample_id)
                report["num_samples_skipped"] += 1
                progress_reporter.step()
                continue
            # a function of the sample whose disassembly is gone keeps its minhash, as in
            # recalculateMinHashes: dropping the sample's minhashes would take it along for good
            # One of another length than the configured signature (an earlier import stored such
            # unchecked) does not fit the band projection and would stop the repair at this sample on
            # every run; it can match nothing and has no disassembly to be recomputed from, so it goes
            minhash_bits = self.config.MINHASH_CONFIG.MINHASH_SIGNATURE_BITS
            signature_bytes = self.config.MINHASH_CONFIG.MINHASH_SIGNATURE_LENGTH * (1 if minhash_bits <= 8 else 4)
            kept_entries = [entry for entry in function_entries if not entry.xcfg and entry.minhash]
            kept = []
            for entry in kept_entries:
                if len(entry.minhash) == signature_bytes:
                    # rebuilt from its bytes, it would write an empty composition over the function's:
                    # it takes the one read with the entry, as deleteMinHashesForSample clears it
                    minhash = entry.getMinHash(minhash_bits=minhash_bits)
                    minhash.shingler_composition = entry.minhash_shingle_composition or {}
                    kept.append(minhash)
            if len(kept) != len(kept_entries):
                LOGGER.warning(
                    "Repairing MinHashes: dropping %d minhashes of sample %d that are not %d bytes long and have no disassembly.",
                    len(kept_entries) - len(kept),
                    sample_id,
                    signature_bytes,
                )
            report["num_functions_dropped"] += self._storage.deleteMinHashesForSample(sample_id)
            if minhashes or kept:
                self._storage.addMinHashes(minhashes + kept)
            if not kept:
                # with kept minhashes from an older smda, the sample stays listed as stale
                self._storage.setMinHashVersionForSamples(SmdaConfig().VERSION, [sample_id])
            report["num_functions_rehashed"] += len(minhashes)
            report["num_samples_repaired"] += 1
            progress_reporter.step()
        return report

    # Reports PROGRESS
    @Remote(progress=True)
    def updateMinHashes(self, function_ids, finish_samples=False, progress_reporter=NoProgressReporter()):
        """Find unhashed functions in storage and calculate their MinHashes, optionally filter by function_ids or get function_entries passed directly

        With finish_samples, every sample still waiting for its minhashes is then finished the way
        updateMinHashesForSample finishes its sample, once nothing in it is left to hash. GET
        /complete_minhashes asks for it, as that is how samples submitted with deferred hashing get
        hashed. recalculateMinHashes and updateMinHashesForSample record versions themselves and
        leave it off.
        """
        return self._updateMinHashes(function_ids, progress_reporter, finish_samples=finish_samples)

    def _updateMinHashes(self, function_ids, progress_reporter, failed_function_ids=None, finish_samples=False):
        # failed_function_ids, when given, collects the functions that could not be hashed
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
                minhashes = self.calculateMinHashes(unhashed_functions, progress_reporter=progress_reporter, failed_function_ids=failed_function_ids)
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
                minhashes = self.calculateMinHashes(unhashed_functions, progress_reporter=progress_reporter, failed_function_ids=failed_function_ids)
                if minhashes:
                    self._storage.addMinHashes(minhashes)
                    num_updated += len(minhashes)
                    LOGGER.info("Updated minhashes for %d function entries.", len(minhashes))
                progress_reporter.step()
        if finish_samples:
            self._finishPendingSamples()
        return num_updated

    def _finishPendingSamples(self) -> None:
        """Record the minhash version of every sample still pending since it was stored and now
        hashed completely and, with STORAGE_DROP_DISASSEMBLY, drop its disassembly - what
        updateMinHashesForSample does for a sample hashed on submission.

        This goes by what storage holds rather than by what this run hashed, so a retried, repeated
        or overlapping run finishes the same samples, and so does one for a sample with nothing
        large enough to hash. A sample repairMinHashes or recalculateMinHashes rehashed stays
        pending until then, as neither drops disassembly. Only samples stored by an smda at or above
        the escaper compatibility threshold and by the running shingler revision count: minhashes
        written across an incompatible upgrade, like those of an import or of a sample stored before
        the marker existed, are left to repairMinHashes.
        """
        pending_sample_ids = self._storage.getSamplesWithPendingMinHashes(self.getMinHashCompatibilityThreshold())
        # as in repairMinHashes, a function without disassembly cannot be hashed and does not hold
        # its sample back; every other function large enough to hash has to have its minhash
        unhashed_sample_ids = self._storage.getSamplesWithUnhashedFunctions(pending_sample_ids)
        finished_sample_ids = [sample_id for sample_id in pending_sample_ids if sample_id not in unhashed_sample_ids]
        LOGGER.info("Updating MinHashes: %d of %d pending samples hashed completely, recording them as current.", len(finished_sample_ids), len(pending_sample_ids))
        if finished_sample_ids:
            self._storage.setMinHashVersionForSamples(SmdaConfig().VERSION, finished_sample_ids, finish_pending=True)
            if self.config.STORAGE_CONFIG.STORAGE_DROP_DISASSEMBLY:
                for sample_id in finished_sample_ids:
                    self._storage.deleteXcfgForSampleId(sample_id)

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
        self._storage.setMinHashVersionForSamples(SmdaConfig().VERSION, [sample_id], finish_pending=True)
        if not function_entries:
            return 0
        if self.config.STORAGE_CONFIG.STORAGE_DROP_DISASSEMBLY:
            self._storage.deleteXcfgForSampleId(sample_id)
        return update_result

    # Reports PROGRESS
    @Remote(progress=True, kwjson_locations=["exclude_picblockhashes"], results_version=RESULTS_VERSION, time_limited=True)
    def getUniqueBlocks(
        self,
        sample_ids,
        family_id=None,
        covers_required=10,
        min_instructions=0,
        gentle_length_score=False,
        max_blocks_per_function=0,
        demote_generic_blocks=False,
        exclude_picblockhashes=None,
        progress_reporter=NoProgressReporter(),
    ):
        """Collect the blocks unique to <sample_ids> and greedily pick a multi-set cover of them.

        The cover is a k-of-n construction: <covers_required> is that k, i.e. how many selected
        blocks every sample must be reached by. Lower values yield a smaller rule on weaker
        evidence. <min_instructions> drops shorter blocks before the selection runs, so the cover is
        chosen from blocks a caller would keep rather than from ones it discards afterwards - a
        two-instruction block is not worth a YARA string. Both are clamped to sane values. Among
        blocks that cover as many samples with the same score, the cover takes the one whose YARA
        string has the most fixed hex digits, then the lowest picblockhash (coverPreference, #283),
        so the same input always gives the same rule.

        Four options try the other directions of #283. They are off by default and leave the cover
        unchanged unless given; the statistics echo them either way:
        - <gentle_length_score> rescores the blocks with lengthScore's gentle decline, which only
          changes those longer than 11 instructions: a 12-instruction block no longer loses half
          its length score.
        - <max_blocks_per_function> caps how many selected blocks one function of one sample
          contributes, see selectBlockCover.
        - <demote_generic_blocks> ranks function exits and mostly-wildcard blocks (isGenericBlock)
          behind every other block that reaches as many samples.
        - <exclude_picblockhashes> is a negative set: blocks with these hashes are dropped before
          the cover, e.g. those of siblings that are not in the index. The list is stored with the
          job rather than in its arguments, as it can run to many thousands of hashes. Called
          through MinHashIndex, even an explicit None or [] is stored and makes a job of its own,
          so leave it out when there is nothing to exclude.

        NOTE: result["yara_rule"] is the cover's selection as a list of picblockhash strings, not
        YARA source - the byte patterns live per block in unique_blocks[hash]["escaped_sequence"].

        statistics reports how far the cover got: "num_samples_covered" counts the samples reached
        by at least covers_required selected blocks, and "yara_covers" is the number of selected
        blocks covering the least covered sample, i.e. the k the cover actually achieved. It also
        echoes the parameters, so a result says what shaped it, and "blocks_considered" records
        how many blocks survived min_instructions and the negative set, which "blocks_excluded"
        counts, while the counts above describe everything found. "generic_blocks" counts the
        considered blocks isGenericBlock flags, only when demote_generic_blocks asks for it.
        "blocks_without_instructions" counts the blocks found whose function was stored without its
        disassembly: they are left out of unique_blocks and of the cover, having no instructions to
        show and no bytes to match on.
        """
        progress = UniqueBlocksProgressReporter.wrap(progress_reporter)
        covers_required = max(1, int(covers_required))
        min_instructions = max(0, int(min_instructions))
        max_blocks_per_function = max(0, int(max_blocks_per_function or 0))
        excluded = {blockHashAsInt(block_hash) for block_hash in exclude_picblockhashes or []}
        blocks_result_dict = self._storage.getUniqueBlocks(sample_ids, progress_reporter=progress)
        blocks_result_dict["statistics"]["has_yara_rule"] = False
        blocks_result_dict["statistics"]["yara_covers"] = 0
        blocks_result_dict["statistics"]["has_complete_yara_rule"] = False
        # a block whose function has no disassembly (STORAGE_DROP_DISASSEMBLY, #42) has no
        # instructions to show and no bytes to match on: it is counted rather than returned, since
        # whatever renders the blocks reads their instructions, and it can never join the cover
        found_blocks = blocks_result_dict["unique_blocks"]
        unique_blocks = {block_hash: entry for block_hash, entry in found_blocks.items() if entry["instructions"]}
        blocks_result_dict["statistics"]["blocks_without_instructions"] = len(found_blocks) - len(unique_blocks)
        if min_instructions:
            unique_blocks = {block_hash: entry for block_hash, entry in unique_blocks.items() if entry["length"] >= min_instructions}
        num_before_exclusion = len(unique_blocks)
        if excluded:
            unique_blocks = {block_hash: entry for block_hash, entry in unique_blocks.items() if blockHashAsInt(block_hash) not in excluded}
        if gentle_length_score:
            for entry in unique_blocks.values():
                # the storage's score with the gentle length share, which differs past 11 instructions
                entry["score"] = blockScore(len(entry["samples"]), len(sample_ids), entry["length"], gentle=True)
        blocks_result_dict["unique_blocks"] = unique_blocks
        blocks_result_dict["statistics"]["covers_required"] = covers_required
        blocks_result_dict["statistics"]["min_instructions"] = min_instructions
        blocks_result_dict["statistics"]["gentle_length_score"] = bool(gentle_length_score)
        blocks_result_dict["statistics"]["max_blocks_per_function"] = max_blocks_per_function
        blocks_result_dict["statistics"]["demote_generic_blocks"] = bool(demote_generic_blocks)
        blocks_result_dict["statistics"]["blocks_excluded"] = num_before_exclusion - len(unique_blocks)
        blocks_result_dict["statistics"]["blocks_considered"] = len(unique_blocks)
        # enrich with escaped sequences, before the cover: it breaks score ties by their fixed hex digits (#283)
        sample_addr_borders = {}
        sample_escaper = {}
        for sample_id in sample_ids:
            sample_entry = self._storage.getSampleById(sample_id)
            assert sample_entry is not None
            sample_addr_borders[sample_id] = {"lower": sample_entry.base_addr, "upper": sample_entry.base_addr + sample_entry.binary_size}
            sample_escaper[sample_id] = SmdaFunction.getInstructionEscaper(sample_entry.architecture)
        progress.start_phase("escape", len(unique_blocks))
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
            progress.step()
        # greedily produce a multi set cover of picblockhashes for sample_ids, i.e. a YARA rule :)
        demoted = {block_hash for block_hash, entry in unique_blocks.items() if isGenericBlock(entry)} if demote_generic_blocks else frozenset()
        if demote_generic_blocks:
            blocks_result_dict["statistics"]["generic_blocks"] = len(demoted)
        # the cover is short next to escaping the blocks, so its phase takes no steps and finish() fills it
        progress.start_phase("cover", 1)
        yara_rule, sample_coverage = selectBlockCover(unique_blocks, sample_ids, covers_required, max_blocks_per_function=max_blocks_per_function, demoted=demoted)
        samples_covered = {sample_id for sample_id, count in sample_coverage.items() if count >= covers_required}
        # an empty request covers its zero samples vacuously - do not report a rule for it
        if len(samples_covered) == len(sample_ids):
            blocks_result_dict["statistics"]["has_yara_rule"] = bool(yara_rule)
            blocks_result_dict["statistics"]["has_complete_yara_rule"] = bool(yara_rule)
        elif yara_rule:
            blocks_result_dict["statistics"]["has_yara_rule"] = True
        blocks_result_dict["statistics"]["num_samples_covered"] = len(samples_covered)
        # how many selected blocks reach the least covered sample - reported as 0 before (#144)
        blocks_result_dict["statistics"]["yara_covers"] = min(sample_coverage.values()) if sample_coverage else 0
        blocks_result_dict["yara_rule"] = yara_rule
        blocks_result_dict["unique_blocks"] = unique_blocks
        progress.finish()
        return blocks_result_dict

    @staticmethod
    def _asJobResult(matcher, match_report):
        """The report as the job's result, kept from answering later requests when it fell back unforeseen.

        A shortlist the server found unavailable is part of the job's arguments, so its fallback
        result has a cache key of its own. One that became unavailable only after submission (a
        rebuild of the function range index started in between) is not, and would otherwise be
        served to every later identical request, shortlist or not (#217).
        """
        if matcher.fellBackUnforeseen():
            return UncacheableResult(match_report)
        return match_report

    # Reports PROGRESS
    @Remote(progress=True, json_locations=[0], results_version=RESULTS_VERSION, time_limited=True)
    def getMatchesForSmdaReport(
        self,
        report_json,
        minhash_threshold=None,
        pichash_size=None,
        band_matches_required=None,
        shortlist_size=None,
        band_df_cutoff=None,
        shortlist_unavailable=None,
        progress_reporter=NoProgressReporter(),
    ):
        matcher = MatcherQuery(
            self,
            minhash_threshold=minhash_threshold,
            pichash_size=pichash_size,
            band_matches_required=band_matches_required,
            progress_reporter=progress_reporter,
            shortlist_size=shortlist_size,
            band_df_cutoff=band_df_cutoff,
            shortlist_unavailable=shortlist_unavailable,
        )
        smda_report = SmdaReport.fromDict(report_json)
        match_report = matcher.getMatchesForSmdaReport(smda_report)
        return self._asJobResult(matcher, match_report)

    # Reports PROGRESS
    @Remote(progress=True, file_locations=[0], results_version=RESULTS_VERSION, time_limited=True)
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
        progress_reporter=NoProgressReporter(),
    ):
        config = SmdaConfig()
        SMDA_REPORT = None
        DISASSEMBLER = Disassembler(config)
        SMDA_REPORT = DISASSEMBLER.disassembleBuffer(binary, base_address)
        matcher = MatcherQuery(
            self,
            minhash_threshold=minhash_threshold,
            pichash_size=pichash_size,
            band_matches_required=band_matches_required,
            progress_reporter=progress_reporter,
            shortlist_size=shortlist_size,
            band_df_cutoff=band_df_cutoff,
            shortlist_unavailable=shortlist_unavailable,
        )
        match_report = matcher.getMatchesForSmdaReport(SMDA_REPORT)
        return self._asJobResult(matcher, match_report)

    # Reports PROGRESS
    @Remote(progress=True, file_locations=[0], results_version=RESULTS_VERSION, time_limited=True)
    def getMatchesForUnmappedBinary(
        self,
        binary,
        minhash_threshold=None,
        pichash_size=None,
        band_matches_required=None,
        shortlist_size=None,
        band_df_cutoff=None,
        shortlist_unavailable=None,
        progress_reporter=NoProgressReporter(),
    ):
        config = SmdaConfig()
        SMDA_REPORT = None
        DISASSEMBLER = Disassembler(config)
        SMDA_REPORT = DISASSEMBLER.disassembleUnmappedBuffer(binary)
        matcher = MatcherQuery(
            self,
            minhash_threshold=minhash_threshold,
            pichash_size=pichash_size,
            band_matches_required=band_matches_required,
            progress_reporter=progress_reporter,
            shortlist_size=shortlist_size,
            band_df_cutoff=band_df_cutoff,
            shortlist_unavailable=shortlist_unavailable,
        )
        match_report = matcher.getMatchesForSmdaReport(SMDA_REPORT)
        return self._asJobResult(matcher, match_report)

    # Reports PROGRESS
    @Remote(progress=True, results_version=RESULTS_VERSION, time_limited=True)
    def getMatchesForSample(
        self,
        sample_id,
        minhash_threshold=None,
        pichash_size=None,
        band_matches_required=None,
        shortlist_size=None,
        band_df_cutoff=None,
        shortlist_unavailable=None,
        progress_reporter=NoProgressReporter(),
    ):
        matcher = MatcherSample(
            self,
            minhash_threshold=minhash_threshold,
            pichash_size=pichash_size,
            band_matches_required=band_matches_required,
            progress_reporter=progress_reporter,
            shortlist_size=shortlist_size,
            band_df_cutoff=band_df_cutoff,
            shortlist_unavailable=shortlist_unavailable,
        )
        match_report = matcher.getMatchesForSample(sample_id)
        return self._asJobResult(matcher, match_report)

    # Reports PROGRESS
    @Remote(progress=True, results_version=RESULTS_VERSION, time_limited=True)
    def getMatchesForSampleVs(
        self,
        sample_id,
        other_sample_id,
        minhash_threshold=None,
        pichash_size=None,
        band_matches_required=None,
        band_df_cutoff=None,
        progress_reporter=NoProgressReporter(),
    ):
        matcher = MatcherVs(
            self,
            minhash_threshold=minhash_threshold,
            pichash_size=pichash_size,
            band_matches_required=band_matches_required,
            progress_reporter=progress_reporter,
            band_df_cutoff=band_df_cutoff,
        )
        match_report = matcher.getMatchesForSample(sample_id, other_sample_id)
        return self._asJobResult(matcher, match_report)

    # Reports PROGRESS
    @Remote(progress=True, results_version=RESULTS_VERSION, time_limited=True)
    def getMatchesForSampleVsGroup(
        self,
        sample_id,
        other_sample_ids: List[int],
        minhash_threshold=None,
        pichash_size=None,
        band_matches_required=None,
        band_df_cutoff=None,
        progress_reporter=NoProgressReporter(),
    ):
        matcher = MatcherVsGroup(
            self,
            minhash_threshold=minhash_threshold,
            pichash_size=pichash_size,
            band_matches_required=band_matches_required,
            progress_reporter=progress_reporter,
            band_df_cutoff=band_df_cutoff,
        )
        match_report = matcher.getMatchesForSample(sample_id, other_sample_ids)
        return self._asJobResult(matcher, match_report)

    @Remote(results_version=RESULTS_VERSION, time_limited=True)
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
    def calculateMinHashes(self, function_entries, progress_reporter=NoProgressReporter(), failed_function_ids=None):
        """MinHashes of the given functions; failed_function_ids, when given, is extended by those that
        have disassembly and are large enough to hash but could not be."""
        minhashes = []
        smda_functions = []
        LOGGER.info("Calculating MinHashes: hashing for %d function entries requested.", len(function_entries))
        without_disassembly = 0
        for func in function_entries:
            binary_info = BinaryInfo(b"")
            binary_info.architecture = func.architecture
            try:
                smda_function = smdaFunctionFromXcfg(func.xcfg, binary_info)
            except Exception as error:
                # an imported function holds whatever disassembly the export stated: one smda cannot
                # rebuild would fail the batch, and so every job over the corpus, on every run
                LOGGER.warning("Calculating MinHashes: function %d cannot be rebuilt from its stored disassembly and is skipped: %s", func.function_id, error)
                if failed_function_ids is not None:
                    failed_function_ids.append(func.function_id)
                continue
            if smda_function is None:
                without_disassembly += 1
                continue
            smda_functions.append((func.function_id, smda_function))
        if without_disassembly:
            # a function whose disassembly was dropped (STORAGE_DROP_DISASSEMBLY, or over the 16 MiB
            # document limit, #42) cannot be hashed; failing the batch would leave every other
            # function in it unhashed as well, on every retry
            LOGGER.warning("Calculating MinHashes: %d function entries have no stored disassembly and are skipped.", without_disassembly)
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
                    minhash = self.minhasher.calculateMinHashFromStorage(smda_function)
                    if minhash is not None:
                        minhashes.append(minhash)
                    if is_stepping:
                        progress_reporter.step()
            LOGGER.info("Calculated minhashes for %d function entries!", len(minhashes))
            if len(minhashes) < len(smda_functions):
                # each one is logged with its function id by calculateMinHashFromStorage
                LOGGER.warning("Calculating MinHashes: %d function entries could not be hashed and stay unhashed.", len(smda_functions) - len(minhashes))
            if failed_function_ids is not None:
                hashed_function_ids = {minhash.function_id for minhash in minhashes}
                failed_function_ids.extend(function_id for function_id, _ in smda_functions if function_id not in hashed_function_ids)
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
