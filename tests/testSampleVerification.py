"""#270: a binary uploaded for a sample indexed from a report replaces its analysis by the binary's
disassembly, in place."""

import hashlib
import os
import time
from types import SimpleNamespace
from typing import Any, Callable, Dict, Optional, Set
from unittest import TestCase
from unittest.mock import MagicMock, patch

import pytest
from smda.common.SmdaReport import SmdaReport

from mcrit.config.McritConfig import McritConfig
from mcrit.config.MinHashConfig import MinHashConfig
from mcrit.config.QueueConfig import QueueConfig
from mcrit.config.ShinglerConfig import ShinglerConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.SearchCursor import FullSearchCursor
from mcrit.index.SearchQueryParser import SearchQueryParser
from mcrit.storage.SampleEntry import SampleEntry
from mcrit.storage.StorageFactory import StorageFactory
from mcrit.Worker import Worker

from .context import getTestMongoServerAndPort

EXAMPLE_REPORT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "example_report.smda")
BINARY = b"MZ\x90\x00 the bytes of a sample first indexed from a report"
BINARY_SHA256 = hashlib.sha256(BINARY).hexdigest()
# the functions of example_report.smda whose PicHash the "binary" disagrees on, and the one it lacks
CHANGED_OFFSETS = (0, 356, 364, 1004, 1172)
MISSING_OFFSET = 4424
# where the "binary" is loaded instead, in the rebased variant of the report
REBASE_DELTA = 0x10000


class SampleVerificationTest(TestCase):
    def _storageConfig(self) -> StorageConfig:
        return StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MEMORY, STORAGE_DROP_DISASSEMBLY=False, STORAGE_KEEP_SUBMITTED_BINARIES=True)

    def setUp(self):
        self.config = McritConfig()
        # an instance attribute: the class-level StorageConfig, shared by every McritConfig, stays as it is
        self.config.STORAGE_CONFIG = self._storageConfig()
        self.config.MINHASH_CONFIG = MinHashConfig()
        self.config.MINHASH_CONFIG.MINHASH_POOL_INDEXING = False
        self.config.SHINGLER_CONFIG = ShinglerConfig()
        self.config.QUEUE_CONFIG = QueueConfig()
        self.storage = StorageFactory.getStorage(self.config)
        self.storage.clearStorage()

    def tearDown(self):
        self.storage.clearStorage()

    # helpers

    @staticmethod
    def _report(changed=(), missing=None, rebased=False) -> SmdaReport:
        report = SmdaReport.fromFile(EXAMPLE_REPORT)
        assert report is not None
        delta = REBASE_DELTA if rebased else 0
        if rebased:
            report = SampleVerificationTest._rebased(report, delta)
        report.sha256 = BINARY_SHA256
        report.family = "reported_family"
        report.version = "1.0"
        report.filename = "reported.exe"
        for smda_function in report.getFunctions():
            if (smda_function.offset or 0) - delta in changed:
                smda_function.pic_hash = (smda_function.pic_hash or 0) ^ 0x5A5A
        if missing is not None:
            del report.xcfg[missing + delta]
            assert report.statistics is not None
            report.statistics.num_functions = (report.statistics.num_functions or 0) - 1
        # smda caches the sorted function list
        report._sorted_functions = None
        return report

    @staticmethod
    def _rebased(report: SmdaReport, delta: int) -> SmdaReport:
        """The same code loaded delta bytes higher: every address moves with the base address."""

        def moved(addresses):
            return [int(address) + delta for address in addresses]

        report_dict = report.toDict()
        xcfg = {}
        for function_dict in report_dict["xcfg"].values():
            function_dict["offset"] += delta
            function_dict["blocks"] = {
                int(block) + delta: [[instruction[0] + delta, *instruction[1:]] for instruction in instructions] for block, instructions in function_dict["blocks"].items()
            }
            for refs in ("apirefs", "stringrefs"):
                function_dict[refs] = {int(address) + delta: value for address, value in function_dict[refs].items()}
            for refs in ("blockrefs", "outrefs"):
                function_dict[refs] = {int(address) + delta: moved(targets) for address, targets in function_dict[refs].items()}
            function_dict["inrefs"] = moved(function_dict["inrefs"])
            xcfg[function_dict["offset"]] = function_dict
        report_dict["xcfg"] = xcfg
        report_dict["base_addr"] += delta
        rebased = SmdaReport.fromDict(report_dict)
        assert rebased is not None
        return rebased

    def _worker(self) -> Worker:
        return Worker(queue=MagicMock(), config=self.config, storage=self.storage)

    def _addReported(self, sha256_verified: Optional[bool] = False) -> SampleEntry:
        """A sample as POST /samples indexes it: hashed, its function names labels by an analyst."""
        worker = self._worker()
        sample_entry = worker._addReport(self._report(), sha256_verified=False)
        assert sample_entry is not None
        self.storage.updateFunctionLabels(self._report(), "analyst")
        if sha256_verified is not False:
            self._setVerified(sample_entry.sample_id, sha256_verified)
        # a copy: MemoryStorage hands out the entry it keeps, which changes with the sample
        return SampleEntry.fromDict(sample_entry.toDict())

    def _setVerified(self, sample_id: int, sha256_verified: Optional[bool]) -> None:
        self.storage._samples[sample_id].sha256_verified = sha256_verified

    def _setClaim(self, sample_id: int, token: str, job_id: Optional[str], claimed_at: float) -> None:
        self.storage._verification_claims[sample_id] = {"token": token, "job_id": job_id, "claimed_at": claimed_at, "staged": None}

    def _upload(self, report: SmdaReport, family=None, worker: Optional[Worker] = None, while_disassembling: Optional[Callable[[], Any]] = None) -> Dict[str, Any]:
        worker = worker or self._worker()

        def disassemble(binary):
            if while_disassembling is not None:
                while_disassembling()
            return report

        with patch("mcrit.Worker.Disassembler") as disassembler:
            disassembler.return_value.disassembleUnmappedBuffer.side_effect = disassemble
            result = worker.addBinarySample(BINARY, None, family, None, False, None, None)
        assert result is not None
        return result

    def _bandFunctionIds(self) -> Set[int]:
        return {function_id for band in self.storage._bands.values() for function_ids in band.values() for function_id in function_ids}

    def _functions(self, sample_id):
        return {function_entry.offset: function_entry for function_entry in self.storage.getFunctionsBySampleId(sample_id) or []}

    @staticmethod
    def _failStaging():
        """Makes storing the new analysis fail partway, after some of it was written."""
        calls = {"n": 0}
        from mcrit.storage.MemoryStorage import MemoryStorage

        original = MemoryStorage._buildFunctionEntry

        def failing(self, *args, **kwargs):
            calls["n"] += 1
            if calls["n"] > 3:
                raise RuntimeError("storage failed while staging")
            return original(self, *args, **kwargs)

        return patch.object(MemoryStorage, "_buildFunctionEntry", failing)

    def _stagedLeftovers(self) -> int:
        return sum(1 for claim in self.storage._verification_claims.values() if claim["staged"] is not None)

    def _storedUnder(self, sample_id: int) -> Dict[str, int]:
        """Everything kept under a sample id, by what holds it."""
        return {
            "functions": len(self.storage._sample_id_to_function_ids.get(sample_id, [])),
            "functions_by_scan": sum(1 for entry in self.storage._functions.values() if entry.sample_id == sample_id),
            "pichash_entries": sum(1 for entries in self.storage._pichashes.values() for entry in entries if entry[1] == sample_id),
            "smda_extras": int(sample_id in self.storage._smda_extras),
        }

    def _derivedIndexesAgree(self) -> bool:
        return True

    def _assertConsistent(self):
        """Family counters and the derived indexes agree with what is stored."""
        self.assertEqual(0, self.storage.recomputeFamilyStats()["num_families_corrected"])
        self.assertTrue(self._derivedIndexesAgree())
        self.assertEqual(0, self._stagedLeftovers())

    # tests

    def testAnAgreeingBinaryStillReplacesTheAnalysis(self):
        sample_entry = self._addReported()
        before = self._functions(sample_entry.sample_id)
        result = self._upload(self._report())
        verification = result["verification"]
        self.assertEqual("replaced", verification["status"])
        # how far the report agreed is told, and decides nothing
        self.assertEqual((1.0, 10, 10, 10), (verification["agreement"], verification["matched_functions"], verification["binary_functions"], verification["stored_functions"]))
        self.assertEqual((10, 10, 10, 0), tuple(verification[key] for key in ("old_num_functions", "new_num_functions", "labels_carried", "labels_dropped")))
        self.assertIs(True, result["sample_info"]["sha256_verified"])
        self.assertEqual(sample_entry.sample_id, result["sample_info"]["sample_id"])
        self.assertTrue(result["binary_stored"])
        after = self._functions(sample_entry.sample_id)
        # the analysis is MCRIT's own now: new functions, the labels carried over
        self.assertEqual(set(before), set(after))
        self.assertFalse({entry.function_id for entry in before.values()} & {entry.function_id for entry in after.values()})
        self.assertEqual(10, sum(len(entry.function_labels) for entry in after.values()))
        self.assertEqual("reported_family", result["sample_info"]["family"])
        self._assertConsistent()

    def testADisagreeingBinaryReplacesTheAnalysisInPlace(self):
        sample_entry = self._addReported()
        sample_id = sample_entry.sample_id
        before = self._functions(sample_id)
        old_function_ids = {entry.function_id for entry in before.values()}
        self.assertTrue(old_function_ids & self._bandFunctionIds())
        old_pichash = before[0].pichash
        result = self._upload(self._report(changed=CHANGED_OFFSETS, missing=MISSING_OFFSET), family="uploaded_family")
        verification = result["verification"]
        self.assertEqual("replaced", verification["status"])
        self.assertAlmostEqual(4 / 10, verification["agreement"])
        self.assertEqual((10, 9), (verification["old_num_functions"], verification["new_num_functions"]))
        self.assertEqual((9, 1), (verification["labels_carried"], verification["labels_dropped"]))
        sample_info = result["sample_info"]
        self.assertEqual(sample_id, sample_info["sample_id"])
        self.assertIs(True, sample_info["sha256_verified"])
        # the upload named a family; version and filename it did not name are the sample's
        self.assertEqual(("uploaded_family", "1.0", "reported.exe"), (sample_info["family"], sample_info["version"], sample_info["filename"]))
        self.assertEqual(9, sample_info["statistics"]["num_functions"])
        after = self._functions(sample_id)
        self.assertEqual(9, len(after))
        self.assertNotIn(MISSING_OFFSET, after)
        self.assertFalse(old_function_ids & {entry.function_id for entry in after.values()})
        for offset, entry in after.items():
            self.assertEqual(["analyst"], [label.username for label in entry.function_labels])
            self.assertEqual(before[offset].function_labels[0].function_label, entry.function_labels[0].function_label)
            self.assertEqual(sample_info["family_id"], entry.family_id)
        # the old functions are gone from every index; the new ones are hashed and banded
        bands = self._bandFunctionIds()
        self.assertFalse(old_function_ids & bands)
        self.assertTrue({entry.function_id for entry in after.values()} & bands)
        self.assertFalse(any(match[2] in old_function_ids for match in self.storage.getMatchesForPicHash(old_pichash)))
        self.assertEqual(sample_id, next(iter(self.storage.getMatchesForPicHash(after[0].pichash)))[1])
        for function_id in old_function_ids:
            self.assertFalse(self.storage.isFunctionId(function_id))
        # family counters: the sample moved with its functions
        new_family = self.storage.getFamily(sample_info["family_id"])
        assert new_family is not None
        self.assertEqual((1, 9), (new_family.num_samples, new_family.num_functions))
        self.assertIsNone(self.storage.getFamily(sample_entry.family_id))
        self._assertConsistent()
        # and a second upload of the binary is the plain known-sample path
        again = self._upload(self._report(changed=CHANGED_OFFSETS, missing=MISSING_OFFSET))
        self.assertNotIn("verification", again)

    def testAReplacementKeepsTheFamilyWhenTheUploadNamesNone(self):
        sample_entry = self._addReported()
        result = self._upload(self._report(changed=CHANGED_OFFSETS))
        self.assertEqual("replaced", result["verification"]["status"])
        self.assertEqual(("reported_family", sample_entry.family_id), (result["sample_info"]["family"], result["sample_info"]["family_id"]))
        family = self.storage.getFamily(sample_entry.family_id)
        assert family is not None
        self.assertEqual((1, 10), (family.num_samples, family.num_functions))
        self._assertConsistent()

    def testOffsetsAreComparedAndLabelsCarriedFromTheBaseAddress(self):
        sample_entry = self._addReported()
        result = self._upload(self._report(changed=CHANGED_OFFSETS[:2], rebased=True))
        verification = result["verification"]
        self.assertEqual((8, 10), (verification["matched_functions"], verification["labels_carried"]))
        self.assertEqual(0, verification["labels_dropped"])
        after = self._functions(sample_entry.sample_id)
        stored = self.storage.getSampleById(sample_entry.sample_id)
        assert stored is not None
        self.assertEqual((self._report().base_addr or 0) + REBASE_DELTA, stored.base_addr)
        self.assertEqual(10, sum(len(entry.function_labels) for entry in after.values()))
        self.assertIn(MISSING_OFFSET + REBASE_DELTA, after)

    def testWhatChangesWhileTheBinaryIsDisassembledIsKept(self):
        sample_entry = self._addReported()
        sample_id = sample_entry.sample_id
        labelled = self._functions(sample_id)[1004]

        def edits():
            self.storage.modifySample(sample_id, {"family_name": "renamed_family", "is_library": True, "version": "2.0", "component": "loader"})
            self.storage.modifyFunction(labelled.function_id, {"function_name": "added_meanwhile"}, username="second_analyst")

        result = self._upload(self._report(changed=CHANGED_OFFSETS[:2]), while_disassembling=edits)
        sample_info = result["sample_info"]
        self.assertEqual("replaced", result["verification"]["status"])
        self.assertEqual(("renamed_family", True, "2.0", "loader"), (sample_info["family"], sample_info["is_library"], sample_info["version"], sample_info["component"]))
        family = self.storage.getFamily(sample_info["family_id"])
        assert family is not None
        self.assertEqual((1, 10, 1), (family.num_samples, family.num_functions, family.num_library_samples))
        self.assertIsNone(self.storage.getFamily(sample_entry.family_id))
        labels = {(label.username, label.function_label) for label in self._functions(sample_id)[1004].function_labels}
        self.assertIn(("second_analyst", "added_meanwhile"), labels)
        self.assertEqual(11, result["verification"]["labels_carried"])
        self._assertConsistent()

    def testAPlainTermFindsTheNewFunctionByItsCarriedLabel(self):
        # a label other than the name, carried to the new function, is searched as one (#301)
        sample_entry = self._addReported()
        sample_id = sample_entry.sample_id
        renamed = self._functions(sample_id)[1004]
        self.storage.modifyFunction(renamed.function_id, {"function_name": "qz_carried_label"}, username="second_analyst")
        cursor = FullSearchCursor(None, [("function_id", True)])
        self.assertEqual([renamed.function_id], list(self.storage.findFunctionByString(SearchQueryParser().parse("qz_carried"), cursor=cursor)))
        result = self._upload(self._report(changed=CHANGED_OFFSETS[:2]))
        self.assertEqual("replaced", result["verification"]["status"])
        carried = self._functions(sample_id)[1004]
        self.assertNotEqual("qz_carried_label", carried.function_name)
        self.assertNotEqual(renamed.function_id, carried.function_id)
        self.assertEqual([carried.function_id], list(self.storage.findFunctionByString(SearchQueryParser().parse("qz_carried"), cursor=cursor)))
        self._assertConsistent()

    def testASampleDeletedWhileItsBinaryIsDisassembledStaysDeleted(self):
        sample_entry = self._addReported()
        sample_id = sample_entry.sample_id
        result = self._upload(self._report(changed=CHANGED_OFFSETS), while_disassembling=lambda: self.storage.deleteSample(sample_id))
        self.assertEqual("sample_deleted", result["verification"]["status"])
        self.assertIsNone(result["sample_info"])
        self.assertFalse(self.storage.isSampleId(sample_id))
        self.assertEqual(0, sum(self._storedUnder(sample_id).values()))
        self.assertIsNone(self.storage.getFamily(sample_entry.family_id))
        self._assertConsistent()

    def testADeadJobsStagingIsInvisibleAndGoesWithTheSample(self):
        sample_entry = self._addReported()
        sample_id = sample_entry.sample_id
        changed_pichash = next(f.pic_hash for f in self._report(changed=CHANGED_OFFSETS).getFunctions() if f.offset == CHANGED_OFFSETS[0])
        kept_pichash = next(f.pic_hash for f in self._report().getFunctions() if f.offset not in CHANGED_OFFSETS)
        # a job that staged the new analysis and died: nothing released its claim
        self.assertEqual("claimed", self.storage.claimSampleVerification(sample_id, "dead-job", None, 3600))
        self.assertTrue(self.storage.stageSampleAnalysis(sample_id, self._report(changed=CHANGED_OFFSETS), "dead-job"))
        self.assertEqual(10, len(self._functions(sample_id)))
        self.assertEqual({}, {pichash: matches for pichash, matches in self.storage.getMatchesForPicHashes([changed_pichash]).items() if matches})
        self.assertEqual({sample_id}, {match[1] for match in self.storage.getMatchesForPicHashes([kept_pichash])[kept_pichash]})
        query = self._report()
        query.sha256 = "ab" * 32
        match_report = self._worker().getMatchesForSmdaReport(query.toDict())
        self.assertEqual([sample_id], [match["sample_id"] for match in match_report["matches"]["samples"]])
        self.assertEqual(10, self.storage.getStats()["num_functions"])
        # deleting the sample takes what the dead job staged with it
        self.assertTrue(self.storage.deleteSample(sample_id))
        self.assertEqual(0, sum(self._storedUnder(sample_id).values()))
        self._assertConsistent()

    def testAFailureWhileStoringTheNewAnalysisLeavesTheOldOneIntact(self):
        sample_entry = self._addReported()
        before = {offset: (entry.function_id, entry.pichash, len(entry.function_labels)) for offset, entry in self._functions(sample_entry.sample_id).items()}
        bands = self._bandFunctionIds()
        with self._failStaging(), self.assertRaises(RuntimeError):
            self._upload(self._report(changed=CHANGED_OFFSETS, missing=MISSING_OFFSET))
        after = {offset: (entry.function_id, entry.pichash, len(entry.function_labels)) for offset, entry in self._functions(sample_entry.sample_id).items()}
        self.assertEqual(before, after)
        self.assertEqual(bands, self._bandFunctionIds())
        stored = self.storage.getSampleById(sample_entry.sample_id)
        assert stored is not None
        self.assertIs(False, stored.sha256_verified)
        self.assertEqual(10, self.storage.getStats()["num_functions"])
        self._assertConsistent()
        self.assertIsNone(self.storage.getSampleVerificationClaim(sample_entry.sample_id))
        # the claim went with the failure: the next upload replaces the sample
        self.assertEqual("replaced", self._upload(self._report(changed=CHANGED_OFFSETS))["verification"]["status"])

    def testAVerifiedSampleIsNotReplaced(self):
        sample_entry = self._addReported(sha256_verified=True)
        worker = self._worker()
        with patch.object(worker, "_replaceReportedSample") as replace:
            result = self._upload(self._report(changed=CHANGED_OFFSETS), worker=worker)
            replace.assert_not_called()
        self.assertNotIn("verification", result)
        self.assertEqual(sample_entry.sample_id, result["sample_info"]["sample_id"])

    def testASampleOfUnknownStateIsReplacedLikeAnUnverifiedOne(self):
        sample_entry = self._addReported(sha256_verified=None)
        stored = self.storage.getSampleById(sample_entry.sample_id)
        assert stored is not None
        self.assertIsNone(stored.sha256_verified)
        result = self._upload(self._report(changed=CHANGED_OFFSETS))
        self.assertEqual("replaced", result["verification"]["status"])
        self.assertIs(True, result["sample_info"]["sha256_verified"])

    def testTwoUploadsAtOnceReplaceTheSampleOnce(self):
        sample_entry = self._addReported()
        second_results = []

        def another_upload():
            # the second job runs while the first holds the claim, between its claim and its writes
            second_results.append(self._upload(self._report(changed=CHANGED_OFFSETS)))

        first = self._upload(self._report(changed=CHANGED_OFFSETS, missing=MISSING_OFFSET), while_disassembling=another_upload)
        self.assertEqual({"status": "in_progress"}, second_results[0]["verification"])
        self.assertIs(False, second_results[0]["sample_info"]["sha256_verified"])
        self.assertEqual("replaced", first["verification"]["status"])
        self.assertEqual(10, first["verification"]["old_num_functions"])
        self.assertEqual(9, len(self._functions(sample_entry.sample_id)))

    def testTheClaimOfAJobTheQueueShowsEndedIsTakenOverAtOnce(self):
        sample_entry = self._addReported()
        worker = self._worker()
        worker.queue.get_job.return_value = SimpleNamespace(is_finished=False, is_failed=False, locked_by="another-worker")
        self._setClaim(sample_entry.sample_id, "running-job", "job-1", time.time())
        self.assertEqual({"status": "in_progress"}, self._upload(self._report(), worker=worker)["verification"])
        worker.queue.get_job.return_value = SimpleNamespace(is_finished=False, is_failed=True, locked_by=None)
        self.assertEqual("replaced", self._upload(self._report(), worker=worker)["verification"]["status"])
        worker.queue.get_job.assert_called_with("job-1")

    def testAJobThatLostItsClaimStops(self):
        sample_entry = self._addReported()
        before = {entry.function_id for entry in self._functions(sample_entry.sample_id).values()}
        result = self._upload(self._report(changed=CHANGED_OFFSETS), while_disassembling=lambda: self._setClaim(sample_entry.sample_id, "taken-over", None, time.time()))
        self.assertEqual("claim_lost", result["verification"]["status"])
        self.assertEqual(before, {entry.function_id for entry in self._functions(sample_entry.sample_id).values()})
        self.assertEqual(0, self._stagedLeftovers())

    def testAQuerySampleOfTheSameHashIsLeftAlone(self):
        sample_entry = self._addReported()
        query_entry = self.storage.addSmdaReport(self._report(), isQuery=True)
        assert query_entry is not None
        query_function_ids = self.storage.getFunctionIdsBySampleId(query_entry.sample_id)
        self.assertEqual("replaced", self._upload(self._report(changed=CHANGED_OFFSETS))["verification"]["status"])
        self.assertEqual(query_function_ids, self.storage.getFunctionIdsBySampleId(query_entry.sample_id))
        self.assertIsNotNone(self.storage.getSampleById(query_entry.sample_id))
        self.assertEqual(sample_entry.sample_id, self.storage.getSampleBySha256(BINARY_SHA256).sample_id)

    def testTheComparisonIsByOffsetFromTheBaseAddressAndPicHash(self):
        report = self._report(changed=CHANGED_OFFSETS[:2])
        base_addr = report.base_addr or 0
        # stored at another base: the offsets still line up from it
        stored = {(smda_function.offset or 0) + REBASE_DELTA: smda_function.pic_hash or 0 for smda_function in self._report().getFunctions()}
        # one stored function more than the binary has: the larger set is the measure
        stored[base_addr + REBASE_DELTA + 123456] = 1
        comparison = Worker.compareWithStoredFunctions(stored, base_addr + REBASE_DELTA, report)
        self.assertEqual((8, 10, 11), (comparison["matched_functions"], comparison["binary_functions"], comparison["stored_functions"]))
        self.assertAlmostEqual(8 / 11, comparison["agreement"])


@pytest.mark.mongo
class MongoSampleVerificationTest(SampleVerificationTest):
    def _storageConfig(self) -> StorageConfig:
        server, port = getTestMongoServerAndPort()
        self._db_name = f"test_sample_verification_{os.getpid()}"
        return StorageConfig(
            STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB,
            STORAGE_SERVER=server,
            STORAGE_PORT=port,
            STORAGE_MONGODB_DBNAME=self._db_name,
            STORAGE_DROP_DISASSEMBLY=False,
            STORAGE_KEEP_SUBMITTED_BINARIES=True,
        )

    def tearDown(self):
        self.storage._getDb().client.drop_database(self._db_name)

    def _setVerified(self, sample_id, sha256_verified):
        if sha256_verified is None:
            # a sample stored before the field existed
            self.storage._getDb().samples.update_one({"sample_id": sample_id}, {"$unset": {"sha256_verified": ""}})
        else:
            self.storage._getDb().samples.update_one({"sample_id": sample_id}, {"$set": {"sha256_verified": sha256_verified}})

    def _setClaim(self, sample_id, token, job_id, claimed_at):
        import datetime

        verification = {"token": token, "job_id": job_id, "claimed_at": datetime.datetime.fromtimestamp(claimed_at, datetime.UTC)}
        self.storage._getDb().samples.update_one({"sample_id": sample_id}, {"$set": {"verification": verification}})

    def _bandFunctionIds(self):
        db = self.storage._getDb()
        return {
            function_id
            for band_number in range(self.config.STORAGE_CONFIG.STORAGE_NUM_BANDS)
            for document in db["band_%d" % band_number].find({}, {"function_ids": 1})
            for function_id in document.get("function_ids", [])
        }

    @staticmethod
    def _failStaging():
        """Fails the insert of the staged function documents, after their disassembly was written."""
        from mcrit.storage.MongoDbStorage import MongoDbStorage

        original = MongoDbStorage._dbInsertMany

        def failing(self, collection, data):
            if collection == "functions_staging":
                original(self, collection, data[:3])
                raise RuntimeError("storage failed while staging")
            return original(self, collection, data)

        return patch.object(MongoDbStorage, "_dbInsertMany", failing)

    def _stagedLeftovers(self) -> int:
        db = self.storage._getDb()
        return sum(db[collection].count_documents({}) for collection in ("functions_staging", "xcfg_staging", "smda_extras_staging"))

    def _storedUnder(self, sample_id: int) -> Dict[str, int]:
        db = self.storage._getDb()
        function_ids = [document["function_id"] for document in db.functions.find({"sample_id": sample_id}, {"function_id": 1})]
        return {
            "functions": len(function_ids),
            "xcfg": db.xcfg.count_documents({}) - db.xcfg.count_documents({"_id": {"$in": [document["function_id"] for document in db.functions.find({}, {"function_id": 1})]}}),
            "function_ranges": db.function_ranges.count_documents({"sample_id": sample_id}),
            "picblockhashes": db.picblockhashes.count_documents({"sample_ids": sample_id}),
            "smda_extras": db.smda_extras.count_documents({"sample_id": sample_id}),
        }

    def _derivedIndexesAgree(self) -> bool:
        """The pichash counts, picblockhash index and function ranges are what a rebuild would make."""
        db = self.storage._getDb()
        counts = {document["_pichash"]: document["df"] for document in db.pichash_counts.find({}, {"_id": 0})}
        expected: Dict[str, int] = {}
        holders: Dict[str, Set[int]] = {}
        ranges: Dict[int, Set[int]] = {}
        for document in db.functions.find({}, {"_id": 0, "_pichash": 1, "_picblockhashes": 1, "sample_id": 1, "function_id": 1}):
            expected[document["_pichash"]] = expected.get(document["_pichash"], 0) + 1
            for block in document.get("_picblockhashes") or []:
                holders.setdefault(block["hash"], set()).add(document["sample_id"])
            ranges.setdefault(document["sample_id"], set()).add(document["function_id"])
        index = {document["_id"]: set(document["sample_ids"]) for document in db.picblockhashes.find({})}
        recorded: Dict[int, Set[int]] = {}
        for document in db.function_ranges.find({}):
            recorded.setdefault(document["sample_id"], set()).update(range(document["first_function_id"], document["last_function_id"] + 1))
        self.assertEqual(expected, counts)
        self.assertEqual(holders, index)
        self.assertEqual(ranges, recorded)
        return True

    def _assertConsistent(self):
        super()._assertConsistent()
        self.assertEqual(0, self.storage._getDb().samples.count_documents({"verification": {"$exists": True}}))

    def testAReplacementInterruptedMidSwapIsFinishedByTheNextUpload(self):
        from mcrit.storage.MongoDbStorage import MongoDbStorage

        sample_entry = self._addReported()
        sample_id = sample_entry.sample_id
        with patch.object(MongoDbStorage, "_addToPicBlockHashIndex", side_effect=RuntimeError("died midway")), self.assertRaises(RuntimeError):
            self._upload(self._report(changed=CHANGED_OFFSETS, missing=MISSING_OFFSET))
        db = self.storage._getDb()
        # past the point of no return: both function sets are under the sample, the swap recorded
        document = db.samples.find_one({"sample_id": sample_id})
        assert document is not None
        self.assertIn("swap", document["verification"])
        self.assertEqual(19, db.functions.count_documents({"sample_id": sample_id}))
        # which matching copes with meanwhile
        query = self._report()
        query.sha256 = "ab" * 32
        self.assertEqual([sample_id], [match["sample_id"] for match in self._worker().getMatchesForSmdaReport(query.toDict())["matches"]["samples"]])
        # the next upload finishes it, with what it says about the sample
        result = self._upload(self._report(changed=CHANGED_OFFSETS, missing=MISSING_OFFSET), family="second_upload_family")
        self.assertEqual(
            {"status": "replaced", "resumed": True, "old_num_functions": 10, "new_num_functions": 9, "labels_carried": 9, "labels_dropped": 1},
            result["verification"],
        )
        self.assertIs(True, result["sample_info"]["sha256_verified"])
        self.assertEqual("second_upload_family", result["sample_info"]["family"])
        self.assertEqual(9, result["sample_info"]["statistics"]["num_functions"])
        functions = self._functions(sample_id)
        self.assertEqual(9, len(functions))
        self.assertEqual(9, sum(len(entry.function_labels) for entry in functions.values()))
        self.assertEqual({result["sample_info"]["family_id"]}, {entry.family_id for entry in functions.values()})
        self.assertEqual(9, db.xcfg.count_documents({}))
        self._assertConsistent()
        # hashed after all
        self.assertTrue({entry.function_id for entry in functions.values()} & self._bandFunctionIds())

    def testASampleDeletedMidSwapLeavesNothingBehind(self):
        from mcrit.storage.MongoDbStorage import MongoDbStorage

        sample_entry = self._addReported()
        sample_id = sample_entry.sample_id
        original = MongoDbStorage._addToPicHashCounts
        deleted = []

        def delete_then_count(storage, function_documents):
            # the new functions are under the sample and their counting claimed when it goes
            if not deleted:
                deleted.append(storage.deleteSample(sample_id))
            return original(storage, function_documents)

        with patch.object(MongoDbStorage, "_addToPicHashCounts", delete_then_count):
            result = self._upload(self._report(changed=CHANGED_OFFSETS, missing=MISSING_OFFSET))
        self.assertEqual([True], deleted)
        self.assertEqual("sample_deleted", result["verification"]["status"])
        self.assertEqual(0, sum(self._storedUnder(sample_id).values()))
        self.assertEqual(0, self.storage._getDb().functions.count_documents({}))
        self.assertIsNone(self.storage.getFamily(sample_entry.family_id))
        self._assertConsistent()

    def testASampleDeletedAfterAJobDiedMidSwapLeavesNothingBehind(self):
        from mcrit.storage.MongoDbStorage import MongoDbStorage

        sample_entry = self._addReported()
        sample_id = sample_entry.sample_id
        # dies with both function sets under the sample, the new ones counted, the old ones not yet removed
        with patch.object(MongoDbStorage, "_pullBandEntries", side_effect=RuntimeError("died midway")), self.assertRaises(RuntimeError):
            self._upload(self._report(changed=CHANGED_OFFSETS, missing=MISSING_OFFSET))
        db = self.storage._getDb()
        self.assertEqual(19, db.functions.count_documents({"sample_id": sample_id}))
        self.assertTrue(self.storage.deleteSample(sample_id))
        self.assertEqual(0, sum(self._storedUnder(sample_id).values()))
        self.assertEqual(0, db.functions.count_documents({}))
        self.assertIsNone(self.storage.getFamily(sample_entry.family_id))
        self._assertConsistent()

    def testASwapDuringADeletionStops(self):
        from mcrit.storage.MongoDbStorage import MongoDbStorage

        sample_entry = self._addReported()
        sample_id = sample_entry.sample_id
        self.assertEqual("claimed", self.storage.claimSampleVerification(sample_id, "job", None, 3600))
        self.assertTrue(self.storage.stageSampleAnalysis(sample_id, self._report(changed=CHANGED_OFFSETS), "job"))
        original = MongoDbStorage._getFunctionMinHashesBySampleId
        swapped = []

        def swap_meanwhile(storage, deleted_sample_id):
            # deleteSample has begun, the sample document is still there
            if not swapped:
                swapped.append(storage.swapSampleAnalysis(sample_id, "job"))
            return original(storage, deleted_sample_id)

        with patch.object(MongoDbStorage, "_getFunctionMinHashesBySampleId", swap_meanwhile):
            self.assertTrue(self.storage.deleteSample(sample_id))
        self.assertEqual([{"status": "claim_lost"}], swapped)
        self.assertEqual(0, sum(self._storedUnder(sample_id).values()))
        self.assertEqual(0, self.storage._getDb().functions.count_documents({}))
        self._assertConsistent()

    def _pichashCountsAgreeWithARebuild(self) -> bool:
        db = self.storage._getDb()
        stored = {document["_pichash"]: document["df"] for document in db.pichash_counts.find({}, {"_id": 0})}
        self.storage.rebuildPicHashCountIndex()
        return stored == {document["_pichash"]: document["df"] for document in db.pichash_counts.find({}, {"_id": 0})}

    def testAFailureAddingThePicHashCountsMarksThemForARebuild(self):
        from mcrit.storage.MongoDbStorage import MongoDbStorage

        self._addReported()
        with patch.object(MongoDbStorage, "_addToPicHashCounts", side_effect=RuntimeError("failed adding counts")), self.assertRaises(RuntimeError):
            self._upload(self._report(changed=CHANGED_OFFSETS))
        # a failed add may have landed in part, so the counts wait for a rebuild
        self.assertFalse(self.storage.isPicHashCountIndexComplete())
        result = self._upload(self._report(changed=CHANGED_OFFSETS))
        self.assertEqual("replaced", result["verification"]["status"])
        self.assertFalse(self.storage.isPicHashCountIndexComplete())
        self.storage.rebuildPicHashCountIndex()
        self.assertTrue(self.storage.isPicHashCountIndexComplete())
        self.assertTrue(self._pichashCountsAgreeWithARebuild())

    def testCountsAJobDiedAddingAreRebuiltNotTrusted(self):
        from mcrit.storage.MongoDbStorage import MongoDbStorage

        class Died(BaseException):
            """A job killed outright: nothing of it gets to clean up."""

        sample_entry = self._addReported()
        original = MongoDbStorage._addToPicHashCounts

        def partial_add(storage, function_documents):
            original(storage, function_documents[:4])
            raise Died()

        with patch.object(MongoDbStorage, "_addToPicHashCounts", partial_add), self.assertRaises(Died):
            self._upload(self._report(changed=CHANGED_OFFSETS))
        db = self.storage._getDb()
        db.samples.update_one({"sample_id": sample_entry.sample_id}, {"$set": {"verification.claimed_at": MongoSampleVerificationTest._longAgo()}})
        result = self._upload(self._report(changed=CHANGED_OFFSETS))
        self.assertEqual("replaced", result["verification"]["status"])
        self.assertTrue(result["verification"]["resumed"])
        # partial counts are never trusted as they are: the cutoff falls back until a rebuild
        self.assertFalse(self.storage.isPicHashCountIndexComplete())
        self.storage.rebuildPicHashCountIndex()
        self.assertTrue(self.storage.isPicHashCountIndexComplete())
        self._assertConsistent()

    def testAJobLosingItsClaimBeforeThePointOfNoReturnLeavesTheMetadata(self):
        from mcrit.storage.MongoDbStorage import MongoDbStorage

        sample_entry = self._addReported()
        sample_id = sample_entry.sample_id
        original = MongoDbStorage._markSwapStep

        def taken_over_first(storage, marked_sample_id, token, step, value=True):
            if step == "started":
                self._setClaim(sample_id, "taken-over", None, time.time())
            return original(storage, marked_sample_id, token, step, value)

        with patch.object(MongoDbStorage, "_markSwapStep", taken_over_first):
            result = self._upload(self._report(changed=CHANGED_OFFSETS), family="uploaded_family")
        self.assertEqual("claim_lost", result["verification"]["status"])
        stored = self.storage.getSampleById(sample_id)
        assert stored is not None
        self.assertEqual(("reported_family", sample_entry.family_id), (stored.family, stored.family_id))
        self.assertIsNone(self.storage.getFamilyId("uploaded_family"))
        self.assertEqual(10, len(self._functions(sample_id)))
        self.assertEqual(0, self.storage.recomputeFamilyStats()["num_families_corrected"])

    def testDeleteSampleCountsByTheFamilyItClaimedTheSampleIn(self):
        from mcrit.storage.MongoDbStorage import MongoDbStorage

        sample_entry = self._addReported()
        original = MongoDbStorage.getSampleById
        moved = []

        def read_then_moved(storage, sample_id):
            entry = original(storage, sample_id)
            if not moved:
                moved.append(sample_id)
                # lands after deleteSample's first read, before it takes the claim
                self.assertTrue(storage.modifySample(sample_id, {"family_name": "moved_family", "is_library": True}))
            return entry

        with patch.object(MongoDbStorage, "getSampleById", read_then_moved):
            self.assertTrue(self.storage.deleteSample(sample_entry.sample_id))
        self.assertEqual([sample_entry.sample_id], moved)
        self.assertIsNone(self.storage.getFamily(sample_entry.family_id))
        self.assertIsNone(self.storage.getFamilyId("moved_family"))
        self.assertEqual(0, self.storage.recomputeFamilyStats()["num_families_corrected"])

    def testADeleteSampleThatDiedIsNotTakenOverByAnUpload(self):
        sample_entry = self._addReported()
        db = self.storage._getDb()
        db.samples.update_one(
            {"sample_id": sample_entry.sample_id}, {"$set": {"verification": {"token": "deleteSample:x", "deleting": True, "claimed_at": MongoSampleVerificationTest._longAgo()}}}
        )
        self.assertEqual({"status": "in_progress"}, self._upload(self._report())["verification"])

    def testAClaimOfADeadJobIsTakenOverAndItsStagingDiscarded(self):
        sample_entry = self._addReported()
        db = self.storage._getDb()
        db.functions_staging.insert_one({"staging_id": 999, "function_id": 10**6, "sample_id": sample_entry.sample_id, "family_id": 0, "offset": 0})
        db.xcfg_staging.insert_one({"_id": 10**6, "staging_id": 999, "_xcfg": "{}"})
        db.smda_extras_staging.insert_one({"staging_id": 999, "chunk": 0, "num_chunks": 1, "_extras": "{}"})
        stale = {"token": "dead-job", "job_id": None, "claimed_at": MongoSampleVerificationTest._longAgo(), "staging_id": 999, "staging_function_ids": [10**6, 10**6]}
        db.samples.update_one({"sample_id": sample_entry.sample_id}, {"$set": {"verification": stale}})
        result = self._upload(self._report())
        self.assertEqual("replaced", result["verification"]["status"])
        self.assertEqual(0, self._stagedLeftovers())
        self.assertIsNone(db.xcfg.find_one({"_id": 10**6}))
        self._assertConsistent()
        # a live claim is respected
        self._setVerified(sample_entry.sample_id, False)
        self._setClaim(sample_entry.sample_id, "live-job", None, time.time())
        self.assertEqual({"status": "in_progress"}, self._upload(self._report())["verification"])

    @staticmethod
    def _longAgo():
        import datetime

        return datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC)
