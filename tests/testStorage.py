import json
import logging
import os
from unittest import TestCase, main
from unittest.mock import patch

import pytest
from smda.common.SmdaReport import SmdaReport

from mcrit.config.McritConfig import McritConfig
from mcrit.config.MinHashConfig import MinHashConfig
from mcrit.config.QueueConfig import QueueConfig
from mcrit.config.ShinglerConfig import ShinglerConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.index.SearchCursor import FullSearchCursor
from mcrit.index.SearchQueryParser import SearchQueryParser
from mcrit.minhash.MinHash import MinHash
from mcrit.storage.FunctionEntry import FunctionEntry
from mcrit.storage.MongoDbStorage import MongoDbStorage
from mcrit.storage.SampleEntry import SampleEntry
from mcrit.storage.StorageFactory import StorageFactory

from .context import getTestMongoServerAndPort

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logging.disable(logging.CRITICAL)


def buildMongoStorageConfig(dbname):
    """A mongo StorageConfig pointed at the server TEST_MONGODB names."""
    server, port = getTestMongoServerAndPort()
    return StorageConfig(
        STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB,
        STORAGE_SERVER=server,
        STORAGE_PORT=port,
        STORAGE_MONGODB_DBNAME=dbname,
    )


class MemoryStorageTest(TestCase):
    def setUp(self):
        self._storage_config = StorageConfig(
            STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MEMORY,
            STORAGE_DROP_DISASSEMBLY=False,
        )
        mcrit_config = McritConfig()
        mcrit_config.STORAGE_CONFIG = self._storage_config
        mcrit_config.MINHASH_CONFIG = MinHashConfig()
        mcrit_config.MINHASH_CONFIG.MINHASH_SIGNATURE_LENGTH = 10
        mcrit_config.MINHASH_CONFIG.MINHASH_SIGNATURE_BITS = 8
        mcrit_config.SHINGLER_CONFIG = ShinglerConfig()
        mcrit_config.QUEUE_CONFIG = QueueConfig()
        self.storage = StorageFactory.getStorage(mcrit_config)
        # get example_file_path
        THIS_FILE_PATH = str(os.path.abspath(__file__))
        PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
        self.example_file_path = os.sep.join([PROJECT_ROOT, "tests", "example_report.smda"])

    def tearDown(self):
        self.storage.clearStorage()

    def testBasicStorageUsage(self):
        self.storage.clearStorage()
        smda_report = SmdaReport.fromFile(self.example_file_path)
        assert smda_report is not None
        self.storage.addSmdaReport(smda_report)
        stats = self.storage.getStats()
        self.assertEqual(1, stats["num_samples"])
        self.assertEqual(10, stats["num_functions"])
        self.assertEqual(10, stats["num_pichashes"])
        # without pichash aggregation, the field is reported as None instead of a misleading 0
        stats_without_pichash = self.storage.getStats(with_pichash=False)
        self.assertEqual(1, stats_without_pichash["num_samples"])
        self.assertEqual(10, stats_without_pichash["num_functions"])
        self.assertIsNone(stats_without_pichash["num_pichashes"])

    def testModifyFunction(self):
        # fkie-cad/mcritweb#72: a function's name can be set; the name is recorded as a label
        # by the user who set it, once per (user, name); query functions and unknown ids are refused
        self.storage.clearStorage()
        smda_report = SmdaReport.fromFile(self.example_file_path)
        assert smda_report is not None
        sample_entry = self.storage.addSmdaReport(smda_report)
        assert sample_entry is not None
        function_entry = (self.storage.getFunctionsBySampleId(sample_entry.sample_id) or [])[0]
        function_id = function_entry.function_id
        labels_before = len(function_entry.function_labels)
        self.assertTrue(self.storage.modifyFunction(function_id, {"function_name": "decrypt_config"}, username="alice"))
        modified = self.storage.getFunctionById(function_id)
        assert modified is not None
        self.assertEqual("decrypt_config", modified.function_name)
        self.assertEqual(labels_before + 1, len(modified.function_labels))
        self.assertEqual(("decrypt_config", "alice"), (modified.function_labels[-1].function_label, modified.function_labels[-1].username))
        # the same label by the same user is not recorded twice, by another user it is
        self.assertTrue(self.storage.modifyFunction(function_id, {"function_name": "decrypt_config"}, username="alice"))
        self.assertTrue(self.storage.modifyFunction(function_id, {"function_name": "decrypt_config"}, username="bob"))
        modified = self.storage.getFunctionById(function_id)
        assert modified is not None
        self.assertEqual(labels_before + 2, len(modified.function_labels))
        self.assertEqual("bob", modified.function_labels[-1].username)
        # without a user the label is anonymous; an empty name clears the name and records nothing
        self.assertTrue(self.storage.modifyFunction(function_id, {"function_name": "sub_1234"}))
        modified = self.storage.getFunctionById(function_id)
        assert modified is not None
        self.assertEqual("anonymous", modified.function_labels[-1].username)
        self.assertTrue(self.storage.modifyFunction(function_id, {"function_name": ""}, username="alice"))
        modified = self.storage.getFunctionById(function_id)
        assert modified is not None
        self.assertEqual("", modified.function_name)
        self.assertEqual(labels_before + 3, len(modified.function_labels))
        # the other functions of the sample are untouched, the entry round-trips through toDict
        untouched = (self.storage.getFunctionsBySampleId(sample_entry.sample_id) or [])[1]
        self.assertEqual(labels_before, len(untouched.function_labels))
        self.assertEqual(modified.toDict()["function_labels"], [label.toDict() for label in modified.function_labels])
        self.assertFalse(self.storage.modifyFunction(function_id + 100000, {"function_name": "x"}, username="alice"))
        self.assertFalse(self.storage.modifyFunction(-1, {"function_name": "x"}, username="alice"))
        self.assertTrue(self.storage.modifyFunction(function_id, {}, username="alice"))

    def testStatusAnswersInlineXcfgOnMemoryStorage(self):
        # the interface default is None ("not applicable"), so /status must keep working on
        # backends without the pre-split shape - a raise here broke every memory-backed call.
        # Any definitive answer is fine; what must not happen is an exception.
        self.assertIn(self.storage.hasInlineXcfgRemaining(), (True, False, None))
        # this index is memory-backed regardless of the suite it runs in, so it exercises the
        # interface default specifically: answered, and omitted from /status rather than False
        from .context import config as memory_index_config

        status = MinHashIndex(memory_index_config).getStatus(with_pichash=False)["status"]
        self.assertNotIn("inline_xcfg_remaining", status)

    def testFamilyHandling(self):
        self.storage.clearStorage()
        self.storage.addFamily("family_1")
        self.storage.addFamily("family_2")
        id_3 = self.storage.addFamily("family_3")
        id_3_again = self.storage.addFamily("family_3")
        self.assertEqual(id_3, 3)
        self.assertEqual(id_3_again, 3)

        # family 0 is default: ""
        self.assertEqual(0, self.storage.getFamilyId(""))
        self.assertEqual("", self.storage.getFamily(0).family_name)
        self.assertEqual(4, len(self.storage.getFamilyIds()))
        self.assertEqual("family_1", self.storage.getFamily(1).family_name)
        self.assertEqual(3, self.storage.getFamilyId("family_3"))
        self.assertIsNone(self.storage.getFamily(1000))
        self.assertIsNone(self.storage.getFamilyId("nonexistent"))

        # family modification
        self.storage.modifyFamily(1, {"family_name": "family_1a"})
        self.assertEqual(None, self.storage.getFamilyId("family_1"))
        self.assertEqual(4, self.storage.getFamilyId("family_1a"))
        # family deletion
        self.storage.deleteFamily(4)
        self.assertEqual(None, self.storage.getFamilyId("family_1a"))
        # actors (#57): set, normalised, kept through a rename, cleared
        self.assertEqual([], self.storage.getFamily(2).actors)
        self.assertTrue(self.storage.modifyFamily(2, {"actors": [" Actor A", "Actor B", "Actor A"]}))
        self.assertEqual(["Actor A", "Actor B"], self.storage.getFamily(2).actors)
        self.storage.modifyFamily(2, {"family_name": "family_2b", "actors": ["Actor C"]})
        family_2b = self.storage.getFamilyId("family_2b")
        self.assertIsNotNone(family_2b)
        self.assertTrue(self.storage.modifyFamily(family_2b, {"actors": []}))
        self.assertEqual([], self.storage.getFamily(family_2b).actors)
        self.assertFalse(self.storage.modifyFamily(4242, {"actors": ["x"]}))
        # a rename keeps the actors, and a rename onto an existing family merges both lists
        self.storage.modifyFamily(family_2b, {"actors": ["Actor D"]})
        self.storage.modifyFamily(family_2b, {"family_name": "family_2c"})
        family_2c = self.storage.getFamilyId("family_2c")
        self.assertEqual(["Actor D"], self.storage.getFamily(family_2c).actors)
        self.assertEqual(None, self.storage.getFamilyId("family_2b"))
        other = self.storage.addFamily("family_2d")
        self.storage.modifyFamily(other, {"actors": ["Actor E", "Actor D"]})
        self.storage.modifyFamily(family_2c, {"family_name": "family_2d"})
        self.assertEqual(["Actor E", "Actor D"], self.storage.getFamily(other).actors)

    def _twoReports(self, family="family_1"):
        with open(self.example_file_path) as fjson:
            smda_json = json.load(fjson)
        report_a = SmdaReport.fromDict(smda_json)
        report_b = SmdaReport.fromDict(smda_json)
        assert report_a is not None and report_b is not None
        report_a.family = report_b.family = family
        report_b.sha256 = 64 * "b"
        return report_a, report_b

    def _actualFamilyCounts(self, family_id):
        samples = [s for s in self.storage.getSamples(0, 1000) if s.family_id == family_id]
        num_functions = sum(len(self.storage.getFunctionsBySampleId(s.sample_id) or []) for s in samples)
        return {"num_samples": len(samples), "num_functions": num_functions, "num_library_samples": len([s for s in samples if s.is_library])}

    def _storedFamilyCounts(self, family_id):
        family = self.storage.getFamily(family_id)
        assert family is not None
        return {"num_samples": family.num_samples, "num_functions": family.num_functions, "num_library_samples": family.num_library_samples}

    def testFamilyStatsFollowTheSamplesThroughMovesAndDeletions(self):
        # #151: the per-family counters are what the samples and functions say, after every write path
        self.storage.clearStorage()
        report_a, report_b = self._twoReports()
        sample_a = self.storage.addSmdaReport(report_a)
        sample_b = self.storage.addSmdaReport(report_b)
        assert sample_a is not None and sample_b is not None
        family_1 = sample_a.family_id
        self.assertEqual({"num_samples": 2, "num_functions": 20, "num_library_samples": 0}, self._storedFamilyCounts(family_1))
        self.storage.modifySample(sample_a.sample_id, {"is_library": True})
        self.assertEqual(1, self._storedFamilyCounts(family_1)["num_library_samples"])
        self.storage.modifySample(sample_a.sample_id, {"is_library": True})  # unchanged: no double count
        self.assertEqual(1, self._storedFamilyCounts(family_1)["num_library_samples"])
        # moving a sample moves its counts, and the same family is not a move at all
        self.storage.modifySample(sample_a.sample_id, {"family_name": "family_1"})
        self.assertEqual(self._actualFamilyCounts(family_1), self._storedFamilyCounts(family_1))
        self.storage.modifySample(sample_a.sample_id, {"family_name": "family_2"})
        family_2 = self.storage.getFamilyId("family_2")
        self.assertEqual({"num_samples": 1, "num_functions": 10, "num_library_samples": 1}, self._storedFamilyCounts(family_2))
        self.assertEqual({"num_samples": 1, "num_functions": 10, "num_library_samples": 0}, self._storedFamilyCounts(family_1))
        self.assertEqual(self._actualFamilyCounts(family_2), self._storedFamilyCounts(family_2))
        # deleting the last sample of a family deletes the family; deleting one of two keeps it
        self.storage.deleteSample(sample_a.sample_id)
        self.assertIsNone(self.storage.getFamily(family_2))
        self.assertEqual({"num_samples": 1, "num_functions": 10, "num_library_samples": 0}, self._storedFamilyCounts(family_1))
        self.assertEqual(
            {"num_families": 0, "num_families_corrected": 0, "num_families_created": 0, "corrections": {}} | {"num_families": len(self.storage.getFamilyIds())},
            self.storage.recomputeFamilyStats(),
        )

    def testRenamingAFamilyToItsOwnNameChangesNothing(self):
        # the name resolves to the family itself: merging it into itself deleted the family (mongo),
        # raised KeyError (memory), and doubled the counters of family 0, whose name is ""
        self.storage.clearStorage()
        report_a, report_b = self._twoReports()
        report_b.family = ""
        sample_a = self.storage.addSmdaReport(report_a)
        sample_b = self.storage.addSmdaReport(report_b)
        assert sample_a is not None and sample_b is not None
        family_1 = sample_a.family_id
        self.assertEqual(0, sample_b.family_id)
        counts_before = {family_id: self._storedFamilyCounts(family_id) for family_id in (0, family_1)}
        self.assertTrue(self.storage.modifyFamily(family_1, {"family_name": "family_1"}))
        self.assertIsNotNone(self.storage.getFamily(family_1))
        self.assertTrue(self.storage.modifyFamily(0, {"family_name": ""}))
        for family_id, counts in counts_before.items():
            self.assertEqual(counts, self._storedFamilyCounts(family_id))
            self.assertEqual(self._actualFamilyCounts(family_id), self._storedFamilyCounts(family_id))
        self.assertEqual("family_1", self.storage.getFamily(family_1).family_name)
        self.assertEqual(family_1, self.storage.getSampleById(sample_a.sample_id).family_id)
        self.assertEqual(0, self.storage.getSampleById(sample_b.sample_id).family_id)
        # the rest of the update still applies
        self.assertTrue(self.storage.modifyFamily(family_1, {"family_name": "family_1", "is_library": True}))
        self.assertTrue(self.storage.getSampleById(sample_a.sample_id).is_library)
        self.assertEqual(1, self._storedFamilyCounts(family_1)["num_library_samples"])

    def testRenamingAFamilyToItsOwnNameLeavesAnotherOfTheSameNameAlone(self):
        # names are not unique: resolving the name found the other family, and merged this one into it
        self.storage.clearStorage()
        report_a, report_b = self._twoReports()
        report_b.family = "family_2"
        sample_a = self.storage.addSmdaReport(report_a)
        sample_b = self.storage.addSmdaReport(report_b)
        assert sample_a is not None and sample_b is not None
        family_1, family_2 = sample_a.family_id, sample_b.family_id
        self._nameFamilyInStorage(family_2, "family_1")
        self.assertEqual(family_1, self.storage.getFamilyId("family_1"))
        counts_before = {family_id: self._storedFamilyCounts(family_id) for family_id in (family_1, family_2)}
        self.assertTrue(self.storage.modifyFamily(family_2, {"family_name": "family_1"}))
        for family_id, counts in counts_before.items():
            self.assertEqual(counts, self._storedFamilyCounts(family_id))
        self.assertEqual(family_2, self.storage.getSampleById(sample_b.sample_id).family_id)

    def testRenamingAFamilyReindexesEachFunctionUnderItsOwnSample(self):
        # MemoryStorage re-keyed the moved functions with whichever sample its loop over all samples
        # had ended on, so renaming a family whose samples were not the last ones stored raised KeyError
        self.storage.clearStorage()
        report_a, report_b = self._twoReports()
        report_b.family = "family_2"
        sample_a = self.storage.addSmdaReport(report_a)
        sample_b = self.storage.addSmdaReport(report_b)
        assert sample_a is not None and sample_b is not None
        family_1, family_2 = sample_a.family_id, sample_b.family_id
        self.assertTrue(self.storage.modifyFamily(family_1, {"family_name": "family_1a"}))
        family_1a = self.storage.getFamilyId("family_1a")
        self.assertEqual(family_1a, self.storage.getSampleById(sample_a.sample_id).family_id)
        self.assertEqual(self._actualFamilyCounts(family_1a), self._storedFamilyCounts(family_1a))
        # both reports hold the same functions, so each pichash names one function of each sample
        for function_entry in self.storage.getFunctionsBySampleId(sample_a.sample_id) or []:
            self.assertEqual(family_1a, function_entry.family_id)
            pichash_matches = self.storage.getMatchesForPicHash(function_entry.pichash)
            self.assertIn((family_1a, sample_a.sample_id, function_entry.function_id), pichash_matches)
            self.assertNotIn((family_1, sample_a.sample_id, function_entry.function_id), pichash_matches)
            self.assertEqual({family_1a, family_2}, {family_id for family_id, _, _ in pichash_matches})

    def testRecomputeFamilyStatsCorrectsDriftedCounters(self):
        self.storage.clearStorage()
        report_a, _ = self._twoReports()
        sample_a = self.storage.addSmdaReport(report_a)
        assert sample_a is not None
        family_id = sample_a.family_id
        self._driftFamilyCounters(family_id, num_samples=5, num_functions=3)
        self.assertEqual({"num_samples": 5, "num_functions": 3, "num_library_samples": 0}, self._storedFamilyCounts(family_id))
        report = self.storage.recomputeFamilyStats()
        self.assertEqual(1, report["num_families_corrected"])
        self.assertEqual(
            {"before": {"num_samples": 5, "num_functions": 3, "num_library_samples": 0}, "after": {"num_samples": 1, "num_functions": 10, "num_library_samples": 0}},
            report["corrections"][family_id],
        )
        self.assertEqual({"num_samples": 1, "num_functions": 10, "num_library_samples": 0}, self._storedFamilyCounts(family_id))
        stats = self.storage.getStats(with_pichash=False)
        self.assertEqual(1, stats["num_samples"])
        self.assertEqual(10, stats["num_functions"])

    def _nameFamilyInStorage(self, family_id, family_name):
        # bypasses modifyFamily, the way recomputeFamilyStats re-creates a missing family document
        # under the name its samples carry, whatever other family has it
        self.storage._families[family_id].family_name = family_name

    def _driftFamilyCounters(self, family_id, num_samples, num_functions):
        family = self.storage._families[family_id]
        family.num_samples = num_samples
        family.num_functions = num_functions

    def testMinHashesOfOneSampleCanBeDroppedAndTheirVersionTracked(self):
        # #142
        self.storage.clearStorage()
        with open(self.example_file_path) as fjson:
            report = SmdaReport.fromDict(json.load(fjson))
        assert report is not None
        sample_entry = self.storage.addSmdaReport(report)
        assert sample_entry is not None
        sample_id = sample_entry.sample_id
        minhashes = [MinHash(function_id=function_id, minhash_signature=[0x30 + function_id + index for index in range(10)], minhash_bits=8) for function_id in [0, 2, 3, 5, 7, 8]]
        self.storage.addMinHashes(minhashes)
        # nothing recorded yet: stale; recorded below the threshold: stale; at or above: current
        self.assertEqual([sample_id], self.storage.getSamplesWithStaleMinHashes("4.4.5"))
        self.assertEqual(1, self.storage.countSamplesWithStaleMinHashes("4.4.5"))
        self.storage.setMinHashVersionForSamples("4.4.0", [sample_id])
        self.assertEqual([sample_id], self.storage.getSamplesWithStaleMinHashes("4.4.5"))
        self.storage.setMinHashVersionForSamples("4.4.5", [sample_id])
        self.assertEqual([], self.storage.getSamplesWithStaleMinHashes("4.4.5"))
        self.assertEqual(0, self.storage.countSamplesWithStaleMinHashes("4.4.5"))
        self.storage.setMinHashVersionForSamples("4.9.9")  # all samples
        self.assertEqual([], self.storage.getSamplesWithStaleMinHashes("4.9.9"))
        # dropping the sample's minhashes empties its functions and the band index, and forgets the version
        self.assertEqual(6, self.storage.deleteMinHashesForSample(sample_id))
        self.assertTrue(all(not f.minhash for f in self.storage.getFunctionsBySampleId(sample_id)))
        self.assertEqual(0, self._numBandEntries())
        self.assertEqual([sample_id], self.storage.getSamplesWithStaleMinHashes("4.4.5"))
        self.assertEqual(0, self.storage.deleteMinHashesForSample(sample_id))
        self.assertEqual(0, self.storage.deleteMinHashesForSample(4242))

    def _numBandEntries(self):
        return sum(len(function_ids) for band in self.storage._bands.values() for function_ids in band.values())

    def testSampleHandling(self):
        self.storage.clearStorage()
        # TODO: different samples required, because addSmdaReport wont accept identical hashes
        with open(self.example_file_path) as fjson:
            smda_json = json.load(fjson)
        smda_report_a = SmdaReport.fromDict(smda_json)
        assert smda_report_a is not None
        smda_report_a.family = "family_1"
        smda_report_a.is_library = False
        smda_report_a.sha256 = 64 * "a"
        smda_report_b = SmdaReport.fromDict(smda_json)
        assert smda_report_b is not None
        smda_report_b.family = "family_1"
        smda_report_b.is_library = False
        smda_report_b.sha256 = 64 * "b"
        smda_report_c = SmdaReport.fromDict(smda_json)
        assert smda_report_c is not None
        smda_report_c.family = "family_2"
        smda_report_c.is_library = False
        smda_report_c.sha256 = 64 * "c"
        smda_report_d = SmdaReport.fromDict(smda_json)
        assert smda_report_d is not None
        smda_report_d.family = "family_3"
        smda_report_d.is_library = True
        smda_report_d.version = "3.42"
        smda_report_d.sha256 = 64 * "d"
        self.storage.addSmdaReport(smda_report_a)
        self.storage.addSmdaReport(smda_report_b)
        self.storage.addSmdaReport(smda_report_c)
        sample_entry_d = self.storage.addSmdaReport(smda_report_d)
        # produce minhashes for  later testing of clean deletion
        unhashed_function_ids = self.storage.getUnhashedFunctions(None, only_function_ids=True)
        unhashed_functions = self.storage.getUnhashedFunctions(unhashed_function_ids)
        # minhashes = self.calculateMinHashes(unhashed_functions)
        from smda.common.BinaryInfo import BinaryInfo
        from smda.common.SmdaFunction import SmdaFunction

        from mcrit.minhash.MinHasher import MinHasher

        minhasher = MinHasher(MinHashConfig(), ShinglerConfig())
        minhashes = []
        smda_functions = []
        for func in unhashed_functions:
            binary_info = BinaryInfo(b"")
            binary_info.architecture = func.architecture
            smda_functions.append((func.function_id, SmdaFunction.fromDict(func.xcfg, binary_info=binary_info)))
        smda_functions = [(function_id, smda_function) for function_id, smda_function in smda_functions if minhasher.isMinHashableFunction(smda_function)]
        for smda_function in smda_functions:
            minhashes.append(minhasher.calculateMinHashFromStorage(smda_function))
        if minhashes:
            self.storage.addMinHashes(minhashes)
        # start tests
        self.assertIsInstance(sample_entry_d, SampleEntry)
        self.assertEqual(sample_entry_d.sample_id, 3)
        self.assertEqual(None, self.storage.addSmdaReport(smda_report_d))

        self.assertEqual([0, 1, 2, 3], self.storage.getSampleIds())
        self.assertTrue(self.storage.isSampleId(0))
        self.assertFalse(self.storage.isSampleId(4))
        self.assertEqual(None, self.storage.getSampleById(4))
        self.assertEqual(2, self.storage.getSampleById(2).sample_id)
        self.assertEqual(None, self.storage.getSampleIdByFunctionId(40))
        self.assertEqual(3, self.storage.getSampleIdByFunctionId(30))
        self.assertEqual(None, self.storage.getSamplesByFamilyId(4))
        self.assertEqual([0, 1], [s.sample_id for s in self.storage.getSamplesByFamilyId(1)])
        self.assertEqual(None, self.storage.getLibraryInfoForSampleId(2))
        self.assertEqual({"family": "family_3", "version": "3.42"}, self.storage.getLibraryInfoForSampleId(3))

        self.assertEqual(None, self.storage.getLibraryInfoForSampleId(1000))

        self.assertEqual(0, self.storage.getSampleBySha256(64 * "a").sample_id)
        self.assertEqual(None, self.storage.getSampleBySha256(64 * "z"))

        # test modifications
        self.storage.modifySample(3, {"family_name": "changed_family", "version": "new_version", "component": "new_component", "is_library": True})
        self.assertEqual("changed_family", self.storage.getSampleById(3).family)
        self.assertEqual("new_version", self.storage.getSampleById(3).version)
        self.assertEqual("new_component", self.storage.getSampleById(3).component)
        self.assertEqual(True, self.storage.getSampleById(3).is_library)

        # test deletions
        self.assertFalse(self.storage.deleteSample(1000))
        functions_to_be_deleted = self.storage.getFunctionsBySampleId(3)
        function_ids_to_be_deleted = [f.function_id for f in functions_to_be_deleted]
        minhashes_of_deleted_functions = [f.getMinHash(minhash_bits=MinHashConfig.MINHASH_SIGNATURE_BITS) for f in functions_to_be_deleted if f.minhash]
        delete_result = self.storage.deleteSample(3)
        self.assertTrue(delete_result)
        self.assertEqual(None, self.storage.getSampleById(3))
        # functions, minhashes will be cascadically deleted
        self.assertEqual(None, self.storage.getSampleIdByFunctionId(30))
        # no function id should be contained in minhash bands
        for minhash in minhashes_of_deleted_functions:
            candidates = self.storage.getCandidatesForMinHash(minhash)
            self.assertTrue(len(set(candidates).intersection(set(function_ids_to_be_deleted))) == 0)
        new_report_d = self.storage.addSmdaReport(smda_report_d)
        self.assertIsNotNone(new_report_d)
        self.assertEqual(new_report_d.sample_id, 4)
        self.assertTrue(self.storage.isFunctionId(49))

    def testGetSampleBySha256(self):
        self.storage.clearStorage()
        # an empty collection must yield None straight from the lookup, without a count-based emptiness guard
        self.assertEqual(None, self.storage.getSampleBySha256(64 * "0"))
        smda_report = SmdaReport.fromFile(self.example_file_path)
        sample_entry = self.storage.addSmdaReport(smda_report)
        self.assertEqual(sample_entry.sample_id, self.storage.getSampleBySha256(smda_report.sha256).sample_id)
        self.assertEqual(None, self.storage.getSampleBySha256(64 * "0"))

    def testFunctionHandling(self):
        self.storage.clearStorage()
        # TODO use SmdaReport.fromFile
        with open(self.example_file_path) as fjson:
            smda_json = json.load(fjson)
        smda_report_a = SmdaReport.fromDict(smda_json)
        assert smda_report_a is not None
        smda_report_a.sha256 = 64 * "a"
        smda_report_a.family = "family_1"
        smda_report_b = SmdaReport.fromDict(smda_json)
        assert smda_report_b is not None
        smda_report_b.family = "family_1"
        smda_report_b.sha256 = 64 * "b"
        self.storage.addSmdaReport(smda_report_a)
        self.storage.addSmdaReport(smda_report_b)

        self.assertTrue(self.storage.isFunctionId(0))
        self.assertTrue(self.storage.isFunctionId(1))
        self.assertFalse(self.storage.isFunctionId(30))
        functions = self.storage.getFunctionsBySampleId(1)
        self.assertIsNotNone(functions)
        self.assertEqual(list(range(10, 20)), [entry.function_id for entry in functions])

        function = self.storage.getFunctionById(1, with_xcfg=False)
        self.assertIsNone(function.xcfg)
        function = self.storage.getFunctionById(1, with_xcfg=True)
        self.assertEqual(1, function.function_id)
        self.assertNotEqual({}, function.xcfg)
        self.storage.deleteXcfgForSampleId(function.sample_id)
        function = self.storage.getFunctionById(1, with_xcfg=True)
        self.assertEqual({}, function.xcfg)
        function2 = self.storage.getFunctionById(15, with_xcfg=True)
        self.assertNotEqual({}, function2.xcfg)
        self.storage.deleteXcfgData()
        function2 = self.storage.getFunctionById(15, with_xcfg=True)
        self.assertEqual({}, function2.xcfg)

        self.assertIsNone(self.storage.getFunctionById(1000))
        functions = self.storage.getFunctionsBySampleId(1000)
        self.assertIsNone(functions)

        self.storage.deleteXcfgForSampleId(1000)

    def testHashHandling(self):
        storage_config = StorageConfig()
        storage_config.STORAGE_BANDS = {2: 2, 3: 8}
        storage_config.STORAGE_BAND_SEED = 0

        self.storage.clearStorage()
        with open(self.example_file_path) as fjson:
            smda_json = json.load(fjson)
        smda_report_a = SmdaReport.fromDict(smda_json)
        assert smda_report_a is not None
        smda_report_a.sha256 = 64 * "a"
        smda_report_a.family = "family_1"
        smda_report_b = SmdaReport.fromDict(smda_json)
        assert smda_report_b is not None
        smda_report_b.family = "family_1"
        smda_report_b.sha256 = 64 * "b"
        self.storage.addSmdaReport(smda_report_a)
        self.storage.addSmdaReport(smda_report_b)

        # pichash tests
        sample_entry = SampleEntry(smda_report_a, sample_id=1, family_id=1)
        smda_function_356 = smda_report_a.getFunction(356)
        assert smda_function_356 is not None
        function_entry = FunctionEntry(sample_entry, smda_function_356, 1)
        # Will this work?
        initial_pichash = function_entry.pichash
        pichashes = self.storage.getPicHashMatchesByFunctionId(1)
        self.assertTrue(initial_pichash in pichashes)
        family_sample_and_function_ids = self.storage.getMatchesForPicHash(initial_pichash)
        self.assertTrue(self.storage.isPicHash(initial_pichash))
        self.assertEqual(set([(1, 0, 1), (1, 1, 11)]), family_sample_and_function_ids)

        not_a_pichash = 0
        self.assertEqual(set(), self.storage.getMatchesForPicHash(not_a_pichash))

        pichashes_by_function_ids = self.storage.getPicHashMatchesByFunctionIds(list(range(10, 20)))
        pichashes_by_sample_id = self.storage.getPicHashMatchesBySampleId(1)
        self.assertEqual(pichashes_by_function_ids, pichashes_by_sample_id)

        self.assertIsNone(self.storage.getPicHashMatchesBySampleId(1000))
        self.assertIsNone(self.storage.getPicHashMatchesByFunctionId(1000))
        self.assertEqual(pichashes, self.storage.getPicHashMatchesByFunctionIds([1, 1, 1000]))

        # minhash tests
        # TODO check if MinHash initialization works
        minhash_a = MinHash(function_id=1, minhash_signature=[0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39], minhash_bits=8)
        minhash_b = MinHash(function_id=3, minhash_signature=[0x30, 0x31, 0x30, 0x33, 0x30, 0x30, 0x30, 0x37, 0x38, 0x39], minhash_bits=8)
        function_entry = self.storage.getFunctionById(1)
        self.assertEqual(b"", function_entry.minhash)
        status = self.storage.addMinHash(minhash_a)
        self.assertTrue(status)
        self.storage.addMinHash(minhash_b)
        function_entry = self.storage.getFunctionById(1)
        minhash_queried = self.storage.getMinHashByFunctionId(1)
        self.assertEqual(minhash_a.getMinHash(), minhash_queried)
        minhash_queried = self.storage.getMinHashByFunctionId(3)
        self.assertEqual(minhash_b.getMinHash(), minhash_queried)

        self.assertFalse(self.storage.addMinHash(MinHash(function_id=1000)))
        self.assertFalse(self.storage.addMinHash(MinHash(function_id=None)))

        self.assertEqual(None, self.storage.getMinHashByFunctionId(1000))

        # minhash band tests
        candidates = self.storage.getCandidatesForMinHash(minhash_a)
        self.assertEqual(set([1, 3]), candidates)

        candidates = self.storage.getCandidatesForMinHashes({1000: minhash_a})
        self.assertEqual({1000: set([1, 3])}, candidates)

        # band rebuild test
        self.storage.rebuildMinhashBandIndex()
        minhash_c = MinHash(function_id=1000, minhash_signature=[0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39], minhash_bits=8)
        candidates_after = self.storage.getCandidatesForMinHashes({1000: minhash_c})
        self.assertEqual(candidates, candidates_after)

    def testMatchingCache(self):
        cache = self.storage.createMatchingCache([])
        self.assertTrue(hasattr(cache, "getMinHashByFunctionId"))
        self.assertTrue(hasattr(cache, "getSampleIdByFunctionId"))

    def testMatchingCacheAllowSelfReturnDoesNotMutateStorage(self):
        self.storage.clearStorage()
        smda_report = SmdaReport.fromFile(self.example_file_path)
        assert smda_report is not None
        self.storage.addSmdaReport(smda_report)

        cache = self.storage.createMatchingCache([0], allow_self_return=True)
        cache_only_entry = self.storage.getFunctionById(0, with_xcfg=True)
        cache_only_entry.sample_id = 999
        cache_only_entry.minhash = b"cache-only-minhash"
        cache.addFunctionEntriesToCache([cache_only_entry])

        self.assertEqual(0, self.storage.getSampleIdByFunctionId(0))
        self.assertNotEqual(b"cache-only-minhash", self.storage.getMinHashByFunctionId(0))
        self.assertEqual(999, cache.getSampleIdByFunctionId(0))
        self.assertEqual(b"cache-only-minhash", cache.getMinHashByFunctionId(0))
        self.assertEqual(set([0]), cache.getFunctionIdsBySampleId(999))

    def testMatchingCacheSupportsQueryFunctions(self):
        self.storage.clearStorage()
        smda_report = SmdaReport.fromFile(self.example_file_path)
        assert smda_report is not None
        query_sample = self.storage.addSmdaReport(smda_report, isQuery=True)
        assert query_sample is not None
        query_function_ids = self.storage.getFunctionIdsBySampleId(query_sample.sample_id)
        assert query_function_ids is not None
        query_function_id = query_function_ids[0]
        query_function = self.storage.getFunctionById(query_function_id)
        assert query_function is not None

        for allow_self_return in (False, True):
            cache = self.storage.createMatchingCache([query_function_id], allow_self_return=allow_self_return)
            self.assertEqual(query_sample.sample_id, cache.getSampleIdByFunctionId(query_function_id))
            self.assertEqual(query_function.minhash, cache.getMinHashByFunctionId(query_function_id))
            self.assertEqual(set([query_function_id]), set(cache.getFunctionIdsBySampleId(query_sample.sample_id)))

    def testMinHashesForQueryFunctionsAreNotBanded(self):
        self.storage.clearStorage()
        smda_report = SmdaReport.fromFile(self.example_file_path)
        query_sample = self.storage.addSmdaReport(smda_report, isQuery=True)
        assert query_sample is not None
        query_function_ids = sorted(self.storage.getFunctionIdsBySampleId(query_sample.sample_id))
        # query functions live in their own collection, keyed by a negative function id
        self.assertTrue(all(function_id < 0 for function_id in query_function_ids))

        single = MinHash(function_id=query_function_ids[0], minhash_signature=[0x30 + index for index in range(10)], minhash_bits=8)
        self.assertTrue(self.storage.addMinHash(single))
        self.assertEqual(single.getMinHash(), self.storage.getMinHashByFunctionId(single.function_id))
        # they are only ever query input, so they must not become candidates for anyone
        self.assertEqual(set(), self.storage.getCandidatesForMinHash(single))

        # the same holds for the bulk path, which additionally skips functions that do not exist
        bulk = MinHash(function_id=query_function_ids[1], minhash_signature=[0x40 + index for index in range(10)], minhash_bits=8)
        unknown = MinHash(function_id=-9999, minhash_signature=[0x50 + index for index in range(10)], minhash_bits=8)
        self.storage.addMinHashes([bulk, unknown])
        self.assertEqual(bulk.getMinHash(), self.storage.getMinHashByFunctionId(bulk.function_id))
        self.assertEqual(set(), self.storage.getCandidatesForMinHash(bulk))
        self.assertIsNone(self.storage.getMinHashByFunctionId(unknown.function_id))
        self.assertFalse(self.storage.addMinHash(unknown))

    def testGetSamplesIsQuery(self):
        self.storage.clearStorage()
        smda_report = SmdaReport.fromFile(self.example_file_path)
        assert smda_report is not None
        query_sample = self.storage.addSmdaReport(smda_report, isQuery=True)
        assert query_sample is not None

        # query samples live in their own collection and must not show up in the regular listing
        self.assertEqual([], self.storage.getSamples(start_index=0, limit=0))
        query_samples = self.storage.getSamples(start_index=0, limit=0, is_query=True)
        self.assertEqual([query_sample.sample_id], [entry.sample_id for entry in query_samples])
        self.assertEqual(query_sample.sample_id, self.storage.getSampleBySha256(smda_report.sha256, is_query=True).sample_id)
        self.assertIsNone(self.storage.getSampleBySha256(smda_report.sha256))

    def testUniqueBlocks(self):
        self.storage.clearStorage()
        smda_report = SmdaReport.fromFile(self.example_file_path)
        assert smda_report is not None
        sample_entry = self.storage.addSmdaReport(smda_report)
        assert sample_entry is not None

        result = self.storage.getUniqueBlocks([sample_entry.sample_id])
        unique_blocks = result["unique_blocks"]
        self.assertTrue(unique_blocks)
        # Worker.getUniqueBlocks reads these off every entry to build the block cover, so both
        # backends have to deliver the same shape (the memory one used to omit three of them)
        for block in unique_blocks.values():
            self.assertEqual({"samples", "length", "function_id", "sample_id", "offset", "instructions", "escaped_sequence", "score"}, set(block.keys()))
        # every block is unique to the only sample in storage
        self.assertEqual(len(unique_blocks), result["statistics"]["unique_blocks_overall"])
        for block in unique_blocks.values():
            self.assertEqual([sample_entry.sample_id], list(block["samples"]))
            # instructions are looked up in the xcfg by block offset - this is where the two
            # backends key their blocks differently (int vs. str after a JSON round trip)
            self.assertTrue(block["instructions"], f"no instructions extracted for block at offset {block['offset']}")

    def testMatchesForPicBlockHash(self):
        self.storage.clearStorage()
        smda_report = SmdaReport.fromFile(self.example_file_path)
        assert smda_report is not None
        sample_entry = self.storage.addSmdaReport(smda_report)
        assert sample_entry is not None

        function_entries = self.storage.getFunctionsBySampleId(sample_entry.sample_id)
        assert function_entries is not None
        picblockhash = next(fe.picblockhashes[0]["hash"] for fe in function_entries if fe.picblockhashes)
        matches = self.storage.getMatchesForPicBlockHash(picblockhash)
        self.assertTrue(matches)
        for match in matches:
            self.assertEqual(4, len(match))
            self.assertEqual(sample_entry.sample_id, match[1])

    def testSubmissionSurvivesAwkwardInstructionBytes(self):
        # #85: submitting a report used to die on a single instruction, because SmdaBasicBlock
        # computed its opcode block hash eagerly and getDetailed() asserted that capstone decodes
        # the stored bytes to exactly one instruction. Both were fixed in SMDA 4.0.0, below the
        # floor declared here, and MCRIT never asks for that hash itself - this pins the boundary
        # so a future SMDA change cannot quietly drag the submission path back into the escaper.
        for label, instruction_bytes, mnemonic, operands in [
            # capstone splits this into `wait` + `fnstcw word ptr [esp]` where SMDA/IDA report one
            ("x87 WAIT prefix", "9bd93c24", "fnstcw", "word ptr [esp]"),
            # bytes capstone cannot decode at all, as a report from another disassembler may carry
            ("undecodable", "0f04", "unknown", ""),
        ]:
            with self.subTest(instruction=label):
                self.storage.clearStorage()
                with open(self.example_file_path) as fjson:
                    smda_json = json.load(fjson)
                function_id = sorted(smda_json["xcfg"])[0]
                block_id = sorted(smda_json["xcfg"][function_id]["blocks"])[0]
                block = smda_json["xcfg"][function_id]["blocks"][block_id]
                block[0] = [block[0][0], instruction_bytes, mnemonic, operands]
                report = SmdaReport.fromDict(smda_json)
                assert report is not None
                report.sha256 = 64 * "e"

                sample_entry = self.storage.addSmdaReport(report)
                self.assertIsNotNone(sample_entry)
                assert sample_entry is not None
                function_entries = self.storage.getFunctionsBySampleId(sample_entry.sample_id)
                self.assertTrue(function_entries)

    def testGetSampleEntriesByIds(self):
        # #111's batch read: one $in per collection instead of one find_one per sample_id
        self.storage.clearStorage()
        report_a, report_b = self._twoReports()
        sample_a = self.storage.addSmdaReport(report_a)
        sample_b = self.storage.addSmdaReport(report_b)
        assert sample_a is not None and sample_b is not None
        query_sample = self.storage.addSmdaReport(report_a, isQuery=True)
        assert query_sample is not None
        self.assertLess(query_sample.sample_id, 0)
        unknown_id = max(sample_a.sample_id, sample_b.sample_id) + 1000
        requested_ids = [sample_a.sample_id, sample_b.sample_id, query_sample.sample_id, unknown_id, sample_a.sample_id]
        entries = self.storage.getSampleEntriesByIds(requested_ids)
        # the unknown id is left out, the duplicate is answered once
        self.assertEqual({sample_a.sample_id, sample_b.sample_id, query_sample.sample_id}, set(entries.keys()))
        self.assertEqual(sample_a.toDict(), entries[sample_a.sample_id].toDict())
        self.assertEqual(sample_b.toDict(), entries[sample_b.sample_id].toDict())
        self.assertEqual(query_sample.toDict(), entries[query_sample.sample_id].toDict())
        self.assertEqual({}, self.storage.getSampleEntriesByIds([]))
        self.assertEqual({}, self.storage.getSampleEntriesByIds([unknown_id]))

    def testGetFamilyEntriesByIds(self):
        self.storage.clearStorage()
        id_a = self.storage.addFamily("family_a")
        id_b = self.storage.addFamily("family_b")
        unknown_id = max(id_a, id_b) + 1000
        entries = self.storage.getFamilyEntriesByIds([id_a, id_b, unknown_id, id_a])
        # the unknown id is left out, the duplicate is answered once
        self.assertEqual({id_a, id_b}, set(entries.keys()))
        self.assertEqual("family_a", entries[id_a].family_name)
        self.assertEqual("family_b", entries[id_b].family_name)
        # batch entries carry no sample list, same as getFamily
        self.assertIsNone(entries[id_a].samples)
        self.assertIsNone(entries[id_b].samples)
        self.assertEqual({}, self.storage.getFamilyEntriesByIds([]))
        self.assertEqual({}, self.storage.getFamilyEntriesByIds([unknown_id]))
        # nor after GET /families/<id> has attached one to what getFamily handed out
        family = self.storage.getFamily(id_a)
        assert family is not None
        family.samples = {}
        self.assertIsNone(self.storage.getFamilyEntriesByIds([id_a])[id_a].samples)
        self.assertNotIn("samples", self.storage.getFamilyEntriesByIds([id_a])[id_a].toDict())


@pytest.mark.mongo
class MongoDbStorageTest(MemoryStorageTest):
    def setUp(self):
        self._storage_config = buildMongoStorageConfig("test_mongodbstorage_mcrit")
        self._storage_config.STORAGE_DROP_DISASSEMBLY = False
        mcrit_config = McritConfig()
        mcrit_config.STORAGE_CONFIG = self._storage_config
        mcrit_config.MINHASH_CONFIG = MinHashConfig()
        mcrit_config.MINHASH_CONFIG.MINHASH_SIGNATURE_LENGTH = 10
        mcrit_config.MINHASH_CONFIG.MINHASH_SIGNATURE_BITS = 8
        mcrit_config.SHINGLER_CONFIG = ShinglerConfig()
        mcrit_config.QUEUE_CONFIG = QueueConfig()
        self.storage = StorageFactory.getStorage(mcrit_config)
        # get example_file_path
        THIS_FILE_PATH = str(os.path.abspath(__file__))
        PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
        self.example_file_path = os.sep.join([PROJECT_ROOT, "tests", "example_report.smda"])

    def testAnOversizedDisassemblyBlobIsDroppedAndTheFunctionKept(self):
        # #42: MongoDB refuses a document over 16 MiB; the whole batch used to fail as
        # "Database insert failed." with nothing saying which document, and the sample was lost
        self.storage.clearStorage()
        db = self.storage._getDb()
        huge = "x" * (16 * 1024 * 1024 + 1)
        with patch("mcrit.storage.MongoDbStorage.LOGGER") as logger:
            self.storage._insertXcfgDocuments([{"function_id": 5, "_xcfg": huge}, {"function_id": 6, "_xcfg": "{}"}])
        self.assertIn("Dropping the disassembly of %d function(s)", logger.warning.call_args.args[0])
        self.assertEqual(1, logger.warning.call_args.args[1])
        self.assertEqual([6], [document["_id"] for document in db.xcfg.find({}, {"_id": 1})])
        error = db.error.find_one({}, sort=[("ts", -1)])
        assert error is not None
        self.assertIn("exceed the 16 MiB limit", error["error_msg"])
        self.assertEqual(5, error["error_details"]["oversized"][0]["document"]["_id"])
        self.assertGreater(error["error_details"]["oversized"][0]["bytes"], 16 * 1024 * 1024)
        self.assertTrue(error["error_details"]["dropped"])

    def testAnOversizedFunctionDocumentFailsNamingItself(self):
        self.storage.clearStorage()
        huge = "x" * (16 * 1024 * 1024 + 1)
        with self.assertRaises(ValueError) as raised:
            self.storage._dbInsertMany("functions", [{"function_id": 8, "sample_id": 1}, {"function_id": 9, "sample_id": 1, "blob": huge}, {"function_id": 10, "sample_id": 1}])
        self.assertIn("16 MiB", str(raised.exception))
        self.assertIn("'function_id': 9", str(raised.exception))
        self.assertNotIn("'function_id': 10", str(raised.exception))
        # the ordered insert stopped at the oversized document
        self.assertEqual([8], [d["function_id"] for d in self.storage._getDb().functions.find({}, {"function_id": 1})])

    def testAnOversizedBlobInTheMiddleKeepsTheOthers(self):
        self.storage.clearStorage()
        huge = "x" * (16 * 1024 * 1024 + 1)
        with patch("mcrit.storage.MongoDbStorage.LOGGER"):
            inserted = self.storage._insertXcfgDocuments(
                [
                    {"function_id": 1, "_xcfg": "{}"},
                    {"function_id": 2, "_xcfg": huge},
                    {"function_id": 3, "_xcfg": "{}"},
                    {"function_id": 4, "_xcfg": huge},
                    {"function_id": 5, "_xcfg": "{}"},
                ]
            )
        self.assertIsNone(inserted)
        self.assertEqual([1, 3, 5], sorted(document["_id"] for document in self.storage._getDb().xcfg.find({}, {"_id": 1})))

    def _driftFamilyCounters(self, family_id, num_samples, num_functions):
        self.storage._getDb().families.update_one({"family_id": family_id}, {"$set": {"num_samples": num_samples, "num_functions": num_functions}})

    def _nameFamilyInStorage(self, family_id, family_name):
        self.storage._getDb().families.update_one({"family_id": family_id}, {"$set": {"family_name": family_name}})

    def testRecomputeCreatesTheFamilyDocumentSamplesReferenceWithoutOne(self):
        # the family_id 1908 case of #151: samples carry an id that no family document describes
        self.storage.clearStorage()
        report_a, _ = self._twoReports()
        sample_a = self.storage.addSmdaReport(report_a)
        assert sample_a is not None
        db = self.storage._getDb()
        db.families.delete_one({"family_id": sample_a.family_id})
        report = self.storage.recomputeFamilyStats()
        self.assertEqual(1, report["num_families_created"])
        self.assertEqual("family_1", self.storage.getFamily(sample_a.family_id).family_name)
        self.assertEqual({"num_samples": 1, "num_functions": 10, "num_library_samples": 0}, self._storedFamilyCounts(sample_a.family_id))

    def testAnEnsuredFamilyIdIsNeverHandedOutAgain(self):
        # an imported family id at or beyond the counter would otherwise be re-issued by addFamily
        self.storage.clearStorage()
        report_a, _ = self._twoReports()
        self.storage.importSampleEntry(SampleEntry(report_a, sample_id=0, family_id=5))
        self.assertGreater(self.storage.addFamily("after_import"), 5)
        self.assertEqual(1, self.storage._getDb().families.count_documents({"family_id": 5}))

    def testAMoveIsCountedOnlyByTheRequestThatPerformsIt(self):
        # of two identical concurrent moves only one changes the sample; the other must not
        # touch the counters (modelled by the second call seeing the sample already moved)
        self.storage.clearStorage()
        report_a, _ = self._twoReports()
        sample_a = self.storage.addSmdaReport(report_a)
        assert sample_a is not None
        family_1 = sample_a.family_id
        self.storage.modifySample(sample_a.sample_id, {"family_name": "family_moved"})
        family_moved = self.storage.getFamilyId("family_moved")
        self.assertEqual(self._actualFamilyCounts(family_moved), self._storedFamilyCounts(family_moved))
        # the sample document is already in family_moved: a stale request for the same move
        db = self.storage._getDb()
        db.samples.update_one({"sample_id": sample_a.sample_id}, {"$set": {"family_id": family_1, "family": "family_1"}})
        db.samples.update_one({"sample_id": sample_a.sample_id}, {"$set": {"family_id": family_moved, "family": "family_moved"}})
        self.storage.modifySample(sample_a.sample_id, {"family_name": "family_moved"})
        self.assertEqual(self._actualFamilyCounts(family_moved), self._storedFamilyCounts(family_moved))

    def testImportingASampleEnsuresItsFamilyDocument(self):
        self.storage.clearStorage()
        report_a, _ = self._twoReports()
        sample_entry = SampleEntry(report_a, sample_id=0, family_id=77)
        self.storage.importSampleEntry(sample_entry)
        family = self.storage.getFamily(77)
        assert family is not None
        self.assertEqual("family_1", family.family_name)
        self.assertEqual(1, family.num_samples)
        self.assertEqual(10, family.num_functions)

    def testAStatsUpdateAgainstAMissingFamilyIsLogged(self):
        self.storage.clearStorage()
        with patch("mcrit.storage.MongoDbStorage.LOGGER") as logger:
            self.storage._updateFamilyStats(4242, 1, 10, 0)
        self.assertIn("has no document", logger.warning.call_args.args[0])
        self.assertEqual(4242, logger.warning.call_args.args[1])

    def _numBandEntries(self):
        db = self.storage._getDb()
        return sum(len(d.get("function_ids", [])) for band_id in range(self.storage._storage_config.STORAGE_NUM_BANDS) for d in db["band_%d" % band_id].find({}))

    def _createSecondStorage(self):
        mcrit_config = McritConfig()
        mcrit_config.STORAGE_CONFIG = self._storage_config
        mcrit_config.MINHASH_CONFIG = MinHashConfig()
        mcrit_config.SHINGLER_CONFIG = ShinglerConfig()
        mcrit_config.QUEUE_CONFIG = QueueConfig()
        return StorageFactory.getStorage(mcrit_config)

    # --- substring search on function_name over the distinct names (fkie-cad/mcritweb#76) ----

    def _storageWithNamedFunctions(self):
        self.storage.clearStorage()
        with open(self.example_file_path) as fjson:
            smda_report = SmdaReport.fromDict(json.load(fjson))
        self.storage.addSmdaReport(smda_report)
        function_ids = sorted(entry.function_id for entry in self.storage.getFunctionsBySampleId(0))
        names = ["qz_alpha", "QZ_Alphabet", "kryptos_config", "KryptosConfig", "sub_401000"]
        for function_id, name in zip(function_ids, names):
            self.storage._getDb().functions.update_one({"function_id": function_id}, {"$set": {"function_name": name}})
        return dict(zip(names, function_ids))

    def _searchFunctionNames(self, term, sort_by="function_id", is_ascending=True):
        parsed = SearchQueryParser().parse(term)
        cursor = FullSearchCursor(None, [(sort_by, is_ascending), ("function_id", True)] if sort_by != "function_id" else [("function_id", is_ascending)])
        return [entry.function_name for entry in self.storage.findFunctionByString(parsed, cursor=cursor, max_num_results=100).values()]

    def testSubstringSearchOnFunctionNamesUsesTheDistinctNames(self):
        by_name = self._storageWithNamedFunctions()
        self.assertEqual(["qz_alpha", "QZ_Alphabet"], self._searchFunctionNames("qz_alph"))
        self.assertEqual(["kryptos_config", "KryptosConfig"], self._searchFunctionNames("KRYPTOS"))
        self.assertEqual(["QZ_Alphabet", "qz_alpha"], self._searchFunctionNames("qz_alph", is_ascending=False))
        self.assertEqual([], self._searchFunctionNames("zzzzq"))
        # the query MongoDB gets is an $in of the matching names, not a regex
        query = self.storage._get_search_query(["function_name"], SearchQueryParser().parse("qz_alph"), None, distinct_fields={"function_name": "functions"})
        self.assertEqual({"function_name": {"$in": ["QZ_Alphabet", "qz_alpha"]}}, {k: {op: sorted(v) for op, v in c.items()} for k, c in query.items()})
        self.assertEqual([entry.function_id for entry in self.storage.findFunctionByString(SearchQueryParser().parse("zzzzq")).values()], [])
        self.assertEqual(sorted(by_name.values())[:2], sorted(entry.function_id for entry in self.storage.findFunctionByString(SearchQueryParser().parse("qz_alph")).values()))

    def testSubstringSearchFallsBackToTheRegexAboveTheCap(self):
        self._storageWithNamedFunctions()
        original_cap = MongoDbStorage._DISTINCT_VALUES_CAP
        try:
            MongoDbStorage._DISTINCT_VALUES_CAP = 2
            self.assertIsNone(self.storage._getDistinctValues("functions", "function_name"))
            query = self.storage._get_search_query(["function_name"], SearchQueryParser().parse("qz_alph"), None, distinct_fields={"function_name": "functions"})
            self.assertTrue(hasattr(query["function_name"], "search"))
            # same answers either way
            self.assertEqual(["qz_alpha", "QZ_Alphabet"], self._searchFunctionNames("qz_alph"))
            self.assertEqual([], self._searchFunctionNames("zzzzq"))
            self.assertEqual(["kryptos_config", "KryptosConfig"], self._searchFunctionNames("KRYPTOS"))
        finally:
            MongoDbStorage._DISTINCT_VALUES_CAP = original_cap

    def testSubstringSearchFallsBackToTheRegexAboveTheByteCap(self):
        # thousands of long mangled symbols stay under the count cap but would not fit one $in
        self._storageWithNamedFunctions()
        original_cap = MongoDbStorage._DISTINCT_VALUES_MAX_BYTES
        try:
            MongoDbStorage._DISTINCT_VALUES_MAX_BYTES = 16
            self.assertIsNone(self.storage._getDistinctValues("functions", "function_name"))
            query = self.storage._get_search_query(["function_name"], SearchQueryParser().parse("qz_alph"), None, distinct_fields={"function_name": "functions"})
            self.assertTrue(hasattr(query["function_name"], "search"))
            self.assertEqual(["qz_alpha", "QZ_Alphabet"], self._searchFunctionNames("qz_alph"))
        finally:
            MongoDbStorage._DISTINCT_VALUES_MAX_BYTES = original_cap

    def testSubstringSearchCombinesWithOtherConditionsAndNegation(self):
        self._storageWithNamedFunctions()
        self.assertEqual(["qz_alpha", "QZ_Alphabet"], self._searchFunctionNames("sample_id:0 qz_alph"))
        self.assertEqual([], self._searchFunctionNames("sample_id:1 qz_alph"))
        excluded = self._searchFunctionNames("function_name:!?qz_alph")
        self.assertNotIn("qz_alpha", excluded)
        self.assertNotIn("QZ_Alphabet", excluded)
        self.assertIn("kryptos_config", excluded)
        self.assertEqual(len(self.storage.getFunctionsBySampleId(0)) - 2, len(excluded))

    def testTheOverCapVerdictIsCachedAcrossSearches(self):
        # on a corpus past the cap the capped scan costs ~0.9 s and always gives the same answer,
        # so it runs once, not on every search
        self._storageWithNamedFunctions()
        original_cap = MongoDbStorage._DISTINCT_VALUES_CAP
        try:
            MongoDbStorage._DISTINCT_VALUES_CAP = 2
            functions = self.storage._getDb().functions
            with patch.object(type(functions), "aggregate", autospec=True, side_effect=type(functions).aggregate) as scan:
                self.assertEqual(["qz_alpha", "QZ_Alphabet"], self._searchFunctionNames("qz_alph"))
                self.assertEqual(["kryptos_config", "KryptosConfig"], self._searchFunctionNames("KRYPTOS"))
                self.assertEqual(1, scan.call_count)
        finally:
            MongoDbStorage._DISTINCT_VALUES_CAP = original_cap

    def testDistinctValuesAreListedOnlyForSubstringSearches(self):
        self._storageWithNamedFunctions()
        with patch.object(self.storage, "_getDistinctValues", wraps=self.storage._getDistinctValues) as listing:
            self.storage.findFunctionByString(SearchQueryParser().parse("sample_id:0"))
            self.assertEqual(0, listing.call_count)
            self.storage.findFunctionByString(SearchQueryParser().parse("function_name:qz_alpha"))
            self.assertEqual(0, listing.call_count)
            self.storage.findFunctionByString(SearchQueryParser().parse("qz_alph"))
            self.assertEqual(1, listing.call_count)

    def testCounterInitIsIdempotent(self):
        # constructing storage repeatedly against the same database must not add counter documents (#105)
        self.storage.clearStorage()
        second_storage = self._createSecondStorage()
        second_storage._getDb()
        counters = self.storage._getDb().counters
        for name in ["query_samples", "query_functions"]:
            self.assertEqual(1, counters.count_documents({"name": name}))

    def testCounterDuplicateCleanupAndUniqueIndex(self):
        self.storage.clearStorage()
        counters = self.storage._getDb().counters
        # recreate the pre-fix state: non-unique index, one real counter and several {value: 1} duplicates (#105)
        counters.drop()
        counters.create_index("name")
        counters.insert_one({"name": "query_samples", "value": 242})
        counters.insert_many([{"name": "query_samples", "value": 1} for _ in range(5)])
        counters.insert_one({"name": "query_functions", "value": 77})
        counters.insert_many([{"name": "query_functions", "value": 1} for _ in range(5)])
        second_storage = self._createSecondStorage()
        second_storage._getDb()
        # only the highest-valued document survives per name and the index is unique afterwards
        self.assertEqual(1, counters.count_documents({"name": "query_samples"}))
        self.assertEqual(242, counters.find_one({"name": "query_samples"})["value"])
        self.assertEqual(1, counters.count_documents({"name": "query_functions"}))
        self.assertEqual(77, counters.find_one({"name": "query_functions"})["value"])
        self.assertTrue(counters.index_information()["name_1"].get("unique", False))
        # counter increments continue from the real value
        self.assertEqual(242, self.storage._useCounter("query_samples"))
        self.assertEqual(243, counters.find_one({"name": "query_samples"})["value"])

    def testCounterDuplicateCleanupCoversEveryName(self):
        # the unique index is built over the whole collection, so the cleanup has to be too: duplicates under any
        # other counter name (all of them are written by name-only upserts, and mongoqueue's "job" counter shares
        # this collection) would otherwise fail the index build and take storage initialisation down with it (#105)
        self.storage.clearStorage()
        counters = self.storage._getDb().counters
        counters.drop()
        counters.create_index("name")
        counters.insert_one({"name": "functions", "value": 500})
        counters.insert_many([{"name": "functions", "value": 1} for _ in range(3)])
        counters.insert_one({"name": "job", "value": 17})
        counters.insert_one({"name": "job", "value": 1})
        second_storage = self._createSecondStorage()
        # initialisation has to succeed rather than raise DuplicateKeyError while building the unique index
        second_storage._getDb()
        self.assertEqual(1, counters.count_documents({"name": "functions"}))
        self.assertEqual(500, counters.find_one({"name": "functions"})["value"])
        self.assertEqual(1, counters.count_documents({"name": "job"}))
        self.assertEqual(17, counters.find_one({"name": "job"})["value"])
        self.assertTrue(counters.index_information()["name_1"].get("unique", False))

    ##### picblockhash inverted index #####

    def _twoIdenticalSamples(self):
        """Two samples built from the same report, so they carry exactly the same block hashes."""
        with open(self.example_file_path) as fjson:
            smda_json = json.load(fjson)
        report_a = SmdaReport.fromDict(smda_json)
        report_b = SmdaReport.fromDict(smda_json)
        assert report_a is not None and report_b is not None
        report_a.sha256 = 64 * "a"
        report_b.sha256 = 64 * "b"
        return self.storage.addSmdaReport(report_a), self.storage.addSmdaReport(report_b)

    def testPicBlockHashIndexAgreesWithTheScan(self):
        # the whole point of the index: it must answer exactly what reading every function answers
        self.storage.clearStorage()
        entry_a, entry_b = self._twoIdenticalSamples()
        for sample_ids in ([entry_a.sample_id], [entry_a.sample_id, entry_b.sample_id]):
            self.assertTrue(self.storage._isPicBlockHashIndexComplete())
            from_index = set(self.storage.getUniqueBlocks(sample_ids)["unique_blocks"])
            self.storage._setPicBlockHashIndexComplete(False)
            from_scan = set(self.storage.getUniqueBlocks(sample_ids)["unique_blocks"])
            self.storage._setPicBlockHashIndexComplete(True)
            self.assertEqual(from_scan, from_index, f"index and scan disagree for {sample_ids}")

    def testPicBlockHashIndexIsMaintainedOnAddAndDelete(self):
        self.storage.clearStorage()
        entry_a, entry_b = self._twoIdenticalSamples()
        index = self.storage._getDb()[self.storage._PICBLOCKHASH_INDEX_COLLECTION]
        # both samples hold every block, so nothing is unique to either
        self.assertEqual({}, self.storage.getUniqueBlocks([entry_a.sample_id])["unique_blocks"])
        self.assertTrue(index.count_documents({"sample_ids": entry_b.sample_id}) > 0)
        # deleting one hands every block back to the other
        self.storage.deleteSample(entry_b.sample_id)
        self.assertEqual(0, index.count_documents({"sample_ids": entry_b.sample_id}))
        self.assertTrue(self.storage.getUniqueBlocks([entry_a.sample_id])["unique_blocks"])
        # and the delete leaves no emptied posting lists behind (the #149 residue)
        self.assertEqual(0, index.count_documents({"sample_ids": []}))
        # removing the last holder removes the documents themselves
        self.storage.deleteSample(entry_a.sample_id)
        self.assertEqual(0, index.count_documents({}))

    def testRebuildPicBlockHashIndexReproducesIncrementalState(self):
        # the rebuild is the repair path, so it has to land on what the write hooks built
        self.storage.clearStorage()
        self._twoIdenticalSamples()
        index = self.storage._getDb()[self.storage._PICBLOCKHASH_INDEX_COLLECTION]
        incremental = {d["_id"]: sorted(d["sample_ids"]) for d in index.find({})}
        self.assertTrue(incremental)
        num_hashes = self.storage.rebuildPicBlockHashIndex()
        rebuilt = {d["_id"]: sorted(d["sample_ids"]) for d in index.find({})}
        self.assertEqual(incremental, rebuilt)
        self.assertEqual(len(incremental), num_hashes)

    def testFamilyReassignmentLeavesPicBlockHashIndexAlone(self):
        # the index carries no family_id, so a relabel cannot invalidate it - pinned so that a
        # future change which adds family_id here has to confront this test first
        self.storage.clearStorage()
        entry_a, _ = self._twoIdenticalSamples()
        index = self.storage._getDb()[self.storage._PICBLOCKHASH_INDEX_COLLECTION]
        before = {d["_id"]: sorted(d["sample_ids"]) for d in index.find({})}
        self.storage.modifySample(entry_a.sample_id, {"family_name": "a_different_family"})
        after = {d["_id"]: sorted(d["sample_ids"]) for d in index.find({})}
        self.assertEqual(before, after)

    def testPicBlockHashIndexIsNotWrittenWhileIncomplete(self):
        # an index nothing trusts is not worth maintaining, and half-maintaining it would make the
        # eventual rebuild look unnecessary
        self.storage.clearStorage()
        self.storage._setPicBlockHashIndexComplete(False)
        self._twoIdenticalSamples()
        index = self.storage._getDb()[self.storage._PICBLOCKHASH_INDEX_COLLECTION]
        self.assertEqual(0, index.count_documents({}))
        # ... and the rebuild recovers it
        self.storage.rebuildPicBlockHashIndex()
        self.assertTrue(self.storage._isPicBlockHashIndexComplete())
        self.assertTrue(index.count_documents({}) > 0)

    def testStorageInitializationCreatesIndexes(self):
        expected_indexed_fields = {
            "samples": {"sample_id", "sha256", "family_id"},
            "families": {"family_id", "family_name"},
            "functions": {"function_id", "sample_id", "family_id", "function_name", "_pichash", "_picblockhashes.hash"},
            "query_samples": {"sample_id", "sha256"},
        }
        for collection, expected_fields in expected_indexed_fields.items():
            index_information = self.storage._getDb()[collection].index_information()
            indexed_fields = set(key for index in index_information.values() for key, _direction in index["key"])
            self.assertTrue(expected_fields.issubset(indexed_fields), f"missing indexes on {collection}: {expected_fields - indexed_fields}")

    def testStorageDoesNotIndexPicBlockHashOffset(self):
        # No query filters or sorts on "_picblockhashes.offset" - getMatchesForPicBlockHash only
        # projects it, which an index cannot serve. Pinned so the index is not reintroduced by
        # symmetry with "_picblockhashes.hash", which is filtered on and does need one.
        index_information = self.storage._getDb()["functions"].index_information()
        indexed_fields = set(key for index in index_information.values() for key, _direction in index["key"])
        self.assertNotIn("_picblockhashes.offset", indexed_fields)

    def testGetSampleBySha256ForQuerySamples(self):
        self.storage.clearStorage()
        self.assertEqual(None, self.storage.getSampleBySha256(64 * "0", is_query=True))
        smda_report = SmdaReport.fromFile(self.example_file_path)
        query_entry = self.storage.addSmdaReport(smda_report, isQuery=True)
        self.assertEqual(query_entry.sample_id, self.storage.getSampleBySha256(smda_report.sha256, is_query=True).sample_id)
        # the regular sample collection must remain unaffected
        self.assertEqual(None, self.storage.getSampleBySha256(smda_report.sha256))

    def testRebuildMinhashBandIndexMatchesIncrementalBands(self):
        self.storage.clearStorage()
        smda_report = SmdaReport.fromFile(self.example_file_path)
        self.storage.addSmdaReport(smda_report)
        # give a subset of the 10 functions minhashes, leave the others unhashed
        minhashes = [MinHash(function_id=function_id, minhash_signature=[0x30 + function_id + index for index in range(10)], minhash_bits=8) for function_id in [0, 2, 3, 5, 7, 8]]
        self.storage.addMinHashes(minhashes)
        band_collections = ["band_%d" % band_id for band_id in range(self._storage_config.STORAGE_NUM_BANDS)]

        def dumpBands():
            return {
                collection: sorted((document["band_hash"], sorted(document["function_ids"])) for document in self.storage._getDb()[collection].find({}, {"_id": 0}))
                for collection in band_collections
            }

        bands_before = dumpBands()
        # a batch size of 2 forces the rebuild through multiple keyset pages
        self.storage._minhash_config.MINHASH_BAND_REBUILD_WORK_PACKAGE_SIZE = 2
        rebuild_result = self.storage.rebuildMinhashBandIndex()
        self.assertEqual(len(minhashes), rebuild_result["minhash_functions_indexed"])
        self.assertEqual(bands_before, dumpBands())

    def _storageWithBandedSample(self):
        self.storage.clearStorage()
        smda_report = SmdaReport.fromFile(self.example_file_path)
        sample_entry = self.storage.addSmdaReport(smda_report)
        minhashes = [MinHash(function_id=function_id, minhash_signature=[0x30 + function_id + index for index in range(10)], minhash_bits=8) for function_id in [0, 2, 3, 5, 7, 8]]
        self.storage.addMinHashes(minhashes)
        return sample_entry, ["band_%d" % band_id for band_id in range(self.storage._storage_config.STORAGE_NUM_BANDS)]

    def testDeleteSampleLeavesNoEmptyBandDocuments(self):
        # #149: a pull that empties a posting list removes the document, and a pull on a
        # band hash without a document does not create one
        sample_entry, band_collections = self._storageWithBandedSample()
        db = self.storage._getDb()
        self.assertGreater(sum(db[c].count_documents({}) for c in band_collections), 0)
        self.storage.deleteSample(sample_entry.sample_id)
        for collection in band_collections:
            self.assertEqual(0, db[collection].count_documents({}), collection)
        # the same pull again, against an index that no longer holds the hashes: nothing appears
        self.storage._updateBands({0: {12345: [1, 2]}, 1: {67890: [3]}}, method="pull")
        self.assertEqual(0, db.band_0.count_documents({}))
        self.assertEqual(0, db.band_1.count_documents({}))

    def testPullingKeepsTheOtherMembersOfABand(self):
        self.storage.clearStorage()
        db = self.storage._getDb()
        db.band_0.insert_one({"band_hash": 4242, "function_ids": [1, 2, 3]})
        db.band_0.insert_one({"band_hash": 4343, "function_ids": [1]})
        self.storage._updateBands({0: {4242: [1], 4343: [1]}}, method="pull")
        self.assertEqual([2, 3], db.band_0.find_one({"band_hash": 4242})["function_ids"])
        self.assertIsNone(db.band_0.find_one({"band_hash": 4343}))

    def testPurgeEmptyBandDocumentsRemovesOnlyTheTombstones(self):
        self.storage.clearStorage()
        db = self.storage._getDb()
        db.band_0.insert_many([{"band_hash": 1, "function_ids": []}, {"band_hash": 2}, {"band_hash": 3, "function_ids": [7]}])
        db.band_1.insert_one({"band_hash": 9, "function_ids": []})
        self.assertEqual(3, self.storage.purgeEmptyBandDocuments())
        self.assertEqual([3], [d["band_hash"] for d in db.band_0.find({}, {"band_hash": 1})])
        self.assertEqual(0, db.band_1.count_documents({}))


@pytest.mark.mongo
class MongoDbXcfgSplitTest(TestCase):
    """The disassembly split (#137): `_xcfg` lives in its own collection keyed by function id,
    and every reader that carried it before must still hand callers the same shape."""

    @classmethod
    def setUpClass(cls):
        cls.example_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "example_report.smda")

    def setUp(self):
        config = McritConfig()
        config.STORAGE_CONFIG = buildMongoStorageConfig("test_xcfg_split_mcrit")
        config.MINHASH_CONFIG = MinHashConfig()
        config.SHINGLER_CONFIG = ShinglerConfig()
        self.storage = StorageFactory.getStorage(config)
        self.storage.clearStorage()
        report = SmdaReport.fromFile(self.example_file_path)
        report.family = "xcfg_split"
        self.sample_entry = self.storage.addSmdaReport(report)

    def testBlobsLiveOutsideTheFunctionDocuments(self):
        db = self.storage._getDb()
        self.assertEqual(db.functions.count_documents({}), db.xcfg.count_documents({}))
        for function_document in db.functions.find({}, {"_id": 0}):
            self.assertNotIn("_xcfg", function_document)
        # keyed by function id, so no secondary index is needed for the lookup
        self.assertEqual(
            sorted(document["_id"] for document in db.xcfg.find({}, {"_id": 1})),
            sorted(document["function_id"] for document in db.functions.find({}, {"function_id": 1, "_id": 0})),
        )

    def testReadersThatCarriedTheBlobStillDo(self):
        functions = self.storage.getFunctionsBySampleId(self.sample_entry.sample_id)
        self.assertTrue(functions)
        # feeds Worker.calculateMinHashes and the code-reference extraction in match reports
        self.assertTrue(all(entry.xcfg for entry in functions))
        unhashed = self.storage.getUnhashedFunctions()
        self.assertTrue(unhashed)
        self.assertTrue(all(entry.xcfg for entry in unhashed))
        paged = self.storage.getFunctions(0, 5)
        self.assertTrue(paged)
        self.assertTrue(all(entry.xcfg for entry in paged))

    def testWithXcfgFlagStillGatesTheBlob(self):
        function_id = self.storage.getFunctionsBySampleId(self.sample_entry.sample_id)[0].function_id
        self.assertIsNone(self.storage.getFunctionById(function_id).xcfg)
        self.assertNotEqual({}, self.storage.getFunctionById(function_id, with_xcfg=True).xcfg)

    def testHasInlineXcfgRemainingTracksTheSplitState(self):
        # False on the split shape, True once blobs sit inline again - this is the /status
        # signal for a half-migrated instance, so both transitions must be reported
        self.assertFalse(self.storage.hasInlineXcfgRemaining())
        self._revertToInlineBlobs()
        self.assertTrue(self.storage.hasInlineXcfgRemaining())

    def testFinishedInplaceMarkerDecidesWithoutScanning(self):
        # the terminal marker migrate_xcfg_split writes on completion is authoritative for
        # "migrated" and must win over the bounded probe (which could not prove a negative here)
        db = self.storage._getDb()
        self._revertToInlineBlobs()
        db.c1_migration_state.insert_one({"_id": "inplace:functions:xcfg", "finished_at": "2026-08-24T00:00:00+00:00"})
        try:
            self.assertFalse(self.storage.hasInlineXcfgRemaining())
        finally:
            db.c1_migration_state.drop()

    def testCopyModeMarkerDoesNotDecide(self):
        # a rehearsal copy completing says nothing about this instance's functions collection -
        # only an inplace completion is authoritative for "no inline blobs remain"
        db = self.storage._getDb()
        self._revertToInlineBlobs()
        db.c1_migration_state.insert_one({"_id": "copy:functions:xcfg", "finished_at": "2026-08-24T00:00:00+00:00"})
        try:
            self.assertTrue(self.storage.hasInlineXcfgRemaining())
        finally:
            db.c1_migration_state.drop()

    def testStatusSurfacesTheInlineXcfgSignal(self):
        mcrit_config = McritConfig()
        mcrit_config.STORAGE_CONFIG = buildMongoStorageConfig("test_xcfg_split_mcrit")
        status = MinHashIndex(mcrit_config).getStatus(with_pichash=False)["status"]
        self.assertFalse(status["inline_xcfg_remaining"])
        self._revertToInlineBlobs()
        status = MinHashIndex(mcrit_config).getStatus(with_pichash=False)["status"]
        self.assertTrue(status["inline_xcfg_remaining"])

    def _revertToInlineBlobs(self):
        """Rewind a sample into the pre-split shape: blobs back inside the function documents,
        blob collection gone. This is exactly what an instance looks like between deploying the
        split code and finishing migrate_xcfg_split."""
        db = self.storage._getDb()
        for blob_document in db.xcfg.find({}):
            db.functions.update_one({"function_id": blob_document["_id"]}, {"$set": {"_xcfg": blob_document["_xcfg"]}})
        db.xcfg.drop()

    def testReadersFallBackToInlineBlobsBeforeMigration(self):
        """This code ships before the migration runs, so readers must serve inline `_xcfg`
        instead of reporting empty disassembly - calculateMinHashes and match reports would
        otherwise silently compute from nothing on an un-migrated instance."""
        self._revertToInlineBlobs()
        db = self.storage._getDb()
        self.assertGreater(db.functions.count_documents({"_xcfg": {"$exists": True}}), 0)
        functions = self.storage.getFunctionsBySampleId(self.sample_entry.sample_id)
        self.assertTrue(all(entry.xcfg for entry in functions))
        function_id = functions[0].function_id
        self.assertNotEqual({}, self.storage.getFunctionById(function_id, with_xcfg=True).xcfg)
        unhashed = self.storage.getUnhashedFunctions()
        self.assertTrue(unhashed)
        self.assertTrue(all(entry.xcfg for entry in unhashed))

    def testBlobWinsOverStaleInlineCopy(self):
        """Once a blob exists it is the source of truth; a leftover inline copy (e.g. after a
        rollback to the pre-$unset state) must not shadow it."""
        db = self.storage._getDb()
        function_id = self.storage.getFunctionsBySampleId(self.sample_entry.sample_id)[0].function_id
        fresh = self.storage.getFunctionById(function_id, with_xcfg=True).xcfg
        db.functions.update_one({"function_id": function_id}, {"$set": {"_xcfg": "{}"}})
        served = self.storage.getFunctionById(function_id, with_xcfg=True).xcfg
        self.assertEqual(fresh, served)

    def testDroppedDisassemblyReadsAsEmptyDictNotNone(self):
        # None means "not requested", {} means "dropped" - the distinction the pre-split code
        # preserved by blanking the field in place instead of removing it
        function_id = self.storage.getFunctionsBySampleId(self.sample_entry.sample_id)[0].function_id
        self.storage.deleteXcfgForSampleId(self.sample_entry.sample_id)
        self.assertEqual(0, self.storage._getDb().xcfg.count_documents({}))
        self.assertEqual({}, self.storage.getFunctionById(function_id, with_xcfg=True).xcfg)

    def testDeleteXcfgDataDropsTheCollection(self):
        self.assertGreater(self.storage._getDb().xcfg.count_documents({}), 0)
        self.storage.deleteXcfgData()
        self.assertEqual(0, self.storage._getDb().xcfg.count_documents({}))

    def testDeleteSampleTakesItsBlobsWithIt(self):
        self.assertGreater(self.storage._getDb().xcfg.count_documents({}), 0)
        self.storage.deleteSample(self.sample_entry.sample_id)
        self.assertEqual(0, self.storage._getDb().functions.count_documents({}))
        self.assertEqual(0, self.storage._getDb().xcfg.count_documents({}))

    def testQueryFunctionsUseTheirOwnBlobCollection(self):
        report = SmdaReport.fromFile(self.example_file_path)
        report.sha256 = 64 * "e"
        query_entry = self.storage.addSmdaReport(report, isQuery=True)
        db = self.storage._getDb()
        self.assertGreater(db.query_xcfg.count_documents({}), 0)
        self.assertTrue(all(document["_id"] < 0 for document in db.query_xcfg.find({}, {"_id": 1})))
        query_function_id = self.storage.getFunctionsBySampleId(query_entry.sample_id)[0].function_id
        self.assertNotEqual({}, self.storage.getFunctionById(query_function_id, with_xcfg=True).xcfg)
        self.storage.deleteSample(query_entry.sample_id)
        self.assertEqual(0, db.query_xcfg.count_documents({}))


if __name__ == "__main__":
    main()
