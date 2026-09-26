import os
import unittest
from unittest.mock import patch

import pymongo
import pytest
from picblocks.blockhasher import BlockHasher
from smda.common.SmdaReport import SmdaReport
from smda.intel.IntelInstructionEscaper import IntelInstructionEscaper

from mcrit.config.McritConfig import McritConfig
from mcrit.config.MinHashConfig import MinHashConfig
from mcrit.config.QueueConfig import QueueConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.matchers.MatcherQuery import MatcherQuery
from mcrit.matchers.MatcherSample import MatcherSample
from mcrit.matchers.MatcherVs import MatcherVs
from mcrit.matchers.MatcherVsGroup import MatcherVsGroup
from mcrit.queue.QueueFactory import QueueFactory
from mcrit.storage.StorageFactory import StorageFactory

from .context import getTestMongoServerAndPort

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
DB_NAME = "test_architecture_separation"


def load_report(name, sha256=None):
    report = SmdaReport.fromFile(os.path.join(FIXTURES, name))
    assert report is not None
    if sha256 is not None:
        # the same code under another hash: a second sample the storage does not deduplicate
        report.sha256 = sha256
    return report


def without_timing(report):
    return {key: value for key, value in report.items() if key != "info"}


def matches_against(report, sample_id):
    return sorted((function["fid"], *match) for function in report["matches"]["functions"] for match in function["matches"] if match[1] == sample_id)


def reported_sample_ids(report):
    by_function = {match[1] for function in report["matches"]["functions"] for match in function["matches"]}
    return by_function | {sample["sample_id"] for sample in report["matches"]["samples"]}


class MemoryCorpus:
    def _config(self):
        config = McritConfig()
        config.MINHASH_CONFIG = MinHashConfig()
        config.STORAGE_CONFIG = StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MEMORY)
        config.QUEUE_CONFIG = QueueConfig(QUEUE_METHOD=QueueFactory.QUEUE_METHOD_FAKE)
        return config

    def _relabel(self, index, sample_id, architecture):
        index._storage._samples[sample_id].architecture = architecture

    def _corpus(self, *reports):
        index = MinHashIndex(config=self._config())
        sample_ids = []
        for report in reports:
            entry = index._storage.addSmdaReport(report)
            index.queue._worker.updateMinHashesForSample(entry.sample_id)
            sample_ids.append(entry.sample_id)
        return index, sample_ids

    def _match(self, index, sample_id):
        return MatcherSample(index.queue._worker).getMatchesForSample(sample_id)


class ArchitectureSeparationTest(MemoryCorpus, unittest.TestCase):
    """Matching reports no matches against samples of another architecture (#93)."""

    def test_samples_of_another_architecture_are_not_reported(self):
        index, (own, other) = self._corpus(load_report("crossarch_aarch64_a.smda"), load_report("crossarch_aarch64_b.smda"))
        self.assertIn(other, reported_sample_ids(self._match(index, own)))
        self._relabel(index, other, "dalvik")
        report = self._match(index, own)
        self.assertNotIn(other, reported_sample_ids(report))

    def test_an_unknown_architecture_is_not_another_one(self):
        index, (own, other) = self._corpus(load_report("crossarch_aarch64_a.smda"), load_report("crossarch_aarch64_b.smda"))
        expected = self._match(index, own)
        self._relabel(index, other, "")
        self.assertEqual(without_timing(expected), without_timing(self._match(index, own)))
        # and a sample of unknown architecture is not filtered against anything
        self._relabel(index, own, "")
        self._relabel(index, other, "aarch64")
        self.assertIn(other, reported_sample_ids(self._match(index, own)))

    def test_the_architectures_come_from_the_lookup_the_report_needs_anyway(self):
        # one batched lookup of the matched samples, not one per sample (#111)
        index, (own, *_others) = self._corpus(
            load_report("crossarch_aarch64_a.smda"),
            load_report("crossarch_aarch64_b.smda"),
            load_report("crossarch_aarch64_b.smda", sha256="ab" * 32),
            load_report("crossarch_aarch64_b.smda", sha256="cd" * 32),
        )
        storage = index._storage
        with (
            patch.object(storage, "getSampleEntriesByIds", wraps=storage.getSampleEntriesByIds) as batched,
            patch.object(storage, "getLibraryInfoForSampleId", wraps=storage.getLibraryInfoForSampleId) as per_sample,
        ):
            reported = reported_sample_ids(self._match(index, own))
        self.assertGreater(len(reported), 2)
        self.assertEqual(0, per_sample.call_count)
        self.assertEqual(1, batched.call_count)


class ArchitectureSeparationOtherMatchersTest(MemoryCorpus, unittest.TestCase):
    """The query, vs and vs-group reports leave out other architectures just like sample matching."""

    def _stored_copies(self):
        # two stored samples of the same code, so each matches the other and the query
        index, (other, same) = self._corpus(load_report("crossarch_aarch64_b.smda"), load_report("crossarch_aarch64_b.smda", sha256="ab" * 32))
        return index, index.queue._worker, other, same

    def test_query_reports_no_samples_of_another_architecture(self):
        index, worker, other, same = self._stored_copies()
        self.assertTrue({other, same} <= reported_sample_ids(MatcherQuery(worker).getMatchesForSmdaReport(load_report("crossarch_aarch64_a.smda"))))
        self._relabel(index, other, "dalvik")
        reported = reported_sample_ids(MatcherQuery(worker).getMatchesForSmdaReport(load_report("crossarch_aarch64_a.smda")))
        self.assertIn(same, reported)
        self.assertNotIn(other, reported)

    def test_function_query_reports_no_samples_of_another_architecture(self):
        index, _worker, other, same = self._stored_copies()
        report = load_report("crossarch_aarch64_a.smda").toDict()
        # one function of report a that matches report b by MinHash, one by PicHash
        for offset in (0x100003438, 0x1000039A8):
            single_function = {**report, "xcfg": {offset: report["xcfg"][offset]}}
            self._relabel(index, other, "aarch64")
            self.assertTrue({other, same} <= reported_sample_ids(index.getMatchesForSmdaFunction(single_function)))
            self._relabel(index, other, "dalvik")
            reported = reported_sample_ids(index.getMatchesForSmdaFunction(single_function))
            self.assertIn(same, reported)
            self.assertNotIn(other, reported)

    def test_vs_reports_nothing_against_another_architecture(self):
        index, worker, other, same = self._stored_copies()
        self.assertIn(other, reported_sample_ids(MatcherVs(worker).getMatchesForSample(same, other)))
        self._relabel(index, other, "dalvik")
        self.assertNotIn(other, reported_sample_ids(MatcherVs(worker).getMatchesForSample(same, other)))

    def test_vs_group_leaves_out_only_the_other_architecture(self):
        index, worker, other, same = self._stored_copies()
        own = index._storage.addSmdaReport(load_report("crossarch_aarch64_a.smda")).sample_id
        worker.updateMinHashesForSample(own)
        self._relabel(index, other, "dalvik")
        reported = reported_sample_ids(MatcherVsGroup(worker).getMatchesForSample(own, [other, same]))
        self.assertIn(same, reported)
        self.assertNotIn(other, reported)


@pytest.mark.mongo
class ArchitectureShortlistTest(unittest.TestCase):
    """Two-stage matching needs the function range index, which MongoDbStorage keeps."""

    def setUp(self):
        server, port = getTestMongoServerAndPort()
        self.addCleanup(lambda: pymongo.MongoClient(server, int(port)).drop_database(DB_NAME))

    def _config(self, shortlist_size=0):
        server, port = getTestMongoServerAndPort()
        config = McritConfig()
        config.MINHASH_CONFIG = MinHashConfig(MINHASH_MATCHING_SHORTLIST_SIZE=shortlist_size)
        config.STORAGE_CONFIG = StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB, STORAGE_SERVER=server, STORAGE_PORT=port, STORAGE_MONGODB_DBNAME=DB_NAME)
        config.QUEUE_CONFIG = QueueConfig(QUEUE_METHOD=QueueFactory.QUEUE_METHOD_FAKE)
        return config

    def test_samples_of_another_architecture_take_no_place_on_the_shortlist(self):
        index = MinHashIndex(config=self._config())
        index._storage.clearStorage()
        sample_ids = []
        for report in (load_report("crossarch_aarch64_a.smda"), load_report("crossarch_aarch64_b.smda"), load_report("crossarch_aarch64_b.smda", sha256="ab" * 32)):
            entry = index._storage.addSmdaReport(report)
            index.queue._worker.updateMinHashesForSample(entry.sample_id)
            sample_ids.append(entry.sample_id)
        own, other, same = sample_ids
        worker = index.queue._worker
        # the two copies tie on votes, and a tie goes to the lower sample id: the one relabelled
        index._storage._database.samples.update_one({"sample_id": other}, {"$set": {"architecture": "dalvik"}})
        unrestricted = reported_sample_ids(MatcherSample(worker).getMatchesForSample(own))
        self.assertIn(same, unrestricted)
        self.assertNotIn(other, unrestricted)
        # PicHash matches are reported whatever the shortlist, the MinHash ones only for shortlisted samples
        shortlisted = MinHashIndex(config=self._config(shortlist_size=1)).queue._worker
        expected = matches_against(MatcherSample(worker).getMatchesForSample(own), same)
        # (function id, family id, sample id, function id, score, flags): some are MinHash matches
        self.assertTrue(any(match[4] < 100 for match in expected))
        self.assertEqual(expected, matches_against(MatcherSample(shortlisted).getMatchesForSample(own), same))


class BlockHashEscaperTest(unittest.TestCase):
    def test_stored_block_hashes_are_escaped_by_their_own_architecture(self):
        """picblocks 2.1.0 escapes with the function's own escaper; before, every block as Intel."""
        config = McritConfig()
        config.STORAGE_CONFIG = StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MEMORY)
        config.QUEUE_CONFIG = QueueConfig(QUEUE_METHOD=QueueFactory.QUEUE_METHOD_FAKE)

        def stored_hashes():
            index = MinHashIndex(config=config)
            entry = index._storage.addSmdaReport(load_report("crossarch_aarch64_a.smda"))
            return {block["hash"] for function in index._storage.getFunctionsBySampleId(entry.sample_id) for block in function.picblockhashes}

        own = stored_hashes()
        with patch.object(BlockHasher, "_getInstructionEscaper", lambda self, block: IntelInstructionEscaper):
            as_intel = stored_hashes()
        self.assertTrue(own)
        # a block may escape alike under both, most do not
        self.assertLess(len(own & as_intel), len(own) // 2)


if __name__ == "__main__":
    unittest.main()
