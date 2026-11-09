"""Matching against a corpus holding functions whose sample document is gone (#270).

Imports of older releases could attach functions to a sample id that has no sample, a delete can
stop half-way, and a sample can be deleted while a job matches against it. Each matcher used to
assert on the missing sample, failing every job that met one of those functions.
"""

import json
import logging
import os
from unittest import TestCase, main
from unittest.mock import patch

import pymongo
import pytest
from smda.common.SmdaReport import SmdaReport

import mcrit.matchers.MatcherInterface as matcher_interface
from mcrit.config.McritConfig import McritConfig
from mcrit.config.MinHashConfig import MinHashConfig
from mcrit.config.QueueConfig import QueueConfig
from mcrit.config.ShinglerConfig import ShinglerConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.matchers.MatcherQuery import MatcherQuery
from mcrit.matchers.MatcherSample import MatcherSample
from mcrit.queue.QueueFactory import QueueFactory
from mcrit.storage.StorageFactory import StorageFactory

from .context import getTestMongoServerAndPort

logging.disable(logging.CRITICAL)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
ORPHAN_SHA256 = "0d" * 32


def loadReport(name):
    with open(os.path.join(PROJECT_ROOT, "tests", name)) as handle:
        return SmdaReport.fromDict(json.load(handle))


class OrphanedFunctionsMemoryTest(TestCase):
    def _storageConfig(self):
        return StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MEMORY)

    def _orphanSample(self, sample_id):
        # what an old import or an unfinished delete leaves: the functions, but no sample
        del self.storage._samples[sample_id]

    def _dropStorage(self):
        self.storage.clearStorage()

    def setUp(self):
        config = McritConfig()
        config.STORAGE_CONFIG = self._storageConfig()
        config.MINHASH_CONFIG = MinHashConfig()
        config.SHINGLER_CONFIG = ShinglerConfig()
        config.QUEUE_CONFIG = QueueConfig()
        config.QUEUE_CONFIG.QUEUE_METHOD = QueueFactory.QUEUE_METHOD_FAKE
        self.index = MinHashIndex(config=config)
        self.storage = self.index.getStorage()
        self.worker = self.index.queue._worker
        self.storage.clearStorage()
        self.query_sample_id = self._addSample(loadReport("example_report.smda")).sample_id
        self.other_sample_id = self._addSample(loadReport("example_report_2.smda")).sample_id

    def tearDown(self):
        self._dropStorage()

    def _addSample(self, report):
        entry = self.storage.addSmdaReport(report)
        self.worker.updateMinHashesForSample(entry.sample_id)
        return entry

    def _addOrphan(self, name="example_report.smda"):
        """By default a copy of the query sample, so its functions share every PicHash, PicBlockHash and MinHash."""
        report = loadReport(name)
        report.sha256 = ORPHAN_SHA256
        orphan_id = self._addSample(report).sample_id
        self.orphan_function_ids = [entry.function_id for entry in self.storage.getFunctionsBySampleId(orphan_id)]
        self._orphanSample(orphan_id)
        self.assertIsNone(self.storage.getSampleById(orphan_id))
        return orphan_id

    def _match(self, **kwargs):
        return MatcherSample(self.worker, **kwargs).getMatchesForSample(self.query_sample_id)

    def _matchLoggingWarnings(self, **kwargs):
        with patch.object(matcher_interface.LOGGER, "warning") as warning:
            report = self._match(**kwargs)
        return report, warning

    @staticmethod
    def _matchedSampleIds(report):
        return {entry["sample_id"] for entry in report["matches"]["samples"]}

    def _assertOneOrphanWarning(self, warning, orphan_id, with_matches=True):
        orphan_warnings = [call for call in warning.call_args_list if "no sample document" in call.args[0]]
        self.assertEqual(len(orphan_warnings), 1)
        _message, num_samples, num_matches, sample_ids = orphan_warnings[0].args
        if with_matches:
            self.assertGreater(num_matches, 1)
        else:
            self.assertEqual(num_matches, 0)
        self.assertEqual((num_samples, sample_ids), (1, [orphan_id]))

    def testOrphanSharesPicHashAndPicBlockHashWithTheQuery(self):
        orphan_id = self._addOrphan()
        query_function = next(entry for entry in self.storage.getFunctionsBySampleId(self.query_sample_id) if entry.picblockhashes)
        self.assertIn(orphan_id, {match[1] for match in self.storage.getMatchesForPicHash(query_function.pichash)})
        picblockhash = query_function.picblockhashes[0]["hash"]
        self.assertIn(orphan_id, {match[1] for match in self.storage.getMatchesForPicBlockHash(picblockhash)})

    def testPicHashMatchesAgainstAnOrphanAreLeftOut(self):
        before = self._match()
        orphan_id = self._addOrphan()
        after, warning = self._matchLoggingWarnings()
        self.assertEqual(after["matches"], before["matches"])
        self.assertNotIn(orphan_id, self._matchedSampleIds(after))
        self.assertIn(self.other_sample_id, self._matchedSampleIds(after))
        self._assertOneOrphanWarning(warning, orphan_id)

    def testMinHashMatchesAgainstAnOrphanAreLeftOut(self):
        # a pichash_size no function reaches leaves the MinHash stage as the only way to match
        before = self._match(pichash_size=10**6)
        self.assertTrue(before["matches"]["aggregation"]["minhash"]["num_own_functions_matched"])
        orphan_id = self._addOrphan()
        after, warning = self._matchLoggingWarnings(pichash_size=10**6)
        self.assertEqual(after["matches"], before["matches"])
        self._assertOneOrphanWarning(warning, orphan_id)

    def testQueryMatchingLeavesOutTheOrphan(self):
        self._addOrphan()
        with patch.object(matcher_interface.LOGGER, "warning") as warning:
            report = MatcherQuery(self.worker).getMatchesForSmdaReport(loadReport("example_report.smda"))
        self.assertEqual(self._matchedSampleIds(report), {self.query_sample_id, self.other_sample_id})
        self.assertEqual(len([call for call in warning.call_args_list if "no sample document" in call.args[0]]), 1)

    def testAnOrphanTakesNoShortlistPlace(self):
        # the orphan, an exact copy of the query, would outvote every real sample for the one place
        self._addOrphan()
        report = self._match(shortlist_size=1)
        self.assertEqual(report["info"]["matching"]["applied"]["shortlist_size"], 1)
        self.assertIn(self.other_sample_id, self._matchedSampleIds(report))

    def testAnOrphanThatOnlyLostShortlistVotesIsWarnedAbout(self):
        # a copy of the other sample shares no PicHash with the query, only bands, and loses the
        # one place to the original on the tie; with the query's architecture known, every voted
        # sample is looked up, so the orphan is found and taken out of the votes
        before = self._match(shortlist_size=1)
        orphan_id = self._addOrphan("example_report_2.smda")
        after, warning = self._matchLoggingWarnings(shortlist_size=1)
        self.assertEqual(after["matches"], before["matches"])
        self._assertOneOrphanWarning(warning, orphan_id, with_matches=False)

    def _addVotedSamples(self):
        # the query (example_report) shares bands with all three and PicHashes with the library
        self.small_sample_id = self._addSample(loadReport("example_report_3.smda")).sample_id
        self.library_sample_id = self._addSample(loadReport("library_report.smda")).sample_id

    def _sampleLookups(self, **kwargs):
        with patch.object(self.storage, "getSampleEntriesByIds", wraps=self.storage.getSampleEntriesByIds) as lookup:
            report = self._match(**kwargs)
        return report, [set(call.args[0]) for call in lookup.call_args_list]

    def testAShortlistedJobLooksUpSamplesOnce(self):
        self._addVotedSamples()
        report, lookups = self._sampleLookups(shortlist_size=1)
        self.assertEqual(report["info"]["matching"]["applied"]["shortlist_size"], 1)
        self.assertEqual(len(lookups), 1)

    def testWithoutAnArchitectureOnlyTheNeededSamplesAreLookedUp(self):
        self._addVotedSamples()
        self._setArchitecture(self.query_sample_id, "")
        _report, lookups = self._sampleLookups(shortlist_size=1)
        self.assertEqual(len(lookups), 1)
        # the own sample, the PicHash-matched library and the one shortlisted sample (which may be
        # the library) - not every sample that received a vote
        self.assertTrue({self.query_sample_id, self.library_sample_id} <= lookups[0])
        self.assertLessEqual(len(lookups[0] & {self.other_sample_id, self.small_sample_id}), 1)

    def _setArchitecture(self, sample_id, architecture):
        self.storage._samples[sample_id].architecture = architecture

    def testFunctionVsAnOrphanedFunction(self):
        self._addOrphan()
        query_function_id = self.storage.getFunctionsBySampleId(self.query_sample_id)[0].function_id
        self.assertIsNone(self.index.getMatchesFunctionVs(query_function_id, self.orphan_function_ids[0]))
        self.assertIsNotNone(self.index.getMatchesFunctionVs(query_function_id, query_function_id))

    def testRestrictToShortlistWithUnresolvableSampleIdsDoesNotRestrict(self):
        matcher = MatcherSample(self.worker)
        matcher._sample_shortlist = {1}
        with patch.object(matcher, "_resolveSampleIds", return_value=None):
            restricted = matcher._restrictToShortlist({10: {100, 101}, 11: {200}})
        self.assertEqual(restricted, {10: {100, 101}, 11: {200}})
        self.assertEqual(matcher._shortlist_fallback, "function_range_index_incomplete")
        self.assertTrue(matcher.fellBackUnforeseen())
        # and so for the batches that follow, and in the report
        self.assertEqual(matcher._restrictToShortlist({12: {300}}), {12: {300}})
        self.assertEqual(matcher._getMatchingInfo()["fallbacks"], {"shortlist_size": "function_range_index_incomplete"})


@pytest.mark.mongo
class OrphanedFunctionsMongoTest(OrphanedFunctionsMemoryTest):
    DB_NAME = f"test_orphaned_functions_{os.getpid()}"

    def _storageConfig(self):
        server, port = getTestMongoServerAndPort()
        return StorageConfig(
            STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB,
            STORAGE_SERVER=server,
            STORAGE_PORT=port,
            STORAGE_MONGODB_DBNAME=self.DB_NAME,
        )

    def _orphanSample(self, sample_id):
        self.storage._getDb().samples.delete_one({"sample_id": sample_id})

    def _setArchitecture(self, sample_id, architecture):
        self.storage._getDb().samples.update_one({"sample_id": sample_id}, {"$set": {"architecture": architecture}})

    def _dropStorage(self):
        server, port = getTestMongoServerAndPort()
        pymongo.MongoClient(server, int(port)).drop_database(self.DB_NAME)


if __name__ == "__main__":
    main()
