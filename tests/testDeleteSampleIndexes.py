"""deleteSample keeps the pichash counts and the function ranges in step with what is stored (#261).

addSmdaReport adds a sample's functions to both indexes; deleteSample used to leave them there, so
a deleted sample stayed counted and a deleted and re-added one was counted twice. Every check here
compares against the truth the rebuilds derive from the functions collection.
"""

import json
import logging
import os
from unittest import TestCase, main

import pytest
from smda.common.SmdaReport import SmdaReport

from mcrit.config.McritConfig import McritConfig
from mcrit.config.QueueConfig import QueueConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.queue.QueueFactory import QueueFactory
from mcrit.storage.StorageFactory import StorageFactory

from .context import getTestMongoServerAndPort

logging.disable(logging.CRITICAL)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
REPORTS = ["example_report.smda", "example_report_2.smda", "example_report_3.smda"]
DB_NAME = "test_delete_sample_indexes"


def loadReport(name, sha256=None):
    with open(os.path.join(PROJECT_ROOT, "tests", name)) as handle:
        report = SmdaReport.fromDict(json.load(handle))
    assert report is not None
    if sha256 is not None:
        # the same code under another hash: a second sample holding the very same pichashes
        report.sha256 = sha256
    return report


@pytest.mark.mongo
class DeleteSampleIndexesTest(TestCase):
    def setUp(self):
        server, port = getTestMongoServerAndPort()
        config = McritConfig()
        config.STORAGE_CONFIG = StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB, STORAGE_SERVER=server, STORAGE_PORT=port, STORAGE_MONGODB_DBNAME=DB_NAME)
        config.QUEUE_CONFIG = QueueConfig(QUEUE_METHOD=QueueFactory.QUEUE_METHOD_FAKE)
        self.storage = MinHashIndex(config=config)._storage
        self.storage.clearStorage()
        self.addCleanup(self.storage._getDb().client.drop_database, DB_NAME)
        # an empty corpus starts with both indexes complete, so every add below maintains them
        self.assertTrue(self.storage.isPicHashCountIndexComplete())
        self.assertTrue(self.storage.isFunctionRangeIndexComplete())
        self.sample_ids = [self.storage.addSmdaReport(loadReport(name)).sample_id for name in REPORTS]

    # what is stored, and what the rebuilds would derive from it

    def _storedCounts(self):
        collection = self.storage._getDb()[self.storage._PICHASH_COUNT_COLLECTION]
        return {document["_pichash"]: document["df"] for document in collection.find({}, {"_pichash": 1, "df": 1, "_id": 0})}

    def _countsFromFunctions(self):
        truth = {}
        for document in self.storage._getDb().functions.find({"_pichash": {"$ne": None}}, {"_pichash": 1, "_id": 0}):
            truth[document["_pichash"]] = truth.get(document["_pichash"], 0) + 1
        return truth

    def _storedRanges(self):
        collection = self.storage._getDb()[self.storage._FUNCTION_RANGE_COLLECTION]
        return sorted((document["sample_id"], document["first_function_id"], document["last_function_id"]) for document in collection.find({}, {"_id": 0}))

    def _rangesFromRebuild(self):
        self.storage.rebuildFunctionRangeIndex()
        return self._storedRanges()

    # the tests

    def testDeletingASampleTakesItOutOfBothIndexes(self):
        self.assertEqual(self._storedCounts(), self._countsFromFunctions())
        deleted = self.sample_ids[1]

        self.assertTrue(self.storage.deleteSample(deleted))

        self.assertEqual(self._storedCounts(), self._countsFromFunctions())
        ranges = self._storedRanges()
        self.assertNotIn(deleted, {sample_id for sample_id, _, _ in ranges})
        self.assertEqual(ranges, self._rangesFromRebuild())

    def testADeletedAndReAddedSampleIsCountedOnce(self):
        # the case of #261: the counts had grown by every re-added function
        self.storage.deleteSample(self.sample_ids[0])
        readded = self.storage.addSmdaReport(loadReport(REPORTS[0]))

        self.assertEqual(self._storedCounts(), self._countsFromFunctions())
        ranges = self._storedRanges()
        self.assertEqual(ranges, self._rangesFromRebuild())
        self.assertIn(readded.sample_id, {sample_id for sample_id, _, _ in ranges})
        self.assertNotIn(self.sample_ids[0], {sample_id for sample_id, _, _ in ranges})

    def testAHashStillHeldElsewhereKeepsItsCount(self):
        twin = self.storage.addSmdaReport(loadReport(REPORTS[0], sha256="ab" * 32))
        shared = {pichash for pichash, df in self._storedCounts().items() if df >= 2}
        self.assertTrue(shared, "the twin sample shares no pichash, so this proves nothing")

        self.storage.deleteSample(twin.sample_id)

        counts = self._storedCounts()
        self.assertEqual(counts, self._countsFromFunctions())
        self.assertTrue(all(pichash in counts for pichash in shared))

    def testAHashNobodyHoldsAnyMoreLosesItsDocument(self):
        own = set(self._countsFromFunctions())
        for sample_id in self.sample_ids:
            self.storage.deleteSample(sample_id)

        self.assertTrue(own, "the fixture corpus carries no pichashes, so this proves nothing")
        self.assertEqual({}, self._storedCounts())
        self.assertEqual([], self._storedRanges())

    def testAnIncompleteCountIndexIsLeftToItsRebuild(self):
        # while incomplete the counts are not trusted and a rebuild will replace them, so a
        # delete does not touch them (adds skip them too)
        self.storage._setPicHashCountIndexComplete(False)
        before = self._storedCounts()

        self.storage.deleteSample(self.sample_ids[2])

        self.assertEqual(before, self._storedCounts())
        self.storage.rebuildPicHashCountIndex()
        self.assertEqual(self._storedCounts(), self._countsFromFunctions())

    def testAHashHeldTwiceBySampleIsTakenOutTwice(self):
        # no fixture sample repeats a pichash, so make one: two of its functions share a hash,
        # counted 2 by the rebuild, and the delete has to take both out, not one per hash
        functions = self.storage._getDb().functions
        first, second = [document["function_id"] for document in functions.find({"sample_id": self.sample_ids[0]}, {"function_id": 1}).limit(2)]
        shared = functions.find_one({"function_id": first})["_pichash"]
        functions.update_one({"function_id": second}, {"$set": {"_pichash": shared}})
        self.storage.rebuildPicHashCountIndex()
        self.assertEqual(2, self._storedCounts()[shared])

        self.storage.deleteSample(self.sample_ids[0])

        self.assertEqual(self._storedCounts(), self._countsFromFunctions())

    def testARetriedDeleteTakesTheCountsOutOnce(self):
        # the queue runs a failed delete job again: the first attempt took the counts out and
        # then died before the functions went, so the second finds them all still there
        twin = self.storage.addSmdaReport(loadReport(REPORTS[0], sha256="cd" * 32))
        original = self.storage._deleteXcfgForFunctionIds

        def dies_once(function_ids):
            self.storage._deleteXcfgForFunctionIds = original
            raise RuntimeError("connection lost")

        self.storage._deleteXcfgForFunctionIds = dies_once
        with self.assertRaises(RuntimeError):
            self.storage.deleteSample(twin.sample_id)
        self.assertTrue(self.storage.deleteSample(twin.sample_id))

        # the original sample still holds every hash the twin held, each counted once
        self.assertEqual(self._storedCounts(), self._countsFromFunctions())
        self.assertTrue(all(df >= 1 for df in self._storedCounts().values()))


if __name__ == "__main__":
    main()
