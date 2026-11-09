"""getStats counts the distinct pichashes from the count index while that index is complete.

The count used to be a `$group` over the whole functions collection, run on every /status. The
count index already holds one document per distinct hash, so while it is complete the number is
read from there. Every check compares against the distinct pichashes of the functions collection,
the truth both the grouping and the index are derived from.
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
DB_NAME = "test_status_pichash_count"


def loadReport(name, sha256=None):
    with open(os.path.join(PROJECT_ROOT, "tests", name)) as handle:
        report = SmdaReport.fromDict(json.load(handle))
    assert report is not None
    if sha256 is not None:
        # the same code under another hash: a second sample holding the very same pichashes
        report.sha256 = sha256
    return report


@pytest.mark.mongo
class StatusPicHashCountTest(TestCase):
    def setUp(self):
        server, port = getTestMongoServerAndPort()
        config = McritConfig()
        config.STORAGE_CONFIG = StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB, STORAGE_SERVER=server, STORAGE_PORT=port, STORAGE_MONGODB_DBNAME=DB_NAME)
        config.QUEUE_CONFIG = QueueConfig(QUEUE_METHOD=QueueFactory.QUEUE_METHOD_FAKE)
        self.storage = MinHashIndex(config=config)._storage
        self.storage.clearStorage()
        self.addCleanup(self.storage._getDb().client.drop_database, DB_NAME)
        # an empty corpus starts with the count index complete, so every add below maintains it
        self.assertTrue(self.storage.isPicHashCountIndexComplete())
        self.sample_ids = [self.storage.addSmdaReport(loadReport(name)).sample_id for name in REPORTS]

    def _distinctPicHashes(self):
        return len(self.storage._getDb().functions.distinct("_pichash", {"_pichash": {"$ne": None}}))

    def _grouped(self):
        """The count with the index disregarded, as /status answered before."""
        self.storage._setPicHashCountIndexComplete(False)
        try:
            return self.storage.getStats()["num_pichashes"]
        finally:
            self.storage._setPicHashCountIndexComplete(True)

    def _assertCounted(self):
        truth = self._distinctPicHashes()
        self.assertGreater(truth, 0, "the fixture corpus carries no pichashes, so this proves nothing")
        self.assertTrue(self.storage.isPicHashCountIndexComplete())
        self.assertEqual(self.storage.getStats()["num_pichashes"], truth)
        self.assertEqual(self._grouped(), truth)

    def testAfterInserts(self):
        self._assertCounted()

    def testAfterAddingTheSameHashesAgain(self):
        self.storage.addSmdaReport(loadReport(REPORTS[0], sha256="ab" * 32))
        self._assertCounted()

    def testAfterDeletingASample(self):
        before = self._distinctPicHashes()
        self.assertTrue(self.storage.deleteSample(self.sample_ids[1]))
        self.assertLess(self._distinctPicHashes(), before, "the delete took no hash away, so this proves nothing")
        self._assertCounted()

    def testAfterARebuild(self):
        self.storage.rebuildPicHashCountIndex()
        self._assertCounted()

    def testTheIndexIsWhatIsCounted(self):
        """With the index complete the count comes from it, not from the functions."""
        counts = self.storage._getDb()[self.storage._PICHASH_COUNT_COLLECTION]
        counts.insert_one({"_pichash": hex(0x123456789ABCDEF), "df": 1})
        self.assertEqual(self.storage.getStats()["num_pichashes"], self._distinctPicHashes() + 1)

    def testAHashNoFunctionHoldsAnyMoreIsNotCounted(self):
        """A delete that died between its decrement and its cleanup leaves documents at df 0."""
        counts = self.storage._getDb()[self.storage._PICHASH_COUNT_COLLECTION]
        counts.insert_many([{"_pichash": hex(0x1111), "df": 0}, {"_pichash": hex(0x2222), "df": -1}])
        self._assertCounted()

    def testAnIncompleteIndexFallsBackToTheFunctions(self):
        counts = self.storage._getDb()[self.storage._PICHASH_COUNT_COLLECTION]
        self.storage._setPicHashCountIndexComplete(False)
        # an incomplete index can hold anything, a recalculation (#249) for instance leaves it stale
        counts.delete_many({})
        self.assertEqual(self.storage.getStats()["num_pichashes"], self._distinctPicHashes())

    def testAFunctionWithoutAPicHashIsNoHash(self):
        """Both paths agree on it; the grouping used to count the missing hash as one more."""
        self.storage._getDb().functions.insert_one({"function_id": 10**9, "sample_id": self.sample_ids[0], "family_id": 0})
        self._assertCounted()

    def testWithoutPicHashNothingIsCounted(self):
        self.assertIsNone(self.storage.getStats(with_pichash=False)["num_pichashes"])


if __name__ == "__main__":
    main()
