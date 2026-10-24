"""A function without a PicHash is stored with pichash 0, and 0 is not a hash two functions can share.

The sample-vs-sample and sample-vs-group matchers grouped functions by pichash as stored, so any two
functions without one were reported as an exact match of each other.
"""

import json
import logging
import os
from unittest import TestCase, main

import pymongo
import pytest
from smda.common.SmdaReport import SmdaReport

import mcrit.matchers.MatcherFlags as MatcherFlags
from mcrit.config.McritConfig import McritConfig
from mcrit.config.MinHashConfig import MinHashConfig
from mcrit.config.QueueConfig import QueueConfig
from mcrit.config.ShinglerConfig import ShinglerConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.matchers.MatcherSample import MatcherSample
from mcrit.matchers.MatcherVs import MatcherVs
from mcrit.matchers.MatcherVsGroup import MatcherVsGroup
from mcrit.queue.QueueFactory import QueueFactory
from mcrit.storage.StorageFactory import StorageFactory

from .context import getTestMongoServerAndPort

logging.disable(logging.CRITICAL)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# three functions of example_report.smda, all well above the default PICHASH_SIZE and of
# different code: one loses its PicHash in the first sample, another in the second, and the
# third keeps a real one in both
UNHASHED_IN_A, UNHASHED_IN_B, HASHED_IN_BOTH = 4424, 1172, 2780


def loadReport(sha256=None, unhashed_offset=None):
    with open(os.path.join(PROJECT_ROOT, "tests", "example_report.smda")) as handle:
        report = SmdaReport.fromDict(json.load(handle))
    assert report is not None
    if sha256 is not None:
        report.sha256 = sha256
    for smda_function in report.getFunctions():
        if smda_function.offset == unhashed_offset:
            smda_function.pic_hash = None
    return report


class PicHashZeroMemoryTest(TestCase):
    def _storageConfig(self):
        return StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MEMORY)

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
        self.sample_a = self._addSample(loadReport(unhashed_offset=UNHASHED_IN_A))
        self.sample_b = self._addSample(loadReport(sha256="0e" * 32, unhashed_offset=UNHASHED_IN_B))
        self.functions_a = {entry.offset: entry for entry in self.storage.getFunctionsBySampleId(self.sample_a)}
        self.functions_b = {entry.offset: entry for entry in self.storage.getFunctionsBySampleId(self.sample_b)}
        self.assertEqual((self.functions_a[UNHASHED_IN_A].pichash, self.functions_b[UNHASHED_IN_B].pichash), (0, 0))

    def tearDown(self):
        self._dropStorage()

    def _addSample(self, report):
        sample_id = self.storage.addSmdaReport(report).sample_id
        self.worker.updateMinHashesForSample(sample_id)
        return sample_id

    def _pichashMatchedFunctionIds(self, report, own_offset):
        own_function_id = self.functions_a[own_offset].function_id
        function_report = next(entry for entry in report["matches"]["functions"] if entry["fid"] == own_function_id)
        return {match[2] for match in function_report["matches"] if match[4] & MatcherFlags.IS_PICHASH_FLAG}

    def _assertNoMatchOnPicHashZero(self, report):
        self.assertNotIn(self.functions_b[UNHASHED_IN_B].function_id, self._pichashMatchedFunctionIds(report, UNHASHED_IN_A))
        self.assertIn(self.functions_b[HASHED_IN_BOTH].function_id, self._pichashMatchedFunctionIds(report, HASHED_IN_BOTH))

    def testMatcherVs(self):
        self._assertNoMatchOnPicHashZero(MatcherVs(self.worker).getMatchesForSample(self.sample_a, self.sample_b))

    def testMatcherVsGroup(self):
        self._assertNoMatchOnPicHashZero(MatcherVsGroup(self.worker).getMatchesForSample(self.sample_a, [self.sample_b]))

    def testMatcherSample(self):
        # the corpus path reads the storage's pichash lookups rather than grouping itself
        self._assertNoMatchOnPicHashZero(MatcherSample(self.worker).getMatchesForSample(self.sample_a))

    def testFunctionVsDoesNotCallPicHashZeroAnExactMatch(self):
        def flags(offset_a, offset_b):
            result = self.index.getMatchesFunctionVs(self.functions_a[offset_a].function_id, self.functions_b[offset_b].function_id)
            return result["match_entry"]["matches"][-1]

        self.assertFalse(flags(UNHASHED_IN_A, UNHASHED_IN_B) & MatcherFlags.IS_PICHASH_FLAG)
        self.assertTrue(flags(HASHED_IN_BOTH, HASHED_IN_BOTH) & MatcherFlags.IS_PICHASH_FLAG)

    def testPicHashQueryForZeroAnswersNoMatches(self):
        # GET /query/pichash/0000000000000000 and its summary read this
        self.assertEqual(self.index.getMatchesForPicHash(0), set())
        self.assertEqual(len(self.index.getMatchesForPicHash(self.functions_a[HASHED_IN_BOTH].pichash)), 2)

    def testStorageLookupsSkipPicHashZero(self):
        function_ids = [self.functions_a[UNHASHED_IN_A].function_id, self.functions_a[HASHED_IN_BOTH].function_id]
        matches = self.storage.getPicHashMatchesByFunctionIds(function_ids)
        self.assertEqual(set(matches), {self.functions_a[HASHED_IN_BOTH].pichash})
        self.assertEqual(self.storage.getMatchesForPicHashes([0]), {})


@pytest.mark.mongo
class PicHashZeroMongoTest(PicHashZeroMemoryTest):
    DB_NAME = f"test_pichash_zero_matchers_{os.getpid()}"

    def _storageConfig(self):
        server, port = getTestMongoServerAndPort()
        return StorageConfig(
            STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB,
            STORAGE_SERVER=server,
            STORAGE_PORT=port,
            STORAGE_MONGODB_DBNAME=self.DB_NAME,
        )

    def _dropStorage(self):
        server, port = getTestMongoServerAndPort()
        pymongo.MongoClient(server, int(port)).drop_database(self.DB_NAME)


if __name__ == "__main__":
    main()
