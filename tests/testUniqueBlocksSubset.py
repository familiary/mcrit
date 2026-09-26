"""Unique blocks of some of the stored samples, the others counting as the rest of the corpus."""

import os
import unittest

import pymongo
import pytest
from smda.common.SmdaReport import SmdaReport

from mcrit.config.McritConfig import McritConfig
from mcrit.config.QueueConfig import QueueConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.queue.QueueFactory import QueueFactory
from mcrit.storage.StorageFactory import StorageFactory

from .context import getTestMongoServerAndPort

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
DB_NAME = "test_unique_blocks_subset"


def as_int(block_hash):
    # MongoDbStorage keys unique blocks by the hex it stores, MemoryStorage by the integer
    return int(block_hash, 16) if isinstance(block_hash, str) else block_hash


class MemoryUniqueBlocksSubsetTest(unittest.TestCase):
    def _storageConfig(self):
        return StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MEMORY)

    def _index(self, storage_config=None):
        config = McritConfig()
        config.STORAGE_CONFIG = storage_config or self._storageConfig()
        config.QUEUE_CONFIG = QueueConfig(QUEUE_METHOD=QueueFactory.QUEUE_METHOD_FAKE)
        index = MinHashIndex(config=config)
        # two near-duplicates, so that some of the first one's blocks are held by the second as well
        self.first, self.second = (
            index._storage.addSmdaReport(SmdaReport.fromFile(os.path.join(FIXTURES, name))) for name in ("crossarch_aarch64_a.smda", "crossarch_aarch64_b.smda")
        )
        return index

    @staticmethod
    def _blockHashes(storage, sample_id):
        return {as_int(block["hash"]) for function in storage.getFunctionsBySampleId(sample_id) for block in function.picblockhashes}

    def test_the_blocks_of_one_sample_that_no_other_holds(self):
        storage = self._index()._storage
        own, other = self._blockHashes(storage, self.first.sample_id), self._blockHashes(storage, self.second.sample_id)
        self.assertTrue(own & other, "the fixtures share no blocks, the test would not show anything")
        result = storage.getUniqueBlocks([self.first.sample_id])
        self.assertEqual(own - other, {as_int(block_hash) for block_hash in result["unique_blocks"]})
        self.assertEqual({self.first.sample_id}, set(result["statistics"]["by_sample_id"]))
        self.assertEqual(len(own), result["statistics"]["by_sample_id"][self.first.sample_id]["total_blocks"])
        self.assertEqual(len(own - other), result["statistics"]["unique_blocks_overall"])


@pytest.mark.mongo
class MongoUniqueBlocksSubsetTest(MemoryUniqueBlocksSubsetTest):
    """The same data answers the same on MongoDbStorage, which the memory backend mirrors."""

    def _storageConfig(self):
        server, port = getTestMongoServerAndPort()
        return StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB, STORAGE_SERVER=server, STORAGE_PORT=port, STORAGE_MONGODB_DBNAME=DB_NAME)

    def setUp(self):
        # leftovers of an aborted run would shift the sample ids both backends are compared by
        self.tearDown()

    def tearDown(self):
        server, port = getTestMongoServerAndPort()
        pymongo.MongoClient(server, int(port)).drop_database(DB_NAME)

    def test_both_backends_agree(self):
        memory = self._index(StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MEMORY))._storage.getUniqueBlocks([self.first.sample_id])
        mongo = self._index()._storage.getUniqueBlocks([self.first.sample_id])
        self.assertEqual(memory["statistics"], mongo["statistics"])
        self.assertEqual({as_int(h) for h in memory["unique_blocks"]}, {as_int(h) for h in mongo["unique_blocks"]})
