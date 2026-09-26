import os
import unittest
from unittest.mock import patch

import pymongo
import pytest
from picblocks.blockhasher import BlockHasher
from smda.common.SmdaReport import SmdaReport
from smda.intel.IntelInstructionEscaper import IntelInstructionEscaper

from mcrit.config.McritConfig import McritConfig
from mcrit.config.QueueConfig import QueueConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.queue.QueueFactory import QueueFactory
from mcrit.storage.MongoDbStorage import PICBLOCKS_VERSION
from mcrit.storage.StorageFactory import StorageFactory

from .context import getTestMongoServerAndPort

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
DB_NAME = "test_picblockhash_recalculation"


def load_report(name, sha256=None):
    report = SmdaReport.fromFile(os.path.join(FIXTURES, name))
    assert report is not None
    if sha256 is not None:
        # the same code under another hash: a second sample the storage does not deduplicate
        report.sha256 = sha256
    return report


@pytest.mark.mongo
class PicBlockHashRecalculationTest(unittest.TestCase):
    """recalculatePicHashes redoes the block hashes a picblocks before 2.1.0 escaped as Intel code (#240)."""

    def setUp(self):
        server, port = getTestMongoServerAndPort()
        self.addCleanup(lambda: pymongo.MongoClient(server, int(port)).drop_database(DB_NAME))
        config = McritConfig()
        config.STORAGE_CONFIG = StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB, STORAGE_SERVER=server, STORAGE_PORT=port, STORAGE_MONGODB_DBNAME=DB_NAME)
        config.QUEUE_CONFIG = QueueConfig(QUEUE_METHOD=QueueFactory.QUEUE_METHOD_FAKE)
        self.index = MinHashIndex(config=config)
        self.storage = self.index._storage
        self.storage.clearStorage()
        self.samples = self.storage._database.samples

    def _block_hashes(self, sample_id):
        return {function.offset: sorted((block["offset"], block["hash"]) for block in function.picblockhashes) for function in self.storage.getFunctionsBySampleId(sample_id)}

    def _add_from_before_the_fix(self, report):
        """A sample as picblocks before 2.1.0 stored it: every block escaped as Intel code, and no
        picblocks version recorded, since nothing recorded one then."""
        with patch.object(BlockHasher, "_getInstructionEscaper", lambda self, block: IntelInstructionEscaper):
            entry = self.storage.addSmdaReport(report)
        self.samples.update_one({"sample_id": entry.sample_id}, {"$unset": {"picblockhash_version": ""}})
        return entry

    def _stale_count(self):
        return self.index.getStatus()["status"]["num_samples_with_stale_picblockhashes"]

    def test_a_stored_sample_records_the_picblocks_it_was_hashed_with(self):
        entry = self.storage.addSmdaReport(load_report("crossarch_aarch64_a.smda"))
        self.assertEqual(PICBLOCKS_VERSION, self.samples.find_one({"sample_id": entry.sample_id})["picblockhash_version"])
        self.assertEqual(0, self._stale_count())

    def test_non_intel_block_hashes_from_before_are_recomputed(self):
        reference = self.storage.addSmdaReport(load_report("crossarch_aarch64_a.smda", sha256="ab" * 32))
        old = self._add_from_before_the_fix(load_report("crossarch_aarch64_a.smda"))
        # Intel block hashes never changed: an Intel sample from before is not counted as stale (this
        # one is still rehashed, for the smda that disassembled it, 1.5.12, is older than the threshold)
        intel = self._add_from_before_the_fix(load_report("crossarch_intel_a.smda"))
        intel_hashes = self._block_hashes(intel.sample_id)
        self.assertNotEqual(self._block_hashes(reference.sample_id), self._block_hashes(old.sample_id))
        self.assertEqual(1, self._stale_count())

        result = self.index.queue._worker.recalculatePicHashes()

        self.assertEqual(1, result["stale_picblockhash_samples"])
        self.assertEqual(self._block_hashes(reference.sample_id), self._block_hashes(old.sample_id))
        self.assertEqual(PICBLOCKS_VERSION, self.samples.find_one({"sample_id": old.sample_id})["picblockhash_version"])
        self.assertEqual(intel_hashes, self._block_hashes(intel.sample_id))
        self.assertEqual(0, self._stale_count())
        # the rewritten hashes are not yet in the inverted index getUniqueBlocks reads
        self.assertFalse(self.storage._isPicBlockHashIndexComplete())
        # and a second run has nothing left to do
        self.assertEqual(0, self.index.queue._worker.recalculatePicHashes()["stale_picblockhash_samples"])

    def test_cil_block_hashes_from_before_are_recomputed(self):
        reference = self.storage.addSmdaReport(load_report("crossarch_cil_a.smda", sha256="ab" * 32))
        old = self._add_from_before_the_fix(load_report("crossarch_cil_a.smda"))
        self.assertNotEqual(self._block_hashes(reference.sample_id), self._block_hashes(old.sample_id))
        self.index.queue._worker.recalculatePicHashes()
        self.assertEqual(self._block_hashes(reference.sample_id), self._block_hashes(old.sample_id))
        self.assertEqual(0, self._stale_count())

    def test_an_interrupted_run_leaves_the_index_marked_incomplete(self):
        self._add_from_before_the_fix(load_report("crossarch_aarch64_a.smda"))
        self.storage._setPicBlockHashIndexComplete(True)
        functions = type(self.storage._database.functions)
        with patch.object(functions, "bulk_write", side_effect=RuntimeError("interrupted")), self.assertRaises(RuntimeError):
            self.index.queue._worker.recalculatePicHashes()
        # the block hashes may be half rewritten, so getUniqueBlocks must not trust the index
        self.assertFalse(self.storage._isPicBlockHashIndexComplete())

    def test_a_sample_that_cannot_be_rehashed_completely_stays_stale(self):
        old = self._add_from_before_the_fix(load_report("crossarch_aarch64_a.smda"))
        function_id = self.storage._database.functions.find_one({"sample_id": old.sample_id}, sort=[("function_id", 1)])["function_id"]
        # a function whose disassembly is gone (e.g. dropped with STORAGE_DROP_DISASSEMBLY) keeps its old block hashes
        self.storage._database.xcfg.delete_one({"_id": function_id})
        self.storage._database.functions.update_one({"function_id": function_id}, {"$unset": {"_xcfg": ""}})

        result = self.index.queue._worker.recalculatePicHashes()

        self.assertEqual(1, result["xcfg_missing"])
        self.assertNotIn("picblockhash_version", self.samples.find_one({"sample_id": old.sample_id}))
        self.assertEqual(1, self._stale_count())


if __name__ == "__main__":
    unittest.main()
