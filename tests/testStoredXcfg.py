"""Rebuilding SmdaFunctions from the disassembly MCRIT stores (the `xcfg` of a FunctionEntry),
for a function that was stored without it.

STORAGE_DROP_DISASSEMBLY, a blob over MongoDB's 16 MiB limit (#42), and an import of an export from
such an instance all leave a function whose xcfg reads back as `{}`. smda cannot rebuild a function
from that - current releases raise "serialized function is incomplete", releases before 4.4.5 a
KeyError - and every rebuild path used to hand it over anyway, so one such function failed the
whole batch it was in, on every retry. Each rebuild path is run here on the smda 1.5.12 example
report, once as stored and once with one function's disassembly gone.
"""

import json
import os
import unittest
from copy import deepcopy
from unittest.mock import patch

import pytest
from smda.common.SmdaReport import SmdaReport

from mcrit.config.McritConfig import McritConfig
from mcrit.config.MinHashConfig import MinHashConfig
from mcrit.config.ShinglerConfig import ShinglerConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.storage.FunctionEntry import missingXcfgFields, smdaFunctionFromXcfg
from mcrit.storage.MatchingResult import MatchingResult
from mcrit.storage.StorageFactory import StorageFactory
from mcrit.storage.UniqueBlocksResult import UniqueBlocksResult

from .context import config
from .testStorage import buildMongoStorageConfig

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
EXAMPLE_REPORT = os.path.join(TESTS_DIR, "example_report.smda")


class SmdaFunctionFromXcfgTest(unittest.TestCase):
    def test_an_incomplete_xcfg_names_what_it_lacks(self):
        with open(EXAMPLE_REPORT) as handle:
            function_dict = deepcopy(next(iter(json.load(handle)["xcfg"].values())))
        del function_dict["inrefs"]
        del function_dict["metadata"]["tfidf"]
        self.assertEqual(["inrefs", "metadata.tfidf"], missingXcfgFields(function_dict))
        with self.assertRaisesRegex(ValueError, "lacks inrefs, metadata.tfidf"):
            smdaFunctionFromXcfg(function_dict)

    def test_no_disassembly_rebuilds_as_none(self):
        self.assertIsNone(smdaFunctionFromXcfg(None))
        self.assertIsNone(smdaFunctionFromXcfg({}))


class RebuildPathsTest(unittest.TestCase):
    """Every path that rebuilds functions from storage, on the smda 1.5.12 example report."""

    @classmethod
    def setUpClass(cls):
        with open(EXAMPLE_REPORT) as handle:
            cls.report = SmdaReport.fromDict(json.load(handle))

    def setUp(self):
        self.index = MinHashIndex(config=deepcopy(config))
        self.storage = self.index._storage
        self.storage.clearStorage()
        self.worker = self.index.queue._worker
        self.sample_entry = self.storage.addSmdaReport(self.report)
        self.function_entries = self.storage.getFunctionsBySampleId(self.sample_entry.sample_id)

    def _dropDisassemblyOfOneHashableFunction(self):
        hashable = self.storage.getUnhashedFunctions(None)
        self.assertGreater(len(hashable), 1)
        self.storage._functions[hashable[0].function_id].xcfg = {}
        return len(hashable)

    def test_the_older_xcfg_rebuilds_on_every_path(self):
        self.assertTrue(all(entry.toSmdaFunction() is not None for entry in self.function_entries))
        self.assertGreater(self.worker.updateMinHashes(None), 0)
        with patch("mcrit.storage.MatchingResult.LOGGER") as logger:
            MatchingResult(self.sample_entry).clusterLinkHuntResult(self.function_entries, [])
        logger.warning.assert_not_called()
        blocks = self.storage.getUniqueBlocks([self.sample_entry.sample_id])["unique_blocks"]
        self.assertTrue(blocks)
        self.assertTrue(all(block["instructions"] for block in blocks.values()))

    def test_minhashing_a_sample_skips_a_function_without_disassembly(self):
        num_hashable = self._dropDisassemblyOfOneHashableFunction()
        # updateMinHashesForSample asks for the sample's function ids, which MemoryStorage does not
        # filter by disassembly - MongoDbStorage filters neither selection
        with patch("mcrit.Worker.LOGGER") as logger:
            self.assertEqual(num_hashable - 1, self.worker.updateMinHashesForSample(self.sample_entry.sample_id))
        self.assertIn("no stored disassembly", logger.warning.call_args.args[0])

    def test_link_hunt_clustering_skips_a_function_without_disassembly(self):
        entries = deepcopy(self.function_entries)
        entries[0].xcfg = {}
        # an entry loaded without its xcfg is skipped the same way
        entries[1].xcfg = None
        with patch("mcrit.storage.MatchingResult.LOGGER") as logger:
            MatchingResult(self.sample_entry).clusterLinkHuntResult(entries, [])
        self.assertIsNone(entries[0].toSmdaFunction())
        # one warning for the call, with the count, not one per entry and not silence
        logger.warning.assert_called_once()
        self.assertEqual(2, logger.warning.call_args.args[1])
        self.assertIn("no disassembly", logger.warning.call_args.args[0])

    def test_unique_blocks_of_a_sample_without_disassembly_are_counted_not_returned(self):
        """The job completes; blocks with nothing to show or match on are counted instead of returned."""
        sample_id = self.sample_entry.sample_id
        with_disassembly = self.storage.getUniqueBlocks([sample_id])["unique_blocks"]
        # what STORAGE_DROP_DISASSEMBLY does once the sample is hashed
        self.storage.deleteXcfgForSampleId(sample_id)
        found = self.storage.getUniqueBlocks([sample_id])["unique_blocks"]
        self.assertEqual(set(with_disassembly), set(found))
        self.assertTrue(all(block["instructions"] == [] for block in found.values()))
        result = self.worker.getUniqueBlocks([sample_id])
        # MCRITweb's block table reads every returned block's instructions
        self.assertEqual({}, result["unique_blocks"])
        self.assertEqual(len(with_disassembly), result["statistics"]["blocks_without_instructions"])
        self.assertEqual(0, result["statistics"]["blocks_considered"])
        # no bytes to match on, so no rule - rather than a cover that cannot be rendered
        self.assertEqual([], result["yara_rule"])
        self.assertFalse(result["statistics"]["has_yara_rule"])
        self.assertFalse(result["statistics"]["has_complete_yara_rule"])

    def test_the_job_leaves_out_only_the_blocks_without_disassembly(self):
        sample_id = self.sample_entry.sample_id
        before = self.worker.getUniqueBlocks([sample_id])
        self.assertEqual(0, before["statistics"]["blocks_without_instructions"])
        # empty the function of a block the cover selects while it still has its disassembly
        selected = before["yara_rule"][0]
        emptied = before["unique_blocks"][selected]["function_id"]
        without = {block_hash for block_hash, block in before["unique_blocks"].items() if block["function_id"] == emptied}
        self.storage._functions[emptied].xcfg = {}
        result = self.worker.getUniqueBlocks([sample_id])
        self.assertEqual(set(before["unique_blocks"]) - without, set(result["unique_blocks"]))
        self.assertTrue(all(block["instructions"] for block in result["unique_blocks"].values()))
        self.assertEqual(len(without), result["statistics"]["blocks_without_instructions"])
        self.assertTrue(result["yara_rule"])
        self.assertFalse(without.intersection(result["yara_rule"]))

    def test_a_stored_result_with_a_block_without_disassembly_still_renders(self):
        """A result stored before the job left such blocks out can still carry one; the cover skips it."""
        wire = json.loads(json.dumps(self.worker.getUniqueBlocks([self.sample_entry.sample_id])))
        blocks = UniqueBlocksResult.fromDict(wire)
        selected = blocks.generateBlockCover()["block_hashes"][0]
        wire["unique_blocks"][selected]["instructions"] = []
        wire["unique_blocks"][selected]["escaped_sequence"] = ""
        blocks = UniqueBlocksResult.fromDict(wire)
        cover = blocks.generateBlockCover()
        self.assertTrue(cover["block_hashes"])
        self.assertNotIn(selected, cover["block_hashes"])
        rule = blocks.generateYaraRule(wrap_at=0)
        self.assertNotIn("{  }", rule)
        for block_hash in cover["block_hashes"]:
            self.assertIn(f"$blockhash_{block_hash} = {{ {wire['unique_blocks'][block_hash]['escaped_sequence']} }}", rule)

    def test_unique_blocks_skip_only_the_function_without_disassembly(self):
        blocks = self.storage.getUniqueBlocks([self.sample_entry.sample_id])["unique_blocks"]
        emptied = next(iter(blocks.values()))["function_id"]
        self.storage._functions[emptied].xcfg = {}
        blocks = self.storage.getUniqueBlocks([self.sample_entry.sample_id])["unique_blocks"]
        self.assertTrue(all(not block["instructions"] for block in blocks.values() if block["function_id"] == emptied))
        self.assertTrue(all(block["instructions"] for block in blocks.values() if block["function_id"] != emptied))


@pytest.mark.mongo
class MongoRebuildPathsTest(unittest.TestCase):
    def setUp(self):
        mongo_config = McritConfig()
        mongo_config.STORAGE_CONFIG = buildMongoStorageConfig("test_stored_xcfg_mcrit")
        mongo_config.MINHASH_CONFIG = MinHashConfig()
        mongo_config.SHINGLER_CONFIG = ShinglerConfig()
        self.storage = StorageFactory.getStorage(mongo_config)
        self.storage.clearStorage()
        self.sample_entry = self.storage.addSmdaReport(SmdaReport.fromFile(EXAMPLE_REPORT))
        self.worker = MinHashIndex(config=deepcopy(config)).queue._worker

    def tearDown(self):
        self.storage.clearStorage()

    def test_a_missing_blob_is_skipped_by_minhashing(self):
        db = self.storage._getDb()
        unhashed = self.storage.getUnhashedFunctions()
        db.xcfg.delete_one({"_id": unhashed[0].function_id})
        # the reader decodes the missing blob to {} and still selects the function
        unhashed = self.storage.getUnhashedFunctions()
        self.assertEqual(1, sum(1 for entry in unhashed if entry.xcfg == {}))
        self.assertEqual(len(unhashed) - 1, len(self.worker.calculateMinHashes(unhashed)))

    def test_pichash_recalculation_reads_older_xcfg_and_skips_an_empty_one(self):
        # the example report was written by smda 1.5.12, older than the escaper compatibility
        # threshold, so every one of its functions is recalculated from the stored xcfg
        db = self.storage._getDb()
        function_ids = [document["function_id"] for document in db.functions.find({}, {"function_id": 1, "_id": 0})]
        # an xcfg stored as {}: what importing an export of an instance with dropped disassembly writes
        db.xcfg.update_one({"_id": function_ids[0]}, {"$set": {"_xcfg": "{}"}})
        with patch("mcrit.storage.MongoDbStorage.LOGGER") as logger:
            self.storage.recalculateAllPicHashes()
        warnings = [call.args[0] for call in logger.warning.call_args_list]
        self.assertTrue(any(message.startswith("1 functions could not be updated") for message in warnings), warnings)

    def test_pichash_recalculation_does_not_count_a_skipped_functions_block_hashes(self):
        db = self.storage._getDb()
        block_hash_counts = {
            document["function_id"]: len(document.get("_picblockhashes", [])) for document in db.functions.find({}, {"function_id": 1, "_picblockhashes": 1, "_id": 0})
        }
        emptied = next(function_id for function_id, count in block_hash_counts.items() if count)
        db.xcfg.update_one({"_id": emptied}, {"$set": {"_xcfg": "{}"}})
        result = self.storage.recalculateAllPicHashes()
        self.assertEqual(sum(block_hash_counts.values()) - block_hash_counts[emptied], result["picblockhashes_updatable"])

    def test_unique_blocks_of_a_sample_without_disassembly_carry_no_instructions(self):
        sample_id = self.sample_entry.sample_id
        with_disassembly = self.storage.getUniqueBlocks([sample_id])["unique_blocks"]
        self.assertTrue(with_disassembly)
        self.assertTrue(all(block["instructions"] for block in with_disassembly.values()))
        db = self.storage._getDb()
        emptied = next(iter(with_disassembly.values()))["function_id"]
        # one blob stored as {} (an import of an export with dropped disassembly) ...
        db.xcfg.update_one({"_id": emptied}, {"$set": {"_xcfg": "{}"}})
        blocks = self.storage.getUniqueBlocks([sample_id])["unique_blocks"]
        self.assertEqual(set(with_disassembly), set(blocks))
        self.assertTrue(all(not block["instructions"] for block in blocks.values() if block["function_id"] == emptied))
        self.assertTrue(all(block["instructions"] for block in blocks.values() if block["function_id"] != emptied))
        # ... and every blob gone, as STORAGE_DROP_DISASSEMBLY leaves the sample
        self.storage.deleteXcfgForSampleId(sample_id)
        blocks = self.storage.getUniqueBlocks([sample_id])["unique_blocks"]
        self.assertEqual(set(with_disassembly), set(blocks))
        self.assertTrue(all(block["instructions"] == [] for block in blocks.values()))


if __name__ == "__main__":
    unittest.main()
