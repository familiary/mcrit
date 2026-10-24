"""A function without a PicHash is stored with PicHash 0, which is no hash it shares with anything.

An SMDA report declaring a current smda_version without metadata.pic_hash (or one smda cannot rehash)
gives its functions none, and FunctionEntry stores `pic_hash or 0`. Query matching and the in-memory
PicHash index already skipped 0, but MongoDbStorage looked it up like any other value and the vs and
vs-group matchers grouped by it, so every such function was reported as an exact match of every other
one, across unrelated samples and families - and MemoryStorage failed sample matching with KeyError: 0.
"""

import hashlib
import logging
import unittest
from copy import deepcopy

import pymongo
import pytest
from smda.common.SmdaReport import SmdaReport
from smda.SmdaConfig import SmdaConfig

from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.matchers.MatcherFlags import IS_PICHASH_FLAG
from mcrit.storage.StorageFactory import StorageFactory

from .context import config, getTestMongoServerAndPort

logging.disable(logging.CRITICAL)

NUM_FUNCTIONS = 4


def pichashless_report(name, first_mnemonic, family):
    """NUM_FUNCTIONS distinct 12-instruction functions, none shared with a report of another first_mnemonic."""
    xcfg = {}
    for index in range(NUM_FUNCTIONS):
        offset = 0x1000 + index * 0x40
        body = [(first_mnemonic, "eax, 0x%x" % (index + 1))] + [("add", "eax, %s" % register) for register in ("ebx", "ecx", "edx")] * 3 + [("inc", "eax"), ("ret", "")]
        instructions = [[offset + position, "90", mnemonic, operands] for position, (mnemonic, operands) in enumerate(body)]
        metadata = {"binweight": len(body), "characteristics": None, "confidence": None, "function_name": "", "strongly_connected_components": [], "tfidf": None}
        xcfg[str(offset)] = {"apirefs": {}, "blockrefs": {}, "blocks": {str(offset): instructions}, "inrefs": [], "offset": offset, "outrefs": {}, "metadata": metadata}
    statistics = {key: 0 for key in ("num_api_calls", "num_basic_blocks", "num_failed_functions", "num_failed_instructions", "num_function_calls", "num_instructions")}
    statistics.update({"num_functions": NUM_FUNCTIONS, "num_leaf_functions": NUM_FUNCTIONS, "num_recursive_functions": 0})
    return SmdaReport.fromDict(
        {
            "architecture": "intel",
            "base_addr": 0x1000,
            "binary_size": NUM_FUNCTIONS * 0x40,
            "bitness": 32,
            "code_areas": [],
            "confidence_threshold": 0,
            "disassembly_errors": {},
            "execution_time": 0,
            "identified_alignment": 0,
            "message": "",
            "metadata": {"family": family, "filename": name},
            "sha256": hashlib.sha256(name.encode()).hexdigest(),
            # a current version: smda keeps the absent pic_hash instead of computing one
            "smda_version": SmdaConfig.VERSION,
            "statistics": statistics,
            "status": "ok",
            "timestamp": "2026-10-11T00-00-00",
            "xcfg": xcfg,
        }
    )


class PicHashZeroTest(unittest.TestCase):
    def _mcritConfig(self):
        return deepcopy(config)

    def setUp(self):
        self.index = MinHashIndex(self._mcritConfig())
        self.storage = self.index.getStorage()
        self.storage.clearStorage()
        self.worker = self.index.queue._worker
        self.sample_a = self.storage.addSmdaReport(pichashless_report("a", "mov", "family_a"))
        self.sample_b = self.storage.addSmdaReport(pichashless_report("b", "sub", "family_b"))

    def _pichashMatches(self, report):
        return [match for function in report["matches"]["functions"] for match in function["matches"] if match[4] & IS_PICHASH_FLAG]

    def test_functions_without_a_pichash_do_not_match_each_other_by_it(self):
        stored = {entry.pichash for sample in (self.sample_a, self.sample_b) for entry in self.storage.getFunctionsBySampleId(sample.sample_id)}
        self.assertEqual({0}, stored)
        reports = {
            "sample": self.worker.getMatchesForSample(self.sample_a.sample_id),
            "vs": self.worker.getMatchesForSampleVs(self.sample_a.sample_id, self.sample_b.sample_id),
            "vs_group": self.worker.getMatchesForSampleVsGroup(self.sample_a.sample_id, [self.sample_b.sample_id]),
        }
        for kind, report in reports.items():
            with self.subTest(kind=kind):
                self.assertEqual([], self._pichashMatches(report))
                self.assertEqual([], [entry for entry in report["matches"]["samples"] if entry["family"] == "family_b"])
        for function_id in self.storage.getFunctionIdsBySampleId(self.sample_a.sample_id):
            self.assertIsNone(self.storage.getPicHashMatchesByFunctionId(function_id))

    def test_pichash_zero_is_not_a_pichash(self):
        self.assertFalse(self.storage.isPicHash(0))
        self.assertEqual(set(), self.storage.getMatchesForPicHash(0))
        self.assertEqual({}, self.storage.getMatchesForPicHashes([0]))

    def test_comparing_two_functions_without_a_pichash_sets_no_pichash_flag(self):
        function_a = self.storage.getFunctionIdsBySampleId(self.sample_a.sample_id)[0]
        function_b = self.storage.getFunctionIdsBySampleId(self.sample_b.sample_id)[0]
        result = self.index.getMatchesFunctionVs(function_a, function_b)
        self.assertEqual(0, result["match_entry"]["matches"][4] & IS_PICHASH_FLAG)

    def test_samples_and_families_without_a_pichash_can_change_family(self):
        self.assertTrue(self.storage.modifySample(self.sample_a.sample_id, {"family_name": "family_c"}))
        moved = self.storage.getSampleById(self.sample_a.sample_id)
        assert moved is not None
        self.assertEqual("family_c", moved.family)
        self.assertTrue(self.storage.modifyFamily(self.sample_b.family_id, {"family_name": "family_c"}))
        self.assertEqual(
            {moved.family_id},
            {entry.family_id for sample in (self.sample_a, self.sample_b) for entry in self.storage.getFunctionsBySampleId(sample.sample_id)},
        )


@pytest.mark.mongo
class MongoPicHashZeroTest(PicHashZeroTest):
    DB_NAME = "test_pichash_zero_mcrit"

    def _mcritConfig(self):
        server, port = getTestMongoServerAndPort()
        mcrit_config = deepcopy(config)
        mcrit_config.STORAGE_CONFIG = StorageConfig(
            STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB, STORAGE_SERVER=server, STORAGE_PORT=port, STORAGE_MONGODB_DBNAME=self.DB_NAME
        )
        return mcrit_config

    def test_a_function_document_without_a_pichash_is_skipped_by_the_batch_lookup(self):
        function_ids = self.storage.getFunctionIdsBySampleId(self.sample_a.sample_id)
        self.storage._getDb().functions.update_one({"function_id": function_ids[0]}, {"$unset": {"_pichash": ""}})
        self.assertEqual({}, self.storage.getPicHashMatchesByFunctionIds(function_ids))

    def tearDown(self):
        server, port = getTestMongoServerAndPort()
        client = pymongo.MongoClient(server, int(port))
        client.drop_database(self.DB_NAME)
        client.close()


if __name__ == "__main__":
    unittest.main()
