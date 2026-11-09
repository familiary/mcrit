"""A full minhash recalculation keeps the minhashes it can't compute again.

GET /recalculate_minhashes dropped every minhash first and then hashed what had disassembly, so the
minhashes of functions whose disassembly is gone (STORAGE_DROP_DISASSEMBLY, or never stored) were
lost for good, and their samples were stamped as hashed by the running smda all the same.
"""

import dataclasses
import json
import logging
import os
import unittest
from copy import deepcopy

import pymongo
import pytest
from smda.common.SmdaReport import SmdaReport

from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.Worker import Worker

from .context import config
from .testExportImportRoundTrip import mongo_config

logging.disable(logging.CRITICAL)

TESTS = os.path.dirname(os.path.abspath(__file__))


class RecalculationMixin(unittest.TestCase):
    __test__ = False

    def _config(self):
        raise NotImplementedError

    def _drop_disassembly(self, storage, sample_id, function_ids=None):
        raise NotImplementedError

    def _add(self, index, report_name):
        with open(os.path.join(TESTS, report_name)) as fjson:
            report = SmdaReport.fromDict(json.load(fjson))
        assert report is not None
        sample_entry = index.getStorage().addSmdaReport(report)
        assert sample_entry is not None
        index.getResultForJob(index.updateMinHashesForSample(sample_entry.sample_id, force_recalculation=True))
        return sample_entry

    def _minhashes(self, storage, sample_id):
        return {entry.function_id: entry.minhash for entry in storage.getFunctionsBySampleId(sample_id) if entry.minhash}

    def _tracking(self, mcrit_config):
        """the config with shingles tracked, so that minhashes carry a shingle composition"""
        # a new object, not an edit: McritConfig holds its sub-configs as class attributes shared by every copy
        mcrit_config.MINHASH_CONFIG = dataclasses.replace(mcrit_config.MINHASH_CONFIG, MINHASH_TRACK_SHINGLES=True)
        return mcrit_config

    def _compositions(self, storage, sample_id):
        return {entry.function_id: entry.minhash_shingle_composition for entry in storage.getFunctionsBySampleId(sample_id) if entry.minhash}

    def test_minhashes_without_disassembly_are_kept_and_found(self):
        index = MinHashIndex(self._config())
        storage = index.getStorage()
        without_disassembly = self._add(index, "example_report.smda")
        with_disassembly = self._add(index, "example_report_2.smda")
        kept_before = self._minhashes(storage, without_disassembly.sample_id)
        rehashed_before = self._minhashes(storage, with_disassembly.sample_id)
        self.assertTrue(kept_before and rehashed_before)
        self._drop_disassembly(storage, without_disassembly.sample_id)
        storage.setMinHashVersionForSamples("4.0.0", [without_disassembly.sample_id, with_disassembly.sample_id])

        num_updated = index.getResultForJob(index.recalculateMinHashes(force_recalculation=True))

        self.assertEqual(len(rehashed_before), num_updated)
        self.assertEqual(kept_before, self._minhashes(storage, without_disassembly.sample_id))
        self.assertEqual(rehashed_before, self._minhashes(storage, with_disassembly.sample_id))
        # every minhash is in the fresh band index, the kept ones included
        minhash_bits = self._config().MINHASH_CONFIG.MINHASH_SIGNATURE_BITS
        for entry in storage.getFunctionsBySampleId(without_disassembly.sample_id) + storage.getFunctionsBySampleId(with_disassembly.sample_id):
            if entry.minhash:
                self.assertIn(entry.function_id, storage.getCandidatesForMinHash(entry.getMinHash(minhash_bits=minhash_bits)))
        # only the sample that was hashed again is recorded as hashed by the running smda
        self.assertEqual([without_disassembly.sample_id], storage.getSamplesWithStaleMinHashes(Worker.getMinHashCompatibilityThreshold()))

    def test_a_repair_keeps_the_minhashes_of_functions_without_disassembly(self):
        """A sample only some of whose functions lost their disassembly is rehashed by repairMinHashes."""
        index = MinHashIndex(self._config())
        storage = index.getStorage()
        sample_entry = self._add(index, "example_report.smda")
        before = self._minhashes(storage, sample_entry.sample_id)
        without = sorted(before)[: len(before) // 2]
        self._drop_disassembly(storage, sample_entry.sample_id, function_ids=without)
        storage.setMinHashVersionForSamples("4.0.0", [sample_entry.sample_id])
        index.getResultForJob(index.repairMinHashes(force_recalculation=True))
        self.assertEqual(before, self._minhashes(storage, sample_entry.sample_id))
        minhash_bits = self._config().MINHASH_CONFIG.MINHASH_SIGNATURE_BITS
        for entry in storage.getFunctionsBySampleId(sample_entry.sample_id):
            if entry.function_id in without:
                self.assertIn(entry.function_id, storage.getCandidatesForMinHash(entry.getMinHash(minhash_bits=minhash_bits)))
        # it keeps minhashes of an older smda, so it stays listed
        self.assertEqual([sample_entry.sample_id], storage.getSamplesWithStaleMinHashes(Worker.getMinHashCompatibilityThreshold()))

    def test_a_repair_keeps_the_compositions_of_functions_without_disassembly(self):
        """re-added as rebuilt from their bytes, their minhashes wrote an empty composition"""
        index = MinHashIndex(self._tracking(self._config()))
        storage = index.getStorage()
        sample_entry = self._add(index, "example_report.smda")
        before = self._compositions(storage, sample_entry.sample_id)
        self.assertGreater(len(before), 1)
        self.assertTrue(all(before.values()))
        without = sorted(before)[: len(before) // 2]
        self._drop_disassembly(storage, sample_entry.sample_id, function_ids=without)
        storage.setMinHashVersionForSamples("4.0.0", [sample_entry.sample_id])
        report = index.getResultForJob(index.repairMinHashes(force_recalculation=True))
        self.assertEqual(1, report["num_samples_repaired"])
        # those rehashed get the composition computed again, the same as before
        self.assertEqual(before, self._compositions(storage, sample_entry.sample_id))


class RecalculationMemoryTest(RecalculationMixin):
    __test__ = True

    def _config(self):
        return deepcopy(config)

    def test_a_recalculation_keeps_the_compositions_of_functions_without_disassembly(self):
        """re-banded through addMinHash, their minhashes wrote an empty composition; MongoDB
        storage only re-bands them"""
        index = MinHashIndex(self._tracking(self._config()))
        storage = index.getStorage()
        without_disassembly = self._add(index, "example_report.smda")
        with_disassembly = self._add(index, "example_report_2.smda")
        kept_before = self._compositions(storage, without_disassembly.sample_id)
        rehashed_before = self._compositions(storage, with_disassembly.sample_id)
        self.assertTrue(kept_before and all(kept_before.values()) and rehashed_before and all(rehashed_before.values()))
        self._drop_disassembly(storage, without_disassembly.sample_id)
        index.getResultForJob(index.recalculateMinHashes(force_recalculation=True))
        self.assertEqual(kept_before, self._compositions(storage, without_disassembly.sample_id))
        self.assertEqual(rehashed_before, self._compositions(storage, with_disassembly.sample_id))

    def _drop_disassembly(self, storage, sample_id, function_ids=None):
        for entry in storage.getFunctionsBySampleId(sample_id):
            if function_ids is None or entry.function_id in function_ids:
                storage._functions[entry.function_id].xcfg = {}


@pytest.mark.mongo
class RecalculationMongoTest(RecalculationMixin):
    __test__ = True
    DB_NAME = "test_recalculate_minhashes"

    def setUp(self):
        storage_config = mongo_config(self.DB_NAME).STORAGE_CONFIG
        self._client = pymongo.MongoClient(storage_config.STORAGE_SERVER, int(storage_config.STORAGE_PORT))
        self._client.drop_database(self.DB_NAME)

    def tearDown(self):
        self._client.drop_database(self.DB_NAME)
        self._client.close()

    def _config(self):
        return mongo_config(self.DB_NAME)

    def _drop_disassembly(self, storage, sample_id, function_ids=None):
        if function_ids is None:
            function_ids = [entry.function_id for entry in storage.getFunctionsBySampleId(sample_id)]
        storage._getDb().xcfg.delete_many({"_id": {"$in": list(function_ids)}})
        # and a function from before migrate_xcfg_split with its disassembly emptied
        storage._getDb().functions.update_one({"function_id": function_ids[0]}, {"$set": {"_xcfg": "{}"}})

    def test_the_fresh_band_index_has_every_index(self):
        index = MinHashIndex(self._config())
        self._add(index, "example_report.smda")
        index.getResultForJob(index.recalculateMinHashes(force_recalculation=True))
        database = index.getStorage()._getDb()
        for band_id in range(self._config().STORAGE_CONFIG.STORAGE_NUM_BANDS):
            keys = [tuple(spec["key"]) for spec in database["band_%d" % band_id].index_information().values()]
            self.assertIn((("band_hash", 1), ("df", 1)), keys)
            self.assertIn((("band_hash", 1), ("bucket", 1)), keys)


if __name__ == "__main__":
    unittest.main()
