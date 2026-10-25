"""GET /complete_minhashes hashes the samples submitted with deferred hashing, and has to finish them
the way hashing on submission does: record the smda that produced their minhashes, so
repairMinHashes does not take them for stale forever, and drop their disassembly with
STORAGE_DROP_DISASSEMBLY. It used to do neither."""

import json
import os
import unittest
from copy import deepcopy
from unittest.mock import MagicMock, patch

import falcon
import falcon.testing
import pymongo
import pytest
from smda.common.SmdaReport import SmdaReport

from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.minhash.MinHasher import MINHASH_SHINGLER_REVISION
from mcrit.server.StatusResource import StatusResource
from mcrit.Worker import Worker

from .context import config, getTestMongoServerAndPort
from .testMinHashRepair import AARCH64_REPORT, EXAMPLE_REPORT
from .testStorage import buildMongoStorageConfig


class CompleteMinHashes(unittest.TestCase):
    def _storage_config(self):
        return deepcopy(config.STORAGE_CONFIG)

    def _forget_pending(self, index, sample_id):
        """The sample as one stored before samples were marked as pending."""
        index.getStorage()._minhash_pending.pop(sample_id)

    def _drop_xcfg(self, index, function_id):
        """What a blob over MongoDB's 16 MiB limit leaves (#42)."""
        index.getStorage()._functions[function_id].xcfg = {}

    def _set_pending(self, index, sample_id, smda_version=None, shingler_revision=None):
        """The sample as an earlier smda or shingler revision stored it."""
        storage = index.getStorage()
        stored_version, stored_revision = storage._minhash_pending[sample_id]
        storage._minhash_pending[sample_id] = (smda_version or stored_version, stored_revision if shingler_revision is None else shingler_revision)

    def _break_xcfg(self, index, function_id):
        """A disassembly smda cannot rebuild a function from."""
        function_entry = index.getStorage()._functions[function_id]
        function_entry.xcfg = {key: value for key, value in function_entry.xcfg.items() if key != "blocks"}

    def _index(self, drop_disassembly=True, workpack_size=None):
        index_config = deepcopy(config)
        # class attributes of McritConfig, which deepcopy(config) leaves shared with every other test
        index_config.STORAGE_CONFIG = self._storage_config()
        index_config.STORAGE_CONFIG.STORAGE_DROP_DISASSEMBLY = drop_disassembly
        index_config.MINHASH_CONFIG = deepcopy(config.MINHASH_CONFIG)
        if workpack_size is not None:
            index_config.MINHASH_CONFIG.MINHASH_GENERATION_WORKPACK_SIZE = workpack_size
        index = MinHashIndex(config=index_config)
        index.getStorage().clearStorage()
        return index

    def _deferred(self, index, path=EXAMPLE_REPORT, only_small=False):
        """A sample as addReport(calculate_hashes=False) leaves it: stored, nothing hashed."""
        report = SmdaReport.fromFile(path)
        assert report is not None
        if only_small:
            report.sha256 = "ab" * 32
            report.xcfg = {offset: function for offset, function in report.xcfg.items() if function.num_instructions <= 10}
        sample_entry = index.getStorage().addSmdaReport(report)
        assert sample_entry is not None
        return sample_entry.sample_id

    def _complete(self, index):
        resp = falcon.Response()
        StatusResource(index).on_get_complete_minhashes(falcon.Request(falcon.testing.create_environ(path="/complete_minhashes")), resp)
        assert resp.data is not None
        return index.getResultForJob(json.loads(resp.data)["data"])

    def _pending(self, index):
        return index.getStorage().getSamplesWithPendingMinHashes(Worker.getMinHashCompatibilityThreshold())

    def _stale(self, index):
        return index.getStorage().getSamplesWithStaleMinHashes(Worker.getMinHashCompatibilityThreshold())

    def _functions(self, index, sample_id):
        return index.getStorage().getFunctionsBySampleId(sample_id)

    def _unhashed_ids(self, index, sample_id):
        return index.getStorage().getUnhashedFunctions([f.function_id for f in self._functions(index, sample_id)], only_function_ids=True)

    def _hash_one_function(self, index, sample_id):
        storage = index.getStorage()
        unhashed = storage.getUnhashedFunctions(self._unhashed_ids(index, sample_id)[:1])
        minhashes = index.queue._worker.calculateMinHashes(unhashed)
        self.assertEqual(1, len(minhashes))
        storage.addMinHashes(minhashes)

    def test_a_deferred_sample_is_recorded_and_its_disassembly_dropped(self):
        # a workpack small enough for the sample to span several batches
        index = self._index(workpack_size=7)
        sample_id = self._deferred(index)
        self.assertEqual([sample_id], self._stale(index))
        self.assertGreater(self._complete(index), 7)
        functions = self._functions(index, sample_id)
        # functions too small to hash never get a minhash, and do not hold the sample back
        self.assertTrue(any(not f.minhash for f in functions))
        self.assertTrue(any(f.minhash for f in functions))
        self.assertEqual([], self._stale(index))
        self.assertTrue(all(not f.xcfg for f in functions))
        self.assertEqual([], self._pending(index))
        report = index.getResultForJob(index.repairMinHashes(force_recalculation=True))
        self.assertEqual(0, report["num_samples_stale"])

    def test_without_drop_disassembly_the_sample_is_recorded_and_keeps_it(self):
        index = self._index(drop_disassembly=False)
        sample_id = self._deferred(index)
        self._complete(index)
        self.assertEqual([], self._stale(index))
        self.assertTrue(all(f.xcfg for f in self._functions(index, sample_id)))

    def test_a_sample_with_nothing_large_enough_to_hash_is_finished(self):
        index = self._index()
        sample_id = self._deferred(index, AARCH64_REPORT, only_small=True)
        self.assertTrue(self._functions(index, sample_id))
        self.assertEqual(0, self._complete(index))
        self.assertEqual([], self._stale(index))
        self.assertTrue(all(not f.xcfg for f in self._functions(index, sample_id)))

    def test_a_retried_run_finishes_what_a_crashed_one_hashed_in_part(self):
        index = self._index(workpack_size=7)
        sample_id = self._deferred(index)
        worker = index.queue._worker
        calculate = worker.calculateMinHashes
        calls = []

        def crash_on_the_second_batch(function_entries, **kwargs):
            calls.append(1)
            if len(calls) == 2:
                raise RuntimeError("worker died")
            return calculate(function_entries, **kwargs)

        worker.calculateMinHashes = crash_on_the_second_batch
        with self.assertRaises(RuntimeError):
            worker.updateMinHashes(None, finish_samples=True)
        worker.calculateMinHashes = calculate
        self.assertTrue(any(f.minhash for f in self._functions(index, sample_id)))
        self.assertEqual([sample_id], self._stale(index))
        # the retry hashes the rest, and finishes the sample with the minhashes the first attempt wrote
        self.assertGreater(self._complete(index), 0)
        self.assertEqual([], self._unhashed_ids(index, sample_id))
        self.assertEqual([], self._stale(index))
        self.assertTrue(all(not f.xcfg for f in self._functions(index, sample_id)))

    def test_a_run_with_nothing_left_to_hash_still_finishes(self):
        """An overlapping run, or one before it, hashed everything without finishing anything."""
        index = self._index()
        sample_id = self._deferred(index)
        self.assertGreater(index.getResultForJob(index.updateMinHashes(None, force_recalculation=True)), 0)
        self.assertEqual(0, self._complete(index))
        self.assertEqual([], self._stale(index))
        self.assertTrue(all(not f.xcfg for f in self._functions(index, sample_id)))
        # and a repeated run changes nothing
        self.assertEqual(0, self._complete(index))
        self.assertEqual([], self._stale(index))

    def test_a_function_without_disassembly_does_not_hold_its_sample_back(self):
        index = self._index()
        sample_id = self._deferred(index)
        without = self._unhashed_ids(index, sample_id)[0]
        self._drop_xcfg(index, without)
        self._complete(index)
        self.assertEqual([without], self._unhashed_ids(index, sample_id))
        self.assertEqual([], self._stale(index))
        self.assertTrue(all(not f.xcfg for f in self._functions(index, sample_id)))

    def test_a_failed_hashing_finishes_only_the_other_samples(self):
        """A function smda cannot rebuild is skipped and stays unhashed (#266), so its sample stays
        pending with its disassembly, while the job finishes every other sample."""
        index = self._index()
        sample_id = self._deferred(index)
        other_id = self._deferred(index, AARCH64_REPORT)
        broken_id = self._unhashed_ids(index, sample_id)[0]
        self._break_xcfg(index, broken_id)
        index.queue._worker.updateMinHashes(None, finish_samples=True)
        self.assertEqual([broken_id], self._unhashed_ids(index, sample_id))
        self.assertEqual([sample_id], self._pending(index))
        self.assertEqual([sample_id], self._stale(index))
        self.assertTrue(all(f.xcfg for f in self._functions(index, sample_id)))
        self.assertFalse(any(f.xcfg for f in self._functions(index, other_id)))

    def test_a_sample_stored_by_an_older_smda_is_finished_after_its_repair(self):
        """repairMinHashes rehashed it with the running smda, so the next complete can finish it."""
        index = self._index()
        sample_id = self._deferred(index)
        self._set_pending(index, sample_id, smda_version="4.0.0")
        self.assertEqual([], self._pending(index))
        report = index.getResultForJob(index.repairMinHashes(force_recalculation=True))
        self.assertEqual(1, report["num_samples_repaired"])
        self.assertEqual([sample_id], self._pending(index))
        self._complete(index)
        self.assertEqual([], self._pending(index))
        self.assertEqual([], self._stale(index))
        self.assertTrue(all(not f.xcfg for f in self._functions(index, sample_id)))

    def test_a_sample_stored_before_a_shingler_revision_is_finished_after_its_recalculation(self):
        index = self._index()
        sample_id = self._deferred(index)
        self._set_pending(index, sample_id, shingler_revision=MINHASH_SHINGLER_REVISION - 1)
        self.assertEqual([], self._pending(index))
        index.getResultForJob(index.recalculateMinHashes(force_recalculation=True))
        self.assertEqual([sample_id], self._pending(index))
        self._complete(index)
        self.assertEqual([], self._pending(index))
        self.assertTrue(all(not f.xcfg for f in self._functions(index, sample_id)))

    def test_samples_not_waiting_for_their_minhashes_are_left_to_repair(self):
        index = self._index()
        # hashed by /complete_minhashes before samples were marked as pending
        legacy_id = self._deferred(index)
        self._forget_pending(index, legacy_id)
        index.getResultForJob(index.updateMinHashes(None, force_recalculation=True))
        # imported with minhashes an older smda computed, with a function left to hash
        older_id = self._deferred(index, AARCH64_REPORT)
        self._forget_pending(index, older_id)
        self._hash_one_function(index, older_id)
        index.getStorage().setMinHashVersionForSamples("4.0.0", [older_id])
        self._complete(index)
        self.assertEqual([legacy_id, older_id], self._stale(index))
        self.assertTrue(all(f.xcfg for f in self._functions(index, legacy_id)))
        self.assertTrue(all(f.xcfg for f in self._functions(index, older_id)))

    def test_a_sample_stored_by_an_incompatible_smda_is_left_to_repair(self):
        """minhashes written before an escaper change may have been escaped differently"""
        index = self._index()
        sample_id = self._deferred(index)
        self._set_pending(index, sample_id, smda_version="4.0.0")
        self.assertGreater(self._complete(index), 0)
        self.assertEqual([sample_id], self._stale(index))
        self.assertTrue(all(f.xcfg for f in self._functions(index, sample_id)))

    def test_a_sample_stored_by_a_compatible_older_smda_is_finished(self):
        index = self._index()
        sample_id = self._deferred(index)
        self._set_pending(index, sample_id, smda_version=Worker.getMinHashCompatibilityThreshold())
        self._complete(index)
        self.assertEqual([], self._stale(index))
        self.assertTrue(all(not f.xcfg for f in self._functions(index, sample_id)))

    def test_a_repaired_or_recalculated_sample_is_still_finished(self):
        """Neither drops disassembly, so neither may end the wait; the next complete does."""
        index = self._index()
        repaired_id = self._deferred(index)
        report = index.getResultForJob(index.repairMinHashes(force_recalculation=True))
        self.assertEqual(1, report["num_samples_repaired"])
        self.assertEqual([], self._stale(index))
        recalculated_id = self._deferred(index, AARCH64_REPORT)
        index.getResultForJob(index.recalculateMinHashes(force_recalculation=True))
        self.assertEqual([], self._stale(index))
        self.assertEqual([repaired_id, recalculated_id], self._pending(index))
        self.assertTrue(all(f.xcfg for f in self._functions(index, repaired_id)))
        self.assertEqual(0, self._complete(index))
        self.assertEqual([], self._pending(index))
        self.assertTrue(all(not f.xcfg for f in self._functions(index, repaired_id)))
        self.assertTrue(all(not f.xcfg for f in self._functions(index, recalculated_id)))

    def test_the_marker_is_written_after_the_functions(self):
        index = self._index()
        storage = index.getStorage()
        report = SmdaReport.fromFile(EXAMPLE_REPORT)
        assert report is not None
        # the last step of an insert before the marker: the functions are written by then
        with patch.object(storage, "_updateFamilyStats", side_effect=RuntimeError("interrupted")):
            with self.assertRaises(RuntimeError):
                storage.addSmdaReport(report)
        sample_entry = storage.getSampleBySha256(report.sha256)
        assert sample_entry is not None
        self.assertTrue(self._functions(index, sample_entry.sample_id))
        self.assertEqual([], self._pending(index))

    def test_query_samples_are_never_pending(self):
        index = self._index()
        sample_id = self._deferred(index)
        report = SmdaReport.fromFile(AARCH64_REPORT)
        assert report is not None
        query_entry = index.getStorage().addSmdaReport(report, isQuery=True)
        assert query_entry is not None
        self.assertLess(query_entry.sample_id, 0)
        self.assertEqual([sample_id], self._pending(index))

    def test_the_other_callers_of_update_minhashes_are_unchanged(self):
        index = self._index()
        sample_id = self._deferred(index)
        # plain updateMinHashes(None) hashes, records nothing and keeps the disassembly
        self.assertGreater(index.getResultForJob(index.updateMinHashes(None, force_recalculation=True)), 0)
        self.assertEqual([sample_id], self._stale(index))
        self.assertTrue(all(f.xcfg for f in self._functions(index, sample_id)))
        # recalculateMinHashes records every sample itself and has never dropped disassembly
        index.getResultForJob(index.recalculateMinHashes(force_recalculation=True))
        self.assertEqual([], self._stale(index))
        self.assertTrue(all(f.xcfg for f in self._functions(index, sample_id)))
        self.assertEqual([sample_id], self._pending(index))
        # updateMinHashesForSample records and drops its sample even with a minhash from before
        other_id = self._deferred(index, AARCH64_REPORT)
        self._hash_one_function(index, other_id)
        index.getResultForJob(index.updateMinHashesForSample(other_id, force_recalculation=True))
        self.assertEqual([], self._stale(index))
        self.assertTrue(all(not f.xcfg for f in self._functions(index, other_id)))
        self.assertEqual([sample_id], self._pending(index))

    def test_the_route_asks_for_the_samples_to_be_finished(self):
        index = MagicMock()
        index.updateMinHashes.return_value = "0123456789abcdef01234567"
        StatusResource(index).on_get_complete_minhashes(falcon.Request(falcon.testing.create_environ(path="/complete_minhashes")), falcon.Response())
        index.updateMinHashes.assert_called_once_with(None, finish_samples=True, force_recalculation=True, username=None)


@pytest.mark.mongo
class CompleteMinHashesMongo(CompleteMinHashes):
    # per run, so concurrent runs against the same mongod do not collide
    DB_NAME = f"test_complete_minhashes_{os.getpid()}"

    def setUp(self):
        self.addCleanup(self._drop_database)

    def _drop_database(self):
        server, port = getTestMongoServerAndPort()
        with pymongo.MongoClient(server, int(port)) as client:
            client.drop_database(self.DB_NAME)

    def _storage_config(self):
        return buildMongoStorageConfig(self.DB_NAME)

    def _forget_pending(self, index, sample_id):
        index.getStorage()._getDb().samples.update_one({"sample_id": sample_id}, {"$unset": {"minhash_pending": ""}})

    def _drop_xcfg(self, index, function_id):
        index.getStorage()._getDb().xcfg.delete_one({"_id": function_id})

    def _set_pending(self, index, sample_id, smda_version=None, shingler_revision=None):
        stored = {"minhash_pending.smda_version": smda_version, "minhash_pending.shingler_revision": shingler_revision}
        index.getStorage()._getDb().samples.update_one({"sample_id": sample_id}, {"$set": {key: value for key, value in stored.items() if value is not None}})

    def _break_xcfg(self, index, function_id):
        db = index.getStorage()._getDb()
        xcfg = json.loads(db.xcfg.find_one({"_id": function_id})["_xcfg"])
        del xcfg["blocks"]
        db.xcfg.update_one({"_id": function_id}, {"$set": {"_xcfg": json.dumps(xcfg)}})

    def test_a_blob_stored_as_empty_is_no_disassembly(self):
        """What importing an export of an instance with dropped disassembly writes."""
        index = self._index()
        sample_id = self._deferred(index)
        storage = index.getStorage()
        empty_blob = self._unhashed_ids(index, sample_id)[0]
        storage._getDb().xcfg.update_one({"_id": empty_blob}, {"$set": {"_xcfg": "{}"}})
        self._complete(index)
        self.assertEqual([empty_blob], self._unhashed_ids(index, sample_id))
        self.assertEqual([], self._stale(index))


if __name__ == "__main__":
    unittest.main()
