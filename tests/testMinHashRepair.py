import json
import os
import unittest
from unittest.mock import MagicMock, patch

import falcon
import falcon.testing
import pymongo
import pytest
from smda.common.SmdaReport import SmdaReport
from smda.SmdaConfig import SmdaConfig

from mcrit.client.McritClient import McritClient
from mcrit.config.McritConfig import McritConfig
from mcrit.config.QueueConfig import QueueConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.minhash.MinHasher import MINHASH_SHINGLER_REVISION
from mcrit.queue.QueueFactory import QueueFactory
from mcrit.server.StatusResource import StatusResource
from mcrit.storage.StorageFactory import StorageFactory
from mcrit.Worker import Worker

from .context import config, getTestMongoServerAndPort

EXAMPLE_REPORT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "example_report.smda")


class SelectiveMinHashRepair(unittest.TestCase):
    """#142: only the samples an older escaper hashed are rehashed, and /status says how many
    there are"""

    def _index_with_report(self):
        index = MinHashIndex(config)
        with open(EXAMPLE_REPORT) as fjson:
            report = SmdaReport.fromDict(json.load(fjson))
        assert report is not None
        sample_entry = index.getStorage().addSmdaReport(report)
        assert sample_entry is not None
        index.updateMinHashesForSample(sample_entry.sample_id, force_recalculation=True)
        return index, sample_entry

    def _hashed_function_ids(self, index, sample_id):
        return sorted(f.function_id for f in index.getStorage().getFunctionsBySampleId(sample_id) if f.minhash)

    def test_hashing_records_the_running_smda_and_a_repair_is_a_no_op(self):
        index, sample_entry = self._index_with_report()
        storage = index.getStorage()
        self.assertGreater(len(self._hashed_function_ids(index, sample_entry.sample_id)), 0)
        self.assertEqual([], storage.getSamplesWithStaleMinHashes(Worker.getMinHashCompatibilityThreshold()))
        self.assertEqual(0, index.getStatus(with_pichash=False)["status"]["num_samples_with_stale_minhashes"])
        report = index.getResultForJob(index.repairMinHashes(force_recalculation=True))
        self.assertEqual(0, report["num_samples_stale"])
        self.assertEqual(0, report["num_samples_repaired"])

    def test_a_stale_sample_is_rehashed_in_place(self):
        index, sample_entry = self._index_with_report()
        storage = index.getStorage()
        hashed_before = self._hashed_function_ids(index, sample_entry.sample_id)
        bands_before = {band: dict(hashes) for band, hashes in storage._bands.items()}
        storage.setMinHashVersionForSamples("4.0.0", [sample_entry.sample_id])
        self.assertEqual([sample_entry.sample_id], storage.getSamplesWithStaleMinHashes("4.4.5"))
        self.assertEqual(1, index.getStatus(with_pichash=False)["status"]["num_samples_with_stale_minhashes"])
        report = index.getResultForJob(index.repairMinHashes(force_recalculation=True))
        self.assertEqual(1, report["num_samples_repaired"])
        self.assertEqual(len(hashed_before), report["num_functions_dropped"])
        self.assertEqual(len(hashed_before), report["num_functions_rehashed"])
        self.assertEqual(SmdaConfig().VERSION, report["smda_version"])
        self.assertEqual(hashed_before, self._hashed_function_ids(index, sample_entry.sample_id))
        # the same escaper gives the same band index back, without a rebuild
        self.assertEqual(bands_before, {band: dict(hashes) for band, hashes in storage._bands.items()})
        self.assertEqual([], storage.getSamplesWithStaleMinHashes("4.4.5"))
        self.assertEqual(0, index.getStatus(with_pichash=False)["status"]["num_samples_with_stale_minhashes"])

    def test_a_sample_whose_disassembly_is_gone_keeps_its_minhashes(self):
        """hash first, drop second: without disassembly nothing could replace the old hashes"""
        index, sample_entry = self._index_with_report()
        storage = index.getStorage()
        hashed_before = self._hashed_function_ids(index, sample_entry.sample_id)
        storage.setMinHashVersionForSamples("4.0.0", [sample_entry.sample_id])
        storage.deleteXcfgForSampleId(sample_entry.sample_id)
        report = index.getResultForJob(index.repairMinHashes(force_recalculation=True))
        self.assertEqual(1, report["num_samples_stale"])
        self.assertEqual(1, report["num_samples_skipped"])
        self.assertEqual(0, report["num_samples_repaired"])
        self.assertEqual(0, report["num_functions_dropped"])
        self.assertEqual(hashed_before, self._hashed_function_ids(index, sample_entry.sample_id))
        # still stale: the next smda with disassembly at hand can repair it
        self.assertEqual([sample_entry.sample_id], storage.getSamplesWithStaleMinHashes("4.4.5"))

    def test_a_sample_without_a_recorded_version_counts_as_stale(self):
        index, sample_entry = self._index_with_report()
        storage = index.getStorage()
        storage._minhash_versions.clear()
        self.assertEqual([sample_entry.sample_id], storage.getSamplesWithStaleMinHashes("4.4.5"))
        storage.setMinHashVersionForSamples("garbage", [sample_entry.sample_id])
        self.assertEqual([sample_entry.sample_id], storage.getSamplesWithStaleMinHashes("4.4.5"))

    def test_the_full_recalculation_records_the_version_for_every_sample(self):
        index, sample_entry = self._index_with_report()
        storage = index.getStorage()
        storage._minhash_versions.clear()
        index.getResultForJob(index.recalculateMinHashes(force_recalculation=True))
        self.assertEqual([], storage.getSamplesWithStaleMinHashes("4.4.5"))


AARCH64_REPORT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "crossarch_aarch64_a.smda")


class ShinglerRevisionRepair(unittest.TestCase):
    """#238: minhashes computed before a shingler changed for a sample's architecture are stale,
    and repairMinHashes gives them the ones the running shinglers compute"""

    def _index(self, storage_config=None):
        index = MinHashIndex(config if storage_config is None else storage_config)
        entries = {}
        for name, path in (("aarch64", AARCH64_REPORT), ("intel", EXAMPLE_REPORT)):
            report = SmdaReport.fromFile(path)
            assert report is not None
            entries[name] = index.getStorage().addSmdaReport(report)
            index.updateMinHashesForSample(entries[name].sample_id, force_recalculation=True)
        return index, entries

    def _minhashes(self, index, sample_id):
        return {f.function_id: f.minhash for f in index.getStorage().getFunctionsBySampleId(sample_id) if f.minhash}

    def _from_before_the_revision(self, index, entries):
        """Both samples as a release before revision 1 left them: minhashes computed without the AArch64
        frame size, and no revision recorded."""
        storage = index.getStorage()
        minhasher = index.queue._worker.minhasher
        # ShingleLoader loads the shinglers from their files, so patch the instance the worker hashes with,
        # and hash in this process: the worker's pool would not see the patch
        (fuzzy_stat_pair,) = [shingler for shingler in minhasher._shinglers if type(shingler).__name__ == "FuzzyStatPairShingler"]
        with patch.object(fuzzy_stat_pair, "_getAArch64StackSize", return_value=0):
            for entry in entries.values():
                functions = [(f.function_id, f.toSmdaFunction()) for f in storage.getFunctionsBySampleId(entry.sample_id) if f.xcfg]
                old_minhashes = minhasher.calculateMinHashesFromStorage([(i, f) for i, f in functions if minhasher.isMinHashableFunction(f)])
                storage.deleteMinHashesForSample(entry.sample_id)
                storage.addMinHashes(old_minhashes)
                storage.setMinHashVersionForSamples(SmdaConfig().VERSION, [entry.sample_id])
        self._forget_revisions(storage, [entry.sample_id for entry in entries.values()])

    def _forget_revisions(self, storage, sample_ids):
        for sample_id in sample_ids:
            storage._minhash_shingler_revisions.pop(sample_id, None)

    def _set_revision(self, storage, sample_id, revision):
        storage._minhash_shingler_revisions[sample_id] = revision

    def test_an_older_recorded_revision_is_stale(self):
        index, entries = self._index()
        storage = index.getStorage()
        self._set_revision(storage, entries["aarch64"].sample_id, 0)
        self._set_revision(storage, entries["intel"].sample_id, 0)
        self.assertEqual([entries["aarch64"].sample_id], storage.getSamplesWithStaleMinHashes(Worker.getMinHashCompatibilityThreshold()))

    def test_a_sample_with_nothing_large_enough_to_hash_is_repaired(self):
        """it holds no minhashes at all, so repairing it records it as current rather than skipping it forever"""
        index = self._index()[0]
        storage = index.getStorage()
        report = SmdaReport.fromFile(AARCH64_REPORT)
        assert report is not None
        report.sha256 = "ab" * 32
        report.xcfg = {offset: function for offset, function in report.xcfg.items() if function.num_instructions <= 10}
        entry = storage.addSmdaReport(report)
        index.updateMinHashesForSample(entry.sample_id, force_recalculation=True)
        self.assertEqual({}, self._minhashes(index, entry.sample_id))
        self._forget_revisions(storage, [entry.sample_id])
        self.assertIn(entry.sample_id, storage.getSamplesWithStaleMinHashes(Worker.getMinHashCompatibilityThreshold()))
        # and one without any function at all
        empty_report = SmdaReport.fromFile(AARCH64_REPORT)
        assert empty_report is not None
        empty_report.sha256 = "cd" * 32
        empty_report.xcfg = {}
        empty = storage.addSmdaReport(empty_report)
        index.updateMinHashesForSample(empty.sample_id, force_recalculation=True)
        self._forget_revisions(storage, [entry.sample_id, empty.sample_id])
        self.assertTrue({entry.sample_id, empty.sample_id} <= set(storage.getSamplesWithStaleMinHashes(Worker.getMinHashCompatibilityThreshold())))
        report = index.getResultForJob(index.repairMinHashes(force_recalculation=True))
        self.assertEqual(0, report["num_samples_skipped"])
        self.assertEqual([], storage.getSamplesWithStaleMinHashes(Worker.getMinHashCompatibilityThreshold()))

    def test_aarch64_minhashes_from_before_the_revision_are_stale_and_repaired(self):
        index, entries = self._index()
        storage = index.getStorage()
        current = self._minhashes(index, entries["aarch64"].sample_id)
        intel = self._minhashes(index, entries["intel"].sample_id)
        self.assertEqual([], storage.getSamplesWithStaleMinHashes(Worker.getMinHashCompatibilityThreshold()))
        self._from_before_the_revision(index, entries)
        self.assertNotEqual(current, self._minhashes(index, entries["aarch64"].sample_id))
        # Intel shingling did not change, so an Intel sample without a revision is not stale
        self.assertEqual([entries["aarch64"].sample_id], storage.getSamplesWithStaleMinHashes(Worker.getMinHashCompatibilityThreshold()))
        self.assertEqual(1, index.getStatus(with_pichash=False)["status"]["num_samples_with_stale_minhashes"])
        report = index.getResultForJob(index.repairMinHashes(force_recalculation=True))
        self.assertEqual(1, report["num_samples_repaired"])
        self.assertEqual(MINHASH_SHINGLER_REVISION, report["shingler_revision"])
        self.assertEqual(current, self._minhashes(index, entries["aarch64"].sample_id))
        self.assertEqual(intel, self._minhashes(index, entries["intel"].sample_id))
        self.assertEqual([], storage.getSamplesWithStaleMinHashes(Worker.getMinHashCompatibilityThreshold()))


@pytest.mark.mongo
class ShinglerRevisionRepairMongo(ShinglerRevisionRepair):
    DB_NAME = "test_shingler_revision_repair"

    def _index(self, storage_config=None):
        server, port = getTestMongoServerAndPort()
        self.addCleanup(lambda: pymongo.MongoClient(server, int(port)).drop_database(self.DB_NAME))
        mongo_config = McritConfig()
        mongo_config.STORAGE_CONFIG = StorageConfig(
            STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB, STORAGE_SERVER=server, STORAGE_PORT=port, STORAGE_MONGODB_DBNAME=self.DB_NAME
        )
        mongo_config.QUEUE_CONFIG = QueueConfig(QUEUE_METHOD=QueueFactory.QUEUE_METHOD_FAKE)
        pymongo.MongoClient(server, int(port)).drop_database(self.DB_NAME)
        return super()._index(mongo_config)

    def _forget_revisions(self, storage, sample_ids):
        storage._database.samples.update_many({"sample_id": {"$in": sample_ids}}, {"$unset": {"minhash_shingler_revision": ""}})

    def _set_revision(self, storage, sample_id, revision):
        storage._database.samples.update_one({"sample_id": sample_id}, {"$set": {"minhash_shingler_revision": revision}})


class RepairRouteAndClient(unittest.TestCase):
    def test_the_route_schedules_the_job(self):
        index = MagicMock()
        index.repairMinHashes.return_value = "0123456789abcdef01234567"
        resp = falcon.Response()
        StatusResource(index).on_post_repair_minhashes(falcon.Request(falcon.testing.create_environ(path="/repair_minhashes", method="POST")), resp)
        assert resp.data is not None
        self.assertEqual("0123456789abcdef01234567", json.loads(resp.data)["data"])
        index.repairMinHashes.assert_called_once_with(force_recalculation=True)

    def test_the_client_posts_and_answers_the_job_id(self):
        response = MagicMock(status_code=200)
        response.json.return_value = {"status": "successful", "data": "0123456789abcdef01234567"}
        with patch("mcrit.client.McritClient.requests.post", return_value=response) as post:
            self.assertEqual("0123456789abcdef01234567", McritClient("http://mcrit.test").repairMinHashes())
        self.assertIn("/repair_minhashes", post.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
