"""An imported sample's functions go only to the sample the import itself created.

addImportData checked the export's dict key, importSampleEntry the entry's own sha256, and when
that one was already known importSampleEntry returned the entry unchanged - still carrying the
sample_id of the exporting instance - so its functions were attached to whatever local sample had
that id, or to none. That took a crafted export (any key, the entry stating an existing sha256 or,
on MongoDB, a query operator) or two imports of the same export running at once.
"""

import collections
import copy
import json
import logging
import os
import unittest
from unittest.mock import patch

import pymongo
import pytest
from smda.common.SmdaReport import SmdaReport

from mcrit.config.McritConfig import McritConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.storage.SampleEntry import SampleEntry
from mcrit.storage.StorageFactory import StorageFactory

from .context import config, getTestMongoServerAndPort

logging.disable(logging.CRITICAL)

THIS_FILE_PATH = str(os.path.abspath(__file__))
PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
EXAMPLE_REPORT = os.sep.join([PROJECT_ROOT, "tests", "example_report.smda"])
EXAMPLE_REPORT_2 = os.sep.join([PROJECT_ROOT, "tests", "example_report_2.smda"])
ORPHAN_ID = 10**9


def storage_config(mongo_db_name=None):
    mcrit_config = McritConfig()
    if mongo_db_name is None:
        mcrit_config.STORAGE_CONFIG = StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MEMORY)
    else:
        server, port = getTestMongoServerAndPort()
        mcrit_config.STORAGE_CONFIG = StorageConfig(
            STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB, STORAGE_SERVER=server, STORAGE_PORT=port, STORAGE_MONGODB_DBNAME=mongo_db_name
        )
    mcrit_config.MINHASH_CONFIG = config.MINHASH_CONFIG
    mcrit_config.SHINGLER_CONFIG = config.SHINGLER_CONFIG
    mcrit_config.QUEUE_CONFIG = config.QUEUE_CONFIG
    return mcrit_config


def load_report(path):
    with open(path) as fjson:
        report = SmdaReport.fromDict(json.load(fjson))
    assert report is not None
    return report


class MemoryImportSampleIdentityTest(unittest.TestCase):
    mongo_db_name = None

    def setUp(self):
        self.index = MinHashIndex(storage_config(self.mongo_db_name))
        self.storage = self.index.getStorage()
        self.storage.clearStorage()
        self.victim = self.storage.addSmdaReport(load_report(EXAMPLE_REPORT))
        self.other = self.storage.addSmdaReport(load_report(EXAMPLE_REPORT_2))
        assert self.victim is not None and self.other is not None
        # the other sample as this instance exports it, JSON on the wire
        self.export = json.loads(json.dumps(self.index.getExportData([self.other.sample_id])))

    def tearDown(self):
        self.storage.clearStorage()

    def _function_counts(self, storage=None):
        """functions per sample_id they carry, whether or not a sample has that id"""
        return collections.Counter(function_entry.sample_id for function_entry in (storage or self.storage).getFunctions(0, 0))

    def _crafted(self, sha256, sample_id, key="ab" * 32):
        crafted = copy.deepcopy(self.export)
        ((exported_key, entry),) = crafted["sample_entries"].items()
        entry["sha256"] = sha256
        entry["sample_id"] = sample_id
        crafted["sample_entries"] = {key: entry}
        crafted["function_entries"] = {key: crafted["function_entries"].pop(exported_key)}
        return crafted

    def test_importSampleEntry_answers_none_for_a_known_sha256(self):
        entry = SampleEntry(load_report(EXAMPLE_REPORT), sample_id=self.other.sample_id, family_id=self.victim.family_id)
        self.assertIsNone(self.storage.importSampleEntry(entry))
        self.assertEqual(2, len(self.storage.getSampleIds()))

    def test_an_entry_stating_a_known_sha256_brings_no_functions(self):
        before = self._function_counts()
        for sample_id in (self.victim.sample_id, ORPHAN_ID):
            report = self.index.addImportData(self._crafted(self.victim.sha256, sample_id))
            self.assertEqual(0, report["num_samples_imported"], sample_id)
            self.assertEqual(1, report["num_samples_skipped"], sample_id)
            self.assertEqual(0, report["num_functions_imported"], sample_id)
            self.assertEqual(before, self._function_counts(), sample_id)
        self.assertEqual(2, len(self.storage.getSampleIds()))

    def test_an_entry_stating_another_sha256_than_its_key_is_skipped(self):
        report = self.index.addImportData(self._crafted("cd" * 32, ORPHAN_ID))
        self.assertEqual((0, 1), (report["num_samples_imported"], report["num_samples_skipped"]))
        self.assertIsNone(self.storage.getSampleBySha256("ab" * 32))
        self.assertIsNone(self.storage.getSampleBySha256("cd" * 32))

    def test_a_sample_added_meanwhile_keeps_its_own_functions(self):
        """two imports of one export: both pass the key check before either has added the sample"""
        target = MinHashIndex(storage_config(None if self.mongo_db_name is None else self.mongo_db_name + "_target"))
        target_storage = target.getStorage()
        target_storage.clearStorage()
        try:
            # unrelated local samples, one of them holding the id the export carries
            for sha256 in ("11" * 32, "22" * 32):
                report = load_report(EXAMPLE_REPORT)
                report.sha256 = sha256
                self.assertIsNotNone(target_storage.addSmdaReport(report))
            self.assertIsNotNone(target_storage.getSampleById(self.other.sample_id))
            self.assertEqual(1, target.addImportData(self.export)["num_samples_imported"])
            imported = target_storage.getSampleBySha256(self.other.sha256)
            assert imported is not None
            self.assertNotEqual(self.other.sample_id, imported.sample_id)
            counts = self._function_counts(target_storage)
            # the key check of the second import ran before the first one had added the sample
            with patch.object(target_storage, "getPresentSha256s", return_value=set()):
                report = target.addImportData(self.export)
            self.assertEqual((0, 1), (report["num_samples_imported"], report["num_samples_skipped"]))
            self.assertEqual(counts, self._function_counts(target_storage))
        finally:
            target_storage.clearStorage()

    def test_an_honest_import_is_unchanged(self):
        target = MinHashIndex(storage_config(None if self.mongo_db_name is None else self.mongo_db_name + "_target"))
        target.getStorage().clearStorage()
        try:
            report = target.addImportData(self.export)
            self.assertEqual((1, 0, self.other.statistics["num_functions"]), (report["num_samples_imported"], report["num_samples_skipped"], report["num_functions_imported"]))
            imported = target.getStorage().getSampleBySha256(self.other.sha256)
            assert imported is not None
            self.assertEqual(self.other.statistics["num_functions"], len(target.getStorage().getFunctionIdsBySampleId(imported.sample_id) or []))
        finally:
            target.getStorage().clearStorage()


@pytest.mark.mongo
class MongoImportSampleIdentityTest(MemoryImportSampleIdentityTest):
    mongo_db_name = "test_import_sample_identity_mcrit"

    @classmethod
    def tearDownClass(cls):
        server, port = getTestMongoServerAndPort()
        client = pymongo.MongoClient(server, int(port))
        for name in (cls.mongo_db_name, cls.mongo_db_name + "_target"):
            client.drop_database(name)

    def test_an_operator_as_sha256_matches_nothing(self):
        before = self._function_counts()
        report = self.index.addImportData(self._crafted({"$exists": True}, ORPHAN_ID))
        self.assertEqual((0, 1), (report["num_samples_imported"], report["num_samples_skipped"]))
        self.assertEqual(before, self._function_counts())


if __name__ == "__main__":
    unittest.main()
