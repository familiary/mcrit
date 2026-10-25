"""What a failed import or submission leaves behind, and what imports and resubmissions add to the indexes.

A sample is written before its functions. An import that failed part way left samples without their
functions, and so did a submission whose function insert MongoDB refused; in both cases a corrected
retry was answered that the sample existed already. Imported functions were also never added to the
function range index, and resubmitting a report wrote its function names as labels unchecked.
"""

import copy
import json
import logging
import os
import unittest
from unittest import mock

import pymongo
import pymongo.errors
import pytest
from smda.common.SmdaReport import SmdaReport

from mcrit.index.MinHashIndex import MinHashIndex

from .context import config
from .testExportImportRoundTrip import mongo_config

logging.disable(logging.CRITICAL)

THIS_FILE_PATH = str(os.path.abspath(__file__))
PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))


def load_report(name="example_report.smda", family=None):
    with open(os.sep.join([PROJECT_ROOT, "tests", name])) as fjson:
        report = SmdaReport.fromDict(json.load(fjson))
    assert report is not None
    if family is not None:
        report.family = family
    return report


class IngestWriteOrderMixin(unittest.TestCase):
    """Run against both storages; subclasses provide _config()."""

    __test__ = False

    def _config(self):
        raise NotImplementedError

    def _index(self):
        return MinHashIndex(self._config())

    def _export_of(self, *report_names):
        source = self._index()
        for report_name in report_names:
            source.getStorage().addSmdaReport(load_report(report_name, family="exported"))
        return json.loads(json.dumps(source.getExportData(compress_data=False)))

    def _family_counters(self, index):
        storage = index.getStorage()
        families = {family_id: storage.getFamily(family_id) for family_id in storage.getFamilyIds()}
        # a family the import added stays, empty: only what is counted has to be as before
        counters = {family_id: (entry.num_samples, entry.num_functions) for family_id, entry in families.items() if entry is not None}
        return {family_id: counted for family_id, counted in counters.items() if counted != (0, 0)}

    def test_an_import_that_fails_part_way_stores_none_of_its_samples(self):
        export_data = self._export_of("example_report.smda", "example_report_2.smda")
        first_sha256, second_sha256 = list(export_data["sample_entries"])
        broken = copy.deepcopy(export_data)
        # the second sample's functions don't read: its first function lacks every field
        function_entries = broken["function_entries"][second_sha256]
        function_entries[next(iter(function_entries))] = {}
        target = self._index()
        counters_before = self._family_counters(target)
        with self.assertRaises(Exception):
            target.addImportData(broken)
        storage = target.getStorage()
        self.assertIsNone(storage.getSampleBySha256(first_sha256))
        self.assertIsNone(storage.getSampleBySha256(second_sha256))
        self.assertEqual(0, target.getStatus(with_pichash=False)["status"]["num_functions"])
        self.assertEqual(counters_before, self._family_counters(target))
        # and the corrected import goes through in full
        report = target.addImportData(export_data)
        self.assertEqual((2, 0), (report["num_samples_imported"], report["num_samples_skipped"]))
        for sha256 in (first_sha256, second_sha256):
            sample_entry = storage.getSampleBySha256(sha256)
            assert sample_entry is not None
            self.assertEqual(sample_entry.statistics["num_functions"], len(storage.getFunctionsBySampleId(sample_entry.sample_id)))

    def test_rolling_back_leaves_samples_that_were_there_alone(self):
        export_data = self._export_of("example_report.smda", "example_report_2.smda")
        first_sha256, second_sha256 = list(export_data["sample_entries"])
        target = self._index()
        kept = target.getStorage().addSmdaReport(load_report("example_report_3.smda"))
        assert kept is not None
        kept_functions = len(target.getStorage().getFunctionsBySampleId(kept.sample_id))
        kept_counters = self._family_counters(target)
        broken = copy.deepcopy(export_data)
        function_entries = broken["function_entries"][second_sha256]
        function_entries[next(iter(function_entries))] = {}
        with self.assertRaises(Exception):
            target.addImportData(broken)
        storage = target.getStorage()
        still_there = storage.getSampleById(kept.sample_id)
        assert still_there is not None
        self.assertEqual(kept.sha256, still_there.sha256)
        self.assertEqual(kept_functions, len(storage.getFunctionsBySampleId(kept.sample_id)))
        self.assertEqual(kept_counters, self._family_counters(target))

    def test_a_failed_import_leaves_a_sample_another_writer_stored_meanwhile_alone(self):
        export_data = self._export_of("example_report.smda", "example_report_2.smda")
        first_sha256, second_sha256 = list(export_data["sample_entries"])
        target = self._index()
        storage = target.getStorage()
        import_sample_entry = storage.importSampleEntry
        theirs = []

        def stored_meanwhile(sample_entry):
            # another writer stores the first sample between the import's check and its insert; on an
            # empty instance it gets the id it had in the export, which is what the rollback checked
            if sample_entry.sha256 == first_sha256 and not theirs:
                theirs.append(storage.addSmdaReport(load_report("example_report.smda")))
            return import_sample_entry(sample_entry)

        # an export that does not read is refused before any sample is stored, so the import fails
        # after its samples are written instead: at the function insert, as one MongoDB refuses
        with mock.patch.object(storage, "importSampleEntry", side_effect=stored_meanwhile):
            with mock.patch.object(storage, "importFunctionEntries", side_effect=RuntimeError("refused")):
                with self.assertRaises(RuntimeError):
                    target.addImportData(export_data)
        assert theirs[0] is not None
        self.assertEqual(export_data["sample_entries"][first_sha256]["sample_id"], theirs[0].sample_id)
        still_there = storage.getSampleBySha256(first_sha256)
        assert still_there is not None
        self.assertEqual(theirs[0].sample_id, still_there.sample_id)
        # with its own functions only: the import attached none of the export's to it
        self.assertEqual(theirs[0].statistics["num_functions"], len(storage.getFunctionsBySampleId(still_there.sample_id)))
        self.assertIsNone(storage.getSampleBySha256(second_sha256))

    def test_a_first_submission_keeps_every_name_its_disassembly_found(self):
        index = self._index()
        report = load_report()
        functions = list(report.getFunctions())
        long_name = "?" + "x" * 300 + "@@YAXXZ"
        functions[0].function_name = long_name
        functions[1].function_name = "café_init"
        index.addReport(report, calculate_hashes=False, username="first")
        storage = index.getStorage()
        sample_entry = storage.getSampleBySha256(report.sha256)
        labels = {entry.offset: [label.function_label for label in entry.function_labels] for entry in storage.getFunctionsBySampleId(sample_entry.sample_id)}
        self.assertEqual([long_name], labels[functions[0].offset])
        self.assertEqual(["café_init"], labels[functions[1].offset])

    def test_a_resubmitted_report_adds_only_labels_a_rename_could_have_set(self):
        index = self._index()
        report = load_report()
        index.addReport(report, calculate_hashes=False, username="first")
        functions = list(report.getFunctions())
        names = {
            functions[0].offset: "  a_valid_name  ",
            functions[1].offset: "x" * 257,
            functions[2].offset: "bell\x07",
            functions[3].offset: "sub_401000",
            functions[4].offset: "",
        }
        for smda_function in functions:
            smda_function.function_name = names.get(smda_function.offset)
        index.addReport(report, calculate_hashes=False, username="second")
        storage = index.getStorage()
        sample_entry = storage.getSampleBySha256(report.sha256)
        labels = {
            entry.offset: [label.function_label for label in entry.function_labels if label.username == "second"]
            for entry in storage.getFunctionsBySampleId(sample_entry.sample_id)
        }
        self.assertEqual(["a_valid_name"], labels[functions[0].offset])
        for offset in list(names)[1:]:
            self.assertEqual([], labels[offset], repr(names[offset]))
        # resubmitting the same names again adds nothing twice
        index.addReport(report, calculate_hashes=False, username="second")
        entry = {entry.offset: entry for entry in storage.getFunctionsBySampleId(sample_entry.sample_id)}[functions[0].offset]
        self.assertEqual(1, sum(label.username == "second" for label in entry.function_labels))


class IngestWriteOrderMemoryTest(IngestWriteOrderMixin):
    __test__ = True

    def _config(self):
        return config


@pytest.mark.mongo
class IngestWriteOrderMongoTest(IngestWriteOrderMixin):
    __test__ = True
    DB_NAME = "test_ingest_write_order"

    def setUp(self):
        server, port = self._address()
        self._client = pymongo.MongoClient(server, int(port))
        self._client.drop_database(self.DB_NAME)

    def tearDown(self):
        self._client.drop_database(self.DB_NAME)
        self._client.close()

    def _address(self):
        mcrit_config = mongo_config(self.DB_NAME)
        return mcrit_config.STORAGE_CONFIG.STORAGE_SERVER, mcrit_config.STORAGE_CONFIG.STORAGE_PORT

    def _config(self):
        return mongo_config(self.DB_NAME)

    def _export_of(self, *report_names):
        # the source has to be another database than the target
        source_config = mongo_config(self.DB_NAME + "_source")
        try:
            source = MinHashIndex(source_config)
            for report_name in report_names:
                source.getStorage().addSmdaReport(load_report(report_name, family="exported"))
            return json.loads(json.dumps(source.getExportData(compress_data=False)))
        finally:
            self._client.drop_database(self.DB_NAME + "_source")

    def test_imported_functions_are_in_the_function_range_index(self):
        export_data = self._export_of("example_report.smda")
        target = self._index()
        storage = target.getStorage()
        # a range lookup cached before the import, as a matching job running alongside it would hold
        storage._getFunctionRangeLookup()
        target.addImportData(export_data)
        sample_entry = storage.getSampleBySha256(next(iter(export_data["sample_entries"])))
        function_ids = [entry.function_id for entry in storage.getFunctionsBySampleId(sample_entry.sample_id)]
        self.assertEqual({sample_entry.sample_id: len(function_ids)}, storage.getSampleFunctionCounts([sample_entry.sample_id]))
        import numpy as np

        self.assertEqual({sample_entry.sample_id}, set(storage.getSampleIdsForFunctionIdArray(np.array(function_ids, dtype=np.int64)).tolist()))

    def _refuse_function_inserts(self, storage):
        original = storage._dbInsertMany

        def refusing(collection, data):
            if collection == "functions" and data:
                # what MongoDB answers a function document it can't store, e.g. one over 16 MiB,
                # after it stored the ones before it
                original(collection, data[:1])
                # _dbInsertMany's own answer to a refused insert other than an oversized document
                raise ValueError("Database insert failed.")
            return original(collection, data)

        return mock.patch.object(storage, "_dbInsertMany", refusing)

    def test_a_submission_whose_functions_are_refused_leaves_no_sample(self):
        index = self._index()
        storage = index.getStorage()
        report = load_report(family="refused")
        with self._refuse_function_inserts(storage):
            with self.assertRaises(ValueError):
                storage.addSmdaReport(report)
        self.assertIsNone(storage.getSampleBySha256(report.sha256))
        database = storage._getDb()
        self.assertEqual((0, 0), (database.functions.count_documents({}), database.xcfg.count_documents({})))
        self.assertEqual(0, database.families.count_documents({"num_samples": {"$ne": 0}}))
        # the retry is not told that the sample exists already
        sample_entry = storage.addSmdaReport(load_report(family="refused"))
        assert sample_entry is not None
        self.assertEqual(sample_entry.statistics["num_functions"], database.functions.count_documents({"sample_id": sample_entry.sample_id}))

    def test_an_import_whose_functions_are_refused_stores_none_of_its_samples(self):
        export_data = self._export_of("example_report.smda", "example_report_2.smda")
        target = self._index()
        storage = target.getStorage()
        with self._refuse_function_inserts(storage):
            with self.assertRaises(ValueError):
                target.addImportData(export_data)
        database = storage._getDb()
        self.assertEqual((0, 0, 0), (database.samples.count_documents({}), database.functions.count_documents({}), database.xcfg.count_documents({})))
        self.assertEqual(0, database.families.count_documents({"num_samples": {"$ne": 0}}))
        report = target.addImportData(export_data)
        self.assertEqual(2, report["num_samples_imported"])


if __name__ == "__main__":
    unittest.main()
