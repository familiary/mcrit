import json
import logging
import os
import unittest
from unittest.mock import patch

import falcon
import falcon.testing
import pymongo
import pytest
from smda.common.SmdaReport import SmdaReport

from mcrit.config.McritConfig import McritConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.libs.utility import decompress_decode
from mcrit.server.StatusResource import StatusResource
from mcrit.storage.StorageFactory import StorageFactory

from .context import config, getTestMongoServerAndPort

logging.disable(logging.CRITICAL)

TESTS_PATH = os.path.dirname(os.path.abspath(__file__))
EXAMPLE_REPORTS = [os.path.join(TESTS_PATH, name) for name in ("example_report.smda", "example_report_2.smda")]


def add_reports(index):
    sample_ids = []
    for path in EXAMPLE_REPORTS:
        with open(path) as fjson:
            report = SmdaReport.fromDict(json.load(fjson))
        assert report is not None
        sample_ids.append(index.addReport(report, calculate_hashes=False)["sample_info"]["sample_id"])
    return sample_ids


class ExportMemoryCapTest(unittest.TestCase):
    """STORAGE_MAX_EXPORT_SIZE protects the server against an export too large to hold. The export routes
    keep only the entries' JSON text, so what the cap counts is what they hold, and both refuse past it."""

    def setUp(self):
        self.index = MinHashIndex(config)
        self.index.getStorage().clearStorage()
        self.sample_ids = add_reports(self.index)

    def _get(self, path, params=None):
        app = falcon.App()
        resource = StatusResource(self.index)
        app.add_route("/export", resource, suffix="export")
        app.add_route("/export/{comma_separated_sample_ids}", resource, suffix="export_selection")
        result = falcon.testing.TestClient(app).simulate_get(path, params=params)
        self.assertEqual(len(result.content), int(result.headers["content-length"]))
        return result.json

    def _uncompressed(self, data):
        # a compressed entry is a zip archive, stamped with the time it was written
        if data["content"]["is_compressed"]:
            data["function_entries"] = {sha256: json.loads(decompress_decode(entry)) for sha256, entry in data["function_entries"].items()}
        return data

    def _counted(self, data, keys=("sample_entries", "function_entries", "family_actors")):
        # the size of the entries' JSON text, as the export counts it against the cap: family names included
        return sum(len(json.dumps(entry)) for key in keys for entry in data[key].values()) + sum(len(json.dumps(name)) for name in data["family_mapping"].values())

    def test_the_routes_answer_what_getExportData_returns(self):
        self.assertEqual(json.dumps(self.index.getExportData()).encode(), b"".join(self.index.getExportJson()))
        for compress in (False, True):
            params = {"compress": "true"} if compress else None
            for path, sample_ids in (("/export", None), (f"/export/{self.sample_ids[1]}", [self.sample_ids[1]])):
                with self.subTest(compress=compress, path=path):
                    body = self._get(path, params)
                    self.assertEqual("successful", body["status"])
                    expected = json.loads(json.dumps(self.index.getExportData(sample_ids, compress_data=compress)))
                    self.assertEqual(self._uncompressed(expected), self._uncompressed(body["data"]))
                    self.assertEqual(len(sample_ids or self.sample_ids), body["data"]["content"]["num_samples"])
        self.assertEqual({"status": "successful", "data": {}}, self._get("/export/1,a"))

    def test_the_cap_counts_the_export_text_it_holds(self):
        data = json.loads(b"".join(self.index.getExportJson()))
        counted = self._counted(data)
        self.assertEqual(len(self.sample_ids), len(data["sample_entries"]))
        with patch.object(self.index._storage_config, "STORAGE_MAX_EXPORT_SIZE", counted):
            fragments = self.index.getExportJson()
            # the entries' text is all the export holds, header and separators aside
            self.assertLess(sum(len(fragment) for fragment in fragments) - counted, 4096)
        with patch.object(self.index._storage_config, "STORAGE_MAX_EXPORT_SIZE", counted - 1):
            with self.assertRaises(MemoryError):
                self.index.getExportJson()

    def test_the_cap_counts_family_actors(self):
        # a family's actors (#57) travel in the export too, and are as plentiful as the families
        family_id = self.index.getStorage().getSampleById(self.sample_ids[0]).family_id
        self.index.modifyFamily(family_id, {"actors": [f"actor {number:02d} {'y' * 50}" for number in range(64)]})
        data = json.loads(b"".join(self.index.getExportJson()))
        self.assertEqual(64, len(data["family_actors"][str(family_id)]))
        with patch.object(self.index._storage_config, "STORAGE_MAX_EXPORT_SIZE", self._counted(data, keys=("sample_entries", "function_entries"))):
            with self.assertRaises(MemoryError):
                self.index.getExportJson()
        with patch.object(self.index._storage_config, "STORAGE_MAX_EXPORT_SIZE", self._counted(data)):
            self.assertEqual(data, json.loads(b"".join(self.index.getExportJson())))

    def test_a_compressed_sample_is_counted_while_its_text_is_held(self):
        # compressed, a sample's entries are held as text until they are compressed: the cap sees that text
        data = self._uncompressed(json.loads(b"".join(self.index.getExportJson(compress_data=True))))
        largest = max(len(json.dumps(entries)) for entries in data["function_entries"].values())
        with patch.object(self.index._storage_config, "STORAGE_MAX_EXPORT_SIZE", largest - 1):
            with self.assertRaises(MemoryError):
                self.index.getExportJson(compress_data=True)

    def test_a_sample_is_read_in_batches(self):
        # a large sample's entries are never all held as objects at once: the export takes them one batch at a time
        storage = self.index.getStorage()
        expected = {compress: self._uncompressed(json.loads(b"".join(self.index.getExportJson(compress_data=compress)))) for compress in (False, True)}
        entries = {sample_id: storage.getFunctionsBySampleId(sample_id) for sample_id in self.sample_ids}

        def one_at_a_time(sample_id, batch_size=1000):
            for function_entry in entries[sample_id]:
                yield [function_entry]

        with patch.object(storage, "getFunctionsBySampleId", side_effect=AssertionError("the whole sample was loaded at once")):
            with patch.object(storage, "getFunctionsBySampleIdInBatches", side_effect=one_at_a_time):
                for compress in (False, True):
                    with self.subTest(compress=compress):
                        self.assertEqual(expected[compress], self._uncompressed(json.loads(b"".join(self.index.getExportJson(compress_data=compress)))))

    def test_the_routes_never_build_the_export_as_python_objects(self):
        # getExportData keeps every entry as an object, several times the size the cap counts
        with patch.object(self.index, "getExportData", side_effect=AssertionError("the export was held as Python objects")):
            for path in ("/export", "/export/" + ",".join(str(sample_id) for sample_id in self.sample_ids)):
                for params in (None, {"compress": "true"}):
                    with self.subTest(path=path, params=params):
                        self.assertEqual("successful", self._get(path, params)["status"])

    def test_a_compressed_export_is_admitted_by_what_it_holds(self):
        # a sample's uncompressed text is counted while it is held, and not once it is compressed: the cap that
        # admits the export is below the raw text of all samples together
        raw = json.loads(b"".join(self.index.getExportJson()))
        raw_total = sum(len(json.dumps(entries)) for entries in raw["function_entries"].values())

        def admitted(cap):
            with patch.object(self.index._storage_config, "STORAGE_MAX_EXPORT_SIZE", cap):
                try:
                    self.index.getExportJson(compress_data=True)
                except MemoryError:
                    return False
            return True

        low, high = 0, 4 * raw_total
        self.assertTrue(admitted(high))
        while high - low > 1:
            middle = (low + high) // 2
            low, high = (low, middle) if admitted(middle) else (middle, high)
        self.assertLess(high, raw_total)

    def test_both_routes_refuse_an_export_past_the_cap(self):
        with patch.object(self.index._storage_config, "STORAGE_MAX_EXPORT_SIZE", 1024):
            for path in ("/export", "/export/" + ",".join(str(sample_id) for sample_id in self.sample_ids)):
                with self.subTest(path=path):
                    body = self._get(path)
                    self.assertEqual("failed", body["status"])
                    self.assertIn("maximum memory limit", body["data"]["message"])


class FunctionBatchesMixin(unittest.TestCase):
    def _checkBatches(self, index):
        storage = index.getStorage()
        storage.clearStorage()
        sample_id = add_reports(index)[0]
        whole = [entry.toDict() for entry in storage.getFunctionsBySampleId(sample_id)]
        batches = list(storage.getFunctionsBySampleIdInBatches(sample_id, batch_size=3))
        self.assertEqual([3] * (len(whole) // 3) + ([len(whole) % 3] if len(whole) % 3 else []), [len(batch) for batch in batches])
        self.assertEqual(whole, [entry.toDict() for batch in batches for entry in batch])
        self.assertEqual([], list(storage.getFunctionsBySampleIdInBatches(sample_id + 1000)))


class MemoryFunctionBatchesTest(FunctionBatchesMixin):
    def test_the_batches_hold_the_entries_of_the_sample(self):
        self._checkBatches(MinHashIndex(config))


@pytest.mark.mongo
class MongoDbFunctionBatchesTest(FunctionBatchesMixin):
    mongo_db_name = "test_export_function_batches_mcrit"

    def test_the_batches_hold_the_entries_of_the_sample(self):
        server, port = getTestMongoServerAndPort()
        mongo_config = McritConfig()
        mongo_config.STORAGE_CONFIG = StorageConfig(
            STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB, STORAGE_SERVER=server, STORAGE_PORT=port, STORAGE_MONGODB_DBNAME=self.mongo_db_name
        )
        mongo_config.QUEUE_CONFIG = config.QUEUE_CONFIG
        self._checkBatches(MinHashIndex(mongo_config))

    @classmethod
    def tearDownClass(cls):
        server, port = getTestMongoServerAndPort()
        pymongo.MongoClient(server, int(port)).drop_database(cls.mongo_db_name)


if __name__ == "__main__":
    unittest.main()
