"""A sample whose SMDA report carried no usable timestamp stays readable.

smda reads a report whose timestamp is missing or empty as None, and SampleEntry stored that as
null; SampleEntry.fromDict then parsed the field unconditionally, so every reader of the sample -
the listing, the export, the matching jobs resolving it as a candidate, deleteSample and the
query cleanup - failed with a TypeError, and the submitting request answered 500 after the write.
"""

import json
import logging
import os
import unittest
from unittest.mock import MagicMock, patch

import falcon.testing
import pymongo
import pytest
from smda.common.SmdaReport import SmdaReport

from mcrit.config.McritConfig import McritConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.server import application_routes
from mcrit.storage.SampleEntry import SampleEntry
from mcrit.storage.StorageFactory import StorageFactory
from mcrit.Worker import Worker

from .context import config, getTestMongoServerAndPort

logging.disable(logging.CRITICAL)

THIS_FILE_PATH = str(os.path.abspath(__file__))
PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
EXAMPLE_REPORT = os.sep.join([PROJECT_ROOT, "tests", "example_report.smda"])


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


def report_json(sha256, **changes):
    with open(EXAMPLE_REPORT) as fjson:
        report = json.load(fjson)
    report["sha256"] = sha256
    report.update(changes)
    return report


class SampleEntryTimestampTest(unittest.TestCase):
    def test_a_timestamp_toDict_cannot_have_written_reads_as_none(self):
        entry_dict = SampleEntry.toDict(SampleEntry.fromDict({**self._entry_dict(), "timestamp": "2024-01-02T03-04-05"}))
        self.assertEqual("2024-01-02T03-04-05", entry_dict["timestamp"])
        # null as stored for a report without one, a missing key, and a year below 1000 as strftime
        # wrote it before Python 3.14, which pads it to four digits
        for timestamp in (None, "", "999-01-02T00-00-00", "not a timestamp", 5):
            self.assertIsNone(SampleEntry.fromDict({**self._entry_dict(), "timestamp": timestamp}).timestamp, timestamp)
        entry_dict = self._entry_dict()
        del entry_dict["timestamp"]
        self.assertIsNone(SampleEntry.fromDict(entry_dict).timestamp)

    def _entry_dict(self):
        return {
            "architecture": "intel",
            "base_addr": 0,
            "binary_size": 0,
            "binweight": 0,
            "bitness": 32,
            "component": "",
            "family_id": 0,
            "family": "",
            "filename": "",
            "is_library": False,
            "sample_id": 0,
            "sha256": "aa" * 32,
            "smda_version": "",
            "statistics": {},
            "timestamp": None,
            "version": "",
        }


class MemorySampleTimestampTest(unittest.TestCase):
    mongo_db_name = None

    def setUp(self):
        self.index = MinHashIndex(storage_config(self.mongo_db_name))
        self.storage = self.index.getStorage()
        self.storage.clearStorage()
        with patch.object(application_routes, "create_index", return_value=self.index), patch.object(McritConfig, "AUTH_TOKEN", ""):
            self.client = falcon.testing.TestClient(application_routes.get_app())

    def tearDown(self):
        self.storage.clearStorage()

    def test_a_report_without_timestamp_is_stored_and_read_back(self):
        result = self.client.simulate_post("/samples", json=report_json("11" * 32))
        self.assertEqual(falcon.HTTP_200, result.status)
        honest_id = result.json["data"]["sample_info"]["sample_id"]
        sample_ids = [honest_id]
        for number, changes in enumerate(({"timestamp": ""}, {"timestamp": None})):
            payload = report_json("%02x" % (0x22 + number) * 32)
            payload.update(changes)
            if changes["timestamp"] is None:
                del payload["timestamp"]
            result = self.client.simulate_post("/samples", json=payload)
            self.assertEqual(falcon.HTTP_200, result.status, changes)
            sample_info = result.json["data"]["sample_info"]
            self.assertIsNone(sample_info["timestamp"])
            sample_ids.append(sample_info["sample_id"])
        for sample_id in sample_ids:
            self.assertEqual(falcon.HTTP_200, self.client.simulate_get(f"/samples/{sample_id}").status, sample_id)
        self.assertEqual(set(sample_ids), {int(sample_id) for sample_id in self.client.simulate_get("/samples").json["data"]})
        self.assertEqual(set(sample_ids), set(self.storage.getSampleEntriesByIds(sample_ids)))
        family_id = self.storage.getSampleById(honest_id).family_id
        self.assertEqual(falcon.HTTP_200, self.client.simulate_get(f"/families/{family_id}", params={"with_samples": "true"}).status)
        # the whole corpus exports, and the export imports into another instance
        export = json.loads(json.dumps(self.index.getExportData()))
        target = MinHashIndex(storage_config())
        self.assertEqual(len(sample_ids), target.addImportData(export)["num_samples_imported"])
        # an untouched sample that shares every function with them still matches
        self.assertTrue(self.index.getResultForJob(self.index.getMatchesForSample(honest_id, force_recalculation=True)))
        # and the sample can be deleted again
        self.assertTrue(self.storage.deleteSample(sample_ids[1]))
        self.assertIsNone(self.storage.getSampleById(sample_ids[1]))

    def test_a_query_sample_without_timestamp_does_not_stop_the_cleanup(self):
        report = SmdaReport.fromDict(report_json("33" * 32, timestamp=""))
        assert report is not None
        self.storage.addSmdaReport(report, isQuery=True)
        self.assertEqual([None], [sample_entry.timestamp for sample_entry in self.storage.getSamples(start_index=0, limit=0, is_query=True)])
        # the cleanup reaches its own guard for such a sample instead of failing before it
        queue = MagicMock()
        queue.clean_interval = 10**9
        worker = Worker(queue=queue, config=storage_config(self.mongo_db_name), storage=self.storage)
        setattr(worker, "getQueueData", lambda *args, **kwargs: [])
        self.assertEqual(0, worker.doDbCleanup()["num_query_samples_deleted"])


@pytest.mark.mongo
class MongoSampleTimestampTest(MemorySampleTimestampTest):
    mongo_db_name = "test_sample_timestamp_mcrit"

    @classmethod
    def tearDownClass(cls):
        server, port = getTestMongoServerAndPort()
        pymongo.MongoClient(server, int(port)).drop_database(cls.mongo_db_name)


if __name__ == "__main__":
    unittest.main()
