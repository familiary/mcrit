"""A submitted report's sha256 must be a sha256 before it identifies a sample.
smda copies the field of a report unchecked; MongoDB read an object in its place, such as
{"$regex": "^ab"}, as a query operator and resolved the submission to an existing sample.
"""

import json
import logging
import os
import unittest
from unittest.mock import patch

import falcon.testing
import pymongo
import pytest

from mcrit.config.McritConfig import McritConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.server import application_routes
from mcrit.storage.SampleEntry import SampleEntry
from mcrit.storage.StorageFactory import StorageFactory

from .context import config, getTestMongoServerAndPort

logging.disable(logging.CRITICAL)

EXAMPLE_REPORT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "example_report.smda")


class SampleSha256Test(unittest.TestCase):
    mongo_db_name = None

    def setUp(self):
        mcrit_config = McritConfig()
        if self.mongo_db_name is None:
            mcrit_config.STORAGE_CONFIG = StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MEMORY)
        else:
            server, port = getTestMongoServerAndPort()
            mcrit_config.STORAGE_CONFIG = StorageConfig(
                STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB, STORAGE_SERVER=server, STORAGE_PORT=port, STORAGE_MONGODB_DBNAME=self.mongo_db_name
            )
        mcrit_config.MINHASH_CONFIG = config.MINHASH_CONFIG
        mcrit_config.SHINGLER_CONFIG = config.SHINGLER_CONFIG
        mcrit_config.QUEUE_CONFIG = config.QUEUE_CONFIG
        self.index = MinHashIndex(mcrit_config)
        self.storage = self.index.getStorage()
        self.storage.clearStorage()
        with patch.object(application_routes, "create_index", return_value=self.index), patch.object(McritConfig, "AUTH_TOKEN", ""):
            self.client = falcon.testing.TestClient(application_routes.get_app())

    def tearDown(self):
        self.storage.clearStorage()

    def _report(self, sha256):
        with open(EXAMPLE_REPORT) as fjson:
            report = json.load(fjson)
        report["sha256"] = sha256
        return report

    def test_a_report_with_a_real_sha256_is_accepted(self):
        self.assertTrue(SampleEntry.isValidSha256("ab" * 32))
        with open(EXAMPLE_REPORT) as fjson:
            result = self.client.simulate_post("/samples", json=json.load(fjson))
        self.assertEqual("200 OK", result.status)

    def test_a_report_with_something_else_as_sha256_is_rejected_unstored(self):
        honest = self.client.simulate_post("/samples", json=self._report("ab" * 32))
        self.assertEqual("200 OK", honest.status)
        for sha256 in ({"$regex": "^ab"}, {"$ne": ""}, "ab", "zz" * 32, "ab" * 33, "ab" * 32 + "zz", "", None, 5, ["ab" * 32]):
            result = self.client.simulate_post("/samples", json=self._report(sha256))
            self.assertEqual("400 Bad Request", result.status, sha256)
            self.assertEqual("failed", result.json["status"])
        self.assertEqual(1, len(self.client.simulate_get("/samples").json["data"]))
        # the lookup itself does not read an object as an operator either
        self.assertIsNone(self.storage.getSampleBySha256({"$regex": "^ab"}))

    def _report_with_pic_hash(self, sha256, pic_hash):
        report = self._report(sha256)
        # 4.8.0 is recent enough that smda keeps the stated PicHash instead of recalculating it
        report["smda_version"] = "4.8.0"
        next(iter(report["xcfg"].values()))["metadata"]["pic_hash"] = pic_hash
        return report

    def test_a_report_with_a_pic_hash_that_is_not_an_unsigned_64_bit_int_is_rejected_unstored(self):
        for pic_hash in (-1, 2**64, 1.5, "ab", True, {"$ne": 0}, [1]):
            result = self.client.simulate_post("/samples", json=self._report_with_pic_hash("ab" * 32, pic_hash))
            self.assertEqual("400 Bad Request", result.status, pic_hash)
            self.assertEqual("failed", result.json["status"])
        self.assertEqual({}, self.client.simulate_get("/samples").json["data"])
        # the bounds themselves and a function without a PicHash are stored
        for pic_hash, sha256 in ((0, "ab" * 32), (2**64 - 1, "cd" * 32), (None, "ef" * 32)):
            result = self.client.simulate_post("/samples", json=self._report_with_pic_hash(sha256, pic_hash))
            self.assertEqual("200 OK", result.status, pic_hash)

    def test_a_malformed_report_is_rejected(self):
        result = self.client.simulate_post("/samples", json={"sha256": "ab" * 32})
        self.assertEqual("400 Bad Request", result.status)


@pytest.mark.mongo
class MongoSampleSha256Test(SampleSha256Test):
    mongo_db_name = "test_sample_sha256_mcrit"

    @classmethod
    def tearDownClass(cls):
        server, port = getTestMongoServerAndPort()
        pymongo.MongoClient(server, int(port)).drop_database(cls.mongo_db_name)


if __name__ == "__main__":
    unittest.main()
