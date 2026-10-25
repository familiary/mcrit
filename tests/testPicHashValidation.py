"""A pichash or picblockhash is stored only if it is an integer of 0..2**64-1.

smda keeps the pic_hash a current report states and an import carries the exported values, so
both are whatever was sent. MongoDB stored a negative one as "0x-000000000000005", which no
reader decodes, one of 2**64 or more wider than the 16 digits pichash ordering relies on (#145),
and an operator dict as a sub-document that failed the insert halfway.
"""

import json
import logging
import os
import unittest
from unittest.mock import patch

import falcon.testing
import pymongo
import pytest
from smda.common.SmdaReport import SmdaReport
from smda.SmdaConfig import SmdaConfig

from mcrit.config.McritConfig import McritConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.libs.utility import compress_encode, decompress_decode
from mcrit.server import application_routes
from mcrit.storage.FunctionEntry import isValidPicHash
from mcrit.storage.StorageFactory import StorageFactory

from .context import config, getTestMongoServerAndPort

logging.disable(logging.CRITICAL)

THIS_FILE_PATH = str(os.path.abspath(__file__))
PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
EXAMPLE_REPORT = os.sep.join([PROJECT_ROOT, "tests", "example_report.smda"])
DB_NAME = "test_pichash_validation_mcrit"

UNSTORABLE = (-5, 1 << 64, {"$eq": 5}, True, 5.0, "0x5")


def storage_config(mongo_db_name=None):
    mcrit_config = McritConfig()
    if mongo_db_name is None:
        mcrit_config.STORAGE_CONFIG = StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MEMORY, STORAGE_DROP_DISASSEMBLY=False)
    else:
        server, port = getTestMongoServerAndPort()
        mcrit_config.STORAGE_CONFIG = StorageConfig(
            STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB, STORAGE_SERVER=server, STORAGE_PORT=port, STORAGE_MONGODB_DBNAME=mongo_db_name, STORAGE_DROP_DISASSEMBLY=False
        )
    # the same hashing configuration as the in-memory test config, so exports import across both
    mcrit_config.MINHASH_CONFIG = config.MINHASH_CONFIG
    mcrit_config.SHINGLER_CONFIG = config.SHINGLER_CONFIG
    mcrit_config.QUEUE_CONFIG = config.QUEUE_CONFIG
    return mcrit_config


def report_json_stating(pic_hash, sha256="aa" * 32):
    """The example report under another sha256, its first function stating the given pic_hash.

    A report of the running smda version, so that smda keeps the stated value instead of
    recomputing it, as it does for older reports.
    """
    with open(EXAMPLE_REPORT) as fjson:
        report_json = json.load(fjson)
    report_json["sha256"] = sha256
    report_json["smda_version"] = SmdaConfig().VERSION
    report_json["xcfg"][next(iter(report_json["xcfg"]))]["metadata"]["pic_hash"] = pic_hash
    return report_json


class PicHashValueTest(unittest.TestCase):
    def test_what_a_pichash_may_be(self):
        for valid in (0, 1, 0x4D2, (1 << 64) - 1):
            self.assertTrue(isValidPicHash(valid), valid)
        for invalid in (-1, 1 << 64, *UNSTORABLE, None, [5]):
            self.assertFalse(isValidPicHash(invalid), invalid)


class PicHashValidationTest(unittest.TestCase):
    """Over a memory storage; MongoPicHashValidationTest runs the same over MongoDB."""

    def setUp(self):
        self.index = MinHashIndex(storage_config())

    def _sample_count(self):
        return len(self.index.getStorage().getSampleIds())

    def test_a_report_stating_an_unstorable_pic_hash_is_refused_before_anything_is_written(self):
        storage = self.index.getStorage()
        for pic_hash in UNSTORABLE:
            report = SmdaReport.fromDict(report_json_stating(pic_hash))
            assert report is not None
            # smda hands the value on as it was stated
            self.assertIn(pic_hash, [smda_function.pic_hash for smda_function in report.getFunctions()])
            for is_query in (False, True):
                with self.assertRaises(ValueError, msg=(pic_hash, is_query)):
                    storage.addSmdaReport(report, isQuery=is_query)
            self.assertIsNone(storage.getSampleBySha256(report.sha256), pic_hash)
            self.assertIsNone(storage.getSampleBySha256(report.sha256, is_query=True), pic_hash)
        self.assertEqual(0, self._sample_count())
        # a missing pic_hash is stored as 0, as before, and so is every value in range
        for number, pic_hash in enumerate((None, 0, (1 << 64) - 1)):
            report = SmdaReport.fromDict(report_json_stating(pic_hash, sha256="%02x" % (0xB0 + number) * 32))
            assert report is not None
            sample_entry = storage.addSmdaReport(report)
            assert sample_entry is not None
            self.assertIn(pic_hash or 0, {function_entry.pichash for function_entry in storage.getFunctionsBySampleId(sample_entry.sample_id) or []})

    def test_post_samples_answers_400_and_stores_nothing(self):
        with patch.object(application_routes, "create_index", return_value=self.index), patch.object(McritConfig, "AUTH_TOKEN", ""):
            client = falcon.testing.TestClient(application_routes.get_app())
        for pic_hash in (-5, 1 << 64, {"$eq": 5}):
            result = client.simulate_post("/samples", json=report_json_stating(pic_hash))
            self.assertEqual(falcon.HTTP_400, result.status, pic_hash)
            self.assertIn("pic_hash", result.json["data"]["message"])
        # a report smda refuses as malformed is a client error as well
        malformed = report_json_stating(0x1234)
        malformed["xcfg"][next(iter(malformed["xcfg"]))]["blocks"] = []
        self.assertEqual(falcon.HTTP_400, client.simulate_post("/samples", json=malformed).status)
        self.assertEqual(0, self._sample_count())
        result = client.simulate_post("/samples", json=report_json_stating(0x1234))
        self.assertEqual(falcon.HTTP_200, result.status)
        self.assertFalse(result.json["data"]["existed"])
        self.assertEqual(1, self._sample_count())

    def _export(self, tamper, compress_data=False):
        source = MinHashIndex(storage_config())
        with open(EXAMPLE_REPORT) as fjson:
            report = SmdaReport.fromDict(json.load(fjson))
        assert report is not None
        source.getStorage().addSmdaReport(report)
        export_data = json.loads(json.dumps(source.getExportData(compress_data=compress_data)))
        sha256 = next(iter(export_data["function_entries"]))
        function_entries = export_data["function_entries"][sha256]
        if compress_data:
            function_entries = json.loads(decompress_decode(function_entries))
        tamper(next(entry for entry in function_entries.values() if entry["picblockhashes"]))
        export_data["function_entries"][sha256] = compress_encode(json.dumps(function_entries)) if compress_data else function_entries
        return export_data, sha256

    def test_an_import_skips_a_sample_carrying_an_unstorable_value(self):
        def set_pichash(value):
            return lambda entry: entry.update(pichash=value)

        def set_blockhash(value):
            return lambda entry: entry["picblockhashes"][0].update(hash=value)

        tampers = [set_pichash(value) for value in UNSTORABLE] + [set_blockhash(value) for value in (-7, 1 << 64, None)]
        tampers.append(lambda entry: entry.update(picblockhashes="0x5"))
        for number, tamper in enumerate(tampers):
            export_data, sha256 = self._export(tamper, compress_data=number % 2 == 1)
            import_report = self.index.addImportData(export_data)
            self.assertEqual(0, import_report["num_samples_imported"], number)
            self.assertEqual(1, import_report["num_samples_skipped"], number)
            self.assertEqual(0, import_report["num_functions_imported"], number)
            self.assertIsNone(self.index.getStorage().getSampleBySha256(sha256), number)
        self.assertEqual(0, self._sample_count())
        self.assertEqual(0, self.index.getStatus(with_pichash=False)["status"]["num_functions"])
        # the same export untouched imports as before
        export_data, sha256 = self._export(lambda entry: None)
        self.assertEqual(1, self.index.addImportData(export_data)["num_samples_imported"])
        self.assertIsNotNone(self.index.getStorage().getSampleBySha256(sha256))


@pytest.mark.mongo
class MongoPicHashValidationTest(PicHashValidationTest):
    def setUp(self):
        server, port = getTestMongoServerAndPort()
        self.client = pymongo.MongoClient(server, int(port))
        self.client.drop_database(DB_NAME)
        self.index = MinHashIndex(storage_config(DB_NAME))
        self.db = self.client[DB_NAME]
        self.assertTrue(self.index.getStorage().isPichashPadded())

    def tearDown(self):
        self.client.drop_database(DB_NAME)

    def test_nothing_is_left_behind(self):
        storage = self.index.getStorage()
        for pic_hash in UNSTORABLE:
            for is_query in (False, True):
                report = SmdaReport.fromDict(report_json_stating(pic_hash))
                assert report is not None
                with self.assertRaises(ValueError):
                    storage.addSmdaReport(report, isQuery=is_query)
        for collection in (
            "samples",
            "functions",
            "query_samples",
            "query_functions",
            "xcfg",
            "query_xcfg",
            "smda_extras",
            "query_smda_extras",
            "function_ranges",
            "pichash_counts",
        ):
            self.assertEqual(0, self.db[collection].count_documents({}), collection)


if __name__ == "__main__":
    unittest.main()
