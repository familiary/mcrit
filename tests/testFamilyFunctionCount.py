"""A family's function counter counts the functions stored, not what a report's statistics claim.

addSmdaReport and importSampleEntry incremented the family by `statistics.num_functions`, which smda
copies from a submitted report unchecked and an export states as it was written. A claim of 2^63 - 1
minus a family's counter saturated it on MongoDB, and every later report into that family then failed
after its sample and functions were written, neither counted nor minhashed.
"""

import json
import logging
import os
import unittest
from copy import deepcopy

import pymongo
import pytest
from smda.common.SmdaReport import SmdaReport

from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.storage.StorageFactory import StorageFactory

from .context import config, getTestMongoServerAndPort

logging.disable(logging.CRITICAL)

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
INT64_MAX = 2**63 - 1


def loadReportDict(name, sha256, family, num_functions=None):
    with open(os.path.join(TESTS_DIR, name)) as handle:
        report_dict = json.load(handle)
    report_dict["sha256"] = sha256
    report_dict["metadata"] = dict(report_dict["metadata"], family=family)
    if num_functions is not None:
        report_dict["statistics"] = dict(report_dict["statistics"], num_functions=num_functions)
    return report_dict


class FamilyFunctionCountTest(unittest.TestCase):
    def _mcritConfig(self):
        return deepcopy(config)

    def setUp(self):
        self.index = MinHashIndex(self._mcritConfig())
        self.storage = self.index.getStorage()
        self.storage.clearStorage()

    def _addReport(self, *args, **kwargs):
        return self.storage.addSmdaReport(SmdaReport.fromDict(loadReportDict(*args, **kwargs)))

    def _counters(self, family_name):
        family_entry = self.storage.getFamily(self.storage.getFamilyId(family_name))
        return family_entry.num_samples, family_entry.num_functions

    def test_a_report_counts_the_functions_it_holds(self):
        # example_report_3 claims 9 functions and holds 1
        honest = self._addReport("example_report_3.smda", "11" * 32, "emotet")
        self.assertEqual((1, 1), self._counters("emotet"))
        claimed = self._addReport("example_report.smda", "22" * 32, "emotet", num_functions=INT64_MAX - 1)
        self.assertEqual((2, 11), self._counters("emotet"))
        # the next report into the family is stored and counted as well
        self.assertIsNotNone(self._addReport("example_report_2.smda", "33" * 32, "emotet"))
        self.assertEqual((3, 20), self._counters("emotet"))
        # moving and deleting a sample take out what was counted for it
        self.storage.modifySample(claimed.sample_id, {"family_name": "other"})
        self.assertEqual(((2, 10), (1, 10)), (self._counters("emotet"), self._counters("other")))
        self.storage.deleteSample(honest.sample_id)
        self.assertEqual((1, 9), self._counters("emotet"))
        self.assertEqual(0, self.storage.recomputeFamilyStats()["num_families_corrected"])

    def test_an_import_counts_the_functions_it_imports(self):
        victim = self._addReport("example_report.smda", "11" * 32, "emotet")
        claims = [INT64_MAX - 10, -(2**63), 9.3e18, "10", None]
        for index, claim in enumerate(claims):
            export = json.loads(json.dumps(self.index.getExportData([victim.sample_id])))
            entry = export["sample_entries"].pop("11" * 32)
            sha256 = "%02x" % (0x20 + index) * 32
            entry.update(sha256=sha256, statistics=dict(entry["statistics"], num_functions=claim))
            export["sample_entries"] = {sha256: entry}
            export["function_entries"] = {sha256: export["function_entries"].pop("11" * 32)}
            self.assertEqual(1, self.index.addImportData(export)["num_samples_imported"], claim)
        self.assertEqual((1 + len(claims), 10 * (1 + len(claims))), self._counters("emotet"))
        # the next report into the family is stored and counted as well
        self.assertIsNotNone(self._addReport("example_report_2.smda", "99" * 32, "emotet"))
        self.assertEqual((2 + len(claims), 10 * (1 + len(claims)) + 9), self._counters("emotet"))
        self.assertEqual(0, self.storage.recomputeFamilyStats()["num_families_corrected"])


@pytest.mark.mongo
class MongoFamilyFunctionCountTest(FamilyFunctionCountTest):
    DB_NAME = "test_family_function_count_mcrit"

    def _mcritConfig(self):
        server, port = getTestMongoServerAndPort()
        mcrit_config = deepcopy(config)
        mcrit_config.STORAGE_CONFIG = StorageConfig(
            STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB, STORAGE_SERVER=server, STORAGE_PORT=port, STORAGE_MONGODB_DBNAME=self.DB_NAME
        )
        return mcrit_config

    def tearDown(self):
        server, port = getTestMongoServerAndPort()
        client = pymongo.MongoClient(server, int(port))
        client.drop_database(self.DB_NAME)
        client.close()


if __name__ == "__main__":
    unittest.main()
