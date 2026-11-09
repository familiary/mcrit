"""A sample whose SMDA report carries no statistics is stored, matched and exported like any other.

smda reads a missing or empty `statistics` as None - it writes {} itself for a report without a
disassembly - and SampleEntry keeps {}. addSmdaReport read `statistics["num_functions"]` only after it
had written the sample and its functions, so POST /samples answered 500 for a sample that was stored
anyway, every matching job that then found a match in it failed with KeyError: 'num_functions', and
so did every import of an export holding it.
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
from mcrit.matchers.MatcherQueryFunction import MatcherQueryFunction
from mcrit.storage.StorageFactory import StorageFactory

from .context import config, getTestMongoServerAndPort

logging.disable(logging.CRITICAL)

EXAMPLE_REPORT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "example_report.smda")


class SampleWithoutStatisticsTest(unittest.TestCase):
    def _mcritConfig(self):
        return deepcopy(config)

    def setUp(self):
        self.index = MinHashIndex(self._mcritConfig())
        self.storage = self.index.getStorage()
        self.storage.clearStorage()
        self.worker = self.index.queue._worker
        with open(EXAMPLE_REPORT) as handle:
            self.report_dict = json.load(handle)
        self.num_functions = len(self.report_dict["xcfg"])
        self.victim = self.storage.addSmdaReport(SmdaReport.fromDict(self.report_dict))

    def _addWithout(self, sha256, statistics):
        report_dict = dict(self.report_dict, sha256=sha256)
        report_dict["metadata"] = dict(report_dict["metadata"], family="no_statistics")
        if statistics is None:
            del report_dict["statistics"]
        else:
            report_dict["statistics"] = statistics
        return self.storage.addSmdaReport(SmdaReport.fromDict(report_dict))

    def _numFunctionsMatched(self, report):
        return {entry["sha256"]: entry["num_functions"] for entry in report["matches"]["samples"]}

    def test_a_sample_without_statistics_is_stored_counted_and_matched(self):
        # POST /query/function, answered in the web process
        function_query = SmdaReport.fromDict(dict(self.report_dict, sha256="55" * 32, xcfg={"364": self.report_dict["xcfg"]["364"]}))
        for sha256, statistics in (("22" * 32, {}), ("33" * 32, None)):
            with self.subTest(statistics=statistics):
                planted = self._addWithout(sha256, statistics)
                self.assertEqual({}, self.storage.getSampleById(planted.sample_id).statistics)
                reports = {
                    "sample": self.worker.getMatchesForSample(self.victim.sample_id),
                    "vs": self.worker.getMatchesForSampleVs(self.victim.sample_id, planted.sample_id),
                    "query": self.worker.getMatchesForSmdaReport(dict(self.report_dict, sha256="44" * 32)),
                    "function": MatcherQueryFunction(self.worker).getMatchesForSmdaFunction(function_query),
                }
                for kind, report in reports.items():
                    self.assertEqual(self.num_functions, self._numFunctionsMatched(report)[sha256], kind)
        family_entry = self.storage.getFamily(self.storage.getFamilyId("no_statistics"))
        self.assertEqual((2, 2 * self.num_functions), (family_entry.num_samples, family_entry.num_functions))

    def test_an_export_holding_such_a_sample_imports(self):
        self._addWithout("22" * 32, {})
        self._addWithout("33" * 32, dict(self.report_dict["statistics"], num_functions="ten"))
        export = json.loads(json.dumps(self.index.getExportData()))
        # an entry whose statistics are no dict, as only a written export carries them
        foreign = dict(export["sample_entries"]["22" * 32], sha256="66" * 32, statistics="num_functions: 10")
        export["sample_entries"]["66" * 32] = foreign
        export["function_entries"]["66" * 32] = export["function_entries"]["22" * 32]
        # into the instance holding the first three: skipped, as far as their entries state a count
        report = self.index.addImportData(deepcopy(export))
        self.assertEqual((1, 3, self.num_functions), (report["num_samples_imported"], report["num_samples_skipped"], report["num_functions_skipped"]))
        # into an emptied one, as a restore does
        self.storage.clearStorage()
        report = self.index.addImportData(deepcopy(export))
        self.assertEqual((4, 4 * self.num_functions), (report["num_samples_imported"], report["num_functions_imported"]))
        victim = self.storage.getSampleBySha256(self.report_dict["sha256"])
        matched = self._numFunctionsMatched(self.worker.getMatchesForSample(victim.sample_id))
        self.assertEqual({"22" * 32, "33" * 32, "66" * 32}, set(matched))
        self.assertEqual((self.num_functions, self.num_functions), (matched["22" * 32], matched["66" * 32]))
        self.assertEqual({}, self.storage.getSampleBySha256("66" * 32).statistics)
        self.assertEqual(4, self.index.addImportData(deepcopy(export))["num_samples_skipped"])


@pytest.mark.mongo
class MongoSampleWithoutStatisticsTest(SampleWithoutStatisticsTest):
    DB_NAME = "test_sample_without_statistics_mcrit"

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
