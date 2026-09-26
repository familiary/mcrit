"""What STORAGE_BAND_DF_CUTOFF skips, measured off the query path (#201).

The cutoff drops band hashes whose posting list is longer than it. Posting lists lengthen as the
corpus grows, so a fixed cutoff skips a growing share of the index without any error or log line
saying so; the coverage report is how an operator sees that happen.
"""

import json
import logging
import os
import unittest
from unittest import mock

import falcon
import falcon.testing
import pytest
from smda.common.SmdaReport import SmdaReport

from mcrit.client.McritClient import McritBadRequest, McritClient
from mcrit.config.McritConfig import McritConfig
from mcrit.config.MinHashConfig import MinHashConfig
from mcrit.config.QueueConfig import QueueConfig
from mcrit.config.ShinglerConfig import ShinglerConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.queue.QueueFactory import QueueFactory
from mcrit.server import application_routes
from mcrit.server.StatusResource import StatusResource
from mcrit.storage.MongoDbStorage import MongoDbStorage
from mcrit.storage.StorageFactory import StorageFactory
from mcrit.storage.StorageInterface import BAND_DF_CUTOFF_MAX, BAND_DF_REFERENCE_CUTOFFS

from .context import getTestMongoServerAndPort

THIS_FILE_PATH = str(os.path.abspath(__file__))
PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
REPORTS = ["example_report.smda", "example_report_2.smda", "example_report_3.smda", "library_report.smda"]
DB_NAME = "test_band_df_cutoff_coverage"

# posting lists per band, chosen so every reference cutoff and the small test cutoffs fall between
# lengths: 6 hashes and 372 postings in total
SYNTHETIC_BANDS = {
    0: {11: [1, 2, 3], 12: [4], 13: [5, 6, 7, 8, 9, 10]},
    1: {21: [1, 2]},
    2: {31: list(range(100, 400)), 32: list(range(500, 560))},
}


def buildConfig(storage_method=StorageFactory.STORAGE_METHOD_MEMORY, band_df_cutoff=0, bucket_size=0, db_suffix=""):
    server, port = getTestMongoServerAndPort()
    config = McritConfig()
    # server and database are only read by the MongoDB backend
    config.STORAGE_CONFIG = StorageConfig(
        STORAGE_METHOD=storage_method,
        STORAGE_SERVER=server,
        STORAGE_PORT=port,
        STORAGE_MONGODB_DBNAME=DB_NAME + db_suffix,
        STORAGE_BAND_DF_CUTOFF=band_df_cutoff,
        STORAGE_BAND_BUCKET_SIZE=bucket_size,
    )
    config.MINHASH_CONFIG = MinHashConfig()
    config.SHINGLER_CONFIG = ShinglerConfig()
    config.QUEUE_CONFIG = QueueConfig()
    config.QUEUE_CONFIG.QUEUE_MONGODB_DBNAME = DB_NAME + db_suffix + "_queue"
    # LocalQueue runs a job as it is submitted, so a scheduled job's result is readable right away
    config.QUEUE_CONFIG.QUEUE_METHOD = QueueFactory.QUEUE_METHOD_FAKE
    return config


def syntheticIndex(band_df_cutoff=0):
    index = MinHashIndex(config=buildConfig(band_df_cutoff=band_df_cutoff))
    storage = index._storage
    for band_number, postings in SYNTHETIC_BANDS.items():
        storage._bands[band_number] = {band_hash: list(function_ids) for band_hash, function_ids in postings.items()}
    return index


def loadReport(name):
    with open(os.sep.join([PROJECT_ROOT, "tests", name])) as handle:
        return SmdaReport.fromDict(json.load(handle))


def numbersOf(report):
    """Everything a coverage report measures, without the fields that describe the backend."""
    return {"totals": report["totals"], "bands": report["bands"], "at_reference_cutoffs": report["at_reference_cutoffs"]}


class BandDfCutoffCoverageTest(unittest.TestCase):
    def testConfiguredCutoffIsEvaluatedWhenNoneIsGiven(self):
        report = syntheticIndex(band_df_cutoff=2)._storage.getBandDfCutoffCoverage()
        self.assertTrue(report["available"])
        self.assertEqual(report["band_df_cutoff"], 2)
        self.assertEqual(report["band_df_cutoff_source"], "STORAGE_BAND_DF_CUTOFF")
        totals = report["totals"]
        self.assertEqual((totals["band_hashes"], totals["postings"]), (6, 372))
        # df 3, 6, 300 and 60 are over 2; df 1 and 2 are not - the cutoff keeps df == cutoff
        self.assertEqual((totals["band_hashes_over_cutoff"], totals["postings_over_cutoff"]), (4, 369))
        self.assertAlmostEqual(totals["postings_over_cutoff_fraction"], 369 / 372)
        self.assertAlmostEqual(totals["band_hashes_over_cutoff_fraction"], 4 / 6)
        self.assertEqual(totals["max_df"], 300)
        self.assertEqual(len(report["bands"]), report["num_bands"])
        band_0 = report["bands"][0]
        self.assertEqual(band_0["band_number"], 0)
        self.assertEqual((band_0["band_hashes"], band_0["postings"], band_0["band_hashes_over_cutoff"], band_0["postings_over_cutoff"]), (3, 10, 2, 9))
        self.assertEqual(report["bands"][5]["postings"], 0)
        self.assertEqual(report["bands"][5]["postings_over_cutoff_fraction"], 0.0)

    def testNegativeConfiguredCutoffReportsAsOff(self):
        """The lookup treats a cutoff <= 0 as off, so the report must not refuse a negative setting."""
        report = syntheticIndex(band_df_cutoff=-1)._storage.getBandDfCutoffCoverage()
        self.assertTrue(report["available"])
        self.assertEqual((report["band_df_cutoff"], report["configured_band_df_cutoff"]), (0, 0))
        self.assertEqual(report["totals"]["postings_over_cutoff"], 0)
        self.assertEqual(report["totals"]["postings"], 372)

    def testExplicitCutoffOverridesTheConfiguredOne(self):
        report = syntheticIndex(band_df_cutoff=2)._storage.getBandDfCutoffCoverage(band_df_cutoff=5)
        self.assertEqual(report["band_df_cutoff"], 5)
        self.assertEqual(report["band_df_cutoff_source"], "parameter")
        self.assertEqual(report["configured_band_df_cutoff"], 2)
        self.assertEqual((report["totals"]["band_hashes_over_cutoff"], report["totals"]["postings_over_cutoff"]), (3, 366))

    def testCutoffZeroSkipsNothingAndStillReportsTheDistribution(self):
        """Off is a cutoff that skips nothing; the reference cutoffs say what one would skip."""
        for report in (syntheticIndex()._storage.getBandDfCutoffCoverage(), syntheticIndex(band_df_cutoff=2)._storage.getBandDfCutoffCoverage(band_df_cutoff=0)):
            self.assertTrue(report["available"])
            self.assertEqual(report["band_df_cutoff"], 0)
            self.assertEqual((report["totals"]["band_hashes_over_cutoff"], report["totals"]["postings_over_cutoff"]), (0, 0))
            self.assertEqual(report["totals"]["postings_over_cutoff_fraction"], 0.0)
            self.assertIn("off", report["message"])
            self.assertEqual([entry["band_df_cutoff"] for entry in report["at_reference_cutoffs"]], list(BAND_DF_REFERENCE_CUTOFFS))
            at_reference = {entry["band_df_cutoff"]: (entry["band_hashes_over_cutoff"], entry["postings_over_cutoff"]) for entry in report["at_reference_cutoffs"]}
            self.assertEqual(at_reference, {50: (2, 360), 100: (1, 300), 200: (1, 300), 500: (0, 0), 1000: (0, 0)})

    def testEmptyIndexReportsZeroesRatherThanFailing(self):
        report = MinHashIndex(config=buildConfig(band_df_cutoff=2))._storage.getBandDfCutoffCoverage()
        self.assertTrue(report["available"])
        self.assertEqual(report["totals"]["postings"], 0)
        self.assertEqual(report["totals"]["postings_over_cutoff_fraction"], 0.0)

    def testInvalidCutoffIsRefused(self):
        storage = syntheticIndex()._storage
        for cutoff in (-1, 1.5, True, "200", BAND_DF_CUTOFF_MAX + 1):
            with self.subTest(cutoff=cutoff):
                with self.assertRaises(ValueError):
                    storage.getBandDfCutoffCoverage(band_df_cutoff=cutoff)

    def testLargestBsonIntegerIsAccepted(self):
        report = syntheticIndex()._storage.getBandDfCutoffCoverage(band_df_cutoff=BAND_DF_CUTOFF_MAX)
        self.assertEqual(report["band_df_cutoff"], 2**63 - 1)
        self.assertEqual(report["totals"]["postings_over_cutoff"], 0)

    def testReportCarriesNoPerHashDiagnostics(self):
        """band_hashes_without_df is gone: the (band_hash, df) index cannot tell a healthy bucket above 0
        from one whose bucket 0 is missing, so only a per-hash group could count it."""
        report = syntheticIndex()._storage.getBandDfCutoffCoverage(band_df_cutoff=2)
        self.assertNotIn("band_hashes_without_df", report["totals"])
        self.assertNotIn("band_hashes_without_df", report["bands"][0])

    def testHeadlineIsLoggedAtInfo(self):
        storage = syntheticIndex()._storage
        logging.disable(logging.NOTSET)
        try:
            with self.assertLogs("mcrit.storage.StorageInterface", level="INFO") as captured:
                report = storage.getBandDfCutoffCoverage(band_df_cutoff=2)
        finally:
            logging.disable(logging.CRITICAL)
        self.assertIn(report["message"], captured.output[-1])
        self.assertIn("Band df cutoff 2 skips 99.19% of band postings (369 of 372)", report["message"])

    def testMemoryStorageSaysItDoesNotApplyTheCutoff(self):
        report = syntheticIndex()._storage.getBandDfCutoffCoverage(band_df_cutoff=2)
        self.assertFalse(report["backend_applies_cutoff"])
        self.assertIn("does not apply the cutoff", report["message"])

    def testProgressIsReportedPerBand(self):
        storage = syntheticIndex()._storage
        reporter = mock.MagicMock()
        storage.getBandDfCutoffCoverage(progress_reporter=reporter)
        reporter.set_total.assert_called_once_with(storage._storage_config.STORAGE_NUM_BANDS)
        self.assertEqual(reporter.step.call_count, storage._storage_config.STORAGE_NUM_BANDS)

    def testRunsAsAJob(self):
        """The index schedules the measurement as a job whose result is the report."""
        index = syntheticIndex(band_df_cutoff=2)
        job_id = index.getBandDfCutoffCoverage(band_df_cutoff=5, force_recalculation=True)
        self.assertEqual(index.getJobData(job_id)["payload"]["method"], "getBandDfCutoffCoverage")
        result = index.getResultForJob(job_id)
        self.assertEqual(result, index._storage.getBandDfCutoffCoverage(band_df_cutoff=5))
        self.assertEqual(result["band_df_cutoff"], 5)


class BandDfCutoffCoverageResourceTest(unittest.TestCase):
    @staticmethod
    def _call(query_string):
        index = mock.MagicMock()
        index.getBandDfCutoffCoverage.return_value = "0123456789abcdef01234567"
        resp = falcon.Response()
        req = falcon.Request(falcon.testing.create_environ(path="/band_df_cutoff_coverage", query_string=query_string))
        StatusResource(index).on_get_band_df_cutoff_coverage(req, resp)
        assert resp.data is not None
        return index, resp, json.loads(resp.data)

    def testSchedulesAFreshJob(self):
        for query_string, expected in (("", None), ("band_df_cutoff=0", 0), ("band_df_cutoff=200", 200)):
            with self.subTest(query_string=query_string):
                index, resp, payload = self._call(query_string)
                self.assertEqual(resp.status, falcon.HTTP_200)
                self.assertEqual(payload, {"status": "successful", "data": "0123456789abcdef01234567"})
                index.getBandDfCutoffCoverage.assert_called_once_with(band_df_cutoff=expected, force_recalculation=True, username=None)

    def testLargestBsonIntegerIsAccepted(self):
        index, resp, _ = self._call("band_df_cutoff=%d" % (2**63 - 1))
        self.assertEqual(resp.status, falcon.HTTP_200)
        index.getBandDfCutoffCoverage.assert_called_once_with(band_df_cutoff=2**63 - 1, force_recalculation=True, username=None)

    def testMalformedCutoffIsABadRequest(self):
        """Refused, not ignored: measuring the configured cutoff instead would answer another question.

        2**63 is refused here rather than failing the job: no BSON integer can carry it.
        """
        for value in ("-1", "abc", "1.5", "", str(2**63), str(10**30)):
            with self.subTest(value=value):
                index, resp, payload = self._call("band_df_cutoff=" + value)
                self.assertEqual(resp.status, falcon.HTTP_400)
                self.assertEqual(payload["status"], "failed")
                self.assertIn("band_df_cutoff", payload["data"]["message"])
                index.getBandDfCutoffCoverage.assert_not_called()


class BandDfCutoffCoverageEndToEndTest(unittest.TestCase):
    """Client -> REST route -> index -> job -> storage, on a real falcon app."""

    def setUp(self):
        self.index = syntheticIndex(band_df_cutoff=2)
        with mock.patch.object(application_routes, "create_index", return_value=self.index), mock.patch.object(McritConfig, "AUTH_TOKEN", ""):
            self.app = falcon.testing.TestClient(application_routes.get_app())

    def _forward(self, url, headers=None, params=None, timeout=None):
        # every McritClient request carries a timeout (#216); None would wait on a hung server forever
        self.assertIsNotNone(timeout)
        path = url.replace("http://mcrit.test", "")
        result = self.app.simulate_get(path, params=params, headers=headers)
        response = mock.MagicMock(status_code=result.status_code, url=url, text=result.text)
        response.json.return_value = result.json
        return response

    def testClientRequestsTheCoverageJob(self):
        client = McritClient("http://mcrit.test")
        with mock.patch("mcrit.client.McritClient.requests.get", side_effect=self._forward):
            configured_job = client.requestBandDfCutoffCoverage()
            explicit_job = client.requestBandDfCutoffCoverage(band_df_cutoff=5)
        configured = self.index.getResultForJob(configured_job)
        explicit = self.index.getResultForJob(explicit_job)
        self.assertEqual((configured["band_df_cutoff"], configured["band_df_cutoff_source"]), (2, "STORAGE_BAND_DF_CUTOFF"))
        self.assertEqual(configured["totals"]["postings_over_cutoff"], 369)
        self.assertEqual((explicit["band_df_cutoff"], explicit["band_df_cutoff_source"]), (5, "parameter"))
        self.assertEqual(explicit["totals"]["postings_over_cutoff"], 366)

    def testClientErrorModes(self):
        with mock.patch("mcrit.client.McritClient.requests.get", side_effect=self._forward):
            self.assertIsNone(McritClient("http://mcrit.test").requestBandDfCutoffCoverage(band_df_cutoff=-1))
            with self.assertRaises(McritBadRequest):
                McritClient("http://mcrit.test", raise_client_errors=True).requestBandDfCutoffCoverage(band_df_cutoff=-1)
            raw = McritClient("http://mcrit.test", raw_responses=True).requestBandDfCutoffCoverage(band_df_cutoff=-1)
        self.assertEqual(raw.status_code, 400)


class MongoBandDfCountPipelineTest(unittest.TestCase):
    """The shape of the MongoDB count, checked without a database."""

    def testOneGroupOverDfAlone(self):
        """One running total per band, not one group entry per band hash: no allowDiskUse needed."""
        pipeline = MongoDbStorage._bandDfCountPipeline([1, *BAND_DF_REFERENCE_CUTOFFS])
        self.assertEqual(len(pipeline), 1)
        self.assertEqual(list(pipeline[0]), ["$group"])
        self.assertIsNone(pipeline[0]["$group"]["_id"])
        referenced = json.dumps(pipeline)
        self.assertIn('"$df"', referenced)
        self.assertNotIn("$band_hash", referenced)

    def testAggregateIsHintedAndDoesNotSpillToDisk(self):
        storage = MongoDbStorage(buildConfig(StorageFactory.STORAGE_METHOD_MONGODB))
        database = mock.MagicMock()
        collection = database.__getitem__.return_value
        collection.index_information.return_value = {"_id_": {"key": [("_id", 1)]}, "band_hash_1_df_1": {"key": [("band_hash", 1), ("df", 1)]}}
        collection.aggregate.return_value = iter([{"_id": None, "band_hashes": 4, "postings": 369, "max_df": 300, "hashes_over_2": 4, "postings_over_2": 369}])
        with mock.patch.object(storage, "_getDb", return_value=database):
            counts = storage._countBandDf(0, [2])
        self.assertEqual(counts, {"band_hashes": 4, "postings": 369, "max_df": 300, "over": {2: [4, 369]}})
        (pipeline,), kwargs = collection.aggregate.call_args
        self.assertEqual(pipeline, MongoDbStorage._bandDfCountPipeline([2]))
        self.assertEqual(kwargs, {"hint": "band_hash_1_df_1"})


@pytest.mark.mongo
class MongoBandDfCutoffCoverageTest(unittest.TestCase):
    """The MongoDB count comes from the (band_hash, df) index alone and must agree with the posting lists."""

    @classmethod
    def setUpClass(cls):
        cls.memory = MinHashIndex(config=buildConfig())
        cls.mongo = MinHashIndex(config=buildConfig(StorageFactory.STORAGE_METHOD_MONGODB))
        cls.mongo._storage.clearStorage()
        for index in (cls.memory, cls.mongo):
            for name in REPORTS:
                entry = index._storage.addSmdaReport(loadReport(name))
                if entry is not None:
                    index.queue._worker.updateMinHashesForSample(entry.sample_id)

    @classmethod
    def tearDownClass(cls):
        client = cls.mongo._storage._getDb().client
        for suffix in ("", "_bucketed", "_legacy"):
            client.drop_database(DB_NAME + suffix)
            client.drop_database(DB_NAME + suffix + "_queue")

    def _freshMongo(self, db_suffix, bucket_size=0, band_df_cutoff=0):
        index = MinHashIndex(config=buildConfig(StorageFactory.STORAGE_METHOD_MONGODB, band_df_cutoff=band_df_cutoff, bucket_size=bucket_size, db_suffix=db_suffix))
        index._storage.clearStorage()
        return index

    def testFixtureCorpusHasPostingListsOverTheCutoff(self):
        """Otherwise every comparison below would hold for a report of zeroes."""
        report = self.memory._storage.getBandDfCutoffCoverage(band_df_cutoff=1)
        self.assertGreater(report["totals"]["band_hashes_over_cutoff"], 0)
        self.assertGreater(report["totals"]["postings_over_cutoff"], report["totals"]["band_hashes_over_cutoff"])
        self.assertLess(report["totals"]["band_hashes_over_cutoff"], report["totals"]["band_hashes"])

    def testBothBackendsReportTheSameNumbers(self):
        for cutoff in (1, 2, 0):
            with self.subTest(cutoff=cutoff):
                memory_report = self.memory._storage.getBandDfCutoffCoverage(band_df_cutoff=cutoff)
                mongo_report = self.mongo._storage.getBandDfCutoffCoverage(band_df_cutoff=cutoff)
                self.assertTrue(mongo_report["available"], mongo_report["message"])
                self.assertEqual(numbersOf(mongo_report), numbersOf(memory_report))
                self.assertTrue(mongo_report["backend_applies_cutoff"])

    def testConfiguredCutoffIsReadFromTheMongoConfig(self):
        configured = MinHashIndex(config=buildConfig(StorageFactory.STORAGE_METHOD_MONGODB, band_df_cutoff=1))._storage.getBandDfCutoffCoverage()
        self.assertEqual((configured["band_df_cutoff"], configured["band_df_cutoff_source"]), (1, "STORAGE_BAND_DF_CUTOFF"))
        self.assertEqual(numbersOf(configured), numbersOf(self.memory._storage.getBandDfCutoffCoverage(band_df_cutoff=1)))

    def testBucketingGivesTheSameNumbers(self):
        """Only bucket 0 carries df under bucketing; a spilled hash must still count once, with its total.

        The bucketed collections are written through the same _updateBands the indexer uses, from
        the fixture corpus' own posting lists, at a bucket size of 1 so every hash held by more
        than one function spills.
        """
        bucketed = self._freshMongo("_bucketed", bucket_size=1, band_df_cutoff=1)
        bucketed._storage._updateBands({band_number: dict(postings) for band_number, postings in self.memory._storage._bands.items()})
        spilled = bucketed._storage._getDb()["band_0"].count_documents({"bucket": {"$gt": 0}})
        self.assertGreater(spilled, 0, "the fixture must actually spill at a bucket size of 1")
        for cutoff in (None, 0, 2):
            with self.subTest(cutoff=cutoff):
                bucketed_report = bucketed._storage.getBandDfCutoffCoverage(band_df_cutoff=cutoff)
                reference = self.mongo._storage.getBandDfCutoffCoverage(band_df_cutoff=1 if cutoff is None else cutoff)
                self.assertTrue(bucketed_report["available"], bucketed_report["message"])
                self.assertEqual(numbersOf(bucketed_report), numbersOf(reference))

    def testIncompleteDfIndexIsRefusedNotGuessed(self):
        """A database from before df existed must not report df-less posting lists as empty."""
        legacy = self._freshMongo("_legacy")
        storage = legacy._storage
        storage._updateBands({0: {7: [1, 2, 3]}, 1: {8: [4]}})
        storage._getDb()["band_0"].update_many({}, {"$unset": {"df": ""}})
        storage._setBandDfIndexComplete(False)
        report = storage.getBandDfCutoffCoverage(band_df_cutoff=1)
        self.assertFalse(report["available"])
        self.assertIsNone(report["totals"])
        self.assertEqual(report["bands"], [])
        self.assertIn("rebuild_band_df_index", report["message"])
        # the rebuild is the remedy the message names, and afterwards the numbers are there
        storage.rebuildBandDfIndex()
        report = storage.getBandDfCutoffCoverage(band_df_cutoff=1)
        self.assertTrue(report["available"])
        self.assertEqual((report["totals"]["postings"], report["totals"]["postings_over_cutoff"]), (4, 3))

    def testMissingDfIndexIsRefused(self):
        legacy = self._freshMongo("_legacy")
        storage = legacy._storage
        storage._updateBands({0: {7: [1, 2, 3]}})
        storage._getDb()["band_3"].drop_index([("band_hash", 1), ("df", 1)])
        report = storage.getBandDfCutoffCoverage(band_df_cutoff=1)
        self.assertFalse(report["available"])
        self.assertIn("[3]", report["message"])
        self.assertIsNone(report["totals"])

    def testSpilledHashWithAnEmptiedBucketZeroCountsOnce(self):
        """A pull can empty bucket 0 while it keeps the hash's df for the buckets above it."""
        bucketed = self._freshMongo("_bucketed", bucket_size=2)
        storage = bucketed._storage
        storage._updateBands({0: {7: [1, 2, 3, 4, 5]}, 1: {8: [6]}})
        storage._updateBands({0: {7: [1, 2]}}, method="pull")
        band_0 = storage._getDb()["band_0"]
        self.assertEqual(band_0.find_one({"band_hash": 7, "bucket": 0})["function_ids"], [])
        self.assertGreater(band_0.count_documents({"band_hash": 7}), 1)
        totals = storage.getBandDfCutoffCoverage(band_df_cutoff=2)["totals"]
        self.assertEqual((totals["band_hashes"], totals["postings"], totals["max_df"]), (2, 4, 3))
        self.assertEqual((totals["band_hashes_over_cutoff"], totals["postings_over_cutoff"]), (1, 3))

    def testHashMissingBucketZeroIsCountedOnceTheRebuildRepairsIt(self):
        """Bucket 0 holds a hash's only df: without it the postings above are neither counted nor served."""
        bucketed = self._freshMongo("_bucketed", bucket_size=2)
        storage = bucketed._storage
        storage._updateBands({0: {7: [1, 2, 3], 9: [4, 5, 6, 10, 11]}})
        band_0 = storage._getDb()["band_0"]
        band_0.delete_one({"band_hash": 9, "bucket": 0})
        totals = storage.getBandDfCutoffCoverage(band_df_cutoff=1)["totals"]
        self.assertEqual((totals["band_hashes"], totals["postings"]), (1, 3))
        storage.rebuildBandDfIndex()
        repaired = band_0.find_one({"band_hash": 9, "bucket": 0})
        self.assertIsNotNone(repaired, "the rebuild must recreate the missing bucket 0")
        self.assertEqual((repaired["df"], repaired["tail"], repaired["tail_n"]), (3, 2, 1))
        totals = storage.getBandDfCutoffCoverage(band_df_cutoff=1)["totals"]
        self.assertEqual((totals["band_hashes"], totals["postings"], totals["max_df"]), (2, 6, 3))
        self.assertEqual((totals["band_hashes_over_cutoff"], totals["postings_over_cutoff"]), (2, 6))
        # the recreated bucket 0 is returned by a lookup too, which reads function_ids off every
        # document; a query function whose only band hash is 9 still finds the surviving postings
        query = mock.Mock(hasMinHash=mock.Mock(return_value=True))
        with mock.patch.object(storage, "getBandHashesForMinHash", return_value={0: 9}):
            for accumulation in ("dict", "numpy"):
                with self.subTest(accumulation=accumulation), mock.patch.object(storage._storage_config, "STORAGE_CANDIDATE_ACCUMULATION", accumulation):
                    candidates = storage.getCandidatesForMinHashes({42: query})
                    self.assertEqual({key: set(value) for key, value in candidates.items()}, {42: {6, 10, 11}})
        # the recompute after a pull recreates a missing bucket 0 the same way
        band_0.delete_one({"band_hash": 9, "bucket": 0})
        storage._updateBands({0: {9: [6]}}, method="pull")
        recomputed = band_0.find_one({"band_hash": 9, "bucket": 0})
        self.assertEqual((recomputed["function_ids"], recomputed["df"]), ([], 2))

    def testAHashWithoutPostingsGetsNoBucketZero(self):
        """Only surviving postings recreate a bucket 0; an emptied hash must not leave a df-0 document behind."""
        bucketed = self._freshMongo("_bucketed", bucket_size=2)
        storage = bucketed._storage
        band_0 = storage._getDb()["band_0"]
        band_0.insert_one({"band_hash": 9, "bucket": 1, "function_ids": []})
        storage.rebuildBandDfIndex()
        self.assertIsNone(band_0.find_one({"band_hash": 9, "bucket": 0}))
        storage._recomputeBandBookkeeping(band_0, [9])
        self.assertIsNone(band_0.find_one({"band_hash": 9, "bucket": 0}))

    def testLargestBsonIntegerIsAccepted(self):
        report = self.mongo._storage.getBandDfCutoffCoverage(band_df_cutoff=BAND_DF_CUTOFF_MAX)
        self.assertTrue(report["available"], report["message"])
        self.assertEqual(report["totals"]["postings_over_cutoff"], 0)
        self.assertGreater(report["totals"]["postings"], 0)

    def testCountIsCoveredByTheDfIndex(self):
        """No band document is fetched: the plan reads band_hash and df from the index alone."""
        storage = self.mongo._storage
        pipeline = storage._bandDfCountPipeline([1, *BAND_DF_REFERENCE_CUTOFFS])
        self.assertEqual(storage._bandDfIndexName(0), "band_hash_1_df_1")
        explain = storage._getDb().command("explain", {"aggregate": "band_0", "pipeline": pipeline, "cursor": {}, "hint": "band_hash_1_df_1"}, verbosity="queryPlanner")
        plan = json.dumps(explain, default=str)
        self.assertIn("IXSCAN", plan)
        self.assertNotIn("FETCH", plan)
        self.assertNotIn("COLLSCAN", plan)


if __name__ == "__main__":
    unittest.main()
