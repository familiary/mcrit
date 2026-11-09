"""What the band df cutoff and the shortlist left out of one matching job, in its report (#201).

The coverage report measures what a cutoff skips of the whole index, as a job of its own. These
counts are per query: each matching report says how many of its band lookups the cutoff skipped,
so a query the cutoff has started to degrade says so in its own result.
"""

import json
import logging
import os
import random
import unittest
from collections import Counter
from unittest import mock

import pytest
from smda.common.SmdaReport import SmdaReport

from mcrit.config.McritConfig import McritConfig
from mcrit.config.MinHashConfig import MinHashConfig
from mcrit.config.QueueConfig import QueueConfig
from mcrit.config.ShinglerConfig import ShinglerConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.minhash.MinHash import MinHash
from mcrit.queue.QueueFactory import QueueFactory
from mcrit.queue.QueueRemoteCalls import UncacheableResult
from mcrit.storage.StorageFactory import StorageFactory
from mcrit.storage.StorageInterface import BandDfSkips

from .context import getTestMongoServerAndPort

THIS_FILE_PATH = str(os.path.abspath(__file__))
PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
# on these three reports a shortlist of 1 drops one of the two samples sample 0 matches
REPORTS = ("example_report.smda", "example_report_2.smda", "example_report_3.smda")
# per process, so concurrent runs against one test server never share a database
DB_NAME = "test_band_df_skips_%d" % os.getpid()


def loadReport(name):
    with open(os.sep.join([PROJECT_ROOT, "tests", name])) as handle:
        return SmdaReport.fromDict(json.load(handle))


class MemoryBandDfSkipsTest(unittest.TestCase):
    STORAGE_METHOD = StorageFactory.STORAGE_METHOD_MEMORY

    def _config(self, db_name=DB_NAME, bucket_size=0):
        server, port = getTestMongoServerAndPort()
        config = McritConfig()
        # new objects, not edits: McritConfig holds its sub-configs as class attributes
        config.STORAGE_CONFIG = StorageConfig(
            STORAGE_METHOD=self.STORAGE_METHOD, STORAGE_SERVER=server, STORAGE_PORT=port, STORAGE_MONGODB_DBNAME=db_name, STORAGE_BAND_BUCKET_SIZE=bucket_size
        )
        config.MINHASH_CONFIG = MinHashConfig(MINHASH_POOL_INDEXING=False, MINHASH_POOL_MATCHING=False)
        config.SHINGLER_CONFIG = ShinglerConfig()
        config.QUEUE_CONFIG = QueueConfig(QUEUE_METHOD=QueueFactory.QUEUE_METHOD_FAKE, QUEUE_MONGODB_DBNAME=db_name + "_queue")
        return config

    def setUp(self):
        self._build()

    def _build(self, **config_options):
        self.index = MinHashIndex(config=self._config(**config_options))
        self.storage = self.index._storage
        self.storage.clearStorage()
        self.worker = self.index.queue._worker
        self.sample_ids = []
        for name in REPORTS:
            entry = self.storage.addSmdaReport(loadReport(name))
            self.worker.updateMinHashesForSample(entry.sample_id)
            self.sample_ids.append(entry.sample_id)
        self.sample_id = self.sample_ids[0]
        bits = self.index.config.MINHASH_CONFIG.MINHASH_SIGNATURE_BITS
        self.minhashes = {entry.function_id: entry.getMinHash(minhash_bits=bits) for entry in self.storage.getFunctionsBySampleId(self.sample_id) if entry.minhash}

    def tearDown(self):
        self.storage.clearStorage()

    def _addCopyOfTheQuerySample(self, copy_number=1):
        """Another sample holding the query's functions: every band hash of the query then has a df
        of at least 2, so a cutoff of 1 leaves its functions without any band."""
        report = loadReport(REPORTS[0])
        report.sha256 = "%064x" % (200 + copy_number)
        entry = self.storage.addSmdaReport(report)
        self.worker.updateMinHashesForSample(entry.sample_id)
        self.sample_ids.append(entry.sample_id)

    def _df(self):
        """(band, band hash) -> how many corpus functions hold it, counted from the functions themselves."""
        df = Counter()
        bits = self.index.config.MINHASH_CONFIG.MINHASH_SIGNATURE_BITS
        for sample_id in self.sample_ids:
            for entry in self.storage.getFunctionsBySampleId(sample_id):
                if entry.minhash:
                    df.update(self.storage.getBandHashesForMinHash(entry.getMinHash(minhash_bits=bits)).items())
        return df

    def _expected(self, cutoff, band_matches_required=1):
        df = self._df()
        lookups = skipped = postings = 0
        affected = cut_off = 0
        for minhash in self.minhashes.values():
            band_hashes = self.storage.getBandHashesForMinHash(minhash).items()
            over = [df[key] for key in band_hashes if df[key] > cutoff]
            lookups += len(band_hashes)
            skipped += len(over)
            postings += sum(over)
            affected += bool(over)
            # fewer bands left than a candidate needs hits: no candidate can reach it
            # a requirement above the band count could not be met with every band left either
            cut_off += bool(over) and band_matches_required <= len(band_hashes) and len(band_hashes) - len(over) < band_matches_required
        return {
            "band_lookups": lookups,
            "band_lookups_skipped": skipped,
            "postings_skipped": postings,
            "query_functions_with_skipped_bands": affected,
            "query_functions_cut_off": cut_off,
        }

    def _counted(self, cutoff, lookup="getCandidatesForMinHashes", band_matches_required=1):
        skips = BandDfSkips()
        getattr(self.storage, lookup)(self.minhashes, band_matches_required=band_matches_required, band_df_cutoff=cutoff, skips=skips)
        return {key: value for key, value in skips.toDict(band_matches_required).items() if key in self._expected(0)}

    def _plantPostings(self, band_number, band_hash, function_ids):
        """Give one band hash exactly this posting list, whatever the backend."""
        if self.STORAGE_METHOD == StorageFactory.STORAGE_METHOD_MEMORY:
            self.storage._bands[band_number][band_hash] = list(function_ids)
        else:
            self.storage._updateBands({band_number: {band_hash: list(function_ids)}}, method="push")

    def _candidateGeneration(self, **knobs):
        return self.worker.getMatchesForSample(self.sample_id, **knobs)["info"]["matching"]["candidate_generation"]

    def testTheLookupCountsWhatTheCutoffSkips(self):
        expected = self._expected(1)
        self.assertGreater(expected["band_lookups_skipped"], 0, "the fixture must hold band hashes over the cutoff")
        self.assertEqual(expected, self._counted(1))
        self._addCopyOfTheQuerySample()
        expected = self._expected(1)
        self.assertGreater(expected["query_functions_cut_off"], 0, "the copy puts every band of the query over the cutoff")
        self.assertEqual(expected, self._counted(1))

    def testTheCutOffCountFollowsBandMatchesRequired(self):
        for band_matches_required in (1, 2):
            expected = self._expected(1, band_matches_required)
            self.assertEqual(expected, self._counted(1, band_matches_required=band_matches_required), band_matches_required)
            info = self._candidateGeneration(band_df_cutoff=1, band_matches_required=band_matches_required)
            self.assertEqual(expected, {key: info[key] for key in expected}, band_matches_required)
            self.assertEqual(expected["query_functions_cut_off"] > 0, info["degraded"], band_matches_required)

    def testOneBandLeftIsCutOffWhenTwoAreRequired(self):
        """19 of 20 bands over the cutoff: findable with one band hit required, not with two (the default)."""
        rng = random.Random(201)
        signature_length = self.index.config.MINHASH_CONFIG.MINHASH_SIGNATURE_LENGTH
        query = MinHash(minhash_signature=[rng.randrange(2**32) for _ in range(signature_length)])
        band_hashes = self.storage.getBandHashesForMinHash(query)
        first, *others = sorted(band_hashes)
        self._plantPostings(first, band_hashes[first], [10**6 + 1])
        for band_number in others:
            self._plantPostings(band_number, band_hashes[band_number], [10**6 + 1, 10**6 + 2, 10**6 + 3])
        for band_matches_required, candidates, cut_off in ((1, {10**6 + 1}, 0), (2, set(), 1)):
            skips = BandDfSkips()
            found = self.storage.getCandidatesForMinHashes({7: query}, band_matches_required=band_matches_required, band_df_cutoff=2, skips=skips)
            self.assertEqual(candidates, set(found.get(7, ())), band_matches_required)
            self.assertEqual(len(others), skips.band_lookups_skipped)
            self.assertEqual(cut_off, skips.toDict(band_matches_required)["query_functions_cut_off"], band_matches_required)
        # without the cutoff, two band hits are there to be had: it is the cutoff that lost them
        self.assertIn(10**6 + 1, self.storage.getCandidatesForMinHashes({7: query}, band_matches_required=2, band_df_cutoff=0)[7])

    def testAStorageThatPredatesTheCountStillMatches(self):
        lookup = self.storage.getCandidatesForMinHashes
        calls = []

        def predating(function_id_to_minhash, band_matches_required=1, band_df_cutoff=None):
            calls.append(len(function_id_to_minhash))
            return lookup(function_id_to_minhash, band_matches_required=band_matches_required, band_df_cutoff=band_df_cutoff)

        expected = self.worker.getMatchesForSample(self.sample_id, band_df_cutoff=1)["matches"]
        with mock.patch.object(self.storage, "getCandidatesForMinHashes", new=predating):
            report = self.worker.getMatchesForSample(self.sample_id, band_df_cutoff=1)
        info = report["info"]["matching"]["candidate_generation"]
        self.assertEqual((False, "storage_unsupported"), (info["band_df_skips_measured"], info["band_df_skips_unmeasured_reason"]))
        # nothing was counted, not even the lookups
        self.assertEqual((None, None, None), (info["band_lookups"], info["band_lookups_skipped"], info["query_functions_cut_off"]))
        self.assertEqual(expected, report["matches"])
        # decided from the signature: the lookup ran once, not once failing and once again
        self.assertEqual(1, len(calls))
        self.assertNotIsInstance(report, UncacheableResult)

    def testAStorageTakingKeywordsIsCounted(self):
        lookup = self.storage.getCandidatesForMinHashes

        def forwarding(function_id_to_minhash, **kwargs):
            return lookup(function_id_to_minhash, **kwargs)

        with mock.patch.object(self.storage, "getCandidatesForMinHashes", new=forwarding):
            info = self._candidateGeneration(band_df_cutoff=1, band_matches_required=1)
        expected = self._expected(1)
        self.assertTrue(info["band_df_skips_measured"])
        self.assertEqual(expected, {key: info[key] for key in expected})

    def testAnErrorInsideTheCountFailsTheJob(self):
        """A TypeError raised while counting is a bug to surface, not a storage to work around."""
        with mock.patch.object(BandDfSkips, "addSkipped", side_effect=TypeError("bug in the count")):
            with self.assertRaisesRegex(TypeError, "bug in the count"):
                self.worker.getMatchesForSample(self.sample_id, band_df_cutoff=1)

    def testARequirementAboveTheBandCountIsNotBlamedOnTheCutoff(self):
        skips = BandDfSkips()
        skips.addLookups(1, 20)
        skips.addLookups(2, 20)
        for _ in range(5):
            skips.addSkipped([1], 7)
        for _ in range(20):
            skips.addSkipped([2], 7)
        # 25 of 20 bands is unreachable with or without the cutoff: nothing is the cutoff's doing,
        # not even a function that lost every band
        self.assertEqual(0, skips.queryFunctionsCutOff(25))
        # within the band count, 15 bands left miss 16 and 0 left miss anything
        self.assertEqual(2, skips.queryFunctionsCutOff(16))
        self.assertEqual(1, skips.queryFunctionsCutOff(15))
        num_bands = self.storage._storage_config.STORAGE_NUM_BANDS
        info = self._candidateGeneration(band_df_cutoff=1, band_matches_required=num_bands + 5)
        expected = self._expected(1, num_bands + 5)
        self.assertGreater(expected["query_functions_with_skipped_bands"], 0)
        self.assertEqual(0, expected["query_functions_cut_off"])
        self.assertEqual(expected, {key: info[key] for key in expected})
        self.assertFalse(info["degraded"])

    def testALookupWithoutTheCutoffCountsNothingSkipped(self):
        self.assertEqual(self._expected(10**6), self._counted(0))
        self.assertEqual(0, self._counted(10**6)["band_lookups_skipped"])

    def testTheSingleLookupCountsToo(self):
        skips = BandDfSkips()
        for minhash in self.minhashes.values():
            self.storage.getCandidatesForMinHash(minhash, band_matches_required=1, band_df_cutoff=1, skips=skips)
        expected = self._expected(1)
        self.assertEqual(
            (expected["band_lookups"], expected["band_lookups_skipped"], expected["postings_skipped"]), (skips.band_lookups, skips.band_lookups_skipped, skips.postings_skipped)
        )

    def testCountingDoesNotChangeTheCandidates(self):
        uncounted = self.storage.getCandidatesForMinHashes(self.minhashes, band_matches_required=1, band_df_cutoff=1)
        counted = self.storage.getCandidatesForMinHashes(self.minhashes, band_matches_required=1, band_df_cutoff=1, skips=BandDfSkips())
        self.assertEqual(uncounted, counted)

    def testAReportOverTheCutoffCarriesTheSkips(self):
        info = self._candidateGeneration(band_df_cutoff=1, band_matches_required=1)
        self.assertEqual(self._expected(1), {key: info[key] for key in self._expected(1)})
        # bands were skipped, but every query function kept one: not degraded
        self.assertEqual(0, info["query_functions_cut_off"])
        self.assertFalse(info["degraded"])
        self._addCopyOfTheQuerySample()
        info = self._candidateGeneration(band_df_cutoff=1, band_matches_required=1)
        expected = self._expected(1)
        self.assertEqual(expected, {key: info[key] for key in expected})
        self.assertTrue(info["band_df_skips_measured"])
        # a query function lost every band: the fuzzy path cannot find it, so recall is at risk
        self.assertTrue(info["degraded"])
        # no shortlist ran, so it dropped nothing and says so with None rather than 0
        self.assertEqual((None, None, None), (info["shortlist_voted_samples"], info["shortlist_dropped_samples"], info["shortlist_dropped_candidates"]))

    def testANormalReportCountsNothingSkipped(self):
        info = self._candidateGeneration()
        self.assertEqual(self._expected(0)["band_lookups"], info["band_lookups"])
        self.assertGreater(info["band_lookups"], 0)
        self.assertEqual((0, 0, 0, 0), (info["band_lookups_skipped"], info["postings_skipped"], info["query_functions_with_skipped_bands"], info["query_functions_cut_off"]))
        self.assertFalse(info["degraded"])

    def testAShortlistReportsWhatItDropped(self):
        info = self._candidateGeneration(shortlist_size=1)
        self.assertEqual(0, info["band_lookups_skipped"])
        self.assertGreaterEqual(info["shortlist_voted_samples"], 2)
        self.assertEqual(info["shortlist_voted_samples"] - 1, info["shortlist_dropped_samples"])
        self.assertGreater(info["shortlist_dropped_candidates"], 0)
        # dropping samples is what a shortlist is for, not a degradation
        self.assertFalse(info["degraded"])
        # stage 1 bands the query once and the batch loop reuses it: every lookup counted once
        self.assertEqual(self._expected(0)["band_lookups"], info["band_lookups"])

    def testAShortlistThatFallsBackMidJobCountsTheLookupsOnce(self):
        answers = iter([True])
        with mock.patch.object(self.storage, "isFunctionRangeIndexComplete", side_effect=lambda: next(answers, False)):
            info = self._candidateGeneration(shortlist_size=1, band_df_cutoff=1, band_matches_required=2)
        expected = self._expected(1, band_matches_required=2)
        self.assertEqual(expected, {key: info[key] for key in expected})
        self.assertIsNone(info["shortlist_dropped_candidates"])

    def testWithoutAMinHashStageThereIsNothingToCount(self):
        self.assertIsNone(self._candidateGeneration(band_matches_required=0, band_df_cutoff=1))

    def testCountingCanBeSwitchedOff(self):
        with mock.patch.object(self.storage._storage_config, "STORAGE_BAND_DF_COUNT_SKIPS", False):
            info = self._candidateGeneration(band_df_cutoff=1)
            counted = self.storage.getCandidatesForMinHashes(self.minhashes, band_matches_required=1, band_df_cutoff=1, skips=BandDfSkips())
        self.assertEqual((False, "disabled"), (info["band_df_skips_measured"], info["band_df_skips_unmeasured_reason"]))
        self.assertIsNone(info["band_lookups_skipped"])
        self.assertFalse(info["degraded"])
        self.assertEqual(self._expected(1)["band_lookups"], info["band_lookups"])
        self.assertEqual(self.storage.getCandidatesForMinHashes(self.minhashes, band_matches_required=1, band_df_cutoff=1), counted)

    def testADegradedQueryIsLoggedAndANormalOneIsNot(self):
        logging.disable(logging.NOTSET)
        try:
            # skipped bands without a query function losing all of them: not logged
            with self.assertLogs("mcrit.matchers.MatcherInterface", level="INFO") as logs:
                self._candidateGeneration(band_df_cutoff=1, band_matches_required=1)
            self.assertFalse(any("degraded" in line for line in logs.output), logs.output)
            self._addCopyOfTheQuerySample()
            with self.assertLogs("mcrit.matchers.MatcherInterface", level="INFO") as logs:
                self._candidateGeneration(band_df_cutoff=1, band_matches_required=1)
            self.assertTrue(any("Candidate generation degraded" in line for line in logs.output), logs.output)
            # a shortlist dropping samples is by design and not logged as degraded either
            with self.assertLogs("mcrit.matchers.MatcherInterface", level="INFO") as logs:
                self._candidateGeneration(shortlist_size=1)
            self.assertFalse(any("degraded" in line for line in logs.output), logs.output)
        finally:
            logging.disable(logging.CRITICAL)


@pytest.mark.mongo
class MongoBandDfSkipsTest(MemoryBandDfSkipsTest):
    STORAGE_METHOD = StorageFactory.STORAGE_METHOD_MONGODB

    def tearDown(self):
        client = self.storage._getDb().client
        for db_name in (DB_NAME, DB_NAME + "_bucketed"):
            client.drop_database(db_name)
            client.drop_database(db_name + "_queue")

    def testBucketedBandsCountTheWholePostingList(self):
        """Under STORAGE_BAND_BUCKET_SIZE a spilled hash is skipped once, with its df across all buckets."""
        self._build(db_name=DB_NAME + "_bucketed", bucket_size=2)
        self._addCopyOfTheQuerySample(1)
        self._addCopyOfTheQuerySample(2)
        self.assertTrue(self.storage.isBandDfIndexComplete())
        self.assertGreater(self.storage._getDb()["band_0"].count_documents({"bucket": {"$gt": 0}}), 0, "the fixture must spill")
        for cutoff in (1, 2):
            for band_matches_required in (1, 2):
                expected = self._expected(cutoff, band_matches_required)
                self.assertGreater(expected["band_lookups_skipped"], 0)
                for accumulation in ("dict", "numpy"):
                    with mock.patch.object(self.storage._storage_config, "STORAGE_CANDIDATE_ACCUMULATION", accumulation):
                        self.assertEqual(expected, self._counted(cutoff, band_matches_required=band_matches_required), (cutoff, band_matches_required, accumulation))

    def testVsResultsCountedWithoutTheDfIndexAreNotCached(self):
        self.storage._setBandDfIndexComplete(False)
        vs = self.worker.getMatchesForSampleVs(self.sample_id, self.sample_ids[1], band_df_cutoff=1)
        group = self.worker.getMatchesForSampleVsGroup(self.sample_id, self.sample_ids[1:], band_df_cutoff=1)
        for report in (vs, group):
            self.assertEqual("band_df_index_incomplete", report["info"]["matching"]["candidate_generation"]["band_df_skips_unmeasured_reason"])
            self.assertIsInstance(report, UncacheableResult)
        self.storage._setBandDfIndexComplete(True)
        self.assertNotIsInstance(self.worker.getMatchesForSampleVs(self.sample_id, self.sample_ids[1], band_df_cutoff=1), UncacheableResult)
        self.assertNotIsInstance(self.worker.getMatchesForSampleVsGroup(self.sample_id, self.sample_ids[1:], band_df_cutoff=1), UncacheableResult)

    def testAResultCountedWithoutTheDfIndexIsNotCached(self):
        """Once the index is rebuilt the same request must be counted, not served the unmeasured result."""
        self.storage._setBandDfIndexComplete(False)
        self.assertIsInstance(self.worker.getMatchesForSample(self.sample_id, band_df_cutoff=1), UncacheableResult)
        # without a cutoff nothing needed the df index
        self.assertNotIsInstance(self.worker.getMatchesForSample(self.sample_id), UncacheableResult)
        self.storage._setBandDfIndexComplete(True)
        self.assertNotIsInstance(self.worker.getMatchesForSample(self.sample_id, band_df_cutoff=1), UncacheableResult)

    def testBothAccumulationsCount(self):
        expected = self._expected(1)
        for accumulation in ("dict", "numpy"):
            with mock.patch.object(self.storage._storage_config, "STORAGE_CANDIDATE_ACCUMULATION", accumulation):
                self.assertEqual(expected, self._counted(1), accumulation)
        self.assertEqual(expected, self._counted(1, lookup="getCandidateArraysForMinHashes"))

    def testSwitchedOffTheLookupMakesNoSkipProbe(self):
        """STORAGE_BAND_DF_COUNT_SKIPS=False must drop the $unionWith, not only the report."""
        pipelines = []
        build = self.storage._bandLookupPipeline

        def spy(*args, **kwargs):
            pipeline = build(*args, **kwargs)
            pipelines.append(pipeline)
            return pipeline

        with mock.patch.object(self.storage, "_bandLookupPipeline", side_effect=spy):
            self.storage.getCandidatesForMinHashes(self.minhashes, band_matches_required=1, band_df_cutoff=1, skips=BandDfSkips())
            self.assertTrue(any("$unionWith" in stage for pipeline in pipelines for stage in pipeline))
            pipelines.clear()
            with mock.patch.object(self.storage._storage_config, "STORAGE_BAND_DF_COUNT_SKIPS", False):
                self.storage.getCandidatesForMinHashes(self.minhashes, band_matches_required=1, band_df_cutoff=1, skips=BandDfSkips())
        self.assertTrue(pipelines)
        self.assertFalse(any("$unionWith" in stage for pipeline in pipelines for stage in pipeline))

    def testWithoutTheDfIndexTheSkipsAreReportedAsUnmeasured(self):
        """Without a trusted df the lookup cannot tell a skipped hash from one the corpus lacks."""
        self.storage._setBandDfIndexComplete(False)
        info = self._candidateGeneration(band_df_cutoff=1)
        self.assertFalse(info["band_df_skips_measured"])
        self.assertEqual("band_df_index_incomplete", info["band_df_skips_unmeasured_reason"])
        self.assertIsNone(info["band_lookups_skipped"])
        self.assertGreater(info["band_lookups"], 0)

    def testTheSkippedHashesComeFromTheIndexAlone(self):
        """The count must not read a skipped posting list: only the admitted ones are fetched."""
        wanted = sorted({band_hash for minhash in self.minhashes.values() for band_hash in self.storage.getBandHashesForMinHash(minhash).values()})
        pipeline = self.storage._bandLookupPipeline(wanted, band_df_cutoff=1, df_index_complete=True, skipped_from="band_0")
        explain = self.storage._getDb().command("aggregate", "band_0", pipeline=pipeline, explain=True)
        stages = []

        def walk(node):
            if isinstance(node, dict):
                if "stage" in node:
                    stages.append((node["stage"], node.get("keyPattern")))
                for key, value in node.items():
                    if key != "rejectedPlans":
                        walk(value)
            elif isinstance(node, list):
                for value in node:
                    walk(value)

        walk(explain)
        index_scans = [key_pattern for stage, key_pattern in stages if stage == "IXSCAN"]
        self.assertEqual([{"band_hash": 1, "df": 1}] * 2, index_scans, stages)
        self.assertEqual(1, [stage for stage, _ in stages].count("FETCH"), stages)


if __name__ == "__main__":
    unittest.main()
