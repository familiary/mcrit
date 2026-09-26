"""Tests for the two-stage (shortlist) 1-vs-N matching path and the function range index."""

import json
import logging
import os
from unittest import TestCase, main

import numpy as np
import pytest
from smda.common.SmdaReport import SmdaReport

from mcrit.config.McritConfig import McritConfig
from mcrit.config.MinHashConfig import MinHashConfig
from mcrit.config.QueueConfig import QueueConfig
from mcrit.config.ShinglerConfig import ShinglerConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.matchers.MatcherSample import MatcherSample
from mcrit.queue.QueueFactory import QueueFactory
from mcrit.storage.StorageFactory import StorageFactory

from .context import getTestMongoServerAndPort
from .testPerJobMatchingKnobs import assertCutoffKeepsListsOfItsLength

LOG = logging.getLogger(__name__)
logging.disable(logging.CRITICAL)

THIS_FILE_PATH = str(os.path.abspath(__file__))
PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
REPORTS = ["example_report.smda", "example_report_2.smda", "example_report_3.smda", "library_report.smda"]

DB_NAME = "test_two_stage_matching"


def buildConfig(shortlist_size=0, band_df_cutoff=0):
    server, port = getTestMongoServerAndPort()
    config = McritConfig()
    config.STORAGE_CONFIG = StorageConfig(
        STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB,
        STORAGE_SERVER=server,
        STORAGE_PORT=port,
        STORAGE_MONGODB_DBNAME=DB_NAME,
        STORAGE_BAND_DF_CUTOFF=band_df_cutoff,
    )
    config.MINHASH_CONFIG = MinHashConfig()
    config.MINHASH_CONFIG.MINHASH_MATCHING_SHORTLIST_SIZE = shortlist_size
    config.SHINGLER_CONFIG = ShinglerConfig()
    config.QUEUE_CONFIG = QueueConfig()
    config.QUEUE_CONFIG.QUEUE_MONGODB_DBNAME = DB_NAME + "_queue"
    # these tests drive Worker methods directly, so the queue only has to hand one back
    config.QUEUE_CONFIG.QUEUE_METHOD = QueueFactory.QUEUE_METHOD_FAKE
    return config


def loadReport(name):
    with open(os.sep.join([PROJECT_ROOT, "tests", name])) as handle:
        return SmdaReport.fromDict(json.load(handle))


@pytest.mark.mongo
class TwoStageMatchingTest(TestCase):
    @classmethod
    def setUpClass(cls):
        config = buildConfig()
        index = MinHashIndex(config=config)
        index._storage.clearStorage()
        cls.sample_ids = []
        for name in REPORTS:
            entry = index._storage.addSmdaReport(loadReport(name))
            if entry is not None:
                index.queue._worker.updateMinHashesForSample(entry.sample_id)
                cls.sample_ids.append(entry.sample_id)
        cls.query_sample_id = cls.sample_ids[0]

    def _match(self, shortlist_size=0, band_df_cutoff=0):
        index = MinHashIndex(config=buildConfig(shortlist_size, band_df_cutoff))
        return MatcherSample(index.queue._worker).getMatchesForSample(self.query_sample_id)

    @staticmethod
    def _sampleIds(report):
        return {entry["sample_id"] for entry in report["matches"]["samples"]}

    @staticmethod
    def _functionMatches(report):
        """(query function id, matched sample id) -> score, flattened out of the report."""
        flattened = {}
        for entry in report["matches"]["functions"]:
            for match in entry["matches"]:
                flattened[(entry["fid"], match[1])] = match[3]
        return flattened

    def testFunctionRangeIndexIsBuiltAndComplete(self):
        storage = MinHashIndex(config=buildConfig())._storage
        num_samples = storage.rebuildFunctionRangeIndex()
        self.assertEqual(num_samples, len(self.sample_ids))
        self.assertTrue(storage.isFunctionRangeIndexComplete())

    def testFunctionIdsResolveToTheirOwnSample(self):
        """Every function must map back to the sample that actually holds it."""
        storage = MinHashIndex(config=buildConfig())._storage
        storage.rebuildFunctionRangeIndex()
        for sample_id in self.sample_ids:
            function_ids = [entry.function_id for entry in storage.getFunctionsBySampleId(sample_id)]
            if not function_ids:
                continue
            resolved = storage.getSampleIdsForFunctionIdArray(np.array(function_ids, dtype=np.int64))
            self.assertTrue(bool(np.all(resolved == sample_id)), "sample %d resolved to %s" % (sample_id, set(resolved.tolist())))

    def testUnknownFunctionIdResolvesToMinusOne(self):
        storage = MinHashIndex(config=buildConfig())._storage
        storage.rebuildFunctionRangeIndex()
        resolved = storage.getSampleIdsForFunctionIdArray(np.array([10**9], dtype=np.int64))
        self.assertEqual(int(resolved[0]), -1)

    def testShortlistDisabledByDefaultLeavesResultsUnchanged(self):
        """The knob defaulting to 0 must mean 'no shortlist', bit for bit."""
        self.assertEqual(MinHashConfig().MINHASH_MATCHING_SHORTLIST_SIZE, 0)
        self.assertEqual(StorageConfig().STORAGE_BAND_DF_CUTOFF, 0)
        first = self._match()
        second = self._match()
        self.assertEqual(self._sampleIds(first), self._sampleIds(second))
        self.assertEqual(self._functionMatches(first), self._functionMatches(second))

    def testShortlistBoundsTheNumberOfMatchedSamples(self):
        storage = MinHashIndex(config=buildConfig())._storage
        storage.rebuildFunctionRangeIndex()
        reference = self._match()
        shortlisted = self._match(shortlist_size=1)
        # the query's own sample is always kept, so a shortlist of 1 admits at most 2
        self.assertLessEqual(len(self._sampleIds(shortlisted)), 2)
        self.assertLessEqual(len(self._sampleIds(shortlisted)), len(self._sampleIds(reference)))

    def testShortlistKeepsScoresExactForTheSamplesItKeeps(self):
        """Restricting *which* samples are matched must not change *how* they are matched."""
        storage = MinHashIndex(config=buildConfig())._storage
        storage.rebuildFunctionRangeIndex()
        reference = self._functionMatches(self._match())
        shortlisted = self._functionMatches(self._match(shortlist_size=2))
        self.assertTrue(shortlisted, "shortlisted run produced no function matches")
        for key, score in shortlisted.items():
            self.assertIn(key, reference)
            self.assertEqual(score, reference[key], "score changed for %s" % (key,))

    def testShortlistSizeCanBeSetPerJob(self):
        """A job's own shortlist_size wins over the configured one, in both directions (#217)."""
        storage = MinHashIndex(config=buildConfig())._storage
        storage.rebuildFunctionRangeIndex()
        reference = self._sampleIds(self._match())
        configured_index = MinHashIndex(config=buildConfig(shortlist_size=1))
        # configured on, switched off for this job: the unrestricted result
        unrestricted = MatcherSample(configured_index.queue._worker, shortlist_size=0).getMatchesForSample(self.query_sample_id)
        self.assertEqual(reference, self._sampleIds(unrestricted))
        # configured off, switched on for this job
        default_index = MinHashIndex(config=buildConfig())
        shortlisted = MatcherSample(default_index.queue._worker, shortlist_size=1).getMatchesForSample(self.query_sample_id)
        self.assertLessEqual(len(self._sampleIds(shortlisted)), 2)
        self.assertLess(len(self._sampleIds(shortlisted)), len(reference))

    def testBandDfCutoffCanBeSetPerJob(self):
        """A job's band_df_cutoff reaches the band lookup, and overrides the configured one (#217)."""
        index = MinHashIndex(config=buildConfig())
        storage = index._storage
        function_entries = storage.getFunctionsBySampleId(self.query_sample_id)
        bits = index.config.MINHASH_CONFIG.MINHASH_SIGNATURE_BITS
        minhashes = {entry.function_id: entry.getMinHash(minhash_bits=bits) for entry in function_entries if entry.minhash}
        unrestricted = storage.getCandidatesForMinHashes(minhashes, band_matches_required=1)
        restricted = storage.getCandidatesForMinHashes(minhashes, band_matches_required=1, band_df_cutoff=1)
        self.assertLess(sum(map(len, restricted.values())), sum(map(len, unrestricted.values())))
        configured = MinHashIndex(config=buildConfig(band_df_cutoff=1))._storage
        self.assertEqual(restricted, configured.getCandidatesForMinHashes(minhashes, band_matches_required=1))
        self.assertEqual(unrestricted, configured.getCandidatesForMinHashes(minhashes, band_matches_required=1, band_df_cutoff=0))

    def testMatcherHandsItsBandDfCutoffToTheLookup(self):
        index = MinHashIndex(config=buildConfig())
        worker = index.queue._worker
        unrestricted = self._functionMatches(MatcherSample(worker).getMatchesForSample(self.query_sample_id))
        restricted = self._functionMatches(MatcherSample(worker, band_df_cutoff=1).getMatchesForSample(self.query_sample_id))
        self.assertLess(len(restricted), len(unrestricted))

    def testStageOneAppliesTheCutoffToo(self):
        """Stage 2 matches the candidates stage 1 fetched, so stage 1 has to fetch them under the cutoff:
        a shortlist that keeps every sample must then give exactly the one-stage result."""
        index = MinHashIndex(config=buildConfig())
        index._storage.rebuildFunctionRangeIndex()
        worker = index.queue._worker
        # per job, so that a lookup falling back to the configured cutoff (none) shows
        one_stage = self._functionMatches(MatcherSample(worker, band_df_cutoff=1).getMatchesForSample(self.query_sample_id))
        self.assertLess(len(one_stage), len(self._functionMatches(self._match())))
        two_stage = MatcherSample(worker, shortlist_size=10**6, band_df_cutoff=1).getMatchesForSample(self.query_sample_id)
        self.assertEqual(one_stage, self._functionMatches(two_stage))

    def testAPostingListAsLongAsTheCutoffIsKept(self):
        """On both lookup paths: the df index, and measuring the list while that index is incomplete."""
        index = MinHashIndex(config=buildConfig())
        storage = index._storage
        bits = index.config.MINHASH_CONFIG.MINHASH_SIGNATURE_BITS
        minhashes = {entry.function_id: entry.getMinHash(minhash_bits=bits) for entry in storage.getFunctionsBySampleId(self.query_sample_id) if entry.minhash}

        def posting_lengths(minhash):
            targets, _ = storage._collectBandHashTargets({-1: minhash})
            return [
                len(document.get("function_ids", []))
                for band_number, band_hashes in targets.items()
                for document in storage._getDb()["band_%d" % band_number].find({"band_hash": {"$in": list(band_hashes)}}, {"function_ids": 1})
            ]

        self.assertTrue(storage.isBandDfIndexComplete())
        assertCutoffKeepsListsOfItsLength(self, storage, minhashes, posting_lengths)
        storage._setBandDfIndexComplete(False)
        try:
            assertCutoffKeepsListsOfItsLength(self, storage, minhashes, posting_lengths)
        finally:
            storage._setBandDfIndexComplete(True)

    def testMatchingJobsApplyTheirOwnKnobs(self):
        """Through the queue, as the server submits them: the job runs with what it was given."""
        index = MinHashIndex(config=buildConfig())
        index._storage.rebuildFunctionRangeIndex()

        def report(**knobs):
            return index.getResultForJob(index.getMatchesForSample(self.query_sample_id, force_recalculation=True, **knobs))

        plain = report(shortlist_size=0, band_df_cutoff=0)
        self.assertLessEqual(len(self._sampleIds(report(shortlist_size=1, band_df_cutoff=0))), 2)
        self.assertLess(len(self._sampleIds(report(shortlist_size=1, band_df_cutoff=0))), len(self._sampleIds(plain)))
        self.assertLess(len(self._functionMatches(report(shortlist_size=0, band_df_cutoff=1))), len(self._functionMatches(plain)))

    def testShortlistFallsBackWhenRangeIndexIsIncomplete(self):
        """Without a trustworthy range index the matcher must match the whole corpus, not guess."""
        storage = MinHashIndex(config=buildConfig())._storage
        storage.rebuildFunctionRangeIndex()
        storage._setFunctionRangeIndexComplete(False)
        try:
            fallback = self._match(shortlist_size=1)
            self.assertEqual(self._sampleIds(fallback), self._sampleIds(self._match()))
        finally:
            storage._setFunctionRangeIndexComplete(True)

    def testPicHashCutoffDropsOnlyOverCommonHashes(self):
        """A cutoff above every holder count must change nothing; a cutoff of 1 must bite."""
        storage = MinHashIndex(config=buildConfig())._storage
        function_ids = [entry.function_id for entry in storage.getFunctionsBySampleId(self.query_sample_id)]
        unrestricted = storage.getPicHashMatchesByFunctionIds(function_ids)

        generous = buildConfig()
        generous.MINHASH_CONFIG.MINHASH_PICHASH_MAX_MATCHES = 10**9
        generous_matches = MinHashIndex(config=generous)._storage.getPicHashMatchesByFunctionIds(function_ids)
        self.assertEqual(generous_matches, unrestricted)

        strict = buildConfig()
        strict.MINHASH_CONFIG.MINHASH_PICHASH_MAX_MATCHES = 1
        strict_matches = MinHashIndex(config=strict)._storage.getPicHashMatchesByFunctionIds(function_ids)
        # every hash is still reported as a key; what the cutoff removes is the holders behind it
        self.assertEqual(set(strict_matches), set(unrestricted))
        for pichash, holders in strict_matches.items():
            self.assertLessEqual(len(holders), len(unrestricted[pichash]))
        self.assertLessEqual(
            sum(len(holders) for holders in strict_matches.values()),
            sum(len(holders) for holders in unrestricted.values()),
        )

    def testPicHashCountsMatchTheFunctionsCollection(self):
        """Every stored count must equal the number of functions actually holding that hash.

        The cutoff's correctness rests on this: a count that is too low hides a hash that should
        have been searched, and one that is too high wastes the fetch the cutoff exists to avoid.
        """
        storage = MinHashIndex(config=buildConfig())._storage
        storage.rebuildPicHashCountIndex()
        database = storage._getDb()
        truth = {group["_id"]: group["n"] for group in database.functions.aggregate([{"$match": {"_pichash": {"$ne": None}}}, {"$group": {"_id": "$_pichash", "n": {"$sum": 1}}}])}
        stored = {document["_pichash"]: document["df"] for document in database[storage._PICHASH_COUNT_COLLECTION].find({}, {"_pichash": 1, "df": 1, "_id": 0})}
        self.assertEqual(stored, truth)

    def testPicHashFilterAgreesWithCounting(self):
        """The indexed filter must select exactly the hashes the counting fallback would."""
        index = MinHashIndex(config=buildConfig())
        storage = index._storage
        storage.rebuildPicHashCountIndex()
        hashes = list(dict.fromkeys(document["_pichash"] for document in storage._getDb().functions.find({"_pichash": {"$ne": None}}, {"_pichash": 1, "_id": 0}).limit(300)))
        for cutoff in (1, 3, 1000):
            storage._minhash_config.MINHASH_PICHASH_MAX_MATCHES = cutoff
            with_index = set(storage._filterPicHashesByMatchCount(list(hashes)))
            storage._setPicHashCountIndexComplete(False)
            try:
                counted = set(storage._filterPicHashesByMatchCount(list(hashes)))
            finally:
                storage._setPicHashCountIndexComplete(True)
            self.assertEqual(with_index, counted, "cutoff %d selected different hashes" % cutoff)

    def testAnInterruptedRebuildLeavesTheIndexMarkedIncomplete(self):
        """A rebuild that dies part-way must not leave the flag claiming a complete index.

        Both indexes fail *silently* when read while half-built: a missing function range
        attributes a function to no sample, and a missing pichash count excludes that hash from
        the filter entirely. So the flag has to drop before the collection is emptied rather
        than only be restored after it is refilled - otherwise a crashed or killed rebuild
        leaves a database that looks trustworthy and is not.
        """
        storage = MinHashIndex(config=buildConfig())._storage

        class Boom(Exception):
            pass

        class FailingFunctions:
            """Stands in for db.functions and fails the moment the rebuild reads it."""

            def __getattr__(self, name):
                def fail(*args, **kwargs):
                    raise Boom("rebuild interrupted while reading functions.%s" % name)

                return fail

        class DatabaseWithFailingFunctions:
            """Proxies the real database; pymongo builds a fresh Collection per attribute
            access, so patching db.functions directly does not stick."""

            def __init__(self, database):
                self._database = database

            def __getattr__(self, name):
                if name == "functions":
                    return FailingFunctions()
                return getattr(self._database, name)

            def __getitem__(self, name):
                return self._database[name]

        real_database = storage._getDb()
        for rebuild, is_complete, setter in (
            (storage.rebuildFunctionRangeIndex, storage.isFunctionRangeIndexComplete, storage._setFunctionRangeIndexComplete),
            (storage.rebuildPicHashCountIndex, storage.isPicHashCountIndexComplete, storage._setPicHashCountIndexComplete),
        ):
            setter(True)
            self.assertTrue(is_complete())
            storage._getDb = lambda database=real_database: DatabaseWithFailingFunctions(database)
            try:
                with self.assertRaises(Boom):
                    rebuild()
            finally:
                del storage._getDb
            self.assertFalse(is_complete(), "%s left the flag claiming complete after being interrupted" % rebuild.__name__)
            rebuild()
            self.assertTrue(is_complete(), "%s did not restore its flag on a clean run" % rebuild.__name__)

    def testBandRebuildKeepsTheIndexTheCutoffNeeds(self):
        """Rebuilding the bands must leave the (band_hash, df) index the cutoff reads.

        _updateBands maintains df from the first write, so a rebuilt band collection has correct
        counts either way - the failure is silent and performance-only: without the compound
        index STORAGE_BAND_DF_CUTOFF falls back to scanning, which is the cost it exists to
        avoid. Cheap to assert, invisible otherwise.
        """
        storage = MinHashIndex(config=buildConfig())._storage
        storage.rebuildMinhashBandIndex()
        for band_number in range(storage._storage_config.STORAGE_NUM_BANDS):
            index_keys = [tuple(index["key"].items()) for index in storage._getDb()["band_%d" % band_number].list_indexes()]
            self.assertIn(
                (("band_hash", 1), ("df", 1)),
                index_keys,
                "band_%d lost the (band_hash, df) index the df cutoff reads" % band_number,
            )

    def testPicHashCutoffDefaultsToOff(self):
        self.assertEqual(MinHashConfig().MINHASH_PICHASH_MAX_MATCHES, 0)

    def testFreshDatabaseVouchesForBothIndexes(self):
        """A database with no functions maintains both indexes from its first write.

        Without this a new instance would keep both perfectly up to date and still take the
        fallback path forever, because nothing had ever vouched for them.
        """
        config = buildConfig()
        config.STORAGE_CONFIG.STORAGE_MONGODB_DBNAME = DB_NAME + "_fresh"
        storage = MinHashIndex(config=config)._storage
        storage.clearStorage()
        self.assertTrue(storage.isFunctionRangeIndexComplete())
        self.assertTrue(storage.isBandDfIndexComplete())

    def testBandDfIsMaintainedOnInsert(self):
        """df must equal the posting list it counts, or the cutoff hides lists that should match."""
        storage = MinHashIndex(config=buildConfig())._storage
        for band_number in range(storage._storage_config.STORAGE_NUM_BANDS):
            collection = storage._getDb()["band_%d" % band_number]
            mismatching = collection.count_documents({"$expr": {"$ne": ["$df", {"$size": {"$ifNull": ["$function_ids", []]}}]}})
            self.assertEqual(mismatching, 0, "band_%d has %d documents whose df disagrees with its posting list" % (band_number, mismatching))

    def testBandDfCutoffKeepsMatchesItDoesNotFilter(self):
        """A cutoff above every posting list must leave results identical."""
        reference = self._match()
        generous = self._match(band_df_cutoff=10**9)
        self.assertEqual(self._sampleIds(reference), self._sampleIds(generous))
        self.assertEqual(self._functionMatches(reference), self._functionMatches(generous))


if __name__ == "__main__":
    main()
