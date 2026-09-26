#!/usr/bin/python3

import json
import logging
import os
import unittest
from unittest import mock

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

logging.disable(logging.CRITICAL)

THIS_FILE_PATH = str(os.path.abspath(__file__))
PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
EXAMPLE_REPORT = os.sep.join([PROJECT_ROOT, "tests", "example_report.smda"])
EXAMPLE_REPORT_2 = os.sep.join([PROJECT_ROOT, "tests", "example_report_2.smda"])


@pytest.mark.mongo
class BandBucketingTest(unittest.TestCase):
    """A band posting list must be able to outgrow one MongoDB document.

    A posting list is a `function_ids` array inside a single document and MongoDB caps a document
    at 16 MB, which is about 1.35M ids (1.05M once they need int64). On a 7,244-sample real corpus
    the longest posting list across all 20 bands held 36,183 ids, putting the wall near 270,000
    samples. The $push does not degrade there, it raises, and indexing stops. Sharding
    cannot move it, because a document cannot span shards.

    What matters for correctness is that splitting a posting list changes nothing a caller sees.
    """

    @classmethod
    def setUpClass(cls):
        with open(EXAMPLE_REPORT) as handle:
            cls.report = SmdaReport.fromDict(json.load(handle))
        with open(EXAMPLE_REPORT_2) as handle:
            cls.report_2 = SmdaReport.fromDict(json.load(handle))

    def _index(self, bucket_size, db_suffix):
        # MongoDB-backed on purpose: bucketing exists because of a MongoDB document limit, so a
        # MemoryStorage run would exercise none of it.
        server, port = getTestMongoServerAndPort()
        db_name = "test_band_bucketing_" + db_suffix
        index_config = McritConfig()
        index_config.STORAGE_CONFIG = StorageConfig(
            STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB,
            STORAGE_SERVER=server,
            STORAGE_PORT=port,
            STORAGE_MONGODB_DBNAME=db_name,
            STORAGE_BAND_BUCKET_SIZE=bucket_size,
        )
        index_config.MINHASH_CONFIG = MinHashConfig()
        index_config.SHINGLER_CONFIG = ShinglerConfig()
        index_config.QUEUE_CONFIG = QueueConfig()
        index_config.QUEUE_CONFIG.QUEUE_MONGODB_DBNAME = db_name + "_queue"
        index_config.QUEUE_CONFIG.QUEUE_METHOD = QueueFactory.QUEUE_METHOD_FAKE
        index = MinHashIndex(config=index_config)
        index._storage.clearStorage()
        return index

    @staticmethod
    def _fill(index, reports):
        for report in reports:
            index.addReport(report)

    def _bandDocuments(self, index):
        return index._storage._getDb()["band_0"].count_documents({})

    def testPostingListSplitsAcrossDocuments(self):
        """A posting list longer than the cap spans several documents, none over the cap.

        Driven through _updateBands with known ids rather than through a corpus: whether an
        indexed fixture happens to contain a band hash hot enough to spill is a property of the
        fixture, not of the code under test, and an earlier version of this test passed for that
        reason alone (no hash in it had even three postings, so a cap of two never triggered).
        """
        index = self._index(bucket_size=3, db_suffix="split")
        storage = index._storage
        band_hash = 424242
        storage._updateBands({0: {band_hash: [1, 2, 3, 4, 5, 6, 7]}})
        collection = storage._getDb()["band_0"]
        documents = sorted(collection.find({"band_hash": band_hash}, {"_id": 0}), key=lambda d: d.get("bucket", 0))
        self.assertEqual(len(documents), 3, "seven postings at a cap of three must occupy three buckets")
        self.assertEqual([len(d["function_ids"]) for d in documents], [3, 3, 1])
        self.assertEqual([d["bucket"] for d in documents], [0, 1, 2])
        self.assertEqual(documents[0]["df"], 7, "bucket 0 carries the total")
        self.assertEqual(documents[0]["tail"], 2)
        self.assertEqual(documents[0]["tail_n"], 1)
        # and the postings themselves survive the split, in order
        recovered = [fid for document in documents for fid in document["function_ids"]]
        self.assertEqual(recovered, [1, 2, 3, 4, 5, 6, 7])

    def testAppendingContinuesIntoTheTailBucket(self):
        """A second write must fill the part-full tail before opening a new bucket."""
        index = self._index(bucket_size=3, db_suffix="append")
        storage = index._storage
        band_hash = 515151
        storage._updateBands({0: {band_hash: [1, 2, 3, 4]}})
        storage._updateBands({0: {band_hash: [5, 6, 7]}})
        collection = storage._getDb()["band_0"]
        documents = sorted(collection.find({"band_hash": band_hash}, {"_id": 0}), key=lambda d: d.get("bucket", 0))
        self.assertEqual([len(d["function_ids"]) for d in documents], [3, 3, 1])
        self.assertEqual(documents[0]["df"], 7)
        recovered = [fid for document in documents for fid in document["function_ids"]]
        self.assertEqual(sorted(recovered), [1, 2, 3, 4, 5, 6, 7])

    def testTotalDfIsKeptOnBucketZero(self):
        """df on bucket 0 is the total across buckets - the cutoff reads it and nothing else."""
        index = self._index(bucket_size=2, db_suffix="df")
        self._fill(index, [self.report, self.report_2])
        collection = index._storage._getDb()["band_0"]
        for row in collection.aggregate(
            [
                {"$project": {"band_hash": 1, "bucket": {"$ifNull": ["$bucket", 0]}, "n": {"$size": {"$ifNull": ["$function_ids", []]}}}},
                {"$group": {"_id": "$band_hash", "total": {"$sum": "$n"}}},
            ]
        ):
            head = collection.find_one({"band_hash": row["_id"], "bucket": 0}, {"df": 1, "_id": 0})
            self.assertIsNotNone(head, "every hash must have a bucket 0 carrying its counters")
            self.assertEqual(head["df"], row["total"], "df on bucket 0 must be the total across buckets")

    def testMatchingIsUnchangedByBucketing(self):
        """The point of the whole change: results must be identical, bucketed or not."""
        plain = self._index(bucket_size=0, db_suffix="plain")
        self._fill(plain, [self.report, self.report_2])
        # bucket size 1 so every posting list of more than one entry is split. A larger cap would
        # leave this fixture unbucketed - no band hash in it holds three postings - and the
        # comparison would pass by testing two identical unbucketed indexes against each other.
        bucketed = self._index(bucket_size=1, db_suffix="bucketed")
        self._fill(bucketed, [self.report, self.report_2])
        spilled = bucketed._storage._getDb()["band_0"].count_documents({"bucket": {"$gt": 0}})
        self.assertGreater(spilled, 0, "the comparison is worthless unless something actually spilled")

        # MinHashIndex.getMatchesForSample queues a job and returns its id - comparing those
        # compares two UUIDs and passes whatever the matcher did. Drive the matcher directly.
        sample_id = sorted(sample.sample_id for sample in plain._storage.getSamples(start_index=0, limit=0))[0]
        plain_matches = MatcherSample(plain.queue._worker).getMatchesForSample(sample_id)
        bucketed_matches = MatcherSample(bucketed.queue._worker).getMatchesForSample(sample_id)
        self.assertTrue(plain_matches["matches"]["samples"], "the fixture must produce at least one match")
        self.assertEqual(
            json.dumps(plain_matches["matches"], sort_keys=True),
            json.dumps(bucketed_matches["matches"], sort_keys=True),
            "bucketing must not change a single reported match",
        )

    def testDeletionKeepsBookkeepingConsistent(self):
        """A pull can empty a bucket anywhere in the chain; the counters must still describe reality."""
        index = self._index(bucket_size=2, db_suffix="delete")
        self._fill(index, [self.report, self.report_2])
        storage = index._storage
        sample_ids = sorted(sample.sample_id for sample in storage.getSamples(start_index=0, limit=0))
        victim_function_ids = {function.function_id for function in storage.getFunctionsBySampleId(sample_ids[0])}
        self.assertTrue(victim_function_ids)
        storage.deleteSample(sample_ids[0])
        collection = storage._getDb()["band_0"]
        for band_number in range(storage._storage_config.STORAGE_NUM_BANDS):
            leaked = storage._getDb()["band_%d" % band_number].count_documents({"function_ids": {"$in": sorted(victim_function_ids)}})
            self.assertEqual(leaked, 0, "the deleted sample's postings must be gone from every bucket")
        for row in collection.aggregate(
            [
                {"$project": {"band_hash": 1, "bucket": {"$ifNull": ["$bucket", 0]}, "n": {"$size": {"$ifNull": ["$function_ids", []]}}}},
                {"$group": {"_id": "$band_hash", "total": {"$sum": "$n"}}},
            ]
        ):
            head = collection.find_one({"band_hash": row["_id"], "bucket": 0}, {"df": 1, "_id": 0})
            if head is not None:
                self.assertEqual(head["df"], row["total"], "df must match the postings that survived the deletion")

    def _rawStorage(self, bucket_size, db_suffix, df_cutoff=0):
        server, port = getTestMongoServerAndPort()
        config = McritConfig()
        config.STORAGE_CONFIG = StorageConfig(
            STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB,
            STORAGE_SERVER=server,
            STORAGE_PORT=port,
            STORAGE_MONGODB_DBNAME="test_band_bucketing_raw_" + db_suffix,
            STORAGE_BAND_BUCKET_SIZE=bucket_size,
            STORAGE_BAND_DF_CUTOFF=df_cutoff,
        )
        storage = StorageFactory.getStorage(config)
        storage.clearStorage()
        return storage

    def _bandState(self, storage, band_hash=4242):
        return sorted(
            (document.get("bucket"), document.get("df"), document.get("tail"), document.get("tail_n"), list(document.get("function_ids") or []))
            for document in storage._getDb()["band_0"].find({"band_hash": band_hash})
        )

    def _pullFromSevenPostings(self, bucket_size, db_suffix, victims):
        # one hash, 7 postings: with bucket_size 2 that is buckets 0..3 holding [1,2] [3,4] [5,6] [7]
        storage = self._rawStorage(bucket_size, db_suffix)
        storage._updateBands({0: {4242: list(range(1, 8))}}, method="push")
        storage._updateBands({0: {4242: victims}}, method="pull")
        state = self._bandState(storage)
        surviving = sorted(function_id for _bucket, _df, _tail, _tail_n, ids in state for function_id in ids)
        return storage, state, surviving

    def testPullUnbucketedRemovesPostings(self):
        _storage, _state, surviving = self._pullFromSevenPostings(0, "pull_plain", [5, 6, 7])
        self.assertEqual(surviving, [1, 2, 3, 4])

    def testPullReachesEveryBucket(self):
        """A pull must remove postings from every bucket of a hash, not only the first document it matches."""
        _storage, state, surviving = self._pullFromSevenPostings(2, "pull_upper", [5, 6, 7])
        self.assertEqual(surviving, [1, 2, 3, 4])
        head = [row for row in state if row[0] == 0][0]
        self.assertEqual(head[1], 4, "df must count only the surviving postings")

    def testPullAcrossBucketZeroAndUpper(self):
        _storage, state, surviving = self._pullFromSevenPostings(2, "pull_mixed", [1, 5])
        self.assertEqual(surviving, [2, 3, 4, 6, 7])
        head = [row for row in state if row[0] == 0][0]
        self.assertEqual(head[1], 5)

    def testEmptyingBucketZeroKeepsBookkeeping(self):
        """Bucket 0 alone carries df/tail/tail_n, so it must survive while any other bucket of the hash does."""
        storage, state, surviving = self._pullFromSevenPostings(2, "pull_zero", [1, 2])
        self.assertEqual(surviving, [3, 4, 5, 6, 7])
        heads = [row for row in state if row[0] == 0]
        self.assertEqual(len(heads), 1, "bucket 0 must survive to carry df/tail/tail_n")
        self.assertEqual(heads[0][1:4], (5, 3, 1))
        # purging residue must not take it either
        storage.purgeEmptyBandDocuments()
        self.assertEqual(self._bandState(storage), state)
        # and the next push continues from the real tail with the real df
        storage._updateBands({0: {4242: [8, 9]}}, method="push")
        state = self._bandState(storage)
        head = [row for row in state if row[0] == 0][0]
        self.assertEqual(head[1], 7)
        self.assertEqual(sorted(i for row in state for i in row[4]), [3, 4, 5, 6, 7, 8, 9])
        self.assertTrue(all(len(row[4]) <= 2 for row in state))

    def testPullingEverythingRemovesTheHash(self):
        storage, state, _surviving = self._pullFromSevenPostings(2, "pull_all", list(range(1, 8)))
        self.assertEqual(state, [])

    def testPullOfUnknownHashCreatesNothing(self):
        storage = self._rawStorage(2, "pull_unknown")
        storage._updateBands({0: {4242: [1]}}, method="pull")
        self.assertEqual(self._bandState(storage), [])

    def _candidates(self, storage, band_hash, band_matches_required=1):
        """The candidates every lookup path finds for a query function whose band 0 hash is band_hash."""
        query = mock.Mock(hasMinHash=mock.Mock(return_value=True))
        found = {}
        with mock.patch.object(storage, "getBandHashesForMinHash", return_value={0: band_hash}):
            for accumulation in ("dict", "numpy"):
                with mock.patch.object(storage._storage_config, "STORAGE_CANDIDATE_ACCUMULATION", accumulation):
                    found[accumulation] = sorted(storage.getCandidatesForMinHashes({42: query}, band_matches_required=band_matches_required).get(42, []))
            found["single"] = sorted(storage.getCandidatesForMinHash(query, band_matches_required=band_matches_required))
        return found

    def testLookupReachesTheBucketsAShrunkHashKeeps(self):
        """Pulls can leave a hash under the cutoff with its postings above bucket 0.

        The df-indexed lookup matched bucket 0 alone - the only document carrying df - so those
        postings were never returned, silently: the hash looked like one with no candidates.
        """
        for df_cutoff in (0, 2):
            with self.subTest(df_cutoff=df_cutoff):
                storage = self._rawStorage(2, "lookup_shrunk", df_cutoff=df_cutoff)
                self.assertTrue(storage.isBandDfIndexComplete())
                # buckets [1,2] [3,4] [5,6] [7]; pulling 1..5 leaves [] [6] [7] with df 2
                storage._updateBands({0: {4242: list(range(1, 8))}}, method="push")
                storage._updateBands({0: {4242: [1, 2, 3, 4, 5]}}, method="pull")
                self.assertEqual([6, 7], sorted(i for row in self._bandState(storage) for i in row[4]))
                self.assertEqual({"dict": [6, 7], "numpy": [6, 7], "single": [6, 7]}, self._candidates(storage, 4242))
                # one band hash is one vote: a posting read twice would pass a second band's worth
                self.assertEqual({"dict": [], "numpy": [], "single": []}, self._candidates(storage, 4242, band_matches_required=2))

    def testLookupReadsBucketZeroOnceWhenItKeepsPostings(self):
        """Bucket 0 still holding a posting while an upper bucket survives: every posting once, no more."""
        storage = self._rawStorage(2, "lookup_shrunk_head", df_cutoff=2)
        # buckets [1,2] [3,4] [5,6] [7]; pulling 2..6 leaves [1] [7] with df 2 and tail 3
        storage._updateBands({0: {4242: list(range(1, 8))}}, method="push")
        storage._updateBands({0: {4242: [2, 3, 4, 5, 6]}}, method="pull")
        self.assertEqual({"dict": [1, 7], "numpy": [1, 7], "single": [1, 7]}, self._candidates(storage, 4242))
        self.assertEqual({"dict": [], "numpy": [], "single": []}, self._candidates(storage, 4242, band_matches_required=2))

    def testALookupReadsARecreatedBucketZeroAsEmpty(self):
        """The recompute after a pull recreates a missing bucket 0 for its bookkeeping, without a posting list."""
        for df_cutoff in (0, 2):
            with self.subTest(df_cutoff=df_cutoff):
                storage = self._rawStorage(2, "lookup_recreated", df_cutoff=df_cutoff)
                storage._updateBands({0: {4242: list(range(1, 8))}}, method="push")
                storage._getDb()["band_0"].delete_one({"band_hash": 4242, "bucket": 0})
                storage._updateBands({0: {4242: [3, 4, 5]}}, method="pull")
                head = storage._getDb()["band_0"].find_one({"band_hash": 4242, "bucket": 0})
                self.assertIsNotNone(head)
                # [6] and [7] survive in buckets 2 and 3: df 2, which the cutoff admits
                self.assertEqual({"dict": [6, 7], "numpy": [6, 7], "single": [6, 7]}, self._candidates(storage, 4242))

    def testAStrayDfOnAnUpperBucketDoesNotAdmitIt(self):
        """A pull run with bucketing switched off stamps df on every bucket; only bucket 0's may count."""
        storage = self._rawStorage(2, "lookup_stray_df", df_cutoff=2)
        storage._updateBands({0: {4242: list(range(1, 8))}}, method="push")
        storage._updateBands({0: {4242: [1, 2, 3, 4, 5]}}, method="pull")
        storage._getDb()["band_0"].update_many({"band_hash": 4242, "bucket": {"$gt": 0}}, {"$set": {"df": 1}})
        self.assertEqual({"dict": [6, 7], "numpy": [6, 7], "single": [6, 7]}, self._candidates(storage, 4242))
        self.assertEqual({"dict": [], "numpy": [], "single": []}, self._candidates(storage, 4242, band_matches_required=2))

    def testTheLookupStaysOnTheDfIndex(self):
        """Decided from the (band_hash, df) index entries, so an over-cutoff bucket 0 is never read."""
        storage = self._rawStorage(2, "lookup_df_index", df_cutoff=2)
        storage._updateBands({0: {**{band_hash: [1] for band_hash in range(200)}, **{band_hash: [1, 2, 3, 4, 5] for band_hash in range(1000, 1050)}}}, method="push")
        wanted = list(range(200)) + list(range(1000, 1050))
        explain = storage._getDb().command("aggregate", "band_0", pipeline=storage._bandLookupPipeline(wanted, True), explain=True)
        stages = []

        def walk(node):
            if isinstance(node, dict):
                if node.get("stage") == "IXSCAN":
                    stages.append(node)
                # plans the optimizer rejected say nothing about what runs; their layout also differs
                # between mongod versions, so they are skipped wherever they appear
                for key, value in node.items():
                    if key != "rejectedPlans":
                        walk(value)
            elif isinstance(node, list):
                for value in node:
                    walk(value)

        walk(explain)
        self.assertTrue(stages, explain)
        self.assertTrue(all(stage["keyPattern"] == {"band_hash": 1, "df": 1} for stage in stages), [stage["keyPattern"] for stage in stages])

    def testALookupReadsTheSettingsOnce(self):
        """Not once per band: the df index flag is read once per lookup call and passed down."""
        storage = self._rawStorage(2, "lookup_settings", df_cutoff=2)
        storage._updateBands({0: {4242: [1, 2]}}, method="push")
        query = mock.Mock(hasMinHash=mock.Mock(return_value=True))
        every_band = {band_number: 4242 for band_number in range(storage._storage_config.STORAGE_NUM_BANDS)}
        with (
            mock.patch.object(storage, "getBandHashesForMinHash", return_value=every_band),
            mock.patch.object(storage, "isBandDfIndexComplete", wraps=storage.isBandDfIndexComplete) as flag,
        ):
            storage.getCandidatesForMinHashes({42: query})
        self.assertEqual(1, flag.call_count)

    def testLookupStillSkipsASpilledHashOverTheCutoff(self):
        storage = self._rawStorage(2, "lookup_spilled", df_cutoff=2)
        storage._updateBands({0: {5555: [10, 11, 12, 13, 14]}}, method="push")
        self.assertEqual({"dict": [], "numpy": [], "single": []}, self._candidates(storage, 5555))
        # without a cutoff every bucket of it is found
        storage = self._rawStorage(2, "lookup_spilled_nocut")
        storage._updateBands({0: {5555: [10, 11, 12, 13, 14]}}, method="push")
        self.assertEqual({"dict": [10, 11, 12, 13, 14], "numpy": [10, 11, 12, 13, 14], "single": [10, 11, 12, 13, 14]}, self._candidates(storage, 5555))

    def testCutoffAboveBucketSizeIsRejected(self):
        """Only bucket 0 carries df, so a cutoff a spilled hash could fit under would return bucket 0 alone."""
        with self.assertRaises(ValueError):
            self._rawStorage(2, "cutoff", df_cutoff=5)._getDb()


if __name__ == "__main__":
    unittest.main()
