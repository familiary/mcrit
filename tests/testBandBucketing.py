#!/usr/bin/python3

import json
import logging
import os
import unittest

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

    def testASpilledHashIsNotServedAsBucketZeroWithoutTheDfIndex(self):
        """With the cutoff equal to the bucket size, a full bucket 0 passes a $size test on its own."""
        storage = self._rawStorage(2, "cutoff_equal", df_cutoff=2)
        storage._updateBands({0: {4242: [1, 2, 3], 4343: [4, 5]}}, method="push")
        storage._setBandDfIndexComplete(False)
        served = {document["band_hash"] for document in storage._getDb()["band_0"].aggregate(storage._bandLookupPipeline([4242, 4343]))}
        self.assertEqual({4343}, served)
        storage._setBandDfIndexComplete(True)
        served = {document["band_hash"] for document in storage._getDb()["band_0"].aggregate(storage._bandLookupPipeline([4242, 4343]))}
        self.assertEqual({4343}, served)

    def testCutoffAboveBucketSizeIsRejected(self):
        """Only bucket 0 carries df, so a cutoff a spilled hash could fit under would return bucket 0 alone."""
        with self.assertRaises(ValueError):
            self._rawStorage(2, "cutoff", df_cutoff=5)._getDb()


if __name__ == "__main__":
    unittest.main()
