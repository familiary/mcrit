"""The matching-cache fetch deduplicates by signature, and that changes nothing about results.

Several functions in these fixtures deliberately carry the *same* MinHash signature, which is
what a real corpus does at scale (2.46x distinct-signature dedup measured on 257 Malpedia
samples). The fetch decodes one signature per distinct signature instead of one per candidate
function; these tests pin that the cache it hands the matcher, and the match report built from
it, are identical to what the per-function fetch produced.
"""

import json
import logging
import os
from copy import deepcopy
from typing import Optional
from unittest import TestCase, main

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

LOG = logging.getLogger(__name__)
logging.disable(logging.CRITICAL)

THIS_FILE_PATH = str(os.path.abspath(__file__))
PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
REPORTS = ["example_report.smda", "example_report_2.smda", "example_report_3.smda"]

DB_NAME = "test_matching_cache_dedup"


def loadReport(name):
    with open(os.sep.join([PROJECT_ROOT, "tests", name])) as handle:
        return SmdaReport.fromDict(json.load(handle))


def buildMemoryConfig():
    config = McritConfig()
    config.STORAGE_CONFIG = StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MEMORY)
    config.MINHASH_CONFIG = MinHashConfig()
    config.SHINGLER_CONFIG = ShinglerConfig()
    config.QUEUE_CONFIG = QueueConfig()
    config.QUEUE_CONFIG.QUEUE_METHOD = QueueFactory.QUEUE_METHOD_FAKE
    return config


def buildMongoConfig():
    server, port = getTestMongoServerAndPort()
    config = McritConfig()
    config.STORAGE_CONFIG = StorageConfig(
        STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB,
        STORAGE_SERVER=server,
        STORAGE_PORT=port,
        STORAGE_MONGODB_DBNAME=DB_NAME,
    )
    config.MINHASH_CONFIG = MinHashConfig()
    config.SHINGLER_CONFIG = ShinglerConfig()
    config.QUEUE_CONFIG = QueueConfig()
    config.QUEUE_CONFIG.QUEUE_MONGODB_DBNAME = DB_NAME + "_queue"
    config.QUEUE_CONFIG.QUEUE_METHOD = QueueFactory.QUEUE_METHOD_FAKE
    return config


def comparableReport(report):
    """The match report without the fields that legitimately differ between two runs."""
    stripped = deepcopy(report)
    stripped["info"].pop("job", None)
    return stripped


def cacheShape(cache):
    """Everything the matcher can observe about a cache, by value rather than by identity."""
    return (
        dict(cache._func_id_to_minhash),
        dict(cache._func_id_to_sample_id),
        {sample_id: set(function_ids) for sample_id, function_ids in cache._sample_id_to_func_ids.items()},
    )


def dedupFactor(cache):
    """candidate functions / distinct signature objects actually held."""
    minhashes = list(cache._func_id_to_minhash.values())
    if not minhashes:
        return 1.0
    return len(minhashes) / len({id(minhash) for minhash in minhashes})


class MatchingCacheDedupTestBase:
    """Shared assertions; the two subclasses only differ in which storage backend they build.

    A mixin rather than a TestCase, so it is not collected on its own; plain asserts keep it
    free of the TestCase methods it does not have.
    """

    query_sample_id: Optional[int] = None

    def _makeIndex(self):
        raise NotImplementedError

    def _duplicateSignatures(self, storage):
        """Give a handful of functions from other samples the query sample's first signature.

        Returns the number of functions sharing it. Done on the stored data rather than by
        crafting a report, so the duplicates are indistinguishable from naturally occurring
        ones - which is the case the fetch has to get right.
        """
        raise NotImplementedError

    def _referenceCacheData(self, storage, function_ids):
        """The mapping a per-function fetch would build, derived without the fetch under test."""
        raise NotImplementedError

    def _candidateFunctionIds(self, matcher):
        """Every function id one matching run asks the cache for."""
        requested = []
        original = matcher._storage.createMatchingCache

        def recording(function_ids, *args, **kwargs):
            requested.extend(function_ids)
            return original(function_ids, *args, **kwargs)

        matcher._storage.createMatchingCache = recording
        try:
            matcher.getMatchesForSample(self.query_sample_id)
        finally:
            matcher._storage.createMatchingCache = original
        return requested

    def testCacheContentsMatchAPerFunctionFetch(self):
        index = self._makeIndex()
        storage = index._storage
        matcher = MatcherSample(index.queue._worker)
        function_ids = self._candidateFunctionIds(matcher)
        assert function_ids, "the fixture produced no candidates, nothing is being tested"

        cache = storage.createMatchingCache(function_ids)
        reference_minhashes, reference_sample_ids = self._referenceCacheData(storage, function_ids)
        held_minhashes, held_sample_ids, held_sample_to_functions = cacheShape(cache)

        assert held_minhashes == reference_minhashes
        assert held_sample_ids == reference_sample_ids
        expected_sample_to_functions = {}
        for function_id, sample_id in reference_sample_ids.items():
            expected_sample_to_functions.setdefault(sample_id, set()).add(function_id)
        assert held_sample_to_functions == expected_sample_to_functions

        # the point of the change: one decoded signature object per distinct signature
        assert len({id(minhash) for minhash in held_minhashes.values()}) == len(set(held_minhashes.values()))
        assert dedupFactor(cache) > 1.0, "the fixture holds no duplicate signatures, the dedup is untested"

    def testMatchReportIsUnchanged(self):
        index = self._makeIndex()
        report = MatcherSample(index.queue._worker).getMatchesForSample(self.query_sample_id)

        # replay the same query with the deduplication defeated: every function gets its own
        # decoded signature object again, exactly as the per-function fetch produced
        legacy_index = self._makeIndex()
        legacy_storage = legacy_index._storage
        original = legacy_storage._getCacheDataForFunctionIds

        def undeduplicated(function_ids):
            cache_data = original(function_ids)
            cache_data["func_id_to_minhash"] = {function_id: bytes(minhash) for function_id, minhash in cache_data["func_id_to_minhash"].items()}
            return cache_data

        legacy_storage._getCacheDataForFunctionIds = undeduplicated
        legacy_report = MatcherSample(legacy_index.queue._worker).getMatchesForSample(self.query_sample_id)

        assert comparableReport(report) == comparableReport(legacy_report)
        assert report["matches"]["functions"], "the fixture produced no function matches, nothing is being compared"


class MemoryMatchingCacheDedupTest(MatchingCacheDedupTestBase, TestCase):
    def setUp(self):
        self.reports = [loadReport(name) for name in REPORTS]
        self.query_sample_id = None
        self._duplicated_signature = None

    def _makeIndex(self):
        index = MinHashIndex(config=buildMemoryConfig())
        worker = index.queue._worker
        sample_ids = []
        for report in self.reports:
            entry = index._storage.addSmdaReport(report)
            assert entry is not None
            worker.updateMinHashesForSample(entry.sample_id)
            sample_ids.append(entry.sample_id)
        self.query_sample_id = sample_ids[0]
        self._duplicateSignatures(index._storage)
        return index

    def _duplicateSignatures(self, storage):
        hashed = [entry for entry in storage._functions.values() if entry.minhash]
        assert len(hashed) > 6
        signature = bytes(hashed[0].minhash)
        for entry in hashed[1:6]:
            # a fresh equal-valued object per function, so nothing is shared by accident
            entry.minhash = bytes(signature)
        return 6

    def _referenceCacheData(self, storage, function_ids):
        minhashes = {}
        sample_ids = {}
        for function_id in set(function_ids):
            entry = storage._query_functions[function_id] if function_id < 0 else storage._functions[function_id]
            minhashes[entry.function_id] = entry.minhash
            sample_ids[entry.function_id] = entry.sample_id
        return minhashes, sample_ids


@pytest.mark.mongo
class MongoMatchingCacheDedupTest(MatchingCacheDedupTestBase, TestCase):
    def setUp(self):
        self.reports = [loadReport(name) for name in REPORTS]
        self.query_sample_id = None

    def _makeIndex(self):
        index = MinHashIndex(config=buildMongoConfig())
        index._storage.clearStorage()
        worker = index.queue._worker
        sample_ids = []
        for report in self.reports:
            entry = index._storage.addSmdaReport(report)
            assert entry is not None
            worker.updateMinHashesForSample(entry.sample_id)
            sample_ids.append(entry.sample_id)
        self.query_sample_id = sample_ids[0]
        self._duplicateSignatures(index._storage)
        return index

    def _duplicateSignatures(self, storage):
        collection = storage._getDb()["functions"]
        hashed = [document for document in collection.find({"minhash": {"$ne": ""}}, {"_id": 0, "function_id": 1, "minhash": 1}).sort("function_id", 1)]
        assert len(hashed) > 6
        signature = hashed[0]["minhash"]
        for document in hashed[1:6]:
            collection.update_one({"function_id": document["function_id"]}, {"$set": {"minhash": signature}})
        return 6

    def _referenceCacheData(self, storage, function_ids):
        minhashes = {}
        sample_ids = {}
        for collection_name, wanted in (
            ("functions", [function_id for function_id in set(function_ids) if function_id >= 0]),
            ("query_functions", [function_id for function_id in set(function_ids) if function_id < 0]),
        ):
            if not wanted:
                continue
            for document in storage._getDb()[collection_name].find({"function_id": {"$in": wanted}}, {"_id": 0, "function_id": 1, "sample_id": 1, "minhash": 1}):
                # stored as BSON binary, or as hex text by an MCRIT before that
                stored = document["minhash"]
                minhashes[document["function_id"]] = stored if isinstance(stored, bytes) else bytes.fromhex(stored)
                sample_ids[document["function_id"]] = document["sample_id"]
        return minhashes, sample_ids


if __name__ == "__main__":
    main()
