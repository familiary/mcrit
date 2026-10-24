"""An import that fails while storing leaves nothing behind - no sample, function, minhash, band or
index entry, and no family it created - and an export the storage could not hold is refused before
anything is stored, by both backends alike (#299, #300)."""

import contextlib
import copy
import json
import logging
import os
import threading
import unittest
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import falcon
import falcon.testing
import pymongo
import pytest
from bson import ObjectId
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError
from smda.common.SmdaReport import SmdaReport

from mcrit.config.McritConfig import McritConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.libs.utility import compress_encode, decompress_decode
from mcrit.server.StatusResource import StatusResource
from mcrit.storage.MongoDbStorage import MongoDbStorage
from mcrit.storage.StorageFactory import StorageFactory
from mcrit.storage.StorageInterface import StorageInterface

from .context import config, getTestMongoServerAndPort

logging.disable(logging.CRITICAL)

EXAMPLE_REPORT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "example_report.smda")
ADDED_SHA256 = "ab" * 32
EXISTING_FAMILY_SHA256 = "cd" * 32


def load_report(family="", sha256=None):
    with open(EXAMPLE_REPORT) as fjson:
        report = SmdaReport.fromDict(json.load(fjson))
    assert report is not None
    report.family = family
    if sha256 is not None:
        report.sha256 = sha256
    return report


def failing_after(calls, function, error):
    """function, raising error from the given call on"""
    num_calls = [0]

    def wrapper(*args, **kwargs):
        num_calls[0] += 1
        if num_calls[0] >= calls:
            raise error
        return function(*args, **kwargs)

    return wrapper


def then_failing(function, error):
    """function, raising error once it has run"""

    def wrapper(*args, **kwargs):
        function(*args, **kwargs)
        raise error

    return wrapper


def failing_from(calls, function, error, before_failing, runs_first=False):
    """function, raising error from the given call on, once it has run if runs_first, and calling
    before_failing right before it raises"""
    num_calls = [0]

    def wrapper(*args, **kwargs):
        num_calls[0] += 1
        if num_calls[0] < calls:
            return function(*args, **kwargs)
        if runs_first:
            function(*args, **kwargs)
        before_failing()
        raise error

    return wrapper


class MemoryImportRollbackTest(unittest.TestCase):
    # whether a family counter overflows as MongoDB's 64-bit integers do
    COUNTERS_OVERFLOW = False

    def _config(self):
        return config

    def setUp(self):
        # an instance holding the code the export copies, so that the import shares its pichashes,
        # bands and block hashes, in a family the export names too
        self.index = MinHashIndex(self._config())
        self.storage = self.index.getStorage()
        sample_entry = self.storage.addSmdaReport(load_report(family="win.existing"))
        assert sample_entry is not None
        self.index.getResultForJob(self.index.updateMinHashesForSample(sample_entry.sample_id, force_recalculation=True))
        export = self.index.getExportData(compress_data=True)
        source_sha256 = sample_entry.sha256
        sample_dict = export["sample_entries"][source_sha256]
        self.export = copy.deepcopy(export)
        # two copies of it: one in a family new here, one in the family it is in
        self.export["family_mapping"] = {"7": "win.added", "8": "win.existing"}
        self.export["sample_entries"] = {
            ADDED_SHA256: dict(sample_dict, sha256=ADDED_SHA256, family_id=7, family="win.added"),
            EXISTING_FAMILY_SHA256: dict(sample_dict, sha256=EXISTING_FAMILY_SHA256, family_id=8, family="win.existing"),
        }
        self.export["function_entries"] = {sha256: export["function_entries"][source_sha256] for sha256 in (ADDED_SHA256, EXISTING_FAMILY_SHA256)}
        self.export = json.loads(json.dumps(self.export))
        self.assertTrue(any(function["minhash"] for function in json.loads(decompress_decode(self.export["function_entries"][ADDED_SHA256])).values()))

    def _snapshot(self):
        storage = self.storage
        return {
            "samples": {sample_id: sample_entry.toDict() for sample_id, sample_entry in storage._samples.items()},
            "sample_by_sha256": dict(storage._sample_by_sha256),
            "functions": {function_id: function_entry.toDict() for function_id, function_entry in storage._functions.items()},
            "function_ids": {sample_id: list(function_ids) for sample_id, function_ids in storage._sample_id_to_function_ids.items() if function_ids},
            "pichashes": {pichash: sorted(matches) for pichash, matches in storage._pichashes.items()},
            "bands": copy.deepcopy(storage._bands),
            "families": {family_id: family_entry.toDict() for family_id, family_entry in storage._families.items()},
        }

    def _failureStages(self):
        storage = self.storage
        error = ValueError("Database insert failed.")
        return {
            "functions refused": patch.object(storage, "importFunctionEntries", side_effect=error),
            "functions stored": patch.object(storage, "importFunctionEntries", side_effect=then_failing(storage.importFunctionEntries, error)),
            "minhashes partly stored": patch.object(storage, "addMinHash", side_effect=failing_after(3, storage.addMinHash, error)),
            "bands partly stored": patch.object(storage, "_addMinHashToBands", side_effect=failing_after(3, storage._addMinHashToBands, error)),
            "minhashes stored": patch.object(storage, "addMinHashes", side_effect=then_failing(storage.addMinHashes, error)),
        }

    def _post_import(self, body):
        request = falcon.Request(falcon.testing.create_environ(path="/import", method="POST", body=body))
        response = falcon.Response()
        StatusResource(self.index).on_post_import(request, response)
        return response

    def testFailedImportLeavesNothingBehind(self):
        for stage, failure in self._failureStages().items():
            with self.subTest(stage=stage):
                before = self._snapshot()
                with failure, self.assertRaisesRegex(ValueError, "Database insert failed"):
                    self.index.addImportData(copy.deepcopy(self.export))
                self.assertEqual(before, self._snapshot())
        # and a retry imports it all, counting the family it created
        report = self.index.addImportData(copy.deepcopy(self.export))
        self.assertEqual((2, 1, 1), (report["num_samples_imported"], report["num_families_imported"], report["num_families_skipped"]))
        storage = self.storage
        for sha256 in (ADDED_SHA256, EXISTING_FAMILY_SHA256):
            sample_entry = storage.getSampleBySha256(sha256)
            assert sample_entry is not None
            for function_entry in storage.getFunctionsBySampleId(sample_entry.sample_id) or []:
                if function_entry.minhash:
                    minhash = function_entry.getMinHash(self._config().MINHASH_CONFIG.MINHASH_SIGNATURE_BITS)
                    self.assertIn(function_entry.function_id, storage.getCandidatesForMinHash(minhash))

    def _laterSampleFailureStages(self, before_failing):
        """Failures from the second sample on, each calling before_failing first"""
        storage = self.storage
        error = ValueError("Database insert failed.")
        return {
            "second sample's functions refused": patch.object(storage, "importFunctionEntries", side_effect=failing_from(2, storage.importFunctionEntries, error, before_failing)),
            "second sample's functions stored": patch.object(
                storage, "importFunctionEntries", side_effect=failing_from(2, storage.importFunctionEntries, error, before_failing, runs_first=True)
            ),
        }

    def testFailureAtALaterSampleTakesBackTheEarlierOnes(self):
        # an import stores each sample with its functions before it reads the next, so when the
        # second sample fails, the first is stored, counted and indexed already, though not banded
        storage = self.storage
        witnessed = []

        def first_sample_is_stored():
            sample_entry = storage.getSampleBySha256(ADDED_SHA256)
            witnessed.append(sample_entry is not None and bool(storage.getFunctionIdsBySampleId(sample_entry.sample_id)))

        for stage, failure in self._laterSampleFailureStages(first_sample_is_stored).items():
            with self.subTest(stage=stage):
                witnessed.clear()
                before = self._snapshot()
                with failure, self.assertRaisesRegex(ValueError, "Database insert failed"):
                    self.index.addImportData(copy.deepcopy(self.export))
                self.assertEqual([True], witnessed[-1:])
                after = self._snapshot()
                if "pichash_counts" in before and not storage.isPicHashCountIndexComplete():
                    # counting failed: the counts are left as they are, the first sample's among
                    # them, unread until rebuilt, as testPartlyCountedPichashesAreNoLongerTrusted
                    self.assertEqual("second sample's pichash counts refused", stage)
                    del before["pichash_counts"], after["pichash_counts"]
                self.assertEqual(before, after)
        report = self.index.addImportData(copy.deepcopy(self.export))
        self.assertEqual((2, 1), (report["num_samples_imported"], report["num_families_imported"]))

    def testFailingRollbackDoesNotHideTheCause(self):
        storage = self.storage
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(storage, "addMinHashes", side_effect=ValueError("Database insert failed.")))
            stack.enter_context(patch.object(storage, "discardImportedSample", side_effect=RuntimeError("discard failed")))
            stack.enter_context(patch.object(storage, "discardImportedFamily", side_effect=RuntimeError("discard failed")))
            with self.assertRaisesRegex(ValueError, "Database insert failed"):
                self.index.addImportData(copy.deepcopy(self.export))

    def testFamilyAnotherWriterFiledASampleUnderIsKept(self):
        storage = self.storage
        other_sha256 = "ef" * 32

        def another_writer_then_failure(function_entries):
            # the family exists from the import's first write on, for every writer
            assert storage.addSmdaReport(load_report(family="win.added", sha256=other_sha256)) is not None
            raise ValueError("Database insert failed.")

        with patch.object(storage, "importFunctionEntries", side_effect=another_writer_then_failure):
            with self.assertRaisesRegex(ValueError, "Database insert failed"):
                self.index.addImportData(copy.deepcopy(self.export))
        family_id = storage.getFamilyId("win.added")
        self.assertIsNotNone(family_id)
        other = storage.getSampleBySha256(other_sha256)
        assert other is not None
        self.assertEqual(family_id, other.family_id)
        self.assertEqual(1, storage.getFamily(family_id).num_samples)
        self.assertIsNone(storage.getSampleBySha256(ADDED_SHA256))

    def testOnlyFamiliesTheImportCreatedAreCounted(self):
        storage = self.storage
        storage.addFamily("win.added")
        report = self.index.addImportData(copy.deepcopy(self.export))
        self.assertEqual((0, 2), (report["num_families_imported"], report["num_families_skipped"]))

    def testEnsureFamilySaysWhetherItAdded(self):
        storage = self.storage
        family_id, is_created = storage.ensureFamily("win.new")
        self.assertTrue(is_created)
        self.assertEqual((family_id, False), storage.ensureFamily("win.new"))
        self.assertEqual(family_id, storage.addFamily("win.new"))

    def testExportTheStorageCannotHoldIsRefused(self):
        # MongoDB failed these at insert, with a 500 after part of the import was written, while
        # memory storage kept them: both refuse them now, before storing anything
        def functions_of(export):
            for sha256, encoded in export["function_entries"].items():
                function_entries = json.loads(decompress_decode(encoded))
                yield from function_entries.values()
                # written back after the caller changed them
                export["function_entries"][sha256] = compress_encode(json.dumps(function_entries))

        def first_sample(export):
            return export["sample_entries"][ADDED_SHA256]

        def binary_size_beyond_int64(export):
            first_sample(export)["binary_size"] = 2**63

        def function_count_beyond_int64(export):
            first_sample(export)["statistics"]["num_functions"] = 2**64

        def statistic_beyond_int64(export):
            first_sample(export)["statistics"]["num_instructions"] = -(2**63) - 1

        def version_beyond_int64(export):
            first_sample(export)["version"] = 2**70

        def base_address_beyond_uint64(export):
            first_sample(export)["base_addr"] = 2**64

        def num_blocks_beyond_int64(export):
            for function_entry in functions_of(export):
                function_entry["num_blocks"] = 2**63

        def matches_beyond_int64(export):
            for function_entry in functions_of(export):
                function_entry["matches"] = {"1": [2**63]}

        def offset_beyond_uint64(export):
            for function_entry in functions_of(export):
                function_entry["offset"] = 2**64

        def pichash_beyond_uint64(export):
            for function_entry in functions_of(export):
                function_entry["pichash"] = 2**64

        def pichash_negative(export):
            for function_entry in functions_of(export):
                function_entry["pichash"] = -1

        def statistic_key_with_nul(export):
            first_sample(export)["statistics"]["num\x00instructions"] = 1

        def matches_key_with_nul(export):
            for function_entry in functions_of(export):
                function_entry["matches"] = {"1\x00": []}

        def composition_key_with_nul(export):
            for function_entry in functions_of(export):
                function_entry["minhash_shingle_composition"] = {"\x00": 1}

        def picblockhash_key_with_nul(export):
            for function_entry in functions_of(export):
                function_entry["picblockhashes"] = [{"hash": 1, "offset": 0, "len\x00gth": 4}]

        def picblockhashes_a_number(export):
            for function_entry in functions_of(export):
                function_entry["picblockhashes"] = 5

        def picblockhash_not_a_mapping(export):
            for function_entry in functions_of(export):
                function_entry["picblockhashes"] = [5]

        def picblockhash_hash_text(export):
            for function_entry in functions_of(export):
                function_entry["picblockhashes"] = [{"hash": "x", "offset": 0}]

        def picblockhash_hash_beyond_uint64(export):
            for function_entry in functions_of(export):
                function_entry["picblockhashes"] = [{"hash": 2**64, "offset": 0}]

        def picblockhash_without_offset(export):
            for function_entry in functions_of(export):
                function_entry["picblockhashes"] = [{"hash": 1}]

        def picblockhash_length_beyond_int64(export):
            for function_entry in functions_of(export):
                function_entry["picblockhashes"] = [{"hash": 1, "offset": 0, "length": 2**63}]

        def label_beyond_int64(export):
            for function_entry in functions_of(export):
                function_entry["function_labels"] = [{"function_label": 2**63, "username": "user", "timestamp": "2020-01-01T00:00:00"}]

        for malform in (
            binary_size_beyond_int64,
            function_count_beyond_int64,
            statistic_beyond_int64,
            version_beyond_int64,
            base_address_beyond_uint64,
            num_blocks_beyond_int64,
            matches_beyond_int64,
            offset_beyond_uint64,
            pichash_beyond_uint64,
            pichash_negative,
            statistic_key_with_nul,
            matches_key_with_nul,
            composition_key_with_nul,
            picblockhash_key_with_nul,
            picblockhashes_a_number,
            picblockhash_not_a_mapping,
            picblockhash_hash_text,
            picblockhash_hash_beyond_uint64,
            picblockhash_without_offset,
            picblockhash_length_beyond_int64,
            label_beyond_int64,
        ):
            with self.subTest(malform=malform.__name__):
                export = copy.deepcopy(self.export)
                malform(export)
                before = self._snapshot()
                response = self._post_import(json.dumps(export))
                self.assertEqual(falcon.HTTP_400, response.status)
                self.assertIn("Import rejected", json.loads(response.data)["data"]["message"])
                self.assertEqual(before, self._snapshot())
        # the extremes that fit are stored
        export = copy.deepcopy(self.export)
        first_sample(export)["binary_size"] = 2**63 - 1
        first_sample(export)["base_addr"] = -(2**63)
        for function_entry in functions_of(export):
            function_entry["pichash"] = 2**64 - 1
            function_entry["picblockhashes"] = [{"hash": 2**64 - 1, "offset": 2**64 - 1, "length": -(2**63)}]
        response = self._post_import(json.dumps(export))
        self.assertEqual(falcon.HTTP_200, response.status)
        sample_entry = self.storage.getSampleBySha256(ADDED_SHA256)
        assert sample_entry is not None
        self.assertEqual((2**63 - 1, 2**63), (sample_entry.binary_size, sample_entry.base_addr))
        function_entry = (self.storage.getFunctionsBySampleId(sample_entry.sample_id) or [])[0]
        self.assertEqual((2**64 - 1, [{"hash": 2**64 - 1, "offset": 2**64 - 1, "length": -(2**63)}]), (function_entry.pichash, function_entry.picblockhashes))

    def testSampleFailingAfterItsInsertIsRemoved(self):
        # importSampleEntry stored the second sample and then failed to count it, so the import never
        # learnt of it to remove it again
        storage = self.storage
        update_family_stats = storage._updateFamilyStats
        num_counted = [0]

        def failing_for_the_second_sample(family_id, num_samples_inc, *args):
            if num_samples_inc > 0:
                num_counted[0] += 1
                if num_counted[0] == 2:
                    raise ValueError("Database update failed.")
            return update_family_stats(family_id, num_samples_inc, *args)

        before = self._snapshot()
        with patch.object(storage, "_updateFamilyStats", side_effect=failing_for_the_second_sample):
            with self.assertRaisesRegex(ValueError, "Database update failed"):
                self.index.addImportData(copy.deepcopy(self.export))
        self.assertEqual(before, self._snapshot())

    def testFunctionCountOverflowingItsFamilyLeavesNothingBehind(self):
        # a count that fits 64 bit, but not added to the family's: MongoDB refuses the increment once
        # the sample is stored, while memory storage's counters do not overflow
        export = copy.deepcopy(self.export)
        export["sample_entries"][EXISTING_FAMILY_SHA256]["statistics"]["num_functions"] = 2**63 - 1
        before = self._snapshot()
        if self.COUNTERS_OVERFLOW:
            with self.assertRaises(Exception):
                self.index.addImportData(export)
            self.assertEqual(before, self._snapshot())
        else:
            self.assertEqual(2, self.index.addImportData(export)["num_samples_imported"])

    def testNullPichashIsImportedAsZero(self):
        # as FunctionEntry stores a function SMDA reported without one; MongoDB storage failed it in hex()
        export = copy.deepcopy(self.export)
        for sha256, encoded in export["function_entries"].items():
            function_entries = json.loads(decompress_decode(encoded))
            for function_entry in function_entries.values():
                function_entry["pichash"] = None
            export["function_entries"][sha256] = compress_encode(json.dumps(function_entries))
        self.assertEqual(2, self.index.addImportData(export)["num_samples_imported"])
        sample_entry = self.storage.getSampleBySha256(ADDED_SHA256)
        assert sample_entry is not None
        self.assertEqual({0}, {function_entry.pichash for function_entry in self.storage.getFunctionsBySampleId(sample_entry.sample_id) or []})


class StorageInterfaceDefaultTest(unittest.TestCase):
    def testEnsureFamilyOfAnotherBackendLooksUpAndAdds(self):
        class Backend(StorageInterface):
            def __init__(self):
                self.families: Dict[str, int] = {}

            def getFamilyId(self, family_name: str) -> Optional[int]:
                return self.families.get(family_name)

            def addFamily(self, family_name: str) -> int:
                return self.families.setdefault(family_name, len(self.families))

        backend = Backend()
        self.assertEqual((0, True), backend.ensureFamily("win.a"))
        self.assertEqual((0, False), backend.ensureFamily("win.a"))
        self.assertEqual((1, True), backend.ensureFamily("win.b"))


def mongo_config(db_name):
    server, port = getTestMongoServerAndPort()
    mcrit_config = McritConfig()
    mcrit_config.STORAGE_CONFIG = StorageConfig(
        STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB, STORAGE_SERVER=server, STORAGE_PORT=port, STORAGE_MONGODB_DBNAME=db_name, STORAGE_DROP_DISASSEMBLY=False
    )
    mcrit_config.MINHASH_CONFIG = config.MINHASH_CONFIG
    mcrit_config.SHINGLER_CONFIG = config.SHINGLER_CONFIG
    mcrit_config.QUEUE_CONFIG = config.QUEUE_CONFIG
    return mcrit_config


@pytest.mark.mongo
class MongoImportRollbackTest(MemoryImportRollbackTest):
    DB_NAME = f"test_import_rollback_{os.getpid()}"
    COUNTERS_OVERFLOW = True

    def _config(self):
        return mongo_config(self.DB_NAME)

    def setUp(self):
        server, port = getTestMongoServerAndPort()
        self.client = pymongo.MongoClient(server, int(port))
        self.client.drop_database(self.DB_NAME)
        super().setUp()
        # a fresh database vouches for the indexes it maintains, so the import has to keep them right
        self.assertTrue(self.storage._isPicBlockHashIndexComplete() and self.storage.isPicHashCountIndexComplete() and self.storage.isFunctionRangeIndexComplete())

    def tearDown(self):
        self.client.drop_database(self.DB_NAME)
        self.client.close()

    def _snapshot(self):
        db = self.storage._getDb()
        collections = ["samples", "families", "functions", "xcfg", "picblockhashes", "pichash_counts", "function_ranges"]
        collections += ["band_%d" % band_number for band_number in range(self._config().STORAGE_CONFIG.STORAGE_NUM_BANDS)]
        snapshot = {}
        for collection in collections:
            documents = [{key: value for key, value in document.items() if not isinstance(value, ObjectId)} for document in db[collection].find()]
            snapshot[collection] = sorted(documents, key=repr)
        return snapshot

    def _failureStages(self):
        storage = self.storage
        error = ValueError("Database insert failed.")
        update_bands = storage._updateBands

        def some_bands_then_failure(band_hashes, method="push"):
            if method != "push":
                return update_bands(band_hashes, method=method)
            update_bands({0: band_hashes[0]})
            raise error

        def failing_for_functions(function):
            def wrapper(collection, data):
                if collection == "functions":
                    raise error
                return function(collection, data)

            return wrapper

        return {
            # the disassembly is written before the functions
            "functions refused": patch.object(storage, "_dbInsertMany", side_effect=failing_for_functions(storage._dbInsertMany)),
            "picblockhash index refused": patch.object(storage, "_addToPicBlockHashIndex", side_effect=error),
            "picblockhash index stored": patch.object(storage, "_addToPicBlockHashIndex", side_effect=then_failing(storage._addToPicBlockHashIndex, error)),
            "function ranges refused": patch.object(storage, "_recordFunctionRange", side_effect=error),
            "bands refused": patch.object(storage, "_addMinHashesToBands", side_effect=error),
            "bands partly stored": patch.object(storage, "_updateBands", side_effect=some_bands_then_failure),
            "minhashes stored": patch.object(storage, "addMinHashes", side_effect=then_failing(storage.addMinHashes, error)),
            # last: the counts are no longer kept once counting failed
            "pichash counts refused": patch.object(storage, "_addToPicHashCounts", side_effect=error),
        }

    def _laterSampleFailureStages(self, before_failing):
        storage = self.storage
        error = ValueError("Database insert failed.")
        insert_many = storage._dbInsertMany
        insert_functions = failing_from(2, lambda data: insert_many("functions", data), error, before_failing)

        def failing_for_the_second_sample_functions(collection, data):
            if collection == "functions":
                return insert_functions(data)
            return insert_many(collection, data)

        return {
            **super()._laterSampleFailureStages(before_failing),
            "second sample's functions refused by MongoDB": patch.object(storage, "_dbInsertMany", side_effect=failing_for_the_second_sample_functions),
            "second sample's picblockhash index refused": patch.object(
                storage, "_addToPicBlockHashIndex", side_effect=failing_from(2, storage._addToPicBlockHashIndex, error, before_failing)
            ),
            "second sample's function range refused": patch.object(
                storage, "_recordFunctionRange", side_effect=failing_from(2, storage._recordFunctionRange, error, before_failing)
            ),
            # last: the counts are no longer kept once counting failed
            "second sample's pichash counts refused": patch.object(storage, "_addToPicHashCounts", side_effect=failing_from(2, storage._addToPicHashCounts, error, before_failing)),
        }

    def testPartlyCountedPichashesAreNoLongerTrusted(self):
        # counting failed after some or all of the counts were written: they may be off now
        storage = self.storage
        before = self._snapshot()
        with patch.object(storage, "_addToPicHashCounts", side_effect=then_failing(storage._addToPicHashCounts, ValueError("Database insert failed."))):
            with self.assertRaisesRegex(ValueError, "Database insert failed"):
                self.index.addImportData(copy.deepcopy(self.export))
        self.assertFalse(storage.isPicHashCountIndexComplete())
        # everything else as it was; the counts as counting left them, unread until rebuilt
        after = self._snapshot()
        del before["pichash_counts"], after["pichash_counts"]
        self.assertEqual(before, after)

    def testCountsLeftByAFailedImportAreNotTrustedAgainOnAnEmptyDatabase(self):
        # an import into a database without functions fails counting its second sample: the first
        # sample's counts stay behind, untrusted, with no function left to hold them. The next start
        # vouches for the counts of an empty database, so it must not vouch for those
        storage = self.storage
        source = storage.getSampleBySha256(load_report().sha256)
        assert source is not None
        self.assertTrue(storage.deleteSample(source.sample_id))
        self.assertEqual(0, storage._getDb().functions.count_documents({}))
        failure = failing_from(2, storage._addToPicHashCounts, ValueError("Database insert failed."), lambda: None)
        with patch.object(storage, "_addToPicHashCounts", side_effect=failure):
            with self.assertRaisesRegex(ValueError, "Database insert failed"):
                self.index.addImportData(copy.deepcopy(self.export))
        db = storage._getDb()
        self.assertEqual(0, db.functions.count_documents({}))
        self.assertGreater(db.pichash_counts.count_documents({}), 0)
        self.assertFalse(storage.isPicHashCountIndexComplete())

        def counts():
            return sorted((document["_pichash"], document["df"]) for document in db.pichash_counts.find({}, {"_id": 0}))

        restarted = MinHashIndex(self._config())
        self.assertTrue(restarted.getStorage().isPicHashCountIndexComplete())
        self.assertEqual(2, restarted.addImportData(copy.deepcopy(self.export))["num_samples_imported"])
        imported = counts()
        # as a clean import counts them, which a rebuild from the functions reproduces
        restarted.getStorage().rebuildPicHashCountIndex()
        self.assertEqual(counts(), imported)

    def testWriterRestoresTheFamilyAFailedImportRemovedBeforeItFiled(self):
        # the rollback of a failed import removed the family between another writer's lookup and filing
        storage = self.storage
        add_family = storage.addFamily

        def removed_after_the_lookup(family_name):
            family_id = add_family(family_name)
            storage._getDb().families.delete_one({"family_id": family_id})
            return family_id

        with patch.object(storage, "addFamily", side_effect=removed_after_the_lookup):
            sample_entry = storage.addSmdaReport(load_report(family="win.raced", sha256="ef" * 32))
        assert sample_entry is not None
        family = storage.getFamily(sample_entry.family_id)
        assert family is not None
        self.assertEqual(("win.raced", 1, 10), (family.family_name, family.num_samples, family.num_functions))
        with patch.object(storage, "addFamily", side_effect=removed_after_the_lookup):
            self.assertTrue(storage.modifySample(sample_entry.sample_id, {"family_name": "win.moved"}))
        moved = storage.getSampleById(sample_entry.sample_id)
        assert moved is not None
        family = storage.getFamily(moved.family_id)
        assert family is not None
        self.assertEqual(("win.moved", 1, 10), (family.family_name, family.num_samples, family.num_functions))

    def testReportFailingToFileItsSampleCanBeSubmittedAgain(self):
        # a sample left without its functions would answer every resubmission that it exists already
        storage = self.storage
        db = storage._getDb()
        error = ValueError("Database insert failed.")
        failures = {
            "_ensureFamilyDocument": error,
            # its chunks written, then the failure
            "_insertSmdaExtras": then_failing(storage._insertSmdaExtras, error),
        }
        for number, (step, failure) in enumerate(failures.items()):
            with self.subTest(step=step):
                sha256 = "e%d" % number * 32
                with patch.object(storage, step, side_effect=failure):
                    with self.assertRaisesRegex(ValueError, "Database insert failed"):
                        storage.addSmdaReport(load_report(family="win.resubmitted", sha256=sha256))
                self.assertIsNone(storage.getSampleBySha256(sha256))
                self.assertEqual(0, db.smda_extras.count_documents({"sample_id": {"$nin": storage.getSampleIds()}}))
                sample_entry = storage.addSmdaReport(load_report(family="win.resubmitted", sha256=sha256))
                assert sample_entry is not None
                self.assertEqual(10, len(storage.getFunctionIdsBySampleId(sample_entry.sample_id) or []))
                self.assertIsNotNone(storage.getSmdaExtras(sample_entry.sample_id))
        family = storage.getFamily(storage.getFamilyId("win.resubmitted"))
        assert family is not None
        self.assertEqual((2, 20), (family.num_samples, family.num_functions))

    def testFamilyAWriterFiledUnderWhileTheRollbackDeletedItIsRestored(self):
        storage = self.storage
        family_id, _ = storage.ensureFamily("win.raced")
        find_one_and_delete = Collection.find_one_and_delete

        def a_writer_files_first(collection, *args, **kwargs):
            if collection.name == "families":
                storage._getDb().samples.insert_one({"sample_id": 10**6, "sha256": "ef" * 32, "family_id": family_id})
            return find_one_and_delete(collection, *args, **kwargs)

        with patch.object(Collection, "find_one_and_delete", autospec=True, side_effect=a_writer_files_first):
            self.assertFalse(storage.discardImportedFamily(family_id))
        family = storage.getFamily(family_id)
        assert family is not None
        self.assertEqual("win.raced", family.family_name)

    def testFamilyAnotherWriterAddedConcurrentlyIsNeitherCountedNorRemoved(self):
        # another writer adds the family between the import's lookup and its upsert
        storage = self.storage
        other_writer = MongoDbStorage(self._config())
        use_counter = storage._useCounter

        def another_writer_first(name):
            if name == "families" and other_writer.getFamilyId("win.added") is None:
                other_writer.addFamily("win.added")
            return use_counter(name)

        with patch.object(storage, "_useCounter", side_effect=another_writer_first):
            family_id, is_created = storage.ensureFamily("win.added")
        self.assertEqual((other_writer.getFamilyId("win.added"), False), (family_id, is_created))
        with patch.object(storage, "addMinHashes", side_effect=ValueError("Database insert failed.")):
            with self.assertRaises(ValueError):
                self.index.addImportData(copy.deepcopy(self.export))
        self.assertEqual(family_id, storage.getFamilyId("win.added"))
        self.assertEqual(1, storage._getDb().families.count_documents({"family_name": "win.added"}))

    def testOfConcurrentWritersAddingOneFamilyOnlyOneIsToldItDid(self):
        storages = [MongoDbStorage(self._config()) for _ in range(6)]
        names = ["win.concurrent_%d" % number for number in range(20)]
        results = []
        barrier = threading.Barrier(len(storages))

        def add(storage):
            barrier.wait()
            for name in names:
                results.append((name, storage.ensureFamily(name)))

        threads = [threading.Thread(target=add, args=(storage,)) for storage in storages]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        for name in names:
            outcomes = [outcome for result_name, outcome in results if result_name == name]
            self.assertEqual(1, sum(is_created for _, is_created in outcomes), name)
            self.assertEqual(1, len({family_id for family_id, _ in outcomes}), name)
            self.assertEqual(1, self.storage._getDb().families.count_documents({"family_name": name}), name)

    def testFamilyNamesAreUniqueOnAFreshDatabase(self):
        self.assertTrue(self.storage._getDb().families.index_information()["family_name_1"].get("unique"))
        with self.assertRaises(DuplicateKeyError):
            self.storage._getDb().families.insert_one({"family_id": 999, "family_name": "win.existing"})

    def testDatabaseHoldingDuplicateNamesKeepsWorkingWithoutTheIndex(self):
        # as an older instance left it: the index without the constraint, and a name held twice
        families = self.storage._getDb().families
        families.drop_index("family_name_1")
        families.create_index("family_name")
        families.insert_one({"family_id": 999, "family_name": "win.existing", "num_samples": 0, "num_functions": 0, "num_library_samples": 0})
        logging.disable(logging.NOTSET)
        try:
            with self.assertLogs("mcrit.storage.MongoDbStorage", level="ERROR") as logs:
                storage = MongoDbStorage(self._config())
                storage._getDb()
        finally:
            logging.disable(logging.CRITICAL)
        self.assertEqual(1, len(logs.records))
        self.assertIn("'win.existing'", logs.output[0])
        self.assertFalse(families.index_information()["family_name_1"].get("unique"))
        # nothing is merged, and families are still added and found
        self.assertEqual(2, families.count_documents({"family_name": "win.existing"}))
        family_id, is_created = storage.ensureFamily("win.new")
        self.assertTrue(is_created)
        self.assertEqual((family_id, False), storage.ensureFamily("win.new"))
        self.assertFalse(storage.ensureFamily("win.existing")[1])
        self.assertEqual(2, families.count_documents({"family_name": "win.existing"}))
        # once the duplicate is resolved, the next start creates the index
        families.delete_one({"family_id": 999})
        storage = MongoDbStorage(self._config())
        storage._getDb()
        self.assertTrue(families.index_information()["family_name_1"].get("unique"))

    def testBlockHashIndexIsUpdatedPerSampleInOnePass(self):
        # the same updates in the same order, with the documents of several samples interleaved (#300)
        storage = self.storage
        held = [(sample_id, "0x%x" % (index % 7)) for index, sample_id in enumerate([3, 1, 2, 3, 1, 2, 3, 1])]
        documents: List[Dict[str, Any]] = [{"sample_id": sample_id, "_picblockhashes": [{"hash": block_hash}]} for sample_id, block_hash in held]
        documents.append({"sample_id": 4, "_picblockhashes": []})
        # by sample, and by hash within one
        expected = sorted(set(held))
        with patch.object(Collection, "bulk_write", autospec=True) as bulk_write:
            self.assertEqual(len(expected), storage._addToPicBlockHashIndex(documents))
        updates = bulk_write.call_args[0][1]
        self.assertEqual(expected, [(update._doc["$addToSet"]["sample_ids"], update._filter["_id"]) for update in updates])


if __name__ == "__main__":
    unittest.main()
