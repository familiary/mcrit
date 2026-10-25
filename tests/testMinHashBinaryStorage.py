"""Minhashes are stored as BSON binary, and the hex text an earlier MCRIT stored is still read.

The raw `functions` documents are inspected and rewritten directly, so these tests pin what is on
disk and not only what the storage hands back. "" stays the marker of a function not hashed yet.
"""

import io
import json
import logging
import os
import sys
import unittest
from contextlib import redirect_stdout
from copy import deepcopy
from datetime import datetime
from unittest.mock import patch

import pymongo
import pytest
from bson import Binary
from smda.common.SmdaReport import SmdaReport

from mcrit.config.McritConfig import McritConfig
from mcrit.config.MinHashConfig import MinHashConfig
from mcrit.config.QueueConfig import QueueConfig
from mcrit.config.ShinglerConfig import ShinglerConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.libs.mongoqueue import DEFAULT_INSERT
from mcrit.matchers.MatcherSample import MatcherSample
from mcrit.migrations import migrate_minhash_binary
from mcrit.minhash.MinHash import MinHash
from mcrit.queue.QueueFactory import QueueFactory
from mcrit.storage.MongoDbStorage import MongoDbStorage
from mcrit.storage.StorageFactory import StorageFactory

from .context import getTestMongoServerAndPort

logging.disable(logging.CRITICAL)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
REPORTS = ["example_report.smda", "example_report_2.smda", "example_report_3.smda"]
DB_NAME = "test_minhash_binary_storage"
IMPORT_DB_NAME = "test_minhash_binary_storage_import"


def loadReport(name):
    with open(os.path.join(PROJECT_ROOT, "tests", name)) as handle:
        report = SmdaReport.fromDict(json.load(handle))
    assert report is not None
    return report


def buildConfig(db_name):
    server, port = getTestMongoServerAndPort()
    config = McritConfig()
    config.STORAGE_CONFIG = StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB, STORAGE_SERVER=server, STORAGE_PORT=port, STORAGE_MONGODB_DBNAME=db_name)
    config.MINHASH_CONFIG = MinHashConfig()
    config.SHINGLER_CONFIG = ShinglerConfig()
    config.QUEUE_CONFIG = QueueConfig(QUEUE_METHOD=QueueFactory.QUEUE_METHOD_FAKE)
    return config


def comparableReport(report):
    stripped = deepcopy(report)
    stripped["info"].pop("job", None)
    return stripped


class MinHashUnitTest(unittest.TestCase):
    """The conversions on their own, without a database."""

    SIGNATURE = bytes(range(16))

    def test_from_storage_reads_every_form_a_document_can_hold(self):
        self.assertEqual(self.SIGNATURE, MongoDbStorage._minHashFromStorage(self.SIGNATURE.hex()))
        self.assertEqual(self.SIGNATURE, MongoDbStorage._minHashFromStorage(self.SIGNATURE))
        self.assertEqual(self.SIGNATURE, MongoDbStorage._minHashFromStorage(Binary(self.SIGNATURE)))
        self.assertEqual(b"", MongoDbStorage._minHashFromStorage(""))
        self.assertEqual(b"", MongoDbStorage._minHashFromStorage(None))

    def test_from_storage_hands_out_plain_bytes(self):
        self.assertIs(bytes, type(MongoDbStorage._minHashFromStorage(Binary(self.SIGNATURE))))
        self.assertIs(bytes, type(MongoDbStorage._minHashFromStorage(self.SIGNATURE.hex())))

    def test_for_storage_keeps_a_signature_as_bytes(self):
        stored = MongoDbStorage._minHashForStorage(self.SIGNATURE)
        self.assertIs(bytes, type(stored))
        self.assertEqual(self.SIGNATURE, stored)

    def test_for_storage_marks_an_empty_signature_as_not_hashed(self):
        # an empty binary would not match {"minhash": ""}, which getUnhashedFunctions queries
        self.assertEqual("", MongoDbStorage._minHashForStorage(b""))
        self.assertIs(str, type(MongoDbStorage._minHashForStorage(b"")))

    def test_the_uri_is_built_as_mcrit_builds_it(self):
        build = MongoDbStorage.buildMongoUri
        self.assertEqual("mongodb://h:27999/db", build("h", "27999", "db"))
        # credentials go in as configured: a password holding "@" has to be configured as "%40"
        # for MCRIT to connect at all, and quoting it again would send "%2540"
        self.assertEqual("mongodb://u:s3cr%40t@h:27999/db?authSource=admin", build("h", "27999", "db", "u", "s3cr%40t", "authSource=admin"))
        # only both together are credentials, and an empty port is how a host list is given
        self.assertEqual("mongodb://h:27999/db", build("h", "27999", "db", "u", ""))
        self.assertEqual("mongodb://a:1,b:2/db?replicaSet=rs", build("a:1,b:2", "", "db", flags="replicaSet=rs"))

    def test_mcrit_connects_with_the_uri_the_migration_builds(self):
        # what keeps "connects the way MCRIT does" true: MongoDbStorage itself goes through
        # buildMongoUri, credentials, flags and an empty port included
        storage_config = StorageConfig(STORAGE_SERVER="a:1,b:2", STORAGE_PORT="", STORAGE_MONGODB_DBNAME="db")
        storage_config.STORAGE_MONGODB_USERNAME = "u"
        storage_config.STORAGE_MONGODB_PASSWORD = "s3cr%40t"
        storage_config.STORAGE_MONGODB_FLAGS = "authSource=admin"
        config = McritConfig()
        config.STORAGE_CONFIG = storage_config
        storage = MongoDbStorage(config)
        with (
            patch("mcrit.storage.MongoDbStorage.MongoClient") as client,
            patch.object(MongoDbStorage, "_ensureIndexAndUnknownFamily"),
        ):
            storage._getDb()
        self.assertEqual("mongodb://u:s3cr%40t@a:1,b:2/db?authSource=admin", client.call_args.args[0])
        self.assertEqual(MongoDbStorage.buildMongoUri("a:1,b:2", "", "db", "u", "s3cr%40t", "authSource=admin"), client.call_args.args[0])

    def _runMigrationMain(self, argv, **storage_values):
        """main() with the given storage config; returns (uri connected to, database name used).

        Port 27999: the clients are never used, but the test guard refuses any client to 27017.
        """
        storage_config = StorageConfig(STORAGE_SERVER="h", STORAGE_PORT="27999", STORAGE_MONGODB_DBNAME="db")
        for key, value in storage_values.items():
            setattr(storage_config, key, value)
        config = McritConfig()
        config.STORAGE_CONFIG = storage_config
        uris, databases = [], []

        def client_for(uri, connect=True):
            uris.append(uri)
            return pymongo.MongoClient(uri, connect=False)

        def record_count(db):
            databases.append(db.name)
            db.client.close()
            return {}

        with (
            patch.object(migrate_minhash_binary, "McritConfig", return_value=config),
            patch.object(migrate_minhash_binary, "MongoClient", side_effect=client_for),
            patch.object(migrate_minhash_binary, "count", side_effect=record_count),
        ):
            self.assertEqual(0, migrate_minhash_binary.main(argv))
        return uris[0], databases[0]

    def test_the_migration_connects_as_mcrit_does(self):
        # a deployment that requires a login needs nothing beyond its mcrit configuration
        credentials = {"STORAGE_MONGODB_USERNAME": "u", "STORAGE_MONGODB_PASSWORD": "s3cr%40t", "STORAGE_MONGODB_FLAGS": "authSource=admin"}
        self.assertEqual(
            ("mongodb://u:s3cr%40t@h:27999/db?authSource=admin", "db"),
            self._runMigrationMain(["--mode", "count"], **credentials),
        )
        self.assertEqual(MongoDbStorage.buildMongoUri("h", "27999", "db", "u", "s3cr%40t", "authSource=admin"), self._runMigrationMain(["--mode", "count"], **credentials)[0])
        # a host list has no port of its own
        self.assertEqual(
            ("mongodb://a:1,b:2/db?replicaSet=rs", "db"),
            self._runMigrationMain(["--mode", "count"], STORAGE_SERVER="a:1,b:2", STORAGE_PORT="", STORAGE_MONGODB_FLAGS="replicaSet=rs"),
        )

    def test_a_uri_names_the_database_unless_db_does(self):
        self.assertEqual(("mongodb://other:1/x", "x"), self._runMigrationMain(["--mode", "count", "--uri", "mongodb://other:1/x"]))
        self.assertEqual(("mongodb://other:1/x", "y"), self._runMigrationMain(["--mode", "count", "--uri", "mongodb://other:1/x", "--db", "y"]))
        # a URI without a database falls back to the configured one
        self.assertEqual(("mongodb://other:1", "db"), self._runMigrationMain(["--mode", "count", "--uri", "mongodb://other:1"]))
        # --db alone migrates that database, and authenticates against it as MCRIT would
        self.assertEqual(("mongodb://h:27999/z", "z"), self._runMigrationMain(["--mode", "count", "--db", "z"]))

    def test_encode_turns_the_hex_of_a_function_dict_into_binary(self):
        function_dict = {"function_id": 1, "minhash": self.SIGNATURE.hex()}
        MongoDbStorage._encodeMinHash(function_dict)
        self.assertIs(bytes, type(function_dict["minhash"]))
        self.assertEqual(self.SIGNATURE, function_dict["minhash"])

    def test_encode_leaves_unhashed_binary_and_absent_minhashes_alone(self):
        unhashed = {"minhash": ""}
        MongoDbStorage._encodeMinHash(unhashed)
        self.assertEqual({"minhash": ""}, unhashed)
        binary = {"minhash": self.SIGNATURE}
        MongoDbStorage._encodeMinHash(binary)
        self.assertEqual({"minhash": self.SIGNATURE}, binary)
        absent = {"function_id": 1}
        MongoDbStorage._encodeMinHash(absent)
        self.assertEqual({"function_id": 1}, absent)

    def test_decode_gives_the_hex_function_entry_reads(self):
        for stored in (self.SIGNATURE, Binary(self.SIGNATURE), self.SIGNATURE.hex()):
            function_dict = {"minhash": stored}
            MongoDbStorage._decodeMinHash(function_dict)
            self.assertEqual({"minhash": self.SIGNATURE.hex()}, function_dict)

    def test_decode_keeps_the_unhashed_marker_and_absent_minhashes(self):
        for stored in ("", None):
            function_dict = {"minhash": stored}
            MongoDbStorage._decodeMinHash(function_dict)
            self.assertEqual({"minhash": ""}, function_dict)
        absent = {"function_id": 1}
        MongoDbStorage._decodeMinHash(absent)
        self.assertEqual({"function_id": 1}, absent)

    def test_encode_then_decode_is_the_identity_on_hex(self):
        function_dict = {"minhash": self.SIGNATURE.hex()}
        MongoDbStorage._encodeMinHash(function_dict)
        MongoDbStorage._decodeMinHash(function_dict)
        self.assertEqual({"minhash": self.SIGNATURE.hex()}, function_dict)


@pytest.mark.mongo
class MinHashBinaryStorageTest(unittest.TestCase):
    def setUp(self):
        server, port = getTestMongoServerAndPort()
        self.client = pymongo.MongoClient(server, int(port))
        # cleanups run last-in first-out: the client has to outlive the drops
        self.addCleanup(self.client.close)
        for name in (DB_NAME, IMPORT_DB_NAME):
            self.client.drop_database(name)
            self.addCleanup(self.client.drop_database, name)
        self.config = buildConfig(DB_NAME)
        self.signature_bytes = self.config.MINHASH_CONFIG.MINHASH_SIGNATURE_LENGTH * self.config.MINHASH_CONFIG.MINHASH_SIGNATURE_BITS // 8
        self.index = self._indexReports(self.config)
        self.storage = self.index._storage
        self.functions = self.storage._getDb().functions
        self.sample_ids = sorted(self.storage.getSampleIds())
        self.hashed_ids = self._hashedIds()
        self.assertGreater(len(self.sample_ids), 1)
        self.assertGreater(len(self.hashed_ids), 14, "too few hashed functions to page and mix")
        self.assertGreater(self.functions.count_documents({"minhash": ""}), 0, "the fixtures hold no function too small to be hashed")

    def _indexReports(self, config):
        index = MinHashIndex(config=config)
        index._storage.clearStorage()
        for name in REPORTS:
            entry = index._storage.addSmdaReport(loadReport(name))
            assert entry is not None
            index.queue._worker.updateMinHashesForSample(entry.sample_id)
        return index

    # raw access to the stored documents

    def _rawMinHashes(self, functions=None):
        return {
            document["function_id"]: document["minhash"] for document in (self.functions if functions is None else functions).find({}, {"_id": 0, "function_id": 1, "minhash": 1})
        }

    def _hashedIds(self):
        return sorted(function_id for function_id, stored in self._rawMinHashes().items() if stored != "")

    def _binaryReference(self):
        """function_id -> signature bytes, for the hashed functions, as the storage wrote them."""
        reference = {function_id: bytes(stored) for function_id, stored in self._rawMinHashes().items() if stored != ""}
        self.assertEqual(set(self.hashed_ids), set(reference))
        return reference

    def _setRaw(self, function_id, value):
        self.functions.update_one({"function_id": function_id}, {"$set": {"minhash": value}})

    def _rewriteAsHex(self, function_ids):
        for function_id in function_ids:
            stored = self.functions.find_one({"function_id": function_id})["minhash"]
            if not isinstance(stored, str):
                self._setRaw(function_id, bytes(stored).hex())

    def _matchingMinHashes(self, function_ids):
        cache = self.storage.createMatchingCache(function_ids)
        return dict(cache._func_id_to_minhash)

    def _bandContents(self):
        """band_hash -> sorted function_ids per band collection, whatever bucket holds them."""
        contents = {}
        for band_number in range(self.config.STORAGE_CONFIG.STORAGE_NUM_BANDS):
            per_hash = {}
            for document in self.storage._getDb()["band_%d" % band_number].find({}):
                per_hash.setdefault(document["band_hash"], []).extend(document.get("function_ids", []))
            contents[band_number] = {band_hash: sorted(function_ids) for band_hash, function_ids in per_hash.items()}
        return contents

    def _functionsInBands(self, function_ids):
        return sum(
            self.storage._getDb()["band_%d" % band_number].count_documents({"function_ids": {"$in": list(function_ids)}})
            for band_number in range(self.config.STORAGE_CONFIG.STORAGE_NUM_BANDS)
        )

    # what is stored

    def test_hashed_functions_are_stored_as_binary_of_the_signature_length(self):
        for function_id, stored in self._rawMinHashes().items():
            if stored == "":
                continue
            self.assertIs(bytes, type(stored), function_id)
            self.assertEqual(self.signature_bytes, len(stored), function_id)
        # the server sees BSON binary and no text, whatever the driver decodes it to
        self.assertEqual(len(self.hashed_ids), self.functions.count_documents({"minhash": {"$type": "binData"}}))
        self.assertEqual(0, self.functions.count_documents({"minhash": {"$type": "string", "$ne": ""}}))

    def test_unhashed_functions_keep_the_empty_string(self):
        unhashed = [stored for stored in self._rawMinHashes().values() if not isinstance(stored, bytes)]
        self.assertGreater(len(unhashed), 0)
        self.assertEqual({""}, set(unhashed))

    def test_add_minhash_stores_binary(self):
        function_id = self.functions.find_one({"minhash": ""})["function_id"]
        signature = bytes(range(self.signature_bytes))
        self.assertTrue(self.storage.addMinHash(MinHash(function_id, signature, minhash_bits=self.config.MINHASH_CONFIG.MINHASH_SIGNATURE_BITS)))
        stored = self.functions.find_one({"function_id": function_id})["minhash"]
        self.assertIs(bytes, type(stored))
        self.assertEqual(signature, stored)
        self.assertEqual(signature, self.storage.getFunctionById(function_id).minhash)

    def test_reading_back_gives_the_signature_that_was_stored(self):
        reference = self._binaryReference()
        for sample_id in self.sample_ids:
            for entry in self.storage.getFunctionsBySampleId(sample_id):
                self.assertEqual(reference.get(entry.function_id, b""), entry.minhash, entry.function_id)

    # hex written by an earlier MCRIT

    def test_hex_documents_read_as_the_same_function_entries(self):
        reference = self._binaryReference()
        before = {entry.function_id: entry.toDict() for sample_id in self.sample_ids for entry in self.storage.getFunctionsBySampleId(sample_id)}
        self._rewriteAsHex(reference)
        self.assertEqual(len(reference), self.functions.count_documents({"minhash": {"$type": "string", "$ne": ""}}))

        after = {entry.function_id: entry.toDict() for sample_id in self.sample_ids for entry in self.storage.getFunctionsBySampleId(sample_id)}
        self.assertEqual(before, after)
        for function_id, signature in reference.items():
            self.assertEqual(signature, self.storage.getFunctionById(function_id).minhash)

    def test_hex_documents_give_the_same_matching_cache(self):
        reference = self._binaryReference()
        function_ids = sorted(reference)
        before = self._matchingMinHashes(function_ids)
        self.assertEqual(reference, {function_id: bytes(minhash) for function_id, minhash in before.items()})

        self._rewriteAsHex(function_ids)

        self.assertEqual(before, self._matchingMinHashes(function_ids))

    def test_hex_documents_give_the_same_match_report(self):
        query_sample_id = self.sample_ids[0]
        report = MatcherSample(self.index.queue._worker).getMatchesForSample(query_sample_id)
        self.assertTrue(report["matches"]["functions"], "the fixtures produced no function matches, nothing is being compared")

        self._rewriteAsHex(self.hashed_ids)

        legacy_report = MatcherSample(self.index.queue._worker).getMatchesForSample(query_sample_id)
        self.assertEqual(comparableReport(report), comparableReport(legacy_report))

    def test_the_cache_fetch_handles_one_signature_in_both_forms(self):
        reference = self._binaryReference()
        function_ids = sorted(reference)
        shared = reference[function_ids[0]]
        # the same signature as hex, as binary, and as hex again: what a half-migrated corpus holds
        self._setRaw(function_ids[1], shared.hex())
        self._setRaw(function_ids[2], shared)
        self._setRaw(function_ids[3], shared.hex())
        # and the rest of the corpus alternating
        for position, function_id in enumerate(function_ids[4:]):
            if position % 2:
                self._setRaw(function_id, reference[function_id].hex())

        held = self._matchingMinHashes(function_ids)

        expected = dict(reference)
        for function_id in function_ids[1:4]:
            expected[function_id] = shared
        self.assertEqual(expected, {function_id: bytes(minhash) for function_id, minhash in held.items()})

    # the unhashed marker

    def test_unhashed_functions_are_the_empty_ones_and_not_the_binary_ones(self):
        victim = self.sample_ids[-1]
        victim_hashed = [function_id for function_id in self.hashed_ids if self.functions.find_one({"function_id": function_id})["sample_id"] == victim]
        self.assertGreater(len(victim_hashed), 0)
        self.assertEqual([], self.storage.getUnhashedFunctions(only_function_ids=True))

        self.storage.deleteMinHashesForSample(victim)
        # half of what is left is hex: neither form is unhashed
        remaining = [function_id for function_id in self.hashed_ids if function_id not in set(victim_hashed)]
        self._rewriteAsHex(remaining[::2])
        self.assertTrue(any(type(value) is bytes for value in self._rawMinHashes().values()))
        self.assertTrue(any(isinstance(value, str) and value for value in self._rawMinHashes().values()))

        unhashed_ids = self.storage.getUnhashedFunctions(only_function_ids=True)
        self.assertEqual(set(victim_hashed), set(unhashed_ids))
        self.assertEqual("", self.functions.find_one({"function_id": victim_hashed[0]})["minhash"])
        entries = self.storage.getUnhashedFunctions(function_ids=victim_hashed)
        self.assertEqual(sorted(victim_hashed), sorted(entry.function_id for entry in entries))
        self.assertTrue(all(entry.minhash == b"" for entry in entries))

    # the band index

    def test_rebuilding_the_band_index_from_hex_and_binary_gives_the_same_bands(self):
        reference = self._binaryReference()
        # a small batch, so that mixed forms meet inside one batch and across batch borders
        self.storage._minhash_config.MINHASH_BAND_REBUILD_WORK_PACKAGE_SIZE = 7
        result = self.storage.rebuildMinhashBandIndex()
        self.assertEqual(len(reference), result["minhash_functions_indexed"])
        all_binary = self._bandContents()
        self.assertTrue(any(all_binary.values()))

        self._rewriteAsHex(sorted(reference)[::2])
        result = self.storage.rebuildMinhashBandIndex()
        self.assertEqual(len(reference), result["minhash_functions_indexed"])
        mixed = self._bandContents()
        self.assertEqual(all_binary, mixed)

        self._rewriteAsHex(sorted(reference))
        self.storage.rebuildMinhashBandIndex()
        self.assertEqual(all_binary, self._bandContents())

    def _assertDeletingRemovesBandEntries(self, as_hex):
        victim = self.sample_ids[0]
        victim_ids = [document["function_id"] for document in self.functions.find({"sample_id": victim, "minhash": {"$ne": ""}})]
        others = [function_id for function_id in self.hashed_ids if function_id not in set(victim_ids)]
        self.assertGreater(len(victim_ids), 0)
        self.assertGreater(len(others), 0)
        if as_hex:
            self._rewriteAsHex(victim_ids)
        stored = [self.functions.find_one({"function_id": function_id})["minhash"] for function_id in victim_ids]
        self.assertTrue(all(isinstance(value, str) if as_hex else type(value) is bytes for value in stored))
        self.assertGreater(self._functionsInBands(victim_ids), 0)
        others_before = self._functionsInBands(others)

        self.assertTrue(self.storage.deleteSample(victim))

        self.assertEqual(0, self._functionsInBands(victim_ids))
        self.assertEqual(0, self.functions.count_documents({"sample_id": victim}))
        self.assertEqual(others_before, self._functionsInBands(others))

    def test_deleting_a_sample_with_binary_minhashes_removes_its_band_entries(self):
        self._assertDeletingRemovesBandEntries(as_hex=False)

    def test_deleting_a_sample_with_hex_minhashes_removes_its_band_entries(self):
        self._assertDeletingRemovesBandEntries(as_hex=True)

    # export and import

    def test_exports_carry_hex_and_imports_store_binary(self):
        reference = self._binaryReference()
        export_data = json.loads(json.dumps(self.index.getExportData(compress_data=False)))
        exported = {int(function_id): function for functions in export_data["function_entries"].values() for function_id, function in functions.items()}
        self.assertEqual(self.functions.count_documents({}), len(exported))
        for function_id, function in exported.items():
            self.assertIs(str, type(function["minhash"]), function_id)
            self.assertEqual(reference.get(function_id, b"").hex(), function["minhash"], function_id)

        target = MinHashIndex(config=buildConfig(IMPORT_DB_NAME))
        target._storage.clearStorage()
        report = target.addImportData(export_data)
        self.assertEqual(len(REPORTS), report["num_samples_imported"])

        imported = self._rawMinHashes(target._storage._getDb().functions)
        self.assertEqual(len(exported), len(imported))
        stored_hashed = sorted(bytes(stored) for stored in imported.values() if stored != "")
        self.assertEqual(sorted(reference.values()), stored_hashed)
        for function_id, stored in imported.items():
            self.assertTrue(stored == "" or type(stored) is bytes, function_id)
        self.assertEqual(len(reference), target._storage._getDb().functions.count_documents({"minhash": {"$type": "binData"}}))
        self.assertEqual(0, target._storage._getDb().functions.count_documents({"minhash": {"$type": "string", "$ne": ""}}))

    def test_importing_function_entries_stores_binary(self):
        # called on their own: addImportData rewrites every minhash with addMinHashes right after
        # importing, which would hide an import that stored hex
        entries = [entry for entry in self.storage.getFunctionsBySampleId(self.sample_ids[0]) if entry.minhash]
        self.assertGreater(len(entries), 2)
        expected = [entry.minhash for entry in entries[1:]] + [entries[0].minhash]

        imported = self.storage.importFunctionEntries(deepcopy(entries[1:]))
        imported.append(self.storage.importFunctionEntry(deepcopy(entries[0])))

        for function_entry, signature in zip(imported, expected):
            stored = self.functions.find_one({"function_id": function_entry.function_id})["minhash"]
            self.assertIs(bytes, type(stored), function_entry.function_id)
            self.assertEqual(signature, stored, function_entry.function_id)

    def test_query_functions_store_and_read_binary(self):
        query_sample = self.storage.addSmdaReport(loadReport(REPORTS[0]), isQuery=True)
        self.assertIsNotNone(query_sample)
        query_entries = self.storage.getFunctionsBySampleId(query_sample.sample_id)
        minhashes = [minhash for minhash in self.index.queue._worker.calculateMinHashes(query_entries) if minhash.hasMinHash()]
        self.assertGreater(len(minhashes), 1)
        self.assertTrue(all(minhash.function_id < 0 for minhash in minhashes))

        self.storage.addMinHashes(minhashes)

        raw = self._rawMinHashes(self.storage._getDb().query_functions)
        cached = self._matchingMinHashes([minhash.function_id for minhash in minhashes])
        for minhash in minhashes:
            self.assertIs(bytes, type(raw[minhash.function_id]), minhash.function_id)
            self.assertEqual(minhash.getMinHash(), raw[minhash.function_id], minhash.function_id)
            self.assertEqual(minhash.getMinHash(), cached[minhash.function_id], minhash.function_id)


class ConcurrentRewriteDb:
    """A database whose collections run `rewrite(collection)` once, right before the first bulk write.

    That is the window the migration has to survive: documents read, then changed by a running
    MCRIT, then written over.
    """

    def __init__(self, db, rewrite):
        self._db = db
        self._rewrite = rewrite
        self.fired = False

    def __getitem__(self, name):
        collection = self._db[name]
        owner = self

        class Collection:
            def __getattr__(self, attribute):
                return getattr(collection, attribute)

            def bulk_write(self, updates, **kwargs):
                if not owner.fired and updates:
                    owner.fired = True
                    owner._rewrite(collection, updates)
                return collection.bulk_write(updates, **kwargs)

        return Collection()


@pytest.mark.mongo
class MinHashMigrationTest(unittest.TestCase):
    """migrate_minhash_binary on a database whose minhashes are hex."""

    def setUp(self):
        server, port = getTestMongoServerAndPort()
        self.client = pymongo.MongoClient(server, int(port))
        self.addCleanup(self.client.close)
        self.client.drop_database(DB_NAME)
        self.addCleanup(self.client.drop_database, DB_NAME)
        config = buildConfig(DB_NAME)
        index = MinHashIndex(config=config)
        index._storage.clearStorage()
        for name in REPORTS:
            entry = index._storage.addSmdaReport(loadReport(name))
            index.queue._worker.updateMinHashesForSample(entry.sample_id)
        self.db = index._storage._getDb()
        self.reference = {document["function_id"]: bytes(document["minhash"]) for document in self.db.functions.find({"minhash": {"$ne": ""}})}
        self.num_unhashed = self.db.functions.count_documents({"minhash": ""})
        self.assertGreater(len(self.reference), 14, "too few hashed functions to page")
        self.assertGreater(self.num_unhashed, 0)
        for function_id, signature in self.reference.items():
            self.db.functions.update_one({"function_id": function_id}, {"$set": {"minhash": signature.hex()}})

    def _raw(self):
        return {document["function_id"]: document["minhash"] for document in self.db.functions.find({}, {"_id": 0, "function_id": 1, "minhash": 1})}

    def test_count_tells_the_forms_apart(self):
        counts = migrate_minhash_binary.count(self.db)
        self.assertEqual({"hex": len(self.reference), "binary": 0, "not_hashed": self.num_unhashed}, counts["functions"])
        self.assertEqual({"hex": 0, "binary": 0, "not_hashed": 0}, counts["query_functions"])

    def test_binary_converts_everything_once(self):
        converted = migrate_minhash_binary.convert(self.db, "functions", "binary", batch_size=7)

        self.assertEqual(len(self.reference), converted)
        raw = self._raw()
        for function_id, signature in self.reference.items():
            self.assertIs(bytes, type(raw[function_id]), function_id)
            self.assertEqual(signature, raw[function_id], function_id)
        self.assertEqual(self.num_unhashed, sum(1 for stored in raw.values() if stored == ""))
        self.assertEqual({"hex": 0, "binary": len(self.reference), "not_hashed": self.num_unhashed}, migrate_minhash_binary.count(self.db)["functions"])
        # nothing is left in the old form, so a second run has nothing to do
        self.assertEqual(0, migrate_minhash_binary.convert(self.db, "functions", "binary", batch_size=7))
        self.assertEqual(raw, self._raw())

    def test_revert_restores_the_original_hex(self):
        migrate_minhash_binary.convert(self.db, "functions", "binary", batch_size=7)

        converted = migrate_minhash_binary.convert(self.db, "functions", "revert", batch_size=7)

        self.assertEqual(len(self.reference), converted)
        raw = self._raw()
        for function_id, signature in self.reference.items():
            self.assertIs(str, type(raw[function_id]), function_id)
            self.assertEqual(signature.hex(), raw[function_id], function_id)
        self.assertEqual(self.num_unhashed, sum(1 for stored in raw.values() if stored == ""))
        self.assertEqual({"hex": len(self.reference), "binary": 0, "not_hashed": self.num_unhashed}, migrate_minhash_binary.count(self.db)["functions"])
        self.assertEqual(0, migrate_minhash_binary.convert(self.db, "functions", "revert", batch_size=7))

    def test_a_minhash_changed_after_it_was_read_is_not_overwritten(self):
        rehashed = {}

        def rehash(collection, updates):
            # a running MCRIT stores a new signature for the first function of the first batch
            function_id = min(self.reference)
            rehashed[function_id] = bytes(reversed(self.reference[function_id]))
            collection.update_one({"function_id": function_id}, {"$set": {"minhash": rehashed[function_id]}})

        racing = ConcurrentRewriteDb(self.db, rehash)
        converted = migrate_minhash_binary.convert(racing, "functions", "binary", batch_size=7)

        self.assertTrue(racing.fired)
        self.assertEqual(1, len(rehashed))
        self.assertEqual(len(self.reference) - 1, converted)
        raw = self._raw()
        for function_id, signature in self.reference.items():
            self.assertEqual(rehashed.get(function_id, signature), raw[function_id], function_id)
            self.assertIs(bytes, type(raw[function_id]), function_id)

    def test_a_minhash_changed_after_it_was_read_is_not_reverted_over(self):
        migrate_minhash_binary.convert(self.db, "functions", "binary", batch_size=7)
        rehashed = {}

        def rehash(collection, updates):
            function_id = min(self.reference)
            rehashed[function_id] = bytes(reversed(self.reference[function_id]))
            collection.update_one({"function_id": function_id}, {"$set": {"minhash": rehashed[function_id]}})

        racing = ConcurrentRewriteDb(self.db, rehash)
        converted = migrate_minhash_binary.convert(racing, "functions", "revert", batch_size=7)

        self.assertEqual(len(self.reference) - 1, converted)
        raw = self._raw()
        (function_id,) = rehashed
        self.assertEqual(rehashed[function_id], raw[function_id])
        self.assertIs(bytes, type(raw[function_id]))

    def test_main_converts_functions_and_query_functions(self):
        query_reference = {}
        for offset, signature in enumerate(sorted(self.reference.values())[:5], start=1):
            self.db.query_functions.insert_one({"function_id": -offset, "sample_id": -1, "minhash": signature.hex()})
            query_reference[-offset] = signature
        server, port = getTestMongoServerAndPort()
        argv = ["migrate_minhash_binary", "--mode", "binary", "--host", server, "--port", str(port), "--db", DB_NAME, "--batch", "3"]

        with patch.object(sys, "argv", argv), redirect_stdout(io.StringIO()):
            self.assertEqual(0, migrate_minhash_binary.main())

        counts = migrate_minhash_binary.count(self.db)
        self.assertEqual({"hex": 0, "binary": len(self.reference), "not_hashed": self.num_unhashed}, counts["functions"])
        self.assertEqual({"hex": 0, "binary": len(query_reference), "not_hashed": 0}, counts["query_functions"])
        for function_id, signature in query_reference.items():
            self.assertEqual(signature, self.db.query_functions.find_one({"function_id": function_id})["minhash"], function_id)

    def test_progress_is_logged_every_ten_batches_and_once_when_done(self):
        output = io.StringIO()
        with redirect_stdout(output):
            converted = migrate_minhash_binary.convert(self.db, "functions", "binary", batch_size=1)

        lines = output.getvalue().splitlines()
        self.assertEqual(len(self.reference), converted)
        self.assertEqual(len(self.reference) // 10, sum(1 for line in lines if " converted, up to function_id " in line))
        self.assertEqual(1, sum(1 for line in lines if "functions: done, %d minhashes rewritten as binary in %d batches" % (converted, converted) in line))
        self.assertEqual(len(self.reference) // 10 + 1, len(lines))

    def _revert_main(self, *extra):
        server, port = getTestMongoServerAndPort()
        argv = ["--mode", "revert", "--host", server, "--port", str(port), "--db", DB_NAME, *extra]
        with redirect_stdout(io.StringIO()):
            return migrate_minhash_binary.main(argv)

    def test_revert_refuses_while_the_queue_holds_queued_or_running_jobs(self):
        migrate_minhash_binary.convert(self.db, "functions", "binary", batch_size=7)
        converted = self._raw()
        now = datetime.now()
        job = dict(DEFAULT_INSERT, attempts_left=3, method="updateMinHashesForSample")
        self.db.queue.insert_many(
            [
                dict(job, finished_at=now, started_at=now),
                dict(job, attempts_left=0),
                dict(job, terminated=True),
            ]
        )
        self.addCleanup(self.db.queue.drop)
        # finished, failed and terminated jobs do not hold a revert back
        self.assertEqual(0, self._revert_main())
        self.assertEqual(len(self.reference), migrate_minhash_binary.count(self.db)["functions"]["hex"])

        for blocking_job in [job, dict(job, locked_by="worker", locked_at=now, started_at=now)]:
            migrate_minhash_binary.convert(self.db, "functions", "binary", batch_size=7)
            inserted = self.db.queue.insert_one(dict(blocking_job)).inserted_id

            self.assertEqual(1, self._revert_main())
            self.assertEqual(converted, self._raw())

            self.assertEqual(0, self._revert_main("--force"))
            self.assertEqual(len(self.reference), migrate_minhash_binary.count(self.db)["functions"]["hex"])
            self.db.queue.delete_one({"_id": inserted})


if __name__ == "__main__":
    unittest.main()
