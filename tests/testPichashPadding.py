import json
import logging
import os
from unittest import TestCase
from unittest.mock import patch

import pymongo
import pytest
from smda.common.SmdaReport import SmdaReport

from mcrit.config.McritConfig import McritConfig
from mcrit.config.MinHashConfig import MinHashConfig
from mcrit.config.QueueConfig import QueueConfig
from mcrit.config.ShinglerConfig import ShinglerConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.index.SearchCursor import FullSearchCursor, MinimalSearchCursor
from mcrit.index.SearchQueryParser import SearchQueryParser
from mcrit.migrations import migrate_pichash_padding
from mcrit.storage.MongoDbStorage import PICHASH_HEX_DIGITS, MongoDbStorage, encode_pichash_value
from mcrit.storage.StorageFactory import StorageFactory

from .context import getTestMongoServerAndPort

logging.disable(logging.CRITICAL)

THIS_FILE_PATH = str(os.path.abspath(__file__))
PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
EXAMPLE_REPORT = os.sep.join([PROJECT_ROOT, "tests", "example_report.smda"])
DB_NAME = "test_pichash_padding_mcrit"


def build_config(db_name=DB_NAME):
    server, port = getTestMongoServerAndPort()
    mcrit_config = McritConfig()
    mcrit_config.STORAGE_CONFIG = StorageConfig(
        STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB,
        STORAGE_SERVER=server,
        STORAGE_PORT=port,
        STORAGE_MONGODB_DBNAME=db_name,
        STORAGE_DROP_DISASSEMBLY=False,
    )
    mcrit_config.MINHASH_CONFIG = MinHashConfig()
    mcrit_config.MINHASH_CONFIG.MINHASH_SIGNATURE_LENGTH = 10
    mcrit_config.MINHASH_CONFIG.MINHASH_SIGNATURE_BITS = 8
    mcrit_config.SHINGLER_CONFIG = ShinglerConfig()
    mcrit_config.QUEUE_CONFIG = QueueConfig()
    return mcrit_config


def is_padded(value):
    return isinstance(value, str) and value.startswith("0x") and len(value) == 2 + PICHASH_HEX_DIGITS


def short_form(value):
    """The pre-#145 encoding; identical to the padded one unless the value has a leading zero."""
    return hex(int(value, 16))


class PichashMigrationConnectionTest(TestCase):
    """The migration reaches the database MCRIT itself connects to; no MongoDB needed.

    Port 27999: these clients are never used, but the test guard refuses any client to 27017.
    """

    def test_the_uri_is_built_as_mcrit_builds_it(self):
        build = MongoDbStorage.buildMongoUri
        self.assertEqual("mongodb://h:27999/db", build("h", "27999", "db", None, None, ""))
        # credentials go in as configured: a password holding "@" has to be configured as "%40"
        # for MCRIT to connect at all, and quoting it again would send "%2540"
        self.assertEqual("mongodb://u:s3cr%40t@h:27999/db?authSource=admin", build("h", "27999", "db", "u", "s3cr%40t", "authSource=admin"))
        # only both together are credentials, and an empty port is how a host list is given
        self.assertEqual("mongodb://h:27999/db", build("h", "27999", "db", "u", "", ""))
        self.assertEqual("mongodb://a:1,b:2/db?replicaSet=rs", build("a:1,b:2", "", "db", None, None, "replicaSet=rs"))

    def test_mcrit_connects_with_the_same_uri(self):
        # what keeps the claim true: MongoDbStorage._initDb, given the same settings, builds the same URI
        for server, port, username, password, flags in (("a:1,b:2", "", "u", "s3cr%40t", "authSource=admin"), ("h", "27999", "", "", "")):
            storage_config = StorageConfig(STORAGE_SERVER=server, STORAGE_PORT=port, STORAGE_MONGODB_DBNAME="db")
            storage_config.STORAGE_MONGODB_USERNAME = username
            storage_config.STORAGE_MONGODB_PASSWORD = password
            storage_config.STORAGE_MONGODB_FLAGS = flags
            config = McritConfig()
            config.STORAGE_CONFIG = storage_config
            with (
                patch("mcrit.storage.MongoDbStorage.MongoClient") as client,
                patch.object(MongoDbStorage, "_ensureIndexAndUnknownFamily"),
            ):
                MongoDbStorage(config)._getDb()
            self.assertEqual(MongoDbStorage.buildMongoUri(server, port, "db", username, password, flags), client.call_args.args[0])

    def _runMain(self, argv, **storage_values):
        """main() with the given storage config; returns (uri connected to, database name used)."""
        storage_config = StorageConfig(STORAGE_SERVER="h", STORAGE_PORT="27999", STORAGE_MONGODB_DBNAME="db")
        for key, value in storage_values.items():
            setattr(storage_config, key, value)
        config = McritConfig()
        config.STORAGE_CONFIG = storage_config
        uris, databases = [], []

        def client_for(uri, connect=True):
            uris.append(uri)
            return pymongo.MongoClient(uri, connect=False)

        def record_run(db, mode, batch):
            databases.append(db.name)
            db.client.close()
            return {}

        with (
            patch.object(migrate_pichash_padding, "McritConfig", return_value=config),
            patch.object(migrate_pichash_padding, "MongoClient", side_effect=client_for),
            patch.object(migrate_pichash_padding, "run", side_effect=record_run),
            patch("builtins.print"),
        ):
            self.assertEqual(0, migrate_pichash_padding.main(argv))
        return uris[0], databases[0]

    def test_the_migration_connects_as_mcrit_does(self):
        credentials = {"STORAGE_MONGODB_USERNAME": "u", "STORAGE_MONGODB_PASSWORD": "s3cr%40t", "STORAGE_MONGODB_FLAGS": "authSource=admin"}
        self.assertEqual(("mongodb://u:s3cr%40t@h:27999/db?authSource=admin", "db"), self._runMain(["--mode", "verify"], **credentials))
        # a host list has no port of its own
        self.assertEqual(
            ("mongodb://a:1,b:2/db?replicaSet=rs", "db"),
            self._runMain(["--mode", "verify"], STORAGE_SERVER="a:1,b:2", STORAGE_PORT="", STORAGE_MONGODB_FLAGS="replicaSet=rs"),
        )

    def test_a_uri_names_the_database_unless_db_does(self):
        self.assertEqual(("mongodb://other:1/x", "x"), self._runMain(["--mode", "verify", "--uri", "mongodb://other:1/x"]))
        self.assertEqual(("mongodb://other:1/x", "y"), self._runMain(["--mode", "verify", "--uri", "mongodb://other:1/x", "--db", "y"]))
        # a URI without a database falls back to the configured one
        self.assertEqual(("mongodb://other:1", "db"), self._runMain(["--mode", "verify", "--uri", "mongodb://other:1"]))
        # --db alone migrates that database, and authenticates against it as MCRIT would
        self.assertEqual(("mongodb://h:27999/z", "z"), self._runMain(["--mode", "verify", "--db", "z"]))


@pytest.mark.mongo
class PichashPaddingTest(TestCase):
    """#145: pichashes are stored 16 digits wide so that their hex order is their numeric order"""

    def setUp(self):
        server, port = getTestMongoServerAndPort()
        self.client = pymongo.MongoClient(server, int(port))
        # a dropped database makes the storage create its settings from scratch: a fresh instance
        self.client.drop_database(DB_NAME)
        self.db = self.client[DB_NAME]
        self.config = build_config()
        self.storage = StorageFactory.getStorage(self.config)
        with open(EXAMPLE_REPORT) as fjson:
            report = SmdaReport.fromDict(json.load(fjson))
        assert report is not None
        sample_entry = self.storage.addSmdaReport(report)
        assert sample_entry is not None
        self.sample_id = sample_entry.sample_id
        # the example report's pichashes all use 16 digits; give two of them leading zeros so
        # that the two encodings differ for pichashes as well as for block hashes
        first, second = [d["function_id"] for d in self.db.functions.find({}, {"function_id": 1}).sort("function_id", 1).limit(2)]
        self.db.functions.update_one({"function_id": first}, {"$set": {"_pichash": encode_pichash_value(0x4D2, padded=True)}})
        self.db.functions.update_one({"function_id": second}, {"$set": {"_pichash": encode_pichash_value(0x99, padded=True)}})
        self.functions = list(self.db.functions.find({}, {"_id": 0, "function_id": 1, "_pichash": 1, "_picblockhashes": 1}))
        self.assertGreaterEqual(len(self.functions), 10)
        # the documents whose values differ between the two encodings (a leading zero somewhere)
        self.differing = [d for d in self.functions if short_form(d["_pichash"]) != d["_pichash"] or any(short_form(e["hash"]) != e["hash"] for e in d.get("_picblockhashes", []))]
        self.assertGreater(len(self.differing), 0)
        self.assertLess(len(self.differing), len(self.functions))

    def tearDown(self):
        self.client.drop_database(DB_NAME)

    def _stored_pichashes(self):
        return [document["_pichash"] for document in self.db.functions.find({}, {"_pichash": 1})]

    def _stored_blockhashes(self):
        return [entry["hash"] for document in self.db.functions.find({}, {"_picblockhashes": 1}) for entry in document.get("_picblockhashes", [])]

    def _make_legacy(self):
        """Turn the instance into one created before #145: unpadded values, flag unset."""
        migrate_pichash_padding.run(self.db, "unpad")
        self.storage._pichash_padded = None

    def test_a_fresh_instance_stores_padded_values_and_says_so(self):
        settings = self.db.settings.find_one({})
        assert settings is not None
        self.assertTrue(settings["pichash_padded"])
        self.assertTrue(self.storage.isPichashPadded())
        self.assertTrue(self.storage.getStats(with_pichash=False)["pichash_padded"])
        self.assertTrue(all(is_padded(value) for value in self._stored_pichashes()))
        self.assertGreater(len(self._stored_blockhashes()), 0)
        self.assertTrue(all(is_padded(value) for value in self._stored_blockhashes()))
        self.assertTrue(MinHashIndex(self.config).getStatus(with_pichash=False)["status"]["pichash_padded"])

    def test_lookups_by_value_work_on_both_shapes(self):
        function_entries = self.storage.getFunctionsBySampleId(self.sample_id)
        with_blocks = next(fe for fe in function_entries if fe.picblockhashes)
        for legacy in (False, True):
            if legacy:
                self._make_legacy()
                self.assertFalse(self.storage.isPichashPadded())
                self.assertTrue(all(value == short_form(value) for value in self._stored_pichashes()))
            for function_entry in function_entries:
                self.assertTrue(self.storage.isPicHash(function_entry.pichash), (legacy, function_entry.function_id))
                self.assertIn((function_entry.family_id, self.sample_id, function_entry.function_id), self.storage.getMatchesForPicHash(function_entry.pichash))
                self.assertEqual(
                    {function_entry.pichash: {(function_entry.family_id, self.sample_id, function_entry.function_id)}},
                    self.storage.getPicHashMatchesByFunctionId(function_entry.function_id),
                )
            block = with_blocks.picblockhashes[0]
            self.assertIn((with_blocks.family_id, self.sample_id, with_blocks.function_id, block["offset"]), self.storage.getMatchesForPicBlockHash(block["hash"]))
            self.assertFalse(self.storage.isPicHash(0x1234567))
            self.assertEqual(set(), self.storage.getMatchesForPicHash(0x1234567))

    def test_the_aggregation_readers_see_both_widths_while_unpadded(self):
        """getPicHashMatchesByFunctionId(s) group functions by equal pichash; during a migration
        the same value may be stored in either width and must still land in one group"""
        self._make_legacy()
        first, second = self.differing[0]["function_id"], self.differing[1]["function_id"]
        # give both functions the same pichash, one in each width
        value = 0x4D2
        self.db.functions.update_one({"function_id": first}, {"$set": {"_pichash": hex(value)}})
        self.db.functions.update_one({"function_id": second}, {"$set": {"_pichash": encode_pichash_value(value, padded=True)}})
        by_one = self.storage.getPicHashMatchesByFunctionId(first)
        self.assertEqual({first, second}, {t[2] for t in by_one[value]})
        by_many = self.storage.getPicHashMatchesByFunctionIds([first, second])
        self.assertEqual({first, second}, {t[2] for t in by_many[value]})
        self.assertEqual(1, len(by_many))

    def test_a_legacy_instance_in_the_middle_of_a_migration_misses_nothing(self):
        self._make_legacy()
        # half the documents padded by hand: a `pad` run that was interrupted before the flag
        # one of the two leading-zero pichashes padded, the other still short
        rewritten = [d for d in self.differing if short_form(d["_pichash"]) != d["_pichash"]][:1]
        for document in rewritten:
            self.db.functions.update_one({"function_id": document["function_id"]}, {"$set": {"_pichash": encode_pichash_value(int(document["_pichash"], 16), padded=True)}})
        stored = self._stored_pichashes()
        self.assertTrue(any(value != short_form(value) for value in stored))
        self.assertTrue(any(value == short_form(value) and not is_padded(value) for value in stored))
        parser = SearchQueryParser()
        for function_entry in self.storage.getFunctionsBySampleId(self.sample_id):
            self.assertTrue(self.storage.isPicHash(function_entry.pichash))
            found = self.storage.findFunctionByString(parser.parse("pichash:%s" % hex(function_entry.pichash)))
            self.assertIn(function_entry.function_id, found)
        # ... but the answers that would be wrong on mixed widths stay refused
        with self.assertRaises(ValueError):
            self.storage.findFunctionByString(parser.parse("pichash:>0x10"))
        with self.assertRaises(ValueError):
            self.storage.findFunctionByString(parser.parse("function_id:>0"), cursor=FullSearchCursor(None, [("pichash", True), ("function_id", True)]))

    def test_range_conditions_answer_the_numeric_set_when_padded(self):
        parser = SearchQueryParser()
        pichashes = {document["function_id"]: int(document["_pichash"], 16) for document in self.functions}
        pivot = sorted(pichashes.values())[len(pichashes) // 2]
        expected_below = {function_id for function_id, pichash in pichashes.items() if pichash < pivot}
        self.assertGreater(len(expected_below), 0)
        found = self.storage.findFunctionByString(parser.parse("pichash:<%s" % hex(pivot)), max_num_results=1000)
        self.assertEqual(expected_below, set(found))
        # a small bound that variable-width strings would have ordered a wrong set for
        self.assertEqual(set(), set(self.storage.findFunctionByString(parser.parse("pichash:<0x99"), max_num_results=1000)))
        self.assertEqual(set(pichashes), set(self.storage.findFunctionByString(parser.parse("pichash:>=0x0"), max_num_results=1000)))

    def test_a_pichash_sorted_function_search_pages_in_numeric_order(self):
        index = MinHashIndex(self.config)
        expected = [document["function_id"] for document in sorted(self.functions, key=lambda d: (int(d["_pichash"], 16), d["function_id"]))]
        pages = []
        cursor = None
        for _ in range(100):
            result = index.getFunctionSearchResults("function_id:>=0", sort_by="pichash", is_ascending=True, cursor=cursor, limit=4)
            page = [entry["function_id"] for entry in result["search_results"].values()]
            self.assertLessEqual(len(page), 4)
            pages.append(page)
            cursor = result["cursor"]["forward"]
            if cursor is None:
                break
        self.assertEqual(expected, [function_id for page in pages for function_id in page])
        self.assertGreater(len(pages), 2)
        # the backward cursor of the last page reproduces the page before it
        last_backward = MinimalSearchCursor.fromStr(result["cursor"]["backward"])
        self.assertFalse(last_backward.is_forward_search)
        previous = index.getFunctionSearchResults("function_id:>=0", sort_by="pichash", is_ascending=True, cursor=result["cursor"]["backward"], limit=4)
        self.assertEqual(pages[-2], [entry["function_id"] for entry in previous["search_results"].values()])
        # descending order is the reverse walk
        descending = index.getFunctionSearchResults("function_id:>=0", sort_by="pichash", is_ascending=False, limit=1000)
        self.assertEqual(list(reversed(expected)), [entry["function_id"] for entry in descending["search_results"].values()])

    def test_the_migration_round_trips_and_verify_spots_leftovers(self):
        report = migrate_pichash_padding.run(self.db, "verify")
        self.assertTrue(report["pichash_padded"])
        self.assertEqual([], report["problems"])
        self.assertEqual(0, report["collections"]["functions"]["unpadded_pichashes"])
        # rollback
        report = migrate_pichash_padding.run(self.db, "unpad")
        self.assertFalse(report["pichash_padded"])
        self.assertEqual([], report["problems"])
        self.assertEqual(len(self.differing), report["functions"]["rewritten"])
        self.assertTrue(all(value == short_form(value) for value in self._stored_pichashes() + self._stored_blockhashes()))
        self.assertEqual([short_form(document["_pichash"]) for document in self.functions], self._stored_pichashes())
        # migrate: every value padded, flag set, values unchanged
        report = migrate_pichash_padding.run(self.db, "pad", batch_size=5)
        self.assertTrue(report["pichash_padded"])
        self.assertEqual([], report["problems"])
        self.assertEqual(len(self.differing), report["functions"]["rewritten"])
        self.assertTrue(all(is_padded(value) for value in self._stored_pichashes() + self._stored_blockhashes()))
        self.assertEqual(
            {document["function_id"]: document["_pichash"] for document in self.functions},
            {document["function_id"]: document["_pichash"] for document in self.db.functions.find({}, {"function_id": 1, "_pichash": 1})},
        )
        self.assertEqual(
            {document["function_id"]: document.get("_picblockhashes") for document in self.functions},
            {document["function_id"]: document.get("_picblockhashes") for document in self.db.functions.find({}, {"function_id": 1, "_picblockhashes": 1})},
        )
        # re-running is a no-op
        report = migrate_pichash_padding.run(self.db, "pad")
        self.assertEqual(0, report["functions"]["rewritten"] + report["functions"]["swept"])
        # a value a still-running server wrote unpadded after the flag: verify names it, pad sweeps it
        leading_zero = next(d for d in self.differing if short_form(d["_pichash"]) != d["_pichash"])
        self.db.functions.update_one({"function_id": leading_zero["function_id"]}, {"$set": {"_pichash": short_form(leading_zero["_pichash"])}})
        report = migrate_pichash_padding.run(self.db, "verify")
        self.assertEqual(1, len(report["problems"]))
        self.assertEqual(1, report["collections"]["functions"]["unpadded_pichashes"])
        report = migrate_pichash_padding.run(self.db, "pad")
        self.assertEqual([], report["problems"])
        self.assertEqual(1, report["functions"]["rewritten"] + report["functions"]["swept"])

    def test_the_command_line_entry_point(self):
        server, port = getTestMongoServerAndPort()
        self.assertEqual(0, migrate_pichash_padding.main(["--mode", "verify", "--host", server, "--port", str(port), "--db", DB_NAME]))
        self.db.settings.update_one({}, {"$set": {"pichash_padded": True}})
        self.db.functions.update_one({"function_id": self.functions[0]["function_id"]}, {"$set": {"_pichash": "0x1"}})
        self.assertEqual(1, migrate_pichash_padding.main(["--mode", "verify", "--host", server, "--port", str(port), "--db", DB_NAME]))

    def test_values_no_width_spells_are_left_as_stored_and_reported(self):
        """Submissions could store a pichash unchecked. Padded, hex(-5) became the undecodable
        "0x-000000000000005", verify saw nothing, and every later pad or unpad run aborted on it."""
        self._make_legacy()
        negative, wide, operator = [document["function_id"] for document in self.functions[:3]]
        with_blocks = next(document["function_id"] for document in self.functions if document.get("_picblockhashes"))
        self.db.functions.update_one({"function_id": negative}, {"$set": {"_pichash": hex(-5)}})
        self.db.functions.update_one({"function_id": wide}, {"$set": {"_pichash": hex(1 << 64)}})
        self.db.functions.update_one({"function_id": operator}, {"$set": {"_pichash": {"$eq": "0x5"}}})
        self.db.functions.update_one({"function_id": with_blocks}, {"$set": {"_picblockhashes.0.hash": hex(-7)}})
        planted = {document["function_id"]: document for document in self.db.functions.find({"function_id": {"$in": [negative, wide, operator, with_blocks]}}, {"_id": 0})}
        for mode in ("pad", "pad", "unpad", "pad"):
            report = migrate_pichash_padding.run(self.db, mode)
            self.assertEqual(mode == "pad", report["pichash_padded"])
            self.assertEqual(3, report["collections"]["functions"]["unspellable_pichashes"], mode)
            self.assertEqual(1, report["collections"]["functions"]["documents_with_unspellable_blockhashes"], mode)
            self.assertEqual(1, len(report["problems"]), mode)
            stored = {document["function_id"]: document for document in self.db.functions.find({"function_id": {"$in": list(planted)}}, {"_id": 0})}
            for function_id in (negative, wide, operator):
                self.assertEqual(planted[function_id]["_pichash"], stored[function_id]["_pichash"], mode)
            self.assertEqual("-0x7", stored[with_blocks]["_picblockhashes"][0]["hash"], mode)
        # everything else was padded as usual, and a padded reader decodes what it decoded before
        self.assertTrue(all(is_padded(document["_pichash"]) for document in self.db.functions.find({"function_id": {"$nin": [negative, wide, operator]}})))
        self.storage._pichash_padded = None
        self.assertEqual(len(self.functions), len(self.storage.getFunctionsBySampleId(self.sample_id) or []))

    def _leading_zero_function(self):
        return next(d for d in self.functions if short_form(d["_pichash"]) != d["_pichash"])

    def test_the_pichash_cutoff_counts_both_spellings_of_a_value_together(self):
        """An unpadded instance may hold a value in both spellings; the cutoff must see one total."""
        self._make_legacy()
        target = self._leading_zero_function()
        # a second holder of the same value, written in the other (padded) spelling
        other = next(d for d in self.functions if d["function_id"] != target["function_id"])
        self.db.functions.update_one({"function_id": other["function_id"]}, {"$set": {"_pichash": target["_pichash"]}})
        self.storage.rebuildPicHashCountIndex()
        value = int(target["_pichash"], 16)
        for index_complete in (True, False):
            self.storage._setPicHashCountIndexComplete(index_complete)
            self.storage._minhash_config.MINHASH_PICHASH_MAX_MATCHES = 1
            self.assertEqual(set(), self.storage.getPicHashMatchesByFunctionIds([target["function_id"]])[value], index_complete)
            self.storage._minhash_config.MINHASH_PICHASH_MAX_MATCHES = 2
            holders = {function_id for _, _, function_id in self.storage.getPicHashMatchesByFunctionIds([target["function_id"]])[value]}
            self.assertEqual({target["function_id"], other["function_id"]}, holders, index_complete)

    def test_the_batched_pichash_lookup_finds_both_shapes(self):
        """getMatchesForPicHashes (the query matcher's one $in) encodes like the instance stores."""
        function_entries = self.storage.getFunctionsBySampleId(self.sample_id)
        expected = {}
        for function_entry in function_entries:
            expected.setdefault(function_entry.pichash, set()).add((function_entry.family_id, self.sample_id, function_entry.function_id))
        pichashes = list(expected)
        for legacy in (False, True):
            if legacy:
                self._make_legacy()
            self.assertEqual(expected, self.storage.getMatchesForPicHashes(pichashes + [0x1234567]), legacy)
        # mid-migration, one value held in both spellings is one group, cut off as a unit
        target = self._leading_zero_function()
        other = next(d for d in self.functions if d["function_id"] != target["function_id"])
        self.db.functions.update_one({"function_id": other["function_id"]}, {"$set": {"_pichash": target["_pichash"]}})
        value = int(target["_pichash"], 16)
        self.assertNotEqual(hex(value), target["_pichash"])
        self.assertEqual({target["function_id"], other["function_id"]}, {t[2] for t in self.storage.getMatchesForPicHashes([value])[value]})
        self.storage._minhash_config.MINHASH_PICHASH_MAX_MATCHES = 1
        self.assertNotIn(value, self.storage.getMatchesForPicHashes([value]))

    def _recalculation_rewrites(self):
        """Corrupt the hashes of a function with leading-zero block hashes, recalculate, return the
        original values and the stored document afterwards."""
        target = next(d for d in self.differing if any(short_form(e["hash"]) != e["hash"] for e in d.get("_picblockhashes", [])))
        stored = self.db.functions.find_one({"function_id": target["function_id"]})
        assert stored is not None
        original_pichash = int(stored["_pichash"], 16)
        original_blocks = sorted((entry["offset"], int(entry["hash"], 16)) for entry in stored["_picblockhashes"])
        self.db.functions.update_one(
            {"function_id": target["function_id"]},
            {"$set": {"_pichash": hex(original_pichash ^ 1), "_picblockhashes": [{**entry, "hash": hex(int(entry["hash"], 16) ^ 1)} for entry in stored["_picblockhashes"]]}},
        )
        # an unstamped sample disassembled by an smda below the compatibility threshold is rehashed
        self.db.samples.update_one({"sample_id": self.sample_id}, {"$unset": {"pichash_smda_version": ""}, "$set": {"smda_version": "1.5.12"}})
        self.storage.recalculateAllPicHashes()
        rewritten = self.db.functions.find_one({"function_id": target["function_id"]})
        assert rewritten is not None
        return original_pichash, original_blocks, rewritten

    def test_the_recalculation_writes_padded_values_on_a_padded_instance(self):
        original_pichash, original_blocks, rewritten = self._recalculation_rewrites()
        self.assertEqual(encode_pichash_value(original_pichash, padded=True), rewritten["_pichash"])
        self.assertEqual(original_blocks, sorted((entry["offset"], int(entry["hash"], 16)) for entry in rewritten["_picblockhashes"]))
        self.assertTrue(all(is_padded(entry["hash"]) for entry in rewritten["_picblockhashes"]))
        self.assertTrue(any(short_form(entry["hash"]) != entry["hash"] for entry in rewritten["_picblockhashes"]))

    def test_the_recalculation_keeps_the_legacy_width_until_the_migration(self):
        self._make_legacy()
        original_pichash, original_blocks, rewritten = self._recalculation_rewrites()
        self.assertEqual(hex(original_pichash), rewritten["_pichash"])
        self.assertEqual(original_blocks, sorted((entry["offset"], int(entry["hash"], 16)) for entry in rewritten["_picblockhashes"]))
        self.assertTrue(all(entry["hash"] == short_form(entry["hash"]) for entry in rewritten["_picblockhashes"]))

    def test_padding_invalidates_the_pichash_counts_instead_of_dropping_matches(self):
        """After a migration the stored counts are keyed on the old spelling; they must not be trusted."""
        self._make_legacy()
        self.storage.rebuildPicHashCountIndex()
        self.assertTrue(self.storage.isPicHashCountIndexComplete())
        migrate_pichash_padding.run(self.db, "pad")
        self.storage._pichash_padded = None
        self.assertFalse(self.storage.isPicHashCountIndexComplete())
        self.storage._minhash_config.MINHASH_PICHASH_MAX_MATCHES = 1000
        target = self._leading_zero_function()
        matches = self.storage.getPicHashMatchesByFunctionIds([target["function_id"]])
        self.assertIn(target["function_id"], {function_id for _, _, function_id in matches[int(target["_pichash"], 16)]})
        # a rebuild restores the index, keyed on the new spelling
        self.storage.rebuildPicHashCountIndex()
        self.assertEqual(matches, self.storage.getPicHashMatchesByFunctionIds([target["function_id"]]))


LEGACY_DB_NAME = "test_pichash_padding_legacy_mcrit"
FRESH_DB_NAME = "test_pichash_padding_fresh_mcrit"
# pichashes given to the same function of every sample, and to one function of the first
# sample only; the example report's own pichashes have no leading zero
SHARED_PICHASH = 0x4D2
SINGLE_PICHASH = 0x99


def with_int_keys(unique_blocks_result):
    """A getUniqueBlocks result with its block hashes as integers, to compare across spellings."""
    return {int(block_hash, 16): entry for block_hash, entry in unique_blocks_result["unique_blocks"].items()}, unique_blocks_result["statistics"]


@pytest.mark.mongo
class PichashMigrationEquivalenceTest(TestCase):
    """#145: a database written unpadded and then migrated answers what a padded one answers.

    Two databases receive the same samples: one created before #145 (its settings carry no
    pichash_padded flag, so everything is written with `hex()`, including the derived indexes
    that are keyed on the stored spelling), one created fresh. The first is migrated, then both
    are asked the same questions.
    """

    def setUp(self):
        server, port = getTestMongoServerAndPort()
        self.client = pymongo.MongoClient(server, int(port))
        for db_name in (LEGACY_DB_NAME, FRESH_DB_NAME):
            self.client.drop_database(db_name)
        self.legacy_db = self.client[LEGACY_DB_NAME]
        self.fresh_db = self.client[FRESH_DB_NAME]
        # settings that predate the flag: the storage keeps writing the old, unpadded shape
        self.legacy_db.settings.insert_one({"mcrit_db_id": "legacy", "db_state": 0})
        self.legacy_config = build_config(LEGACY_DB_NAME)
        self.fresh_config = build_config(FRESH_DB_NAME)
        self.legacy = StorageFactory.getStorage(self.legacy_config)
        self.fresh = StorageFactory.getStorage(self.fresh_config)
        self.assertFalse(self.legacy.isPichashPadded())
        self.assertTrue(self.fresh.isPichashPadded())
        with open(EXAMPLE_REPORT) as fjson:
            report_json = json.load(fjson)
        # a: the whole report, b: half of its functions, c: the whole report again - so a block
        # is held by two or three samples, and some blocks by a and c only
        half = dict(report_json, xcfg=dict(list(report_json["xcfg"].items())[: len(report_json["xcfg"]) // 2]))
        self.sample_ids = []
        for storage in (self.legacy, self.fresh):
            sample_ids = []
            for sha256, smda_json in (("a", report_json), ("b", half), ("c", report_json)):
                report = SmdaReport.fromDict(smda_json)
                assert report is not None
                report.sha256 = 64 * sha256
                sample_entry = storage.addSmdaReport(report)
                assert sample_entry is not None
                sample_ids.append(sample_entry.sample_id)
            self.sample_ids.append(sample_ids)
        self.assertEqual(self.sample_ids[0], self.sample_ids[1])
        self.sample_a, self.sample_b, self.sample_c = self.sample_ids[0]
        # leading-zero pichashes, written in each database's own spelling: one held by the first
        # function of every sample, one by a single function
        for db, storage in ((self.legacy_db, self.legacy), (self.fresh_db, self.fresh)):
            padded = storage.isPichashPadded()
            for sample_id in self.sample_ids[0]:
                first = db.functions.find({"sample_id": sample_id}).sort("function_id", 1).limit(1)[0]
                db.functions.update_one({"function_id": first["function_id"]}, {"$set": {"_pichash": encode_pichash_value(SHARED_PICHASH, padded)}})
            second = db.functions.find({"sample_id": self.sample_a}).sort("function_id", 1).skip(1).limit(1)[0]
            db.functions.update_one({"function_id": second["function_id"]}, {"$set": {"_pichash": encode_pichash_value(SINGLE_PICHASH, padded)}})
            storage.rebuildPicHashCountIndex()
        # the old instance really is old: its functions and both derived indexes are unpadded and trusted
        for db in (self.legacy_db, self.fresh_db):
            self.assertTrue(self._setting(db, "picblockhash_index_complete"))
            self.assertTrue(self._setting(db, "pichash_count_index_complete"))
        self.assertGreater(self.legacy_db.picblockhashes.count_documents(migrate_pichash_padding.unpadded_query(field="_id")), 0)
        self.assertGreater(self.legacy_db.pichash_counts.count_documents(migrate_pichash_padding.unpadded_query()), 0)
        self.assertGreater(self.legacy_db.functions.count_documents(migrate_pichash_padding.unpadded_block_query()), 0)
        self.assertEqual(0, self.fresh_db.picblockhashes.count_documents(migrate_pichash_padding.unpadded_query(field="_id")))
        # the answers of the old instance before it is touched
        self.legacy_unique_blocks = {tuple(sample_ids): with_int_keys(self.legacy.getUniqueBlocks(sample_ids)) for sample_ids in self._sample_sets()}

    def tearDown(self):
        for db_name in (LEGACY_DB_NAME, FRESH_DB_NAME):
            self.client.drop_database(db_name)

    @staticmethod
    def _setting(db, key):
        return (db.settings.find_one({}, {key: 1}) or {}).get(key)

    def _sample_sets(self):
        return [[self.sample_a], [self.sample_b], [self.sample_a, self.sample_b], [self.sample_a, self.sample_c], [self.sample_a, self.sample_b, self.sample_c]]

    def _migrate_legacy(self, **kwargs):
        report = migrate_pichash_padding.run(self.legacy_db, "pad", **kwargs)
        self.assertEqual([], report["problems"])
        # storage reads the flag once per process; the migration asks for a restart
        self.legacy = StorageFactory.getStorage(self.legacy_config)
        self.assertTrue(self.legacy.isPichashPadded())
        return report

    def _assert_same_unique_blocks(self):
        for sample_ids in self._sample_sets():
            migrated = self.legacy.getUniqueBlocks(sample_ids)
            self.assertEqual(self.fresh.getUniqueBlocks(sample_ids), migrated, sample_ids)
            # and the migration changed only the spelling of what the old instance answered
            self.assertEqual(self.legacy_unique_blocks[tuple(sample_ids)], with_int_keys(migrated), sample_ids)

    def test_the_data_discriminates(self):
        """Without shared blocks of a leading zero, a stale index could not change any answer."""
        # a and c hold the same blocks, b holds some of them
        self.assertEqual({}, self.fresh.getUniqueBlocks([self.sample_a])["unique_blocks"])
        unique_to_a_and_c = self.fresh.getUniqueBlocks([self.sample_a, self.sample_c])["unique_blocks"]
        candidates = {entry["hash"] for document in self.fresh_db.functions.find({"sample_id": self.sample_a}) for entry in document["_picblockhashes"]}
        self.assertGreater(len(unique_to_a_and_c), 0)
        shared = candidates - set(unique_to_a_and_c)
        self.assertTrue(any(block_hash != short_form(block_hash) for block_hash in shared))

    def test_unique_blocks_after_the_migration(self):
        self._migrate_legacy()
        # the index was keyed on the old spelling: it must not be read until it is rebuilt
        self.assertFalse(self._setting(self.legacy_db, "picblockhash_index_complete"))
        self.assertEqual(0, self.legacy_db.picblockhashes.count_documents({}))
        self._assert_same_unique_blocks()
        # ... nor maintained - a sample deleted meanwhile must not leave anything behind
        for storage in (self.legacy, self.fresh):
            storage.deleteSample(self.sample_c)
        self.assertEqual(0, self.legacy_db.picblockhashes.count_documents({}))
        self.legacy_unique_blocks = {tuple(sample_ids): with_int_keys(self.fresh.getUniqueBlocks(sample_ids)) for sample_ids in self._sample_sets()}
        self._assert_same_unique_blocks()
        # the rebuild restores the indexed path, keyed on the new spelling
        self.legacy.rebuildPicBlockHashIndex()
        self.assertTrue(self.legacy._isPicBlockHashIndexComplete())
        self.assertEqual(
            {document["_id"]: sorted(document["sample_ids"]) for document in self.fresh_db.picblockhashes.find({})},
            {document["_id"]: sorted(document["sample_ids"]) for document in self.legacy_db.picblockhashes.find({})},
        )
        self._assert_same_unique_blocks()

    def test_a_run_with_nothing_to_rewrite_keeps_the_indexes(self):
        """Re-running pad, as verify advises for leftovers, must not throw a correct index away."""
        self._migrate_legacy()
        self.legacy.rebuildPicBlockHashIndex()
        self.legacy.rebuildPicHashCountIndex()
        for db in (self.legacy_db, self.fresh_db):
            index_size = db.picblockhashes.count_documents({})
            migrate_pichash_padding.run(db, "pad")
            self.assertTrue(self._setting(db, "picblockhash_index_complete"))
            self.assertTrue(self._setting(db, "pichash_count_index_complete"))
            self.assertEqual(index_size, db.picblockhashes.count_documents({}))
        self._assert_same_unique_blocks()

    def test_unique_blocks_during_an_interrupted_migration(self):
        """A `pad` killed after its first batch leaves both widths behind, and no flag set."""
        num_functions_of_a = self.legacy_db.functions.count_documents({"sample_id": self.sample_a})
        original_put_state = migrate_pichash_padding.put_state

        def killed_after_first_batch(db, state):
            original_put_state(db, state)
            raise KeyboardInterrupt()

        with patch.object(migrate_pichash_padding, "put_state", killed_after_first_batch):
            with self.assertRaises(KeyboardInterrupt):
                migrate_pichash_padding.run(self.legacy_db, "pad", batch_size=num_functions_of_a)
        self.legacy = StorageFactory.getStorage(self.legacy_config)
        self.assertFalse(self.legacy.isPichashPadded())
        # sample a is padded now, b and c are not, and blocks shared between them differ in spelling
        self.assertEqual(0, self.legacy_db.functions.count_documents({"sample_id": self.sample_a, **migrate_pichash_padding.unpadded_block_query()}))
        self.assertGreater(self.legacy_db.functions.count_documents({"sample_id": self.sample_b, **migrate_pichash_padding.unpadded_block_query()}), 0)
        for index_complete in (True, False):
            self.legacy._setPicBlockHashIndexComplete(index_complete)
            for sample_ids in self._sample_sets():
                self.assertEqual(self.legacy_unique_blocks[tuple(sample_ids)], with_int_keys(self.legacy.getUniqueBlocks(sample_ids)), (index_complete, sample_ids))
        # finishing the migration lands where a fresh instance is
        self._migrate_legacy()
        self._assert_same_unique_blocks()

    def test_a_rollback_lands_on_what_an_old_instance_answers(self):
        report = migrate_pichash_padding.run(self.fresh_db, "unpad")
        self.assertEqual([], report["problems"])
        self.fresh = StorageFactory.getStorage(self.fresh_config)
        self.assertFalse(self.fresh.isPichashPadded())
        self.assertFalse(self.fresh._isPicBlockHashIndexComplete())
        for sample_ids in self._sample_sets():
            self.assertEqual(self.legacy.getUniqueBlocks(sample_ids), self.fresh.getUniqueBlocks(sample_ids), sample_ids)
        self.fresh.rebuildPicBlockHashIndex()
        for sample_ids in self._sample_sets():
            self.assertEqual(self.legacy.getUniqueBlocks(sample_ids), self.fresh.getUniqueBlocks(sample_ids), sample_ids)

    def _pichash_answers(self, storage, db):
        function_ids = [document["function_id"] for document in db.functions.find({}, {"function_id": 1}).sort("function_id", 1)]
        pichashes = sorted({int(document["_pichash"], 16) for document in db.functions.find({}, {"_pichash": 1})})
        answers = {
            "by_function_id": {function_id: storage.getPicHashMatchesByFunctionId(function_id) for function_id in function_ids},
            "is_pichash": {pichash: storage.isPicHash(pichash) for pichash in pichashes + [0x1234567]},
            "matches": {pichash: storage.getMatchesForPicHash(pichash) for pichash in pichashes},
            "num_pichashes": storage.getStats()["num_pichashes"],
        }
        for cutoff in (0, 1, 2, 3):
            storage._minhash_config.MINHASH_PICHASH_MAX_MATCHES = cutoff
            answers["cutoff_%d" % cutoff] = storage.getPicHashMatchesByFunctionIds(function_ids)
        storage._minhash_config.MINHASH_PICHASH_MAX_MATCHES = 0
        blocks = sorted({entry["hash"] for document in db.functions.find({}, {"_picblockhashes": 1}) for entry in document["_picblockhashes"]})
        answers["blocks"] = {int(block_hash, 16): storage.getMatchesForPicBlockHash(int(block_hash, 16)) for block_hash in blocks}
        return answers

    def test_pichash_lookups_and_cutoffs_after_the_migration(self):
        expected = self._pichash_answers(self.fresh, self.fresh_db)
        # the cutoff has to decide something: some values are dropped at 2, none at 3
        self.assertNotEqual(expected["cutoff_2"], expected["cutoff_3"])
        self.assertEqual(expected["cutoff_0"], expected["cutoff_3"])
        self.assertEqual(3, len(expected["matches"][SHARED_PICHASH]))
        self._migrate_legacy()
        # the counts were keyed on the old spelling: the cutoff counts holders until they are rebuilt
        self.assertFalse(self.legacy.isPicHashCountIndexComplete())
        self.assertEqual(0, self.legacy_db.pichash_counts.count_documents({}))
        self.assertEqual(expected, self._pichash_answers(self.legacy, self.legacy_db))
        self.legacy.rebuildPicHashCountIndex()
        self.assertTrue(self.legacy.isPicHashCountIndexComplete())
        self.assertEqual(
            {document["_pichash"]: document["df"] for document in self.fresh_db.pichash_counts.find({})},
            {document["_pichash"]: document["df"] for document in self.legacy_db.pichash_counts.find({})},
        )
        self.assertEqual(expected, self._pichash_answers(self.legacy, self.legacy_db))

    def _search_answers(self, config):
        index = MinHashIndex(config)
        answers = {}
        for term in ("pichash:0x4d2", "pichash:0x99", "pichash:!=0x4d2", "pichash:<0x1000", "pichash:>=0x99", "pichash:?4d2"):
            answers[term] = list(index.getFunctionSearchResults(term, limit=1000)["search_results"])
        for is_ascending in (True, False):
            answers["sorted_%s" % is_ascending] = list(
                index.getFunctionSearchResults("function_id:>=0", sort_by="pichash", is_ascending=is_ascending, limit=1000)["search_results"]
            )
        return answers

    def test_searches_after_the_migration(self):
        expected = self._search_answers(self.fresh_config)
        self.assertEqual(3, len(expected["pichash:0x4d2"]))
        self.assertEqual(4, len(expected["pichash:<0x1000"]))
        self._migrate_legacy()
        self.assertEqual(expected, self._search_answers(self.legacy_config))

    def test_verify_names_a_derived_index_of_the_other_width(self):
        self._migrate_legacy()
        # an index rebuilt from a stale backup, say: still unpadded, but marked complete
        for setting, collection_name, document in (
            ("picblockhash_index_complete", "picblockhashes", {"_id": "0x4d2", "sample_ids": [self.sample_a]}),
            ("pichash_count_index_complete", "pichash_counts", {"_pichash": "0x4d2", "df": 1}),
        ):
            self.legacy_db[collection_name].insert_one(document)
            report = migrate_pichash_padding.run(self.legacy_db, "verify")
            self.assertEqual([], report["problems"], "an index that is not marked complete is never read")
            self.legacy_db.settings.update_one({}, {"$set": {setting: True}})
            report = migrate_pichash_padding.run(self.legacy_db, "verify")
            self.assertEqual(1, len(report["problems"]), report)
            self.assertIn(collection_name, report["problems"][0])
            self.legacy_db[collection_name].delete_many({})
            self.legacy_db.settings.update_one({}, {"$set": {setting: False}})
