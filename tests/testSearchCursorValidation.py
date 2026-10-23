import base64
import json
import os
import types
import unittest
from unittest import mock

import falcon
import falcon.testing
import pymongo
import pymongo.collection
import pymongo.errors
import pytest
from smda.common.SmdaReport import SmdaReport

from mcrit.config.McritConfig import McritConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.index.SearchCursor import FullSearchCursor, MinimalSearchCursor
from mcrit.server.StatusResource import StatusResource
from mcrit.storage import MongoDbStorage as mongo_db_storage_module
from mcrit.storage.MongoDbStorage import MongoDbStorage
from mcrit.storage.StorageFactory import StorageFactory

from .context import config, getTestMongoServerAndPort

EXAMPLE_REPORT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "example_report.smda")
FAMILY_NAMES = ["fam0", "fam1", "fam2", "fam3", "fam4"]


def encode_cursor(data):
    return base64.urlsafe_b64encode(json.dumps(data).encode()).decode()


class SearchCursorShapeTestSuite(unittest.TestCase):
    """A cursor comes back from the client: its values went into the storage query as they were, so a
    JSON object in it became a MongoDB query operator on the sort field - {"$regex": ...} made mongod
    run an attacker's backtracking pattern over every function of the corpus"""

    def testCursorsThisServerIssuesAreAccepted(self):
        for record_values in (["fam2", 3], [7], [None, 1], [True, 2], [1.5, 0], ["0x1f", 9], [0xFFFFFFFF80001000, 4], [2**64 - 1, 5], [-(2**63), 6]):
            for direction in (0, 1):
                with self.subTest(record_values=record_values, direction=direction):
                    cursor = MinimalSearchCursor.fromStr(encode_cursor([record_values, direction]))
                    assert cursor is not None
                    self.assertEqual(record_values, cursor.record_values)
                    self.assertEqual(bool(direction), cursor.is_forward_search)

    def testMalformedCursorsAreValueErrors(self):
        malformed = [
            [[{"$regex": "^fam[13]$"}, -1], 1],
            [[{"$in": ["fam0", "fam4"]}], 1],
            [[["fam1"], 1], 1],
            [["fam1", 1], {"$gt": 0}],
            [["fam1", 1], 2],
            [[10**30, 1], 1],
            [[-(10**30)], 1],
            [[2**64, 1], 1],
            [[-(2**63) - 1], 1],
            [["fam1", 1]],
            {"0": ["fam1"], "1": 1},
            "fam1",
            7,
        ]
        for data in malformed:
            with self.subTest(data=data):
                with self.assertRaises(ValueError):
                    MinimalSearchCursor.fromStr(encode_cursor(data))

    def testCursorIssuedPastAKernelSpaceOffsetIsAccepted(self):
        # paging by offset past 0xffffffff8... in memory: the server has to take back the cursor it issued
        issued = MinimalSearchCursor()
        issued.record_values, issued.sort_spec = [0xFFFFFFFF80001000, 17], ["offset", "function_id"]
        cursor = MinimalSearchCursor.fromStr(issued.toStr())
        assert cursor is not None
        self.assertEqual([0xFFFFFFFF80001000, 17], cursor.record_values)

    def testSortOfCursorIsChecked(self):
        for sort_spec in ("family_name", [], [1], [["family_name"]], {"$ne": 1}, ["family name"], ["family_name\n"], ["$where"], ["-"], ["family_name"] * 9, ["a" * 80]):
            with self.subTest(sort_spec=sort_spec):
                with self.assertRaises(ValueError):
                    MinimalSearchCursor.fromStr(encode_cursor([["fam1"], 1, sort_spec]))
        cursor = MinimalSearchCursor.fromStr(encode_cursor([["fam1", 3], 1, ["family_name", "-family_id", "statistics.num_functions"]]))
        assert cursor is not None
        self.assertEqual(["family_name", "-family_id", "statistics.num_functions"], cursor.sort_spec)
        self.assertEqual(("fam1", 3), tuple(cursor.record_values))

    def testCursorMustMatchTheSortOrder(self):
        cursor = MinimalSearchCursor.fromStr(encode_cursor([["fam2"], 1]))
        with self.assertRaises(ValueError):
            FullSearchCursor(cursor, [("family_name", True), ("family_id", True)])
        self.assertEqual(["fam2"], FullSearchCursor(cursor, [("family_name", True)]).record_values)


class MemorySearchCursorTestSuite(unittest.TestCase):
    mongo_db_name = None

    def setUp(self):
        mcrit_config = McritConfig()
        if self.mongo_db_name is None:
            mcrit_config.STORAGE_CONFIG = StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MEMORY)
        else:
            server, port = getTestMongoServerAndPort()
            mcrit_config.STORAGE_CONFIG = StorageConfig(
                STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB, STORAGE_SERVER=server, STORAGE_PORT=port, STORAGE_MONGODB_DBNAME=self.mongo_db_name
            )
        mcrit_config.QUEUE_CONFIG = config.QUEUE_CONFIG
        self.index = MinHashIndex(mcrit_config)
        self.index.getStorage().clearStorage()
        for family_name in FAMILY_NAMES:
            self.index.getStorage().addFamily(family_name)

    def _search(self, cursor_data, params="sort_by=family_name"):
        request = falcon.Request(falcon.testing.create_environ(path="/search/families", query_string="query=&" + params + "&cursor=" + encode_cursor(cursor_data)))
        response = falcon.Response()
        StatusResource(self.index).on_get_search_families(request, response)
        assert response.data is not None
        return response.status, json.loads(response.data)

    def testPagingByCursorStillWorks(self):
        status, payload = self._search([["fam2", -1], 1])
        self.assertEqual(falcon.HTTP_200, status)
        self.assertEqual(["fam2", "fam3", "fam4"], [entry["family_name"] for entry in payload["data"]["search_results"].values()])

    def testOperatorCursorIsAnsweredAsBadInput(self):
        for value in ({"$regex": "^fam[13]$"}, {"$in": ["fam0", "fam4"]}, {"$where": "sleep(100) || true"}):
            with self.subTest(value=value):
                status, payload = self._search([[value, -1], 1])
                self.assertEqual(falcon.HTTP_400, status)
                self.assertEqual("failed", payload["status"])

    def testCursorOfAnotherSortFieldIsAnsweredAsBadInputInMemory(self):
        # same number of values, but a family_id that is a string: the in-memory comparison fails
        # (MongoDB takes such a cursor as it is and answers a page)
        if self.mongo_db_name is not None:
            self.skipTest("MongoDB does not compare the values itself")
        status, _ = self._search([["fam2", "fam3"], 1])
        self.assertEqual(falcon.HTTP_400, status)

    def _issued_cursor(self, params="sort_by=family_name"):
        request = falcon.Request(falcon.testing.create_environ(path="/search/families", query_string="query=&limit=2&" + params))
        response = falcon.Response()
        StatusResource(self.index).on_get_search_families(request, response)
        assert response.data is not None
        return json.loads(response.data)["data"]["cursor"]["forward"]

    def _search_with(self, cursor_str, params):
        request = falcon.Request(falcon.testing.create_environ(path="/search/families", query_string="query=&" + params + "&cursor=" + cursor_str))
        response = falcon.Response()
        StatusResource(self.index).on_get_search_families(request, response)
        assert response.data is not None
        return response.status, json.loads(response.data)

    def testIssuedCursorsNameTheirSort(self):
        cursor = MinimalSearchCursor.fromStr(self._issued_cursor())
        assert cursor is not None
        self.assertEqual(["family_name", "family_id"], cursor.sort_spec)
        cursor = MinimalSearchCursor.fromStr(self._issued_cursor("sort_by=family_name&is_ascending=false"))
        assert cursor is not None
        self.assertEqual(["-family_name", "-family_id"], cursor.sort_spec)

    def _backward_cursor(self, params="sort_by=family_name"):
        # the backward cursor of the second page
        status, payload = self._search_with(self._issued_cursor(params), params + "&limit=2")
        self.assertEqual(falcon.HTTP_200, status)
        backward = payload["data"]["cursor"]["backward"]
        assert backward is not None
        return backward

    def testIssuedBackwardCursorsNameTheirSort(self):
        cursor = MinimalSearchCursor.fromStr(self._backward_cursor())
        assert cursor is not None
        self.assertFalse(cursor.is_forward_search)
        self.assertEqual(["family_name", "family_id"], cursor.sort_spec)
        cursor = MinimalSearchCursor.fromStr(self._backward_cursor("sort_by=family_name&is_ascending=false"))
        assert cursor is not None
        self.assertEqual(["-family_name", "-family_id"], cursor.sort_spec)

    def testBackwardCursorReplayedUnderAnotherSortIsAnsweredAsBadInput(self):
        cursor = self._backward_cursor("sort_by=family_name")
        status, _ = self._search_with(cursor, "sort_by=family_name")
        self.assertEqual(falcon.HTTP_200, status)
        for params in ("sort_by=family_id", "sort_by=family_name&is_ascending=false", "sort_by=num_samples"):
            with self.subTest(params=params):
                status, payload = self._search_with(cursor, params)
                self.assertEqual(falcon.HTTP_400, status)
                self.assertEqual("failed", payload["status"])

    def testIssuedCursorPagesUnderItsOwnSort(self):
        for params in ("sort_by=family_name", "sort_by=family_name&is_ascending=false", "sort_by=family_id"):
            with self.subTest(params=params):
                status, payload = self._search_with(self._issued_cursor(params), params)
                self.assertEqual(falcon.HTTP_200, status)
                # the storage holds the unknown family 0 next to the named ones; the issued page took two
                self.assertEqual(len(FAMILY_NAMES) + 1 - 2, len(payload["data"]["search_results"]))

    def testCursorReplayedUnderAnotherSortIsAnsweredAsBadInput(self):
        cursor = self._issued_cursor("sort_by=family_name")
        for params in ("sort_by=family_id", "sort_by=family_name&is_ascending=false", "sort_by=num_samples"):
            with self.subTest(params=params):
                status, payload = self._search_with(cursor, params)
                self.assertEqual(falcon.HTTP_400, status)
                self.assertEqual("failed", payload["status"])

    def testLegacyCursorStillPages(self):
        # [values, direction] without a sort, as an older server issued them
        status, payload = self._search([["fam2", -1], 1], "sort_by=family_name&is_ascending=false")
        self.assertEqual(falcon.HTTP_200, status)
        status, payload = self._search([["fam2", -1], 1])
        self.assertEqual(falcon.HTTP_200, status)
        self.assertEqual(["fam2", "fam3", "fam4"], [entry["family_name"] for entry in payload["data"]["search_results"].values()])

    def testMalformedSortOfCursorIsAnsweredAsBadInput(self):
        for sort_spec in ("family_name", [], [1, 2], [["family_name"]], {"$ne": 1}, ["family name", "family_id"], ["$where", "family_id"], ["family_name"] * 9, ["a" * 80]):
            with self.subTest(sort_spec=sort_spec):
                status, _ = self._search([["fam2", -1], 1, sort_spec])
                self.assertEqual(falcon.HTTP_400, status)
        status, _ = self._search([["fam2", -1], 1, ["family_name", "family_id"], "extra"])
        self.assertEqual(falcon.HTTP_400, status)

    def testCursorOfAnotherSortOrderIsAnsweredAsBadInput(self):
        status, _ = self._search([["fam2"], 1])
        self.assertEqual(falcon.HTTP_400, status)


@pytest.mark.mongo
class MongoSearchCursorTestSuite(MemorySearchCursorTestSuite):
    """The operator only ever reached MongoDB: the in-memory search compares Python values"""

    mongo_db_name = "test_search_cursor_mcrit"

    @classmethod
    def tearDownClass(cls):
        server, port = getTestMongoServerAndPort()
        pymongo.MongoClient(server, int(port)).drop_database(cls.mongo_db_name)

    def setUp(self):
        super().setUp()
        with open(EXAMPLE_REPORT) as fjson:
            report = SmdaReport.fromDict(json.load(fjson))
        assert report is not None
        self.index.getStorage().addSmdaReport(report)

    ROUTES = {
        "families": ("on_get_search_families", "query=fam"),
        "samples": ("on_get_search_samples", "query=a"),
        # a substring term on the function name also runs the scan of the distinct names
        "functions": ("on_get_search_functions", "query=sub"),
        # and one on the labels the distinct of the labels
        "function_labels": ("on_get_search_functions", "query=function_labels.function_label:%3Fsub"),
        # with a condition on another field it is the plain query: neither probed nor listed for
        "function_labels_in_sample": ("on_get_search_functions", "query=sample_id:0%20function_labels.function_label:%3Fsub"),
    }

    def _route(self, route):
        method, params = self.ROUTES[route]
        request = falcon.Request(falcon.testing.create_environ(path="/search/" + route, query_string=params))
        response = falcon.Response()
        getattr(StatusResource(self.index), method)(request, response)
        assert response.data is not None
        return response.status, json.loads(response.data)

    def _calls_of_search(self, route, storage_config=None):
        storage = self.index.getStorage()
        calls = {"find": [], "aggregate": [], "distinct": []}
        real = {name: getattr(pymongo.collection.Collection, name) for name in calls}

        def spy_on(name):
            def spy(collection, *args, **kwargs):
                calls[name].append(kwargs)
                return real[name](collection, *args, **kwargs)

            return spy

        saved = storage._storage_config
        if storage_config is not None:
            storage._storage_config = storage_config
        try:
            with (
                mock.patch.object(pymongo.collection.Collection, "find", spy_on("find")),
                mock.patch.object(pymongo.collection.Collection, "aggregate", spy_on("aggregate")),
                mock.patch.object(pymongo.collection.Collection, "distinct", spy_on("distinct")),
            ):
                status, _ = self._route(route)
        finally:
            storage._storage_config = saved
        self.assertEqual(falcon.HTTP_200, status)
        return calls

    def testSearchQueriesCarryTheTimeLimit(self):
        # a label search is probed first, see testLabelSearchCarriesTheTimeLimit
        for route in [route for route in self.ROUTES if route != "function_labels"]:
            with self.subTest(route=route):
                calls = self._calls_of_search(route)
                self.assertEqual([30000], [kwargs["max_time_ms"] for kwargs in calls["find"]])
                if route == "functions":
                    self.assertEqual([30000], [kwargs["maxTimeMS"] for kwargs in calls["aggregate"]])
                    self.assertEqual([], calls["distinct"])
                limited = StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB)
                limited.STORAGE_MONGODB_SEARCH_MAX_TIME_MS = 1234
                self.index.getStorage()._over_cap_since.clear()
                calls = self._calls_of_search(route, limited)
                self.assertEqual([1234], [kwargs["max_time_ms"] for kwargs in calls["find"]])
                if route == "functions":
                    self.assertEqual([1234], [kwargs["maxTimeMS"] for kwargs in calls["aggregate"]])
                    self.assertEqual([], calls["distinct"])
                limited.STORAGE_MONGODB_SEARCH_MAX_TIME_MS = 0
                self.index.getStorage()._over_cap_since.clear()
                calls = self._calls_of_search(route, limited)
                self.assertEqual([None], [kwargs["max_time_ms"] for kwargs in calls["find"]])
                if route == "functions":
                    self.assertEqual([0], [kwargs["maxTimeMS"] for kwargs in calls["aggregate"]])
                    self.assertEqual([], calls["distinct"])

    def _calls_of_label_search(self, search_max_time_ms, probe_ms=None, listing_ms=0):
        """The time limits a label search hands MongoDB, on a simulated clock: the probe answers, or
        runs out after probe_ms, and the listing of the labels takes listing_ms."""
        storage = self.index.getStorage()
        storage._over_cap_since.clear()
        clock = [1000.0]
        calls = {"find": [], "distinct": []}
        real_find, real_distinct = pymongo.collection.Collection.find, pymongo.collection.Collection.distinct

        def find(collection, *args, **kwargs):
            calls["find"].append(kwargs["max_time_ms"])
            if "hint" in kwargs and probe_ms is not None:
                clock[0] += probe_ms / 1000
                raise pymongo.errors.ExecutionTimeout("operation exceeded time limit")
            return real_find(collection, *args, **kwargs)

        def distinct(collection, *args, **kwargs):
            calls["distinct"].append(kwargs["maxTimeMS"])
            if 0 < kwargs["maxTimeMS"] <= listing_ms:
                # MongoDB stops it at its limit
                clock[0] += kwargs["maxTimeMS"] / 1000
                raise pymongo.errors.ExecutionTimeout("operation exceeded time limit")
            clock[0] += listing_ms / 1000
            return real_distinct(collection, *args, **kwargs)

        limited = StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB)
        limited.STORAGE_MONGODB_SEARCH_MAX_TIME_MS = search_max_time_ms
        saved = storage._storage_config
        storage._storage_config = limited
        try:
            with (
                mock.patch.object(pymongo.collection.Collection, "find", find),
                mock.patch.object(pymongo.collection.Collection, "distinct", distinct),
                mock.patch.object(mongo_db_storage_module, "time", types.SimpleNamespace(monotonic=lambda: clock[0])),
            ):
                status, payload = self._route("function_labels")
        finally:
            storage._storage_config = saved
        return status, calls, payload

    def testLabelSearchCarriesTheTimeLimit(self):
        probe = MongoDbStorage._LABEL_PROBE_TIME_MS
        # the probe answers within its own short limit, and nothing is listed
        status, calls, probed = self._calls_of_label_search(30000)
        self.assertEqual(falcon.HTTP_200, status)
        self.assertEqual({"find": [probe], "distinct": []}, calls)
        # a probe that runs out is not the search running out: the labels are listed with half of what
        # the probe left of the limit, and the query that follows gets what the listing left
        status, calls, listed = self._calls_of_label_search(30000, probe_ms=probe + 3, listing_ms=200)
        self.assertEqual(falcon.HTTP_200, status)
        self.assertEqual(probe, calls["find"][0])
        self.assertAlmostEqual((30000 - probe - 3) / 2, calls["distinct"][0], delta=1)
        self.assertAlmostEqual(30000 - probe - 3 - 200, calls["find"][1], delta=1)
        self.assertEqual(probed["data"]["search_results"], listed["data"]["search_results"])
        # no limit stays no limit
        status, calls, _ = self._calls_of_label_search(0, probe_ms=probe + 3)
        self.assertEqual({"find": [probe, None], "distinct": [0]}, calls)
        # a search limit no longer than the probe's leaves no room for one
        status, calls, _ = self._calls_of_label_search(probe)
        self.assertEqual({"find": [probe], "distinct": [probe // 2]}, calls)
        # a listing too slow for its half leaves the other half to the regex, which answers the search
        status, calls, too_slow = self._calls_of_label_search(1000, probe_ms=probe + 3, listing_ms=1000)
        self.assertEqual(falcon.HTTP_200, status)
        half = (1000 - probe - 3) // 2
        self.assertEqual({"find": [probe, 1000 - probe - 3 - half], "distinct": [half]}, calls)
        self.assertEqual(probed["data"]["search_results"], too_slow["data"]["search_results"])
        # and is remembered (on the simulated clock, which the check cannot read any more)
        self.assertIn(("functions", "function_labels.function_label"), self.index.getStorage()._over_cap_since)

    def testConfigWithoutTheSettingGetsTheDefault(self):
        # docker-mcrit replaces the config modules with older copies that do not name the setting
        storage = object.__new__(MongoDbStorage)
        setattr(storage, "_storage_config", types.SimpleNamespace())
        self.assertEqual(30000, storage._getSearchMaxTimeMS())

    def testSearchThatRunsOutOfTimeIsAnsweredAsBadInput(self):
        def time_out(*args, **kwargs):
            raise pymongo.errors.ExecutionTimeout("operation exceeded time limit")

        for route in self.ROUTES:
            with self.subTest(route=route):
                with mock.patch.object(pymongo.collection.Collection, "find", time_out):
                    status, payload = self._route(route)
                self.assertEqual(falcon.HTTP_400, status)
                self.assertEqual("failed", payload["status"])
                self.assertIn("took too long", payload["data"]["message"])

    def testSearchTooLargeForMongoDBIsAnsweredAsBadInput(self):
        def too_large(*args, **kwargs):
            raise pymongo.errors.DocumentTooLarge("BSON document too large")

        for route in self.ROUTES:
            with self.subTest(route=route):
                with mock.patch.object(pymongo.collection.Collection, "find", too_large):
                    status, payload = self._route(route)
                self.assertEqual(falcon.HTTP_400, status)
                self.assertIn("too large", payload["data"]["message"])

    def testDistinctNameScanThatRunsOutOfTimeFallsBackAndIsNotRepeated(self):
        real_aggregate = pymongo.collection.Collection.aggregate
        attempts = []

        def time_out_on_scan(collection, pipeline, *args, **kwargs):
            if any("$group" in stage for stage in pipeline):
                attempts.append(1)
                raise pymongo.errors.ExecutionTimeout("operation exceeded time limit")
            return real_aggregate(collection, pipeline, *args, **kwargs)

        self.index.getStorage()._over_cap_since.clear()
        with mock.patch.object(pymongo.collection.Collection, "aggregate", time_out_on_scan):
            for _ in range(3):
                status, _ = self._route("functions")
                self.assertEqual(falcon.HTTP_200, status)
        self.assertEqual(1, len(attempts))

    def testDistinctLabelScanThatRunsOutOfTimeFallsBackAndIsNotRepeated(self):
        attempts = []

        def time_out(collection, *args, **kwargs):
            attempts.append(1)
            raise pymongo.errors.ExecutionTimeout("operation exceeded time limit")

        self.index.getStorage()._over_cap_since.clear()
        # without a probe before it, the listing has the search's whole limit: running out of that is a verdict
        with mock.patch.object(pymongo.collection.Collection, "distinct", time_out), mock.patch.object(self.index.getStorage(), "_canProbe", return_value=False):
            for _ in range(3):
                status, _ = self._route("function_labels")
                self.assertEqual(falcon.HTTP_200, status)
        self.assertEqual(1, len(attempts))


if __name__ == "__main__":
    unittest.main()
