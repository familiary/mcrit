import json
import logging
import os
import unittest
from typing import Any, cast
from unittest.mock import MagicMock, patch

import falcon
import falcon.testing
import pymongo
import pytest
from smda.common.SmdaReport import SmdaReport

from mcrit.config.McritConfig import McritConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.server.FunctionResource import FunctionResource
from mcrit.server.StatusResource import StatusResource
from mcrit.storage.StorageFactory import StorageFactory

from .context import config, getTestMongoServerAndPort

logging.disable(logging.CRITICAL)

EXAMPLE_REPORT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "example_report.smda")


def request(method, query_string="", body=""):
    return falcon.Request(falcon.testing.create_environ(path="/functions", method=method, query_string=query_string, body=body))


class FunctionListingBoundsTestSuite(unittest.TestCase):
    """GET /functions without parameters loaded every function with its disassembly into the server
    process (about ten times the stored size), and POST /functions ran a lookup per id, duplicates
    included, for any number of ids"""

    @classmethod
    def setUpClass(cls):
        cls.index = MinHashIndex(config)
        with open(EXAMPLE_REPORT) as fjson:
            report = SmdaReport.fromDict(json.load(fjson))
        assert report is not None
        cls.index.getStorage().addSmdaReport(report)
        cls.num_functions = len(cls.index.getStorage().getFunctions(0, 0))

    def _get(self, query_string):
        response = falcon.Response()
        FunctionResource(self.index).on_get_collection(request("GET", query_string), response)
        assert response.data is not None
        return json.loads(response.data)["data"]

    def testListingIsServedAPageAtATime(self):
        self.assertGreater(self.num_functions, 3)
        self.assertEqual(self.num_functions, len(self._get("")))
        with patch.object(FunctionResource, "MAX_FUNCTIONS_PER_PAGE", 3):
            for query_string in ("", "limit=0", "limit=-1", "limit=1000000", "limit=x"):
                with self.subTest(query_string=query_string):
                    self.assertEqual(3, len(self._get(query_string)))
            self.assertEqual(2, len(self._get("limit=2")))
            # paging by start reaches every function
            pages = [self._get("start=%d&limit=3" % start) for start in range(0, self.num_functions, 3)]
            self.assertEqual(self.num_functions, len({function_id for page in pages for function_id in page}))

    def testIdsAreLookedUpOnceEach(self):
        index = MagicMock()
        index.getFunctionsByIds.return_value = {}
        response = falcon.Response()
        FunctionResource(index).on_post_collection(request("POST", body=",".join(["7"] * 5000 + ["8", "7"])), response)
        self.assertEqual(falcon.HTTP_200, response.status)
        index.getFunctionsByIds.assert_called_once_with([7, 8])

    def testTooManyIdsAreRefusedBeforeAnyLookup(self):
        index = MagicMock()
        response = falcon.Response()
        body = ",".join(str(function_id) for function_id in range(FunctionResource.MAX_FUNCTION_IDS + 1))
        FunctionResource(index).on_post_collection(request("POST", body=body), response)
        self.assertEqual(falcon.HTTP_400, response.status)
        index.getFunctionsByIds.assert_not_called()
        response = falcon.Response()
        body = ",".join(str(function_id) for function_id in range(FunctionResource.MAX_FUNCTION_IDS))
        index.getFunctionsByIds.return_value = {}
        FunctionResource(index).on_post_collection(request("POST", body=body), response)
        self.assertEqual(falcon.HTTP_200, response.status)
        self.assertEqual(FunctionResource.MAX_FUNCTION_IDS, len(index.getFunctionsByIds.call_args.args[0]))

    def testKnownIdsAreAnswered(self):
        function_ids = sorted(int(function_id) for function_id in self._get("limit=3"))
        response = falcon.Response()
        FunctionResource(self.index).on_post_collection(request("POST", body=",".join(str(function_id) for function_id in function_ids * 2)), response)
        assert response.data is not None
        self.assertEqual(function_ids, sorted(int(function_id) for function_id in json.loads(response.data)["data"]))


class FunctionIdsThroughClientTestSuite(unittest.TestCase):
    """McritClient.getFunctionsByIds asked for all ids in one POST, which the server answers 400 above its cap"""

    @classmethod
    def setUpClass(cls):
        cls.index = MinHashIndex(config)
        with open(EXAMPLE_REPORT) as fjson:
            report = SmdaReport.fromDict(json.load(fjson))
        assert report is not None
        cls.index.getStorage().addSmdaReport(report)
        cls.stored_ids = sorted(function_entry.function_id for function_entry in cls.index.getStorage().getFunctions(0, 0))

    def _client(self):
        from mcrit.client.McritClient import McritClient

        posts = []

        def post(url, data=None, headers=None, timeout=None):
            assert data is not None
            posts.append(data.count(",") + 1)
            response = falcon.Response()
            FunctionResource(self.index).on_post_collection(request("POST", body=data), response)
            reply = MagicMock()
            reply.status_code = int(str(response.status).split()[0])
            reply.json.return_value = json.loads(response.data) if response.data else {}
            return reply

        client = McritClient(mcrit_server="http://mcrit.invalid")
        client._session = MagicMock(post=post)
        return client, posts

    def testMoreIdsThanTheCapAreAnswered(self):
        client, posts = self._client()
        unknown_ids = list(range(10**6, 10**6 + 2 * FunctionResource.MAX_FUNCTION_IDS))
        function_entries = client.getFunctionsByIds(unknown_ids[:15000] + self.stored_ids + unknown_ids[15000:] + self.stored_ids)
        self.assertEqual(self.stored_ids, sorted(function_entries))
        self.assertGreater(len(posts), 2)
        self.assertLessEqual(max(posts), FunctionResource.MAX_FUNCTION_IDS)

    def testRawModeSendsOneRequestAndReturnsTheServersAnswer(self):
        from mcrit.client.McritClient import McritClient

        client, posts = self._client()
        raw_client = McritClient(mcrit_server="http://mcrit.invalid", raw_responses=True)
        raw_client._session = client._session
        ids = list(range(2 * FunctionResource.MAX_FUNCTION_IDS))
        response = cast(Any, raw_client.getFunctionsByIds(ids))
        self.assertEqual([len(ids)], posts)
        self.assertEqual(400, response.status_code)

    def testFailedChunkFailsTheCall(self):
        from mcrit.client.McritClient import McritClient

        client, _ = self._client()
        real_post = client._session.post
        calls = []

        def post(url, data=None, headers=None, timeout=None):
            calls.append(1)
            if len(calls) == 2:
                reply = MagicMock()
                reply.status_code = 500
                return reply
            return real_post(url, data=data, headers=headers, timeout=timeout)

        client._session = MagicMock(post=post)
        ids = self.stored_ids + list(range(10**6, 10**6 + 2 * McritClient.MAX_FUNCTION_IDS_PER_REQUEST))
        self.assertEqual({}, client.getFunctionsByIds(ids))
        self.assertEqual(2, len(calls))

    def testNoIdsAreAnsweredEmpty(self):
        client, _ = self._client()
        self.assertEqual({}, client.getFunctionsByIds([]))


class SearchLimitTestSuite(unittest.TestCase):
    """GET /search/functions?limit=-1 asked the storage for limit + 1 = 0 results, which MongoDB's find()
    reads as no limit, and a large limit was taken as it came: either loaded every function into the
    server process, as GET /functions without a limit did"""

    mongo_db_name = None

    @classmethod
    def setUpClass(cls):
        mcrit_config = McritConfig()
        if cls.mongo_db_name is None:
            mcrit_config.STORAGE_CONFIG = StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MEMORY)
        else:
            server, port = getTestMongoServerAndPort()
            mcrit_config.STORAGE_CONFIG = StorageConfig(
                STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB, STORAGE_SERVER=server, STORAGE_PORT=port, STORAGE_MONGODB_DBNAME=cls.mongo_db_name
            )
        mcrit_config.QUEUE_CONFIG = config.QUEUE_CONFIG
        cls.index = MinHashIndex(mcrit_config)
        cls.index.getStorage().clearStorage()
        with open(EXAMPLE_REPORT) as fjson:
            report = SmdaReport.fromDict(json.load(fjson))
        assert report is not None
        cls.index.getStorage().addSmdaReport(report)
        cls.num_functions = len(cls.index.getFunctionSearchResults("", limit=1000)["search_results"])

    def _search(self, query_string):
        request = falcon.Request(falcon.testing.create_environ(path="/search/functions", query_string="query=&" + query_string))
        response = falcon.Response()
        StatusResource(self.index).on_get_search_functions(request, response)
        assert response.data is not None
        return json.loads(response.data)["data"]

    def testStorageLooksUpIdsInOneBatch(self):
        stored = {entry.function_id: entry for entry in self.index.getStorage().getFunctions(0, 0)}
        found = self.index.getStorage().getFunctionsByIds([*stored, 10**9, -5])
        self.assertEqual(sorted(stored), sorted(found))
        for function_id, function_entry in found.items():
            self.assertEqual(stored[function_id].function_name, function_entry.function_name)
            self.assertIsNone(function_entry.xcfg)
        self.assertEqual({}, self.index.getStorage().getFunctionsByIds([]))

    def testSearchIsServedAPageAtATime(self):
        self.assertGreater(self.num_functions, 3)
        with patch.object(MinHashIndex, "MAX_SEARCH_RESULTS", 3, create=True):
            for query_string in ("limit=-1", "limit=0", "limit=1000000"):
                with self.subTest(query_string=query_string):
                    self.assertEqual(3, len(self._search(query_string)["search_results"]))
            self.assertEqual(2, len(self._search("limit=2")["search_results"]))
            # paging by the forward cursor still reaches every function
            seen = set()
            result = self.index.getFunctionSearchResults("", limit=-1)
            while True:
                seen.update(result["search_results"])
                if result["cursor"]["forward"] is None:
                    break
                result = self.index.getFunctionSearchResults("", cursor=result["cursor"]["forward"], limit=-1)
            self.assertEqual(self.num_functions, len(seen))


@pytest.mark.mongo
class MongoSearchLimitTestSuite(SearchLimitTestSuite):
    """limit=-1 only loaded everything on MongoDB: find(limit=0) is unlimited there"""

    mongo_db_name = "test_function_listing_bounds_mcrit"

    @classmethod
    def tearDownClass(cls):
        server, port = getTestMongoServerAndPort()
        pymongo.MongoClient(server, int(port)).drop_database(cls.mongo_db_name)


if __name__ == "__main__":
    unittest.main()
