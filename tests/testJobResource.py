import json
import unittest
from unittest.mock import MagicMock

import falcon
import falcon.testing

from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.server.JobResource import JobResource

from .context import config

RESULT = {"matches": {"functions": [[1, 2, 3]], "samples": [[4, 5]]}, "info": {"job": {"parameters": "x"}}}
STORED = json.dumps(RESULT).encode("ascii")
JOB_ID = "0123456789abcdef01234567"
RESULT_ID = "fedcba9876543210fedcba98"
MATCHING_JOB = {"payload": {"method": "getMatchesForSample", "params": "[7]"}, "_id": JOB_ID}


class ResultRoutesServeTheStoredBytes(unittest.TestCase):
    """A stored result is JSON already; the routes put its bytes into the envelope instead of
    parsing and re-serialising them (#152), except for compact, which has to edit the dict."""

    @staticmethod
    def _request(query_string=""):
        return falcon.Request(falcon.testing.create_environ(path="/jobs", query_string=query_string))

    def _resource(self, stored=STORED, job_data=MATCHING_JOB):
        index = MagicMock()
        index.getJobData.return_value = job_data
        index.getJobIdForResult.return_value = JOB_ID
        index.getResultBytesForJob.return_value = stored
        index.getResultBytes.return_value = stored
        index.getResultForJob.return_value = json.loads(stored) if stored is not None else None
        index.getResult.return_value = json.loads(stored) if stored is not None else None
        return index, JobResource(index)

    def test_the_job_result_route_passes_the_bytes_through(self):
        index, resource = self._resource()
        resp = falcon.Response()
        resource.on_get_job_result(self._request(), resp, JOB_ID)
        assert resp.data is not None
        self.assertEqual(b'{"status": "successful", "data": ' + STORED + b"}", resp.data)
        self.assertEqual({"status": "successful", "data": RESULT}, json.loads(resp.data))
        index.getResultForJob.assert_not_called()

    def test_the_result_route_passes_the_bytes_through(self):
        index, resource = self._resource()
        resp = falcon.Response()
        resource.on_get_results(self._request(), resp, RESULT_ID)
        assert resp.data is not None
        self.assertEqual({"status": "successful", "data": RESULT}, json.loads(resp.data))
        index.getResultBytes.assert_called_once_with(RESULT_ID)
        index.getResult.assert_not_called()

    def test_compact_still_parses_and_drops_the_function_matches(self):
        for route, argument in ((JobResource.on_get_job_result, JOB_ID), (JobResource.on_get_results, RESULT_ID)):
            index, resource = self._resource()
            resp = falcon.Response()
            route(resource, self._request("compact=true"), resp, argument)
            assert resp.data is not None
            self.assertEqual({"samples": [[4, 5]]}, json.loads(resp.data)["data"]["matches"])
            index.getResultBytesForJob.assert_not_called()
            index.getResultBytes.assert_not_called()

    def test_compact_leaves_a_non_matching_job_alone(self):
        index, resource = self._resource(job_data={"payload": {"method": "getUniqueBlocks", "params": "[[1]]"}, "_id": JOB_ID})
        resp = falcon.Response()
        resource.on_get_job_result(self._request("compact=true"), resp, JOB_ID)
        assert resp.data is not None
        self.assertEqual(RESULT, json.loads(resp.data)["data"])

    def test_a_missing_result_is_still_null(self):
        for query in ("", "compact=true"):
            index, resource = self._resource(stored=None)
            resp = falcon.Response()
            resource.on_get_job_result(self._request(query), resp, JOB_ID)
            assert resp.data is not None
            self.assertEqual({"status": "successful", "data": None}, json.loads(resp.data))

    def test_an_invalid_id_is_rejected_before_any_lookup(self):
        index, resource = self._resource()
        resp = falcon.Response()
        resource.on_get_job_result(self._request(), resp, "nope")
        self.assertEqual(falcon.HTTP_400, resp.status)
        index.getResultBytesForJob.assert_not_called()


class StoredBytesAccessors(unittest.TestCase):
    def test_the_index_hands_out_the_bytes_the_result_was_stored_with(self):
        index = MinHashIndex(config)
        result_id = index.queue._dicts_to_grid(RESULT, metadata={"result": True, "job": "none"})
        self.assertEqual(STORED, index.getResultBytes(result_id))
        self.assertEqual(RESULT, index.getResult(result_id))
        self.assertIsNone(index.getResultBytesForJob("0123456789abcdef01234567"))


class JobCollectionSelectorsTest(unittest.TestCase):
    """GET /jobs?sample_ids=...&job_ids=...: parsing, the sample_ids-without-method 400, and
    forwarding "present but empty" as an empty list rather than as no selector at all."""

    @staticmethod
    def _request(query_string=""):
        return falcon.Request(falcon.testing.create_environ(path="/jobs", query_string=query_string))

    def _resource(self):
        index = MagicMock()
        index.getQueueData.return_value = []
        return index, JobResource(index)

    def test_sample_ids_without_method_is_a_400_and_never_queries(self):
        index, resource = self._resource()
        resp = falcon.Response()
        resource.on_get_collection(self._request("sample_ids=7,8,9"), resp)
        self.assertEqual(falcon.HTTP_400, resp.status)
        index.getQueueData.assert_not_called()

    def test_sample_ids_parses_as_ints_and_ignores_invalid_entries(self):
        index, resource = self._resource()
        resp = falcon.Response()
        resource.on_get_collection(self._request("method=getMatchesForSample&sample_ids=7,x,9,"), resp)
        self.assertNotEqual(falcon.HTTP_400, resp.status)
        kwargs = index.getQueueData.call_args.kwargs
        self.assertEqual("getMatchesForSample", kwargs["method"])
        self.assertEqual([7, 9], kwargs["sample_ids"])
        self.assertIsNone(kwargs["job_ids"])

    def test_sample_ids_present_but_all_invalid_is_forwarded_as_an_empty_list(self):
        index, resource = self._resource()
        resp = falcon.Response()
        resource.on_get_collection(self._request("method=getMatchesForSample&sample_ids=x,y"), resp)
        kwargs = index.getQueueData.call_args.kwargs
        self.assertEqual([], kwargs["sample_ids"])

    def test_job_ids_does_not_require_method(self):
        index, resource = self._resource()
        resp = falcon.Response()
        resource.on_get_collection(self._request("job_ids=0123456789abcdef01234567,fedcba9876543210fedcba98"), resp)
        self.assertNotEqual(falcon.HTTP_400, resp.status)
        kwargs = index.getQueueData.call_args.kwargs
        self.assertEqual(["0123456789abcdef01234567", "fedcba9876543210fedcba98"], kwargs["job_ids"])
        self.assertIsNone(kwargs["sample_ids"])

    def test_job_ids_present_but_empty_is_forwarded_as_an_empty_list(self):
        index, resource = self._resource()
        resp = falcon.Response()
        resource.on_get_collection(self._request("job_ids=,,"), resp)
        kwargs = index.getQueueData.call_args.kwargs
        self.assertEqual([], kwargs["job_ids"])

    def test_neither_parameter_forwards_none(self):
        index, resource = self._resource()
        resp = falcon.Response()
        resource.on_get_collection(self._request(""), resp)
        kwargs = index.getQueueData.call_args.kwargs
        self.assertIsNone(kwargs["sample_ids"])
        self.assertIsNone(kwargs["job_ids"])

    def test_a_repeated_selector_reads_like_its_comma_joined_form(self):
        """falcon hands a repeated parameter over as a list, which the parsers used to call .split on: a 500."""
        for route in ("collection", "count"):
            with self.subTest(route=route):
                index, resource = self._resource()
                index.getQueueCount.return_value = 0
                responder = getattr(resource, f"on_get_{route}")
                getter = index.getQueueData if route == "collection" else index.getQueueCount
                resp = falcon.Response()
                responder(self._request("method=getMatchesForSample&sample_ids=7,8&sample_ids=9&job_ids=0123456789abcdef01234567&job_ids=abcdefabcdefabcdefabcdef"), resp)
                self.assertNotEqual(falcon.HTTP_400, resp.status)
                kwargs = getter.call_args.kwargs
                self.assertEqual([7, 8, 9], kwargs["sample_ids"])
                self.assertEqual(["0123456789abcdef01234567", "abcdefabcdefabcdefabcdef"], kwargs["job_ids"])

    def test_both_selectors_together_with_state_and_filter(self):
        index, resource = self._resource()
        resp = falcon.Response()
        resource.on_get_collection(
            self._request("method=getMatchesForSample&sample_ids=7,8&job_ids=0123456789abcdef01234567&state=finished&filter=x&start=5&limit=10"),
            resp,
        )
        kwargs = index.getQueueData.call_args.kwargs
        self.assertEqual([7, 8], kwargs["sample_ids"])
        self.assertEqual(["0123456789abcdef01234567"], kwargs["job_ids"])
        self.assertEqual("finished", kwargs["state"])
        self.assertEqual("x", kwargs["filter"])
        self.assertEqual(5, kwargs["start_index"])
        self.assertEqual(10, kwargs["limit"])


if __name__ == "__main__":
    unittest.main()
