import datetime
import json
import unittest
import uuid
from unittest.mock import ANY, MagicMock, call

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
            if route is JobResource.on_get_results:
                # the stored bytes are what tells an unknown result from a stored null
                index.getResultBytes.assert_called_once_with(RESULT_ID)
                index.getResult.assert_not_called()
            else:
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

    def test_a_stored_json_null_result_is_present_in_both_modes(self):
        for query in ("", "compact=true"):
            with self.subTest(query=query):
                index, resource = self._resource(stored=b"null")
                resp = falcon.Response()
                resource.on_get_results(self._request(query), resp, RESULT_ID)
                self.assertEqual(falcon.HTTP_200, resp.status)
                assert resp.data is not None
                self.assertEqual({"status": "successful", "data": None}, json.loads(resp.data))
                index.getResultBytes.assert_called_once_with(RESULT_ID)
                index.getResult.assert_not_called()

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


class JobIdsInMemoryMode(unittest.TestCase):
    """Memory storage with the fake queue must be reachable through the job routes (#203).

    LocalQueue minted uuid4 ids, which the routes reject as not being 24 hex characters, so no
    job or result could be looked up in that mode. It now mints ObjectIds, as MongoQueue does.
    """

    def setUp(self):
        self.index = MinHashIndex(config)
        resource = JobResource(self.index)
        app = falcon.App()
        app.add_route("/jobs/{job_id}", resource)
        app.add_route("/jobs/{job_id}/result", resource, suffix="job_result")
        app.add_route("/results/{result_id}", resource, suffix="results")
        app.add_route("/results/{result_id}/job", resource, suffix="result_job")
        self.client = falcon.testing.TestClient(app)

    def test_queue_ids_look_like_the_ids_mongoqueue_hands_out(self):
        # the job id comes from LocalQueue.put, the result id from LocalQueue._file_to_grid
        job_id = self.index.recomputeFamilyStats()
        result_id = self.index.getJobData(job_id)["result"]
        for queue_id in (job_id, result_id):
            self.assertRegex(queue_id, "^[0-9a-f]{24}$")

    def test_every_id_is_new(self):
        # a repeated id does not fail cleanly: the second job's result lands on the first one's
        # entry and awaiting it polls forever, so this has to be caught here, in milliseconds
        queue = self.index.queue
        job_ids = {queue.put({"method": "noSuchMethod", "descriptor": f"d{number}", "file_params": "{}"}) for number in range(20)}
        file_ids = {queue._file_to_grid(b"x") for _ in range(20)}
        self.assertEqual((20, 20), (len(job_ids), len(file_ids)))

    def test_an_unknown_file_id_has_no_metadata(self):
        # None, not an empty dict: delete_job indexes what it gets, and should fail loudly on a missing entry
        queue = self.index.queue
        self.assertIsNone(queue._grid_to_meta("0" * 24))
        self.assertIsNone(queue._grid_to_file("0" * 24))
        self.assertIsNone(queue._grid_to_dicts("0" * 24))

    def test_a_job_and_its_result_can_be_fetched_by_id(self):
        job_id = self.index.recomputeFamilyStats()
        job = self.client.simulate_get(f"/jobs/{job_id}")
        self.assertEqual(falcon.HTTP_200, job.status)
        self.assertEqual(job_id, job.json["data"]["_id"])
        result_id = job.json["data"]["result"]
        expected_result = self.index.getResultForJob(job_id)
        for path in (f"/jobs/{job_id}/result", f"/results/{result_id}"):
            with self.subTest(path=path):
                response = self.client.simulate_get(path)
                self.assertEqual(falcon.HTTP_200, response.status)
                self.assertEqual(expected_result, response.json["data"])
        result_job = self.client.simulate_get(f"/results/{result_id}/job")
        self.assertEqual(falcon.HTTP_200, result_job.status)
        self.assertEqual(job_id, result_job.json["data"]["_id"])

    def test_unknown_ids_answer_404_and_leave_the_queue_intact(self):
        """A lookup must not create entries: LocalQueue keeps its files in defaultdicts, and a None
        entry left behind by a lookup made the next clean() - and with it the next job - fail."""
        self.index.recomputeFamilyStats()  # a real job and result for clean() to walk past
        unknown = "0" * 24
        for path, query in (
            (f"/jobs/{unknown}", ""),
            (f"/jobs/{unknown}/result", ""),
            (f"/results/{unknown}", ""),
            (f"/results/{unknown}/job", ""),
            (f"/results/{unknown}", "compact=true"),
            (f"/jobs/{unknown}/result", "compact=true"),
        ):
            with self.subTest(path=path, query=query):
                response = self.client.simulate_get(path, query_string=query)
                self.assertEqual(falcon.HTTP_404, response.status)
                self.assertEqual("failed", response.json["status"])
                expected = "We don't have a job with that id." if path.startswith("/jobs/") else "We don't have a result with that id."
                self.assertEqual({"message": expected}, response.json["data"])
        queue = self.index.queue
        self.assertNotIn(None, list(queue._files.values()) + list(queue._files_meta.values()))
        queue.clean()

    def test_a_job_id_selection_in_upper_case_finds_the_job(self):
        # GET /jobs?job_ids=... (#210) matches case-insensitively on MongoQueue, which parses the ids
        job_id = self.index.recomputeFamilyStats()
        queue = self.index.queue
        self.assertEqual([job_id], [job.job_id for job in queue.get_jobs(0, 10, job_ids=[job_id.upper()])])
        self.assertEqual(1, queue.get_job_count(job_ids=[job_id.upper()]))

    def test_an_id_in_upper_case_finds_the_same_job_and_result(self):
        # MongoQueue parses either case into the same ObjectId; LocalQueue keys by the lower-case string
        job_id = self.index.recomputeFamilyStats()
        result_id = self.index.getJobData(job_id)["result"]
        self.assertEqual(job_id, self.client.simulate_get(f"/jobs/{job_id.upper()}").json["data"]["_id"])
        self.assertEqual(self.index.getResultForJob(job_id), self.client.simulate_get(f"/results/{result_id.upper()}").json["data"])
        self.assertEqual(job_id, self.client.simulate_get(f"/results/{result_id.upper()}/job").json["data"]["_id"])

    def test_a_job_without_a_result_can_be_deleted(self):
        """A failed job has no result; deleting it made LocalQueue delete the grid entry None, which
        created one, answered 500 and left the next clean() - and every job after it - failing."""
        job_id = self.index.getMatchesForSample(12345)  # no such sample: the job fails
        self.assertIsNone(self.index.getJobData(job_id)["result"])
        response = self.client.simulate_delete(f"/jobs/{job_id}")
        self.assertEqual(falcon.HTTP_200, response.status)
        self.assertEqual(1, response.json["data"]["num_deleted"])
        queue = self.index.queue
        self.assertNotIn(None, list(queue._files.values()) + list(queue._files_meta.values()))
        queue.clean()

    def test_a_job_can_be_deleted_by_id(self):
        job_id = self.index.recomputeFamilyStats()
        response = self.client.simulate_delete(f"/jobs/{job_id}")
        self.assertEqual(falcon.HTTP_200, response.status)
        self.assertEqual(1, response.json["data"]["num_deleted"])
        self.assertIsNone(self.index.getJobData(job_id))
        for missing_id in (job_id, "0" * 24):
            with self.subTest(missing_id=missing_id):
                missing = self.client.simulate_delete(f"/jobs/{missing_id}")
                self.assertEqual(falcon.HTTP_404, missing.status)
                self.assertEqual({"status": "failed", "data": {"message": "We don't have a job with that id."}}, missing.json)


class PendingAndOrphanedResults(unittest.TestCase):
    @staticmethod
    def _request(query_string=""):
        return falcon.Request(falcon.testing.create_environ(path="/jobs", query_string=query_string))

    def test_an_existing_job_without_a_result_answers_null_in_both_modes(self):
        for query in ("", "compact=true"):
            with self.subTest(query=query):
                index = MagicMock()
                index.getJobData.return_value = {"_id": JOB_ID, "result": None}
                index.getResultBytesForJob.return_value = None
                index.getResultForJob.return_value = None
                resp = falcon.Response()
                JobResource(index).on_get_job_result(self._request(query), resp, JOB_ID)
                self.assertEqual(falcon.HTTP_200, resp.status)
                assert resp.data is not None
                self.assertEqual({"status": "successful", "data": None}, json.loads(resp.data))

    def test_a_result_remains_readable_if_its_job_is_gone(self):
        index = MagicMock()
        index.getResultBytes.return_value = STORED
        index.getResult.return_value = json.loads(STORED)
        index.getJobIdForResult.return_value = JOB_ID
        index.getJobData.return_value = None
        for query in ("", "compact=true"):
            with self.subTest(query=query):
                resp = falcon.Response()
                JobResource(index).on_get_results(self._request(query), resp, RESULT_ID)
                self.assertEqual(falcon.HTTP_200, resp.status)
                assert resp.data is not None
                self.assertEqual(RESULT, json.loads(resp.data)["data"])
        resp = falcon.Response()
        JobResource(index).on_get_result_job(self._request(), resp, RESULT_ID)
        self.assertEqual(falcon.HTTP_404, resp.status)
        assert resp.data is not None
        self.assertEqual({"status": "failed", "data": {"message": "We don't have the job for that result id."}}, json.loads(resp.data))

    def test_a_result_without_a_job_is_served_in_full(self):
        index = MagicMock()
        index.getResultBytes.return_value = STORED
        index.getJobIdForResult.return_value = None
        for query in ("", "compact=true"):
            with self.subTest(query=query):
                resp = falcon.Response()
                JobResource(index).on_get_results(self._request(query), resp, RESULT_ID)
                self.assertEqual(falcon.HTTP_200, resp.status)
                assert resp.data is not None
                self.assertEqual(RESULT, json.loads(resp.data)["data"])
        index.getJobData.assert_not_called()

    def test_an_unknown_result_has_no_job_either(self):
        index = MagicMock()
        index.getResultBytes.return_value = None
        index.getJobIdForResult.return_value = None
        resp = falcon.Response()
        JobResource(index).on_get_result_job(self._request(), resp, RESULT_ID)
        self.assertEqual(falcon.HTTP_404, resp.status)
        assert resp.data is not None
        self.assertEqual({"status": "failed", "data": {"message": "We don't have a result with that id."}}, json.loads(resp.data))
        index.getJobData.assert_not_called()


class InvalidIdsAreRejected(unittest.TestCase):
    """Every id route answers 400 before touching the index for anything that is not exactly 24 hex characters."""

    INVALID = {
        "uuid4": str(uuid.uuid4()),
        "23 hex": "0" * 23,
        "25 hex": "0" * 25,
        "24 hex and a suffix": JOB_ID + "-x",
        "24 hex and a newline": JOB_ID + "\n",
        "24 non-hex": "g" * 24,
        "empty": "",
        "missing": None,
    }

    def test_each_route_rejects_each_invalid_id(self):
        routes = {
            "on_get": JobResource.on_get,
            "on_delete": JobResource.on_delete,
            "on_get_results": JobResource.on_get_results,
            "on_get_job_result": JobResource.on_get_job_result,
            "on_get_result_job": JobResource.on_get_result_job,
        }
        for route_name, route in routes.items():
            for label, invalid_id in self.INVALID.items():
                with self.subTest(route=route_name, id=label):
                    index = MagicMock()
                    resp = falcon.Response()
                    route(JobResource(index), falcon.Request(falcon.testing.create_environ(path="/jobs")), resp, invalid_id)
                    self.assertEqual(falcon.HTTP_400, resp.status)
                    # the only call allowed is the audit log entry db_log_msg writes for the rejection
                    self.assertEqual([call._storage.dbLogEvent(ANY, username="anonymous")], index.method_calls)

    def test_a_valid_id_reaches_the_index_in_lower_case(self):
        for valid_id in (JOB_ID, JOB_ID.upper()):
            with self.subTest(id=valid_id):
                index = MagicMock()
                index.getJobData.return_value = None
                resp = falcon.Response()
                JobResource(index).on_get(falcon.Request(falcon.testing.create_environ(path="/jobs")), resp, valid_id)
                self.assertNotEqual(falcon.HTTP_400, resp.status)
                index.getJobData.assert_called_once_with(JOB_ID)


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


class DeletingJobsByFilter(unittest.TestCase):
    """DELETE /jobs narrows by every filter it is given, and refuses one it can't read."""

    def setUp(self):
        self.index = MinHashIndex(config)
        app = falcon.App()
        app.add_route("/jobs", JobResource(self.index), suffix="collection")
        self.client = falcon.testing.TestClient(app)

    def test_a_date_that_does_not_parse_is_a_400_and_deletes_nothing(self):
        job_id = self.index.recomputeFamilyStats()
        for name in ("created_before", "finished_before"):
            for value in ("2026-13-45", "yesterday", "2026-01-01 00:00:00", ""):
                with self.subTest(name=name, value=value):
                    response = self.client.simulate_delete("/jobs", params={name: value}, params_csv=False)
                    self.assertEqual(falcon.HTTP_400, response.status)
                    self.assertIsNotNone(self.index.getJobData(job_id))

    def test_a_repeated_date_is_a_400_too(self):
        job_id = self.index.recomputeFamilyStats()
        response = self.client.simulate_delete("/jobs", query_string="created_before=2000-01-01&created_before=2999-01-01")
        self.assertEqual(falcon.HTTP_400, response.status)
        self.assertIsNotNone(self.index.getJobData(job_id))

    def test_both_date_forms_still_select(self):
        job_id = self.index.recomputeFamilyStats()
        for value in ("2000-01-01", "2000-01-01T00:00:00"):
            response = self.client.simulate_delete("/jobs", params={"created_before": value})
            self.assertEqual((falcon.HTTP_200, 0), (response.status, response.json["data"]["num_deleted"]))
        response = self.client.simulate_delete("/jobs", params={"created_before": "2999-01-01", "method": "recomputeFamilyStats"})
        self.assertEqual((falcon.HTTP_200, 1), (response.status, response.json["data"]["num_deleted"]))
        self.assertIsNone(self.index.getJobData(job_id))

    def test_the_memory_queue_applies_every_filter(self):
        """LocalQueue.delete_jobs ignored created_before, failed without finished_before and deleted nothing without a method."""
        queue = self.index.queue
        family_job = self.index.recomputeFamilyStats()
        failed_job = self.index.getMatchesForSample(12345)  # no such sample: fails, never finishes
        self.assertEqual(0, queue.delete_jobs(method="noSuchMethod"))
        self.assertEqual(0, queue.delete_jobs(created_before=datetime.datetime(2000, 1, 1)))
        # the failed job has no finished_at, so only the finished one is old enough
        self.assertEqual(1, queue.delete_jobs(finished_before=datetime.datetime(2999, 1, 1)))
        self.assertIsNone(self.index.getJobData(family_job))
        self.assertIsNotNone(self.index.getJobData(failed_job))
        self.assertEqual(1, queue.delete_jobs(method="getMatchesForSample", created_before=datetime.datetime(2999, 1, 1)))
