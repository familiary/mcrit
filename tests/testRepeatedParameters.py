"""A query parameter given more than once is refused before any responder runs (RepeatedParameterMiddleware).

falcon hands a repeated parameter over as a list. The responders read single values and called str
and int methods on it - a 500 - or passed it on to storage and the queue as a list.
"""

import json
import unittest
from unittest.mock import MagicMock

import falcon
import falcon.testing

from mcrit.server.application_routes import REPEATABLE_PARAMETERS, RepeatedParameterMiddleware
from mcrit.server.FamilyResource import FamilyResource
from mcrit.server.JobResource import JobResource
from mcrit.server.StatusResource import StatusResource


class RepeatedParameterTest(unittest.TestCase):
    def setUp(self):
        self.index = MagicMock()
        self.index.getQueueData.return_value = []
        self.index.getQueueCount.return_value = 0
        app = falcon.App(middleware=[RepeatedParameterMiddleware()])
        jobs = JobResource(self.index)
        app.add_route("/jobs", jobs, suffix="collection")
        app.add_route("/jobs/count", jobs, suffix="count")
        app.add_route("/jobs/stats", jobs, suffix="stats")
        app.add_route("/jobs/{job_id}/result", jobs, suffix="job_result")
        app.add_route("/families", FamilyResource(self.index), suffix="collection")
        app.add_route("/status", StatusResource(self.index), suffix="status")
        self.client = falcon.testing.TestClient(app)

    def test_a_repeat_is_a_400_naming_it_and_nothing_runs(self):
        for method, path, query in (
            ("GET", "/jobs", "start=1&start=2"),
            ("GET", "/jobs", "limit=1&limit=2"),
            ("GET", "/jobs", "ascending=true&ascending=false"),
            ("GET", "/jobs", "method=a&method=b"),
            ("GET", "/jobs/count", "state=finished&state=failed"),
            ("GET", "/jobs/count", "filter=a&filter=b"),
            ("GET", "/jobs/count", "username=a&username=b"),
            ("GET", "/jobs/stats", "with_refresh=true&with_refresh=false"),
            ("DELETE", "/jobs", "method=a&method=b"),
            ("DELETE", "/jobs", "finished_before=2026-01-01&finished_before=2026-02-01"),
            ("GET", "/jobs/0123456789abcdef01234567/result", "compact=true&compact=true"),
            ("GET", "/families", "start=1&start=2"),
            ("GET", "/status", "with_pichash=true&with_pichash=false"),
        ):
            with self.subTest(method=method, path=path, query=query):
                self.index.reset_mock()
                response = self.client.simulate_request(method, path, query_string=query)
                self.assertEqual(400, response.status_code)
                self.assertEqual("failed", response.json["status"])
                name = query.split("=")[0]
                self.assertEqual(f"Given more than once: {name}.", response.json["data"]["message"])
                # the responder never ran: no storage or queue call of any kind
                self.assertEqual([], self.index.mock_calls)

    def test_every_repeated_parameter_is_named(self):
        response = self.client.simulate_get("/jobs", query_string="start=1&start=2&limit=1&limit=3&state=failed")
        self.assertEqual("Given more than once: limit, start.", json.loads(response.text)["data"]["message"])

    def test_the_comma_list_selectors_may_repeat(self):
        self.assertEqual({"sample_ids", "job_ids"}, set(REPEATABLE_PARAMETERS))
        response = self.client.simulate_get(
            "/jobs", query_string="method=getMatchesForSample&sample_ids=7,8&sample_ids=9&job_ids=0123456789abcdef01234567&job_ids=abcdefabcdefabcdefabcdef"
        )
        self.assertEqual(200, response.status_code)
        kwargs = self.index.getQueueData.call_args.kwargs
        self.assertEqual([7, 8, 9], kwargs["sample_ids"])
        self.assertEqual(["0123456789abcdef01234567", "abcdefabcdefabcdefabcdef"], kwargs["job_ids"])

    def test_single_parameters_pass(self):
        response = self.client.simulate_get("/jobs", query_string="start=1&limit=2&method=getMatchesForSample")
        self.assertEqual(200, response.status_code)
        self.assertEqual((1, 2), (self.index.getQueueData.call_args.kwargs["start_index"], self.index.getQueueData.call_args.kwargs["limit"]))

    def test_an_unknown_route_keeps_its_404(self):
        # the check runs after routing, so a request that routes nowhere is told so
        self.assertEqual(404, self.client.simulate_get("/nonexistent", query_string="a=1&a=2").status_code)


class ClientEncodingTest(unittest.TestCase):
    """getJobCount and deleteQueueData build their queries from params, so a value holding "&" cannot become a second parameter."""

    def test_values_are_encoded_not_spliced(self):
        import datetime
        from unittest.mock import patch

        from mcrit.client.McritClient import McritClient

        client = McritClient("http://mcrit.test")
        answer = MagicMock(status_code=200)
        answer.json.return_value = {"status": "successful", "data": {"sample_info": {}, "job_id": None}}
        cases = (
            ("get", lambda: client.getJobCount(filter="a&state=failed"), {"filter": "a&state=failed"}),
            (
                "delete",
                lambda: client.deleteQueueData(method="m&x=1", finished_before=datetime.datetime(2026, 1, 1)),
                {"method": "m&x=1", "finished_before": "2026-01-01T00:00:00"},
            ),
        )
        for verb, call, expected in cases:
            with self.subTest(verb), patch(f"mcrit.client.McritClient.requests.{verb}", return_value=answer) as request:
                try:
                    call()
                except Exception:
                    pass  # only what was sent matters here
                self.assertNotIn("?", request.call_args.args[0])
                self.assertEqual(expected, request.call_args.kwargs["params"])

    def test_a_submission_keeps_the_query_string_its_caller_encoded(self):
        """MCRITweb percent-encodes filename, family and version itself; params= would encode them twice."""
        from unittest.mock import patch
        from urllib.parse import quote

        from mcrit.client.McritClient import McritClient

        client = McritClient("http://mcrit.test")
        answer = MagicMock(status_code=200)
        answer.json.return_value = {"status": "successful", "data": {"sample_info": {}, "job_id": None}}
        filename, family = quote("sample (1)&x.exe", safe=""), quote("Emotet Loader", safe="")
        with patch("mcrit.client.McritClient.requests.post", return_value=answer) as request:
            try:
                client.addBinarySample(b"MZ", filename=filename, family=family, version="1")
            except Exception:
                pass  # only what was sent matters here
        self.assertEqual(f"http://mcrit.test/samples/binary?filename={filename}&family={family}&version=1", request.call_args.args[0])
        self.assertNotIn("params", request.call_args.kwargs)


class ServedAppTest(unittest.TestCase):
    def test_the_served_app_refuses_a_repeat(self):
        from unittest.mock import patch

        from mcrit.server import application_routes

        with patch.object(application_routes, "create_index", return_value=MagicMock()):
            client = falcon.testing.TestClient(application_routes.get_app())
        self.assertEqual(400, client.simulate_get("/jobs", query_string="start=1&start=2").status_code)


if __name__ == "__main__":
    unittest.main()
