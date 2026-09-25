import inspect
import json
import re
import unittest
from unittest.mock import MagicMock, patch

from mcrit.client.McritClient import (
    McritBadRequest,
    McritClient,
    McritClientError,
    McritConflict,
    McritGone,
    McritNotFound,
    McritRequestError,
    McritServerError,
    McritUnauthorized,
    failure_message,
    handle_response,
)


def answer(status_code, body=None, url="http://mcrit.test/samples/7"):
    """A requests.Response stand-in: the status, the JSON body and the URL are all the client reads."""
    response = MagicMock(status_code=status_code, url=url)
    if body is None:
        response.json.side_effect = ValueError("no JSON")
        response.text = ""
    else:
        response.json.return_value = body
        response.text = json.dumps(body)
    return response


FAILED = {"status": "failed", "data": {"message": "We don't have a sample with that id."}}


class HandleResponseTest(unittest.TestCase):
    def test_by_default_every_failure_answers_none(self):
        for status in (400, 404, 410, 500, 501, 418):
            self.assertIsNone(handle_response(answer(status, FAILED)), status)
        self.assertIsNone(handle_response(answer(200, {"status": "failed", "data": {"message": "nope"}})))

    def test_a_success_still_answers_its_data(self):
        for flags in ({}, {"raise_client_errors": True}, {"raise_server_errors": True}):
            self.assertEqual({"sample_id": 7}, handle_response(answer(200, {"status": "successful", "data": {"sample_id": 7}}), **flags))
            self.assertEqual("job", handle_response(answer(202, {"status": "successful", "data": "job"}), **flags))

    def test_request_errors_raise_their_own_class_with_the_servers_message(self):
        for status, cls in (
            (400, McritBadRequest),
            (401, McritUnauthorized),
            (403, McritUnauthorized),
            (404, McritNotFound),
            (409, McritConflict),
            (410, McritGone),
            (418, McritRequestError),
        ):
            with self.assertRaises(cls) as raised:
                handle_response(answer(status, FAILED), raise_client_errors=True)
            self.assertIsInstance(raised.exception, McritRequestError)
            self.assertIsInstance(raised.exception, McritClientError)
            self.assertEqual(status, raised.exception.status_code)
            self.assertEqual("We don't have a sample with that id.", raised.exception.message)
            self.assertEqual("http://mcrit.test/samples/7", raised.exception.url)
            self.assertIn("We don't have a sample with that id.", str(raised.exception))
            # the other mode leaves them alone
            self.assertIsNone(handle_response(answer(status, FAILED), raise_server_errors=True))

    def test_server_errors_raise_only_in_the_server_mode(self):
        for status in (500, 501, 302):
            with self.assertRaises(McritServerError) as raised:
                handle_response(answer(status, FAILED), raise_server_errors=True)
            self.assertNotIsInstance(raised.exception, McritRequestError)
            self.assertEqual(status, raised.exception.status_code)
            self.assertIsNone(handle_response(answer(status, FAILED), raise_client_errors=True))

    def test_a_failed_two_hundred_is_a_server_error(self):
        with self.assertRaises(McritServerError) as raised:
            handle_response(answer(200, {"status": "failed", "data": {"message": "Failed to modify family."}}), raise_server_errors=True)
        self.assertEqual("Failed to modify family.", raised.exception.message)

    def test_a_body_that_is_not_mcrits_failure_shape_gives_an_empty_message(self):
        self.assertEqual("", failure_message(answer(502)))  # proxy answered with no JSON
        self.assertEqual("", failure_message(answer(500, ["not", "a", "dict"])))
        self.assertEqual("", failure_message(answer(500, {"status": "failed", "data": "just a string"})))
        with self.assertRaises(McritServerError) as raised:
            handle_response(answer(502), raise_server_errors=True)
        self.assertEqual("", raised.exception.message)
        self.assertIn("no message", str(raised.exception))


class ClientModesTest(unittest.TestCase):
    def test_the_default_client_keeps_answering_none(self):
        client = McritClient("http://mcrit.test")
        with patch("mcrit.client.McritClient.requests.get", return_value=answer(404, FAILED)):
            self.assertIsNone(client.getSampleById(7))
        with patch("mcrit.client.McritClient.requests.get", return_value=answer(500, FAILED)):
            self.assertIsNone(client.getSampleById(7))

    def test_a_raising_client_raises_through_its_methods(self):
        client = McritClient("http://mcrit.test", raise_client_errors=True, raise_server_errors=True)
        with patch("mcrit.client.McritClient.requests.get", return_value=answer(404, FAILED)):
            with self.assertRaises(McritNotFound):
                client.getSampleById(7)
        with patch("mcrit.client.McritClient.requests.get", return_value=answer(500, FAILED)):
            with self.assertRaises(McritServerError):
                client.getFamily(1)
        with patch("mcrit.client.McritClient.requests.delete", return_value=answer(400, FAILED)):
            with self.assertRaises(McritBadRequest):
                client.deleteFamily(1)

    def test_modify_family_raises_through_the_client_mode_too(self):
        """modifyFamily grew its actors on a branch written before these modes existed, where
        it called handle_response directly - which answers None whatever mode the client is in."""
        client = McritClient("http://mcrit.test", raise_client_errors=True)
        with patch("mcrit.client.McritClient.requests.put", return_value=answer(400, FAILED, url="http://mcrit.test/families/3")):
            with self.assertRaises(McritBadRequest):
                client.modifyFamily(3, actors=["APT-1"])

    def test_the_maintenance_jobs_raise_through_the_client_mode(self):
        """rebuildPicBlockHashIndex, repairMinHashes and recomputeFamilyStats landed while the
        modes were being written and kept calling handle_response directly, so they answered
        None however the client was built."""
        client = McritClient("http://mcrit.test", raise_client_errors=True, raise_server_errors=True)
        for method, verb in (("rebuildPicBlockHashIndex", "get"), ("repairMinHashes", "post"), ("recomputeFamilyStats", "post")):
            with self.subTest(method=method):
                with patch(f"mcrit.client.McritClient.requests.{verb}", return_value=answer(500, FAILED)):
                    with self.assertRaises(McritServerError):
                        getattr(client, method)()
                with patch(f"mcrit.client.McritClient.requests.{verb}", return_value=answer(401, FAILED)):
                    with self.assertRaises(McritUnauthorized):
                        getattr(client, method)()

    def test_no_method_parses_outside_the_client_mode(self):
        """The ratchet behind the case above: a method that hands its response to
        handle_response itself, rather than to self._handle, ignores the mode it was built in -
        and nothing fails until someone relies on the mode."""
        self.assertEqual([], re.findall(r"(?<![\w.])handle_response\(response\)", inspect.getsource(McritClient)))

    def test_modify_function_raises_through_the_client_mode_too(self):
        """modifyFunction was written before these modes existed, on a branch that merged them
        in later - a method that calls handle_response directly answers None whatever mode the
        client is in."""
        client = McritClient("http://mcrit.test", raise_client_errors=True)
        with patch("mcrit.client.McritClient.requests.put", return_value=answer(404, FAILED, url="http://mcrit.test/functions/7")):
            with self.assertRaises(McritNotFound):
                client.modifyFunction(7, "decrypt_config")

    def test_one_mode_does_not_imply_the_other(self):
        server_only = McritClient("http://mcrit.test", raise_server_errors=True)
        with patch("mcrit.client.McritClient.requests.get", return_value=answer(404, FAILED)):
            self.assertIsNone(server_only.getSampleById(7))
        with patch("mcrit.client.McritClient.requests.get", return_value=answer(500, FAILED)):
            with self.assertRaises(McritServerError):
                server_only.getSampleById(7)

    def test_raw_mode_hands_out_the_response_whatever_the_status(self):
        client = McritClient("http://mcrit.test", raw_responses=True, raise_client_errors=True, raise_server_errors=True)
        response = answer(500, FAILED)
        with patch("mcrit.client.McritClient.requests.get", return_value=response):
            self.assertIs(response, client.getSampleById(7))
            self.assertIs(response, client.getFunctionById(7))
            self.assertIs(response, client.isFunctionId(7))

    def test_a_four_xx_never_reads_as_a_server_failure(self):
        """401 (AuthMiddleware) and 409 (an existing binary) are answers the server gives on
        purpose; with both modes on they must come back as request errors, not as a backend
        failure, and with the server mode alone they stay None."""
        for status in (401, 403, 409, 418):
            with self.assertRaises(McritRequestError):
                handle_response(answer(status, FAILED), raise_client_errors=True, raise_server_errors=True)
            self.assertIsNone(handle_response(answer(status, FAILED), raise_server_errors=True))

    def test_the_rebuild_endpoints_raise_through_the_client_mode(self):
        """rebuildFunctionRangeIndex and rebuildBandDfIndex were written before the modes and
        handed their response to handle_response directly, so they answered None however the
        client was built."""
        client = McritClient("http://mcrit.test", raise_client_errors=True, raise_server_errors=True)
        for method in ("rebuildFunctionRangeIndex", "rebuildBandDfIndex"):
            with self.subTest(method=method):
                with patch("mcrit.client.McritClient.requests.get", return_value=answer(500, FAILED)):
                    with self.assertRaises(McritServerError):
                        getattr(client, method)()
                with patch("mcrit.client.McritClient.requests.get", return_value=answer(401, FAILED)):
                    with self.assertRaises(McritUnauthorized):
                        getattr(client, method)()


class GetQueueDataTest(unittest.TestCase):
    """getQueueData's query string for the sample_ids/job_ids selectors, and raw mode."""

    @staticmethod
    def _success(data=None):
        return answer(200, {"status": "successful", "data": data if data is not None else []})

    def test_sample_ids_and_job_ids_are_sent_as_comma_separated_lists(self):
        client = McritClient("http://mcrit.test")
        with patch("mcrit.client.McritClient.requests.get", return_value=self._success()) as mock_get:
            client.getQueueData(method="getMatchesForSample", sample_ids=[7, 8, 9], job_ids=["a1b2c3d4e5f6a1b2c3d4e5f6"])
        url = mock_get.call_args.args[0]
        self.assertIn("method=getMatchesForSample", url)
        self.assertIn("sample_ids=7,8,9", url)
        self.assertIn("job_ids=a1b2c3d4e5f6a1b2c3d4e5f6", url)

    def test_neither_parameter_is_sent_when_not_given(self):
        client = McritClient("http://mcrit.test")
        with patch("mcrit.client.McritClient.requests.get", return_value=self._success()) as mock_get:
            client.getQueueData()
        url = mock_get.call_args.args[0]
        self.assertNotIn("sample_ids", url)
        self.assertNotIn("job_ids", url)

    def test_an_empty_list_is_still_sent_present_but_empty(self):
        client = McritClient("http://mcrit.test")
        with patch("mcrit.client.McritClient.requests.get", return_value=self._success()) as mock_get:
            client.getQueueData(method="getMatchesForSample", sample_ids=[], job_ids=[])
        url = mock_get.call_args.args[0]
        self.assertIn("sample_ids=&", url)
        self.assertTrue(url.endswith("job_ids="))

    def test_raw_mode_returns_the_response_untouched(self):
        client = McritClient("http://mcrit.test", raw_responses=True)
        response = self._success()
        with patch("mcrit.client.McritClient.requests.get", return_value=response):
            self.assertIs(response, client.getQueueData(sample_ids=[1, 2], job_ids=["x"]))


if __name__ == "__main__":
    unittest.main()
