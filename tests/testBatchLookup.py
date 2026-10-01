import json
import unittest
from unittest.mock import MagicMock, patch

import falcon
import falcon.testing

from mcrit.client.McritClient import DEFAULT_TIMEOUT, McritClient, McritNotFound
from mcrit.server.application_routes import get_app
from mcrit.server.FamilyResource import FamilyResource
from mcrit.server.SampleResource import SampleResource
from mcrit.storage.FamilyEntry import FamilyEntry
from mcrit.storage.SampleEntry import SampleEntry


def _entry(as_dict):
    """A stand-in for a SampleEntry/FamilyEntry: only .toDict() is used by the resources."""
    mock = MagicMock()
    mock.toDict.return_value = as_dict
    return mock


def _sample_dict(sample_id, sha256="a" * 64):
    return {
        "architecture": "x86",
        "base_addr": 0,
        "binary_size": 100,
        "binweight": 1.0,
        "bitness": 32,
        "component": "",
        "family_id": 0,
        "family": "",
        "filename": "f.bin",
        "is_library": False,
        "sample_id": sample_id,
        "sha256": sha256,
        "smda_version": "1.0",
        "statistics": {},
        "timestamp": "2024-01-01T00-00-00",
        "version": "",
    }


def _family_dict(family_id, family_name):
    return {
        "family_id": family_id,
        "family_name": family_name,
        "num_samples": 0,
        "num_functions": 0,
        "num_library_samples": 0,
    }


def _answer(status_code, body):
    """A requests.Response stand-in, as tests/testClientErrors.py builds one."""
    response = MagicMock(status_code=status_code, url="http://mcrit.test/samples/ids")
    response.json.return_value = body
    response.text = json.dumps(body)
    return response


class SampleResourceByIdsTest(unittest.TestCase):
    """POST /samples/ids, mirroring FunctionResource.on_post_collection (#111's batch read
    exposed over the API)."""

    @staticmethod
    def _request(body=b""):
        env = falcon.testing.create_environ(path="/samples/ids", method="POST", body=body)
        return falcon.Request(env)

    def _resource(self):
        index = MagicMock()
        return index, SampleResource(index)

    def test_batch_lookup_returns_entries_keyed_by_id(self):
        index, resource = self._resource()
        index.getSamplesByIds.return_value = {5: _entry({"sample_id": 5}), -1: _entry({"sample_id": -1})}
        resp = falcon.Response()
        resource.on_post_by_ids(self._request(b"5, -1"), resp)
        index.getSamplesByIds.assert_called_once_with([5, -1])
        assert resp.data is not None
        payload = json.loads(resp.data)
        self.assertEqual(falcon.HTTP_200, resp.status)
        self.assertEqual("successful", payload["status"])
        self.assertEqual({"5": {"sample_id": 5}, "-1": {"sample_id": -1}}, payload["data"])

    def test_unknown_ids_are_simply_absent(self):
        index, resource = self._resource()
        index.getSamplesByIds.return_value = {}
        resp = falcon.Response()
        resource.on_post_by_ids(self._request(b"424242"), resp)
        index.getSamplesByIds.assert_called_once_with([424242])
        assert resp.data is not None
        self.assertEqual({}, json.loads(resp.data)["data"])

    def test_duplicate_ids_are_passed_through_once_each_by_the_index(self):
        # the index/storage layer is what dedupes; the resource just forwards the ids it parsed
        index, resource = self._resource()
        index.getSamplesByIds.return_value = {5: _entry({"sample_id": 5})}
        resp = falcon.Response()
        resource.on_post_by_ids(self._request(b"5,5"), resp)
        index.getSamplesByIds.assert_called_once_with([5, 5])
        assert resp.data is not None
        self.assertEqual({"5": {"sample_id": 5}}, json.loads(resp.data)["data"])

    def test_an_empty_body_is_a_400_with_a_message(self):
        index, resource = self._resource()
        resp = falcon.Response()
        resource.on_post_by_ids(self._request(b""), resp)
        index.getSamplesByIds.assert_not_called()
        self.assertEqual(falcon.HTTP_400, resp.status)
        assert resp.data is not None
        self.assertIn("can't be processed", json.loads(resp.data)["data"]["message"])

    def test_a_body_that_is_not_an_id_list_is_a_400(self):
        index, resource = self._resource()
        for malformed in (b"abc", b"1,,2", b"1;2", b"1.5"):
            with self.subTest(body=malformed):
                resp = falcon.Response()
                resource.on_post_by_ids(self._request(malformed), resp)
                index.getSamplesByIds.assert_not_called()
                self.assertEqual(falcon.HTTP_400, resp.status)


class FamilyResourceByIdsTest(unittest.TestCase):
    """POST /families/ids: same shape as the samples route, but no negative ids."""

    @staticmethod
    def _request(body=b""):
        env = falcon.testing.create_environ(path="/families/ids", method="POST", body=body)
        return falcon.Request(env)

    def _resource(self):
        index = MagicMock()
        return index, FamilyResource(index)

    def test_batch_lookup_returns_entries_keyed_by_id(self):
        index, resource = self._resource()
        index.getFamiliesByIds.return_value = {1: _entry({"family_id": 1}), 2: _entry({"family_id": 2})}
        resp = falcon.Response()
        resource.on_post_by_ids(self._request(b"1, 2"), resp)
        index.getFamiliesByIds.assert_called_once_with([1, 2])
        assert resp.data is not None
        payload = json.loads(resp.data)
        self.assertEqual(falcon.HTTP_200, resp.status)
        self.assertEqual({"1": {"family_id": 1}, "2": {"family_id": 2}}, payload["data"])

    def test_a_negative_id_is_rejected_as_malformed(self):
        index, resource = self._resource()
        resp = falcon.Response()
        resource.on_post_by_ids(self._request(b"-1"), resp)
        index.getFamiliesByIds.assert_not_called()
        self.assertEqual(falcon.HTTP_400, resp.status)

    def test_an_empty_body_is_a_400_with_a_message(self):
        index, resource = self._resource()
        resp = falcon.Response()
        resource.on_post_by_ids(self._request(b""), resp)
        index.getFamiliesByIds.assert_not_called()
        self.assertEqual(falcon.HTTP_400, resp.status)
        assert resp.data is not None
        self.assertIn("can't be processed", json.loads(resp.data)["data"]["message"])


class RouterTest(unittest.TestCase):
    """The new literal routes must not collide with the existing int-converter/sha256 routes.

    Runs through the real application_routes.get_app() route table via a WSGI TestClient, with
    create_index() patched out so no real (Mongo-backed by default) index is constructed. A
    successful POST is covered at the resource level above instead of here: falcon.testing's
    TestClient wraps the app with wsgiref.validate, which requires stream.read() to be called
    with an explicit size - post_body = req.stream.read() (mirroring FunctionResource, #111's
    pattern) is called with none, same as it would be for the existing /functions route. Checking
    dispatch (405 on the wrong method, 400 on an empty body, both before that read()) still proves
    the routing without tripping it.
    """

    def _client(self):
        index = MagicMock()
        with patch("mcrit.server.application_routes.create_index", return_value=index):
            app = get_app()
        return index, falcon.testing.TestClient(app)

    def test_samples_ids_resolves_to_the_new_responder_not_the_int_or_sha256_routes(self):
        index, client = self._client()
        # matched, wrong method -> 405 (a real miss/collision with {sample_id:int} would 404
        # instead, since "ids" does not parse as int; a collision with the sha256 route is ruled
        # out by construction, as it lives under /samples/sha256/...)
        response = client.simulate_get("/samples/ids")
        self.assertEqual(405, response.status_code)
        # matched, right method, empty body -> the resource's own 400, not a routing failure
        response = client.simulate_post("/samples/ids")
        self.assertEqual(400, response.status_code)

    def test_families_ids_resolves_to_the_new_responder_not_the_int_route(self):
        index, client = self._client()
        response = client.simulate_get("/families/ids")
        self.assertEqual(405, response.status_code)
        response = client.simulate_post("/families/ids")
        self.assertEqual(400, response.status_code)

    def test_existing_sample_routes_still_resolve_as_before(self):
        index, client = self._client()
        index.isSampleId.return_value = True
        index.getSampleById.return_value = _entry({"sample_id": 5})
        response = client.simulate_get("/samples/5")
        self.assertEqual(200, response.status_code)
        index.isSampleId.assert_called_once_with(5)
        self.assertEqual({"sample_id": 5}, response.json["data"])

        index.getSampleBySha256.return_value = _entry({"sample_id": 5})
        response = client.simulate_get("/samples/sha256/" + "a" * 64)
        self.assertEqual(200, response.status_code)
        index.getSampleBySha256.assert_called_once_with("a" * 64)

    def test_existing_family_routes_still_resolve_as_before(self):
        index, client = self._client()
        index.getFamily.return_value = _entry({"family_id": 3})
        response = client.simulate_get("/families/3", query_string="with_samples=false")
        self.assertEqual(200, response.status_code)
        index.getFamily.assert_called_with(3)
        self.assertEqual({"family_id": 3}, response.json["data"])


class ClientGetSamplesByIdsTest(unittest.TestCase):
    def test_empty_list_answers_without_a_request(self):
        for client in (McritClient("http://mcrit.test"), McritClient("http://mcrit.test", raw_responses=True)):
            with patch("mcrit.client.McritClient.requests.Session.post") as post:
                self.assertEqual({}, client.getSamplesByIds([]))
                post.assert_not_called()

    def test_a_success_answers_entries_keyed_by_int(self):
        client = McritClient("http://mcrit.test")
        body = {"status": "successful", "data": {"5": _sample_dict(5), "-3": _sample_dict(-3, sha256="b" * 64)}}
        with patch("mcrit.client.McritClient.requests.Session.post", return_value=_answer(200, body)) as post:
            result = client.getSamplesByIds([5, -3])
        post.assert_called_once_with("http://mcrit.test/samples/ids", data="5,-3", headers={}, timeout=DEFAULT_TIMEOUT)
        self.assertEqual({5, -3}, set(result.keys()))
        for key, entry in result.items():
            self.assertIsInstance(entry, SampleEntry)
            self.assertEqual(key, entry.sample_id)

    def test_raw_mode_hands_out_the_response(self):
        client = McritClient("http://mcrit.test", raw_responses=True)
        response = _answer(200, {"status": "successful", "data": {}})
        with patch("mcrit.client.McritClient.requests.Session.post", return_value=response):
            self.assertIs(response, client.getSamplesByIds([5]))

    def test_error_modes_match_getFunctionsByIds(self):
        failed = {"status": "failed", "data": {"message": "We don't have a sample with that id."}}
        raising_client = McritClient("http://mcrit.test", raise_client_errors=True, raise_server_errors=True)
        with patch("mcrit.client.McritClient.requests.Session.post", return_value=_answer(404, failed)):
            with self.assertRaises(McritNotFound):
                raising_client.getSamplesByIds([5])
        default_client = McritClient("http://mcrit.test")
        with patch("mcrit.client.McritClient.requests.Session.post", return_value=_answer(404, failed)):
            self.assertEqual({}, default_client.getSamplesByIds([5]))


class ClientGetFamiliesByIdsTest(unittest.TestCase):
    def test_empty_list_answers_without_a_request(self):
        for client in (McritClient("http://mcrit.test"), McritClient("http://mcrit.test", raw_responses=True)):
            with patch("mcrit.client.McritClient.requests.Session.post") as post:
                self.assertEqual({}, client.getFamiliesByIds([]))
                post.assert_not_called()

    def test_a_success_answers_entries_keyed_by_int(self):
        client = McritClient("http://mcrit.test")
        body = {"status": "successful", "data": {"1": _family_dict(1, "fam_one"), "2": _family_dict(2, "fam_two")}}
        with patch("mcrit.client.McritClient.requests.Session.post", return_value=_answer(200, body)) as post:
            result = client.getFamiliesByIds([1, 2])
        post.assert_called_once_with("http://mcrit.test/families/ids", data="1,2", headers={}, timeout=DEFAULT_TIMEOUT)
        self.assertEqual({1: "fam_one", 2: "fam_two"}, {k: v.family_name for k, v in result.items()})
        for entry in result.values():
            self.assertIsInstance(entry, FamilyEntry)
            self.assertIsNone(entry.samples)

    def test_raw_mode_hands_out_the_response(self):
        client = McritClient("http://mcrit.test", raw_responses=True)
        response = _answer(200, {"status": "successful", "data": {}})
        with patch("mcrit.client.McritClient.requests.Session.post", return_value=response):
            self.assertIs(response, client.getFamiliesByIds([1]))

    def test_error_modes_match_getFunctionsByIds(self):
        failed = {"status": "failed", "data": {"message": "We don't have a family with that id."}}
        raising_client = McritClient("http://mcrit.test", raise_client_errors=True, raise_server_errors=True)
        with patch("mcrit.client.McritClient.requests.Session.post", return_value=_answer(404, failed)):
            with self.assertRaises(McritNotFound):
                raising_client.getFamiliesByIds([1])
        default_client = McritClient("http://mcrit.test")
        with patch("mcrit.client.McritClient.requests.Session.post", return_value=_answer(404, failed)):
            self.assertEqual({}, default_client.getFamiliesByIds([1]))


if __name__ == "__main__":
    unittest.main()
