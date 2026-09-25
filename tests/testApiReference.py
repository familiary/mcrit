import ast
import logging
import os
import unittest

from mcrit.server import api_reference

logging.disable(logging.CRITICAL)


class ApiReferenceTest(unittest.TestCase):
    """docs/api_reference.md is generated from the route table (#54): it must match the code and
    every responder must carry a docstring for it to describe"""

    def setUp(self):
        from mcrit.server.application_routes import get_app

        self.entries = api_reference.routes(get_app())

    def test_every_route_is_documented(self):
        undocumented = ["%s %s" % (entry["method"], entry["path"]) for entry in self.entries if not entry["doc"]]
        self.assertEqual([], undocumented)
        self.assertGreaterEqual(len(self.entries), 54)

    def test_the_client_is_cross_referenced(self):
        by_route = {(entry["method"], entry["path"]): entry["clients"] for entry in self.entries}
        self.assertEqual(["getFamily", "isFamilyId"], by_route[("GET", "/families/{family_id:int}")])
        self.assertEqual(["getMatchesForPicHash"], by_route[("GET", "/query/pichash/{pichash}/summary")])
        self.assertEqual(["searchFamilies", "search_families"], by_route[("GET", "/search/families")])
        self.assertEqual(["searchFunctions", "search_functions"], by_route[("GET", "/search/functions")])
        self.assertEqual(["deleteQueueData"], by_route[("DELETE", "/jobs")])
        self.assertEqual(["deleteJob"], by_route[("DELETE", "/jobs/{job_id}")])
        self.assertEqual(["getJobData"], by_route[("GET", "/jobs/{job_id}")])
        self.assertEqual(["getQueueStatistics"], by_route[("GET", "/jobs/stats")])
        # only endpoints without a client method may be left blank
        self.assertEqual(
            ["GET /", "GET /config", "GET /matches/function/{function_id:int}", "GET /samples/{sample_id:int}/functions/{function_id:int}"],
            sorted("%s %s" % (entry["method"], entry["path"]) for entry in self.entries if not entry["clients"]),
        )

    def test_the_committed_reference_is_current(self):
        with open(api_reference.DEFAULT_OUTPUT) as handle:
            committed = handle.read()
        self.assertEqual(api_reference.render(self.entries), committed, "docs/api_reference.md is stale: python -m mcrit.server.api_reference")
        self.assertTrue(os.path.exists(api_reference.CLIENT_SOURCE))


def _client_methods():
    with open(api_reference.CLIENT_SOURCE) as handle:
        tree = ast.parse(handle.read())
    client = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "McritClient")
    return [node for node in client.body if isinstance(node, ast.FunctionDef)]


def _sends_a_request(method):
    for node in ast.walk(method):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            target = node.func.value
            if isinstance(target, ast.Name) and target.id == "requests":
                return True
            if isinstance(target, ast.Name) and target.id == "self" and node.func.attr == "_search_request":
                return True
    return False


class ClientSurfaceTest(unittest.TestCase):
    """The client half of #54: every public McritClient method is typed and documented, and every
    one that sends a request honours raw_responses through _passthrough. Read off the source, so a
    method added later - on main or on another branch - cannot quietly go without."""

    def test_every_public_client_method_is_typed_and_documented(self):
        missing = [
            "%s (%s)" % (method.name, ", ".join(what for what, absent in (("docstring", not ast.get_docstring(method)), ("return annotation", method.returns is None)) if absent))
            for method in _client_methods()
            if not method.name.startswith("_") and (not ast.get_docstring(method) or method.returns is None)
        ]
        self.assertEqual([], missing)

    def test_every_request_method_honours_raw_responses(self):
        # _search_request hands the response to the methods that do the checking
        ignoring_raw = [
            method.name
            for method in _client_methods()
            if method.name != "_search_request" and _sends_a_request(method) and not any(isinstance(node, ast.Attribute) and node.attr == "raw" for node in ast.walk(method))
        ]
        self.assertEqual([], ignoring_raw)
        # and answers the response through _passthrough, which is what keeps the annotations honest
        bare = [
            method.name
            for method in _client_methods()
            if method.name != "_passthrough" and any(isinstance(node, ast.Return) and isinstance(node.value, ast.Name) and node.value.id == "response" for node in ast.walk(method))
        ]
        self.assertEqual([], bare)


if __name__ == "__main__":
    unittest.main()
