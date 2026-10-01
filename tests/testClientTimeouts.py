"""Every request McritClient sends must carry a timeout.

requests waits forever by default, on the connect and on every read. A client pointed at a
server that is down behind a firewall, or up but hung, therefore blocks its caller for as
long as the process lives - in MCRITweb that is a gunicorn request thread, and gunicorn's own
timeout does not reach it under the gthread worker it runs.
"""

import ast
import os
import socket
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

import requests

from mcrit.client import McritClient as client_module
from mcrit.client.McritClient import DEFAULT_TIMEOUT, McritClient

HTTP_VERBS = {"get", "post", "put", "delete", "patch", "head", "options", "request"}


def ok(data=None):
    response = MagicMock(status_code=200)
    response.json.return_value = {"status": "successful", "data": data}
    return response


class ClientTimeoutTest(unittest.TestCase):
    def test_every_request_in_the_client_passes_a_timeout(self):
        """Read off the source, so a method added later cannot quietly go without one."""
        with open(os.path.abspath(client_module.__file__)) as handle:
            tree = ast.parse(handle.read())
        calls, missing = 0, []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            target = node.func.value
            is_session = isinstance(target, ast.Attribute) and target.attr == "_session" and isinstance(target.value, ast.Name) and target.value.id == "self"
            if is_session and node.func.attr in HTTP_VERBS:
                calls += 1
                if not any(keyword.arg == "timeout" for keyword in node.keywords):
                    missing.append(node.lineno)
        self.assertGreater(calls, 50)
        self.assertEqual([], missing, "session calls without timeout= on these lines of McritClient.py")

    def test_the_default_bounds_the_connect_and_leaves_the_read_to_the_caller(self):
        connect, read = DEFAULT_TIMEOUT
        self.assertEqual(10, connect)
        self.assertIsNone(read)
        self.assertEqual(DEFAULT_TIMEOUT, McritClient("http://mcrit.test").timeout)

    def test_each_verb_sends_the_configured_timeout(self):
        client = McritClient("http://mcrit.test", timeout=(3, 30))
        cases = [
            ("get", client.getVersion),
            ("post", client.respawn),
            ("put", lambda: client.modifyFunction(7, "name")),
            ("delete", lambda: client.deleteSample(7)),
        ]
        for verb, call in cases:
            with self.subTest(verb=verb), patch(f"mcrit.client.McritClient.requests.Session.{verb}", return_value=ok()) as sent:
                call()
                self.assertEqual((3, 30), sent.call_args.kwargs["timeout"])

    def test_a_timeout_set_after_construction_is_used(self):
        """MCRITweb builds its clients through a factory, and can set the attribute on any
        mcrit version: an older client simply ignores it."""
        client = McritClient("http://mcrit.test")
        client.timeout = (5, 60)
        with patch("mcrit.client.McritClient.requests.Session.get", return_value=ok()) as sent:
            client.getVersion()
        self.assertEqual((5, 60), sent.call_args.kwargs["timeout"])

    def test_a_server_that_never_answers_raises_instead_of_hanging(self):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        accepted = []

        def accept_and_stay_silent():
            connection, _ = listener.accept()
            accepted.append(connection)

        threading.Thread(target=accept_and_stay_silent, daemon=True).start()
        port = listener.getsockname()[1]
        started = time.monotonic()
        try:
            with self.assertRaises(requests.exceptions.ReadTimeout):
                McritClient(f"http://127.0.0.1:{port}", timeout=(2, 0.5)).getVersion()
        finally:
            for connection in accepted:
                connection.close()
            listener.close()
        self.assertLess(time.monotonic() - started, 5)


if __name__ == "__main__":
    unittest.main()
