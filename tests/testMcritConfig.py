import os
import subprocess
import sys
import types
import unittest
from unittest import mock

from mcrit.__main__ import normalizeBind
from mcrit.config.McritConfig import McritConfig
from mcrit.server import application_routes
from mcrit.server.application_routes import AuthMiddleware

READ_BIND = "from mcrit.config.GunicornConfig import GunicornConfig; print(GunicornConfig().BIND)"
READ_TOKEN = "from mcrit.config.McritConfig import McritConfig; print(McritConfig.AUTH_TOKEN)"


class TestMcritConfigAuthToken(unittest.TestCase):
    def _run(self, code, token=None):
        # use a subprocess, as AUTH_TOKEN is a class attribute evaluated at import time
        env = os.environ.copy()
        env.pop("MCRIT_AUTH_TOKEN", None)
        if token is not None:
            env["MCRIT_AUTH_TOKEN"] = token
        return subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, check=True)

    def test_auth_token_from_env(self):
        result = self._run(READ_TOKEN, token="secret_token_from_env")
        self.assertEqual(result.stdout.decode().strip(), "secret_token_from_env")

    def test_auth_token_defaults_to_empty(self):
        result = self._run(READ_TOKEN)
        self.assertEqual(result.stdout.decode().strip(), "")


class TestServerBind(unittest.TestCase):
    def _bind(self, bind=None):
        # a subprocess, as the default is evaluated at import time like AUTH_TOKEN's
        env = os.environ.copy()
        env.pop("MCRIT_SERVER_BIND", None)
        if bind is not None:
            env["MCRIT_SERVER_BIND"] = bind
        return subprocess.run([sys.executable, "-c", READ_BIND], env=env, capture_output=True, check=True).stdout.decode().strip()

    def test_the_server_binds_loopback_by_default(self):
        # the API needs no credentials unless MCRIT_AUTH_TOKEN is set, so other hosts are opt-in
        self.assertEqual("127.0.0.1:8000", self._bind())
        self.assertEqual("127.0.0.1:8000", self._bind(" "))

    def test_a_bind_without_host_from_env_stays_visible_to_the_server(self):
        self.assertEqual(":8000", self._bind(":8000"))

    def test_bind_from_env(self):
        self.assertEqual("0.0.0.0:8123", self._bind("0.0.0.0:8123"))

    def test_a_bind_without_host_is_loopback(self):
        self.assertEqual("127.0.0.1:8000", normalizeBind(":8000"))
        self.assertEqual("0.0.0.0:8000", normalizeBind("0.0.0.0:8000"))
        self.assertEqual("[::]:8000", normalizeBind("[::]:8000"))

    def _serve(self, bind):
        from mcrit import __main__ as entry_point

        with (
            mock.patch.dict(sys.modules, {"mcrit.server.wsgi": types.SimpleNamespace(app=object())}),
            mock.patch("waitress.serve") as serve,
            mock.patch("mcrit.config.GunicornConfig.GunicornConfig", return_value=types.SimpleNamespace(BIND=bind, USE_GUNICORN=False)),
            mock.patch("platform.system", return_value="Windows"),
        ):
            entry_point.runServer()
        serve.assert_called_once()
        return serve.call_args.kwargs["listen"]

    def test_gunicorn_binds_loopback_for_a_bind_without_host(self):
        from gunicorn.app.base import BaseApplication

        from mcrit import __main__ as entry_point

        binds = []

        def run(application):
            binds.append(application.cfg.bind)

        with (
            mock.patch.dict(sys.modules, {"mcrit.server.wsgi": types.SimpleNamespace(app=lambda *args: [])}),
            mock.patch(
                "mcrit.config.GunicornConfig.GunicornConfig", return_value=types.SimpleNamespace(BIND=":8000", USE_GUNICORN=True, toDict=lambda: {"BIND": ":8000", "WORKERS": 1})
            ),
            mock.patch("platform.system", return_value="Linux"),
            mock.patch.object(BaseApplication, "run", run),
            self.assertRaises(SystemExit),
        ):
            entry_point.runServer(uses_gunicorn=True)
        self.assertEqual([["127.0.0.1:8000"]], binds)

    def test_waitress_listens_on_the_configured_bind(self):
        self.assertEqual("0.0.0.0:8123", self._serve("0.0.0.0:8123"))

    def test_waitress_does_not_listen_on_every_interface_for_a_bind_without_host(self):
        self.assertEqual("127.0.0.1:8000", self._serve(":8000"))


class TestAuthMiddleware(unittest.TestCase):
    # NOTE: patch the logger rather than using assertLogs - other test modules call
    # logging.disable(logging.CRITICAL) at import time, which would swallow the record

    def test_warning_when_no_token_configured(self):
        with mock.patch.object(McritConfig, "AUTH_TOKEN", ""):
            with mock.patch.object(application_routes.LOGGER, "warning") as warning:
                AuthMiddleware()
        warning.assert_called_once()
        self.assertIn("No AUTH_TOKEN configured", warning.call_args.args[0])

    def test_no_warning_when_token_configured(self):
        with mock.patch.object(McritConfig, "AUTH_TOKEN", "some_token"):
            with mock.patch.object(application_routes.LOGGER, "warning") as warning:
                AuthMiddleware()
        warning.assert_not_called()

    def test_token_comparison(self):
        with mock.patch.object(McritConfig, "AUTH_TOKEN", "some_token"):
            middleware = AuthMiddleware()
            self.assertTrue(middleware._token_is_valid("some_token"))
            self.assertFalse(middleware._token_is_valid("other_token"))
            # WSGI decodes header bytes as latin-1, so a high byte arrives as a non-ASCII str -
            # that has to answer False instead of raising out of the responder as a 500
            self.assertFalse(middleware._token_is_valid("some_token\xff"))
            self.assertFalse(middleware._token_is_valid("\xff"))

    def test_any_token_accepted_when_unprotected(self):
        with mock.patch.object(McritConfig, "AUTH_TOKEN", ""):
            middleware = AuthMiddleware()
            self.assertTrue(middleware._token_is_valid("anything"))


if __name__ == "__main__":
    unittest.main()
