import os
import unittest
from unittest.mock import MagicMock, patch

import falcon
import falcon.testing

from mcrit.config.McritConfig import McritConfig
from mcrit.server import application_routes
from mcrit.server.application_routes import CrossSiteRequestMiddleware, getAllowedHosts

POWERSHELL = (
    "Mozilla/5.0 (Windows NT; Windows NT 10.0; en-US) WindowsPowerShell/5.1.19041.4648",
    "Mozilla/5.0 (Windows NT 10.0; Microsoft Windows 10.0.19045; en-US) PowerShell/7.4.5",
)
BROWSER = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"


class _Resource:
    def __init__(self):
        self.calls = 0

    def on_get(self, req, resp):
        self.calls += 1
        resp.media = {"status": "successful"}

    def on_post(self, req, resp):
        self.calls += 1
        resp.media = {"status": "successful"}


class CrossSiteRequestMiddlewareTest(unittest.TestCase):
    """What a foreign web page makes a browser send is refused before any responder runs."""

    def setUp(self):
        self.resource = _Resource()
        app = falcon.App(middleware=[CrossSiteRequestMiddleware()])
        app.add_route("/respawn", self.resource)
        self.client = falcon.testing.TestClient(app)
        patcher = patch.object(McritConfig, "AUTH_TOKEN", "")
        patcher.start()
        self.addCleanup(patcher.stop)
        env = patch.dict(os.environ)
        env.start()
        self.addCleanup(env.stop)
        os.environ.pop("MCRIT_ALLOWED_HOSTS", None)

    def _post(self, host="127.0.0.1:8000", **headers):
        return self.client.simulate_post("/respawn", headers=dict(headers, Host=host))

    def test_clients_that_are_no_browsers_pass_under_any_host(self):
        # MCRITweb reaches the server as mcrit-server:8000; McritClient and the IDA plugin send python-requests
        for host in ("127.0.0.1:8000", "[::1]:8000", "localhost:8000", "mcrit-server:8000", "mcrit.example.org", "rebind.evil.example:8000"):
            self.assertEqual(200, self._post(host=host, **{"User-Agent": "python-requests/2.32.3"}).status_code, host)
            self.assertEqual(200, self._post(host=host).status_code, host)
        self.assertEqual(12, self.resource.calls)

    def test_powershell_is_no_browser(self):
        for agent in POWERSHELL:
            for host in ("127.0.0.1:8000", "localhost:8000", "mcrit-box:8000"):
                self.assertEqual(200, self._post(host=host, **{"User-Agent": agent}).status_code, (agent, host))

    def test_a_browser_may_use_an_address_or_localhost(self):
        for host in ("127.0.0.1:8000", "[::1]:8000", "10.0.0.5:8000", "localhost:8000", "LOCALHOST"):
            self.assertEqual(200, self._post(host=host, **{"User-Agent": BROWSER, "Sec-Fetch-Site": "same-origin"}).status_code, host)
        self.assertEqual(5, self.resource.calls)

    def test_a_dns_rebound_host_is_refused(self):
        # after rebinding, the page is same-origin with the API under a name its author resolves
        for name in ("rebind.evil.example", "rebind", "evil.localhost", "localhost.evil.example", "127.0.0.1.", "localhost."):
            rebound = {"Origin": "http://%s:8000" % name, "Sec-Fetch-Site": "same-origin"}
            self.assertEqual(403, self._post(host=name + ":8000", **rebound).status_code, name)
            # a page may overwrite its User-Agent, but not the Sec-Fetch-* headers
            self.assertEqual(403, self.client.simulate_get("/respawn", headers={"Host": name + ":8000", "Sec-Fetch-Site": "same-origin"}).status_code, name)
        self.assertEqual(0, self.resource.calls)

    def test_allowed_hosts_name_the_instance_for_browsers(self):
        browser = {"User-Agent": BROWSER}
        with patch.dict(os.environ, {"MCRIT_ALLOWED_HOSTS": "mcrit.example.org, mcrit"}):
            self.assertEqual(200, self._post(host="MCRIT.example.org:8000", **browser).status_code)
            self.assertEqual(200, self._post(host="mcrit:8000", **browser).status_code)
            self.assertEqual(403, self._post(host="rebind:8000", **browser).status_code)
        with patch.dict(os.environ, {"MCRIT_ALLOWED_HOSTS": "*"}):
            self.assertEqual(200, self._post(host="rebind.evil.example:8000", **browser).status_code)
            # "*" lifts the Host check only, not the one on cross-site requests
            self.assertEqual(403, self._post(host="rebind.evil.example:8000", Origin="http://evil.example").status_code)
        self.assertEqual(3, self.resource.calls)

    def test_requests_the_browser_marks_as_cross_site_are_refused(self):
        for site in ("cross-site", "same-site"):
            self.assertEqual(403, self._post(**{"Sec-Fetch-Site": site}).status_code, site)
        # a form POST or a no-cors fetch carries the page's origin, "null" when redirected or without referrer
        for origin in ("http://evil.example", "http://10.0.0.7:9000", "http://127.0.0.1:9000", "null"):
            self.assertEqual(403, self._post(Origin=origin).status_code, origin)
        self.assertEqual(200, self._post(**{"Sec-Fetch-Site": "none"}).status_code)
        self.assertEqual(200, self._post(Origin="http://127.0.0.1:8000").status_code)
        self.assertEqual(2, self.resource.calls)

    def test_nothing_is_refused_when_a_token_is_set(self):
        # a foreign page cannot send the apitoken header without a preflight, so the token already decides
        with patch.object(McritConfig, "AUTH_TOKEN", "secret"):
            self.assertEqual(200, self._post(host="rebind.evil.example:8000", Origin="http://evil.example").status_code)

    def test_allowed_hosts_from_env(self):
        with patch.dict(os.environ, {"MCRIT_ALLOWED_HOSTS": " MCRIT.example.org,, other.example "}):
            self.assertEqual(["mcrit.example.org", "other.example"], getAllowedHosts())
        with patch.dict(os.environ):
            os.environ.pop("MCRIT_ALLOWED_HOSTS", None)
            self.assertEqual([], getAllowedHosts())


class ServedAppTest(unittest.TestCase):
    JOB_SCHEDULING_GETS = (
        "/complete_minhashes",
        "/rebuild_index",
        "/rebuild_picblockhash_index",
        "/rebuild_function_range_index",
        "/rebuild_band_df_index",
        "/band_df_cutoff_coverage",
        "/recalculate_pichashes",
        "/recalculate_minhashes",
        "/matches/sample/1",
        "/matches/sample/cross/1,2",
        "/matches/sample/1/2",
        "/uniqueblocks/samples/1,2",
        "/uniqueblocks/family/1",
    )

    def setUp(self):
        self.index = MagicMock()
        patcher = patch.object(McritConfig, "AUTH_TOKEN", "")
        patcher.start()
        self.addCleanup(patcher.stop)
        with patch.object(application_routes, "create_index", return_value=self.index):
            self.client = falcon.testing.TestClient(application_routes.get_app(), headers={"Host": "mcrit-server:8000"})

    def test_the_served_app_refuses_a_cross_site_respawn_and_import(self):
        cross_site = {"Origin": "http://evil.example"}
        self.assertEqual(403, self.client.simulate_post("/respawn", body="a=b", headers=dict(cross_site, **{"Content-Type": "application/x-www-form-urlencoded"})).status_code)
        self.assertEqual(403, self.client.simulate_post("/import", body="{}", headers=dict(cross_site, **{"Content-Type": "text/plain"})).status_code)
        self.index.respawn.assert_not_called()
        self.index.addImportData.assert_not_called()
        # MCRITweb's call: Host mcrit-server:8000, no Origin
        self.assertEqual(200, self.client.simulate_post("/respawn").status_code)
        self.index.respawn.assert_called_once()

    def test_a_browser_schedules_no_job(self):
        """an <img>, a link or a redirect lands here with whatever Sec-Fetch-Site the browser makes of it"""
        for path in self.JOB_SCHEDULING_GETS:
            for headers in (
                {"User-Agent": BROWSER},
                {"User-Agent": BROWSER, "Sec-Fetch-Site": "none", "Host": "127.0.0.1:8000"},
                {"Sec-Fetch-Mode": "navigate", "Host": "127.0.0.1:8000"},
            ):
                self.assertEqual(403, self.client.simulate_get(path, headers=headers).status_code, (path, headers))
        self.assertEqual([], self.index.mock_calls)
        for path in self.JOB_SCHEDULING_GETS:
            for headers in ({"User-Agent": "python-requests/2.32.3"}, {}):
                self.assertNotEqual(403, self.client.simulate_get(path, headers=headers).status_code, path)
        # PowerShell's default User-Agent starts with Mozilla/ but is an API client
        for agent in POWERSHELL:
            for host in ("127.0.0.1:8000", "localhost:8000", "mcrit-box:8000"):
                for path in self.JOB_SCHEDULING_GETS:
                    self.assertNotEqual(403, self.client.simulate_get(path, headers={"User-Agent": agent, "Host": host}).status_code, (agent, host, path))
        # reading stays open to a browser
        self.assertNotEqual(403, self.client.simulate_get("/status", headers={"User-Agent": BROWSER, "Host": "127.0.0.1:8000"}).status_code)


if __name__ == "__main__":
    unittest.main()
