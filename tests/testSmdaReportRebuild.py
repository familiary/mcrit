import json
import os
import unittest
from unittest.mock import MagicMock, patch

import falcon
import falcon.testing
from smda.common.SmdaReport import SmdaReport

from mcrit.client.McritClient import McritClient
from mcrit.server.SampleResource import SampleResource
from mcrit.storage.SampleEntry import SampleEntry

EXAMPLE_REPORT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "example_report.smda")


class SampleEntryKeepsTheReport(unittest.TestCase):
    """#94: a SampleEntry carries everything of the SMDA report but the functions, and can
    give the report back given the functions"""

    def test_the_report_round_trips_through_the_entry(self):
        with open(EXAMPLE_REPORT) as fjson:
            report_dict = json.load(fjson)
        report = SmdaReport.fromDict(report_dict)
        assert report is not None
        # data references are keyed by integer addresses: the one shape MongoDB refuses as keys
        report.data_refs_from = {4096: [4200, 4300]}
        report.data_refs_to = {4200: [4096]}
        entry = SampleEntry(report, sample_id=1, family_id=1)
        self.assertNotIn("xcfg", entry.smda_extras)
        self.assertEqual({"4096": [4200, 4300]}, entry.smda_extras["xdata_refs_from"])
        self.assertNotIn("family", entry.smda_extras.get("metadata", {}))
        xcfg = {int(offset): function.toDict() for offset, function in report.xcfg.items()}
        rebuilt = SmdaReport.fromDict(entry.toSmdaReportDict(xcfg))
        assert rebuilt is not None
        self.assertEqual(report.toDict(), rebuilt.toDict())
        # and through the wire dict of the entry
        again = SampleEntry.fromDict(json.loads(json.dumps(entry.toDict())))
        self.assertEqual(entry.smda_extras, again.smda_extras)

    def test_a_sample_stored_before_the_extras_still_rebuilds(self):
        with open(EXAMPLE_REPORT) as fjson:
            report = SmdaReport.fromDict(json.load(fjson))
        assert report is not None
        entry_dict = SampleEntry(report, sample_id=1, family_id=1).toDict()
        del entry_dict["smda_extras"]
        entry = SampleEntry.fromDict(entry_dict)
        self.assertEqual({}, entry.smda_extras)
        rebuilt = SmdaReport.fromDict(entry.toSmdaReportDict({}))
        assert rebuilt is not None
        self.assertEqual(report.sha256, rebuilt.sha256)
        self.assertEqual(report.family, rebuilt.family)
        self.assertEqual(0, len(rebuilt.xcfg))


class SmdaReportRouteAndClient(unittest.TestCase):
    def test_the_route_serves_the_rebuilt_report(self):
        with open(EXAMPLE_REPORT) as fjson:
            report = SmdaReport.fromDict(json.load(fjson))
        index = MagicMock()
        index.isSampleId.return_value = True
        index.getSmdaReportForSample.return_value = report
        resp = falcon.Response()
        SampleResource(index).on_get_smda_report(falcon.Request(falcon.testing.create_environ(path="/samples/1/smda")), resp, 1)
        assert resp.data is not None and report is not None
        self.assertEqual(report.sha256, json.loads(resp.data)["data"]["sha256"])
        index.isSampleId.return_value = False
        resp = falcon.Response()
        SampleResource(index).on_get_smda_report(falcon.Request(falcon.testing.create_environ(path="/samples/1/smda")), resp, 1)
        self.assertEqual(falcon.HTTP_404, resp.status)

    def test_the_client_answers_a_report_object(self):
        with open(EXAMPLE_REPORT) as fjson:
            report_dict = json.load(fjson)
        response = MagicMock(status_code=200)
        response.json.return_value = {"status": "successful", "data": report_dict}
        client = McritClient("http://mcrit.test")
        with patch("mcrit.client.McritClient.requests.get", return_value=response):
            report = client.getSmdaReportForSample(1)
        assert report is not None
        self.assertEqual(report_dict["sha256"], report.sha256)
        self.assertEqual(len(report_dict["xcfg"]), len(report.xcfg))


if __name__ == "__main__":
    unittest.main()
