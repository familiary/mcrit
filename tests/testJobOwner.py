import unittest
from unittest.mock import MagicMock

import falcon
import falcon.testing

from mcrit.server.BlocksResource import BlocksResource
from mcrit.server.FamilyResource import FamilyResource
from mcrit.server.MatchResource import MatchResource
from mcrit.server.QueryResource import QueryResource
from mcrit.server.SampleResource import SampleResource
from mcrit.server.StatusResource import StatusResource
from mcrit.server.utils import get_username


def _index():
    """A stand-in index with a real configuration: the matching routes resolve their knobs from it (#217)."""
    from .context import config

    index = MagicMock()
    index.config = config
    return index


def _request(path, headers=None, query_string="", body=None, method="GET"):
    environ = falcon.testing.create_environ(path=path, headers=headers or {}, query_string=query_string, body=body or "", method=method)
    return falcon.Request(environ)


class JobOwnerTest(unittest.TestCase):
    """Every job-creating route hands the requesting user on to the queue (fkie-cad/mcritweb#37)"""

    def test_get_username_reads_the_header_mcritweb_sends(self):
        self.assertEqual("alice", get_username(_request("/", headers={"username": "alice"})))
        self.assertIsNone(get_username(_request("/")))

    def _assert_called_with_username(self, index, method, expected):
        self.assertTrue(getattr(index, method).called, method)
        self.assertEqual(expected, getattr(index, method).call_args.kwargs.get("username"), method)

    def test_match_routes_forward_the_user(self):
        for expected, headers in (("alice", {"username": "alice"}), (None, {})):
            with self.subTest(user=expected):
                index = _index()
                index.isSampleId.return_value = True
                resource = MatchResource(index)
                resource.on_get_sample(_request("/matches/sample/1", headers), falcon.Response(), sample_id=1)
                self._assert_called_with_username(index, "getMatchesForSample", expected)
                resource.on_get_sample_vs(_request("/matches/sample/1/2", headers), falcon.Response(), sample_id=1, sample_id_b=2)
                self._assert_called_with_username(index, "getMatchesForSampleVs", expected)
                resource.on_get_sample_cross(_request("/matches/sample/cross/1,2", headers), falcon.Response(), sample_ids="1,2")
                self._assert_called_with_username(index, "getMatchesCross", expected)

    def test_query_and_blocks_routes_forward_the_user(self):
        headers = {"username": "alice"}
        index = _index()
        QueryResource(index).on_post_query_binary(_request("/query/binary", headers, body="MZ", method="POST"), falcon.Response())
        self._assert_called_with_username(index, "getMatchesForUnmappedBinary", "alice")
        QueryResource(index).on_post_query_binary_mapped(_request("/query/binary/mapped/4096", headers, body="MZ", method="POST"), falcon.Response(), base_address="4096")
        self._assert_called_with_username(index, "getMatchesForMappedBinary", "alice")
        BlocksResource(index).on_get_unique_blocks_for_samples(_request("/uniqueblocks/samples/1", headers), falcon.Response(), comma_separated_sample_ids="1")
        self._assert_called_with_username(index, "getUniqueBlocks", "alice")

    def test_collection_and_maintenance_routes_forward_the_user(self):
        headers = {"username": "alice"}
        index = MagicMock()
        SampleResource(index).on_delete(_request("/samples/1", headers, method="DELETE"), falcon.Response(), sample_id=1)
        self._assert_called_with_username(index, "deleteSample", "alice")
        FamilyResource(index).on_delete(_request("/families/1", headers, method="DELETE"), falcon.Response(), family_id=1)
        self._assert_called_with_username(index, "deleteFamily", "alice")
        status = StatusResource(index)
        status.on_get_complete_minhashes(_request("/complete_minhashes", headers), falcon.Response())
        self._assert_called_with_username(index, "updateMinHashes", "alice")
        status.on_get_rebuild_index(_request("/rebuild_index", headers), falcon.Response())
        self._assert_called_with_username(index, "rebuildIndex", "alice")


class ReportSubmissionOwnerTest(unittest.TestCase):
    """The minhash job a submitted report queues is the submitter's too (POST /samples)"""

    def test_add_report_forwards_the_user_to_the_minhash_job(self):
        import json
        import os

        from smda.common.SmdaReport import SmdaReport

        from mcrit.index.MinHashIndex import MinHashIndex

        from .context import config

        index = MinHashIndex(config)
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "example_report.smda")) as fjson:
            report = SmdaReport.fromDict(json.load(fjson))
        assert report is not None
        summary = index.addReport(report, username="alice")
        assert summary is not None
        job = index.queue.get_job(summary["job_id"])
        assert job is not None
        self.assertEqual("alice", job.username)


if __name__ == "__main__":
    unittest.main()
