import json
import unittest

from mcrit.queue.LocalQueue import Job
from mcrit.queue.QueueRemoteCalls import _createJobPayload, get_descriptor, rearrange_params
from mcrit.Worker import RESULTS_VERSION, Worker

# the jobs whose result is a report computed from the corpus, and so reused only within one
# results version (#241)
REPORT_METHODS = {
    "getMatchesForSample",
    "getMatchesForSampleVs",
    "getMatchesForSampleVsGroup",
    "getMatchesForSmdaReport",
    "getMatchesForMappedBinary",
    "getMatchesForUnmappedBinary",
    "combineMatchesToCross",
    "getUniqueBlocks",
}


class ResultsVersionTest(unittest.TestCase):
    def test_every_report_job_declares_the_results_version(self):
        remote_methods = {name: method for name in dir(Worker) if getattr(method := getattr(Worker, name), "remote", False)}
        self.assertTrue(REPORT_METHODS <= set(remote_methods))
        for name, method in remote_methods.items():
            with self.subTest(method=name):
                # a job that changes or reports on stored data keeps its cache as it was
                self.assertEqual(RESULTS_VERSION if name in REPORT_METHODS else None, method.results_version)

    def test_a_versioned_descriptor_is_read_like_any_other(self):
        # Job reads the hash of a query's uploaded binary by position (fkie-cad/mcritweb reads it
        # back through Job.sha256 to promote a query)
        params, _ = rearrange_params([None], {}, [], [])
        hashes = {"0": "ab" * 32}
        descriptor = get_descriptor("getMatchesForUnmappedBinary", params, hashes, results_version=RESULTS_VERSION)
        job = Job({"_id": "0" * 24, "payload": _createJobPayload("getMatchesForUnmappedBinary", params, {}, descriptor)}, None)
        self.assertEqual("ab" * 32, job.sha256)
        self.assertEqual(json.loads(get_descriptor("getMatchesForUnmappedBinary", params, hashes))[:3], json.loads(descriptor)[:3])


if __name__ == "__main__":
    unittest.main()
