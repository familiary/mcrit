import unittest

from mcrit.queue.LocalQueue import LocalQueue
from mcrit.queue.QueueRemoteCalls import BaseRemoteCallerClass, _createJobPayload, get_descriptor, rearrange_params


def test_local_queue_statistics_support_the_server_refresh_contract():
    index = BaseRemoteCallerClass(LocalQueue())
    assert index.getQueueStats() == {}
    assert index.getQueueStats(refresh=True) == {}


def _payload(method, *params, **kwparams):
    """A job payload built the way QueueRemoteCalls.remote_call_function builds one for a
    real call, e.g. _payload("getMatchesForSampleVs", 8, 12, band_matches_required=2)."""
    parsed_params, file_params = rearrange_params(list(params), dict(kwparams), [], [])
    descriptor = get_descriptor(method, parsed_params, {})
    return _createJobPayload(method, parsed_params, file_params, descriptor)


class LocalQueueSelectorTest(unittest.TestCase):
    """get_jobs' sample_ids and job_ids selectors on LocalQueue: the same semantics as
    MongoQueue (first positional argument for sample_ids, id membership for job_ids), applied
    in Python to payload.params and to the job id directly. Jobs are placed straight into
    self.queue._jobs rather than through put(), which would run them on a real worker."""

    def setUp(self):
        self.queue = LocalQueue()

    def _queue_job(self, method, *params, **kwparams):
        job_id = f"job-{len(self.queue._jobs)}"
        self.queue._jobs[job_id] = {"_id": job_id, "number": len(self.queue._jobs), "payload": _payload(method, *params, **kwparams)}
        return job_id

    def test_sample_ids_selects_the_first_argument_only(self):
        job_8 = self._queue_job("getMatchesForSample", 8)
        job_9 = self._queue_job("getMatchesForSample", 9, band_matches_required=2)
        job_1 = self._queue_job("getMatchesForSample", 1)
        job_12 = self._queue_job("getMatchesForSample", 12)
        self._queue_job("test_method", 8)  # a different method: must not be selected

        selected = {job.job_id for job in self.queue.get_jobs(0, 100, method="getMatchesForSample", sample_ids=[8, 9, 12])}
        self.assertEqual({job_8, job_9, job_12}, selected)
        selected_one = {job.job_id for job in self.queue.get_jobs(0, 100, method="getMatchesForSample", sample_ids=[1])}
        self.assertEqual({job_1}, selected_one)

    def test_sample_ids_only_matches_the_vs_jobs_first_argument(self):
        job_first = self._queue_job("getMatchesForSampleVs", 8, 12)
        self._queue_job("getMatchesForSampleVs", 99, 1)
        # 1 is only the SECOND argument of the other job: must not match it
        selected_one = self.queue.get_jobs(0, 100, method="getMatchesForSampleVs", sample_ids=[1])
        self.assertEqual([], selected_one)
        selected_8 = {job.job_id for job in self.queue.get_jobs(0, 100, method="getMatchesForSampleVs", sample_ids=[8])}
        self.assertEqual({job_first}, selected_8)

    def test_sample_ids_accepts_negative_ids(self):
        job_neg = self._queue_job("getMatchesForSample", -5)
        self._queue_job("getMatchesForSample", 5)
        selected = {job.job_id for job in self.queue.get_jobs(0, 100, method="getMatchesForSample", sample_ids=[-5])}
        self.assertEqual({job_neg}, selected)

    def test_sample_ids_ignores_jobs_whose_first_argument_is_not_int_like(self):
        # combineMatchesToCross's only positional argument is a dict, not a sample id
        self._queue_job("combineMatchesToCross", {"8": "some-job-id"})
        selected = self.queue.get_jobs(0, 100, method="combineMatchesToCross", sample_ids=[8])
        self.assertEqual([], selected)

    def test_sample_ids_present_but_empty_selects_nothing(self):
        self._queue_job("getMatchesForSample", 8)
        selected = self.queue.get_jobs(0, 100, method="getMatchesForSample", sample_ids=[])
        self.assertEqual([], selected)

    def test_sample_ids_without_a_method_selects_nothing(self):
        # as on MongoQueue; JobResource refuses it with 400 before either is asked
        self._queue_job("getMatchesForSample", 8)
        self._queue_job("modifySample", 8)
        self.assertEqual([], self.queue.get_jobs(0, 100, sample_ids=[8]))

    def test_job_ids_selects_by_id(self):
        job_a = self._queue_job("getMatchesForSample", 1)
        job_b = self._queue_job("modifySample", 2)
        self._queue_job("getMatchesForSample", 3)
        selected = {job.job_id for job in self.queue.get_jobs(0, 100, job_ids=[job_a, job_b])}
        self.assertEqual({job_a, job_b}, selected)

    def test_job_ids_unknown_id_matches_nothing_for_it(self):
        job_a = self._queue_job("getMatchesForSample", 1)
        selected = {job.job_id for job in self.queue.get_jobs(0, 100, job_ids=[job_a, "does-not-exist"])}
        self.assertEqual({job_a}, selected)

    def test_job_ids_present_but_empty_selects_nothing(self):
        self._queue_job("getMatchesForSample", 1)
        selected = self.queue.get_jobs(0, 100, job_ids=[])
        self.assertEqual([], selected)

    def test_sample_ids_and_job_ids_combine_by_and(self):
        job_8_a = self._queue_job("getMatchesForSample", 8)
        job_9 = self._queue_job("getMatchesForSample", 9)
        job_8_b = self._queue_job("getMatchesForSample", 8, band_matches_required=2)
        selected = {job.job_id for job in self.queue.get_jobs(0, 100, method="getMatchesForSample", sample_ids=[8, 9], job_ids=[job_8_a, job_9])}
        self.assertEqual({job_8_a, job_9}, selected)
        self.assertNotIn(job_8_b, selected)

    def test_neither_selector_keeps_existing_behaviour(self):
        job_a = self._queue_job("getMatchesForSample", 1)
        job_b = self._queue_job("getMatchesForSample", 2)
        selected = {job.job_id for job in self.queue.get_jobs(0, 100, method="getMatchesForSample")}
        self.assertEqual({job_a, job_b}, selected)


if __name__ == "__main__":
    unittest.main()
