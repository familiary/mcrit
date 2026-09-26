"""MINHASH_MATCHING_SHORTLIST_SIZE and STORAGE_BAND_DF_CUTOFF per matching job, not only per deployment (#217)."""

import json
import unittest
from copy import deepcopy
from typing import Any, List
from unittest.mock import MagicMock, patch

import falcon
import falcon.testing
from smda.common.SmdaReport import SmdaReport

from mcrit.client.McritClient import McritClient
from mcrit.config.MinHashConfig import MinHashConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MatchingParameters import MATCHING_KNOBS, MATCHING_PRESETS, applyMatchingPreset, resolveMatchingParams
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.matchers.MatcherSample import MatcherSample
from mcrit.matchers.MatcherVs import MatcherVs
from mcrit.matchers.MatcherVsGroup import MatcherVsGroup
from mcrit.queue.QueueRemoteCalls import UncacheableResult
from mcrit.server.MatchResource import MatchResource
from mcrit.server.QueryResource import QueryResource
from mcrit.server.utils import MatchingParameterError, getMatchingParams
from mcrit.storage.MatchingResult import MatchingResult
from mcrit.storage.MongoDbStorage import MongoDbStorage
from mcrit.Worker import Worker

from .context import config, getTestMongoServerAndPort


def configured(shortlist_size=0, band_df_cutoff=0):
    mcrit_config = deepcopy(config)
    # new objects, not edits: McritConfig holds its sub-configs as class attributes shared by every copy
    mcrit_config.MINHASH_CONFIG = MinHashConfig(MINHASH_MATCHING_SHORTLIST_SIZE=shortlist_size)
    mcrit_config.STORAGE_CONFIG = StorageConfig(STORAGE_METHOD=config.STORAGE_CONFIG.STORAGE_METHOD, STORAGE_BAND_DF_CUTOFF=band_df_cutoff)
    return mcrit_config


class MatchingParamsTest(unittest.TestCase):
    def test_both_knobs_are_read_from_the_request(self):
        self.assertEqual({"shortlist_size": 25, "band_df_cutoff": 200}, getMatchingParams({"shortlist_size": "25", "band_df_cutoff": "200"}))

    def test_unusable_values_are_refused_not_replaced(self):
        """Replacing them with the configured value would answer a question the caller did not ask."""
        for key in ("shortlist_size", "band_df_cutoff"):
            for value in ("-3", "many", "1.5", "", str(2**63)):
                with self.subTest(key=key, value=value), self.assertRaises(MatchingParameterError):
                    getMatchingParams({key: value})
        self.assertEqual({"shortlist_size": 0, "band_df_cutoff": 2**63 - 1}, getMatchingParams({"shortlist_size": "0", "band_df_cutoff": str(2**63 - 1)}))

    def test_the_server_fills_in_every_knob_the_request_leaves_out(self):
        """The job's arguments are its cache key, so they have to hold the values it runs with (#217)."""
        mcrit_config = configured(shortlist_size=100, band_df_cutoff=200)
        minhash_config = mcrit_config.MINHASH_CONFIG
        defaults = {
            "minhash_threshold": minhash_config.MINHASH_MATCHING_THRESHOLD,
            "pichash_size": minhash_config.PICHASH_SIZE,
            "band_matches_required": minhash_config.BAND_MATCHES_REQUIRED,
            "shortlist_size": 100,
            "band_df_cutoff": 200,
        }
        self.assertEqual(defaults, getMatchingParams({}, mcrit_config))
        self.assertEqual({**defaults, "shortlist_size": 5, "minhash_threshold": 70}, getMatchingParams({"shortlist_size": "5", "minhash_score": "70"}, mcrit_config))
        # without the configuration only what the request named, as before
        self.assertEqual({"band_matches_required": 1}, getMatchingParams({"band_matches_required": "1"}))

    def test_matches_restricted_to_named_samples_refuse_a_shortlist(self):
        """Refused, not dropped: a silently different answer would not say what it left out."""
        mcrit_config = configured(shortlist_size=100)
        for request, with_shortlist in (({"shortlist_size": "5"}, False), ({"sample_group_only": "true", "shortlist_size": "5"}, True), ({"shortlist_size": "-1"}, False)):
            with self.subTest(request=request), self.assertRaises(MatchingParameterError):
                getMatchingParams(request, mcrit_config, with_shortlist=with_shortlist)
        # the configured shortlist does not apply to them either, and is not put into their arguments
        self.assertNotIn("shortlist_size", getMatchingParams({}, mcrit_config, with_shortlist=False))
        self.assertNotIn("shortlist_size", getMatchingParams({"sample_group_only": "true"}, mcrit_config))

    def test_a_repeated_parameter_is_refused_for_the_new_knobs(self):
        """falcon hands a repeated query parameter over as a list."""
        for key in ("shortlist_size", "band_df_cutoff"):
            with self.subTest(key), self.assertRaisesRegex(MatchingParameterError, "must be an integer"):
                getMatchingParams({key: ["1", "2"]})


class ResolveMatchingParamsTest(unittest.TestCase):
    """The resolution MinHashIndex applies to every matching job, whoever submits it."""

    def test_a_shortlist_the_storage_cannot_apply_is_marked_in_the_arguments(self):
        """So a fallback result gets its own cache key and is not served once the shortlist works again."""
        mcrit_config = configured(shortlist_size=100)
        storage = MagicMock()
        storage.isFunctionRangeIndexComplete.return_value = False
        self.assertEqual("function_range_index_incomplete", resolveMatchingParams({}, mcrit_config, storage=storage)["shortlist_unavailable"])
        storage.isFunctionRangeIndexComplete.return_value = True
        self.assertNotIn("shortlist_unavailable", resolveMatchingParams({}, mcrit_config, storage=storage))
        # a storage that cannot resolve samples at all
        self.assertEqual("function_range_index_unsupported", resolveMatchingParams({}, mcrit_config, storage=object())["shortlist_unavailable"])
        # nothing to check, and no read, when no shortlist is asked for
        storage.reset_mock()
        self.assertNotIn("shortlist_unavailable", resolveMatchingParams({"shortlist_size": 0}, mcrit_config, storage=storage))
        storage.isFunctionRangeIndexComplete.assert_not_called()
        # a mark the server already made is kept, and not checked again
        marked = resolveMatchingParams({"shortlist_unavailable": "function_range_index_incomplete"}, mcrit_config, storage=storage)
        self.assertEqual("function_range_index_incomplete", marked["shortlist_unavailable"])
        storage.isFunctionRangeIndexComplete.assert_not_called()

    def test_the_knobs_come_first_in_a_fixed_order(self):
        """So a job listing shows each knob in the same position whichever of them a request named."""
        resolved = resolveMatchingParams({"force_recalculation": True, "band_df_cutoff": 3, "shortlist_size": 5}, configured())
        self.assertEqual([*MATCHING_KNOBS[:5], "force_recalculation"], list(resolved))

    def test_a_marker_without_a_shortlist_is_dropped(self):
        """Nothing to fall back from: keeping it would split the cache key for the same job."""
        resolved = resolveMatchingParams({"shortlist_size": 0, "shortlist_unavailable": "function_range_index_incomplete"}, configured())
        self.assertNotIn("shortlist_unavailable", resolved)
        self.assertEqual(resolveMatchingParams({"shortlist_size": 0}, configured()), resolved)

    def test_equal_values_make_one_key(self):
        """A float or bool that equals the configured int is the same job."""
        mcrit_config = configured()
        plain = resolveMatchingParams({}, mcrit_config)
        self.assertEqual(
            plain, resolveMatchingParams({"minhash_threshold": float(plain["minhash_threshold"]), "band_matches_required": float(plain["band_matches_required"])}, mcrit_config)
        )
        self.assertEqual(1, resolveMatchingParams({"band_matches_required": True}, mcrit_config)["band_matches_required"])
        self.assertEqual(json.dumps(plain, sort_keys=True), json.dumps(resolveMatchingParams({"pichash_size": float(plain["pichash_size"])}, mcrit_config), sort_keys=True))

    def test_named_samples_take_no_shortlist(self):
        resolved = resolveMatchingParams({"shortlist_size": 5, "shortlist_unavailable": "function_range_index_incomplete"}, configured(shortlist_size=10), with_shortlist=False)
        self.assertNotIn("shortlist_size", resolved)
        self.assertNotIn("shortlist_unavailable", resolved)

    def test_a_cutoff_above_the_bucket_size_is_refused_for_direct_callers_too(self):
        mcrit_config = configured()
        mcrit_config.STORAGE_CONFIG = StorageConfig(STORAGE_METHOD=config.STORAGE_CONFIG.STORAGE_METHOD, STORAGE_BAND_BUCKET_SIZE=100)
        with self.assertRaisesRegex(MatchingParameterError, "STORAGE_BAND_BUCKET_SIZE"):
            resolveMatchingParams({"band_df_cutoff": 101}, mcrit_config)
        self.assertEqual(100, resolveMatchingParams({"band_df_cutoff": 100}, mcrit_config)["band_df_cutoff"])


class JobCacheTest(unittest.TestCase):
    """A job is reused for a request with the same arguments; the knobs have to be among them."""

    def setUp(self):
        self.index = MinHashIndex(config=configured())
        report = SmdaReport.fromFile("tests/example_report.smda")
        self.sample_id = self.index._storage.addSmdaReport(report).sample_id

    def test_different_knobs_are_different_jobs(self):
        first = self.index.getMatchesForSample(self.sample_id, shortlist_size=0, band_df_cutoff=0)
        self.assertEqual(first, self.index.getMatchesForSample(self.sample_id, shortlist_size=0, band_df_cutoff=0))
        self.assertNotEqual(first, self.index.getMatchesForSample(self.sample_id, shortlist_size=10, band_df_cutoff=0))
        self.assertNotEqual(first, self.index.getMatchesForSample(self.sample_id, shortlist_size=0, band_df_cutoff=10))

    def test_a_changed_configuration_is_a_different_job_through_the_api(self):
        resource_before = MatchResource(self.index)
        app = falcon.App()
        app.add_route("/matches/sample/{sample_id:int}", resource_before, suffix="sample")
        before = falcon.testing.TestClient(app).simulate_get(f"/matches/sample/{self.sample_id}").json["data"]
        # the same corpus, served by a deployment configured with a shortlist
        self.index.config = configured(shortlist_size=10)
        after = falcon.testing.TestClient(app).simulate_get(f"/matches/sample/{self.sample_id}").json["data"]
        self.assertNotEqual(before, after)
        # each job records the settings it ran with, whether or not the request named them
        recorded = [json.loads(self.index.getJobData(job_id)["payload"]["params"]) for job_id in (before, after)]
        self.assertEqual([(0, 0), (10, 0)], [(params["shortlist_size"], params["band_df_cutoff"]) for params in recorded])

    def _requestThroughTheApi(self, query_string=""):
        app = falcon.App()
        app.add_route("/matches/sample/{sample_id:int}", MatchResource(self.index), suffix="sample")
        return falcon.testing.TestClient(app).simulate_get(f"/matches/sample/{self.sample_id}", query_string=query_string).json["data"]

    def test_a_request_relying_on_the_defaults_is_keyed_on_their_values(self):
        """Not on their absence: naming the configured values is the same job, changing a default is not."""
        minhash_config = self.index.config.MINHASH_CONFIG
        implicit = self._requestThroughTheApi()
        explicit = self._requestThroughTheApi(
            f"minhash_score={minhash_config.MINHASH_MATCHING_THRESHOLD}&pichash_size={minhash_config.PICHASH_SIZE}"
            f"&band_matches_required={minhash_config.BAND_MATCHES_REQUIRED}&shortlist_size=0&band_df_cutoff=0"
        )
        self.assertEqual(implicit, explicit)
        for knob, value in (("MINHASH_MATCHING_THRESHOLD", 70), ("PICHASH_SIZE", 20), ("BAND_MATCHES_REQUIRED", 1)):
            with self.subTest(knob):
                changed = configured()
                setattr(changed.MINHASH_CONFIG, knob, value)
                self.index.config = changed
                self.assertNotEqual(implicit, self._requestThroughTheApi())
                self.index.config = configured()

    def test_a_fallback_result_is_not_served_once_the_shortlist_works(self):
        self.index.config = configured(shortlist_size=10)
        self.index._storage._function_range_index_complete = False
        during_rebuild = self._requestThroughTheApi()
        params = json.loads(self.index.getJobData(during_rebuild)["payload"]["params"])
        self.assertEqual("function_range_index_incomplete", params["shortlist_unavailable"])
        self.assertEqual(during_rebuild, self._requestThroughTheApi())
        self.index._storage._function_range_index_complete = True
        self.assertNotEqual(during_rebuild, self._requestThroughTheApi())

    def test_a_fallback_nobody_foresaw_is_never_served_again(self):
        """The index was complete at submission and not when the job ran (a rebuild started in between)."""
        self.index.config = configured(shortlist_size=10)
        self.index._storage._function_range_index_complete = False
        # the submission-time check sees a complete index, the job itself does not
        with patch("mcrit.index.MatchingParameters.shortlistUnavailableReason", return_value=None):
            first = self._requestThroughTheApi()
            job_data = self.index.getJobData(first)
            self.assertNotIn("shortlist_unavailable", json.loads(job_data["payload"]["params"]))
            self.assertIs(False, job_data["cacheable"])
            # the identical request is not handed the fallback result
            self.assertNotEqual(first, self._requestThroughTheApi())
        self.index._storage._function_range_index_complete = True
        shortlisted = self._requestThroughTheApi()
        self.assertEqual(shortlisted, self._requestThroughTheApi())
        self.assertNotIn("cacheable", self.index.getJobData(shortlisted))

    def test_every_matching_job_method_resolves_its_knobs(self):
        """All six MinHashIndex job methods, as the server and a direct caller reach them."""
        submitted = {}
        remote = MinHashIndex.__mro__[1]
        minhash_config = self.index.config.MINHASH_CONFIG
        expected = {
            "minhash_threshold": minhash_config.MINHASH_MATCHING_THRESHOLD,
            "pichash_size": minhash_config.PICHASH_SIZE,
            "band_matches_required": minhash_config.BAND_MATCHES_REQUIRED,
            "band_df_cutoff": 0,
        }
        calls = {
            "getMatchesForSample": ((7,), True),
            "getMatchesForSmdaReport": (({},), True),
            "getMatchesForMappedBinary": ((b"MZ", 0x1000), True),
            "getMatchesForUnmappedBinary": ((b"MZ",), True),
            "getMatchesForSampleVs": ((7, 8), False),
            "getMatchesForSampleVsGroup": ((7, [8, 9]), False),
        }
        for name, (args, takes_shortlist) in calls.items():
            with self.subTest(name), patch.object(remote, name, create=True) as submit:
                submit.side_effect = lambda *a, **kw: submitted.__setitem__("kwargs", kw) or "job"
                getattr(self.index, name)(*args, username="alice")
                kwargs = submitted["kwargs"]
                self.assertEqual("alice", kwargs["username"])
                self.assertEqual(expected, {knob: kwargs[knob] for knob in expected})
                self.assertEqual(takes_shortlist, "shortlist_size" in kwargs)
                if not takes_shortlist:
                    with self.assertRaises(TypeError):
                        getattr(self.index, name)(*args, shortlist_size=3)
                    # None means "not set" and passes, as it does everywhere else
                    getattr(self.index, name)(*args, shortlist_size=None, shortlist_unavailable=None)
                    self.assertNotIn("shortlist_size", submitted["kwargs"])

    def test_direct_callers_are_keyed_on_the_values_too(self):
        """Resolution happens in MinHashIndex, so a script or library caller gets it as the server does."""
        minhash_config = self.index.config.MINHASH_CONFIG
        implicit = self.index.getMatchesForSample(self.sample_id)
        explicit = self.index.getMatchesForSample(
            self.sample_id,
            minhash_threshold=minhash_config.MINHASH_MATCHING_THRESHOLD,
            pichash_size=minhash_config.PICHASH_SIZE,
            band_matches_required=minhash_config.BAND_MATCHES_REQUIRED,
            shortlist_size=0,
            band_df_cutoff=0,
        )
        self.assertEqual(implicit, explicit)
        changed = configured()
        changed.MINHASH_CONFIG.MINHASH_MATCHING_THRESHOLD = 70
        self.index.config = changed
        self.assertNotEqual(implicit, self.index.getMatchesForSample(self.sample_id))


class ResultStorageTest(unittest.TestCase):
    """Both ways a worker stores a finished job's result mark an UncacheableResult's job (#217)."""

    def _worker_and_job(self, result):
        from mcrit.SingleJobWorker import SingleJobWorker

        worker = SingleJobWorker.__new__(SingleJobWorker)
        worker.queue = MagicMock()
        worker.queue.clean_interval = 10**9
        worker.queue._dicts_to_grid.return_value = "0123456789abcdef01234567"
        worker.queue.collection.find_one_and_update.return_value = {"_id": "job"}
        worker.t_last_cleanup = __import__("time").time()
        worker._executeJobProfiled = None
        job = MagicMock()
        job.job_id = "job"
        job.__enter__.return_value = {"payload": {}}
        job.__exit__.return_value = False
        return worker, job

    def test_both_execution_paths_mark_an_uncacheable_result(self):
        # QueueRemoteCallee._executeJobImpl serves mcrit worker; SingleJobWorker._executeJob serves
        # each job mcrit spawningworker hands to a child process
        for path in ("_executeJobImpl", "_executeJob"):
            for result, marked in ((UncacheableResult({"info": {}}), True), ({"info": {}}, False)):
                with self.subTest(path=path, uncacheable=marked):
                    worker, job = self._worker_and_job(result)
                    with patch.object(type(worker), "_executeJobPayload", return_value=result):
                        getattr(worker, path)(job)
                    self.assertEqual(marked, job.mark_uncacheable.called)
                    worker.queue._dicts_to_grid.assert_called_once()


class MinHashThresholdTest(unittest.TestCase):
    """A request's minhash_score is applied: matching used to filter on the configured threshold only."""

    def test_the_requested_threshold_decides_which_matches_are_reported(self):
        mcrit_config = configured()
        mcrit_config.MINHASH_CONFIG.MINHASH_POOL_INDEXING = False
        mcrit_config.MINHASH_CONFIG.MINHASH_POOL_MATCHING = False
        index = MinHashIndex(config=mcrit_config)
        worker = index.queue._worker
        sample_ids = [index._storage.addSmdaReport(SmdaReport.fromFile(f"tests/{name}")).sample_id for name in ("example_report.smda", "example_report_2.smda")]
        for sample_id in sample_ids:
            worker.updateMinHashesForSample(sample_id)

        def foreign_scores(**knobs):
            report = worker.getMatchesForSample(sample_ids[0], **knobs)
            return [match[3] for function in report["matches"]["functions"] for match in function["matches"] if match[1] != sample_ids[0]]

        # every path that filters on the threshold: vectorized, and the pairwise one both in this
        # process and in the process pool (the default configuration)
        for vectorized, pool in ((True, False), (False, False), (False, True)):
            with self.subTest(vectorized=vectorized, pool=pool):
                mcrit_config.MINHASH_CONFIG.MINHASH_MATCHING_VECTORIZED = vectorized
                mcrit_config.MINHASH_CONFIG.MINHASH_POOL_MATCHING = pool
                by_default = foreign_scores()
                self.assertTrue(any(score <= 80 for score in by_default), "the fixture must have matches the higher threshold drops")
                stricter = foreign_scores(minhash_threshold=80)
                self.assertTrue(stricter)
                self.assertTrue(all(score > 80 for score in stricter), stricter)
                self.assertEqual(sorted(score for score in by_default if score > 80), sorted(stricter))


class MatchingInfoTest(unittest.TestCase):
    """The report says which knobs the job was asked for, which it applied, and why they differ (#217)."""

    # on these three reports a shortlist of 1 drops one of the two samples sample 0 matches
    REPORTS = ("example_report.smda", "example_report_2.smda", "example_report_3.smda")

    def _index(self, mcrit_config):
        mcrit_config.MINHASH_CONFIG.MINHASH_POOL_INDEXING = False
        mcrit_config.MINHASH_CONFIG.MINHASH_POOL_MATCHING = False
        index = MinHashIndex(config=mcrit_config)
        sample_ids = [index._storage.addSmdaReport(SmdaReport.fromFile(f"tests/{name}")).sample_id for name in self.REPORTS]
        for sample_id in sample_ids:
            index.queue._worker.updateMinHashesForSample(sample_id)
        return index, index.queue._worker, sample_ids[0]

    def setUp(self):
        self.index, self.worker, self.sample_id = self._index(configured())
        self.unshortlisted = self.worker.getMatchesForSample(self.sample_id)["matches"]
        self.shortlisted = self.worker.getMatchesForSample(self.sample_id, shortlist_size=1)["matches"]
        self.assertNotEqual(self.unshortlisted, self.shortlisted, "the fixture must be one a shortlist changes")

    def test_the_defaults_are_recorded_as_applied(self):
        info = self.worker.getMatchesForSample(self.sample_id)["info"]["matching"]
        minhash_config = self.index.config.MINHASH_CONFIG
        self.assertEqual({"minhash_threshold": None, "pichash_size": None, "band_matches_required": None, "shortlist_size": None, "band_df_cutoff": None}, info["requested"])
        self.assertEqual(
            {
                "minhash_threshold": minhash_config.MINHASH_MATCHING_THRESHOLD,
                "pichash_size": minhash_config.PICHASH_SIZE,
                "band_matches_required": minhash_config.BAND_MATCHES_REQUIRED,
                "shortlist_size": 0,
                "band_df_cutoff": 0,
            },
            info["applied"],
        )
        self.assertEqual({}, info["fallbacks"])

    def test_the_knobs_a_job_was_given_are_recorded_as_requested_and_applied(self):
        index, worker, sample_id = self._index(configured(band_df_cutoff=7))
        info = worker.getMatchesForSample(sample_id, minhash_threshold=70, pichash_size=20, band_matches_required=1)["info"]["matching"]
        self.assertEqual({"minhash_threshold": 70, "pichash_size": 20, "band_matches_required": 1, "shortlist_size": None, "band_df_cutoff": None}, info["requested"])
        # the cutoff the job left to the configuration is the configured one
        self.assertEqual({"minhash_threshold": 70, "pichash_size": 20, "band_matches_required": 1, "shortlist_size": 0, "band_df_cutoff": 7}, info["applied"])

    def test_an_applied_shortlist_is_recorded(self):
        report = self.worker.getMatchesForSample(self.sample_id, shortlist_size=1, band_df_cutoff=7)
        info = report["info"]["matching"]
        self.assertEqual((1, 7), (info["requested"]["shortlist_size"], info["requested"]["band_df_cutoff"]))
        self.assertEqual((1, 7), (info["applied"]["shortlist_size"], info["applied"]["band_df_cutoff"]))
        self.assertEqual({}, info["fallbacks"])
        self.assertNotIsInstance(report, UncacheableResult)

    def test_a_shortlist_the_server_found_unavailable_is_reported_not_applied(self):
        report = self.worker.getMatchesForSample(self.sample_id, shortlist_size=1, shortlist_unavailable="function_range_index_incomplete")
        info = report["info"]["matching"]
        self.assertEqual((1, 0), (info["requested"]["shortlist_size"], info["applied"]["shortlist_size"]))
        self.assertEqual({"shortlist_size": "function_range_index_incomplete"}, info["fallbacks"])
        self.assertEqual(self.unshortlisted, report["matches"])
        # its arguments say it fell back, so its cache key is its own: an ordinary result
        self.assertNotIsInstance(report, UncacheableResult)

    def test_a_shortlist_that_became_unavailable_by_run_time_is_reported_and_not_cached(self):
        self.index._storage._function_range_index_complete = False
        report = self.worker.getMatchesForSample(self.sample_id, shortlist_size=1)
        self.assertEqual(0, report["info"]["matching"]["applied"]["shortlist_size"])
        self.assertEqual({"shortlist_size": "function_range_index_incomplete"}, report["info"]["matching"]["fallbacks"])
        self.assertEqual(self.unshortlisted, report["matches"])
        self.assertIsInstance(report, UncacheableResult)

    def test_a_storage_that_cannot_shortlist_is_reported(self):
        with patch.object(self.index._storage, "getSampleIdsForFunctionIdArray", None):
            report = self.worker.getMatchesForSample(self.sample_id, shortlist_size=1)
        self.assertEqual({"shortlist_size": "function_range_index_unsupported"}, report["info"]["matching"]["fallbacks"])
        self.assertIsInstance(report, UncacheableResult)

    def test_an_index_that_goes_incomplete_mid_job_is_reported(self):
        # complete for the check before shortlisting starts, incomplete once it resolves candidates
        answers = iter([True])
        with patch.object(self.index._storage, "isFunctionRangeIndexComplete", side_effect=lambda: next(answers, False)):
            report = self.worker.getMatchesForSample(self.sample_id, shortlist_size=1)
        self.assertEqual({"shortlist_size": "function_range_index_incomplete"}, report["info"]["matching"]["fallbacks"])
        self.assertEqual(self.unshortlisted, report["matches"])
        self.assertIsInstance(report, UncacheableResult)

    def test_vs_matching_records_that_no_shortlist_applies(self):
        index, worker, sample_id = self._index(configured(shortlist_size=1))
        info = worker.getMatchesForSampleVs(sample_id, sample_id + 1)["info"]["matching"]
        self.assertIsNone(info["applied"]["shortlist_size"])
        group_info = worker.getMatchesForSampleVsGroup(sample_id, [sample_id + 1, sample_id + 2])["info"]["matching"]
        self.assertIsNone(group_info["applied"]["shortlist_size"])
        self.assertEqual({}, info["fallbacks"])

    def test_without_a_minhash_stage_neither_shortlist_nor_cutoff_applies(self):
        info = self.worker.getMatchesForSample(self.sample_id, band_matches_required=0, shortlist_size=1, band_df_cutoff=7)["info"]["matching"]
        self.assertEqual((None, None), (info["applied"]["shortlist_size"], info["applied"]["band_df_cutoff"]))
        self.assertEqual({}, info["fallbacks"])

    def test_it_survives_a_round_trip_through_matching_result(self):
        report = self.worker.getMatchesForSample(self.sample_id, shortlist_size=1, shortlist_unavailable="function_range_index_incomplete")
        result = MatchingResult.fromDict(report)
        self.assertEqual(report["info"]["matching"], result.matching_info)
        self.assertEqual(report["info"]["matching"], result.toDict()["info"]["matching"])
        # reports from before this carry none, and gain none
        del report["info"]["matching"]
        self.assertNotIn("matching", MatchingResult.fromDict(report).toDict()["info"])


class MemoryStorageBandDfCutoffTest(unittest.TestCase):
    def test_the_cutoff_skips_common_band_hashes_per_lookup_and_by_configuration(self):
        index = MinHashIndex(config=configured())
        worker = index.queue._worker
        sample_ids = [
            index._storage.addSmdaReport(SmdaReport.fromFile(f"tests/{name}")).sample_id for name in ("example_report.smda", "example_report_2.smda", "example_report_3.smda")
        ]
        for sample_id in sample_ids:
            worker.updateMinHashesForSample(sample_id)
        bits = index.config.MINHASH_CONFIG.MINHASH_SIGNATURE_BITS
        minhashes = {entry.function_id: entry.getMinHash(minhash_bits=bits) for entry in index._storage.getFunctionsBySampleId(sample_ids[0]) if entry.minhash}
        unrestricted = index._storage.getCandidatesForMinHashes(minhashes, band_matches_required=1)
        restricted = index._storage.getCandidatesForMinHashes(minhashes, band_matches_required=1, band_df_cutoff=1)
        self.assertLess(sum(map(len, restricted.values())), sum(map(len, unrestricted.values())))
        index._storage._storage_config = configured(band_df_cutoff=1).STORAGE_CONFIG
        self.assertEqual(restricted, index._storage.getCandidatesForMinHashes(minhashes, band_matches_required=1))
        self.assertEqual(unrestricted, index._storage.getCandidatesForMinHashes(minhashes, band_matches_required=1, band_df_cutoff=0))

    def test_a_posting_list_as_long_as_the_cutoff_is_kept(self):
        index = MinHashIndex(config=configured())
        worker = index.queue._worker
        sample_ids = [
            index._storage.addSmdaReport(SmdaReport.fromFile(f"tests/{name}")).sample_id for name in ("example_report.smda", "example_report_2.smda", "example_report_3.smda")
        ]
        for sample_id in sample_ids:
            worker.updateMinHashesForSample(sample_id)
        storage = index._storage
        bits = index.config.MINHASH_CONFIG.MINHASH_SIGNATURE_BITS
        minhashes = {entry.function_id: entry.getMinHash(minhash_bits=bits) for entry in storage.getFunctionsBySampleId(sample_ids[0]) if entry.minhash}

        def posting_lengths(minhash):
            return [len(storage._bands[band][band_hash]) for band, band_hash in storage.getBandHashesForMinHash(minhash).items() if band_hash in storage._bands[band]]

        assertCutoffKeepsListsOfItsLength(self, storage, minhashes, posting_lengths)


def assertCutoffKeepsListsOfItsLength(test, storage, minhashes, posting_lengths):
    """A band hash whose posting list is exactly as long as the cutoff is kept, one longer is skipped.

    Finds a query function for which skipping its longest posting lists changes the candidates, so
    that keeping or dropping the lists at the boundary shows in the result.
    """
    for function_id, minhash in minhashes.items():
        lengths = posting_lengths(minhash)
        if not lengths or max(lengths) < 2:
            continue
        longest = max(lengths)

        def candidates(cutoff):
            return storage.getCandidatesForMinHashes({function_id: minhash}, band_matches_required=1, band_df_cutoff=cutoff)[function_id]

        if candidates(longest - 1) != candidates(0):
            test.assertEqual(candidates(0), candidates(longest))
            return
    test.fail("no query function whose longest posting lists decide its candidates")


class ClientTest(unittest.TestCase):
    def test_the_client_sends_the_knobs_only_when_given(self):
        client = McritClient("http://mcrit.test")
        with patch("mcrit.client.McritClient.requests.get") as get:
            get.return_value.status_code = 200
            get.return_value.json.return_value = {"status": "successful", "data": "0123456789abcdef01234567"}
            client.requestMatchesForSample(7, shortlist_size=25, band_df_cutoff=200)
            self.assertEqual({"shortlist_size": 25, "band_df_cutoff": 200, "force_recalculation": False}, get.call_args.kwargs["params"])
            client.requestMatchesForSample(7)
            self.assertNotIn("shortlist_size", get.call_args.kwargs["params"])

    def test_the_route_hands_them_to_the_job(self):
        index = MagicMock()
        index.config = configured(shortlist_size=100)
        index.isSampleId.return_value = True
        app = falcon.App()
        app.add_route("/matches/sample/{sample_id:int}", MatchResource(index), suffix="sample")
        falcon.testing.TestClient(app).simulate_get("/matches/sample/7", query_string="band_df_cutoff=50")
        self.assertEqual((100, 50), (index.getMatchesForSample.call_args.kwargs["shortlist_size"], index.getMatchesForSample.call_args.kwargs["band_df_cutoff"]))


KNOBS = {"shortlist_size": 3, "band_df_cutoff": 4}


class ForwardingTest(unittest.TestCase):
    """Every matching path hands the knobs on to the matcher that applies them."""

    def test_worker_methods_hand_both_knobs_to_their_matcher(self):
        worker = Worker.__new__(Worker)
        knobs = {**KNOBS, "shortlist_unavailable": "function_range_index_incomplete"}
        cases = {
            "MatcherSample": lambda: worker.getMatchesForSample(1, **knobs),
            "MatcherQuery": lambda: worker.getMatchesForSmdaReport({}, **knobs),
            "MatcherQuery ": lambda: worker.getMatchesForMappedBinary(b"", 0x1000, **knobs),
            "MatcherQuery  ": lambda: worker.getMatchesForUnmappedBinary(b"", **knobs),
        }
        for matcher_name, call in cases.items():
            for fell_back in (False, True):
                with (
                    self.subTest(matcher_name, fell_back=fell_back),
                    patch(f"mcrit.Worker.{matcher_name.strip()}") as matcher,
                    patch("mcrit.Worker.SmdaReport"),
                    patch("mcrit.Worker.Disassembler"),
                ):
                    matcher.return_value.fellBackUnforeseen.return_value = fell_back
                    matcher.return_value.getMatchesForSample.return_value = {"info": {}}
                    matcher.return_value.getMatchesForSmdaReport.return_value = {"info": {}}
                    result = call()
                    self.assertEqual(knobs, {knob: matcher.call_args.kwargs[knob] for knob in knobs})
                    # a fallback its arguments did not foresee keeps the job from being reused
                    self.assertEqual(fell_back, isinstance(result, UncacheableResult))

    def test_vs_matching_takes_the_cutoff_and_no_shortlist(self):
        worker = Worker.__new__(Worker)
        for matcher_name, call in {
            "MatcherVs": lambda: worker.getMatchesForSampleVs(1, 2, band_df_cutoff=4),
            "MatcherVsGroup": lambda: worker.getMatchesForSampleVsGroup(1, [2, 3], band_df_cutoff=4),
        }.items():
            with self.subTest(matcher_name), patch(f"mcrit.Worker.{matcher_name}") as matcher:
                call()
                self.assertEqual(4, matcher.call_args.kwargs["band_df_cutoff"])
                self.assertNotIn("shortlist_size", matcher.call_args.kwargs)

    def test_function_queries_hand_every_knob_to_their_matcher(self):
        """Including the threshold and PicHash size, which were passed on as None and so never applied."""
        report = MagicMock(xcfg={0x1000: {}}, sha256="ab" * 32)
        knobs = {**KNOBS, "minhash_threshold": 70, "pichash_size": 20, "band_matches_required": 1, "shortlist_unavailable": "function_range_index_incomplete"}
        with patch("mcrit.index.MinHashIndex.SmdaReport") as smda_report, patch("mcrit.index.MinHashIndex.MatcherQueryFunction") as matcher:
            smda_report.fromDict.return_value = report
            matcher.return_value.getMatchesForSmdaFunction.return_value = {"info": {"job": {}}}
            # force_recalculation arrives as a query parameter and must not break the call
            MinHashIndex.getMatchesForSmdaFunction(MagicMock(), report, force_recalculation=True, exclude_self_matches=True, **knobs)
        self.assertEqual(knobs, {knob: matcher.call_args.kwargs[knob] for knob in knobs})
        self.assertIs(True, matcher.call_args.kwargs["exclude_self_matches"])

    def test_the_function_query_route_hands_on_exclude_self_matches(self):
        """McritClient.getMatchesForSmdaFunction sends it; the route used to drop it."""
        index = MagicMock()
        index.config = configured()
        app = falcon.App()
        app.add_route("/query/function", QueryResource(index), suffix="query_smda_function")
        client = falcon.testing.TestClient(app)
        for query, expected in (("exclude_self_matches=True", True), ("", False)):
            with self.subTest(query=query):
                client.simulate_post("/query/function", query_string=query, json={})
                self.assertIs(expected, index.getMatchesForSmdaFunction.call_args.kwargs["exclude_self_matches"])

    def test_group_only_cross_matching_leaves_the_shortlist_out(self):
        index = MagicMock()
        MinHashIndex.getMatchesCross(index, [1, 2], sample_group_only=True, shortlist_unavailable="function_range_index_incomplete", band_df_cutoff=4)
        self.assertEqual({"band_df_cutoff": 4}, {knob: value for knob, value in index.getMatchesForSampleVsGroup.call_args.kwargs.items() if knob in KNOBS})
        self.assertNotIn("shortlist_unavailable", index.getMatchesForSampleVsGroup.call_args.kwargs)

    def test_cross_matching_asks_its_children_for_no_shortlist(self):
        """A cross compare reads the named samples out of each child's report; a shortlist could drop them."""
        index = MagicMock()
        MinHashIndex.getMatchesCross(index, [1, 2], shortlist_unavailable="function_range_index_incomplete", shortlist_size=None, band_df_cutoff=4)
        kwargs = index.getMatchesForSample.call_args.kwargs
        # explicitly 0, since a child left without one would take the configured shortlist
        self.assertEqual({"shortlist_size": 0, "band_df_cutoff": 4}, {knob: kwargs[knob] for knob in KNOBS})
        self.assertNotIn("shortlist_unavailable", kwargs)

    def test_cross_matching_refuses_a_shortlist_it_is_asked_for(self):
        """Refused rather than dropped, as on the vs routes and the cross route."""
        for sample_group_only in (False, True):
            with self.subTest(sample_group_only=sample_group_only):
                index = MagicMock()
                with self.assertRaises(TypeError):
                    MinHashIndex.getMatchesCross(index, [1, 2], sample_group_only=sample_group_only, **KNOBS)
                index.getMatchesForSample.assert_not_called()
                index.getMatchesForSampleVsGroup.assert_not_called()

    def test_cross_matching_keeps_the_samples_it_names(self):
        """End to end: a configured shortlist of 1 used to zero the pairs a cross compare exists for."""
        mcrit_config = configured(shortlist_size=1)
        mcrit_config.MINHASH_CONFIG.MINHASH_POOL_INDEXING = False
        mcrit_config.MINHASH_CONFIG.MINHASH_POOL_MATCHING = False
        index = MinHashIndex(config=mcrit_config)
        sample_ids = [index._storage.addSmdaReport(SmdaReport.fromFile(f"tests/{name}")).sample_id for name in MatchingInfoTest.REPORTS]
        for sample_id in sample_ids:
            index.queue._worker.updateMinHashesForSample(sample_id)
        cross = index.getResultForJob(index.getMatchesCross(sample_ids))
        unrestricted = index.queue._worker.getMatchesForSample(sample_ids[0], shortlist_size=0)
        matched = {sample["sample_id"] for sample in unrestricted["matches"]["samples"]} - {sample_ids[0]}
        self.assertEqual(2, len(matched))
        for other_id in matched:
            with self.subTest(other_id=other_id):
                self.assertGreater(cross["unweighted"]["matching_percent"][str(sample_ids[0])][str(other_id)], 0)

    def test_the_client_sends_them_on_every_matching_request(self):
        client = McritClient("http://mcrit.test")
        cases = {
            "requestMatchesForSmdaReport": ("post", lambda: client.requestMatchesForSmdaReport(MagicMock(), **KNOBS), KNOBS),
            "requestMatchesForMappedBinary": ("post", lambda: client.requestMatchesForMappedBinary(b"", 0x1000, disassemble_locally=False, **KNOBS), KNOBS),
            "requestMatchesForUnmappedBinary": ("post", lambda: client.requestMatchesForUnmappedBinary(b"", disassemble_locally=False, **KNOBS), KNOBS),
            "requestMatchesForSample": ("get", lambda: client.requestMatchesForSample(1, **KNOBS), KNOBS),
            "requestMatchesForSampleVs": ("get", lambda: client.requestMatchesForSampleVs(1, 2, band_df_cutoff=4), {"band_df_cutoff": 4}),
            "requestMatchesCross": ("get", lambda: client.requestMatchesCross([1, 2], band_df_cutoff=4), {"band_df_cutoff": 4}),
            "getMatchesForSmdaFunction": ("post", lambda: client.getMatchesForSmdaFunction(MagicMock(), **KNOBS), KNOBS),
        }
        for name, (verb, call, expected) in cases.items():
            with self.subTest(name), patch(f"mcrit.client.McritClient.requests.{verb}") as request:
                request.return_value.status_code = 200
                request.return_value.json.return_value = {"status": "successful", "data": "0123456789abcdef01234567"}
                call()
                params = request.call_args.kwargs["params"]
                self.assertEqual(expected, {knob: params[knob] for knob in KNOBS if knob in params})


class VsShortlistTest(unittest.TestCase):
    """A match against named samples takes no shortlist: one ranked over the corpus could drop them."""

    def _app(self, index):
        app = falcon.App()
        resource = MatchResource(index)
        app.add_route("/matches/sample/{sample_id:int}/{sample_id_b:int}", resource, suffix="sample_vs")
        app.add_route("/matches/sample/cross/{sample_ids}", resource, suffix="sample_cross")
        return falcon.testing.TestClient(app)

    def test_the_vs_group_and_cross_routes_take_none(self):
        index = MagicMock()
        index.config = configured(shortlist_size=100, band_df_cutoff=200)
        index.isSampleId.return_value = True
        client = self._app(index)
        # the configured shortlist is not applied to them, and not put into their arguments
        client.simulate_get("/matches/sample/1/2")
        self.assertNotIn("shortlist_size", index.getMatchesForSampleVs.call_args.kwargs)
        self.assertEqual(200, index.getMatchesForSampleVs.call_args.kwargs["band_df_cutoff"])
        client.simulate_get("/matches/sample/cross/1,2", query_string="sample_group_only=true")
        self.assertNotIn("shortlist_size", index.getMatchesCross.call_args.kwargs)
        client.simulate_get("/matches/sample/cross/1,2")
        self.assertNotIn("shortlist_size", index.getMatchesCross.call_args.kwargs)
        # one asked for is refused, valid or not, and nothing is submitted
        index.reset_mock()
        for path, query in (
            ("/matches/sample/1/2", "shortlist_size=5"),
            ("/matches/sample/cross/1,2", "shortlist_size=5"),
            ("/matches/sample/cross/1,2", "sample_group_only=true&shortlist_size=5"),
            ("/matches/sample/1/2", "shortlist_size=-1"),
            ("/matches/sample/cross/1,2", "shortlist_size=abc"),
        ):
            with self.subTest(path=path, query=query):
                self.assertEqual(400, client.simulate_get(path, query_string=query).status_code)
        index.getMatchesForSampleVs.assert_not_called()
        index.getMatchesCross.assert_not_called()

    def test_vs_matchers_ignore_a_configured_shortlist(self):
        index = MinHashIndex(config=configured(shortlist_size=1))
        worker = index.queue._worker
        self.assertEqual(1, MatcherSample(worker)._getShortlistSize())
        self.assertEqual(0, MatcherVs(worker)._getShortlistSize())
        self.assertEqual(0, MatcherVsGroup(worker)._getShortlistSize())
        self.assertEqual(0, MatcherVs(worker, shortlist_size=5)._getShortlistSize())


class BucketSizeTest(unittest.TestCase):
    """Under band bucketing only bucket 0 carries df, so a cutoff above the bucket size cannot be applied."""

    def _config(self):
        mcrit_config = configured(band_df_cutoff=50)
        server, port = getTestMongoServerAndPort()
        # the test server, as every mongo-backed test resolves it: the storage connects lazily
        # and these tests never make it connect, but its configuration should not name another
        mcrit_config.STORAGE_CONFIG = StorageConfig(STORAGE_METHOD="mongodb", STORAGE_SERVER=server, STORAGE_PORT=port, STORAGE_BAND_DF_CUTOFF=50, STORAGE_BAND_BUCKET_SIZE=100)
        return mcrit_config

    def test_the_storage_refuses_a_job_cutoff_above_the_bucket_size(self):
        storage = MongoDbStorage(self._config())
        self.assertEqual(50, storage._bandDfCutoff(None))
        self.assertEqual(100, storage._bandDfCutoff(100))
        with self.assertRaisesRegex(ValueError, "STORAGE_BAND_BUCKET_SIZE"):
            storage._bandDfCutoff(101)

    def test_the_server_refuses_it(self):
        with self.assertRaisesRegex(MatchingParameterError, "STORAGE_BAND_BUCKET_SIZE"):
            getMatchingParams({"band_df_cutoff": "101"}, self._config())
        self.assertEqual(100, getMatchingParams({"band_df_cutoff": "100"}, self._config())["band_df_cutoff"])

    def test_every_matching_route_answers_it_with_a_400(self):
        index = MagicMock()
        index.config = self._config()
        index.isSampleId.return_value = True
        app = falcon.App()
        match_resource, query_resource = MatchResource(index), QueryResource(index)
        app.add_route("/matches/sample/{sample_id:int}", match_resource, suffix="sample")
        app.add_route("/matches/sample/{sample_id:int}/{sample_id_b:int}", match_resource, suffix="sample_vs")
        app.add_route("/matches/sample/cross/{sample_ids}", match_resource, suffix="sample_cross")
        app.add_route("/query", query_resource, suffix="query_smda")
        app.add_route("/query/function", query_resource, suffix="query_smda_function")
        app.add_route("/query/binary", query_resource, suffix="query_binary")
        app.add_route("/query/binary/mapped/{base_address}", query_resource, suffix="query_binary_mapped")
        client = falcon.testing.TestClient(app)
        requests = {
            "getMatchesForSample": lambda query: client.simulate_get("/matches/sample/1", query_string=query),
            "getMatchesForSampleVs": lambda query: client.simulate_get("/matches/sample/1/2", query_string=query),
            "getMatchesCross": lambda query: client.simulate_get("/matches/sample/cross/1,2", query_string=query),
            "getMatchesForSmdaReport": lambda query: client.simulate_post("/query", query_string=query, json={}),
            "getMatchesForSmdaFunction": lambda query: client.simulate_post("/query/function", query_string=query, json={}),
            "getMatchesForUnmappedBinary": lambda query: client.simulate_post("/query/binary", query_string=query, body=b"MZ"),
            "getMatchesForMappedBinary": lambda query: client.simulate_post("/query/binary/mapped/0x1000", query_string=query, body=b"MZ"),
        }
        for method, request in requests.items():
            with self.subTest(method):
                response = request("band_df_cutoff=101")
                self.assertEqual(400, response.status_code)
                self.assertIn("STORAGE_BAND_BUCKET_SIZE", response.json["data"]["message"])
                getattr(index, method).assert_not_called()
                response = request("band_df_cutoff=100")
                if "Binary" in method:
                    # the test client's WSGI validator rejects the handler's size-less stream.read(),
                    # which real servers accept; what matters here is that the value was not refused
                    self.assertNotEqual(400, response.status_code)
                else:
                    self.assertEqual(100, getattr(index, method).call_args.kwargs["band_df_cutoff"])


class PresetTest(unittest.TestCase):
    """Named bundles of knobs a caller picks per query (#217, step 4)."""

    def test_a_preset_fills_in_only_what_the_request_leaves_out(self):
        self.assertEqual({"band_matches_required": 1, "shortlist_size": 100}, applyMatchingPreset({}, "identification"))
        self.assertEqual({"band_matches_required": 3, "shortlist_size": 100}, applyMatchingPreset({"band_matches_required": 3}, "identification"))
        # None is "not set", as everywhere else
        self.assertEqual({"band_matches_required": 1, "shortlist_size": 0}, applyMatchingPreset({"band_matches_required": None, "shortlist_size": 0}, "identification"))
        self.assertEqual({"band_matches_required": 1, "shortlist_size": 0}, applyMatchingPreset({}, "hunt"))
        # a match restricted to the samples it names takes no shortlist, so it gets the rest
        self.assertEqual({"band_matches_required": 1}, applyMatchingPreset({}, "identification", with_shortlist=False))
        self.assertEqual({"band_matches_required": 1}, applyMatchingPreset({}, "hunt", with_shortlist=False))
        self.assertEqual({"hunt", "identification"}, set(MATCHING_PRESETS))
        # matched case-insensitively, for the server and a direct caller alike
        self.assertEqual({"band_matches_required": 1, "shortlist_size": 0}, applyMatchingPreset({}, " Hunt "))

    def test_identification_keeps_a_configured_shortlist_size(self):
        """A deployment that sized its shortlist keeps that size; the preset only turns it on."""
        self.assertEqual(500, applyMatchingPreset({}, "identification", config=configured(shortlist_size=500))["shortlist_size"])
        self.assertEqual(100, applyMatchingPreset({}, "identification", config=configured(shortlist_size=0))["shortlist_size"])
        # hunt turns it off whatever is configured
        self.assertEqual(0, applyMatchingPreset({}, "hunt", config=configured(shortlist_size=500))["shortlist_size"])

    def test_an_unknown_preset_is_refused(self):
        # anything a request can carry, a repeated parameter (a list) included
        unknown: List[Any] = ["fast", "", "Hunt!", None, ["hunt", "hunt"]]
        for preset in unknown:
            with self.subTest(preset=preset), self.assertRaisesRegex(MatchingParameterError, "preset must be one of hunt, identification"):
                applyMatchingPreset({}, preset)
        # the refusal quotes what the caller sent
        with self.assertRaisesRegex(MatchingParameterError, "not 'Fast'"):
            applyMatchingPreset({}, "Fast")

    def test_the_server_expands_it_into_the_knob_values(self):
        mcrit_config = configured(shortlist_size=10)
        defaults = getMatchingParams({}, mcrit_config)
        self.assertEqual(2, defaults["band_matches_required"])
        self.assertEqual({**defaults, "band_matches_required": 1, "shortlist_size": 10}, getMatchingParams({"preset": "identification"}, mcrit_config))
        self.assertEqual({**defaults, "band_matches_required": 1, "shortlist_size": 0}, getMatchingParams({"preset": "hunt"}, mcrit_config))
        self.assertEqual({**defaults, "band_matches_required": 1, "shortlist_size": 10}, getMatchingParams({"preset": " Identification "}, mcrit_config))
        unconfigured = configured()
        self.assertEqual(100, getMatchingParams({"preset": "identification"}, unconfigured)["shortlist_size"])
        explicit = getMatchingParams({"preset": "identification", "band_matches_required": "2", "shortlist_size": "25"}, mcrit_config)
        self.assertEqual((2, 25), (explicit["band_matches_required"], explicit["shortlist_size"]))
        for request in ({"preset": "fast"}, {"preset": ["hunt", "hunt"]}):
            with self.subTest(request=request), self.assertRaises(MatchingParameterError):
                getMatchingParams(request, mcrit_config)

    def test_matches_restricted_to_named_samples_take_the_rest_of_a_preset(self):
        mcrit_config = configured(shortlist_size=10)
        for request, with_shortlist in (({"preset": "identification"}, False), ({"preset": "identification", "sample_group_only": "true"}, True)):
            with self.subTest(request=request):
                parameters = getMatchingParams(request, mcrit_config, with_shortlist=with_shortlist)
                self.assertEqual(1, parameters["band_matches_required"])
                self.assertNotIn("shortlist_size", parameters)

    def test_a_preset_request_shares_its_job_with_the_explicit_one(self):
        """The preset is expanded before submission, so the job is keyed on the values it runs with."""
        index = MinHashIndex(config=configured())
        sample_id = index._storage.addSmdaReport(SmdaReport.fromFile("tests/example_report.smda")).sample_id
        app = falcon.App()
        app.add_route("/matches/sample/{sample_id:int}", MatchResource(index), suffix="sample")
        client = falcon.testing.TestClient(app)
        by_preset = client.simulate_get(f"/matches/sample/{sample_id}", query_string="preset=identification").json["data"]
        explicit = client.simulate_get(f"/matches/sample/{sample_id}", query_string="band_matches_required=1&shortlist_size=100").json["data"]
        self.assertEqual(by_preset, explicit)
        self.assertNotIn("preset", json.loads(index.getJobData(by_preset)["payload"]["params"]))
        self.assertNotEqual(by_preset, client.simulate_get(f"/matches/sample/{sample_id}", query_string="preset=hunt").json["data"])
        # a direct caller gets the same job
        self.assertEqual(by_preset, index.getMatchesForSample(sample_id, preset="identification"))

    def test_every_job_method_takes_a_preset_from_a_direct_caller(self):
        index = MinHashIndex(config=configured(shortlist_size=10))
        remote = MinHashIndex.__mro__[1]
        calls = {
            "getMatchesForSample": ((7,), True),
            "getMatchesForSmdaReport": (({},), True),
            "getMatchesForMappedBinary": ((b"MZ", 0x1000), True),
            "getMatchesForUnmappedBinary": ((b"MZ",), True),
            "getMatchesForSampleVs": ((7, 8), False),
            "getMatchesForSampleVsGroup": ((7, [8, 9]), False),
        }
        for name, (args, takes_shortlist) in calls.items():
            with self.subTest(name), patch.object(remote, name, create=True) as submit:
                getattr(index, name)(*args, preset="identification")
                kwargs = submit.call_args.kwargs
                self.assertNotIn("preset", kwargs)
                self.assertEqual(1, kwargs["band_matches_required"])
                self.assertEqual(10 if takes_shortlist else None, kwargs.get("shortlist_size"))
                with self.assertRaises(MatchingParameterError):
                    getattr(index, name)(*args, preset="fast")
                getattr(index, name)(*args, preset="Hunt")
                self.assertEqual(0 if takes_shortlist else None, submit.call_args.kwargs.get("shortlist_size"))
        with patch("mcrit.index.MinHashIndex.MatcherQueryFunction") as matcher:
            matcher.return_value.getMatchesForSmdaFunction.return_value = {"info": {"job": {}}}
            with patch("mcrit.index.MinHashIndex.SmdaReport.fromDict") as from_dict:
                from_dict.return_value = MagicMock(xcfg={0x1000: {}}, sha256="ab" * 32)
                index.getMatchesForSmdaFunction(MagicMock(), preset="identification", band_matches_required=2)
            self.assertEqual((2, 10), (matcher.call_args.kwargs["band_matches_required"], matcher.call_args.kwargs["shortlist_size"]))

    def test_a_cross_compare_hands_the_preset_to_its_children(self):
        """Each child applies it; a 1-vs-corpus child keeps the shortlist of 0 a cross compare forces."""
        for sample_group_only, child in ((False, "getMatchesForSample"), (True, "getMatchesForSampleVsGroup")):
            with self.subTest(sample_group_only=sample_group_only):
                index = MagicMock()
                MinHashIndex.getMatchesCross(index, [1, 2], sample_group_only=sample_group_only, preset="identification")
                kwargs = getattr(index, child).call_args.kwargs
                self.assertEqual("identification", kwargs["preset"])
                self.assertEqual(None if sample_group_only else 0, kwargs.get("shortlist_size"))
        # and the child, applying it, keeps that explicit 0
        self.assertEqual({"band_matches_required": 1, "shortlist_size": 0}, applyMatchingPreset({"shortlist_size": 0}, "identification"))

    def test_every_matching_route_refuses_an_unknown_preset_and_applies_a_known_one(self):
        index = MagicMock()
        index.config = configured(shortlist_size=10)
        index.isSampleId.return_value = True
        app = falcon.App()
        match_resource, query_resource = MatchResource(index), QueryResource(index)
        app.add_route("/matches/sample/{sample_id:int}", match_resource, suffix="sample")
        app.add_route("/matches/sample/{sample_id:int}/{sample_id_b:int}", match_resource, suffix="sample_vs")
        app.add_route("/matches/sample/cross/{sample_ids}", match_resource, suffix="sample_cross")
        app.add_route("/query", query_resource, suffix="query_smda")
        app.add_route("/query/function", query_resource, suffix="query_smda_function")
        client = falcon.testing.TestClient(app)
        requests = {
            "getMatchesForSample": (lambda query: client.simulate_get("/matches/sample/1", query_string=query), True),
            "getMatchesForSampleVs": (lambda query: client.simulate_get("/matches/sample/1/2", query_string=query), False),
            "getMatchesCross": (lambda query: client.simulate_get("/matches/sample/cross/1,2", query_string=query), False),
            "getMatchesForSmdaReport": (lambda query: client.simulate_post("/query", query_string=query, json={}), True),
            "getMatchesForSmdaFunction": (lambda query: client.simulate_post("/query/function", query_string=query, json={}), True),
        }
        for method, (request, takes_shortlist) in requests.items():
            with self.subTest(method):
                response = request("preset=fast")
                self.assertEqual(400, response.status_code)
                self.assertIn("preset must be one of", response.json["data"]["message"])
                getattr(index, method).assert_not_called()
                request("preset=identification")
                kwargs = getattr(index, method).call_args.kwargs
                self.assertNotIn("preset", kwargs)
                self.assertEqual(1, kwargs["band_matches_required"])
                self.assertEqual(10 if takes_shortlist else None, kwargs.get("shortlist_size"))

    def test_the_client_sends_it_only_when_given(self):
        client = McritClient("http://mcrit.test")
        with patch("mcrit.client.McritClient.requests.get") as get:
            get.return_value.status_code = 200
            get.return_value.json.return_value = {"status": "successful", "data": "0123456789abcdef01234567"}
            for call in (
                lambda preset: client.requestMatchesForSample(7, preset=preset),
                lambda preset: client.requestMatchesForSampleVs(7, 8, preset=preset),
                lambda preset: client.requestMatchesCross([7, 8], preset=preset),
            ):
                call("identification")
                self.assertEqual("identification", get.call_args.kwargs["params"]["preset"])
                call(None)
                self.assertNotIn("preset", get.call_args.kwargs["params"])
        with patch("mcrit.client.McritClient.requests.post") as post:
            post.return_value.status_code = 200
            post.return_value.json.return_value = {"status": "successful", "data": "0123456789abcdef01234567"}
            client.requestMatchesForSmdaReport(MagicMock(toDict=MagicMock(return_value={})), preset="hunt")
            self.assertEqual("hunt", post.call_args.kwargs["params"]["preset"])
            client.getMatchesForSmdaFunction(MagicMock(toDict=MagicMock(return_value={})), preset="hunt")
            self.assertEqual("hunt", post.call_args.kwargs["params"]["preset"])
            client.requestMatchesForUnmappedBinary(b"MZ", disassemble_locally=False, preset="hunt")
            self.assertEqual("hunt", post.call_args.kwargs["params"]["preset"])
            client.requestMatchesForMappedBinary(b"MZ", 0x1000, disassemble_locally=False, preset="hunt")
            self.assertEqual("hunt", post.call_args.kwargs["params"]["preset"])
        # disassembled locally, both hand it on to the report query
        with patch.object(McritClient, "requestMatchesForSmdaReport") as report_query, patch("mcrit.client.McritClient.Disassembler") as disassembler:
            disassembler.return_value.disassembleBuffer.return_value.status = "ok"
            disassembler.return_value.disassembleUnmappedBuffer.return_value.status = "ok"
            client.requestMatchesForMappedBinary(b"MZ", 0x1000, preset="identification")
            self.assertEqual("identification", report_query.call_args.kwargs["preset"])
            client.requestMatchesForUnmappedBinary(b"MZ", preset="identification")
            self.assertEqual("identification", report_query.call_args.kwargs["preset"])


if __name__ == "__main__":
    unittest.main()
