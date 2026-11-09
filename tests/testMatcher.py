#!/usr/bin/python

import json
import logging
import os
import random
import unittest
from copy import deepcopy
from types import SimpleNamespace
from typing import Any, Dict, List, cast
from unittest.mock import patch

from smda.common.SmdaReport import SmdaReport

from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.matchers.MatcherFlags import IS_LIBRARY_FLAG, IS_MINHASH_FLAG, IS_PICHASH_FLAG
from mcrit.matchers.MatcherInterface import MatcherInterface
from mcrit.matchers.MatcherQuery import MatcherQuery
from mcrit.matchers.MatcherSample import MatcherSample

# from mcrit.storage.MemoryStorage import MemoryStorage
from mcrit.matchers.MatcherVs import MatcherVs
from mcrit.matchers.MatcherVsGroup import MatcherVsGroup
from mcrit.storage.SampleEntry import SampleEntry

from .context import config

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logging.disable(logging.CRITICAL)


# MinHash values depend on how SMDA escapes instructions, so a new SMDA release can shift a
# signature element and with it a match score, without any change in mcrit (measured: smda
# 4.4.4 -> 4.4.5 moved exactly one of 64 elements on one fixture function, 65.625 -> 67.1875).
# Freezing scores here therefore pins the test suite to an SMDA version. Instead, mark such
# values MINHASH_SCORE and resolve them at assert time from the very minhashes that were
# stored, so the expectation tracks SMDA while still pinning what this suite is about:
# which functions match which, with what flags, and that the reported score is exactly the
# score of that pair. Everything not derived from a minhash stays a literal.
MINHASH_SCORE = "<score derived from the stored minhashes of the matched pair>"


def score_from_minhashes(own_minhash, foreign_minhash, minhash_bits):
    """Percentage of identical signature fields, computed independently of production code.

    Deliberately does not call MinHash.calculateMinHashScore: deriving the expectation from
    the very function under test would make this a mirror rather than a check.
    """
    field_width = 1 if minhash_bits <= 8 else minhash_bits // 8
    own_fields = [own_minhash[index : index + field_width] for index in range(0, len(own_minhash), field_width)]
    foreign_fields = [foreign_minhash[index : index + field_width] for index in range(0, len(foreign_minhash), field_width)]
    assert len(own_fields) == len(foreign_fields)
    matching_fields = sum(1 for own_field, foreign_field in zip(own_fields, foreign_fields) if own_field == foreign_field)
    return 100.0 * matching_fields / len(own_fields)


def resolve_expected_matches(expected_functions, storage, minhash_bits, extra_minhashes=None):
    """Replace MINHASH_SCORE placeholders with the score of the matched pair.

    `extra_minhashes` supplies minhashes the storage cannot answer for - query functions
    live on the matcher, not in the database.
    """
    extra_minhashes = extra_minhashes or {}

    def lookup(function_id):
        minhash = extra_minhashes.get(function_id) or storage.getMinHashByFunctionId(function_id)
        if not minhash:
            raise AssertionError("no minhash available for function %d, cannot derive expected score" % function_id)
        return minhash

    resolved = deepcopy(expected_functions)
    for function_data in resolved:
        own_minhash = lookup(function_data["fid"])
        rebuilt_matches = []
        for match in function_data["matches"]:
            family_id, sample_id, foreign_function_id, score, flags = match
            if score is MINHASH_SCORE:
                foreign_minhash = lookup(foreign_function_id)
                score = score_from_minhashes(own_minhash, foreign_minhash, minhash_bits)
            rebuilt_matches.append((family_id, sample_id, foreign_function_id, score, flags))
        function_data["matches"] = rebuilt_matches
    return resolved


# Byte/percent aggregates that weight matched bytes by their match score inherit the same
# SMDA sensitivity as the scores themselves (measured: one shifted signature element moved
# frequency_weighted by 16.36 of 4486.94 bytes, i.e. 0.36%). Compare those with a small
# relative tolerance and everything else - counts, unweighted bytes, ids, names - exactly,
# so a weighting regression (which would be a factor, not a fraction of a percent) still
# fails while SMDA drift does not.
SCORE_DERIVED_KEYS = {
    "score_weighted",
    "frequency_weighted",
    "nonlib_score_weighted",
    "nonlib_frequency_weighted",
}
SCORE_DERIVED_TOLERANCE = 0.02


def assert_sample_summaries_equal(test_case, actual_summaries, expected_summaries):
    test_case.assertEqual(len(actual_summaries), len(expected_summaries))
    for actual, expected in zip(actual_summaries, expected_summaries):
        test_case.assertEqual(set(actual), set(expected))
        for key, expected_value in expected.items():
            if key != "matched":
                test_case.assertEqual(actual[key], expected_value, "%s of sample %s" % (key, expected.get("sample_id")))
                continue
            # compare the nested key sets too, so a field added to or removed from a summary group
            # still fails here the way a whole-dict assertEqual would have
            test_case.assertEqual(set(actual["matched"]), set(expected_value), "matched groups of sample %s" % expected.get("sample_id"))
            for group, expected_group in expected_value.items():
                test_case.assertEqual(set(actual["matched"][group]), set(expected_group), "matched.%s fields of sample %s" % (group, expected.get("sample_id")))
                for field, expected_field in expected_group.items():
                    actual_field = actual["matched"][group][field]
                    if field in SCORE_DERIVED_KEYS:
                        test_case.assertAlmostEqual(
                            actual_field,
                            expected_field,
                            delta=max(abs(expected_field) * SCORE_DERIVED_TOLERANCE, 1e-9),
                            msg="matched.%s.%s of sample %s" % (group, field, expected.get("sample_id")),
                        )
                    else:
                        test_case.assertEqual(actual_field, expected_field, "matched.%s.%s of sample %s" % (group, field, expected.get("sample_id")))


class MatcherTestSuite(unittest.TestCase):
    """Run a full example on a memory dump"""

    # pichash_expected = {'pichash_summary': {'num_own_functions_matched': 0, 'num_foreign_functions_matched': 0, 'num_own_functions_matched_as_library': 0, 'num_self_matches': 0, 'bytes_matched': 0}, 'pichash_matches': {}}

    def __init__(self, *args, **kwargs):
        super(MatcherTestSuite, self).__init__(*args, **kwargs)

        THIS_FILE_PATH = str(os.path.abspath(__file__))
        PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
        example_file_path_1 = os.sep.join([PROJECT_ROOT, "tests", "example_report.smda"])
        example_file_path_2 = os.sep.join([PROJECT_ROOT, "tests", "example_report_2.smda"])
        example_file_path_3 = os.sep.join([PROJECT_ROOT, "tests", "example_report_3.smda"])
        library_file_path = os.sep.join([PROJECT_ROOT, "tests", "library_report.smda"])
        self.smda_report_1 = SmdaReport.fromFile(example_file_path_1)
        assert self.smda_report_1 is not None
        self.smda_report_2 = SmdaReport.fromFile(example_file_path_2)
        assert self.smda_report_2 is not None
        self.smda_report_2.family = "test_family"
        self.smda_report_3 = SmdaReport.fromFile(example_file_path_3)
        assert self.smda_report_3 is not None
        self.smda_report_3.family = "test_family_b"
        self.library_report = SmdaReport.fromFile(library_file_path)
        assert self.library_report is not None

        # make a selfmatch (mutate xcfg *before* any getFunctions() call, otherwise the
        # cached function list built by SmdaReport.getFunctions() would hide the mutation)
        _funcs_1 = sorted(self.smda_report_1.xcfg.values(), key=lambda f: f.offset)
        self.smda_report_1.xcfg[_funcs_1[1].offset] = _funcs_1[5]

        function_1_selected = list(self.smda_report_1.getFunctions())[3]
        function_2_selected = list(self.smda_report_2.getFunctions())[5]
        # self.smda_report_2.xcfg[offset] = function_1_selected
        function_2_selected.pic_hash = function_1_selected.pic_hash

    # from matchervs
    function_matches_expected: List[Dict[str, Any]] = [
        {
            "fid": 9,
            "matches": [
                (0, 2, 19, 100.0, IS_MINHASH_FLAG + IS_PICHASH_FLAG + IS_LIBRARY_FLAG),
                (1, 0, 0, MINHASH_SCORE, IS_MINHASH_FLAG),
                (2, 3, 22, MINHASH_SCORE, IS_MINHASH_FLAG),
            ],
            "num_bytes": 354.0,
            "num_instructions": 120,
            "offset": 0,
        },
        {"num_bytes": 35.0, "num_instructions": 11, "offset": 2220, "matches": [], "fid": 10},
        {
            "fid": 11,
            "matches": [
                (0, 2, 20, 100.0, IS_MINHASH_FLAG + IS_PICHASH_FLAG + IS_LIBRARY_FLAG),
                (1, 0, 1, MINHASH_SCORE, IS_MINHASH_FLAG),
            ],
            "num_bytes": 638.0,
            "num_instructions": 207,
            "offset": 364,
        },
        {
            "fid": 12,
            "matches": [(0, 2, 21, 100.0, IS_MINHASH_FLAG + IS_PICHASH_FLAG + IS_LIBRARY_FLAG), (1, 0, 5, 100.0, IS_MINHASH_FLAG + IS_PICHASH_FLAG)],
            "num_bytes": 166.0,
            "num_instructions": 64,
            "offset": 1004,
        },
        {
            "fid": 13,
            "matches": [(1, 0, 3, MINHASH_SCORE, IS_MINHASH_FLAG)],
            "num_bytes": 1047.0,
            "num_instructions": 365,
            "offset": 1172,
        },
        {"num_bytes": 35.0, "num_instructions": 11, "offset": 2220, "matches": [], "fid": 14},
        {"num_bytes": 524.0, "num_instructions": 159, "offset": 2256, "matches": [], "fid": 15},
        {
            "fid": 16,
            "matches": [(1, 0, 5, MINHASH_SCORE, IS_MINHASH_FLAG)],
            "num_bytes": 915.0,
            "num_instructions": 287,
            "offset": 2780,
        },
        {
            "fid": 17,
            "matches": [(1, 0, 6, MINHASH_SCORE, IS_MINHASH_FLAG)],
            "num_bytes": 727.0,
            "num_instructions": 226,
            "offset": 3696,
        },
        {
            "fid": 18,
            "matches": [(1, 0, 7, MINHASH_SCORE, IS_MINHASH_FLAG)],
            "num_bytes": 1850.0,
            "num_instructions": 543,
            "offset": 4424,
        },
    ]

    function_matches_expected_vs: List[Dict[str, Any]] = [
        {
            "fid": 9,
            "matches": [
                (1, 0, 0, MINHASH_SCORE, IS_MINHASH_FLAG),
            ],
            "num_bytes": 354.0,
            "num_instructions": 120,
            "offset": 0,
        },
        {"num_bytes": 35.0, "num_instructions": 11, "offset": 2220, "matches": [], "fid": 10},
        {
            "fid": 11,
            "matches": [
                (1, 0, 1, MINHASH_SCORE, IS_MINHASH_FLAG),
            ],
            "num_bytes": 638.0,
            "num_instructions": 207,
            "offset": 364,
        },
        {
            "fid": 12,
            "matches": [
                (1, 0, 5, 100.0, IS_MINHASH_FLAG + IS_PICHASH_FLAG),
            ],
            "num_bytes": 166.0,
            "num_instructions": 64,
            "offset": 1004,
        },
        {
            "fid": 13,
            "matches": [(1, 0, 3, MINHASH_SCORE, IS_MINHASH_FLAG)],
            "num_bytes": 1047.0,
            "num_instructions": 365,
            "offset": 1172,
        },
        {"num_bytes": 35.0, "num_instructions": 11, "offset": 2220, "matches": [], "fid": 14},
        {"num_bytes": 524.0, "num_instructions": 159, "offset": 2256, "matches": [], "fid": 15},
        {
            "fid": 16,
            "matches": [(1, 0, 5, MINHASH_SCORE, IS_MINHASH_FLAG)],
            "num_bytes": 915.0,
            "num_instructions": 287,
            "offset": 2780,
        },
        {
            "fid": 17,
            "matches": [(1, 0, 6, MINHASH_SCORE, IS_MINHASH_FLAG)],
            "num_bytes": 727.0,
            "num_instructions": 226,
            "offset": 3696,
        },
        {
            "fid": 18,
            "matches": [(1, 0, 7, MINHASH_SCORE, IS_MINHASH_FLAG)],
            "num_bytes": 1850.0,
            "num_instructions": 543,
            "offset": 4424,
        },
    ]

    minhash_aggregation_expected = {
        "num_own_functions_matched": 7,
        "num_foreign_functions_matched": 10,
        "num_own_functions_matched_as_library": 3,
        "num_self_matches": 2,
        "bytes_matched": 5697.0,
    }

    minhash_aggregation_expected_vs = {
        "num_own_functions_matched": 7,
        "num_foreign_functions_matched": 6,
        "num_own_functions_matched_as_library": 0,
        "num_self_matches": 2,
        "bytes_matched": 5697.0,
    }

    minhash_aggregation_expected_query = deepcopy(minhash_aggregation_expected)
    minhash_aggregation_expected_query["num_self_matches"] = 0

    pichash_aggregation_expected = {
        "num_own_functions_matched": 3,
        "num_foreign_functions_matched": 4,
        "num_own_functions_matched_as_library": 3,
        "num_self_matches": 2,
        "bytes_matched": 1158.0,
    }

    pichash_aggregation_expected_query = deepcopy(pichash_aggregation_expected)
    pichash_aggregation_expected_query["num_self_matches"] = 0

    pichash_aggregation_expected_vs = {
        "num_own_functions_matched": 1,
        "num_foreign_functions_matched": 1,
        "num_own_functions_matched_as_library": 0,
        "num_self_matches": 2,
        "bytes_matched": 166.0,
    }
    maxDiff = None
    # sample2
    # TODO mark s.th as library,
    # TODO make s.th a pic+min match
    sample_summary_entry_2_expected = {
        "sample_id": 0,
        "family": "test_family",
        "family_id": 1,
        "version": "",
        "bitness": 32,
        "sha256": "39401c7f9518f9710bfd6d64e13a5bf4efdec42e54c9de363b4871e8f374b579",
        "filename": "",
        "num_bytes": 6158.0,
        "num_functions": 9,
        "is_library": False,
        "matched": {
            "functions": {
                "combined": 7,
                "library": 3,
                "minhashes": 7,
                "pichashes": 1,
            },
            "bytes": {
                "unweighted": 5697.0,
                "score_weighted": 4486.9375,
                "frequency_weighted": 4337.59375,
                "nonlib_unweighted": 4539.0,
                "nonlib_score_weighted": 3434.09375,
                "nonlib_frequency_weighted": 3434.09375,
            },
            "percent": {
                "unweighted": 90.55793991416309,
                "score_weighted": 71.32312033063106,
                "frequency_weighted": 68.94919329200445,
                "nonlib_unweighted": 88.42781998831093,
                "nonlib_score_weighted": 66.90227449834404,
                "nonlib_frequency_weighted": 66.90227449834404,
            },
        },
    }

    sample_summary_entry_3_expected = {
        "sample_id": 3,
        "family": "test_family_b",
        "family_id": 2,
        "version": "",
        "bitness": 32,
        "sha256": "39401c7f9518f9710bfd6d64e13a5bf4efdec42e54c9de363b4871e8f374b57f",
        "filename": "",
        "num_bytes": 351.0,
        "num_functions": 9,
        "is_library": False,
        "matched": {
            "functions": {
                "combined": 1,
                "library": 1,
                "minhashes": 1,
                "pichashes": 0,
            },
            "bytes": {
                "unweighted": 354.0,
                "score_weighted": 298.6875,
                "frequency_weighted": 149.34375,
                "nonlib_unweighted": 0,
                "nonlib_score_weighted": 0,
                "nonlib_frequency_weighted": 0,
            },
            "percent": {
                "unweighted": 5.627086313781593,
                "score_weighted": 4.747854077253219,
                "frequency_weighted": 2.3739270386266096,
                "nonlib_unweighted": 0.0,
                "nonlib_score_weighted": 0.0,
                "nonlib_frequency_weighted": 0.0,
            },
        },
    }

    sample_summary_entry_expected_vs = {
        "sample_id": 0,
        "family": "test_family",
        "family_id": 1,
        "version": "",
        "bitness": 32,
        "sha256": "39401c7f9518f9710bfd6d64e13a5bf4efdec42e54c9de363b4871e8f374b579",
        "filename": "",
        "num_bytes": 6158.0,
        "num_functions": 9,
        "is_library": False,
        "matched": {
            "functions": {
                "combined": 7,
                "library": 0,
                "minhashes": 7,
                "pichashes": 1,
            },
            "bytes": {
                "unweighted": 5697.0,
                "score_weighted": 4486.9375,
                "frequency_weighted": 4486.9375,
                "nonlib_unweighted": 5697.0,
                "nonlib_score_weighted": 4486.9375,
                "nonlib_frequency_weighted": 4486.9375,
            },
            "percent": {
                "unweighted": 90.55793991416309,
                "score_weighted": 71.32312033063106,
                "frequency_weighted": 71.32312033063106,
                "nonlib_unweighted": 90.55793991416309,
                "nonlib_score_weighted": 71.32312033063106,
                "nonlib_frequency_weighted": 71.32312033063106,
            },
        },
    }

    sample_summary_lib_entry_expected = {
        "sample_id": 2,
        "family": "",
        "family_id": 0,
        "version": "",
        "bitness": 32,
        "sha256": "ae38ff0778fb8dfa1deb17301a15165934312648d232d167cd0c0034c24689e2",
        "filename": "",
        "num_bytes": 1158.0,
        "num_functions": 3,
        "is_library": True,
        "matched": {
            "functions": {
                "combined": 3,
                "library": 3,
                "minhashes": 3,
                "pichashes": 3,
            },
            "bytes": {
                "unweighted": 1158.0,
                "score_weighted": 1158.0,
                "frequency_weighted": 981.0,
                "nonlib_unweighted": 0,
                "nonlib_score_weighted": 0,
                "nonlib_frequency_weighted": 0,
            },
            "percent": {
                "unweighted": 18.407248450166904,
                "score_weighted": 18.407248450166904,
                "frequency_weighted": 15.593705293276109,
                "nonlib_unweighted": 0.0,
                "nonlib_score_weighted": 0.0,
                "nonlib_frequency_weighted": 0.0,
            },
        },
    }

    def testMatcherVs(self):
        index = MinHashIndex(config=config)
        worker = index.queue._worker

        sampleEntry2 = index._storage.addSmdaReport(self.smda_report_2)
        id2 = sampleEntry2.sample_id

        sampleEntry1 = index._storage.addSmdaReport(self.smda_report_1)
        id1 = sampleEntry1.sample_id

        libraryEntry = index._storage.addSmdaReport(self.library_report)
        id_lib = libraryEntry.sample_id

        sampleEntry3 = index._storage.addSmdaReport(self.smda_report_3)
        id3 = sampleEntry3.sample_id

        worker.updateMinHashesForSample(id1)
        worker.updateMinHashesForSample(id2)
        worker.updateMinHashesForSample(id_lib)
        worker.updateMinHashesForSample(id3)

        matcher = MatcherVs(worker)
        result = matcher.getMatchesForSample(id1, id2)

        self.assertEqual(result["info"]["sample"], sampleEntry1.toDict())
        self.assertEqual(result["other_sample_info"], sampleEntry2.toDict())
        self.assertTrue("duration" in result["info"]["job"])
        self.assertTrue("timestamp" in result["info"]["job"])
        self.assertTrue(isinstance(result["info"]["job"]["timestamp"], str))
        self.assertTrue(len(result["info"]["job"]["timestamp"]) > 0)
        self.assertNotEqual(result["info"]["job"]["duration"], 0)

        self.assertEqual(result["matches"]["aggregation"]["pichash"], self.pichash_aggregation_expected_vs)
        self.assertEqual(result["matches"]["aggregation"]["minhash"], self.minhash_aggregation_expected_vs)
        self.assertEqual(
            result["matches"]["functions"],
            resolve_expected_matches(self.function_matches_expected_vs, index._storage, config.MINHASH_CONFIG.MINHASH_SIGNATURE_BITS),
        )
        assert_sample_summaries_equal(self, result["matches"]["samples"], [self.sample_summary_entry_expected_vs])

    def testMatcherVsGroup(self):
        index = MinHashIndex(config=config)
        worker = index.queue._worker

        sampleEntry2 = index._storage.addSmdaReport(self.smda_report_2)
        id2 = sampleEntry2.sample_id

        sampleEntry1 = index._storage.addSmdaReport(self.smda_report_1)
        id1 = sampleEntry1.sample_id

        index._storage.addSmdaReport(self.library_report)

        sampleEntry3 = index._storage.addSmdaReport(self.smda_report_3)
        id3 = sampleEntry3.sample_id

        worker.updateMinHashesForSample(id1)
        worker.updateMinHashesForSample(id2)
        worker.updateMinHashesForSample(id3)

        matcher = MatcherVsGroup(worker)
        result = matcher.getMatchesForSample(id1, [id2, id3])

        print(json.dumps(result, indent=1))

        self.assertEqual(result["info"]["sample"], sampleEntry1.toDict())
        self.assertEqual(result["other_sample_infos"], [sampleEntry2.toDict(), sampleEntry3.toDict()])
        self.assertTrue("duration" in result["info"]["job"])
        self.assertTrue("timestamp" in result["info"]["job"])
        self.assertTrue(isinstance(result["info"]["job"]["timestamp"], str))
        self.assertTrue(len(result["info"]["job"]["timestamp"]) > 0)
        self.assertNotEqual(result["info"]["job"]["duration"], 0)
        self.assertTrue("matcher_vs_group" in result["info"]["type"])

        self.assertEqual(result["matches"]["aggregation"]["pichash"], self.pichash_aggregation_expected_vs)
        minhash_aggregation_expected_vs_group = deepcopy(self.minhash_aggregation_expected_vs)
        minhash_aggregation_expected_vs_group["num_foreign_functions_matched"] = 7
        self.assertEqual(result["matches"]["aggregation"]["minhash"], minhash_aggregation_expected_vs_group)
        function_matches_expected_vs_group = []
        for function_data in self.function_matches_expected_vs:
            if function_data["fid"] == 9:
                matches = function_data["matches"]
                matches.append((2, 3, 22, MINHASH_SCORE, 1))
                function_data["matches"] = matches
                function_matches_expected_vs_group.append(function_data)
            else:
                function_matches_expected_vs_group.append(function_data)
        self.assertEqual(
            result["matches"]["functions"],
            resolve_expected_matches(function_matches_expected_vs_group, index._storage, config.MINHASH_CONFIG.MINHASH_SIGNATURE_BITS),
        )
        sample_3_match_result = {
            "family": "test_family_b",
            "family_id": 2,
            "version": "",
            "bitness": 32,
            "sha256": "39401c7f9518f9710bfd6d64e13a5bf4efdec42e54c9de363b4871e8f374b57f",
            "filename": "",
            "sample_id": 3,
            "num_bytes": 351.0,
            "is_library": False,
            "num_functions": 9,
            "matched": {
                "functions": {"minhashes": 1, "pichashes": 0, "combined": 1, "library": 0},
                "bytes": {
                    "unweighted": 354.0,
                    "score_weighted": 298.6875,
                    "frequency_weighted": 298.6875,
                    "nonlib_unweighted": 354.0,
                    "nonlib_score_weighted": 298.6875,
                    "nonlib_frequency_weighted": 298.6875,
                },
                "percent": {
                    "unweighted": 5.627086313781593,
                    "score_weighted": 4.747854077253219,
                    "frequency_weighted": 4.747854077253219,
                    "nonlib_unweighted": 5.627086313781593,
                    "nonlib_score_weighted": 4.747854077253219,
                    "nonlib_frequency_weighted": 4.747854077253219,
                },
            },
        }
        sample_summary_entry_expected_vs_group = [deepcopy(self.sample_summary_entry_expected_vs), sample_3_match_result]
        assert_sample_summaries_equal(self, result["matches"]["samples"], sample_summary_entry_expected_vs_group)

    def testMatcherSample(self):
        index = MinHashIndex(config=config)
        worker = index.queue._worker

        sampleEntry2 = index._storage.addSmdaReport(self.smda_report_2)
        id2 = sampleEntry2.sample_id

        sampleEntry1 = index._storage.addSmdaReport(self.smda_report_1)
        id1 = sampleEntry1.sample_id

        libraryEntry = index._storage.addSmdaReport(self.library_report)
        id_lib = libraryEntry.sample_id

        sampleEntry3 = index._storage.addSmdaReport(self.smda_report_3)
        id3 = sampleEntry3.sample_id

        # a=set([fe.pichash for fe in function_entries_1])
        # b=set([fe.pichash for fe in function_entries_2])
        # print(set.intersection(a,b))

        worker.updateMinHashesForSample(id1)
        worker.updateMinHashesForSample(id2)
        worker.updateMinHashesForSample(id3)
        worker.updateMinHashesForSample(id_lib)

        matcher = MatcherSample(worker)
        result = matcher.getMatchesForSample(id1)

        print(json.dumps(result, indent=4))

        self.assertEqual(result["info"]["sample"], sampleEntry1.toDict())
        self.assertTrue("duration" in result["info"]["job"])
        self.assertNotEqual(result["info"]["job"]["duration"], 0)
        self.assertTrue("timestamp" in result["info"]["job"])
        self.assertTrue(isinstance(result["info"]["job"]["timestamp"], str))
        self.assertTrue(len(result["info"]["job"]["timestamp"]) > 0)

        self.assertEqual(result["matches"]["aggregation"]["minhash"], self.minhash_aggregation_expected)
        self.assertEqual(result["matches"]["aggregation"]["pichash"], self.pichash_aggregation_expected)
        self.assertEqual(
            result["matches"]["functions"],
            resolve_expected_matches(self.function_matches_expected, index._storage, config.MINHASH_CONFIG.MINHASH_SIGNATURE_BITS),
        )
        self.maxDiff = None
        assert_sample_summaries_equal(
            self,
            result["matches"]["samples"],
            [
                self.sample_summary_lib_entry_expected,
                self.sample_summary_entry_2_expected,
                self.sample_summary_entry_3_expected,
            ],
        )

    def testPercentagesUseTheRequestsOwnThresholds(self):
        # #156: with pichash_size below MINHASH_FN_MIN_INS the small functions match by
        # PicHash but used to be missing from the denominator, so a sample matched against
        # an identical copy of itself scored above 100 %
        index = MinHashIndex(config=config)
        worker = index.queue._worker
        # the pristine report: the suite's copy has its 2-instruction function swapped out
        # for a self match, and that tiny function is exactly what this test is about
        this_file_path = str(os.path.abspath(__file__))
        report = SmdaReport.fromFile(os.sep.join([os.path.dirname(this_file_path), "example_report.smda"]))
        assert report is not None
        entry_a = index._storage.addSmdaReport(report)
        twin = SmdaReport.fromDict(report.toDict())
        assert twin is not None
        twin.sha256 = "ff" * 32
        twin.family = "twin_family"
        entry_b = index._storage.addSmdaReport(twin)
        worker.updateMinHashesForSample(entry_a.sample_id)
        worker.updateMinHashesForSample(entry_b.sample_id)
        small_functions = [f for f in index._storage.getFunctionsBySampleId(entry_a.sample_id) if f.num_instructions < config.MINHASH_CONFIG.MINHASH_FN_MIN_INS]
        self.assertGreater(len(small_functions), 0, "the fixture needs functions below MINHASH_FN_MIN_INS to be meaningful")
        for pichash_size in (1, config.MINHASH_CONFIG.PICHASH_SIZE):
            with self.subTest(pichash_size=pichash_size):
                matcher = MatcherVs(worker, pichash_size=pichash_size)
                result = matcher.getMatchesForSample(entry_a.sample_id, entry_b.sample_id)
                summary = [s for s in result["matches"]["samples"] if s["sample_id"] == entry_b.sample_id][0]
                matchable = [f for f in index._storage.getFunctionsBySampleId(entry_a.sample_id) if f.num_instructions >= pichash_size]
                expected_bytes = sum(f.binweight for f in matchable)
                self.assertEqual(expected_bytes, summary["matched"]["bytes"]["unweighted"])
                self.assertAlmostEqual(100.0, summary["matched"]["percent"]["unweighted"])
                for kind, value in summary["matched"]["percent"].items():
                    self.assertLessEqual(value, 100.0 + 1e-9, kind)

    def testMatcherQuery(self):
        index = MinHashIndex(config=config)
        worker = index.queue._worker

        # sampleEntry1 = index._storage.addSmdaReport(self.smda_report_1)
        # id1 = sampleEntry1.sample_id
        sampleEntry2 = index._storage.addSmdaReport(self.smda_report_2)
        id2 = sampleEntry2.sample_id

        sampleEntry1 = index._storage.addSmdaReport(self.smda_report_1)
        id1 = sampleEntry1.sample_id
        index._storage.deleteSample(id1)
        # TODO check if storage.deleteSample is broken

        libraryEntry = index._storage.addSmdaReport(self.library_report)
        id_lib = libraryEntry.sample_id

        sampleEntry3 = index._storage.addSmdaReport(self.smda_report_3)
        id3 = sampleEntry3.sample_id

        # function_entries_1 = index._storage.getFunctionsBySampleId(id1)
        # index.updateMinHashes(function_entries=function_entries_1)

        function_entries_2 = index._storage.getFunctionsBySampleId(id2)
        assert function_entries_2
        worker.updateMinHashesForSample(id2)
        worker.updateMinHashesForSample(id_lib)
        worker.updateMinHashesForSample(id3)

        matcher = MatcherQuery(worker)
        result = matcher.getMatchesForSmdaReport(self.smda_report_1)

        function_matches_expected = deepcopy(self.function_matches_expected)

        for function_data in function_matches_expected:
            own_function_id = function_data["fid"]
            function_data["fid"] = len(function_entries_2) - own_function_id - 1

        sample_info_expected = SampleEntry(self.smda_report_1, sample_id=-1).toDict()

        self.assertEqual(result["info"]["sample"], sample_info_expected)
        self.assertTrue("duration" in result["info"]["job"])
        self.assertNotEqual(result["info"]["job"]["duration"], 0)
        self.assertTrue("timestamp" in result["info"]["job"])
        self.assertTrue(isinstance(result["info"]["job"]["timestamp"], str))
        self.assertTrue(len(result["info"]["job"]["timestamp"]) > 0)

        self.assertEqual(result["matches"]["aggregation"]["pichash"], self.pichash_aggregation_expected_query)
        self.assertEqual(result["matches"]["aggregation"]["minhash"], self.minhash_aggregation_expected_query)
        json.dumps(sorted(result["matches"]["functions"], key=lambda x: x["fid"]), indent=1)
        self.assertEqual(
            sorted(result["matches"]["functions"], key=lambda x: x["fid"]),
            sorted(
                resolve_expected_matches(
                    function_matches_expected,
                    index._storage,
                    config.MINHASH_CONFIG.MINHASH_SIGNATURE_BITS,
                    extra_minhashes={entry.function_id: entry.minhash for entry in matcher._function_entries},
                ),
                key=lambda x: x["fid"],
            ),
        )
        assert_sample_summaries_equal(
            self,
            result["matches"]["samples"],
            [
                self.sample_summary_lib_entry_expected,
                self.sample_summary_entry_2_expected,
                self.sample_summary_entry_3_expected,
            ],
        )


class PairBudgetBatchingTestSuite(unittest.TestCase):
    """MINHASH_MATCHING_MAX_PAIRS packs batches by candidate volume; results must be
    invariant to how query functions are partitioned into batches."""

    def _matchWith(self, max_pairs, batch_size):
        from .context import config as base_config

        run_config = deepcopy(base_config)
        run_config.MINHASH_CONFIG.MINHASH_MATCHING_MAX_PAIRS = max_pairs
        run_config.MINHASH_CONFIG.MINHASH_MATCHING_FUNCTION_BATCH_SIZE = batch_size
        index = MinHashIndex(config=run_config)
        worker = index.queue._worker

        THIS_FILE_PATH = str(os.path.abspath(__file__))
        PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
        report_a = SmdaReport.fromFile(os.sep.join([PROJECT_ROOT, "tests", "example_report.smda"]))
        report_b = SmdaReport.fromFile(os.sep.join([PROJECT_ROOT, "tests", "example_report_2.smda"]))
        report_b.family = "test_family"
        entry_b = index._storage.addSmdaReport(report_b)
        entry_a = index._storage.addSmdaReport(report_a)
        worker.updateMinHashesForSample(entry_a.sample_id)
        worker.updateMinHashesForSample(entry_b.sample_id)
        matcher = MatcherSample(worker)
        return matcher.getMatchesForSample(entry_a.sample_id)["matches"]

    def testPartitionInvariance(self):
        # legacy fixed batches, one batch total
        legacy = self._matchWith(max_pairs=0, batch_size=10000)
        # legacy, many small batches
        legacy_small = self._matchWith(max_pairs=0, batch_size=3)
        # pair budget so small every candidate group flushes alone
        budget_min = self._matchWith(max_pairs=1, batch_size=10000)
        # shipped default budget
        budget_default = self._matchWith(max_pairs=50000000, batch_size=10000)
        self.assertEqual(legacy, legacy_small)
        self.assertEqual(legacy, budget_min)
        self.assertEqual(legacy, budget_default)

    def testFunctionBatchSizeRemainsACeiling(self):
        # a lowered MINHASH_MATCHING_FUNCTION_BATCH_SIZE must keep bounding how many query functions
        # share a batch, or hosts sized per docs/TUNING.md would silently get batches packed to the
        # (much larger) global pair budget instead
        candidates_per_function = 100
        num_functions = 5000

        class StubReporter:
            def set_total(self, total):
                pass

            def step(self):
                pass

        class StubMatcher:
            def __init__(self, batch_size, max_pairs):
                minhash_config = deepcopy(config.MINHASH_CONFIG)
                minhash_config.MINHASH_MATCHING_FUNCTION_BATCH_SIZE = batch_size
                minhash_config.MINHASH_MATCHING_MAX_PAIRS = max_pairs
                self._worker = SimpleNamespace(config=SimpleNamespace(MINHASH_CONFIG=minhash_config))
                self._function_entries = [None] * num_functions
                self._progress_reporter = StubReporter()
                self._num_batches = None

            def _createMinHashCandidateGroups(self, start, end):
                end = min(end, num_functions)
                return {function_id: set(range(function_id * candidates_per_function, (function_id + 1) * candidates_per_function)) for function_id in range(start, end)}

        for batch_size in (200, 500, 10000):
            stub = StubMatcher(batch_size, max_pairs=50000000)
            batches = [len(groups) for groups in MatcherInterface._iterCandidateGroupBatches(cast(MatcherInterface, stub))]
            self.assertEqual(num_functions, sum(batches))
            self.assertLessEqual(max(batches), batch_size, f"batch size {batch_size} stopped bounding the batch")


class PicHashCopiesTestSuite(unittest.TestCase):
    """Copies of one function share a PicHash. Pairing each own copy with each copy of a sample made
    m copies matched against n copies in one sample m*n entries, and m*(m-1) self matches -
    gigabytes for a few thousand copies in one report."""

    OWN_SAMPLE_ID = 7
    BODY = [
        ("b801000000", "mov", "eax, 1"),
        ("bb02000000", "mov", "ebx, 2"),
        ("b903000000", "mov", "ecx, 3"),
        ("ba04000000", "mov", "edx, 4"),
        ("01d8", "add", "eax, ebx"),
        ("01c8", "add", "eax, ecx"),
        ("01d0", "add", "eax, edx"),
        ("31db", "xor", "ebx, ebx"),
        ("31c9", "xor", "ecx, ecx"),
        ("31d2", "xor", "edx, edx"),
        ("40", "inc", "eax"),
        ("c3", "ret", ""),
    ]

    def _harmonize(self, own_function_ids, foreign_holders=(), pichash_size=10):
        own_entries = [SimpleNamespace(function_id=function_id, num_instructions=12) for function_id in own_function_ids]
        stub = SimpleNamespace(_function_entries=own_entries, _pichash_size=pichash_size)
        group = {(1, self.OWN_SAMPLE_ID, function_id) for function_id in own_function_ids}
        group.update((2, sample_id, function_id) for sample_id, function_id in foreign_holders)
        return MatcherInterface._harmonizePicHashMatches(cast(MatcherInterface, stub), {0x1234: group})

    def _inSample(self, matches, sample_id):
        return [match_ids for match_ids in matches if match_ids[1] == sample_id]

    def testCopiesArePairedLinearly(self):
        own_function_ids = list(range(100, 1100))
        holders = {8: list(range(5000, 6000)), 9: [6000, 6001, 6002], 10: [7000]}
        matches = self._harmonize(own_function_ids, [(sample_id, function_id) for sample_id, function_ids in holders.items() for function_id in function_ids])
        self.assertTrue(all(strength == (100, True, False) for strength in matches.values()))
        for sample_id, function_ids in [*holders.items(), (self.OWN_SAMPLE_ID, own_function_ids)]:
            pairs = self._inSample(matches, sample_id)
            # every own copy matches every sample holding the code, and every copy there is matched
            self.assertEqual(set(own_function_ids), {match_ids[0] for match_ids in pairs})
            self.assertEqual(set(function_ids), {match_ids[2] for match_ids in pairs})
            if sample_id != self.OWN_SAMPLE_ID:
                # each own copy with the first copy of the sample, as MinHash matching would pick it
                self.assertEqual(len(own_function_ids) + len(function_ids) - 1, len(pairs))
                self.assertTrue(all((own_function_id, sample_id, function_ids[0]) in matches for own_function_id in own_function_ids))
        self_matches = self._inSample(matches, self.OWN_SAMPLE_ID)
        self.assertEqual(len(own_function_ids), len(self_matches))
        self.assertTrue(all(match_ids[0] != match_ids[2] for match_ids in self_matches))

    def testSingleCopiesKeepEveryPair(self):
        # one own copy matches every copy a sample holds, and every own copy matches a single one
        self.assertEqual({(100, 8, 5000), (100, 8, 5001), (100, 8, 5002)}, set(self._harmonize([100], [(8, 5000), (8, 5001), (8, 5002)])))
        self.assertEqual({(100, 8, 5000), (101, 8, 5000)}, set(self._inSample(self._harmonize([100, 101], [(8, 5000)]), 8)))
        self.assertEqual({(100, self.OWN_SAMPLE_ID, 101), (101, self.OWN_SAMPLE_ID, 100)}, set(self._harmonize([100, 101])))
        # a PicHash held by no own function matches nothing, even at pichash_size 0
        self.assertEqual({}, self._harmonize([], [(8, 5000), (8, 5001)], pichash_size=0))

    def _report(self, name, copies):
        xcfg = {}
        for index in range(copies):
            offset = 0x401000 + 0x40 * index
            instructions, cursor = [], offset
            for hexbytes, mnemonic, operands in self.BODY:
                instructions.append([cursor, hexbytes, mnemonic, operands])
                cursor += len(hexbytes) // 2
            metadata = {"binweight": cursor - offset, "characteristics": None, "confidence": None, "function_name": "", "strongly_connected_components": [[offset]], "tfidf": None}
            xcfg[str(offset)] = {"offset": offset, "blocks": {str(offset): instructions}, "apirefs": {}, "blockrefs": {}, "inrefs": [], "outrefs": {}, "metadata": metadata}
        statistics = ["num_api_calls", "num_basic_blocks", "num_failed_functions", "num_failed_instructions", "num_function_calls", "num_functions", "num_instructions"]
        statistics += ["num_leaf_functions", "num_recursive_functions"]
        return SmdaReport.fromDict(
            {
                "architecture": "intel",
                "base_addr": 0x400000,
                "binary_size": 0x10000,
                "bitness": 32,
                "code_areas": [],
                "confidence_threshold": 0.0,
                "disassembly_errors": {},
                "execution_time": 0.0,
                "identified_alignment": 0,
                "message": "Analysis finished regularly.",
                "metadata": {"family": name, "filename": name, "version": "", "is_library": False, "binweight": 0},
                "sha256": name * 64,
                "smda_version": "1.0.0",
                "statistics": dict.fromkeys(statistics, 0),
                "status": "ok",
                "timestamp": "2026-01-01T00-00-00",
                "xcfg": xcfg,
            }
        )

    def testReportCountsAreThoseOfEveryPair(self):
        index = MinHashIndex(config=deepcopy(config))
        worker = index.queue._worker
        sample_a = index._storage.addSmdaReport(self._report("a", 30)).sample_id
        sample_b = index._storage.addSmdaReport(self._report("b", 20)).sample_id
        for sample_id in (sample_a, sample_b):
            worker.updateMinHashesForSample(sample_id)
        matches = MatcherSample(worker).getMatchesForSample(sample_a)["matches"]
        # every copy in a matches the first copy in b, the other copies in b are spread over them: 49 matches
        # listed where each with each listed 600 - and the counts stay those of every pair
        matched = [[function_match[2] for function_match in function["matches"]] for function in matches["functions"]]
        self.assertEqual(49, sum(len(function_ids) for function_ids in matched))
        self.assertEqual({min(matched[0])}, {function_ids[0] for function_ids in matched})
        self.assertEqual(20, len({function_id for function_ids in matched for function_id in function_ids}))
        for aggregation in matches["aggregation"].values():
            self.assertEqual((30, 20, 30), (aggregation["num_own_functions_matched"], aggregation["num_foreign_functions_matched"], aggregation["num_self_matches"]))
        [sample_summary] = matches["samples"]
        self.assertEqual({"minhashes": 30, "pichashes": 30, "combined": 30, "library": 0}, sample_summary["matched"]["functions"])
        self.assertEqual(100.0, sample_summary["matched"]["percent"]["unweighted"])


class PicHashCopiesShortlistTestSuite(unittest.TestCase):
    """The shortlist votes by the pairs of copies the PicHash stage used to list, so listing fewer of them
    does not change which samples get exact matching."""

    POOL = [
        ("b801000000", "mov", "eax, 1"),
        ("bb02000000", "mov", "ebx, 2"),
        ("b903000000", "mov", "ecx, 3"),
        ("ba04000000", "mov", "edx, 4"),
        ("01d8", "add", "eax, ebx"),
        ("01c8", "add", "eax, ecx"),
        ("01d0", "add", "eax, edx"),
        ("31db", "xor", "ebx, ebx"),
        ("31c9", "xor", "ecx, ecx"),
        ("31d2", "xor", "edx, edx"),
        ("40", "inc", "eax"),
        ("43", "inc", "ebx"),
        ("41", "inc", "ecx"),
        ("42", "inc", "edx"),
        ("29d8", "sub", "eax, ebx"),
        ("29c8", "sub", "eax, ecx"),
    ]

    def _report(self, name, codes, is_library=False, family=None):
        """One function per entry of codes: the same code, the same function, the same PicHash."""
        xcfg = {}
        for index, (code, variant) in enumerate(codes):
            permutation = random.Random(code * 7919 + 13).sample(range(16), 14)
            if variant:
                # one instruction changed: another PicHash, a similar MinHash
                unused = [position for position in range(16) if position not in permutation]
                permutation[7] = (permutation[7] + 1) % 16 if (permutation[7] + 1) % 16 not in permutation else unused[0]
            offset = 0x401000 + 0x80 * index
            instructions, cursor = [], offset
            for hexbytes, mnemonic, operands in [self.POOL[position] for position in permutation] + [("c3", "ret", "")]:
                instructions.append([cursor, hexbytes, mnemonic, operands])
                cursor += len(hexbytes) // 2
            metadata = {"binweight": cursor - offset, "characteristics": None, "confidence": None, "function_name": "", "strongly_connected_components": [[offset]], "tfidf": None}
            xcfg[str(offset)] = {"offset": offset, "blocks": {str(offset): instructions}, "apirefs": {}, "blockrefs": {}, "inrefs": [], "outrefs": {}, "metadata": metadata}
        statistics = dict.fromkeys(["num_api_calls", "num_basic_blocks", "num_failed_functions", "num_failed_instructions", "num_function_calls", "num_instructions"], 0)
        statistics.update(dict.fromkeys(["num_leaf_functions", "num_recursive_functions"], 0))
        statistics["num_functions"] = len(codes)
        return SmdaReport.fromDict(
            {
                "architecture": "intel",
                "base_addr": 0x400000,
                "binary_size": 0x10000,
                "bitness": 32,
                "code_areas": [],
                "confidence_threshold": 0.0,
                "disassembly_errors": {},
                "execution_time": 0.0,
                "identified_alignment": 0,
                "message": "Analysis finished regularly.",
                "metadata": {"family": family or name, "filename": name, "version": "", "is_library": is_library, "binweight": 0},
                "sha256": (name * 64)[:64],
                "smda_version": "1.0.0",
                "statistics": statistics,
                "status": "ok",
                "timestamp": "2026-01-01T00-00-00",
                "xcfg": xcfg,
            }
        )

    def testShortlistRanksCopiesAsItDidWhenEveryCopyWasPaired(self):
        def copies(code, number=1, variant=False):
            return [(code, variant)] * number

        corpus = [
            ("own", copies(1, 3) + copies(2) + copies(3, 2) + [(code, False) for code in range(10, 16)] + copies(4, 4), False, None),
            ("s1", copies(1, 2) + copies(3) + copies(10) + copies(11) + copies(4, 2), False, "fam"),
            ("s2", copies(1) + copies(3, 3) + [(code, False) for code in range(12, 16)] + copies(4, 5) + copies(11, variant=True), False, "fam"),
            ("lib", copies(2, 2) + copies(3, 2) + copies(4, 3), True, None),
            ("s3", copies(1, 4) + copies(13, 2, variant=True) + copies(14), False, "other"),
        ]
        shortlist_config = deepcopy(config)
        shortlist_config.MINHASH_CONFIG.MINHASH_MATCHING_SHORTLIST_SIZE = 1
        shortlist_config.MINHASH_CONFIG.MINHASH_POOL_MATCHING = False
        shortlist_config.MINHASH_CONFIG.MINHASH_POOL_INDEXING = False
        index = MinHashIndex(config=shortlist_config)
        worker = index.queue._worker
        sample_ids = {}
        for name, codes, is_library, family in corpus:
            sample_ids[name] = index._storage.addSmdaReport(self._report(name, codes, is_library, family)).sample_id
            worker.updateMinHashesForSample(sample_ids[name])
        ranked = []
        rank_shortlist = MatcherInterface._rankShortlist

        def spy(matcher, votes, shortlist_size):
            shortlist = rank_shortlist(matcher, votes, shortlist_size)
            ranked.append((dict(votes), set(shortlist)))
            return shortlist

        with patch.object(MatcherInterface, "_rankShortlist", spy):
            matches = MatcherSample(worker).getMatchesForSample(sample_ids["lib"])["matches"]
        # the votes main's pairing of every copy with every copy gave, and its shortlist of one: sample 2,
        # not sample 0 that wins the tie once the votes are counted per listed pair
        self.assertEqual([({0: 79, 1: 39, 2: 91, 4: 7}, {2})], ranked)
        summaries = {sample["sample_id"]: sample["matched"] for sample in matches["samples"]}
        self.assertEqual({0, 1, 2}, set(summaries))
        self.assertEqual({"minhashes": 7, "pichashes": 5, "combined": 7, "library": 0}, summaries[2]["functions"])
        self.assertEqual(100.0, summaries[2]["percent"]["unweighted"])

    def testVotesCountEveryPairOfCopies(self):
        own = [SimpleNamespace(function_id=function_id, num_instructions=12) for function_id in (1, 2, 3)]
        stub = SimpleNamespace(_function_entries=own, _pichash_size=10, _pichash_vote_entries=None)
        group = {(1, 7, 1), (1, 7, 2), (1, 7, 3), (2, 8, 20), (2, 8, 21), (2, 9, 30)}
        listed = MatcherInterface._harmonizePicHashMatches(cast(MatcherInterface, stub), {0x1234: group})
        # 3 own copies against 2 and 1 copies, and each own copy with the 2 others: 6 + 3 + 6 pairs
        self.assertEqual({8: 6, 9: 3, 7: 6}, dict(stub._pichash_vote_entries))
        self.assertLess(len(listed), sum(stub._pichash_vote_entries.values()))


if __name__ == "__main__":
    unittest.main()
