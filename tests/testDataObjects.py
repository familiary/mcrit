#!/usr/bin/python

import json
import logging
import os
import unittest
from copy import deepcopy

from smda.common.SmdaReport import SmdaReport

import mcrit.matchers.MatcherFlags as MatcherFlags
from mcrit.minhash.MinHash import MinHash
from mcrit.storage.FunctionEntry import FunctionEntry
from mcrit.storage.MatchingResult import MatchingResult
from mcrit.storage.SampleEntry import SampleEntry

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logging.disable(logging.CRITICAL)


class MinHashingTestSuite(unittest.TestCase):
    """Run a full example on a memory dump"""

    def testFunctionEntry(self):
        THIS_FILE_PATH = str(os.path.abspath(__file__))
        PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
        example_file_path = os.sep.join([PROJECT_ROOT, "tests", "example_report.smda"])
        with open(example_file_path) as fjson:
            smda_json = json.load(fjson)

        smda_report = SmdaReport.fromDict(smda_json)

        assert smda_report is not None
        sample_entry = SampleEntry(smda_report, sample_id=0, family_id=0)
        minhash = MinHash(function_id=1, minhash_signature=[0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39])
        for smda_function in smda_report.getFunctions():
            function_entry = FunctionEntry(sample_entry, smda_function, 0, minhash=minhash)

            as_dict = function_entry.toDict()
            as_entry = FunctionEntry.fromDict(as_dict)

            # test text-friendly encoding of binary pichashes
            self.assertEqual(as_entry.minhash, function_entry.minhash)

    def testFunctionEntryKeepsTheShingleCompositionOfItsMinHash(self):
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "example_report.smda")) as fjson:
            smda_report = SmdaReport.fromDict(json.load(fjson))
        assert smda_report is not None
        sample_entry = SampleEntry(smda_report, sample_id=0, family_id=0)
        minhash = MinHash(function_id=1, minhash_signature=[0x30, 0x31])
        minhash.shingler_composition = {"EscapedBlockShingler": 2}
        function_entry = FunctionEntry(sample_entry, next(iter(smda_report.getFunctions())), 1, minhash=minhash)
        self.assertEqual({"EscapedBlockShingler": 2}, function_entry.minhash_shingle_composition)
        self.assertEqual({"EscapedBlockShingler": 2}, function_entry.toDict()["minhash_shingle_composition"])
        self.assertEqual({"EscapedBlockShingler": 2}, FunctionEntry.fromDict(function_entry.toDict()).minhash_shingle_composition)

    def testSampleEntry(self):
        THIS_FILE_PATH = str(os.path.abspath(__file__))
        PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
        example_file_path = os.sep.join([PROJECT_ROOT, "tests", "example_report.smda"])
        with open(example_file_path) as fjson:
            smda_json = json.load(fjson)

        smda_report = SmdaReport.fromDict(smda_json)

        assert smda_report is not None
        sample_entry = SampleEntry(smda_report, sample_id=0, family_id=0)

        as_dict = sample_entry.toDict()
        as_entry = SampleEntry.fromDict(as_dict)
        self.assertEqual(as_entry.sample_id, sample_entry.sample_id)

    def testMatchingResult(self):
        THIS_FILE_PATH = str(os.path.abspath(__file__))
        PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
        example_file_path = os.sep.join([PROJECT_ROOT, "tests", "example_matching_report.json"])
        with open(example_file_path) as fjson:
            match_json = json.load(fjson)
        matching_result = MatchingResult.fromDict(match_json)
        assert len(matching_result.sample_matches) == 2
        assert len(matching_result.getFunctionMatches()) == 719
        assert len(set([match.function_id for match in matching_result.getFunctionMatches()])) == 515
        ### test filtering
        # test filtering by family and sample counts
        filtered_result = deepcopy(matching_result)
        filtered_result.filterToFamilyCount(1)
        assert len(filtered_result.getFunctionMatches()) == 414
        assert len(filtered_result.getFunctionMatches(unfiltered=True)) == 719
        filtered_result = deepcopy(matching_result)
        filtered_result.filterToSampleCount(max_samples=1)
        assert len(filtered_result.getFunctionMatches()) == 414
        filtered_result.filterToSampleCount(min_samples=2)
        assert len(filtered_result.getFunctionMatches()) == 0
        # filter by score / library
        filtered_result = deepcopy(matching_result)
        filtered_result.filterToFunctionScore(95)
        assert len(filtered_result.getFunctionMatches()) == 581
        filtered_result = deepcopy(matching_result)
        filtered_result.filterToFunctionScore(min_score=95, library_only=True)
        assert len(filtered_result.getFunctionMatches()) == 718
        filtered_result = deepcopy(matching_result)
        filtered_result.excludeLibraryMatches()
        assert len(filtered_result.getFunctionMatches()) == 715
        assert len(set([match.function_id for match in filtered_result.getFunctionMatches()])) == 513

    def testUniqueFamilyScoreUsesTheBestMatchOfAFunction(self):
        # #157: a function matched twice within one family (two samples, two scores) must
        # contribute its best score to the unique family score, regardless of match order
        THIS_FILE_PATH = str(os.path.abspath(__file__))
        PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
        example_file_path = os.sep.join([PROJECT_ROOT, "tests", "example_matching_report.json"])
        with open(example_file_path) as fjson:
            match_json = json.load(fjson)
        matching_result = MatchingResult.fromDict(match_json)
        # the fixture scores every match of a function alike; spread them so that the order
        # of iteration would show if only the last one were kept
        matches_by_function = {}
        for match in matching_result.function_matches:
            matches_by_function.setdefault(match.function_id, []).append(match)
        for matches in matches_by_function.values():
            for rank, match in enumerate(matches):
                match.matched_score = max(10.0, match.matched_score - 15.0 * rank)
        # and give one function two differently scored matches in two samples of the same
        # family, so that a per-function maximum would credit the weaker sample too much
        sample_ids = [entry.sample_id for entry in matching_result.sample_matches]
        self.assertEqual(2, len(sample_ids))
        twin_matches = next(matches for matches in matches_by_function.values() if len(matches) >= 2)
        for match, sample_id in zip(twin_matches[:2], sample_ids):
            match.matched_family_id = twin_matches[0].matched_family_id
            match.matched_sample_id = sample_id
        twin_matches[0].matched_score, twin_matches[1].matched_score = 90.0, 30.0
        # a function is unique to a family when all its matches are in that one family; the
        # expected bytes are then the best-scored match of the function, summed per sample
        expected_bytes = {entry.sample_id: 0 for entry in matching_result.sample_matches}
        best_per_function_and_sample = {}
        families_per_function = {}
        for match in matching_result.function_matches:
            weighted = match.num_bytes * match.matched_score / 100.0
            key = (match.function_id, match.matched_sample_id)
            best_per_function_and_sample[key] = max(best_per_function_and_sample.get(key, 0), weighted)
            families_per_function.setdefault(match.function_id, set()).add(match.matched_family_id)
        for (function_id, sample_id), weighted in best_per_function_and_sample.items():
            if len(families_per_function[function_id]) == 1:
                expected_bytes[sample_id] += weighted
        differing = [
            fid
            for fid in families_per_function
            if len(families_per_function[fid]) == 1 and len({m.matched_score for m in matching_result.function_matches if m.function_id == fid}) > 1
        ]
        self.assertGreater(len(differing), 0, "the fixture needs functions matched with differing scores to be meaningful")
        for sample_id, bytes_expected in expected_bytes.items():
            info = matching_result.getUniqueFamilyMatchInfoForSample(sample_id)
            self.assertAlmostEqual(bytes_expected, info["bytes_matched"], places=6)
        # and the same numbers when the matches arrive in reverse order
        reversed_result = MatchingResult.fromDict(match_json)
        reversed_result.function_matches = list(reversed(matching_result.function_matches))
        for sample_id, bytes_expected in expected_bytes.items():
            self.assertAlmostEqual(bytes_expected, reversed_result.getUniqueFamilyMatchInfoForSample(sample_id)["bytes_matched"], places=6)

    def testMatchingResultLazyFiltering(self):
        """The filtered_* lists are derived lazily and can be reset, so that a MatchingResult
        may be reused across several independent filter runs (e.g. from a cache)."""
        THIS_FILE_PATH = str(os.path.abspath(__file__))
        PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
        example_file_path = os.sep.join([PROJECT_ROOT, "tests", "example_matching_report.json"])
        with open(example_file_path) as fjson:
            match_json = json.load(fjson)

        # fromDict must not materialize the filtered_* lists up front
        matching_result = MatchingResult.fromDict(match_json)
        assert matching_result._filtered_function_matches is None
        assert matching_result._filtered_sample_matches is None

        # ... but reading them yields the full, unfiltered set
        assert len(matching_result.filtered_function_matches) == 719
        assert len(matching_result.filtered_sample_matches) == 2
        assert matching_result._filtered_function_matches is not None

        # the lazy copy is shallow: entries are shared, the list itself is not
        assert matching_result.filtered_function_matches is not matching_result.function_matches
        assert matching_result.filtered_function_matches[0] is matching_result.function_matches[0]

        # filtering rebinds the filtered list and must leave the originals untouched
        matching_result.filterToFamilyCount(1)
        assert len(matching_result.getFunctionMatches()) == 414
        assert len(matching_result.function_matches) == 719
        assert len(matching_result.getFunctionMatches(unfiltered=True)) == 719

        # resetFilters() restores the unfiltered view, so filters do not accumulate
        matching_result.resetFilters()
        assert len(matching_result.getFunctionMatches()) == 719
        assert len(matching_result.filtered_sample_matches) == 2

        # a second, independent filter run on the same object yields the same result as the
        # first - this is what makes caching a parsed MatchingResult safe
        matching_result.filterToFamilyCount(1)
        assert len(matching_result.getFunctionMatches()) == 414

        # without a reset, applyFilterValues-style filtering is cumulative (documented behaviour)
        matching_result.resetFilters()
        matching_result.filterToSampleCount(max_samples=1)
        assert len(matching_result.getFunctionMatches()) == 414
        matching_result.filterToSampleCount(min_samples=2)
        assert len(matching_result.getFunctionMatches()) == 0

    def testMatchingResultLinkHuntDoesNotLeakIntoWireFormat(self):
        """getLinkHuntResults writes matched_family / matched_link_score / matched_unique onto
        the original entries, which the filtered_* lists now share. Those fields are additive
        only and must stay absent from the serialized form."""
        THIS_FILE_PATH = str(os.path.abspath(__file__))
        PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
        example_file_path = os.sep.join([PROJECT_ROOT, "tests", "example_matching_report.json"])
        with open(example_file_path) as fjson:
            match_json = json.load(fjson)

        matching_result = MatchingResult.fromDict(match_json)
        before = matching_result.toDict()

        link_hunt_result = matching_result.getLinkHuntResults(min_score=50)
        assert len(link_hunt_result) > 0
        # the link-hunt specific fields have indeed been populated on shared entries
        assert any(match.matched_link_score > 0 for match in link_hunt_result)

        # ... yet the serialized report is byte-identical, and the counts are unchanged
        assert matching_result.toDict() == before
        assert len(matching_result.function_matches) == 719
        assert len(matching_result.filtered_function_matches) == 719

    def testMatchingResultRoundTripsThroughItsWireFormat(self):
        """toDict must hand back the shape the matchers produce and fromDict reads: a list of
        per-function summaries. It used to emit a dict keyed by function_id, which fromDict could
        not read back (#44)."""
        THIS_FILE_PATH = str(os.path.abspath(__file__))
        PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
        example_file_path = os.sep.join([PROJECT_ROOT, "tests", "example_matching_report.json"])
        with open(example_file_path) as fjson:
            match_json = json.load(fjson)

        matching_result = MatchingResult.fromDict(match_json)
        wire = matching_result.toDict()
        self.assertIsInstance(wire["matches"]["functions"], list)
        # function_matches is kept sorted by function_id, so the summaries come out in that order
        by_fid = lambda summary: summary["fid"]  # noqa: E731
        self.assertEqual(sorted(match_json["matches"]["functions"], key=by_fid), wire["matches"]["functions"])
        self.assertEqual(match_json["matches"]["aggregation"], wire["matches"]["aggregation"])
        # the sample entry's own serialisation may carry more fields than the example file
        self.assertEqual(SampleEntry.fromDict(match_json["info"]["sample"]).toDict(), wire["info"]["sample"])

        reparsed = MatchingResult.fromDict(wire)
        self.assertEqual(len(matching_result.function_matches), len(reparsed.function_matches))
        self.assertEqual([m.getMatchTuple() for m in matching_result.function_matches], [m.getMatchTuple() for m in reparsed.function_matches])
        self.assertEqual(matching_result.function_id_to_family_ids_matched, reparsed.function_id_to_family_ids_matched)
        self.assertEqual(matching_result.library_matches, reparsed.library_matches)
        self.assertEqual(matching_result.is_query, reparsed.is_query)
        # the flag booleans are views on the flags integer the wire format carries
        flagged = next(m for m in matching_result.function_matches if m.match_is_minhash and m.match_is_pichash and not m.match_is_library)
        expected_flags = MatcherFlags.IS_MINHASH_FLAG | MatcherFlags.IS_PICHASH_FLAG
        self.assertEqual(expected_flags, flagged.match_flags)
        self.assertFalse(flagged.match_is_library)
        self.assertEqual(expected_flags, flagged.getMatchTuple()[4])
        self.assertEqual("Function: fid(%d)" % flagged.function_id, str(flagged)[: len("Function: fid(%d)" % flagged.function_id)])
        self.assertTrue(str(flagged).endswith("flags(mp.)"))

    def testQueryResultKeepsItsNegativeFunctionIdsOnTheWire(self):
        """a query result's function ids are negative on the wire; fromDict folds the sign into
        is_query, so toDict has to put it back for the round trip to hold"""
        THIS_FILE_PATH = str(os.path.abspath(__file__))
        PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
        with open(os.sep.join([PROJECT_ROOT, "tests", "example_matching_report.json"])) as fjson:
            match_json = json.load(fjson)
        for summary in match_json["matches"]["functions"]:
            summary["fid"] = -summary["fid"]
        query_result = MatchingResult.fromDict(match_json)
        self.assertTrue(query_result.is_query)
        wire = query_result.toDict()
        self.assertTrue(all(summary["fid"] < 0 for summary in wire["matches"]["functions"]))
        reparsed = MatchingResult.fromDict(wire)
        self.assertTrue(reparsed.is_query)
        self.assertEqual([m.function_id for m in query_result.function_matches], [m.function_id for m in reparsed.function_matches])


if __name__ == "__main__":
    unittest.main()
