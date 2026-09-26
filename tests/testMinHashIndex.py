#!/usr/bin/python

import json
import logging
import os
import unittest

from smda.common.SmdaReport import SmdaReport

from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.index.SearchQueryParser import SearchQueryParser
from mcrit.libs.utility import generate_unique_pairs
from mcrit.minhash.EscaperFingerprint import getEscaperFingerprint
from mcrit.storage.UniqueBlocksResult import UniqueBlocksResult

from .context import config

EXAMPLE_REPORT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "example_report.smda")

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logging.disable(logging.CRITICAL)


class MinHashIndexTestSuite(unittest.TestCase):
    """Run a full example on a memory dump"""

    def testMinHashIndexInit(self):
        MinHashIndex(config)

    def testCandidatePairGenerator(self):
        test_data = [([], []), ([1, 2, 3, 4], [(1, 2), (1, 3), (1, 4), (2, 3), (2, 4), (3, 4)])]
        for data in test_data:
            generated_all_candidates = [pair for pair in generate_unique_pairs(data[0])]
            self.assertEqual(data[1], generated_all_candidates)


MALFORMED_QUERIES = ["foo)", "(foo", "a AND", "family:(foo"]


class MalformedSearchQueryTestSuite(unittest.TestCase):
    """A query the parser cannot read is bad input, not a server fault (#146)"""

    def testParserReportsMalformedQueryAsValueError(self):
        parser = SearchQueryParser()
        for query in MALFORMED_QUERIES:
            with self.subTest(query=query):
                with self.assertRaises(ValueError) as context:
                    parser.parse(query)
                # pyparsing names the offending position, which is worth passing on to the user
                self.assertIn("char", str(context.exception))

    def testSearchResultsRejectMalformedQuery(self):
        index = MinHashIndex(config)
        searches = [index.getFamilySearchResults, index.getSampleSearchResults, index.getFunctionSearchResults]
        for search in searches:
            for query in MALFORMED_QUERIES:
                with self.subTest(search=search.__name__, query=query):
                    # ValueError is what StatusResource._respond_search answers with a 400
                    with self.assertRaises(ValueError):
                        search(query)

    def testWellFormedQueryStillSearches(self):
        index = MinHashIndex(config)
        self.assertIn("search_results", index.getFamilySearchResults("foo"))
        self.assertIn("search_results", index.getSampleSearchResults("family:foo"))
        self.assertIn("search_results", index.getFunctionSearchResults("offset:>0x100"))


class SortedSearchPagingTestSuite(unittest.TestCase):
    """Sorting by a field breaks ties by the id in the same direction (fkie-cad/mcritweb#59): the order is total,
    and paging through it by cursor neither skips nor repeats an entry, in either direction"""

    def testTieBreakFollowsTheSortDirection(self):
        index = MinHashIndex(config)
        self.assertEqual([("function_id", True)], index._get_sort_data("function_id", None, True))
        self.assertEqual([("function_id", False)], index._get_sort_data("function_id", "function_id", False))
        self.assertEqual([("num_blocks", True), ("function_id", True)], index._get_sort_data("function_id", "num_blocks", True))
        self.assertEqual([("num_blocks", False), ("function_id", False)], index._get_sort_data("function_id", "num_blocks", False))

    def testCursorPagingIsCompleteInBothDirections(self):
        index = MinHashIndex(config)
        this_file_path = str(os.path.abspath(__file__))
        for name in ("example_report.smda", "example_report_2.smda", "library_report.smda"):
            with open(os.sep.join([os.path.dirname(this_file_path), name])) as fjson:
                index.addReport(SmdaReport.fromDict(json.load(fjson)))
        all_functions = index.getFunctionSearchResults("offset:>=0", limit=1000)["search_results"]
        self.assertGreater(len(all_functions), 5)
        for is_ascending in (True, False):
            with self.subTest(ascending=is_ascending):
                seen = []
                cursor = None
                while True:
                    page = index.getFunctionSearchResults("offset:>=0", sort_by="num_instructions", is_ascending=is_ascending, cursor=cursor, limit=4)
                    seen.extend(page["search_results"].values())
                    cursor = page["cursor"]["forward"]
                    if cursor is None or not page["search_results"]:
                        break
                keys = [(entry["num_instructions"], entry["function_id"]) for entry in seen]
                self.assertEqual(sorted(keys, reverse=not is_ascending), keys)
                self.assertEqual(len(all_functions), len(seen))
                self.assertEqual(len(all_functions), len(set(keys)))


class SearchByIdentifierTestSuite(unittest.TestCase):
    """An identifier that looks valid but is not stored is an empty match, not a server fault (#158)"""

    def testUnknownSha256IsNotAMatch(self):
        index = MinHashIndex(config)
        results = index.getSampleSearchResults("a" * 64)
        self.assertIsNone(results["sha_match"])
        self.assertIsNone(results["id_match"])
        self.assertEqual({}, results["search_results"])

    def testKnownSha256IsAMatch(self):
        index = MinHashIndex(config)
        this_file_path = str(os.path.abspath(__file__))
        example_file_path = os.sep.join([os.path.dirname(this_file_path), "example_report.smda"])
        with open(example_file_path) as fjson:
            smda_report = SmdaReport.fromDict(json.load(fjson))
        assert smda_report is not None
        index.addReport(smda_report)
        sample_entry = index.getStorage().getSampleBySha256(smda_report.sha256)
        results = index.getSampleSearchResults(smda_report.sha256)
        self.assertEqual(sample_entry.sample_id, results["sha_match"]["sample_id"])

    def testUnknownIdsAreNotAMatch(self):
        index = MinHashIndex(config)
        self.assertIsNone(index.getFamilySearchResults("12345")["id_match"])
        self.assertIsNone(index.getSampleSearchResults("12345")["id_match"])
        self.assertIsNone(index.getFunctionSearchResults("12345")["id_match"])
        self.assertIsNone(index.getFunctionSearchResults("0xffffffffff")["id_match"])


class BatchLookupTestSuite(unittest.TestCase):
    """getSamplesByIds/getFamiliesByIds just forward to the storage batch reads; this pins the
    delegation (method name, argument), which resource-level tests mock away."""

    def testGetSamplesByIdsDelegatesToStorage(self):
        index = MinHashIndex(config)
        this_file_path = str(os.path.abspath(__file__))
        example_file_path = os.sep.join([os.path.dirname(this_file_path), "example_report.smda"])
        with open(example_file_path) as fjson:
            smda_report = SmdaReport.fromDict(json.load(fjson))
        assert smda_report is not None
        index.addReport(smda_report)
        sample_entry = index.getStorage().getSampleBySha256(smda_report.sha256)
        assert sample_entry is not None
        result = index.getSamplesByIds([sample_entry.sample_id, 999999])
        self.assertEqual({sample_entry.sample_id}, set(result.keys()))
        self.assertEqual(sample_entry.sha256, result[sample_entry.sample_id].sha256)
        self.assertEqual({}, index.getSamplesByIds([]))

    def testGetFamiliesByIdsDelegatesToStorage(self):
        index = MinHashIndex(config)
        family_id = index.getStorage().addFamily("batch_family")
        result = index.getFamiliesByIds([family_id, 999999])
        self.assertEqual({family_id}, set(result.keys()))
        self.assertEqual("batch_family", result[family_id].family_name)
        self.assertIsNone(result[family_id].samples)
        self.assertEqual({}, index.getFamiliesByIds([]))


class UniqueBlocksCoverTestSuite(unittest.TestCase):
    """yara_covers used to be written once as 0 and never assigned again (#144)"""

    def testYaraCoversReportsTheAchievedCover(self):
        index = MinHashIndex(config)
        worker = index.queue._worker
        report = SmdaReport.fromFile(EXAMPLE_REPORT)
        assert report is not None
        sample_entry = index._storage.addSmdaReport(report)
        assert sample_entry is not None

        result = worker.getUniqueBlocks([sample_entry.sample_id])
        statistics = result["statistics"]
        # the only sample in storage owns every block, so the greedy cover reaches the full k of 10
        self.assertTrue(statistics["has_yara_rule"])
        self.assertTrue(statistics["has_complete_yara_rule"])
        self.assertEqual(1, statistics["num_samples_covered"])
        self.assertEqual(10, statistics["yara_covers"])
        self.assertEqual(len(result["yara_rule"]), statistics["yara_covers"])

    def testWrapAtControlsTheRuleWidth(self):
        # #186: renderRule tested the wrap_string *function* for truthiness instead of the
        # wrap_at parameter, so the parameter did nothing and the hex was always wrapped at a
        # hardcoded 80 columns - the single-line branch was unreachable.
        index = MinHashIndex(config)
        worker = index.queue._worker
        report = SmdaReport.fromFile(EXAMPLE_REPORT)
        assert report is not None
        sample_entry = index._storage.addSmdaReport(report)
        assert sample_entry is not None

        # through the wire shape the real caller sees: MCRITweb feeds fromDict a result that has
        # been through JSON, so the block-hash keys are strings by then
        result = json.loads(json.dumps(worker.getUniqueBlocks([sample_entry.sample_id])))
        blocks = UniqueBlocksResult.fromDict(result)

        unwrapped = blocks.generateYaraRule(wrap_at=0)
        narrow = blocks.generateYaraRule(wrap_at=40)
        default = blocks.generateYaraRule()

        # 0 keeps each pattern on one line; a width wraps it
        self.assertNotIn("= {\n", unwrapped)
        self.assertIn("= {\n", narrow)
        # the width is honoured rather than ignored, so a narrower wrap yields more lines
        self.assertGreater(len(narrow.splitlines()), len(unwrapped.splitlines()))
        self.assertGreater(len(narrow.splitlines()), len(default.splitlines()))
        # the default reproduces the width that used to be hardcoded
        self.assertEqual(default, blocks.generateYaraRule(wrap_at=80))

    def testCoversRequiredShapesTheRule(self):
        index = MinHashIndex(config)
        worker = index.queue._worker
        report = SmdaReport.fromFile(EXAMPLE_REPORT)
        assert report is not None
        sample_entry = index._storage.addSmdaReport(report)
        assert sample_entry is not None

        # k of the k-of-n cover: with one sample in storage, every selected block covers it once,
        # so the rule is exactly k blocks long and the achieved cover equals what was asked for
        for covers_required in (1, 3, 10):
            with self.subTest(covers_required=covers_required):
                result = worker.getUniqueBlocks([sample_entry.sample_id], covers_required=covers_required)
                self.assertEqual(covers_required, result["statistics"]["covers_required"])
                self.assertEqual(covers_required, result["statistics"]["yara_covers"])
                self.assertEqual(covers_required, len(result["yara_rule"]))
                self.assertTrue(result["statistics"]["has_complete_yara_rule"])

    def testMinInstructionsFiltersBeforeTheCoverIsChosen(self):
        index = MinHashIndex(config)
        worker = index.queue._worker
        report = SmdaReport.fromFile(EXAMPLE_REPORT)
        assert report is not None
        sample_entry = index._storage.addSmdaReport(report)
        assert sample_entry is not None

        unfiltered = worker.getUniqueBlocks([sample_entry.sample_id])
        self.assertEqual(0, unfiltered["statistics"]["min_instructions"])
        self.assertEqual(len(unfiltered["unique_blocks"]), unfiltered["statistics"]["blocks_considered"])

        min_instructions = 5
        filtered = worker.getUniqueBlocks([sample_entry.sample_id], min_instructions=min_instructions)
        self.assertEqual(min_instructions, filtered["statistics"]["min_instructions"])
        # the short blocks are gone from the result, not merely skipped by the selection
        self.assertLess(len(filtered["unique_blocks"]), len(unfiltered["unique_blocks"]))
        self.assertEqual(len(filtered["unique_blocks"]), filtered["statistics"]["blocks_considered"])
        for block in filtered["unique_blocks"].values():
            self.assertGreaterEqual(block["length"], min_instructions)
        # and the cover is chosen from the survivors only
        for block_hash in filtered["yara_rule"]:
            self.assertIn(block_hash, filtered["unique_blocks"])
        # the counts from the storage layer still describe everything that was found
        self.assertEqual(unfiltered["statistics"]["unique_blocks_overall"], filtered["statistics"]["unique_blocks_overall"])

    def testYaraCoversStaysZeroWithoutASample(self):
        index = MinHashIndex(config)
        worker = index.queue._worker
        result = worker.getUniqueBlocks([])
        self.assertEqual(0, result["statistics"]["yara_covers"])
        self.assertFalse(result["statistics"]["has_yara_rule"])


class EscaperProvenanceTestSuite(unittest.TestCase):
    """MinHashes are derived from smda's escaped instructions, so an export has to say which
    escaper produced them - otherwise incompatible signatures merge silently on import."""

    def testStatusReportsEscaperProvenance(self):
        index = MinHashIndex(config)
        status = index.getStatus(with_pichash=False)["status"]
        self.assertIn("smda_version", status)
        self.assertIn("escaper_fingerprint", status)
        self.assertEqual(getEscaperFingerprint(), status["escaper_fingerprint"])

    def testExportRecordsEscaperProvenance(self):
        index = MinHashIndex(config)
        export_config = index.getExportData()["config"]
        # persisted per architecture, so widening the probe later cannot invalidate exports
        # that are already in the wild (mcrit/minhash/EscaperFingerprint.py)
        self.assertEqual({"intel": getEscaperFingerprint()}, export_config["escaper"])
        self.assertIn("smda_version", export_config)

    def _minimalExport(self, index, **config_overrides):
        export_data = index.getExportData()
        export_data["config"].update(config_overrides)
        return export_data

    def testExportCarriesFamilyActorsAndImportMergesThem(self):
        # #57
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "example_report.smda")) as fjson:
            report = SmdaReport.fromDict(json.load(fjson))
        assert report is not None
        report.family = "actors_family"
        source = MinHashIndex(config)
        sample_entry = source.getStorage().addSmdaReport(report)
        assert sample_entry is not None
        source.getStorage().modifyFamily(sample_entry.family_id, {"actors": ["Actor A"]})
        export_data = source.getExportData()
        self.assertEqual({sample_entry.family_id: ["Actor A"]}, export_data["family_actors"])
        target = MinHashIndex(config)
        target_family_id = target.getStorage().addFamily("actors_family")
        target.getStorage().modifyFamily(target_family_id, {"actors": ["Actor B"]})
        # through JSON, as an export file travels: the family ids become strings
        target.addImportData(json.loads(json.dumps(export_data)))
        self.assertEqual(["Actor B", "Actor A"], target.getStorage().getFamily(target_family_id).actors)

    def testImportedActorsAreHeldToWhatTheApiAccepts(self):
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "example_report.smda")) as fjson:
            report = SmdaReport.fromDict(json.load(fjson))
        assert report is not None
        report.family = "actors_family"
        source = MinHashIndex(config)
        sample_entry = source.getStorage().addSmdaReport(report)
        assert sample_entry is not None
        export_data = json.loads(json.dumps(source.getExportData()))
        # an export edited, or written by something else: names PUT /families would refuse
        export_data["family_actors"] = {str(sample_entry.family_id): ["Actor A", "<script>", "x" * 65, 7, " Actor A "]}
        target = MinHashIndex(config)
        target_family_id = target.getStorage().addFamily("actors_family")
        target.addImportData(export_data)
        self.assertEqual(["Actor A"], target.getStorage().getFamily(target_family_id).actors)

    def testImportOfMatchingEscaperIsNotFlagged(self):
        index = MinHashIndex(config)
        report = index.addImportData(self._minimalExport(index))
        self.assertFalse(report["escaper_mismatch"])

    def testImportOfDifferingEscaperIsFlaggedButAccepted(self):
        index = MinHashIndex(config)
        report = index.addImportData(self._minimalExport(index, escaper={"intel": "0000000000000000"}))
        # the data is still imported: a given escaper change affects only a share of functions
        self.assertIsNotNone(report)
        self.assertTrue(report["escaper_mismatch"])

    def testImportOfLegacyExportWithoutEscaperIsNotFlagged(self):
        # exports predating this field carry no escaper information, so there is nothing to compare
        index = MinHashIndex(config)
        export_data = index.getExportData()
        del export_data["config"]["escaper"]
        report = index.addImportData(export_data)
        self.assertFalse(report["escaper_mismatch"])

    def testImportComparingOnlySharedArchitectures(self):
        """An export carrying an architecture this instance does not probe must not flip the
        flag for the architecture it does carry - intersection semantics, not equality."""
        index = MinHashIndex(config)
        export_data = self._minimalExport(index, escaper={"intel": getEscaperFingerprint(), "aarch64": "deadbeefdeadbeef"})
        report = index.addImportData(export_data)
        self.assertFalse(report["escaper_mismatch"])

    def testImportOfPartiallyDifferingEscaperFlagsOnlyTheMismatch(self):
        index = MinHashIndex(config)
        export_data = self._minimalExport(index, escaper={"intel": "0000000000000000", "aarch64": getEscaperFingerprint()})
        report = index.addImportData(export_data)
        self.assertIsNotNone(report)
        self.assertTrue(report["escaper_mismatch"])

    def testImportWithNoComparableArchitectureIsNotFlagged(self):
        """Nothing shared means nothing comparable - a skip with a warning, not a mismatch."""
        index = MinHashIndex(config)
        report = index.addImportData(self._minimalExport(index, escaper={"aarch64": "deadbeefdeadbeef"}))
        self.assertFalse(report["escaper_mismatch"])


if __name__ == "__main__":
    unittest.main()
