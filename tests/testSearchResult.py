import json
import os
import unittest
from unittest.mock import MagicMock, patch

from smda.common.SmdaReport import SmdaReport

from mcrit.client.McritClient import McritBadRequest, McritClient
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.storage.FamilyEntry import FamilyEntry
from mcrit.storage.FunctionEntry import FunctionEntry
from mcrit.storage.SampleEntry import SampleEntry
from mcrit.storage.SearchResult import SearchResult

from .context import config


def _as_client_answer(wire):
    """The index answers int result keys; through the client (JSON) they are strings, like toDict()"""
    return {**wire, "search_results": {str(key): value for key, value in wire["search_results"].items()}}


def _index_with_report():
    index = MinHashIndex(config)
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "example_report.smda")) as fjson:
        report = SmdaReport.fromDict(json.load(fjson))
    assert report is not None
    report.family = "search_family"
    index.addReport(report)
    return index, report


class SearchResultTest(unittest.TestCase):
    """A search answer as objects, round-tripping the wire format (fkie-cad/mcritweb#64)"""

    def test_function_search_answer_becomes_entries(self):
        index, _ = _index_with_report()
        wire = index.getFunctionSearchResults("offset:>=0", limit=4)
        result = SearchResult.fromDict(wire, FunctionEntry)
        self.assertEqual(4, len(result))
        self.assertTrue(all(isinstance(entry, FunctionEntry) for entry in result))
        self.assertEqual(sorted(int(k) for k in wire["search_results"]), sorted(result.entries))
        self.assertEqual(wire["cursor"], result.cursor)
        self.assertIsNotNone(result.cursor["forward"])
        self.assertIsNone(result.id_match)
        self.assertEqual(_as_client_answer(wire), result.toDict())

    def test_direct_matches_are_entries_without_duplicates(self):
        index, report = _index_with_report()
        sample_entry = index.getStorage().getSampleBySha256(report.sha256)
        assert sample_entry is not None
        by_id = SearchResult.fromDict(index.getSampleSearchResults(str(sample_entry.sample_id)), SampleEntry)
        assert isinstance(by_id.id_match, SampleEntry)
        self.assertEqual(sample_entry.sample_id, by_id.id_match.sample_id)
        self.assertEqual([by_id.id_match], by_id.direct_matches)
        by_sha = SearchResult.fromDict(index.getSampleSearchResults(report.sha256), SampleEntry)
        assert isinstance(by_sha.sha_match, SampleEntry)
        self.assertEqual(sample_entry.sample_id, by_sha.sha_match.sample_id)
        self.assertEqual(1, len(by_sha.direct_matches))
        families = SearchResult.fromDict(index.getFamilySearchResults("search_family"), FamilyEntry)
        self.assertEqual(["search_family"], [entry.family_name for entry in families])

    def test_an_empty_answer_is_an_empty_result(self):
        result = SearchResult.fromDict({"search_results": {}, "cursor": {"forward": None, "backward": None}, "id_match": None, "sha_match": None}, FunctionEntry)
        self.assertEqual(0, len(result))
        self.assertEqual([], result.direct_matches)
        self.assertEqual({"forward": None, "backward": None}, result.cursor)


class McritClientTypedSearchTest(unittest.TestCase):
    def _client_answering(self, data):
        response = MagicMock(status_code=200)
        response.json.return_value = {"status": "successful", "data": data}
        return response

    def test_typed_searches_answer_objects_and_the_old_ones_keep_the_dict(self):
        index, _ = _index_with_report()
        wire = index.getFunctionSearchResults("offset:>=0", limit=3)
        client = McritClient("http://mcrit.test")
        with patch("mcrit.client.McritClient.requests.get", return_value=self._client_answering(wire)) as get:
            typed = client.searchFunctions("offset:>=0", limit=3, sort_by="num_blocks", is_ascending=False)
            self.assertIn("/search/functions?", get.call_args.args[0])
            self.assertIn("sort_by=num_blocks", get.call_args.args[0])
            self.assertIn("is_ascending=False", get.call_args.args[0])
            self.assertEqual(wire, client.search_functions("offset:>=0", limit=3))
        assert typed is not None
        self.assertIsInstance(typed, SearchResult)
        self.assertEqual(3, len(typed))
        self.assertTrue(all(isinstance(entry, FunctionEntry) for entry in typed))
        self.assertEqual(_as_client_answer(wire), typed.toDict())

    def test_raw_responses_answer_the_response_itself(self):
        client = McritClient("http://mcrit.test", raw_responses=True)
        response = MagicMock(status_code=400)
        with patch("mcrit.client.McritClient.requests.get", return_value=response):
            self.assertIs(response, client.searchFunctions("bad query"))
            # the dict method answers the raw response too, like every other method in raw mode
            self.assertIs(response, client.search_functions("bad query"))

    def test_a_failed_search_answers_none(self):
        client = McritClient("http://mcrit.test")
        with patch("mcrit.client.McritClient.requests.get", return_value=MagicMock(status_code=400)):
            self.assertIsNone(client.searchSamples("bad query"))
            self.assertIsNone(client.searchFamilies("bad query"))

    def test_a_raising_client_raises_from_every_search(self):
        """The client's error modes reach the typed searches as well as the dict ones. The typed
        ones were written before the modes existed and parsed with handle_response directly,
        which answers None whatever mode the client is in."""
        client = McritClient("http://mcrit.test", raise_client_errors=True)
        failed = MagicMock(status_code=400, url="http://mcrit.test/search/samples")
        failed.json.return_value = {"status": "failed", "data": {"message": "bad query"}}
        with patch("mcrit.client.McritClient.requests.get", return_value=failed):
            for search in (client.searchSamples, client.searchFamilies, client.searchFunctions, client.search_samples):
                with self.subTest(search=search.__name__ if hasattr(search, "__name__") else "search_samples"):
                    with self.assertRaises(McritBadRequest):
                        search("bad query")


if __name__ == "__main__":
    unittest.main()
