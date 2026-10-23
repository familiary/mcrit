"""POST /import refuses with 400 what it cannot import, before storing anything of it - its families
included - while an import that fails while storing is the server's error (500)."""

import contextlib
import json
import logging
import os
import unittest
from unittest.mock import patch

import falcon
import falcon.testing
from smda.common.SmdaReport import SmdaReport

from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.libs.utility import compress_encode, decompress_decode
from mcrit.server.StatusResource import StatusResource

from .context import config
from .testImportDecompression import corrupted, encrypted

logging.disable(logging.CRITICAL)

EXAMPLE_REPORT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "example_report.smda")


class ImportValidationTestSuite(unittest.TestCase):
    def _export(self):
        index = MinHashIndex(config)
        with open(EXAMPLE_REPORT) as fjson:
            report = SmdaReport.fromDict(json.load(fjson))
        assert report is not None
        index.getStorage().addSmdaReport(report)
        return index.getExportData(compress_data=True)

    def _exportOfNewFamily(self, actors=None):
        export = self._export()
        export["family_mapping"]["7"] = "win.added"
        next(iter(export["sample_entries"].values()))["family_id"] = 7
        if actors is not None:
            export["family_actors"] = {"7": actors}
        return export

    def _post_import(self, index, body):
        request = falcon.Request(falcon.testing.create_environ(path="/import", method="POST", body=body))
        response = falcon.Response()
        StatusResource(index).on_post_import(request, response)
        return response

    def testFailingInsertIsNotAnsweredAsBadInput(self):
        # a ValueError of the storage is the server's failure, not the export's; the samples the import
        # had added are removed again - and when removing one fails in turn, that must not hide the
        # cause. The families it added may stay, as they did before
        for discard_fails in (False, True):
            with self.subTest(discard_fails=discard_fails):
                export = self._exportOfNewFamily()
                index = MinHashIndex(config)
                storage = index.getStorage()
                with contextlib.ExitStack() as stack:
                    stack.enter_context(patch.object(storage, "importFunctionEntries", side_effect=ValueError("Database insert failed.")))
                    if discard_fails:
                        stack.enter_context(patch.object(storage, "discardImportedSample", side_effect=RuntimeError("discard failed")))
                    with self.assertRaisesRegex(ValueError, "Database insert failed"):
                        self._post_import(index, json.dumps(export))
                self.assertEqual(1 if discard_fails else 0, len(storage.getSampleIds()))

    def testActorsThatCannotBeStoredDoNotFailTheImport(self):
        # they are merged once the import is complete: before its minhashes were added, a failure
        # left samples without them, which a retry then skipped as present
        export = self._exportOfNewFamily(actors=["Actor A"])
        index = MinHashIndex(config)
        storage = index.getStorage()
        with (
            patch.object(storage, "modifyFamily", side_effect=ValueError("Database update failed.")),
            patch.object(storage, "addMinHashes", wraps=storage.addMinHashes) as add_minhashes,
        ):
            response = self._post_import(index, json.dumps(export))
        self.assertEqual(falcon.HTTP_200, response.status)
        self.assertEqual(1, json.loads(response.data)["data"]["num_samples_imported"])
        add_minhashes.assert_called_once()
        self.assertEqual([], storage.getFamily(storage.getFamilyId("win.added")).actors)

    def testFailedImportLeavesTheActorsOfFamiliesItFoundAlone(self):
        export = self._exportOfNewFamily(actors=["Actor A"])
        index = MinHashIndex(config)
        storage = index.getStorage()
        family_id = storage.addFamily("win.added")
        storage.modifyFamily(family_id, {"actors": ["Actor B"]})
        with patch.object(storage, "importFunctionEntries", side_effect=ValueError("Database insert failed.")):
            with self.assertRaises(ValueError):
                self._post_import(index, json.dumps(export))
        self.assertEqual(["Actor B"], storage.getFamily(family_id).actors)
        # and an import that succeeds merges them
        self._post_import(index, json.dumps(export))
        self.assertEqual(["Actor B", "Actor A"], storage.getFamily(family_id).actors)

    def testExportOfAnotherConfigurationIsAnsweredAsBadInput(self):
        # it used to be answered with 200 and data null, which a client could not tell from success
        for field in ("shingler", "minhash"):
            with self.subTest(field=field):
                export = self._export()
                export["config"][field] = "0" * 16
                index = MinHashIndex(config)
                response = self._post_import(index, json.dumps(export))
                self.assertEqual(falcon.HTTP_400, response.status)
                self.assertIn(field if field == "shingler" else "MinHash", json.loads(response.data)["data"]["message"])
                self.assertEqual([], index.getStorage().getSampleIds())

    def testMalformedExportIsAnsweredAsBadInputAndLeavesNoFamilies(self):
        def without_mapping(export):
            del export["family_mapping"]

        def version_too_long_to_parse(export):
            # beyond 4300 digits, int() refuses with a plain ValueError, not InvalidVersion
            export["config"]["version"] = "1" * 5000

        def sample_of_unknown_family(export):
            export["family_mapping"] = {"0": "", "7": "win.added"}
            next(iter(export["sample_entries"].values()))["family_id"] = 99

        def actors_not_a_list(export):
            export["family_mapping"]["7"] = "win.added"
            export["family_actors"] = {"7": "APT1"}

        def function_entries_not_json(export):
            export["family_mapping"]["7"] = "win.added"
            for sha256 in export["function_entries"]:
                export["function_entries"][sha256] = compress_encode(b"{not json")

        def actors_of_a_family_id_that_is_no_number(export):
            # "²" is a digit to str.isdigit, but no number to int()
            export["family_mapping"]["7"] = "win.added"
            export["family_actors"] = {"²": ["APT1"]}

        def sample_stated_apart_without_function_count(export):
            # a sample whose entry states another sha256 is skipped while writing, and its count of
            # functions was read only then, once its families were stored
            export["family_mapping"]["7"] = "win.added"
            sample_entry = next(iter(export["sample_entries"].values()))
            sample_entry["sha256"] = "00" * 32
            sample_entry["statistics"] = {}

        def architecture_not_text(export):
            export["family_mapping"]["7"] = "win.added"
            next(iter(export["sample_entries"].values()))["architecture"] = ["intel"]

        def architecture_a_number(export):
            # beside one that is text, it failed sorting the architectures for the escaper warning
            sample_entry = next(iter(export["sample_entries"].values()))
            sample_entry["architecture"] = 5
            export["sample_entries"]["ab" * 32] = dict(sample_entry, architecture="intel", sha256="ab" * 32)

        def function_count_not_a_number(export):
            export["family_mapping"]["7"] = "win.added"
            next(iter(export["sample_entries"].values()))["statistics"]["num_functions"] = "10"

        def function_count_negative(export):
            # counted into the family's statistics as it is
            export["family_mapping"]["7"] = "win.added"
            next(iter(export["sample_entries"].values()))["statistics"]["num_functions"] = -(10**9)

        def is_library_not_a_boolean(export):
            # counted into the family's statistics as a number once the sample was stored
            export["family_mapping"]["7"] = "win.added"
            next(iter(export["sample_entries"].values()))["is_library"] = "yes"

        def family_id_named_twice(export):
            # "7" and "07" are both family 7
            export["family_mapping"].update({"7": "win.added", "07": "win.other"})

        def family_name_not_text(export):
            export["family_mapping"]["7"] = ["win.added"]

        def pichash_not_a_number(export):
            # memory storage failed indexing it, and again removing the sample the import had added
            export["family_mapping"]["7"] = "win.added"
            for sha256, encoded in export["function_entries"].items():
                function_entries = json.loads(decompress_decode(encoded))
                for function_entry in function_entries.values():
                    function_entry["pichash"] = [1]
                export["function_entries"][sha256] = compress_encode(json.dumps(function_entries))

        def function_entries_not_inflating(export):
            for sha256 in export["function_entries"]:
                export["function_entries"][sha256] = corrupted(export["function_entries"][sha256])

        def function_entries_encrypted(export):
            for sha256 in export["function_entries"]:
                export["function_entries"][sha256] = encrypted(export["function_entries"][sha256])

        def function_entries_nested_too_deeply(export):
            for sha256 in export["function_entries"]:
                export["function_entries"][sha256] = compress_encode("[" * 200000 + "]" * 200000)

        for malform in (
            without_mapping,
            version_too_long_to_parse,
            sample_of_unknown_family,
            actors_not_a_list,
            function_entries_not_json,
            actors_of_a_family_id_that_is_no_number,
            sample_stated_apart_without_function_count,
            architecture_not_text,
            architecture_a_number,
            function_count_not_a_number,
            function_count_negative,
            is_library_not_a_boolean,
            family_id_named_twice,
            family_name_not_text,
            pichash_not_a_number,
            function_entries_not_inflating,
            function_entries_encrypted,
            function_entries_nested_too_deeply,
        ):
            with self.subTest(malform=malform.__name__):
                export = self._export()
                malform(export)
                index = MinHashIndex(config)
                families_before = index.getStorage().getFamilyIds()
                response = self._post_import(index, json.dumps(export))
                self.assertEqual(falcon.HTTP_400, response.status)
                self.assertIn("Import rejected", json.loads(response.data)["data"]["message"])
                self.assertEqual(families_before, index.getStorage().getFamilyIds())
                self.assertEqual([], index.getStorage().getSampleIds())

    def testExportsOfOlderVersionsStillImport(self):
        # what older versions stored, and getExportData exports as it is
        def architecture_null(export):
            next(iter(export["sample_entries"].values()))["architecture"] = None

        def family_name_null(export):
            # a report without a family, exported as null: such a sample used to land in a family
            # named null, and goes to the default family now, named "" as local ones are
            export["family_mapping"] = {"3": None}
            sample_entry = next(iter(export["sample_entries"].values()))
            sample_entry["family_id"] = 3
            sample_entry["family"] = None

        def family_id_written_twice_alike(export):
            export["family_mapping"] = {"0": "", "00": ""}

        for older in (architecture_null, family_name_null, family_id_written_twice_alike):
            with self.subTest(older=older.__name__):
                export = self._export()
                older(export)
                index = MinHashIndex(config)
                response = self._post_import(index, json.dumps(export))
                self.assertEqual(falcon.HTTP_200, response.status)
                self.assertEqual(1, json.loads(response.data)["data"]["num_samples_imported"])
                self.assertEqual([0], index.getStorage().getFamilyIds())
                self.assertEqual([""], [sample_entry.family for sample_entry in index.getStorage().getSamples(0, 0)])

    def testPresentSampleIsSkippedWithoutTheFieldsOnlyStoringNeeds(self):
        # it was refused for lacking them, where it used to be skipped
        export = self._export()
        index = MinHashIndex(config)
        self.assertEqual(falcon.HTTP_200, self._post_import(index, json.dumps(export)).status)
        for sample_entry in export["sample_entries"].values():
            del sample_entry["architecture"]
            del sample_entry["is_library"]
        response = self._post_import(index, json.dumps(export))
        self.assertEqual(falcon.HTTP_200, response.status)
        self.assertEqual(1, json.loads(response.data)["data"]["num_samples_skipped"])

    def testActorsOfOneFamilyIdWrittenTwiceAreMerged(self):
        export = self._exportOfNewFamily()
        export["family_actors"] = {"7": ["Actor A", "Actor B"], "07": ["Actor B", "Actor C"]}
        index = MinHashIndex(config)
        self.assertEqual(falcon.HTTP_200, self._post_import(index, json.dumps(export)).status)
        storage = index.getStorage()
        self.assertEqual(["Actor A", "Actor B", "Actor C"], storage.getFamily(storage.getFamilyId("win.added")).actors)


if __name__ == "__main__":
    unittest.main()
