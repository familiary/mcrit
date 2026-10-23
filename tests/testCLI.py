#!/usr/bin/python

import contextlib
import datetime
import io
import logging
import os
import subprocess
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import mcrit
from mcrit.client.McritConsole import McritConsole, get_primary_smda_meta_data, is_smda_report, printable, submitViaSubprocess
from mcrit.storage.SampleEntry import SampleEntry

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logging.disable(logging.CRITICAL)


class FunctionText:
    def __init__(self, text):
        self.text = text

    def __str__(self):
        return self.text


class TestCLI(unittest.TestCase):
    """Run a full example on a memory dump"""

    def testIsSmdaReport(self):
        THIS_FILE_PATH = str(os.path.abspath(__file__))
        PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
        example_file_path_1 = os.sep.join([PROJECT_ROOT, "tests", "example_report_meta.smda"])
        wrong_example_path = os.sep.join([PROJECT_ROOT, "tests", "example_matching_report.json"])
        self.assertTrue(is_smda_report(example_file_path_1))
        self.assertFalse(is_smda_report(wrong_example_path))

    def testGetPrimaryMeta(self):
        THIS_FILE_PATH = str(os.path.abspath(__file__))
        PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
        example_file_path_1 = os.sep.join([PROJECT_ROOT, "tests", "example_report_meta.smda"])
        smda_meta = get_primary_smda_meta_data(example_file_path_1)
        self.assertIsNotNone(smda_meta)
        self.assertEqual(smda_meta["sha256"], "ae38ff0778fb8dfa1deb17301a15165934312648d232d167cd0c0034c24689e1")
        self.assertEqual(smda_meta["filename"], "example_filename")
        self.assertEqual(smda_meta["family"], "example_family")
        self.assertEqual(smda_meta["version"], "example_version")

    def _submitArgs(self, argv):
        console = McritConsole()
        console.client = MagicMock()
        return console, console.parser.parse_args(["client", "submit"] + argv)

    def testSubprocessDoesNotRunModulesFromTheWorkingDirectory(self):
        # -m puts the working directory first on sys.path, and bulk submissions start in folders of untrusted files
        _, args = self._submitArgs(["/some/dir", "--mode", "dir"])
        with patch("mcrit.client.McritConsole.subprocess.Popen") as mock_popen:
            mock_popen.return_value.communicate.return_value = (b"", b"")
            with contextlib.redirect_stdout(io.StringIO()):
                submitViaSubprocess(args, "/some/sample")
        command = mock_popen.call_args[0][0]
        self.assertEqual(sys.executable, command[0])
        # the interpreter and module part of the child's command, without arguments mcrit prints its usage
        interpreter = command[: command.index("mcrit") + 1]
        mcrit_root = os.path.dirname(os.path.dirname(os.path.abspath(mcrit.__file__)))
        env = dict(os.environ, PYTHONPATH=os.pathsep.join(filter(None, [mcrit_root, os.environ.get("PYTHONPATH")])))
        with tempfile.TemporaryDirectory() as tmp_dir:
            os.makedirs(os.path.join(tmp_dir, "mcrit"))
            open(os.path.join(tmp_dir, "mcrit", "__init__.py"), "w").close()
            with open(os.path.join(tmp_dir, "mcrit", "__main__.py"), "w") as fout:
                fout.write("print('planted')\n")
            result = subprocess.run(interpreter, cwd=tmp_dir, env=env, capture_output=True, timeout=120)
        self.assertNotIn(b"planted", result.stdout)
        self.assertNotIn(b"No module named", result.stderr)

    def testSubprocessOutputThatIsNotUtf8IsPrinted(self):
        # a child echoing a file name that is not UTF-8 (CP1251 here) must not end the bulk submission
        _, args = self._submitArgs(["/some/dir", "--mode", "dir"])
        with patch("mcrit.client.McritConsole.subprocess.Popen") as mock_popen:
            mock_popen.return_value.communicate.return_value = (b"SKIPPING: /some/dir/\xef\xf0\xe8\xec\xe5\xf0.exe", b"ERROR: /some/dir/\xef\xf0\xe8\xec\xe5\xf0.exe")
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                submitViaSubprocess(args, "/some/sample")
        self.assertIn("SKIPPING: /some/dir/\\xef\\xf0\\xe8\\xec\\xe5\\xf0.exe", output.getvalue())
        self.assertIn("ERROR: /some/dir/\\xef\\xf0\\xe8\\xec\\xe5\\xf0.exe", output.getvalue())

    def testSubmitPrintsPathsThatAreNotUtf8UnderAStrictLocale(self):
        # under en_US.UTF-8 stdout is strict, unlike under C.UTF-8: the parent's own "Processing file: <path>"
        # raised UnicodeEncodeError for a CP1251-named file and ended the recursive run before the next file
        with tempfile.TemporaryDirectory() as tmp_dir:
            sample_dir = os.path.join(tmp_dir, "some_family", "1.0")
            os.makedirs(sample_dir)
            try:
                for name in (b"\xef\xf0\xe8\xec\xe5\xf0.exe", b"second.exe"):
                    with open(os.path.join(os.fsencode(sample_dir), name), "wb") as fout:
                        fout.write(b"MZ\x90\x00" + name)
            except (OSError, ValueError):
                self.skipTest("this filesystem does not take file names that are not UTF-8")
            console, args = self._submitArgs(["--mode", "recursive", tmp_dir])
            console.client.getSamples.return_value = {}
            printed = io.BytesIO()
            strict_stdout = io.TextIOWrapper(printed, encoding="utf-8", errors="strict", write_through=True)
            with patch("sys.stdout", strict_stdout):
                console._handle_submit(args)
            self.assertIn(b"second.exe", printed.getvalue())
            self.assertIn(b"\\udcef\\udcf0\\udce8\\udcec\\udce5\\udcf0.exe", printed.getvalue())

    def testServerTextIsPrintedWithControlCharactersEscaped(self):
        # family names, versions, filenames and job parameters are free text other API users store
        sequences = "\x1b]0;title\x07\r\x1b[2K\x1b[8m\x9b\n"
        self.assertEqual("\\x1b]0;title\\x07\\r\\x1b[2K\\x1b[8m\\x9b\\n", printable(sequences))
        self.assertEqual("plain text, ünïcode and 1.0", printable("plain text, ünïcode and 1.0"))
        self.assertEqual("5", printable(5))
        console = McritConsole()
        console.client = MagicMock()
        console.client.getQueueData.return_value = [
            SimpleNamespace(job_id="0" * 24, result=None, created_at=None, started_at=None, finished_at=None, parameters=f"addBinarySample(None, {sequences}.exe)", progress=0)
        ]
        family = {"family_id": 1, "family_name": sequences, "num_samples": 1, "num_functions": 1, "num_library_samples": 0}
        sample_entry = SampleEntry(None, 1, 1)
        sample_entry.sha256 = "a" * 64
        sample_entry.filename = sequences + ".exe"
        sample_entry.family = sequences
        sample_entry.version = sequences
        sample_entry.timestamp = datetime.datetime(2026, 1, 1)
        sample_entry.architecture = "intel"
        sample_entry.bitness = 32
        console.client.search_families.return_value = {"search_results": {"1": family}}
        console.client.search_samples.return_value = {"search_results": {"1": {}}}
        console.client.search_functions.return_value = {"search_results": {"1": {}}}
        console.client.requestMatchesForUnmappedBinary.return_value = "0" * 24
        # the sha256 of a submitted report is the submitter's as well, the table shows 8 characters of it
        sha256 = "\x1b[2K\x1b[8m" + "a" * 56
        match = MagicMock(family=sequences, version=sequences, sample_id=1, sha256=sha256, num_functions=1, matched_functions_minhash=1, matched_functions_pichash=1)
        match.matched_functions_library = 0
        for weighting in ["score", "nonlib_score", "frequency", "nonlib_frequency"]:
            setattr(match, f"matched_percent_{weighting}_weighted", 100.0)
        with tempfile.TemporaryDirectory() as tmp_dir:
            sample_path = os.path.join(tmp_dir, "sample.exe")
            with open(sample_path, "wb") as fout:
                fout.write(b"MZ\x90\x00")
            stdout = io.StringIO()
            with (
                patch("mcrit.client.McritConsole.MatchingResult") as matching_result,
                patch("mcrit.client.McritConsole.SampleEntry.fromDict", return_value=sample_entry),
                patch("mcrit.client.McritConsole.FunctionEntry.fromDict", return_value=FunctionText(sequences)),
                contextlib.redirect_stdout(stdout),
            ):
                matching_result.fromDict.return_value.getBestSampleMatchesPerFamily.return_value = [match]
                console._handle_queue(console.parser.parse_args(["client", "queue"]))
                console._handle_search(console.parser.parse_args(["client", "search", "term"]))
                console._handle_query(console.parser.parse_args(["client", "query", sample_path]))
        output = stdout.getvalue()
        self.assertNotIn("\x1b", output)
        self.assertNotIn("\r", output)
        # the queue line, the family, the sample, the function and the family and version of the match
        self.assertGreaterEqual(output.count("\\x1b]0;title\\x07"), 6)
        self.assertIn("|      1 | \\x1b[2K\\x1b[8m |     1 |", output)

    def testQueryOutputIsNamedAfterAValidJobIdOnly(self):
        # the job id is the server's answer and names the file `query -o` writes
        valid = "0123456789abcdef01234567"
        for job_id, expected_files in [(valid, [valid + ".json"]), ("../escaped", []), (None, []), (valid + "/../../escaped", []), (valid + "..", []), (valid + "\x00x", [])]:
            with self.subTest(job_id=job_id), tempfile.TemporaryDirectory() as tmp_dir:
                output_dir = os.path.join(tmp_dir, "out")
                os.makedirs(output_dir)
                # where a job id with a valid prefix and a path after it would be written
                os.makedirs(os.path.join(output_dir, valid))
                expected_files = sorted(expected_files + [valid])
                sample_path = os.path.join(tmp_dir, "sample.exe")
                with open(sample_path, "wb") as fout:
                    fout.write(b"MZ\x90\x00")
                console = McritConsole()
                console.client = MagicMock()
                console.client.requestMatchesForUnmappedBinary.return_value = job_id
                console.client.awaitResult.return_value = {"server": "chosen"}
                args = console.parser.parse_args(["client", "query", "-o", output_dir, sample_path])
                with patch("mcrit.client.McritConsole.MatchingResult"), contextlib.redirect_stdout(io.StringIO()):
                    console._handle_query(args)
                self.assertEqual(expected_files, sorted(os.listdir(output_dir)))
                self.assertEqual(["out", "sample.exe"], sorted(os.listdir(tmp_dir)))
                self.assertEqual(job_id == valid, console.client.awaitResult.called)

    def testBidirectionalControlsAreEscaped(self):
        self.assertEqual("a\\u202eb\\u2066c\\u2069d", printable("a\u202eb\u2066c\u2069d"))

    def testDirModeWarningsEscapeControlCharacters(self):
        sequences = "\x1b]0;title\x07\x1b[2K"
        sample = SimpleNamespace(sample_id=1, sha256="a" * 64, filename="x", family=sequences, version=sequences)
        meta = {"sha256": "a" * 64, "family": sequences, "version": sequences}
        for file_family in (sequences, "other"):
            with self.subTest(file_family=file_family), tempfile.TemporaryDirectory() as tmp_dir:
                with open(os.path.join(tmp_dir, "report.smda"), "w") as fout:
                    fout.write("{}")
                console = McritConsole()
                console.client = MagicMock()
                console.client.getSamples.return_value = {1: sample}
                args = console.parser.parse_args(["client", "submit", "--mode", "dir", "--smda", tmp_dir])
                stdout = io.StringIO()
                with (
                    patch("mcrit.client.McritConsole.is_smda_report", return_value=True),
                    patch("mcrit.client.McritConsole.get_primary_smda_meta_data", return_value=dict(meta, family=file_family)),
                    contextlib.redirect_stdout(stdout),
                ):
                    console._handle_submit(args)
                output = stdout.getvalue()
                self.assertIn("SKIPPING", output)
                self.assertNotIn("\x1b", output)
                self.assertIn("\\x1b]0;title\\x07\\x1b[2K", output)

    def testMalpediaWarningsEscapeControlCharacters(self):
        sequences = "\x1b]0;title\x07\x1b[2K"
        filename = "a" * 64 + "_unpacked" + sequences
        missing = "b" * 64 + "_unpacked" + sequences
        samples = {
            1: SimpleNamespace(sample_id=1, sha256="1" * 64, filename=filename, family=sequences, version=sequences),
            2: SimpleNamespace(sample_id=2, sha256="2" * 64, filename=filename, family=sequences, version=sequences),
            3: SimpleNamespace(sample_id=3, sha256="3" * 64, filename=missing, family=sequences, version=sequences),
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = os.path.join(tmp_dir, "malpedia")
            os.makedirs(root)
            with open(os.path.join(root, "malpedia.bib"), "w") as fout:
                fout.write("")
            console = McritConsole()
            console.client = MagicMock()
            console.client.getSamples.return_value = samples
            args = console.parser.parse_args(["client", "submit", "--mode", "malpedia", root])
            stdout = io.StringIO()
            found = {filename: {"filename": filename, "filepath": "x", "family": "other", "version": "1"}}
            with patch.object(console, "_getMalpediaSamplesByFilename", return_value=found), contextlib.redirect_stdout(stdout):
                console._handle_submit(args)
        output = stdout.getvalue()
        self.assertNotIn("\x1b", output)
        # the duplicate filename, the family mismatch with its MCRIT line, and the sample missing from Malpedia
        self.assertIn("appears to exist more than once", output)
        self.assertIn("has different family/version", output)
        self.assertIn("* MCRIT: \\x1b]0;title", output)
        self.assertIn("present in MCRIT but not in Malpedia", output)
        self.assertEqual(3, output.count("_unpacked\\x1b]0;title"))


if __name__ == "__main__":
    unittest.main()
