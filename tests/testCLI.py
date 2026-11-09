#!/usr/bin/python

import argparse
import contextlib
import datetime
import hashlib
import io
import logging
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import mcrit
from mcrit.client.McritConsole import McritConsole, get_base_addr, get_primary_smda_meta_data, is_smda_report, printable, submitViaSubprocess
from mcrit.storage.SampleEntry import SampleEntry

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logging.disable(logging.CRITICAL)

SHA256 = "a" * 64
REPORT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "example_report_meta.smda")
REPORT_SHA256 = "ae38ff0778fb8dfa1deb17301a15165934312648d232d167cd0c0034c24689e1"


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


class TestSubmitModes(unittest.TestCase):
    def testMalpediaFilenames(self):
        # #280: every dump was skipped, as the pattern lacked the underscore before "dump"
        console = McritConsole.__new__(McritConsole)
        for suffix in ("_unpacked", "_dump_0x00400000", "_dump7_0x00400000", "_dump10_0x0000000140000000", "_dump7_0x00350000_loader"):
            with self.subTest(suffix=suffix):
                self.assertTrue(console._isMalpediaFilename(SHA256 + suffix))
        for filename in (SHA256, SHA256 + "dump7_0x00400000", SHA256 + "_packed", SHA256 + "_dump_0x0040", SHA256 + "_dump_0x00400000000000000", "B" * 64 + "_unpacked"):
            with self.subTest(filename=filename):
                self.assertFalse(console._isMalpediaFilename(filename))

    def testBaseAddressOfADump(self):
        self.assertEqual(0x400000, get_base_addr(SHA256 + "_dump7_0x00400000"))
        self.assertEqual(0x350000, get_base_addr(SHA256 + "_dump7_0x00350000_loader"))
        self.assertEqual(0x140000000, get_base_addr(SHA256 + "_dump_0x0000000140000000"))
        self.assertEqual(0x400000, get_base_addr(SHA256 + "_dump7_0x00400000_loader_x64.bin"))
        self.assertEqual(0x400000, get_base_addr(os.path.join("malpedia", "win.family", SHA256 + "_dump7_0x00400000_loader")))
        # only a Malpedia dump name may go on after its address; other files read as before
        self.assertIsNone(get_base_addr("sample_0x00400000_copy"))
        self.assertIsNone(get_base_addr("sample_dump_0x00400000_loader"))
        self.assertIsNone(get_base_addr(SHA256 + "_unpacked"))

    def testRecursiveSubmitsEveryFile(self):
        # #281: a leftover continue made --mode recursive print every file and submit none
        console = McritConsole.__new__(McritConsole)
        console.client = MagicMock()
        console.client.getSamples.return_value = {}
        with tempfile.TemporaryDirectory() as root:
            os.makedirs(os.path.join(root, "win.family", "1.0"))
            for name in ("a.bin", "b.bin"):
                with open(os.path.join(root, "win.family", "1.0", name), "wb") as fout:
                    fout.write(name.encode())
            with open(os.path.join(root, "c.bin"), "wb") as fout:
                fout.write(b"c")
            # _handle_submit makes the path absolute before dispatching
            with patch("mcrit.client.McritConsole.getSmdaReportFromFilepath", side_effect=lambda args, filepath: MagicMock()):
                console._handle_submit_recursive(argparse.Namespace(filepath=os.path.abspath(root), worker=False))
        reports = [call.args[0] for call in console.client.addReport.call_args_list]
        self.assertEqual(3, len(reports))
        # a file in the root itself has no family folder
        self.assertEqual({("win.family", "1.0"), ("", "")}, {(report.family, report.version) for report in reports})


def writeFile(path, content):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fout:
        fout.write(content)


def consoleWithSamples(*sample_entries):
    console = McritConsole.__new__(McritConsole)
    console.client = MagicMock()
    console.client.getSamples.return_value = {entry.sample_id: entry for entry in sample_entries}
    console.client.getSampleBySha256.side_effect = lambda sha256: next((entry for entry in sample_entries if entry.sha256 == sha256), None)
    return console


def sampleEntry(sample_id, content, family, version):
    return MagicMock(sample_id=sample_id, sha256=hashlib.sha256(content).hexdigest(), family=family, version=version, is_library=False)


def unnamedReport():
    # the example report with an empty family and version, as SMDA writes one it was given neither for
    with open(REPORT_PATH, "rb") as fin:
        return fin.read().replace(b'"example_family"', b'""').replace(b'"example_version"', b'""')


class TestSubmitConsistency(unittest.TestCase):
    def testDirModeNamesTheSkippedFile(self):
        with tempfile.TemporaryDirectory() as root:
            filepath = os.path.join(root, "a.bin")
            writeFile(filepath, b"a")
            console = consoleWithSamples(sampleEntry(7, b"a", "family", "1.0"))
            output = io.StringIO()
            with redirect_stdout(output):
                console._handle_submit_dir(argparse.Namespace(filepath=root, worker=False, smda=False, force_update=False))
        # it named the folder given, not the file
        self.assertIn(f"SKIPPING: {filepath} - already in MCRIT.", output.getvalue())

    def testEveryModeSkipsAReportWithoutFunctions(self):
        # dir mode already did; file, recursive and malpedia mode added an empty sample
        with tempfile.TemporaryDirectory() as tmp:
            malpedia_root = os.path.join(tmp, "malpedia")
            writeFile(os.path.join(malpedia_root, "malpedia.bib"), b"")
            empty_path = os.path.join(malpedia_root, "win.family", "1.0", SHA256 + "_unpacked")
            full_path = os.path.join(malpedia_root, "win.family", "1.0", "b" * 64 + "_unpacked")
            writeFile(empty_path, b"empty")
            writeFile(full_path, b"full")
            # SMDA finds no functions in the one file and some in the other
            reports = {empty_path: MagicMock(xcfg={}), full_path: MagicMock(xcfg={0x401000: MagicMock()})}
            arguments = {
                "file": argparse.Namespace(filepath=empty_path, smda=False, force_update=False),
                "dir": argparse.Namespace(filepath=os.path.dirname(empty_path), worker=False, smda=False, force_update=False),
                # the family folder, as the Malpedia root also holds malpedia.bib
                "recursive": argparse.Namespace(filepath=os.path.join(malpedia_root, "win.family"), worker=False, force_update=False),
                "malpedia": argparse.Namespace(filepath=malpedia_root, worker=False),
            }
            for mode, args in arguments.items():
                console = consoleWithSamples()
                output = io.StringIO()
                with (
                    self.subTest(mode=mode),
                    redirect_stdout(output),
                    patch("mcrit.client.McritConsole.getSmdaReportFromFilepath", side_effect=lambda args, filepath: reports[filepath]),
                ):
                    getattr(console, f"_handle_submit_{mode}")(args)
                    added = [call.args[0] for call in console.client.addReport.call_args_list]
                    self.assertNotIn(reports[empty_path], added)
                    self.assertIn(f"SKIPPING: {empty_path} - SMDA found no functions.", output.getvalue())
                    # file mode was only given the empty one
                    self.assertEqual([] if mode == "file" else [reports[full_path]], added)

    def testRecursiveModeForceUpdatesFromTheFolders(self):
        with tempfile.TemporaryDirectory() as root:
            writeFile(os.path.join(root, "win.family", "1.0", "a.bin"), b"a")
            writeFile(os.path.join(root, "b.bin"), b"b")
            writeFile(os.path.join(root, "win.other", "c.bin"), b"c")
            known = (sampleEntry(7, b"a", "win.old", "0.1"), sampleEntry(8, b"b", "win.kept", "2.0"), sampleEntry(9, b"c", "win.wrong", "3.0"))
            # what no folder names, MCRIT keeps: both for b.bin in the root, the version for c.bin
            expected = {
                (False, False): [],
                (True, False): [call(7, family_name="win.family", version="1.0", is_library=False), call(9, family_name="win.other", version="3.0", is_library=False)],
                (True, True): [
                    call(7, family_name="win.family", version="1.0", is_library=True),
                    call(8, family_name="win.kept", version="2.0", is_library=True),
                    call(9, family_name="win.other", version="3.0", is_library=True),
                ],
            }
            for (force_update, library), calls in expected.items():
                console = consoleWithSamples(*known)
                output = io.StringIO()
                args = argparse.Namespace(filepath=root, worker=False, force_update=force_update, family=None, version=None, library=library)
                with self.subTest(force_update=force_update, library=library), redirect_stdout(output):
                    console._handle_submit_recursive(args)
                    console.client.addReport.assert_not_called()
                    self.assertCountEqual(calls, console.client.modifySample.call_args_list)
                    if not force_update:
                        self.assertEqual(3, output.getvalue().count("- already in MCRIT."))

    def testFileAndDirModeForceUpdateFromTheCommandLine(self):
        # a file other than a report takes what --family, --version and --library give, and None keeps MCRIT's
        with tempfile.TemporaryDirectory() as root:
            filepath = os.path.join(root, "a.bin")
            writeFile(filepath, b"a")
            for mode, family, version, library, update in (
                ("file", "win.new", "1.0", True, call(7, family_name="win.new", version="1.0", is_library=True)),
                ("file", None, "1.0", False, call(7, family_name="win.old", version="1.0", is_library=False)),
                ("dir", "win.new", "1.0", True, call(7, family_name="win.new", version="1.0", is_library=True)),
                ("dir", None, "1.0", False, call(7, family_name="win.old", version="1.0", is_library=False)),
            ):
                console = consoleWithSamples(sampleEntry(7, b"a", "win.old", "0.9"))
                args = argparse.Namespace(
                    filepath=filepath if mode == "file" else root, mode=mode, worker=False, smda=False, force_update=True, family=family, version=version, library=library
                )
                with self.subTest(mode=mode, family=family, library=library), redirect_stdout(io.StringIO()):
                    getattr(console, f"_handle_submit_{mode}")(args)
                    console.client.addReport.assert_not_called()
                    self.assertEqual([update], console.client.modifySample.call_args_list)

    def testFileModeKnowsAReportByItsSample(self):
        # with --smda it compared the report file's own sha256, so it never found the sample in MCRIT
        known = MagicMock(sample_id=7, sha256=REPORT_SHA256, family="win.old", version="0.9", is_library=False)
        with tempfile.TemporaryDirectory() as root:
            unnamed_path = os.path.join(root, "unnamed.smda")
            writeFile(unnamed_path, unnamedReport())
            for report_path, force_update, family, version, library, update in (
                (REPORT_PATH, False, None, None, False, None),
                # a report updates its sample to what it would have been submitted as
                (REPORT_PATH, True, None, None, False, call(7, family_name="example_family", version="example_version", is_library=False)),
                (REPORT_PATH, True, "win.new", None, False, call(7, family_name="win.new", version="example_version", is_library=False)),
                (REPORT_PATH, True, None, "2.0", True, call(7, family_name="example_family", version="2.0", is_library=True)),
                # what neither the report nor the command line names, MCRIT keeps
                (REPORT_PATH, True, None, "", False, call(7, family_name="example_family", version="0.9", is_library=False)),
                (unnamed_path, True, None, None, False, None),
                (unnamed_path, True, "win.new", None, False, call(7, family_name="win.new", version="0.9", is_library=False)),
            ):
                console = consoleWithSamples(known)
                output = io.StringIO()
                args = argparse.Namespace(filepath=report_path, mode="file", smda=True, force_update=force_update, family=family, version=version, library=library, output=None)
                with (
                    self.subTest(report=os.path.basename(report_path), force_update=force_update, family=family, version=version, library=library),
                    redirect_stdout(output),
                ):
                    console._handle_submit_file(args)
                    console.client.addReport.assert_not_called()
                    self.assertEqual([update] if update else [], console.client.modifySample.call_args_list)
                    if not force_update:
                        self.assertIn(f"SKIPPING: {report_path} - already in MCRIT.", output.getvalue())
        # a report of a sample MCRIT lacks is still added
        console = consoleWithSamples()
        args = argparse.Namespace(filepath=REPORT_PATH, mode="file", smda=True, force_update=False, family="win.new", version=None, library=False, output=None)
        with redirect_stdout(io.StringIO()):
            console._handle_submit_file(args)
        console.client.addReport.assert_called_once()
        report = console.client.addReport.call_args.args[0]
        self.assertEqual((REPORT_SHA256, "win.new"), (report.sha256, report.family))

    def testDirModeForceUpdatesFromAReport(self):
        # it only skipped a known report's sample, warning when the report named another family or version
        known = MagicMock(sample_id=7, sha256=REPORT_SHA256, family="win.old", version="0.9", is_library=False)
        with tempfile.TemporaryDirectory() as root:
            with open(REPORT_PATH, "rb") as fin:
                writeFile(os.path.join(root, "named.smda"), fin.read())
            writeFile(os.path.join(root, "unnamed", "unnamed.smda"), unnamedReport())
            for folder, force_update, family, version, library, update in (
                ("", False, None, None, False, None),
                ("", True, None, None, False, call(7, family_name="example_family", version="example_version", is_library=False)),
                ("", True, "win.new", None, False, call(7, family_name="win.new", version="example_version", is_library=False)),
                ("", True, None, "2.0", True, call(7, family_name="example_family", version="2.0", is_library=True)),
                # what neither the report nor the command line names, MCRIT keeps
                ("", True, None, "", False, call(7, family_name="example_family", version="0.9", is_library=False)),
                ("unnamed", True, None, None, False, None),
                ("unnamed", True, "win.new", None, False, call(7, family_name="win.new", version="0.9", is_library=False)),
            ):
                console = consoleWithSamples(known)
                output = io.StringIO()
                args = argparse.Namespace(filepath=os.path.join(root, folder), worker=False, smda=True, force_update=force_update, family=family, version=version, library=library)
                with self.subTest(folder=folder, force_update=force_update, family=family, version=version, library=library), redirect_stdout(output):
                    console._handle_submit_dir(args)
                    console.client.addReport.assert_not_called()
                    self.assertEqual([update] if update else [], console.client.modifySample.call_args_list)
                    if not force_update:
                        self.assertIn("WARNING/SKIPPING", output.getvalue())

    def testDirModeWorkerGetsForceUpdate(self):
        # dir mode leaves a file already in MCRIT to its worker, which never got --force_update
        process = MagicMock()
        process.communicate.return_value = (b"", b"")

        def workerCommand(**arguments):
            args = dict(
                mode="dir",
                force_update=False,
                server=None,
                apitoken=None,
                family=None,
                version=None,
                library=False,
                executables_only=False,
                output=None,
                smda=False,
                worker_timeout=1,
            )
            args.update(arguments)
            with patch("mcrit.client.McritConsole.subprocess.Popen", return_value=process) as popen:
                submitViaSubprocess(argparse.Namespace(**args), "sample.bin")
            command = popen.call_args.args[0]
            self.assertEqual("sample.bin", command[-1])
            return command

        for mode, force_update, passed in (("dir", True, True), ("dir", False, False), ("recursive", True, False), ("malpedia", True, False)):
            with self.subTest(mode=mode, force_update=force_update):
                command = workerCommand(mode=mode, force_update=force_update)
                self.assertEqual(passed, "--force_update" in command)
                self.assertNotIn("--family", command)
                self.assertNotIn("--version", command)
        # an empty --family or --version was dropped, where dir mode without a worker applies it
        command = workerCommand(force_update=True, family="", version="")
        self.assertEqual(["--family", ""], command[command.index("--family") :][:2])
        self.assertEqual(["--version", ""], command[command.index("--version") :][:2])


if __name__ == "__main__":
    unittest.main()
