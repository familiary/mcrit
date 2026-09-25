#!/usr/bin/python

import contextlib
import io
import logging
import os
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from mcrit.client.McritConsole import McritConsole, get_primary_smda_meta_data, getSmdaReportFromFilepath, is_smda_report, submitViaSubprocess

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logging.disable(logging.CRITICAL)


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

    def testRecursiveSubmitsFiles(self):
        console = McritConsole()
        console.client = MagicMock()
        console.client.getSamples.return_value = {}
        with tempfile.TemporaryDirectory() as tmp_dir:
            sample_dir = os.sep.join([tmp_dir, "some_family", "1.0"])
            os.makedirs(sample_dir)
            sample_path = os.sep.join([sample_dir, "some_sample"])
            with open(sample_path, "wb") as fout:
                fout.write(b"MZ\x90\x00")
            args = console.parser.parse_args(["client", "submit", "--mode", "recursive", tmp_dir])
            args.filepath = tmp_dir
            with patch("mcrit.client.McritConsole.getSmdaReportFromFilepath") as mock_get_report:
                mock_get_report.return_value = MagicMock(family="", version="")
                console._handle_submit_recursive(args)
            mock_get_report.assert_called_once_with(args, sample_path)
            console.client.addReport.assert_called_once()

    def _submitArgs(self, argv):
        console = McritConsole()
        console.client = MagicMock()
        console.client.getSamples.return_value = {}
        return console, console.parser.parse_args(["client", "submit"] + argv)

    def testIdaRejectsSmdaReportMode(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            console, args = self._submitArgs(["--disassembler", "ida", "--smda", tmp_dir, "--mode", "dir"])
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                console._handle_submit(args)
            self.assertIn("not compatible with SMDA report loading", stdout.getvalue())
            console.client.getSamples.assert_not_called()

    def testIdaSigsRequireIdaDisassembler(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            console, args = self._submitArgs(["--ida-sigs", tmp_dir, tmp_dir, "--mode", "dir"])
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                console._handle_submit(args)
            self.assertIn("only work with disassembler <ida>", stdout.getvalue())
            console.client.getSamples.assert_not_called()

    def testIdaForcesWorkerForDirMode(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            console, args = self._submitArgs(["--disassembler", "ida", tmp_dir, "--mode", "dir"])
            self.assertFalse(args.worker)
            with patch("mcrit.client.McritConsole.is_ida_available", return_value=True), contextlib.redirect_stdout(io.StringIO()):
                console._handle_submit(args)
            self.assertTrue(args.worker)

    def testSubprocessCommandForwardsIdaOptions(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _, args = self._submitArgs(["--disassembler", "ida", "--ida-sigs", tmp_dir, "--ida-sig-min-matches", "25", tmp_dir, "--mode", "dir"])
            with patch("mcrit.client.McritConsole.subprocess.Popen") as mock_popen:
                mock_popen.return_value.communicate.return_value = (b"", b"")
                with contextlib.redirect_stdout(io.StringIO()):
                    submitViaSubprocess(args, "/some/sample")
            command = mock_popen.call_args[0][0]
            self.assertEqual(command[0], sys.executable)
            self.assertEqual(command[command.index("--disassembler") + 1], "ida")
            self.assertEqual(command[command.index("--ida-sigs") + 1], tmp_dir)
            self.assertEqual(command[command.index("--ida-sig-min-matches") + 1], "25")
            self.assertEqual(command[-1], "/some/sample")

    def testSubprocessCommandParsesWithServerAndToken(self):
        console = McritConsole()
        args = console.parser.parse_args(["client", "--server", "http://127.0.0.1:1", "--apitoken", "token", "submit", "/some/dir", "--mode", "dir", "-f", "some_family"])
        with patch("mcrit.client.McritConsole.subprocess.Popen") as mock_popen:
            mock_popen.return_value.communicate.return_value = (b"", b"")
            with contextlib.redirect_stdout(io.StringIO()):
                submitViaSubprocess(args, "/some/sample")
        reparsed = console.parser.parse_args(mock_popen.call_args[0][0][3:])
        self.assertEqual(reparsed.server, "http://127.0.0.1:1")
        self.assertEqual(reparsed.apitoken, "token")
        self.assertEqual(reparsed.family, "some_family")
        self.assertEqual(reparsed.filepath, "/some/sample")

    def testSubprocessIsKilledOnTimeout(self):
        _, args = self._submitArgs(["/some/dir", "--mode", "dir"])
        with patch("mcrit.client.McritConsole.subprocess.Popen") as mock_popen:
            mock_popen.return_value.communicate.side_effect = [subprocess.TimeoutExpired("mcrit", 1), (b"", b"")]
            with contextlib.redirect_stdout(io.StringIO()):
                submitViaSubprocess(args, "/some/sample")
        mock_popen.return_value.kill.assert_called_once()

    def testGetSmdaReportViaIda(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _, args = self._submitArgs(["--disassembler", "ida", "--ida-sigs", tmp_dir, "-f", "some_family", "-v", "some_version", tmp_dir])
            report = MagicMock(family="", version="")
            with patch("mcrit.client.McritConsole.produceIdaReport", return_value=report) as mock_produce:
                with contextlib.redirect_stdout(io.StringIO()):
                    result = getSmdaReportFromFilepath(args, "/some/sample")
            mock_produce.assert_called_once_with("/some/sample", tmp_dir, 10)
            self.assertIs(result, report)
            self.assertEqual(report.family, "some_family")
            self.assertEqual(report.version, "some_version")

    def testGetSmdaReportViaIdaSkipsEmptyReport(self):
        _, args = self._submitArgs(["--disassembler", "ida", "/some/sample"])
        with patch("mcrit.client.McritConsole.produceIdaReport", return_value=MagicMock(num_functions=0)):
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                self.assertIsNone(getSmdaReportFromFilepath(args, "/some/sample"))
        self.assertIn("no functions", stdout.getvalue())

    def testIdaWithoutIdaDomainFailsBeforeAnyFile(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            console, args = self._submitArgs(["--disassembler", "ida", tmp_dir, "--mode", "dir"])
            stdout = io.StringIO()
            with patch("mcrit.client.McritConsole.is_ida_available", return_value=False), contextlib.redirect_stdout(stdout):
                console._handle_submit(args)
            self.assertIn('pip install "mcrit[ida]"', stdout.getvalue())
            console.client.getSamples.assert_not_called()

    def testIdaSigMinMatchesHasToBePositive(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            console, args = self._submitArgs(["--disassembler", "ida", "--ida-sig-min-matches", "0", tmp_dir, "--mode", "dir"])
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                console._handle_submit(args)
            self.assertIn("at least 1", stdout.getvalue())
            console.client.getSamples.assert_not_called()


if __name__ == "__main__":
    unittest.main()
