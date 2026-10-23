#!/usr/bin/python

import argparse
import hashlib
import io
import logging
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import MagicMock, call, patch

from mcrit.client.McritConsole import McritConsole, get_base_addr, get_primary_smda_meta_data, is_smda_report, submitViaSubprocess

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logging.disable(logging.CRITICAL)

SHA256 = "a" * 64
REPORT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "example_report_meta.smda")
REPORT_SHA256 = "ae38ff0778fb8dfa1deb17301a15165934312648d232d167cd0c0034c24689e1"


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
