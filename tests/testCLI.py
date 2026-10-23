#!/usr/bin/python

import argparse
import logging
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from mcrit.client.McritConsole import McritConsole, get_base_addr, get_primary_smda_meta_data, is_smda_report

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logging.disable(logging.CRITICAL)

SHA256 = "a" * 64


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


if __name__ == "__main__":
    unittest.main()
