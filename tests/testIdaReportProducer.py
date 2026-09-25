#!/usr/bin/python

import hashlib
import logging
import os
import sys
import tempfile
import types
import unittest
from unittest import mock

from packaging import version

from mcrit.client.IdaReportProducer import applySigs, produceIdaReport, selectCandidateSigs

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logging.disable(logging.CRITICAL)

BUNDLE_FILES = [
    "windows/vs-channels/CRT-Desktop/Microsoft.CRT-Desktop_x64.sig",
    "windows/vs-channels/CRT-Desktop/Microsoft.CRT-Desktop_x86.sig",
    "linux/debian/debian-libc6-dev-amd64.sig",
    "linux/debian/debian-libc6-dev-i386.sig",
    "linux/debian/debian-libc6-dev-armhf.sig",
    "linux/debian/debian-libc6-dev-armel.sig",
    "linux/debian/debian-libffi-dev-arm.sig",
    "linux/ubuntu/ubuntu-libssl-dev-amd64.sig",
    "golang/stdlibs/golang_std_pc_ABI0.sig",
    "golang/stdlibs/golang_std_pc_ABI0Internal.sig",
    "golang/stdlibs/golang_std_arm64_ABI0Internal.sig",
    "rust/rust_bundle_x86_64-pc-windows-msvc.sig",
    "rust/rust_bundle_x86_64-unknown-linux-gnu.sig",
    "rust/rust_bundle_i686-unknown-linux-gnu.sig",
    "rust/rust_bundle_aarch64-apple-darwin.sig",
    "rust/x86_64-unknown-linux-gnu/rust_1.27.1_x86_64-unknown-linux-gnu.sig",
]


class FakeIdaFuncs:
    IDASGN_PLANNED = 0
    IDASGN_CURRENT = 1
    IDASGN_APPLIED = 2
    IDASGN_BADARG = 3

    def __init__(self, matches_by_name, states=None, known_names=None, undo=None):
        self.matches_by_name = matches_by_name
        self.states = states or {}
        self.known_names = set(known_names if known_names is not None else matches_by_name)
        self.names = []
        self.undo = undo

    def plan_to_apply_idasgn(self, path):
        name = os.path.splitext(os.path.basename(path))[0]
        if name in self.known_names:
            self.names.append(name)
            if self.undo is not None:
                self.undo.on_undo = self.names.pop
        return True

    def get_idasgn_qty(self):
        return len(self.names)

    def get_idasgn_desc_with_matches(self, index):
        name = self.names[index]
        return (name, 0, self.matches_by_name[name])

    def calc_idasgn_state(self, index):
        return self.states.get(self.names[index], self.IDASGN_APPLIED)


class FakeIdaUndo:
    def __init__(self):
        self.undo_points = []
        self.undos = 0
        self.on_undo = None

    def create_undo_point(self, prefix, label):
        self.undo_points.append((prefix, label))
        return True

    def perform_undo(self):
        self.undos += 1
        if self.on_undo is not None:
            self.on_undo()
            self.on_undo = None
        return True


class FakeReport:
    def __init__(self):
        self.sha256 = "unset"
        self.filename = "unset"
        self.binary_size = -1
        self.smda_version = "unset"


class FakeInterface:
    def __init__(self):
        self.closed = False

    def getBinary(self):
        return b"\x90\x90"

    def close(self):
        self.closed = True


class SelectCandidateSigsTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.sig_root = self.temp_dir.name
        for relative_path in BUNDLE_FILES:
            path = os.path.join(self.sig_root, relative_path)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as sig_file:
                sig_file.write(b"IDASGN")

    def _names(self, selected):
        return sorted(os.path.basename(path) for path in selected)

    def testPeSelectsWindowsSigsOfItsArchitecture(self):
        selected = selectCandidateSigs(self.sig_root, "Portable executable for AMD64 (PE)", "metapc", 64, False, False)
        self.assertTrue(all(os.path.isabs(path) for path in selected))
        self.assertEqual(["Microsoft.CRT-Desktop_x64.sig"], self._names(selected))
        selected = selectCandidateSigs(self.sig_root, "Portable executable for 80386 (PE)", "metapc", 32, False, False)
        self.assertEqual(["Microsoft.CRT-Desktop_x86.sig"], self._names(selected))

    def testElfX64SelectsAmd64Sigs(self):
        selected = selectCandidateSigs(self.sig_root, "ELF64 for x86-64 (Shared object)", "metapc", 64, False, False)
        self.assertEqual(["debian-libc6-dev-amd64.sig", "ubuntu-libssl-dev-amd64.sig"], self._names(selected))

    def testElfArm32SelectsBothSoftAndHardFloat(self):
        selected = selectCandidateSigs(self.sig_root, "ELF for ARM (Executable)", "ARM", 32, False, False)
        self.assertEqual(["debian-libc6-dev-armel.sig", "debian-libc6-dev-armhf.sig", "debian-libffi-dev-arm.sig"], self._names(selected))

    def testGoAddsGolangSigsToPlatformSigs(self):
        selected = selectCandidateSigs(self.sig_root, "ELF64 for x86-64 (Executable)", "metapc", 64, True, False)
        self.assertEqual(
            ["debian-libc6-dev-amd64.sig", "golang_std_pc_ABI0.sig", "golang_std_pc_ABI0Internal.sig", "ubuntu-libssl-dev-amd64.sig"],
            self._names(selected),
        )

    def testRustMatchesArchAndOs(self):
        selected = selectCandidateSigs(self.sig_root, "Portable executable for AMD64 (PE)", "metapc", 64, False, True)
        self.assertIn("rust_bundle_x86_64-pc-windows-msvc.sig", self._names(selected))
        self.assertNotIn("rust_bundle_x86_64-unknown-linux-gnu.sig", self._names(selected))
        elf_selected = self._names(selectCandidateSigs(self.sig_root, "ELF32 for 80386 (Executable)", "metapc", 32, False, True))
        self.assertIn("rust_bundle_i686-unknown-linux-gnu.sig", elf_selected)
        self.assertNotIn("rust_1.27.1_x86_64-unknown-linux-gnu.sig", elf_selected)
        macho_selected = self._names(selectCandidateSigs(self.sig_root, "Mach-O file (ARM64)", "ARM", 64, False, True))
        self.assertEqual(["rust_bundle_aarch64-apple-darwin.sig"], macho_selected)

    def testUnknownYieldsNothing(self):
        self.assertEqual([], selectCandidateSigs(self.sig_root, "Binary file", "MIPS", 32, True, True))


class ApplySigsTest(unittest.TestCase):
    def _runApplySigs(self, sig_paths, min_matches, ida_funcs, ida_undo):
        ida_auto = types.SimpleNamespace(auto_wait=lambda: True)
        modules = {"ida_funcs": ida_funcs, "ida_auto": ida_auto, "ida_undo": ida_undo}
        with mock.patch.dict(sys.modules, modules):
            return applySigs(sig_paths, min_matches)

    def testBelowThresholdIsUndone(self):
        ida_funcs = FakeIdaFuncs({"weak": 3})
        ida_undo = FakeIdaUndo()
        kept = self._runApplySigs(["/sigs/weak.sig"], 10, ida_funcs, ida_undo)
        self.assertEqual([], kept)
        self.assertEqual(1, ida_undo.undos)
        self.assertEqual([("mcrit:", "weak.sig")], ida_undo.undo_points)

    def testAboveThresholdIsKept(self):
        ida_funcs = FakeIdaFuncs({"strong": 42})
        ida_undo = FakeIdaUndo()
        kept = self._runApplySigs(["/sigs/strong.sig"], 10, ida_funcs, ida_undo)
        self.assertEqual([("/sigs/strong.sig", 42)], kept)
        self.assertEqual(0, ida_undo.undos)

    def testBadArgIsHandled(self):
        ida_funcs = FakeIdaFuncs({"broken": 0}, states={"broken": FakeIdaFuncs.IDASGN_BADARG})
        ida_undo = FakeIdaUndo()
        kept = self._runApplySigs(["/sigs/broken.sig"], 10, ida_funcs, ida_undo)
        self.assertEqual([], kept)
        self.assertEqual(1, ida_undo.undos)

    def testUnlocatableSignatureIsUndone(self):
        ida_funcs = FakeIdaFuncs({"missing": 100}, known_names=[])
        ida_undo = FakeIdaUndo()
        kept = self._runApplySigs(["/sigs/missing.sig"], 1, ida_funcs, ida_undo)
        self.assertEqual([], kept)
        self.assertEqual(1, ida_undo.undos)

    def testKeptSignaturesSurviveTheUndoOfLaterOnes(self):
        ida_undo = FakeIdaUndo()
        ida_funcs = FakeIdaFuncs({"first": 40, "weak": 2, "last": 15}, undo=ida_undo)
        kept = self._runApplySigs(["/sigs/first.sig", "/sigs/weak.sig", "/sigs/last.sig"], 10, ida_funcs, ida_undo)
        self.assertEqual([("/sigs/first.sig", 40), ("/sigs/last.sig", 15)], kept)
        self.assertEqual(1, ida_undo.undos)
        self.assertEqual(["first", "last"], ida_funcs.names)


class ProduceIdaReportTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.sample_path = os.path.join(self.temp_dir.name, "sample.exe")
        self.sample_content = b"MZ" + b"\x41" * 200
        with open(self.sample_path, "wb") as sample_file:
            sample_file.write(self.sample_content)
        import smda.ida.IdaInterface as ida_interface_module

        self.ida_interface_class = ida_interface_module.IdaInterface
        self.ida_interface_class.instance = None
        self.addCleanup(setattr, self.ida_interface_class, "instance", None)
        self.interface = FakeInterface()
        self.working_dirs = []

    def _fromPath(self, path, save_on_close=False):
        self.working_dirs.append(os.path.dirname(path))
        self.assertTrue(os.path.isfile(path))
        self.assertNotEqual(os.path.dirname(self.sample_path), os.path.dirname(path))
        return self.interface

    def _patchedRun(self, disassembler, sig_root=None):
        with mock.patch.object(self.ida_interface_class, "fromPath", self._fromPath):
            with mock.patch("smda.Disassembler.Disassembler", disassembler):
                return produceIdaReport(self.sample_path, sig_root=sig_root)

    def testReportMetadataComesFromOriginalFile(self):
        class FakeDisassembler:
            def __init__(self, config=None, backend=None):
                pass

            def disassembleBuffer(self, buffer, base_addr):
                return FakeReport()

        report = self._patchedRun(FakeDisassembler)
        self.assertEqual(hashlib.sha256(self.sample_content).hexdigest(), report.sha256)
        self.assertEqual("sample.exe", report.filename)
        self.assertEqual(len(self.sample_content), report.binary_size)
        self.assertTrue(report.smda_version.startswith("MCRIT4IDA"))
        version.parse(report.smda_version.rsplit(" ", 1)[-1])
        self.assertTrue(self.interface.closed)
        self.assertIsNone(self.ida_interface_class.instance)
        self.assertFalse(os.path.exists(self.working_dirs[0]))

    def testCleanupOnDisassemblyFailure(self):
        class FailingDisassembler:
            def __init__(self, config=None, backend=None):
                pass

            def disassembleBuffer(self, buffer, base_addr):
                raise RuntimeError("disassembly exploded")

        with self.assertRaises(RuntimeError):
            self._patchedRun(FailingDisassembler)
        self.assertTrue(self.interface.closed)
        self.assertIsNone(self.ida_interface_class.instance)
        self.assertFalse(os.path.exists(self.working_dirs[0]))

    def testMissingIdaDomainRaisesInstructiveImportError(self):
        def failing_from_path(path, save_on_close=False):
            raise ImportError("ida-domain is not available.")

        with mock.patch.object(self.ida_interface_class, "fromPath", failing_from_path):
            with self.assertRaises(ImportError) as context:
                produceIdaReport(self.sample_path)
        self.assertIn("mcrit[ida]", str(context.exception))
        self.assertIn("IDADIR", str(context.exception))
        self.assertIsNone(self.ida_interface_class.instance)
