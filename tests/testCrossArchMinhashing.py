#!/usr/bin/python

# Cross-architecture integration test for the architecture-aware shinglers.
#
# This exercises the full pipeline (SmdaReport -> MinHashIndex -> Worker.calculateMinHashes
# -> ShingleLoader -> EscapedBlockShingler / FuzzyStatPairShingler -> storage -> MatcherSample)
# for AArch64 and CIL samples, verifying that:
#   * minhashes are computed with the correct per-architecture escaper (not a hardcoded Intel one),
#   * a near-duplicate of the same architecture yields MINHASH-only matches (score < 100.0),
#   * a PicHash match (score == 100.0) is also detected where expected,
#   * an Intel sample and an AArch64 sample do NOT match via minhash (known limitation until
#     cross-architecture matching is gated - documented here as current behavior).
#
# Fixtures are the SMDA reports you used for validation. Until they are in place under
# tests/fixtures/, the tests skip with a clear message. The expected match offsets below are
# placeholders to be filled in from the reports.

import logging
import os
import unittest

from smda.aarch64.AArch64InstructionEscaper import AArch64InstructionEscaper
from smda.cil.CilInstructionEscaper import CilInstructionEscaper
from smda.common.SmdaReport import SmdaReport
from smda.intel.IntelInstructionEscaper import IntelInstructionEscaper

from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.matchers.MatcherFlags import IS_MINHASH_FLAG, IS_PICHASH_FLAG
from mcrit.matchers.MatcherSample import MatcherSample

from .context import config

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logging.disable(logging.CRITICAL)

THIS_FILE_PATH = str(os.path.abspath(__file__))
# walk up from this file's directory until we find the repo root (marked by pyproject.toml)
_repo_dir = os.path.dirname(THIS_FILE_PATH)
while not os.path.isfile(os.path.join(_repo_dir, "pyproject.toml")):
    parent = os.path.dirname(_repo_dir)
    if parent == _repo_dir:
        break
    _repo_dir = parent
PROJECT_ROOT = _repo_dir
FIXTURE_DIR = os.path.join(PROJECT_ROOT, "tests", "fixtures")


def _report_path(name):
    return os.path.join(FIXTURE_DIR, name)


# Placeholder fixture names - replace with the actual SMDA report filenames once provided.
AARCH64_REPORT_A = _report_path("crossarch_aarch64_a.smda")
AARCH64_REPORT_B = _report_path("crossarch_aarch64_b.smda")
CIL_REPORT_A = _report_path("crossarch_cil_a.smda")
CIL_REPORT_B = _report_path("crossarch_cil_b.smda")
INTEL_REPORT = _report_path("crossarch_intel_a.smda")

# Function offsets (within report A) that you confirmed as:
#   * minhash-only matches (score < 100.0) against report B of the same architecture
#   * pichash matches (score == 100.0) against report B of the same architecture
# AArch64: flexibleferret (A) vs frostyferret (B).
AARCH64_MINHASH_ONLY_OFFSETS = [
    0x100003438,  # score 67.1875 (confirmed validation offset; 70.3125 before AArch64 frame sizes, #238)
    0x100003684,  # score 62.5 (60.9375 before)
    0x100003A74,  # score 54.6875
    0x100004CC8,  # score 75.0
    # 0x100004DC4 scored 57.8125 before; with its frame size it scores 56.25 but shares one band with its
    # partner instead of two, below BAND_MATCHES_REQUIRED, so it is no longer a candidate (#238)
]
AARCH64_PICHASH_OFFSETS = [
    0x1000039A8,  # score 100.0, flags = MINHASH + PICHASH
    0x100004510,  # score 100.0
    0x1000045AC,  # score 100.0
]
# CIL: crossarch_cil_a.smda vs crossarch_cil_b.smda (minhash-only only; no pichash matches observed).
CIL_MINHASH_ONLY_OFFSETS = [
    0x470,  # score 57.8125
    0x970,  # score 78.125
    0x107C,  # score 54.6875
    0x2A3C,  # score 68.75
]
CIL_PICHASH_OFFSETS = []


def _have_fixtures():
    return all(os.path.isfile(p) for p in (AARCH64_REPORT_A, AARCH64_REPORT_B, CIL_REPORT_A, CIL_REPORT_B, INTEL_REPORT))


def _expected_escaper(architecture):
    return {
        "intel": IntelInstructionEscaper,
        "aarch64": AArch64InstructionEscaper,
        "cil": CilInstructionEscaper,
    }[architecture]


class CrossArchMinHashingTestSuite(unittest.TestCase):
    """Full-pipeline minhash matching for AArch64 and CIL samples."""

    @classmethod
    def setUpClass(cls):
        if not _have_fixtures():
            raise unittest.SkipTest(
                "Cross-arch fixtures not present under tests/fixtures/ yet "
                "(expected: crossarch_aarch64_a.smda, crossarch_aarch64_b.smda, "
                "crossarch_cil_a.smda, crossarch_cil_b.smda, crossarch_intel_a.smda)."
            )

    def _assert_minhashes_use_escaper(self, report, architecture):
        escaper = _expected_escaper(architecture)
        index = MinHashIndex(config=config)
        worker = index.queue._worker
        sample_entry = index._storage.addSmdaReport(report)
        worker.updateMinHashesForSample(sample_entry.sample_id)
        function_entries = index._storage.getFunctionsBySampleId(sample_entry.sample_id)
        self.assertTrue(function_entries, "expected functions to be hashed")
        from smda.common.BinaryInfo import BinaryInfo
        from smda.common.SmdaFunction import SmdaFunction

        binary_info = BinaryInfo(b"")
        binary_info.architecture = architecture
        for fe in function_entries:
            # the function entry stores the xcfg; reconstruct to inspect the escaper
            smda_function = SmdaFunction.fromDict(fe.xcfg, binary_info=binary_info)
            self.assertIs(smda_function._escaper, escaper)

    def _submit_pair_and_match(self, report_a, report_b):
        index = MinHashIndex(config=config)
        worker = index.queue._worker
        entry_a = index._storage.addSmdaReport(report_a)
        entry_b = index._storage.addSmdaReport(report_b)
        worker.updateMinHashesForSample(entry_a.sample_id)
        worker.updateMinHashesForSample(entry_b.sample_id)
        matcher = MatcherSample(worker)
        result = matcher.getMatchesForSample(entry_a.sample_id)
        return entry_a, entry_b, result

    def _matches_for_offset(self, result, sample_id, offset):
        # match tuple layout: (foreign_family_id, foreign_sample_id, foreign_function_id, score, flags)
        for function_data in result["matches"]["functions"]:
            if function_data["offset"] == offset:
                return [m for m in function_data["matches"] if m[1] == sample_id]
        return []

    def test_aarch64_minhash_uses_aarch64_escaper(self):
        report = SmdaReport.fromFile(AARCH64_REPORT_A)
        assert report is not None
        self._assert_minhashes_use_escaper(report, "aarch64")

    def test_cil_minhash_uses_cil_escaper(self):
        report = SmdaReport.fromFile(CIL_REPORT_A)
        assert report is not None
        self._assert_minhashes_use_escaper(report, "cil")

    def test_aarch64_near_duplicate_minhash_and_pichash_matches(self):
        report_a = SmdaReport.fromFile(AARCH64_REPORT_A)
        assert report_a is not None
        report_b = SmdaReport.fromFile(AARCH64_REPORT_B)
        assert report_b is not None
        entry_a, entry_b, result = self._submit_pair_and_match(report_a, report_b)

        for offset in AARCH64_MINHASH_ONLY_OFFSETS:
            matches = self._matches_for_offset(result, entry_b.sample_id, offset)
            self.assertTrue(matches, f"expected a minhash match for AArch64 offset {hex(offset)}")
            for m in matches:
                self.assertEqual(m[4] & IS_MINHASH_FLAG, IS_MINHASH_FLAG)
                self.assertLess(m[3], 100.0)

        for offset in AARCH64_PICHASH_OFFSETS:
            matches = self._matches_for_offset(result, entry_b.sample_id, offset)
            self.assertTrue(matches, f"expected a pichash match for AArch64 offset {hex(offset)}")
            for m in matches:
                self.assertEqual(m[4] & IS_PICHASH_FLAG, IS_PICHASH_FLAG)
                self.assertEqual(m[3], 100.0)

    def test_cil_near_duplicate_minhash_and_pichash_matches(self):
        report_a = SmdaReport.fromFile(CIL_REPORT_A)
        assert report_a is not None
        report_b = SmdaReport.fromFile(CIL_REPORT_B)
        assert report_b is not None
        entry_a, entry_b, result = self._submit_pair_and_match(report_a, report_b)

        for offset in CIL_MINHASH_ONLY_OFFSETS:
            matches = self._matches_for_offset(result, entry_b.sample_id, offset)
            self.assertTrue(matches, f"expected a minhash match for CIL offset {hex(offset)}")
            for m in matches:
                self.assertEqual(m[4] & IS_MINHASH_FLAG, IS_MINHASH_FLAG)
                self.assertLess(m[3], 100.0)

        for offset in CIL_PICHASH_OFFSETS:
            matches = self._matches_for_offset(result, entry_b.sample_id, offset)
            self.assertTrue(matches, f"expected a pichash match for CIL offset {hex(offset)}")
            for m in matches:
                self.assertEqual(m[4] & IS_PICHASH_FLAG, IS_PICHASH_FLAG)
                self.assertEqual(m[3], 100.0)

    def test_no_cross_architecture_minhash_match(self):
        intel_report = SmdaReport.fromFile(INTEL_REPORT)
        assert intel_report is not None
        aarch64_report = SmdaReport.fromFile(AARCH64_REPORT_A)
        assert aarch64_report is not None
        index = MinHashIndex(config=config)
        worker = index.queue._worker
        intel_entry = index._storage.addSmdaReport(intel_report)
        aarch64_entry = index._storage.addSmdaReport(aarch64_report)
        worker.updateMinHashesForSample(intel_entry.sample_id)
        worker.updateMinHashesForSample(aarch64_entry.sample_id)
        matcher = MatcherSample(worker)
        result = matcher.getMatchesForSample(intel_entry.sample_id)
        # the only expected matches (if any) should be within the intel sample, never vs AArch64
        for function_data in result["matches"]["functions"]:
            for m in function_data["matches"]:
                self.assertNotEqual(m[0], aarch64_entry.sample_id)


if __name__ == "__main__":
    unittest.main()
