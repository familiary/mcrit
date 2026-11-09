"""A vs-group match of samples whose functions have no PicHash, which FunctionEntry stores as 0.

The other samples' functions are collected by every PicHash of the own sample, while the own side
may leave one out of its grouping (0, for a function without one): such holders must be skipped
rather than looked up, or the match fails with KeyError.
"""

import hashlib
import logging
import unittest
from copy import deepcopy
from unittest.mock import patch

from smda.common.SmdaReport import SmdaReport
from smda.SmdaConfig import SmdaConfig

from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.matchers.MatcherVsGroup import MatcherVsGroup

from .context import config

logging.disable(logging.CRITICAL)


def pichashless_report(name):
    """Two 12-instruction functions, with a current smda_version and no pic_hash: stored with PicHash 0."""
    xcfg = {}
    for index in range(2):
        offset = 0x1000 + index * 0x40
        body = [("mov", "eax, 0x%x" % (index + 1))] + [("add", "eax, %s" % register) for register in ("ebx", "ecx", "edx")] * 3 + [("inc", "eax"), ("ret", "")]
        instructions = [[offset + position, "90", mnemonic, operands] for position, (mnemonic, operands) in enumerate(body)]
        metadata = {"binweight": len(body), "characteristics": None, "confidence": None, "function_name": "", "strongly_connected_components": [], "tfidf": None}
        xcfg[str(offset)] = {"apirefs": {}, "blockrefs": {}, "blocks": {str(offset): instructions}, "inrefs": [], "offset": offset, "outrefs": {}, "metadata": metadata}
    statistics = {key: 0 for key in ("num_api_calls", "num_basic_blocks", "num_failed_functions", "num_failed_instructions", "num_function_calls", "num_instructions")}
    statistics.update({"num_functions": len(xcfg), "num_leaf_functions": len(xcfg), "num_recursive_functions": 0})
    return SmdaReport.fromDict(
        {
            "architecture": "intel",
            "base_addr": 0x1000,
            "binary_size": 0x80,
            "bitness": 32,
            "code_areas": [],
            "confidence_threshold": 0,
            "disassembly_errors": {},
            "execution_time": 0,
            "identified_alignment": 0,
            "message": "",
            "metadata": {"family": "family_%s" % name, "filename": name},
            "sha256": hashlib.sha256(name.encode()).hexdigest(),
            # a current version: smda keeps the absent pic_hash instead of computing one
            "smda_version": SmdaConfig.VERSION,
            "statistics": statistics,
            "status": "ok",
            "timestamp": "2026-10-24T00-00-00",
            "xcfg": xcfg,
        }
    )


class MatcherVsGroupPicHashTest(unittest.TestCase):
    def setUp(self):
        self.index = MinHashIndex(deepcopy(config))
        self.storage = self.index.getStorage()
        self.storage.clearStorage()
        self.worker = self.index.queue._worker
        self.own = self.storage.addSmdaReport(pichashless_report("a")).sample_id
        self.other = self.storage.addSmdaReport(pichashless_report("b")).sample_id
        for sample_id in (self.own, self.other):
            self.worker.updateMinHashesForSample(sample_id)

    def test_functions_without_a_pichash_are_stored_with_0(self):
        stored = {entry.pichash for sample_id in (self.own, self.other) for entry in self.storage.getFunctionsBySampleId(sample_id)}
        self.assertEqual({0}, stored)

    def test_a_pichash_the_own_side_does_not_group_by_is_skipped(self):
        original = MatcherVsGroup._getPicHashMatches

        def without_pichash_0_on_the_own_side(matcher):
            # leaves 0 out of the own grouping, as a function without a PicHash shares no hash
            own_entries = matcher._function_entries
            matcher._function_entries = [entry for entry in own_entries if entry.pichash]
            try:
                return original(matcher)
            finally:
                matcher._function_entries = own_entries

        with patch.object(MatcherVsGroup, "_getPicHashMatches", without_pichash_0_on_the_own_side):
            report = MatcherVsGroup(self.worker).getMatchesForSample(self.own, [self.other])
        self.assertEqual("matcher_vs_group", report["info"]["type"])

    def test_vs_group_of_samples_without_a_pichash(self):
        report = self.worker.getMatchesForSampleVsGroup(self.own, [self.other])
        self.assertEqual("matcher_vs_group", report["info"]["type"])
        self.assertEqual([self.other], [info["sample_id"] for info in report["other_sample_infos"]])


if __name__ == "__main__":
    unittest.main()
