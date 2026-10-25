"""Which blocks the unique-blocks cover picks when their scores tie (#283).

With a single sample every unique block of 7 to 11 instructions scores 100.0, and the cover took
whichever of those came last in the dict. Ties are now broken by the fixed hex digits of a block's
YARA string, wildcards not counted, and then by the lowest picblockhash.
"""

import json
import os
import unittest
from unittest.mock import patch

from smda.common.SmdaReport import SmdaReport

from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.storage.UniqueBlocksResult import UniqueBlocksResult, coverPreference

from .context import config

EXAMPLE_REPORT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "example_report.smda")


def block(escaped_sequence, length=8, score=100.0):
    return {"samples": [1], "length": length, "score": score, "instructions": [[0, "90", "nop", ""]] * length, "escaped_sequence": escaped_sequence}


class CoverPreferenceTest(unittest.TestCase):
    def test_wildcards_do_not_count_as_fixed_digits(self):
        self.assertEqual(14, coverPreference("0x10", block("e8???????? 488b4c2420 c3"))[0])

    def test_the_lower_hash_ranks_higher_among_equal_patterns(self):
        self.assertGreater(coverPreference("0x0103", block("c3")), coverPreference("0xfe04", block("c3")))

    def test_hex_and_integer_keys_order_alike(self):
        self.assertEqual(coverPreference(0x10, block("c3")), coverPreference("0x10", block("c3")))
        self.assertEqual(coverPreference(0x10, block("c3")), coverPreference("16", block("c3")))


class TiedBlockCoverTest(unittest.TestCase):
    """UniqueBlocksResult.generateBlockCover, which MCRITweb renders its rules with."""

    BLOCKS = {
        # a call and a ret: almost all wildcard
        "0xff01": block("e8???????? c3"),
        # a stack-cookie check: the call is wildcarded, the rest fixed
        "0xff02": block("488b4c2420 4833cc e8???????? 4883c438 c3"),
        # distinctive: an immediate and a displacement, nothing wildcarded
        "0x0103": block("c7442430efbeadde 488b4c2420 4833cc 4883c438 c3"),
        # as many fixed digits as the one above, with a higher hash
        "0xfe04": block("c7442430efbeadde 488b4c2420 4833cc 4883c438 c3"),
        # longer and lower scored: ties are broken only among equal scores
        "0x0005": block("c7442430efbeadde " * 6, length=14, score=84.0),
    }

    def _cover(self, blocks, required):
        result = UniqueBlocksResult.fromDict({"statistics": {"by_sample_id": {"1": {}}}, "unique_blocks": blocks, "yara_rule": []})
        return result.generateBlockCover(required_per_sample=required)["block_hashes"]

    def test_the_tied_block_with_the_most_fixed_digits_comes_first(self):
        self.assertEqual(["0x0103", "0xfe04", "0xff02", "0xff01", "0x0005"], self._cover(self.BLOCKS, required=5))

    def test_the_order_of_the_blocks_does_not_change_the_cover(self):
        for blocks in (self.BLOCKS, dict(reversed(list(self.BLOCKS.items())))):
            with self.subTest(first=next(iter(blocks))):
                self.assertEqual(["0x0103", "0xfe04"], self._cover(blocks, required=2))


class WorkerTiedBlockCoverTest(unittest.TestCase):
    """Worker.getUniqueBlocks, which picks the cover a unique-blocks job reports."""

    def setUp(self):
        index = MinHashIndex(config)
        self.worker = index.queue._worker
        self.storage = index._storage
        report = SmdaReport.fromFile(EXAMPLE_REPORT)
        assert report is not None
        sample_entry = self.storage.addSmdaReport(report)
        assert sample_entry is not None
        self.sample_id = sample_entry.sample_id

    def _result(self, reverse=False):
        found = self.storage.getUniqueBlocks([self.sample_id])
        if reverse:
            found["unique_blocks"] = dict(reversed(list(found["unique_blocks"].items())))
        with patch.object(self.storage, "getUniqueBlocks", return_value=found):
            return self.worker.getUniqueBlocks([self.sample_id])

    def test_the_cover_does_not_follow_the_order_of_the_blocks(self):
        forward = self._result()
        top = [block_hash for block_hash, entry in forward["unique_blocks"].items() if entry["score"] == 100.0]
        self.assertGreater(len(top), 10, "the sample has too few tied blocks to show anything")
        self.assertEqual(forward["yara_rule"], self._result(reverse=True)["yara_rule"])

    def test_the_cover_takes_the_tied_blocks_with_the_most_fixed_digits(self):
        result = self._result()
        blocks = result["unique_blocks"]
        # the three with the most fixed digits among the 72 blocks tied at 100.0
        self.assertEqual([4226965691128322828, 8778206589652380330, 12890166994811963859], result["yara_rule"][:3])
        ranked = sorted(blocks, key=lambda block_hash: (blocks[block_hash]["score"], coverPreference(block_hash, blocks[block_hash])), reverse=True)
        self.assertEqual(ranked[:10], result["yara_rule"])

    def test_the_rule_mcritweb_renders_picks_the_same_blocks(self):
        result = json.loads(json.dumps(self._result()))
        cover = UniqueBlocksResult.fromDict(result).generateBlockCover()
        self.assertEqual([str(block_hash) for block_hash in result["yara_rule"]], cover["block_hashes"])


if __name__ == "__main__":
    unittest.main()
