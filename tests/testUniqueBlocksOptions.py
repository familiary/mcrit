"""The opt-in directions of #283 for the unique-blocks cover, all off by default.

A gentler length score, a cap on blocks per function, demoting function exits and mostly-wildcard
blocks, and a negative set of picblockhashes. Unless given, they change nothing.
"""

import json
import os
import random
import unittest
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import falcon
import falcon.testing
import pymongo
import pytest
from smda.common.SmdaReport import SmdaReport

from mcrit.client.McritClient import McritClient
from mcrit.config.McritConfig import McritConfig
from mcrit.config.QueueConfig import QueueConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.queue.LocalQueue import Job
from mcrit.queue.QueueFactory import QueueFactory
from mcrit.queue.QueueRemoteCalls import JobProgressReporter
from mcrit.server.BlocksResource import BlocksResource
from mcrit.server.utils import getExcludedPicBlockHashes, getUniqueBlocksParams
from mcrit.storage.StorageFactory import StorageFactory
from mcrit.storage.UniqueBlocksResult import (
    UniqueBlocksProgressReporter,
    UniqueBlocksResult,
    blockHashAsInt,
    coverPreference,
    isGenericBlock,
    lengthScore,
    picBlockHashesOfReport,
    selectBlockCover,
)

from .context import config, getTestMongoServerAndPort

EXAMPLE_REPORT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "example_report.smda")
LIBRARY_REPORT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "library_report.smda")
DB_NAME = "test_unique_blocks_options"

EPILOGUE = [[0, "5f", "pop", "edi"], [1, "5e", "pop", "esi"], [2, "81c448030000", "add", "esp, 0x348"], [8, "c3", "ret", ""]]
COOKIE_CHECK = [
    [0, "488b4c2420", "mov", "rcx, qword ptr [rsp + 0x20]"],
    [5, "4833cc", "xor", "rcx, rsp"],
    [8, "e800000000", "call", "0x1000"],
    [13, "4883c438", "add", "rsp, 0x38"],
    [17, "c3", "ret", ""],
]
DISTINCT = [[0, "b8efbeadde", "mov", "eax, 0xdeadbeef"], [5, "35adde0000", "xor", "eax, 0xdead"], [10, "c3", "ret", ""]]
LOCALS = [[0, "8b45fc", "mov", "eax, dword ptr [ebp - 4]"], [3, "0345f8", "add", "eax, dword ptr [ebp - 8]"], [6, "8945f4", "mov", "dword ptr [ebp - 0xc], eax"]]


def block(instructions, escaped_sequence, function_id=1, score=100.0):
    return {"samples": [1], "length": len(instructions), "score": score, "function_id": function_id, "instructions": instructions, "escaped_sequence": escaped_sequence}


class RecordingReporter:
    def __init__(self):
        self.total = 0
        self.totals = []
        self.count = 0
        self.values = []
        self.polls = 0

    def set_total(self, total):
        self.total = total
        self.totals.append(total)

    def step(self):
        self.count += 1
        self.values.append(self.count / self.total)

    def poll(self):
        self.polls += 1


def assertProgressFinished(test, reporter):
    """One total, set once and reached exactly, in more than one step."""
    test.assertEqual([reporter.total], reporter.totals)
    test.assertEqual(reporter.total, reporter.count)
    test.assertLess(reporter.values[0], 1.0)


class PhaseRecordingReporter(UniqueBlocksProgressReporter):
    """Records the phases in the order they start, each with the progress it starts at and after each of its steps."""

    def __init__(self):
        self.recorder = RecordingReporter()
        super().__init__(self.recorder)
        self.phases = []

    def start_phase(self, name, total):
        super().start_phase(name, total)
        self.phases.append((name, [self.recorder.count]))

    def step(self, count=1):
        super().step(count)
        self.phases[-1][1].append(self.recorder.count)


def assertPhasesProgress(test, progress, names, increments, exact=frozenset()):
    """<names> started in order, each where the earlier ones' shares end, and moving its progress in at least <increments>[name] increments.

    Each phase of <exact> expects as many steps as it takes, so it reaches its share on its last step and not before.
    """
    units = UniqueBlocksProgressReporter._PHASE_UNITS
    test.assertEqual(names, [name for name, _ in progress.phases])
    start = 0
    for name, counts in progress.phases:
        with test.subTest(phase=name):
            test.assertEqual(start, counts[0])
            test.assertLessEqual(max(counts), start + units[name])
            test.assertGreaterEqual(len(set(counts)) - 1, increments.get(name, 0))
            if name in exact:
                test.assertEqual(start + units[name], counts[-1])
                test.assertLess(counts[-2], start + units[name])
        start += units[name]


class LengthScoreTest(unittest.TestCase):
    def test_the_default_is_the_score_storage_always_gave(self):
        for length in range(1, 60):
            with self.subTest(length=length):
                old = 100.0
                if length < 7:
                    old = 100.0 - (100.0 * (7 - length) / 7)
                elif length > 10:
                    old = 100.0 * (1 / (length - 10))
                self.assertEqual(old, lengthScore(length))

    def test_gentle_only_changes_blocks_past_eleven_instructions(self):
        for length in range(1, 12):
            self.assertEqual(lengthScore(length), lengthScore(length, gentle=True))
        self.assertAlmostEqual(91.67, lengthScore(12, gentle=True), places=2)
        self.assertAlmostEqual(55.0, lengthScore(20, gentle=True))
        # by default a 12-instruction block scores below a 4-instruction one, gently it does not
        self.assertLess(lengthScore(12), lengthScore(4))
        self.assertGreater(lengthScore(12, gentle=True), lengthScore(4, gentle=True))


class GenericBlockTest(unittest.TestCase):
    def test_function_exits_are_generic(self):
        self.assertTrue(isGenericBlock(block(EPILOGUE, "5f 5e 81c448030000 c3")))
        self.assertTrue(isGenericBlock(block(COOKIE_CHECK, "488b4c2420 4833cc e8???????? 4883c438 c3")))

    def test_a_return_with_a_prefix_is_a_return(self):
        for mnemonic in ("repz ret", "bnd ret"):
            gcc_exit = [[0, "5b", "pop", "rbx"], [1, "5d", "pop", "rbp"], [2, "f3c3", mnemonic, ""]]
            with self.subTest(mnemonic=mnemonic):
                self.assertTrue(isGenericBlock(block(gcc_exit, "5b 5d f3c3")))

    def test_minus_one_as_return_value_is_generic(self):
        for setter in (["83c8ff", "or", "eax, 0xffffffff"], ["b8ffffffff", "mov", "eax, 0xffffffff"]):
            minus_one = [[0, *setter], [5, "5d", "pop", "ebp"], [6, "c3", "ret", ""]]
            with self.subTest(setter=setter):
                self.assertTrue(isGenericBlock(block(minus_one, f"{setter[0]} 5d c3")))

    def test_an_msvc_x64_exit_restoring_xmm_registers_is_generic(self):
        xmm_exit = [[0, "0f28742430", "movaps", "xmm6, xmmword ptr [rsp + 0x30]"], [5, "4883c448", "add", "rsp, 0x48"], [9, "5f", "pop", "rdi"], [10, "c3", "ret", ""]]
        self.assertTrue(isGenericBlock(block(xmm_exit, "0f28742430 4883c448 5f c3")))

    def test_an_seh_exit_is_generic(self):
        seh_exit = [
            [0, "8b4df4", "mov", "ecx, dword ptr [ebp - 0xc]"],
            [3, "64890d00000000", "mov", "dword ptr fs:[0], ecx"],
            [10, "59", "pop", "ecx"],
            [11, "5f", "pop", "edi"],
            [12, "8be5", "mov", "esp, ebp"],
            [14, "5d", "pop", "ebp"],
            [15, "c3", "ret", ""],
        ]
        self.assertTrue(isGenericBlock(block(seh_exit, "8b4df4 64890d00000000 59 5f 8be5 5d c3")))

    def test_mostly_wildcards_is_generic(self):
        self.assertTrue(isGenericBlock(block([[0, "e800000000", "call", "0x1000"], [5, "c3", "ret", ""]], "e8???????? c3")))

    def test_an_exit_that_sets_the_return_value_is_generic(self):
        exit_with_value = [[0, "5f", "pop", "edi"], [1, "32c0", "xor", "al, al"], [3, "5b", "pop", "ebx"], [4, "c3", "ret", ""]]
        self.assertTrue(isGenericBlock(block(exit_with_value, "5f 32c0 5b c3")))

    def test_a_flag_as_return_value_is_generic(self):
        flag_exit = [[0, "5f", "pop", "edi"], [1, "0f97c0", "seta", "al"], [4, "5b", "pop", "ebx"], [5, "c3", "ret", ""]]
        self.assertTrue(isGenericBlock(block(flag_exit, "5f 0f97c0 5b c3")))

    def test_an_aarch64_exit_is_generic(self):
        aarch64_exit = [[0, "fd7bc1a8", "ldp", "x29, x30, [sp], #0x10"], [4, "c0035fd6", "ret", ""]]
        self.assertTrue(isGenericBlock(block(aarch64_exit, "fd7bc1a8 c0035fd6")))

    def test_constants_and_arithmetic_before_the_return_are_not(self):
        # an LCG step and an XOR key are what a rule should keep, even right before an epilogue
        lcg = [[0, "69c06d4ec641", "imul", "eax, eax, 0x41c64e6d"], [6, "0539300000", "add", "eax, 0x3039"], [11, "5d", "pop", "ebp"], [12, "c3", "ret", ""]]
        self.assertFalse(isGenericBlock(block(lcg, "69c06d4ec641 0539300000 5d c3")))
        xor_key = [[0, "35efbeadde", "xor", "eax, 0xdeadbeef"], [5, "5d", "pop", "ebp"], [6, "c3", "ret", ""]]
        self.assertFalse(isGenericBlock(block(xor_key, "35efbeadde 5d c3")))

    def test_code_of_its_own_is_not(self):
        self.assertFalse(isGenericBlock(block(DISTINCT, "b8efbeadde 35adde0000 c3")))
        # frame-relative reads are ordinary code without a return to make them an exit
        self.assertFalse(isGenericBlock(block(LOCALS, "8b45fc 0345f8 8945f4")))
        # a leaf function reading its arguments through esp restores nothing before returning
        leaf = [[0, "8b442404", "mov", "eax, dword ptr [esp + 4]"], [4, "03442408", "add", "eax, dword ptr [esp + 8]"], [8, "c3", "ret", ""]]
        self.assertFalse(isGenericBlock(block(leaf, "8b442404 03442408 c3")))


def oldBlockCover(blocks, sample_ids, required_per_sample, preference, cap=0, demoted=frozenset()):
    """The cover loop as Worker.getUniqueBlocks had it before selectBlockCover, with the tie-break,
    rescanning every block for each pick; <cap> and <demoted> as selectBlockCover takes them."""
    selected, sample_coverage, samples_covered = [], {sample_id: 0 for sample_id in sample_ids}, set()
    function_picks: Dict[Any, int] = {}
    while True:
        candidates = []
        for block_hash, entry in blocks.items():
            coverable = set(entry["samples"]).difference(samples_covered)
            if cap and entry.get("function_id") is not None and function_picks.get(entry["function_id"], 0) >= cap:
                continue
            if coverable and block_hash not in selected:
                candidates.append(((len(coverable), block_hash not in demoted, entry["score"], preference(block_hash, entry)), block_hash, coverable))
        if len(samples_covered) == len(sample_ids) or not candidates:
            break
        candidates.sort(key=lambda candidate: candidate[0])
        _, block_hash, coverable = candidates.pop()
        selected.append(block_hash)
        function_picks[blocks[block_hash].get("function_id")] = function_picks.get(blocks[block_hash].get("function_id"), 0) + 1
        for sample_id in coverable:
            sample_coverage[sample_id] += 1
        samples_covered = {sample_id for sample_id, count in sample_coverage.items() if count >= required_per_sample}
    return selected, sample_coverage


class SelectBlockCoverTest(unittest.TestCase):
    def test_it_picks_what_the_old_loop_picked(self):
        rng = random.Random(283)
        for case in range(300):
            sample_ids = list(range(1, rng.randint(1, 5) + 1))
            blocks = {}
            for _ in range(rng.randint(0, 25)):
                samples = sorted(rng.sample(sample_ids, rng.randint(1, len(sample_ids))))
                escaped = " ".join(rng.choice(["8b45fc", "e8????????", "c3", "4883c438", "75??"]) for _ in range(rng.randint(1, 6)))
                blocks[rng.getrandbits(64)] = {
                    "samples": samples,
                    "length": rng.randint(1, 20),
                    "score": rng.choice([100.0, 90.0, 84.0]),
                    "function_id": rng.randint(1, 4),
                    "escaped_sequence": escaped,
                }
            required = rng.randint(1, 3)
            with self.subTest(case=case):
                self.assertEqual(oldBlockCover(blocks, sample_ids, required, coverPreference), selectBlockCover(blocks, sample_ids, required))
            cap, demoted = rng.randint(1, 2), {block_hash for block_hash in blocks if rng.random() < 0.5}
            with self.subTest(case=case, cap=cap, demoted=demoted):
                self.assertEqual(
                    oldBlockCover(blocks, sample_ids, required, coverPreference, cap=cap, demoted=demoted),
                    selectBlockCover(blocks, sample_ids, required, max_blocks_per_function=cap, demoted=demoted),
                )

    def test_one_hash_keyed_as_int_and_as_str_does_not_compare_the_keys(self):
        blocks = {16: block(DISTINCT, "b8efbeadde 35adde0000 c3"), "0x10": block(DISTINCT, "b8efbeadde 35adde0000 c3")}
        self.assertEqual([16, "0x10"], selectBlockCover(blocks, [1], 2)[0])

    def test_blocks_without_a_function_are_not_capped(self):
        blocks = {1: block(DISTINCT, "b8efbeadde 35adde0000 c3", function_id=None), 2: block(LOCALS, "8b45fc 0345f8 8945f4", function_id=None)}
        self.assertEqual([1, 2], selectBlockCover(blocks, [1], 2, max_blocks_per_function=1)[0])

    def test_the_cap_takes_one_block_per_function(self):
        blocks = {1: block(DISTINCT, "b8efbeadde 35adde0000 c3", function_id=7), 2: block(LOCALS, "8b45fc 0345f8 8945f4", function_id=7), 3: block(LOCALS, "8b45fc", function_id=8)}
        self.assertEqual([1, 2, 3], selectBlockCover(blocks, [1], 3)[0])
        selected, coverage = selectBlockCover(blocks, [1], 3, max_blocks_per_function=1)
        self.assertEqual([1, 3], selected)
        self.assertEqual({1: 2}, coverage)

    def test_generic_blocks_go_last_when_demoted(self):
        # the epilogue fixes the most digits, so it wins the tie unless demoted
        blocks = {1: block(EPILOGUE, "5f 5e 81c448030000 c3 5f 5e 5d 5b"), 2: block(DISTINCT, "b8efbeadde 35adde0000 c3")}
        self.assertEqual([1, 2], selectBlockCover(blocks, [1], 2)[0])
        self.assertEqual([2, 1], selectBlockCover(blocks, [1], 2, demoted={1})[0])

    def test_demoting_does_not_outrank_reaching_more_samples(self):
        generic = dict(block(EPILOGUE, "5f 5e 81c448030000 c3"), samples=[1, 2])
        blocks = {1: generic, 2: block(DISTINCT, "b8efbeadde 35adde0000 c3")}
        self.assertEqual([1], selectBlockCover(blocks, [1, 2], 1, demoted={1})[0])


class UniqueBlocksProgressTest(unittest.TestCase):
    def test_steps_that_complete_no_unit_poll_for_termination(self):
        reporter = RecordingReporter()
        progress = UniqueBlocksProgressReporter.wrap(reporter)
        # more steps than units, and more than the phase expected: each completes a unit or polls
        progress.start_phase("eliminate", 2500)
        for _ in range(2550):
            progress.step()
        self.assertEqual(UniqueBlocksProgressReporter._PHASE_UNITS["eliminate"], reporter.count)
        self.assertEqual(2550, reporter.count + reporter.polls)
        progress.finish()
        self.assertEqual(reporter.total, reporter.count)

    def test_steps_past_the_expected_count_do_not_leave_the_phase(self):
        reporter = RecordingReporter()
        progress = UniqueBlocksProgressReporter.wrap(reporter)
        progress.start_phase("eliminate", 10)
        for _ in range(30):
            progress.step()
        self.assertEqual(UniqueBlocksProgressReporter._PHASE_UNITS["eliminate"], reporter.count)
        self.assertEqual(20, reporter.polls)

    def test_a_phase_starts_where_the_last_one_ends_however_far_it_got(self):
        reporter = RecordingReporter()
        progress = UniqueBlocksProgressReporter.wrap(reporter)
        units = UniqueBlocksProgressReporter._PHASE_UNITS
        progress.start_phase("collect", 100)
        progress.start_phase("eliminate", 100)
        for _ in range(50):
            progress.step()
        self.assertEqual(units["collect"] + units["eliminate"] // 2, reporter.count)

    def test_a_job_notices_termination_when_polled(self):
        job = MagicMock()
        job._is_terminated.return_value = True
        reporter = JobProgressReporter(job, -1)
        with self.assertRaisesRegex(Exception, "terminat"):
            reporter.poll()
        job._is_terminated.assert_called_once()


class WorkerOptionsTest(unittest.TestCase):
    """Worker.getUniqueBlocks on memory storage with tests/example_report.smda: one sample, 201 unique blocks."""

    def setUp(self):
        self.index = MinHashIndex(config)
        self.worker = self.index.queue._worker
        report = SmdaReport.fromFile(EXAMPLE_REPORT)
        assert report is not None
        self.report = report
        sample_entry = self.index._storage.addSmdaReport(report)
        assert sample_entry is not None
        self.sample_id = sample_entry.sample_id

    def test_explicit_off_values_equal_omitting_them(self):
        default = self.worker.getUniqueBlocks([self.sample_id])
        # the tie-broken cover testUniqueBlocksTieBreak pins
        self.assertEqual([4226965691128322828, 8778206589652380330, 12890166994811963859], default["yara_rule"][:3])
        for off in ({"gentle_length_score": False, "max_blocks_per_function": 0, "demote_generic_blocks": False, "exclude_picblockhashes": []}, {"max_blocks_per_function": None}):
            with self.subTest(off=off):
                self.assertEqual(default, self.worker.getUniqueBlocks([self.sample_id], **off))
        self.assertEqual(0, default["statistics"]["blocks_excluded"])
        self.assertNotIn("generic_blocks", default["statistics"])

    def test_the_job_reports_its_progress_once_through(self):
        reporter = RecordingReporter()
        result = self.worker.getUniqueBlocks([self.sample_id], progress_reporter=reporter)
        self.assertTrue(result["yara_rule"])
        assertProgressFinished(self, reporter)

    def test_storage_called_directly_finishes_its_own_progress(self):
        reporter = RecordingReporter()
        self.index._storage.getUniqueBlocks([self.sample_id], progress_reporter=reporter)
        assertProgressFinished(self, reporter)

    def test_each_phase_reports_its_share(self):
        progress = PhaseRecordingReporter()
        self.worker.getUniqueBlocks([self.sample_id], progress_reporter=progress)
        # memory storage reports each of its phases only once it is done
        assertPhasesProgress(self, progress, ["collect", "eliminate", "score", "extract", "escape", "cover"], {"escape": 2}, {"escape"})

    def test_the_cap_spreads_the_cover_over_functions(self):
        default = self.worker.getUniqueBlocks([self.sample_id])
        blocks = default["unique_blocks"]
        self.assertLess(len({blocks[block_hash]["function_id"] for block_hash in default["yara_rule"]}), 10)
        capped = self.worker.getUniqueBlocks([self.sample_id], max_blocks_per_function=1)
        function_ids = [capped["unique_blocks"][block_hash]["function_id"] for block_hash in capped["yara_rule"]]
        self.assertEqual(len(function_ids), len(set(function_ids)))
        # the sample has unique blocks in 9 functions only, so a cap of 1 cannot reach 10
        self.assertEqual(9, len(function_ids))
        self.assertEqual(1, capped["statistics"]["max_blocks_per_function"])
        self.assertFalse(capped["statistics"]["has_complete_yara_rule"])

    def test_gentle_lets_a_longer_block_beat_a_short_one(self):
        # 72 blocks tie at 100.0, so the 73rd pick is the first that the length score decides
        default = self.worker.getUniqueBlocks([self.sample_id], covers_required=73)
        gentle = self.worker.getUniqueBlocks([self.sample_id], covers_required=73, gentle_length_score=True)
        self.assertEqual(6, default["unique_blocks"][default["yara_rule"][72]]["length"])
        self.assertEqual(12, gentle["unique_blocks"][gentle["yara_rule"][72]]["length"])
        self.assertTrue(gentle["statistics"]["gentle_length_score"])

    def test_demoting_counts_the_generic_blocks(self):
        result = self.worker.getUniqueBlocks([self.sample_id], demote_generic_blocks=True)
        # five function exits, two of which set the return value first (xor al, al and seta al),
        # and push ebp; push edi; push eax; jmp, which is half wildcards
        self.assertEqual(6, result["statistics"]["generic_blocks"])
        self.assertTrue(result["statistics"]["demote_generic_blocks"])

    def test_the_negative_set_reaches_the_worker_through_the_queue(self):
        default = self.index.getResultForJob(self.index.getUniqueBlocks([self.sample_id]))
        excluded = default["yara_rule"][:3]
        job_id = self.index.getUniqueBlocks([self.sample_id], exclude_picblockhashes=excluded)
        result = self.index.getResultForJob(job_id)
        self.assertEqual(3, result["statistics"]["blocks_excluded"])
        self.assertTrue(set(excluded).isdisjoint(int(block_hash) for block_hash in result["unique_blocks"]))
        self.assertEqual(default["yara_rule"][3:], result["yara_rule"][:7])

    def test_a_negative_set_of_the_sample_itself_leaves_nothing(self):
        result = self.worker.getUniqueBlocks([self.sample_id], exclude_picblockhashes=[hex(block_hash) for block_hash in picBlockHashesOfReport(self.report)])
        self.assertEqual(201, result["statistics"]["blocks_excluded"])
        self.assertEqual([], result["yara_rule"])

    def test_a_cover_of_zero_blocks_per_sample_is_one_of_one(self):
        result = UniqueBlocksResult.fromDict(json.loads(json.dumps(self.worker.getUniqueBlocks([self.sample_id]))))
        self.assertEqual(result.generateBlockCover(required_per_sample=1), result.generateBlockCover(required_per_sample=0))

    def test_the_job_of_a_samples_request_has_no_family(self):
        # the job listing used to read the first option given as the family
        job_id = self.index.getUniqueBlocks([self.sample_id], covers_required=3, exclude_picblockhashes=[16])
        self.assertIsNone(self.index.getJob(job_id).family_id)
        family_job_id = self.index.getUniqueBlocks([self.sample_id], family_id=4, demote_generic_blocks=True)
        self.assertEqual(4, self.index.getJob(family_job_id).family_id)
        self.assertIsNone(Job({}, None).family_id)

    def test_a_demoted_block_gives_way_in_the_job_and_in_mcritweb(self):
        # the example has no generic block among its first picks, so one is made generic here
        default = self.worker.getUniqueBlocks([self.sample_id])
        first = default["yara_rule"][0]
        first_sequence = default["unique_blocks"][first]["escaped_sequence"]
        flag_first = lambda entry: entry["escaped_sequence"] == first_sequence  # noqa: E731
        with patch("mcrit.Worker.isGenericBlock", flag_first), patch("mcrit.storage.UniqueBlocksResult.isGenericBlock", flag_first):
            result = json.loads(json.dumps(self.worker.getUniqueBlocks([self.sample_id], demote_generic_blocks=True)))
            cover = UniqueBlocksResult.fromDict(result).generateBlockCover()
        self.assertEqual(1, result["statistics"]["generic_blocks"])
        self.assertEqual(default["yara_rule"][1:], result["yara_rule"][:9])
        self.assertNotIn(first, result["yara_rule"])
        self.assertEqual([str(block_hash) for block_hash in result["yara_rule"]], cover["block_hashes"])

    def test_mcritweb_reads_the_cap_as_the_job_does(self):
        result = UniqueBlocksResult.fromDict(json.loads(json.dumps(self.worker.getUniqueBlocks([self.sample_id]))))
        self.assertEqual(result.generateBlockCover(max_blocks_per_function=0), result.generateBlockCover(max_blocks_per_function=-1))
        self.assertEqual(result.generateBlockCover(max_blocks_per_function=2), result.generateBlockCover(max_blocks_per_function="2"))

    def test_the_rule_mcritweb_renders_keeps_the_options(self):
        result = json.loads(json.dumps(self.worker.getUniqueBlocks([self.sample_id], max_blocks_per_function=1, demote_generic_blocks=True)))
        cover = UniqueBlocksResult.fromDict(result).generateBlockCover()
        self.assertEqual([str(block_hash) for block_hash in result["yara_rule"]], cover["block_hashes"])


@pytest.mark.mongo
class MongoWorkerOptionsTest(unittest.TestCase):
    """MongoDbStorage keys the blocks by "0x" hex, which the options have to handle like the integers."""

    def _worker(self, storage_config):
        mongo_config = McritConfig()
        mongo_config.STORAGE_CONFIG = storage_config
        mongo_config.QUEUE_CONFIG = QueueConfig(QUEUE_METHOD=QueueFactory.QUEUE_METHOD_FAKE)
        index = MinHashIndex(config=mongo_config)
        sample_entry = index._storage.addSmdaReport(SmdaReport.fromFile(EXAMPLE_REPORT))
        assert sample_entry is not None
        return index.queue._worker, sample_entry.sample_id

    def setUp(self):
        self.tearDown()
        server, port = getTestMongoServerAndPort()
        self.mongo = StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB, STORAGE_SERVER=server, STORAGE_PORT=port, STORAGE_MONGODB_DBNAME=DB_NAME)

    def tearDown(self):
        server, port = getTestMongoServerAndPort()
        pymongo.MongoClient(server, int(port)).drop_database(DB_NAME)

    def test_progress_is_reported_once_through_with_and_without_the_index(self):
        worker, sample_id = self._worker(self.mongo)
        for index_complete in (True, False):
            worker._storage._setPicBlockHashIndexComplete(index_complete)
            for name, get_unique_blocks in (("worker", worker.getUniqueBlocks), ("storage", worker._storage.getUniqueBlocks)):
                with self.subTest(index_complete=index_complete, caller=name):
                    reporter = RecordingReporter()
                    get_unique_blocks([sample_id], progress_reporter=reporter)
                    assertProgressFinished(self, reporter)

    def test_each_phase_moves_the_progress_with_and_without_the_index(self):
        worker, sample_id = self._worker(self.mongo)
        for index_complete in (True, False):
            worker._storage._setPicBlockHashIndexComplete(index_complete)
            with self.subTest(index_complete=index_complete):
                progress = PhaseRecordingReporter()
                worker.getUniqueBlocks([sample_id], progress_reporter=progress)
                # the index is read in slices of 20,000 candidates, so one sample's candidates take a single step
                increments = {"collect": 2, "eliminate": 1 if index_complete else 2, "score": 2, "extract": 2, "escape": 2}
                # the collect and scan phases expect a bound of their steps, the others their count
                exact = {"score", "extract", "escape"} | ({"eliminate"} if index_complete else set())
                assertPhasesProgress(self, progress, ["collect", "eliminate", "score", "extract", "escape", "cover"], increments, exact)
                self.assertEqual(progress.recorder.total, progress.recorder.count)

    def test_both_backends_pick_the_same_cover_with_every_option(self):
        options = {"covers_required": 12, "gentle_length_score": True, "max_blocks_per_function": 2, "demote_generic_blocks": True}
        worker, sample_id = self._worker(StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MEMORY))
        memory = worker.getUniqueBlocks([sample_id], **options)
        worker, sample_id = self._worker(self.mongo)
        mongo = worker.getUniqueBlocks([sample_id], **options)
        self.assertEqual(memory["yara_rule"], [blockHashAsInt(block_hash) for block_hash in mongo["yara_rule"]])

    def test_a_block_in_several_functions_counts_against_the_lowest(self):
        # 42 blocks of these two reports sit in more than one function: the cap must charge them to
        # the same function on both backends, whatever order MongoDB answers in
        options = {"covers_required": 50, "max_blocks_per_function": 1}
        results = []
        for storage_config in (StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MEMORY), self.mongo):
            worker, sample_id = self._worker(storage_config)
            library_entry = worker._storage.addSmdaReport(SmdaReport.fromFile(LIBRARY_REPORT))
            assert library_entry is not None
            if storage_config is self.mongo:
                # stored again highest function_id first, so MongoDB answers in that order rather
                # than the one the functions were added in
                functions = worker._storage._getDb().functions
                documents = sorted(functions.find({}), key=lambda document: -document["function_id"])
                functions.delete_many({})
                functions.insert_many(documents)
            result = worker.getUniqueBlocks([sample_id, library_entry.sample_id], **options)
            results.append(
                (
                    {blockHashAsInt(block_hash): entry["function_id"] for block_hash, entry in result["unique_blocks"].items()},
                    [blockHashAsInt(block_hash) for block_hash in result["yara_rule"]],
                )
            )
        self.assertEqual(results[0], results[1])

    def test_the_negative_set_matches_hex_keys(self):
        worker, sample_id = self._worker(self.mongo)
        default = worker.getUniqueBlocks([sample_id])
        self.assertIsInstance(default["yara_rule"][0], str)
        excluded = [blockHashAsInt(block_hash) for block_hash in default["yara_rule"][:3]]
        result = worker.getUniqueBlocks([sample_id], exclude_picblockhashes=excluded)
        self.assertEqual(3, result["statistics"]["blocks_excluded"])
        self.assertEqual(default["yara_rule"][3:], result["yara_rule"][:7])


class UniqueBlocksRequestTest(unittest.TestCase):
    def test_the_options_are_parsed(self):
        parameters = getUniqueBlocksParams({"max_blocks_per_function": "2", "gentle_length_score": "true", "demote_generic_blocks": "True"})
        self.assertEqual({"max_blocks_per_function": 2, "gentle_length_score": True, "demote_generic_blocks": True}, parameters)
        # off is left out, so it makes the same job as omitting it
        self.assertEqual({}, getUniqueBlocksParams({"max_blocks_per_function": "0", "gentle_length_score": "false"}))
        self.assertEqual({}, getUniqueBlocksParams({"max_blocks_per_function": "-1"}))

    def test_the_negative_set_takes_integers_and_hex(self):
        # sorted and without repeats, so the same set always makes the same job
        self.assertEqual([16, 17, 2**64 - 1], getExcludedPicBlockHashes({"exclude_picblockhashes": [hex(2**64 - 1), 17, 16, "0x10", "0x0010"]}))
        self.assertEqual([], getExcludedPicBlockHashes({"exclude_picblockhashes": []}))

    def test_a_malformed_negative_set_is_refused(self):
        for body in (
            None,
            [],
            {},
            {"exclude_picblockhashes": "0x10"},
            {"exclude_picblockhashes": [True]},
            {"exclude_picblockhashes": ["abc"]},
            {"exclude_picblockhashes": [-1]},
            {"exclude_picblockhashes": [2**64]},
            {"exclude_picblockhashes": ["1_0"]},
            {"exclude_picblockhashes": [" 0x10 "]},
            {"exclude_picblockhashes": ["016"]},
            {"exclude_picblockhashes": ["1" * 5000]},
            {"exclude_picblockhashes": [16], "covers_required": 3},
        ):
            with self.subTest(body=body), self.assertRaises(ValueError):
                getExcludedPicBlockHashes(body)

    @staticmethod
    def _post(path, body, query_string=""):
        return falcon.Request(
            falcon.testing.create_environ(path=path, query_string=query_string, method="POST", body=json.dumps(body), headers={"Content-Type": "application/json"})
        )

    def test_the_post_route_passes_the_negative_set_on(self):
        index = MagicMock()
        index.getUniqueBlocks.return_value = {"statistics": {}, "unique_blocks": {}}
        resp = falcon.Response()
        BlocksResource(index).on_post_unique_blocks_for_samples(self._post("/uniqueblocks/samples/1", {"exclude_picblockhashes": ["0x10"]}, "covers_required=3"), resp, "1")
        index.getUniqueBlocks.assert_called_once_with([1], username=None, covers_required=3, exclude_picblockhashes=[16])

    def test_an_empty_negative_set_makes_the_job_of_a_get(self):
        index = MagicMock()
        index.getUniqueBlocks.return_value = {}
        BlocksResource(index).on_post_unique_blocks_for_family(self._post("/uniqueblocks/family/4", {"exclude_picblockhashes": []}), falcon.Response(), 4)
        index.getUniqueBlocks.assert_called_once_with([], family_id=4, username=None)

    def test_a_refusal_does_not_echo_a_long_value(self):
        index = MagicMock()
        resp = falcon.Response()
        BlocksResource(index).on_post_unique_blocks_for_samples(self._post("/uniqueblocks/samples/1", {"exclude_picblockhashes": ["9" * 5000]}), resp, "1")
        self.assertEqual(falcon.HTTP_400, resp.status)
        assert resp.data is not None
        self.assertLess(len(json.loads(resp.data)["data"]["message"]), 80)
        BlocksResource(index).on_post_unique_blocks_for_samples(self._post("/uniqueblocks/samples/1", {"exclude_picblockhashes": [], "k" * 5000: 1}), resp, "1")
        assert resp.data is not None
        self.assertLess(len(json.loads(resp.data)["data"]["message"]), 120)

    def test_the_post_route_refuses_a_malformed_body(self):
        index = MagicMock()
        resp = falcon.Response()
        BlocksResource(index).on_post_unique_blocks_for_family(self._post("/uniqueblocks/family/4", {"exclude": [16]}), resp, 4)
        index.getUniqueBlocks.assert_not_called()
        self.assertEqual(falcon.HTTP_400, resp.status)

    def test_the_post_route_refuses_a_body_that_is_not_json(self):
        index = MagicMock()
        resp = falcon.Response()
        request = falcon.Request(falcon.testing.create_environ(path="/uniqueblocks/samples/1", method="POST", body="{", headers={"Content-Type": "application/json"}))
        BlocksResource(index).on_post_unique_blocks_for_samples(request, resp, "1")
        index.getUniqueBlocks.assert_not_called()
        self.assertEqual(falcon.HTTP_400, resp.status)
        assert resp.data is not None
        self.assertEqual("the body must be JSON", json.loads(resp.data)["data"]["message"])

    def test_the_client_posts_only_with_a_negative_set(self):
        client = McritClient(mcrit_server="http://mcrit")
        client._session = MagicMock()
        client._handle = MagicMock(return_value="job")
        client.requestUniqueBlocksForSamples([1], max_blocks_per_function=1, demote_generic_blocks=True)
        client._session.get.assert_called_once()
        self.assertEqual({"max_blocks_per_function": 1, "demote_generic_blocks": "true"}, client._session.get.call_args.kwargs["params"])
        client.requestUniqueBlocksForFamily(4, exclude_picblockhashes=[17, "0x10", "16"])
        self.assertEqual({"exclude_picblockhashes": [16, 17]}, client._session.post.call_args.kwargs["json"])
        # off is left out, as the server leaves it out
        client.requestUniqueBlocksForSamples([1], max_blocks_per_function=0, exclude_picblockhashes=[])
        self.assertEqual(1, client._session.post.call_count)
        self.assertEqual({}, client._session.get.call_args.kwargs["params"])

    def test_raw_unique_block_requests_return_the_http_response(self):
        client = McritClient(mcrit_server="http://mcrit", raw_responses=True)
        client._session = MagicMock()
        self.assertIs(client._session.get.return_value, client.requestUniqueBlocksForSamples([1]))
        self.assertIs(client._session.post.return_value, client.requestUniqueBlocksForFamily(4, exclude_picblockhashes=[17]))

    def test_the_client_refuses_what_the_server_would(self):
        client = McritClient(mcrit_server="http://mcrit")
        client._session = MagicMock()
        # a string or a number is not a list of hashes, though a string iterates as its digits
        for not_a_list in ("16", 16):
            with self.subTest(not_a_list=not_a_list), self.assertRaises(ValueError):
                client.requestUniqueBlocksForSamples([1], exclude_picblockhashes=not_a_list)
        self.assertEqual({}, client._getUniqueBlocksParams(gentle_length_score="false", demote_generic_blocks=0))
        self.assertEqual({"gentle_length_score": "true"}, client._getUniqueBlocksParams(gentle_length_score="True"))
        for bad in (True, "1_0", " 16", "016", -1):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                client.requestUniqueBlocksForSamples([1], exclude_picblockhashes=[bad])
        client._session.post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
