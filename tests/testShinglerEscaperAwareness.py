#!/usr/bin/python

import json
import logging
import os
import unittest

# Shinglers use bare module imports (e.g. `from AbstractShingler import ...`) which
# only resolve when mcrit/shinglers/ is on sys.path. MCRIT adds it at load time
# (see ShingleLoader._getShinglerClasses); tests/conftest.py does the same for pytest.
from EscapedBlockShingler import EscapedBlockShingler
from FuzzyStatPairShingler import FuzzyStatPairShingler
from smda.aarch64.AArch64InstructionEscaper import AArch64InstructionEscaper
from smda.cil.CilInstructionEscaper import CilInstructionEscaper
from smda.common.BinaryInfo import BinaryInfo
from smda.common.SmdaFunction import SmdaFunction
from smda.common.SmdaReport import SmdaReport
from smda.intel.IntelInstructionEscaper import IntelInstructionEscaper

from .context import config

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logging.disable(logging.CRITICAL)


class ShinglerEscaperAwarenessTestSuite(unittest.TestCase):
    """Shinglers must use the per-function escaper, not a hardcoded Intel escaper."""

    def _load_intel_function(self):
        THIS_FILE_PATH = str(os.path.abspath(__file__))
        PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
        example_file_path = os.sep.join([PROJECT_ROOT, "tests", "example_report.smda"])
        smda_report = SmdaReport.fromFile(example_file_path)
        assert smda_report is not None
        functions = [f for f in smda_report.getFunctions() if f.num_instructions > 0]
        self.assertTrue(functions, "need at least one function in the fixture")
        return functions[0]

    def _rearchitect(self, intel_function, architecture):
        binary_info = BinaryInfo(b"")
        binary_info.architecture = architecture
        return SmdaFunction.fromDict(intel_function.toDict(), binary_info=binary_info)

    def test_escaper_is_taken_from_function(self):
        intel_function = self._load_intel_function()
        aarch64_function = self._rearchitect(intel_function, "aarch64")
        cil_function = self._rearchitect(intel_function, "cil")
        self.assertIs(intel_function._escaper, IntelInstructionEscaper)
        self.assertIs(aarch64_function._escaper, AArch64InstructionEscaper)
        self.assertIs(cil_function._escaper, CilInstructionEscaper)

    def test_escaped_block_shingler_uses_per_function_escaper(self):
        intel_function = self._load_intel_function()
        aarch64_function = self._rearchitect(intel_function, "aarch64")
        shingler = EscapedBlockShingler(config.SHINGLER_CONFIG)
        intel_sequences = shingler.process(intel_function, 0)
        aarch64_sequences = shingler.process(aarch64_function, 0)
        # both must produce shingles (non-empty) and must not raise
        self.assertTrue(intel_sequences)
        self.assertTrue(aarch64_sequences)
        # AArch64 escaping must differ from raw/Intel escaping for at least some instruction
        first_instruction = next(iter(aarch64_function.getInstructions()))
        self.assertIsNotNone(first_instruction.getMnemonicGroup(AArch64InstructionEscaper))

    def test_fuzzy_stat_pair_shingler_uses_per_function_escaper(self):
        intel_function = self._load_intel_function()
        aarch64_function = self._rearchitect(intel_function, "aarch64")
        cil_function = self._rearchitect(intel_function, "cil")
        shingler = FuzzyStatPairShingler(config.SHINGLER_CONFIG)
        self.assertTrue(shingler.process(intel_function, 0))
        self.assertTrue(shingler.process(aarch64_function, 0))
        self.assertTrue(shingler.process(cil_function, 0))

    def test_fuzzy_stat_pair_stack_size_follows_the_architecture(self):
        intel_function = self._load_intel_function()
        shingler = FuzzyStatPairShingler(config.SHINGLER_CONFIG)
        # Intel code read as another architecture's has no frame that architecture recognises
        self.assertEqual(shingler._getStackSize(self._rearchitect(intel_function, "aarch64")), 0)
        self.assertEqual(shingler._getStackSize(self._rearchitect(intel_function, "cil")), 0)

    def _aarch64_function(self, report_name, offset, first_operands=None, replace=None):
        """A fixture function, optionally with its first instruction's operands or whole entry-block
        instructions ({index: (mnemonic, operands)}) replaced."""
        report_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", report_name)
        with open(report_path) as handle:
            function_dict = json.load(handle)["xcfg"][str(offset)]
        if first_operands is not None:
            function_dict["blocks"][str(offset)][0][3] = first_operands
        for index, (mnemonic, operands) in (replace or {}).items():
            function_dict["blocks"][str(offset)][index][2:4] = [mnemonic, operands]
        binary_info = BinaryInfo(b"")
        binary_info.architecture = "aarch64"
        return SmdaFunction.fromDict(function_dict, binary_info=binary_info)

    def test_fuzzy_stat_pair_stack_size_reads_aarch64_prologues(self):
        """#238: sub sp, sp, #imm and pre-indexed pushes onto sp make up an AArch64 frame"""
        shingler = FuzzyStatPairShingler(config.SHINGLER_CONFIG)
        # sub sp, sp, #0x70 followed by stores into the frame it reserved
        self.assertEqual(0x70, shingler._getStackSize(self._aarch64_function("crossarch_aarch64_a.smda", 0x100002AB0)))
        # stp x20, x19, [sp, #-0x20]! followed by a store into that frame
        self.assertEqual(0x20, shingler._getStackSize(self._aarch64_function("crossarch_aarch64_b.smda", 0x10000496C)))
        # a reservation of 0x1000 is written with a shifted immediate
        shifted = self._aarch64_function("crossarch_aarch64_a.smda", 0x100002AB0, first_operands="sp, sp, #0x1, lsl #12")
        self.assertEqual(0x1000, shingler._getStackSize(shifted))
        # a single register pushed with a pre-indexed str, as Go sets up its frames
        pushed = self._aarch64_function("crossarch_aarch64_b.smda", 0x10000496C, replace={0: ("str", "x30, [sp, #-0x40]!")})
        self.assertEqual(0x40, shingler._getStackSize(pushed))
        # a push followed by a reservation adds up
        both = self._aarch64_function("crossarch_aarch64_b.smda", 0x10000496C, replace={1: ("sub", "sp, sp, #0x30")})
        self.assertEqual(0x20 + 0x30, shingler._getStackSize(both))
        # in either order
        reserved_first = self._aarch64_function("crossarch_aarch64_a.smda", 0x100002AB0, replace={1: ("stp", "x29, x30, [sp, #-0x10]!")})
        self.assertEqual(0x70 + 0x10, shingler._getStackSize(reserved_first))
        # and one beyond the log buckets counts as none, as for Intel
        huge = self._aarch64_function("crossarch_aarch64_a.smda", 0x100002AB0, first_operands="sp, sp, #0xfff, lsl #12")
        self.assertEqual(0, shingler._getStackSize(huge))


if __name__ == "__main__":
    unittest.main()
