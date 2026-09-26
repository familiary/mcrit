#!/usr/bin/env python3

import re
from collections import Counter

from AbstractShingler import AbstractShingler
from LogBucket import LogBucket

from mcrit.libs.utility import generate_unique_pairs

# pre-indexed store that moves sp down: "x29, x30, [sp, #-0x20]!"
AARCH64_PUSH = re.compile(r"\[sp, #-(0x[0-9a-f]+|\d+)\]!$")
# frame reservation: "sp, sp, #0x60", or "sp, sp, #0x1, lsl #12" for 0x1000
AARCH64_RESERVE = re.compile(r"^sp, sp, #(0x[0-9a-f]+|\d+)(, lsl #12)?$")


class FuzzyStatPairShingler(AbstractShingler):
    """Use buckets to introduce fuzziness to CFG stats."""

    def __init__(self, config, weight=1):
        super().__init__(__class__.__name__)
        self._config = config
        self._weight = weight
        self._log_buckets = LogBucket(self._config.SHINGLER_LOGBUCKETS, self._config.SHINGLER_LOGBUCKET_RANGE)

    def _getAArch64StackSize(self, function_object):
        """The frame an AArch64 prologue sets up: what pre-indexed stores push onto sp
        (stp x29, x30, [sp, #-0x20]!) plus what sub sp, sp, #imm reserves, in its first ten
        instructions (#238)."""
        stack_size = 0
        for ins in function_object.blocks[function_object.offset][:10]:
            operands = ins.operands or ""
            if ins.mnemonic in ("stp", "str") and operands.endswith("]!"):
                pushed = AARCH64_PUSH.search(operands)
                if pushed:
                    stack_size += int(pushed.group(1), 0)
            elif ins.mnemonic == "sub":
                reserved = AARCH64_RESERVE.match(operands)
                if reserved:
                    stack_size += int(reserved.group(1), 0) << (12 if reserved.group(2) else 0)
        return stack_size if 0 <= stack_size < self._config.SHINGLER_LOGBUCKETS else 0

    def _getStackSize(self, function_object):
        stack_size = 0
        from smda.aarch64.AArch64InstructionEscaper import AArch64InstructionEscaper
        from smda.intel.IntelInstructionEscaper import IntelInstructionEscaper

        if function_object._escaper is AArch64InstructionEscaper:
            return self._getAArch64StackSize(function_object)
        if function_object._escaper is not IntelInstructionEscaper:
            return stack_size
        for ins in function_object.blocks[function_object.offset][:10]:
            if ins.mnemonic == "sub":
                operands = [op.strip() for op in ins.operands.split(",")]
                if len(operands) == 2 and operands[0] in ["esp", "rsp"]:
                    try:
                        stack_size = int(operands[1], 16)
                        if 0 <= stack_size < self._config.SHINGLER_LOGBUCKETS:
                            break
                        else:
                            stack_size = 0
                    except ValueError:
                        pass
                    try:
                        stack_size = int(operands[1])
                        if 0 <= stack_size < self._config.SHINGLER_LOGBUCKETS:
                            break
                        else:
                            stack_size = 0
                    except ValueError:
                        pass
        return stack_size

    def _create_bucketed_values(self, value, field_name):
        bucketed = []
        if self._config.SHINGLER_LOGBUCKET_CENTERED:
            field_count = Counter()
            bucket_range = self._log_buckets.getLogBucketRange(value)
            for index, bucket in enumerate(bucket_range):
                distance = abs(index - self._config.SHINGLER_LOGBUCKET_RANGE)
                for _ in range(distance, self._config.SHINGLER_LOGBUCKET_RANGE + 1, 1):
                    field_count[bucket] += 1
                    bucketed.append("{}={}:{}".format(field_name, field_count[bucket], bucket))
        else:
            for bucket in self._log_buckets.getLogBucketRange(value):
                bucketed.append("{}:{}".format(field_name, bucket))
        return bucketed

    def _generateByteSequences(self, function_object):
        byte_sequences = []
        mnemonic_type_count = Counter()
        for instruction in function_object.getInstructions():
            mnemonic_type_count[instruction.getMnemonicGroup(function_object._escaper)] += 1
        num_ins_C = mnemonic_type_count["C"] if "C" in mnemonic_type_count else 0
        num_ins_S = mnemonic_type_count["S"] if "S" in mnemonic_type_count else 0
        num_ins_M_rel = int(100 * mnemonic_type_count["M"] / function_object.num_instructions) if "M" in mnemonic_type_count else 0
        num_ins_A_rel = int(100 * mnemonic_type_count["A"] / function_object.num_instructions) if "A" in mnemonic_type_count else 0
        max_block_size = max([block.length for block in function_object.getBlocks()])
        num_calls = function_object.num_calls
        # num_loops = len([component for component in function_object.strongly_connected_components if len(component) > 1])
        stack_size = self._getStackSize(function_object)
        fields = {
            "num_ins_C": num_ins_C,
            "num_ins_S": num_ins_S,
            "num_ins_A_rel": num_ins_A_rel,
            "num_ins_M_rel": num_ins_M_rel,
            "num_calls": num_calls,
            "stack_size": stack_size,
            "max_block_size": max_block_size,
            # "num_returns": num_returns,
            # "stack_size": stack_size,
            # "max_block_size": max_block_size,
        }
        field_values = []
        for field_name, value in fields.items():
            bucket_values = self._create_bucketed_values(value, field_name)
            field_values.extend(bucket_values)
        return field_values
        # optionally group each two fields to create more fuzziness / a larger value corpus to minhash from
        for field_a, field_b in generate_unique_pairs(field_values):
            if field_a.split(":")[0] != field_b.split(":")[0]:
                byte_sequences.append("{}-{}-{}".format(self._name, field_a, field_b))
        return byte_sequences
