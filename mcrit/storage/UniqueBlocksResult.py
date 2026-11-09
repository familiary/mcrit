import datetime
import hashlib
import heapq
import re
import string
from typing import Any, Dict, List, Tuple

from picblocks.blockhasher import BlockHasher

from mcrit.storage.FunctionEntry import picBlockHashEntries


def wrap_string(input_str, max_column_length=100, padding=0):
    words = input_str.split()
    lines = []
    current_line = words[0]

    for word in words[1:]:
        if len(current_line) + 1 + len(word) <= max_column_length:
            current_line += " " + word
        else:
            lines.append(current_line)
            current_line = word

    lines.append(current_line)
    join_char = "\n" + padding * " "
    formatted_output = join_char.join(lines)

    return formatted_output


def coverPreference(block_hash, entry) -> Tuple[int, int]:
    """How the block cover orders blocks that cover as many samples with the same score (#283).

    Scores tie a lot: with a single sample, every block of 7 to 11 instructions scores 100.0, and
    the cover used to take whichever of them came last in the dict. Among tied blocks, the one whose
    YARA string has the most fixed hex digits now wins, wildcards not counted, so a block that is
    mostly a wildcarded call loses to a longer fixed pattern. That favours length in bytes, not
    distinctiveness as such. The lowest picblockhash settles what is still equal, so the same input
    always gives the same cover.
    MongoDbStorage keys the blocks by their "0x" hex, MemoryStorage by the integer, which a
    result that went through JSON carries as a decimal string.
    """
    fixed_nibbles = sum(1 for char in entry.get("escaped_sequence") or "" if char in string.hexdigits)
    return fixed_nibbles, -blockHashAsInt(block_hash)


def blockHashAsInt(block_hash) -> int:
    """A storage key as the picblockhash: an int, a "0x" hex string (MongoDbStorage) or decimal (JSON)."""
    return int(block_hash, 0) if isinstance(block_hash, str) else int(block_hash)


def parseBlockHash(value) -> int:
    """A picblockhash a caller gave, as an integer: an int, or a string of decimal or "0x" hex digits.

    Stricter than blockHashAsInt, which reads storage's own keys: a bool, a sign, spaces,
    underscores, leading zeros in decimal, and anything outside 64 bits are refused with a
    ValueError, by the server and the client alike (#283).
    """
    if isinstance(value, int) and not isinstance(value, bool):
        number = value
    # bounded before int() reads it: no more digits than 64 bits take
    elif isinstance(value, str) and re.fullmatch(r"0[xX][0-9a-fA-F]{1,16}|0|[1-9][0-9]{0,19}", value):
        number = int(value, 0)
    else:
        number = -1
    if not 0 <= number < 2**64:
        shown = repr(value) if len(repr(value)) <= 40 else repr(value)[:37] + "..."
        raise ValueError(f"not a picblockhash: {shown}")
    return number


def lengthScore(length, gentle=False) -> float:
    """How close a block of <length> instructions is to an "ideal" signature block, from 0 to 100.

    7 to 11 instructions score 100 and shorter blocks lose a seventh per missing instruction. Past
    11, the default drops as 1 / (length - 10), so 12 instructions score 50 and 20 score 10. With
    <gentle> it drops as 11 / length instead, so 12 score about 92 and 20 score 55 (#283).
    """
    if length < 7:
        return 100.0 - (100.0 * (7 - length) / 7)
    if length > 10:
        return 100.0 * (11 / length) if gentle else 100.0 * (1 / (length - 10))
    return 100.0


def blockScore(num_block_samples, num_samples, length, gentle=False) -> float:
    """A unique block's score: 80% the share of the samples it covers, 20% its lengthScore."""
    sample_score = 100.0 * num_block_samples / num_samples
    return 0.8 * sample_score + 0.2 * lengthScore(length, gentle=gentle)


_RETURN_MNEMONICS = {"ret", "retn", "retf"}
# a prefix becomes part of the mnemonic, e.g. GCC's "repz ret" or "bnd ret"
_RETURN_PREFIXES = {"rep", "repz", "bnd", "notrack"}
# padding, and the call of a stack-cookie check
_PASSIVE_MNEMONICS = {"nop", "int3", "endbr32", "endbr64", "call"}
_STACK_POINTER = {"sp", "esp", "rsp"}
_STACK_OR_FRAME_POINTER = _STACK_POINTER | {"bp", "ebp", "rbp", "x29", "fp"}
_RETURN_REGISTERS = {"al", "ax", "eax", "rax"}
_STACK_MEMORY = re.compile(r"\[(?:[re]?[sb]p|x29|fp)\b")
_REGISTER = re.compile(r"[a-z][a-z0-9]*")


def _exitRole(mnemonic, operands):
    """What an instruction does in a function exit: "restore", "neutral", or None if it has no place there."""
    parts = [part.strip() for part in operands.split(",")] if operands else []
    destination = parts[0] if parts else ""
    if mnemonic in ("pop", "leave"):
        return "restore"
    # AArch64 restores registers from the stack with ldp/ldr, e.g. ldp x29, x30, [sp], #0x10
    if mnemonic in ("ldp", "ldr") and any(part.startswith("[sp") for part in parts):
        return "restore"
    if destination in _STACK_POINTER and mnemonic in ("add", "sub", "lea", "mov"):
        return "restore"
    # MSVC's x86 SEH frame unlinked: mov dword ptr fs:[0], ecx
    if mnemonic == "mov" and destination.replace(" ", "").endswith("fs:[0]"):
        return "restore"
    # MSVC x64 restores non-volatile xmm registers: movaps xmm6, xmmword ptr [rsp + 0x30]
    if mnemonic in ("movaps", "movups", "movdqa", "movdqu") and destination.startswith("xmm") and any(_STACK_MEMORY.search(part) for part in parts[1:]):
        return "restore"
    if mnemonic in _PASSIVE_MNEMONICS:
        return "neutral"
    # the return value set to a flag: seta al
    if mnemonic.startswith("set") and parts == [destination] and destination in _RETURN_REGISTERS:
        return "neutral"
    if len(parts) != 2:
        return None
    source = parts[1]
    # any register but the return value reloaded from the frame, e.g. a callee-saved one or the
    # cookie: mov rbx, qword ptr [rsp + 0x30]
    if mnemonic == "mov" and destination not in _RETURN_REGISTERS and _STACK_MEMORY.search(source):
        return "neutral"
    # the stack-cookie check: xor rcx, rsp
    if mnemonic == "xor" and source in _STACK_OR_FRAME_POINTER:
        return "neutral"
    if destination in _RETURN_REGISTERS:
        # the return value set to zero, -1, another register, a small constant or a local
        if mnemonic in ("xor", "sub") and source == destination:
            return "neutral"
        if mnemonic == "or" and _isAllOnes(source):
            return "neutral"
        if mnemonic in ("mov", "movzx") and (_REGISTER.fullmatch(source) or _STACK_MEMORY.search(source) or _isSmallImmediate(source) or _isAllOnes(source)):
            return "neutral"
    return None


def _immediate(operand):
    try:
        return int(operand, 0)
    except ValueError:
        return None


def _isSmallImmediate(operand) -> bool:
    value = _immediate(operand)
    return value is not None and 0 <= value <= 0xFF


def _isAllOnes(operand) -> bool:
    """-1, as an immediate of any width: or eax, 0xffffffff."""
    return _immediate(operand) in (-1, 0xFFFF, 0xFFFFFFFF, 0xFFFFFFFFFFFFFFFF)


def _isReturn(mnemonic) -> bool:
    *prefixes, base = mnemonic.split() or [""]
    return base in _RETURN_MNEMONICS and set(prefixes) <= _RETURN_PREFIXES


def isGenericBlock(entry) -> bool:
    """Whether a block is compiler boilerplate rather than code of its own (#283).

    That is a block whose YARA string is at least half wildcards, or a function exit: a block that
    ends in a return, restores something (a pop, leave, a write to the stack pointer such as
    "add rsp, 0x28", AArch64's ldp/ldr from [sp], MSVC x64's xmm registers from the stack, or
    unlinking MSVC's x86 SEH frame from fs:[0]) and otherwise holds only padding, calls (their
    targets are wildcards anyway), "xor rcx, rsp" of a stack-cookie check, registers other than
    the return value reloaded from the frame, and the return value set to zero, -1, a flag,
    another register, a constant up to 0xff or a local. Other arithmetic or a larger constant keeps
    a block out, so does a block without a restore, such as a leaf function that reads its
    arguments through esp. The return-value rules are Intel's, and an AArch64 stack-cookie check
    is not recognised. A heuristic, not a classification: it is only used to demote blocks.
    """
    escaped_sequence = entry.get("escaped_sequence") or ""
    digits = sum(1 for char in escaped_sequence if char in string.hexdigits or char == "?")
    if digits and escaped_sequence.count("?") * 2 >= digits:
        return True
    instructions = entry.get("instructions") or []
    if not instructions or not _isReturn(instructions[-1][2]):
        return False
    roles = [_exitRole(instruction[2], instruction[3]) for instruction in instructions[:-1]]
    return None not in roles and "restore" in roles


class UniqueBlocksProgressReporter:
    """Spreads the phases of a unique-blocks job, in storage and in the Worker, over one total.

    Each phase gets a fixed share of it, so the job's progress only grows and reaches 100% when the
    job is done. A phase is started with how many steps it expects and may take more: its share is
    full after that many, the next start_phase() fills whatever it did not reach, and finish() the
    rest of the job. It lives here rather than with JobProgressReporter so that storage need not
    import the queue.
    """

    # rough shares of the total, in proportion to how long each phase took on MongoDB for the two
    # small reports in tests/
    _PHASE_UNITS = {"collect": 25, "eliminate": 26, "score": 2, "extract": 37, "escape": 7, "cover": 3}

    def __init__(self, reporter=None):
        self._reporter = reporter
        self._reported = 0
        self._phase_start = 0
        self._phase_units = 0
        self._phase_total = 1
        self._phase_steps = 0
        if reporter is not None:
            reporter.set_total(sum(self._PHASE_UNITS.values()))

    @classmethod
    def wrap(cls, reporter):
        """<reporter> itself if it is one, so storage called by the Worker reports into the Worker's total"""
        return reporter if isinstance(reporter, cls) else cls(reporter)

    def start_phase(self, name, total):
        self._finish_phase()
        self._phase_start = self._reported
        self._phase_units = self._PHASE_UNITS[name]
        self._phase_total = max(1, total)
        self._phase_steps = 0

    def step(self, count=1):
        self._phase_steps = min(self._phase_total, self._phase_steps + count)
        reached = self._phase_start + self._phase_units * self._phase_steps // self._phase_total
        if reached > self._reported:
            self._advance(reached - self._reported)
        elif self._reporter is not None:
            # a step that completes no unit still lets a job notice that it was terminated
            self._reporter.poll()

    def _finish_phase(self):
        self._advance(self._phase_start + self._phase_units - self._reported)

    def finish(self):
        self._advance(sum(self._PHASE_UNITS.values()) - self._reported)

    def _advance(self, units):
        if self._reporter is not None:
            for _ in range(units):
                self._reporter.step()
        self._reported += units


def selectBlockCover(blocks, sample_ids, required_per_sample, max_blocks_per_function=0, demoted=frozenset()) -> Tuple[list, dict]:
    """Greedily pick a multi-set cover of <blocks> for <sample_ids>, i.e. the blocks of a YARA rule.

    Each round takes the block that reaches the most samples not yet covered <required_per_sample>
    times, then the highest score, then coverPreference. <max_blocks_per_function> caps how many
    blocks one function contributes, 0 for no cap; a block without a function_id is not capped. A
    function is one function of one sample: a block that storage found in several functions
    carries the lowest function_id among them, so in a cover over several samples a helper they
    share can still contribute up to the cap per function id. The block hashes in <demoted> rank
    behind all other blocks that reach as many samples. Both are off by default (#283). Answers the
    picked block hashes in the order they were picked and how many of them reach each sample.
    """
    selected: list = []
    sample_coverage = {sample_id: 0 for sample_id in sample_ids}
    samples_covered = set()
    function_picks: Dict[Any, int] = {}
    candidates = []
    for position, (block_hash, entry) in enumerate(blocks.items()):
        fixed, negative_hash = coverPreference(block_hash, entry)
        # the position settles a tie before block_hash is compared, which for the same hash keyed
        # once as int and once as str would raise
        candidates.append((-len(set(entry["samples"])), block_hash in demoted, -entry["score"], -fixed, -negative_hash, position, block_hash))
    heapq.heapify(candidates)
    while len(samples_covered) < len(sample_ids):
        # a block's reach only shrinks, so the heap holds an upper bound of each: the first block
        # popped whose bound is still its reach is the best, and one whose bound is stale goes back
        # with its current reach
        best = None
        while candidates:
            candidate = heapq.heappop(candidates)
            block_hash = candidate[-1]
            entry = blocks[block_hash]
            function_id = entry.get("function_id")
            if max_blocks_per_function and function_id is not None and function_picks.get(function_id, 0) >= max_blocks_per_function:
                continue
            coverable = set(entry["samples"]).difference(samples_covered)
            if not coverable:
                continue
            if len(coverable) != -candidate[0]:
                heapq.heappush(candidates, (-len(coverable), *candidate[1:]))
                continue
            best = (block_hash, coverable)
            break
        if best is None:
            break
        block_hash, coverable = best
        selected.append(block_hash)
        function_id = blocks[block_hash].get("function_id")
        function_picks[function_id] = function_picks.get(function_id, 0) + 1
        for sample_id in coverable:
            sample_coverage[sample_id] += 1
        samples_covered = {sample_id for sample_id, count in sample_coverage.items() if count >= required_per_sample}
    return selected, sample_coverage


def picBlockHashesOfReport(smda_report) -> List[int]:
    """The picblockhashes of an SMDA report's blocks, as storage computes them when adding it.

    Meant for the negative set of a unique-blocks request (#283): the blocks of a sibling that is
    not, and should not be, in the index, hashed locally and sent instead of the report.
    """
    blockhasher = BlockHasher()
    lower_addr = smda_report.base_addr
    upper_addr = lower_addr + smda_report.binary_size
    hashes = set()
    for smda_function in smda_report.getFunctions():
        hashes.update(entry["hash"] for entry in picBlockHashEntries(blockhasher, smda_function, lower_addr, upper_addr))
    return sorted(hashes)


# Dataclass, post init
# constructor -> .fromDict


class UniqueBlocksResult:
    statistics: dict
    unique_blocks: dict
    yara_rule: list

    def __init__(self) -> None:
        pass

    def generateYaraRule(
        self,
        min_ins=None,
        max_ins=None,
        min_bytes=None,
        max_bytes=None,
        required_per_sample=10,
        condition_required=7,
        wrap_at=80,
        max_blocks_per_function=None,
        demote_generic_blocks=None,
    ):
        cover = self.generateBlockCover(
            min_ins=min_ins,
            max_ins=max_ins,
            min_bytes=min_bytes,
            max_bytes=max_bytes,
            required_per_sample=required_per_sample,
            max_blocks_per_function=max_blocks_per_function,
            demote_generic_blocks=demote_generic_blocks,
        )
        return self.renderRule(cover, condition_required, wrap_at=wrap_at)

    def renderRule(self, block_cover, condition_required, wrap_at=80):
        """Render the YARA rule. wrap_at is the column to wrap the hex at; 0 keeps it on one line."""
        yara_blocks = block_cover["block_hashes"]
        block_hash_string = ",".join([pic_hash for pic_hash in yara_blocks])
        rule_identifier = hashlib.sha256(block_hash_string.encode()).hexdigest()[:16]
        yara_rule = f"rule mcrit_{rule_identifier} {{\n"
        yara_rule += "    meta:\n"
        yara_rule += '        author = "MCRIT YARA Generator"\n'
        yara_rule += '        description = "Code-based YARA rule composed from potentially unique basic blocks for the selected set of samples/family."\n'
        rule_date = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
        yara_rule += f'        date = "{rule_date}"\n'
        yara_rule += "    strings:\n"
        yara_rule += (
            f"        // Rule generation selected {len(yara_blocks)} picblocks, covering {block_cover['num_samples_covered']}/{self.statistics['num_samples']} input sample(s).\n"
        )
        for pichash, result in self.unique_blocks.items():
            if pichash not in yara_blocks:
                continue
            yarafied = f"        /* picblockhash: {pichash} - coverage: {len(result['samples'])}/{block_cover['num_samples_covered']} samples.\n"
            maxlen_ins = max([len(ins[1]) for ins in result["instructions"]])
            for ins in result["instructions"]:
                yarafied += f"         * {ins[1]:{maxlen_ins}} | {ins[2]} {ins[3]}\n"
            yarafied += "         */\n"
            if wrap_at:
                yarafied += f"        $blockhash_{pichash} = {{\n"
                yarafied += "            " + wrap_string(result["escaped_sequence"], max_column_length=wrap_at, padding=12) + "\n"
                yarafied += "        }\n"
            else:
                yarafied += f"        $blockhash_{pichash} = {{ " + result["escaped_sequence"] + " }\n"
            yara_rule += yarafied + "\n"
        yara_rule += "    condition:\n"
        yara_rule += f"        {min(len(yara_blocks), condition_required)} of them\n"
        yara_rule += "}\n"
        return yara_rule

    def generateBlockCover(self, min_ins=None, max_ins=None, min_bytes=None, max_bytes=None, required_per_sample=10, max_blocks_per_function=None, demote_generic_blocks=None):
        """Pick the blocks of a YARA rule from this result, after filtering them by size.

        <max_blocks_per_function> is as in selectBlockCover, and <demote_generic_blocks> demotes the
        blocks isGenericBlock flags; left at None, both take the values the job ran with, as its
        statistics record them. Given the job's covers_required and no size filters, the cover is
        the job's own.
        """
        block_cover: Dict[str, Any] = {"block_hashes": [], "num_samples_covered": 0, "has_rule": False, "is_complete_cover": False}
        # we need to filter first, according to the desired parameters
        filtered_blocks: Dict[int, Dict[str, Any]] = {}
        for block_hash, entry in self.unique_blocks.items():
            # no disassembly stored for its function: nothing to write a pattern from
            if not entry.get("instructions"):
                continue
            if min_ins and entry["length"] < min_ins:
                continue
            if max_ins and entry["length"] > max_ins:
                continue
            bytes_length = len(entry["escaped_sequence"].replace(" ", "")) // 2
            if min_bytes and bytes_length < min_bytes:
                continue
            if max_bytes and bytes_length > max_bytes:
                continue
            filtered_blocks[block_hash] = entry
        sample_ids = [int(v) for v in self.statistics["by_sample_id"].keys()]
        # as Worker.getUniqueBlocks clamps it: no samples need 0 blocks, so 0 is no cover
        required_per_sample = max(1, int(required_per_sample))
        # the job's own options, so the cover rebuilt here is the one the job picked (#283)
        if max_blocks_per_function is None:
            max_blocks_per_function = self.statistics.get("max_blocks_per_function", 0)
        # as Worker.getUniqueBlocks reads it: below 1 is no cap
        max_blocks_per_function = max(0, int(max_blocks_per_function or 0))
        if demote_generic_blocks is None:
            demote_generic_blocks = self.statistics.get("demote_generic_blocks", False)
        demoted = {block_hash for block_hash, entry in filtered_blocks.items() if isGenericBlock(entry)} if demote_generic_blocks else frozenset()
        yara_rule_blocks, sample_coverage = selectBlockCover(filtered_blocks, sample_ids, required_per_sample, max_blocks_per_function=max_blocks_per_function, demoted=demoted)
        block_cover["num_samples_covered"] = sum(1 for count in sample_coverage.values() if count >= required_per_sample)
        if block_cover["num_samples_covered"] == len(sample_ids):
            block_cover["has_rule"] = True
            block_cover["is_complete_cover"] = True
        elif yara_rule_blocks:
            block_cover["has_rule"] = True
        block_cover["block_hashes"] = yara_rule_blocks
        return block_cover

    def toDict(self):
        # rebuild the original UniqueBlocks result here
        blocks_dict = {
            "statistics": self.statistics,
            "unique_blocks": self.unique_blocks,
            "yara_rule": self.yara_rule,
        }
        return blocks_dict

    @classmethod
    def fromDict(cls, entry_dict):
        blocks_result = cls()
        blocks_result.statistics = entry_dict["statistics"]
        blocks_result.unique_blocks = entry_dict["unique_blocks"]
        blocks_result.yara_rule = entry_dict["yara_rule"]
        return blocks_result

    def __str__(self):
        if self.statistics is not None:
            return "UniqueBlocksResult: {} Samples with {} unique blocks.".format(len(self.statistics["by_sample_id"]), len(self.unique_blocks))
        else:
            return "UniqueBlocksResult: nothing parsed."
