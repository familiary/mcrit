#!/usr/bin/env python3
"""A stable fingerprint of smda's observable escaping behaviour.

MinHashes are derived from smda's *escaped* instruction representation: the shinglers build their
tokens from `SmdaInstruction.getMnemonicGroup()` and `SmdaInstruction.getEscapedOperands()`. That
makes the escaper as much an input to a minhash as MINHASH_SEED is - but unlike the seed it lives
in a separate, independently versioned package, and it is not covered by any config hash. When
smda changes how it escapes, previously stored minhashes silently stop being comparable to newly
computed ones: they remain valid-looking and still match each other, while identical code
submitted afterwards no longer finds them.

smda 4.4.5 is the worked example. It corrected three escaper classifications - most notably it
stopped escaping segment-qualified memory operands (`gs:[0x60]`, `fs:[0x30]`, `es:[edi]`) as
CONST - which changed roughly a fifth of the minhashes on a real corpus, with a small share of
functions losing every LSH band and becoming unretrievable. The change was correct; the problem is
that nothing announced it.

This module runs the escaper over a fixed, committed probe of instructions chosen to exercise the
operand and mnemonic classes escaping actually distinguishes, and hashes the result. Comparing the
fingerprint across smda versions turns "escaping changed" from an invisible condition into a
value that can be recorded, exported and compared.

The probe deliberately depends on as little of smda as possible: `SmdaInstruction` is constructed
directly from four-field lists (offset, bytes, mnemonic, operands), so no report, binary or
disassembly is needed and the fingerprint is computable at import time.
"""

import hashlib
import logging
from typing import Dict, List, Sequence

from smda.common.SmdaFunction import SmdaFunction
from smda.common.SmdaInstruction import SmdaInstruction

LOGGER = logging.getLogger(__name__)

# returned instead of a hash when the running smda does not expose an escaper we can drive;
# this is a diagnostic, so it degrades to "unknown" rather than breaking a status call
FINGERPRINT_UNAVAILABLE = "unavailable"

# Probe instructions as [offset, bytes, mnemonic, operands], the same four-field form used in
# serialized SMDA functions. Chosen to cover the classes escaping distinguishes; the byte fields
# are realistic so that byte-level escaping (jump/call targets, ptr refs, immediates) also runs.
# Every architecture MCRIT computes minhashes for is probed (#93). The non-Intel probes are taken
# from real code - one instruction per distinct escaped form, from the cross-architecture test
# fixtures and, for Dalvik, from an app with its class names and strings replaced by neutral ones
# that escape identically.
# Adding instructions to an architecture's probe changes that architecture's fingerprint, so only
# do so at a deliberate version boundary; adding an architecture leaves the others' unchanged.
ESCAPER_PROBE_INSTRUCTIONS: Dict[str, List[List]] = {
    "intel": [
        # plain registers across widths
        [0x401000, "4053", "push", "rbx"],
        [0x401002, "89c8", "mov", "eax, ecx"],
        [0x401004, "6689c8", "mov", "ax, cx"],
        [0x401007, "88c4", "mov", "ah, al"],
        # segment-qualified memory operands: the class 4.4.5 reclassified
        [0x401009, "65488b042560000000", "mov", "rax, qword ptr gs:[0x60]"],
        [0x401012, "64a130000000", "mov", "eax, dword ptr fs:[0x30]"],
        [0x401018, "648b0d00000000", "mov", "ecx, dword ptr fs:[0]"],
        [0x40101F, "65488b042528000000", "mov", "rax, qword ptr gs:[0x28]"],
        [0x401028, "268b07", "mov", "eax, dword ptr es:[edi]"],
        [0x40102B, "268a07", "mov", "al, byte ptr es:[edi]"],
        # a true far pointer: seg:off with no memory operand
        [0x40102E, "ea0010400033", "jmp", "0x33:0x401000"],
        # absolute and rip-relative pointer references
        [0x401034, "8b0d00104000", "mov", "ecx, dword ptr [0x401000]"],
        [0x40103A, "488b0d34120000", "mov", "rcx, qword ptr [rip + 0x1234]"],
        [0x401041, "488b0dccedffff", "mov", "rcx, qword ptr [rip - 0x1234]"],
        # complex effective addresses
        [0x401048, "8b448820", "mov", "eax, dword ptr [eax + ecx*4 + 0x20]"],
        [0x40104C, "8d0c8500000000", "lea", "ecx, [eax*4]"],
        # immediates, small and wide
        [0x401053, "b801000000", "mov", "eax, 1"],
        [0x401058, "b800104000", "mov", "eax, 0x401000"],
        [0x40105D, "48b8efcdab8967452301", "movabs", "rax, 0x123456789abcdef"],
        # vector file, including the AVX-512 range and mask registers 4.4.5 widened
        [0x401067, "0f28c1", "movaps", "xmm0, xmm1"],
        [0x40106A, "c5f428c1", "vmovaps", "ymm0, ymm1"],
        [0x40106E, "62f17c48280d00000000", "vmovaps", "zmm1, zmm2"],
        [0x401078, "62f17c4928c1", "vmovaps", "zmm0 {k1}, zmm1"],
        [0x40107E, "62f17cc928c1", "vmovaps", "zmm0 {k1} {z}, zmm1"],
        [0x401084, "62f17c48284d00", "vmovaps", "xmm17, xmm18"],
        [0x40108B, "c5f841c9", "kandw", "k1, k2, k3"],
        # string, stack, privileged and BMI mnemonics whose grouping 4.4.5 corrected
        [0x40108F, "f348a5", "rep movsq", "qword ptr es:[rdi], qword ptr [rsi]"],
        [0x401092, "f348a7", "repe cmpsq", "qword ptr [rsi], qword ptr es:[rdi]"],
        [0x401095, "6660", "pushaw", ""],
        [0x401097, "6661", "popaw", ""],
        [0x401099, "0f01d0", "xgetbv", ""],
        [0x40109C, "0f01d1", "xsetbv", ""],
        [0x40109F, "c4e37bf0c108", "rorx", "eax, ecx, 8"],
        [0x4010A5, "c4e262f7c1", "sarx", "eax, ecx, edx"],
        # control, debug and segment registers
        [0x4010AA, "0f20c0", "mov", "eax, cr0"],
        [0x4010AD, "0f21f8", "mov", "eax, dr7"],
        [0x4010B0, "8cc0", "mov", "eax, es"],
        # control flow: intraprocedural and outbound
        [0x4010B2, "e809000000", "call", "0x4010c0"],
        [0x4010B7, "eb05", "jmp", "0x4010be"],
        [0x4010B9, "0f8505000000", "jne", "0x4010c4"],
        [0x4010BF, "ff1500204000", "call", "qword ptr [rip + 0x2000]"],
        [0x4010C5, "c3", "ret", ""],
    ],
    "aarch64": [
        [0x1000024EC, "ff8301d1", "sub", "sp, sp, #0x60"],
        [0x1000024F0, "eb2b016d", "stp", "d11, d10, [sp, #0x10]"],
        [0x100002508, "1f2003d5", "nop", ""],
        [0x100002510, "a30e0094", "bl", "#0x100005f9c"],
        [0x10000253C, "0840601e", "fmov", "d8, d0"],
        [0x100002698, "200020d4", "brk", "#1"],
        [0x100002C98, "00e4002f", "movi", "d0, #0000000000000000"],
        [0x10000250C, "600a0558", "ldr", "x0, #0x10000c658"],
        [0x100002520, "fd031daa", "mov", "x29, x29"],
        [0x100002528, "800b00b4", "cbz", "x0, #0x100002698"],
        [0x100002578, "020080d2", "mov", "x2, #0"],
        [0x1000025C8, "00106c1e", "fmov", "d0, #0.50000000"],
        [0x100002694, "c0035fd6", "ret", ""],
        [0x1000026B4, "f44fc2a8", "ldp", "x20, x19, [sp], #0x20"],
        [0x1000026C8, "ff0b00b9", "str", "wzr, [sp, #8]"],
        [0x1000028F8, "00023fd6", "blr", "x16"],
        [0x100002908, "350108cb", "sub", "x21, x9, x8"],
        [0x100002DCC, "08ef78d3", "ubfx", "x8, x24, #0x38, #4"],
        [0x100002DD8, "2801889a", "csel", "x8, x9, x8, eq"],
        [0x100003CD8, "e0150036", "tbz", "w0, #0, #0x100003f94"],
        [0x1000048B0, "e0179f1a", "cset", "w0, eq"],
        [0x100004970, "200056fa", "ccmp", "x1, x22, #0, eq"],
        [0x100007C54, "007d2448", "casp", "x4, x5, x0, x1, [x8]"],
        [0x1000082F4, "bf3903d5", "dmb", "ishld"],
        [0x100008518, "280940fa", "ccmp", "x9, #0, #8, eq"],
        # escaper classes the fixtures do not reach, encodings checked with capstone
        [0x100010000, "3f2303d5", "paciasp", ""],
        [0x100010004, "40d03bd5", "mrs", "x0, tpidr_el0"],
        [0x100010008, "000080f9", "prfm", "pldl1keep, [x0]"],
        [0x10001000C, "0070404c", "ld1", "{v0.16b}, [x0]"],
        [0x100010010, "001c0c4e", "mov", "v0.s[1], w0"],
        [0x100010014, "5f2403d5", "bti", "c"],
        [0x100010018, "207e0bd5", "dc", "civac, x0"],
        # shifted and extended operands, whose modifier the escaper drops today
        [0x10001001C, "2008028b", "add", "x0, x1, x2, lsl #2"],
        [0x100010020, "2040228b", "add", "x0, x1, w2, uxtw"],
    ],
    "cil": [
        [0x25C, "7201000070", "ldstr", '"SGFjS2Vk"'],
        [0x261, "8001000004", "stsfld", "VN"],
        [0x2B7, "280400000a", "call", "Microsoft.VisualBasic.CompilerServices.Conversions::ToBoolean"],
        [0x6D5, "da", "sub.ovf", ""],
        [0x2439, "00", "nop", ""],
        [0x270, "14", "ldnull", ""],
        [0x2BC, "800a000004", "stsfld", "BD"],
        [0x332, "2001140000", "ldc.i4", "0x1401"],
        [0x351, "2a", "ret", ""],
        [0x7F7, "7203020070", "ldstr", '","'],
        [0xECB, "a2", "stelem.ref", ""],
        [0x2574, "7323000006", "newobj", ".ctor"],
        [0x5DF3, "72630c0070", "ldstr", '",0"'],
        # float constants stay literal, break has a group of its own, and a prefix escapes as a nop does
        [0x3000, "23000000000000e03f", "ldc.r8", "0.5"],
        [0x3009, "01", "break", ""],
        [0x300A, "fe14", "tail.", ""],
    ],
    "dalvik": [
        [0x14A78, "7020c7001000", "invoke-direct", "{v0, v1}, Ljava/io/IOException;-><init>(Ljava/lang/String;)V"],
        [0x14AA8, "5b011100", "iput-object", "v1, v0, Lcom/example/Probe0;->member:Lcom/example/Probe1;"],
        [0x14AC4, "1600e803", "const-wide/16", "v0, #+1000"],
        [0x15220, "d8030301", "add-int/lit8", "v3, v3, #+1"],
        [0x1530E, "0000", "nop", ""],
        [0x14A7E, "0e00", "return-void", ""],
        [0x14A90, "7010c6000000", "invoke-direct", "{v0}, Ljava/io/IOException;-><init>()V"],
        [0x14AD6, "38000f00", "if-eqz", "v0, 0x30"],
        [0x14AE8, "0c00", "move-result-object", "v0"],
        [0x14AF0, "2802", "goto", "0x30"],
        [0x14BE4, "1a010000", "const-string", 'v1, "probe"'],
        [0x14BEC, "62026e00", "sget-object", "v2, Lcom/example/Probe2;->member:Ljava/lang/String;"],
        [0x14BF8, "1101", "return-object", "v1"],
        [0x14C0C, "713046002103", "invoke-static", "{v1, v2, v3}, Lcom/example/Probe3;->member(Ljava/lang/Object;Ljava/util/ArrayList;Ljava/io/File;)Ljava/lang/Object;[]"],
        [0x14C34, "4d020103", "aput-object", "v2, v1, v3"],
        [0x14C46, "710044000000", "invoke-static", "{}, Lcom/example/Probe3;->member"],
        [
            0x14CC0,
            "71404a002143",
            "invoke-static",
            "{v1, v2, v3, v4}, Lcom/example/Probe5;->member(Ljava/lang/Object;Ljava/util/ArrayList;Ljava/io/File;Ljava/util/ArrayList;)Ljava/lang/Object;[]",
        ],
        [0x15202, "35631200", "if-ge", "v3, v6, 0x46"],
        [0x15264, "2113", "array-length", "v3, v1"],
        [0x15268, "b043", "add-int/2addr", "v3, v4"],
        [0x1527A, "715330014142", "invoke-static", "{v1, v4, v2, v4, v3}, Ljava/lang/System;->member(Ljava/lang/Object;ILjava/lang/Object;II)V"],
        [0x152AE, "2311bc00", "new-array", "v1, v1, byte[]"],
        [0x152C0, "94040204", "rem-int", "v4, v2, v4"],
        [0x152E8, "08011500", "move-object/from16", "v1, v21"],
        [0x16A24, "2300bd00", "new-array", "v0, v0, char[]"],
        [0x1802A, "1c04bc00", "const-class", "v4, byte[]"],
        [
            0x1A0D4,
            "760f65020000",
            "invoke-direct/range",
            "{v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14}, Lcom/example/Probe6;-><init>(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V",
        ],
        [0x1B9DA, "2312be00", "new-array", "v2, v1, int[]"],
        [0x1B9EE, "1c05be00", "const-class", "v5, int[]"],
        [
            0x1C7A6,
            "74065a010300",
            "invoke-virtual/range",
            "{v3, v4, v5, v6, v7, v8}, Ljava/nio/channels/FileChannel;->member(Ljava/nio/channels/FileChannel$MapMode;JJ)Ljava/nio/MappedByteBuffer;",
        ],
    ],
}

# what exports record: every probed architecture, each fingerprinted on its own
PROBED_ARCHITECTURES = tuple(ESCAPER_PROBE_INSTRUCTIONS)


def getEscapedProbe(architecture: str = "intel") -> List[str]:
    """Escape the probe instructions exactly as EscapedBlockShingler does, one line each."""
    instructions = ESCAPER_PROBE_INSTRUCTIONS.get(architecture)
    if not instructions:
        raise ValueError("no escaper probe defined for architecture: %s" % architecture)
    # resolving the escaper this way predates smda 4.2.13 only; MCRIT requires >= 4.2.13
    escaper = SmdaFunction.getInstructionEscaper(architecture)
    if escaper is None:
        # an smda without an escaper for it hands back the raw text, which would hash as a value
        raise ValueError("smda has no instruction escaper for architecture: %s" % architecture)
    escaped = []
    for instruction_list in instructions:
        instruction = SmdaInstruction(instruction_list)
        escaped.append("%s %s" % (instruction.getMnemonicGroup(escaper), instruction.getEscapedOperands(escaper)))
    return escaped


def getEscaperFingerprints(architectures: Sequence[str] = PROBED_ARCHITECTURES) -> Dict[str, str]:
    """Per-architecture fingerprints - the shape that gets persisted in exports and status.

    A mapping rather than a combined hash, and deliberately so: the value is *stored*, so a
    later widening of the default probe (adding an architecture, as AArch64, CIL and Dalvik were) must not change what is
    recorded for the architectures already covered. A combined hash would flip for everyone the
    moment the tuple grows, making every export produced before that point mismatch on import
    even though intel escaping never changed - and false positives are the one failure mode a
    diagnostic cannot afford, because they teach operators to ignore it.
    """
    fingerprints = {}
    for architecture in architectures:
        hasher = hashlib.sha256()
        try:
            hasher.update(("[%s]\n" % architecture).encode("utf-8"))
            for line in getEscapedProbe(architecture):
                hasher.update((line + "\n").encode("utf-8"))
        except Exception as exc:
            LOGGER.warning("Could not compute escaper fingerprint for %s (smda too old or API changed): %s", architecture, exc)
            fingerprints[architecture] = FINGERPRINT_UNAVAILABLE
        else:
            fingerprints[architecture] = hasher.hexdigest()[:16]
    return fingerprints


def getEscaperFingerprint(architectures: Sequence[str] = ("intel",)) -> str:
    """A short, stable hash of how the running smda escapes the probe.

    Any change to mnemonic grouping or operand escaping that touches the probe changes this value.
    It says nothing about *how* escaping changed, only that stored minhashes computed under a
    different fingerprint are no longer comparable to freshly computed ones.

    Convenience form over getEscaperFingerprints; for a single architecture both agree. It defaults
    to Intel alone, the value /status has always reported as escaper_fingerprint; persistence uses
    the per-architecture mapping.
    """
    fingerprints = getEscaperFingerprints(architectures)
    if FINGERPRINT_UNAVAILABLE in fingerprints.values():
        return FINGERPRINT_UNAVAILABLE
    return fingerprints[sorted(fingerprints)[0]] if len(fingerprints) == 1 else _combineFingerprints(fingerprints)


def _combineFingerprints(fingerprints: Dict[str, str]) -> str:
    hasher = hashlib.sha256()
    for architecture in sorted(fingerprints):
        hasher.update(("%s:%s\n" % (architecture, fingerprints[architecture])).encode("utf-8"))
    return hasher.hexdigest()[:16]
