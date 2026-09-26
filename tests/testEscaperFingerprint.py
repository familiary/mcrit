#!/usr/bin/python

import logging
import unittest
from unittest.mock import patch

from smda.common.SmdaFunction import SmdaFunction
from smda.common.SmdaInstruction import SmdaInstruction

from mcrit.minhash.EscaperFingerprint import (
    ESCAPER_PROBE_INSTRUCTIONS,
    FINGERPRINT_UNAVAILABLE,
    getEscapedProbe,
    getEscaperFingerprint,
    getEscaperFingerprints,
)

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logging.disable(logging.CRITICAL)


class EscaperFingerprintTestSuite(unittest.TestCase):
    """MinHashes are only comparable across functions escaped by the same smda, so the escaper's
    observable behaviour needs to be expressible as a value that can be compared."""

    def testFingerprintIsDeterministic(self):
        self.assertEqual(getEscaperFingerprint(), getEscaperFingerprint())

    def testFingerprintIsAvailableForSupportedSmda(self):
        # MCRIT requires smda >= 4.2.13, which exposes SmdaFunction.getInstructionEscaper
        self.assertTrue(hasattr(SmdaFunction, "getInstructionEscaper"))
        self.assertNotEqual(FINGERPRINT_UNAVAILABLE, getEscaperFingerprint())

    def testFingerprintIsShortAndHexadecimal(self):
        fingerprint = getEscaperFingerprint()
        self.assertEqual(16, len(fingerprint))
        int(fingerprint, 16)

    def testPerArchitectureMappingAgreesWithTheScalar(self):
        """For Intel, the scalar's default, mapping and scalar must agree - exports store the
        mapping, while status reports the scalar next to it."""
        self.assertEqual(getEscaperFingerprint(), getEscaperFingerprints()["intel"])
        self.assertEqual({"intel": getEscaperFingerprint()}, getEscaperFingerprints(("intel",)))

    def testProbeCoversEveryInstruction(self):
        escaped = getEscapedProbe("intel")
        self.assertEqual(len(ESCAPER_PROBE_INSTRUCTIONS["intel"]), len(escaped))
        self.assertTrue(all(line.strip() for line in escaped))

    def testEveryArchitectureWithMinHashesIsProbed(self):
        # MCRIT computes minhashes for these (#93); an escaper change in any of them must show
        fingerprints = getEscaperFingerprints()
        self.assertEqual({"intel", "aarch64", "cil", "dalvik"}, set(fingerprints))
        self.assertNotIn(FINGERPRINT_UNAVAILABLE, fingerprints.values())
        self.assertEqual(len(set(fingerprints.values())), len(fingerprints))

    def testEachArchitectureIsFingerprintedOnItsOwn(self):
        # adding an architecture must not change what exports recorded for the others
        for architecture in ESCAPER_PROBE_INSTRUCTIONS:
            self.assertEqual(getEscaperFingerprints((architecture,)), {architecture: getEscaperFingerprints()[architecture]})

    def testAnArchitectureSmdaHasNoEscaperForIsUnavailable(self):
        # an older smda answers None for an architecture it lacks, and escapes to raw text with it
        with patch.object(SmdaFunction, "getInstructionEscaper", return_value=None):
            self.assertEqual({"dalvik": FINGERPRINT_UNAVAILABLE}, getEscaperFingerprints(("dalvik",)))

    def testEveryProbeLineIsEscaped(self):
        for architecture, instructions in ESCAPER_PROBE_INSTRUCTIONS.items():
            escaped = getEscapedProbe(architecture)
            self.assertEqual(len(instructions), len(escaped))
            self.assertTrue(all(line.strip() for line in escaped), architecture)

    def testUnknownArchitectureIsRejected(self):
        with self.assertRaises(ValueError):
            getEscapedProbe("not-an-architecture")

    def testFingerprintIsUnavailableWhenEscaperCannotBeResolved(self):
        # smda releases before 4.2.13 have no getInstructionEscaper; the fingerprint is a
        # diagnostic and must degrade instead of breaking callers such as getStatus()
        original = SmdaFunction.getInstructionEscaper
        try:
            del SmdaFunction.getInstructionEscaper
            self.assertEqual(FINGERPRINT_UNAVAILABLE, getEscaperFingerprint())
        finally:
            SmdaFunction.getInstructionEscaper = original

    def testFingerprintReactsToAnEscaperChange(self):
        """The regression this exists to catch: smda 4.4.5 stopped escaping segment-qualified
        memory operands as CONST, which silently changed ~20% of minhashes on a real corpus."""
        baseline = getEscaperFingerprint()
        escaper = SmdaFunction.getInstructionEscaper("intel")
        original = escaper.escapeOperands.__func__ if hasattr(escaper.escapeOperands, "__func__") else escaper.escapeOperands
        try:
            escaper.escapeOperands = staticmethod(lambda instruction: "CHANGED")
            self.assertNotEqual(baseline, getEscaperFingerprint())
        finally:
            escaper.escapeOperands = staticmethod(original)
        self.assertEqual(baseline, getEscaperFingerprint())

    def testProbeExercisesSegmentQualifiedMemoryOperands(self):
        """The probe must keep covering the operand class behind the 4.4.5 change, otherwise the
        fingerprint would not have noticed it."""
        operands = [instruction[3] for instruction in ESCAPER_PROBE_INSTRUCTIONS["intel"]]
        segment_memory = [operand for operand in operands if ":" in operand and "[" in operand]
        self.assertTrue(segment_memory, "probe lost its segment-qualified memory operands")
        far_pointer = [operand for operand in operands if ":" in operand and "[" not in operand]
        self.assertTrue(far_pointer, "probe lost its far-pointer operand")

    def testEveryProbeInstructionEscapesIndividually(self):
        """A single malformed entry would degrade the WHOLE fingerprint to "unavailable" (the
        module degrades loudly on purpose), so rot in the probe table must be attributable to a
        specific instruction rather than surfacing as a blanket loss of provenance."""
        escaper = SmdaFunction.getInstructionEscaper("intel")
        for instruction_list in ESCAPER_PROBE_INSTRUCTIONS["intel"]:
            instruction = SmdaInstruction(instruction_list)
            group = instruction.getMnemonicGroup(escaper)
            self.assertTrue(group, "probe instruction lost its mnemonic group: %r" % (instruction_list,))
            # exercising getEscapedOperands per entry too, so an exception names its culprit here
            instruction.getEscapedOperands(escaper)


if __name__ == "__main__":
    unittest.main()
