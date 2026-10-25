#!/usr/bin/python

import contextlib
import logging
import unittest

from mcrit.config.BandPresets import (
    BAND_PRESETS,
    describeBandProjection,
    getBandProjectionFingerprint,
    validateBandProjection,
)
from mcrit.config.McritConfig import McritConfig
from mcrit.config.MinHashConfig import MinHashConfig
from mcrit.config.QueueConfig import QueueConfig
from mcrit.config.ShinglerConfig import ShinglerConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.minhash.MinHash import MinHash
from mcrit.storage.StorageFactory import StorageFactory

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logging.disable(logging.CRITICAL)


def buildStorage(**storage_kwargs):
    """A memory-backed storage, which is all createBandhashProjection needs."""
    config = McritConfig()
    config.STORAGE_CONFIG = StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MEMORY, **storage_kwargs)
    config.MINHASH_CONFIG = MinHashConfig()
    config.SHINGLER_CONFIG = ShinglerConfig()
    config.QUEUE_CONFIG = QueueConfig()
    return StorageFactory.getStorage(config)


def emptyMinHash(length=64):
    return MinHash(minhash_signature=[0] * length, minhash_bits=8)


class BandPresetTestSuite(unittest.TestCase):
    """The presets are the user-facing half of the explicit strategy, so their shape is part of
    the contract."""

    def testEveryPresetValidates(self):
        for name, projection in BAND_PRESETS.items():
            errors, _warnings = validateBandProjection(projection)
            self.assertEqual([], errors, "preset %s does not validate: %s" % (name, errors))

    def testLegacyPresetsReproduceTheirStrategies(self):
        """The migration property: an instance can move to "explicit" without reindexing, because
        the legacy presets describe exactly the projection its index was built under."""
        for preset, strategy, bands in (
            ("legacy-random-20x4", "random", {4: 20}),
            ("legacy-linear-16x4", "linear", {4: 16}),
        ):
            derived = buildStorage(STORAGE_BAND_STRATEGY=strategy, STORAGE_BANDS=bands)
            explicit = buildStorage(STORAGE_BAND_STRATEGY="explicit", STORAGE_BAND_PRESET=preset)
            minhash = emptyMinHash()
            self.assertEqual(
                derived.createBandhashProjection(minhash),
                explicit.createBandhashProjection(minhash),
                "%s does not reproduce the %s strategy" % (preset, strategy),
            )

    def testSegmentPresetsCoverTheWholeSignature(self):
        for name in ("segment-balanced-v1", "segment-pure-v1", "segment-overlap-v1", "segment-hybrid-v1"):
            description = describeBandProjection(BAND_PRESETS[name])
            self.assertEqual(64, description["coverage"], "%s leaves fields unindexed" % name)
            self.assertGreater(description["pure_block_bands"], 0)
            self.assertGreater(description["pure_stat_bands"], 0)

    def testShippedDefaultsAreHonestAboutTheirGaps(self):
        """The two legacy presets have known weaknesses; validation must surface them rather than
        pass silently, since that is how they went unnoticed."""
        _errors, warnings = validateBandProjection(BAND_PRESETS["legacy-random-20x4"])
        self.assertTrue(any("45 of 64" in warning for warning in warnings))
        _errors, warnings = validateBandProjection(BAND_PRESETS["legacy-linear-16x4"])
        self.assertTrue(any("no band avoids the metric segment" in warning for warning in warnings))


class BandProjectionValidationTestSuite(unittest.TestCase):
    def testRejectsOffsetOutsideSignature(self):
        errors, _ = validateBandProjection([[0, 1, 2, 64]])
        self.assertTrue(any("outside" in error for error in errors))

    def testRejectsRepeatedOffsetWithinBand(self):
        errors, _ = validateBandProjection([[0, 1, 1, 2]])
        self.assertTrue(any("repeats" in error for error in errors))

    def testRejectsEmptyProjectionAndEmptyBand(self):
        errors, _ = validateBandProjection([])
        self.assertTrue(errors)
        errors, _ = validateBandProjection([[0, 1, 2, 3], []])
        self.assertTrue(any("empty" in error for error in errors))

    def testWarnsOnTinyBands(self):
        _errors, warnings = validateBandProjection([[0, 1]])
        self.assertTrue(any("size 2" in warning for warning in warnings))

    def testCoverageIsReportedNotRequired(self):
        """Partial and overlapping projections are legitimate LSH, so an incomplete one warns
        rather than failing."""
        errors, warnings = validateBandProjection([[16, 17, 18, 19]])
        self.assertEqual([], errors)
        self.assertTrue(any("covers 4 of 64" in warning for warning in warnings))


class BandProjectionFingerprintTestSuite(unittest.TestCase):
    def testFingerprintIsStable(self):
        projection = BAND_PRESETS["segment-balanced-v1"]
        self.assertEqual(getBandProjectionFingerprint(projection), getBandProjectionFingerprint(projection))

    def testDifferentProjectionsFingerprintDifferently(self):
        self.assertNotEqual(
            getBandProjectionFingerprint(BAND_PRESETS["segment-pure-v1"]),
            getBandProjectionFingerprint(BAND_PRESETS["segment-balanced-v1"]),
        )

    def testFingerprintIsSensitiveToOffsetOrder(self):
        """A band hash concatenates the field values in projection order, so two projections that
        differ only in ordering produce different band keys and are NOT interchangeable. A
        fingerprint that normalised order would call them identical and defeat its own purpose."""
        forward = [[5, 17, 33, 60]]
        reversed_band = [[60, 33, 17, 5]]
        self.assertNotEqual(getBandProjectionFingerprint(forward), getBandProjectionFingerprint(reversed_band))

    def testFingerprintDependsOnSignatureLength(self):
        projection = [[0, 1, 2, 3]]
        self.assertNotEqual(
            getBandProjectionFingerprint(projection, signature_length=64),
            getBandProjectionFingerprint(projection, signature_length=32),
        )


class ExplicitStrategyTestSuite(unittest.TestCase):
    def testProjectionFromPreset(self):
        storage = buildStorage(STORAGE_BAND_STRATEGY="explicit", STORAGE_BAND_PRESET="segment-balanced-v1")
        projection = storage.createBandhashProjection(emptyMinHash())
        self.assertEqual(15, len(projection))
        self.assertEqual(15, storage._storage_config.STORAGE_NUM_BANDS)

    def testProjectionFromLiteralAllowsMixedBandSizes(self):
        """The constraint the linear strategy enforces - band_size * num_bands == signature length -
        is exactly what forbids per-segment band sizes, and explicit does without it."""
        storage = buildStorage(
            STORAGE_BAND_STRATEGY="explicit",
            STORAGE_BAND_PROJECTION=[[16, 17, 18, 19], [0, 1, 2, 3, 4, 5]],
        )
        projection = storage.createBandhashProjection(emptyMinHash())
        self.assertEqual([4, 6], sorted(len(band) for band in projection.values()))
        self.assertEqual(2, storage._storage_config.STORAGE_NUM_BANDS)

    def testNumBandsIgnoresStorageBandsWhenExplicit(self):
        storage = buildStorage(
            STORAGE_BAND_STRATEGY="explicit",
            STORAGE_BAND_PRESET="segment-balanced-v1",
            STORAGE_BANDS={4: 20},
        )
        self.assertEqual(15, storage._storage_config.STORAGE_NUM_BANDS)

    def testPresetWinsOverLiteralProjection(self):
        storage = buildStorage(
            STORAGE_BAND_STRATEGY="explicit",
            STORAGE_BAND_PRESET="segment-pure-v1",
            STORAGE_BAND_PROJECTION=[[0, 1, 2, 3]],
        )
        self.assertEqual(16, storage._storage_config.STORAGE_NUM_BANDS)

    def testUnknownPresetIsRejectedAtStartup(self):
        """Storage construction reads STORAGE_NUM_BANDS, which resolves the projection, so a
        misspelled preset fails immediately rather than at the first query."""
        with self.assertRaises(AttributeError) as context:
            buildStorage(STORAGE_BAND_STRATEGY="explicit", STORAGE_BAND_PRESET="no-such-preset")
        self.assertIn("no-such-preset", str(context.exception))
        # the message lists what it could have been
        self.assertIn("segment-balanced-v1", str(context.exception))

    def testInvalidProjectionIsRejected(self):
        storage = buildStorage(STORAGE_BAND_STRATEGY="explicit", STORAGE_BAND_PROJECTION=[[0, 1, 2, 999]])
        with self.assertRaises(AttributeError):
            storage.createBandhashProjection(emptyMinHash())

    def testFingerprintOnlyReportedForExplicitStrategy(self):
        explicit = buildStorage(STORAGE_BAND_STRATEGY="explicit", STORAGE_BAND_PRESET="segment-balanced-v1")
        self.assertIsNotNone(explicit.getBandProjectionFingerprint())
        derived = buildStorage(STORAGE_BAND_STRATEGY="random")
        self.assertIsNone(derived.getBandProjectionFingerprint())

    def testBandHashesDifferWhenOffsetOrderDiffers(self):
        """The reason the fingerprint and the presets both preserve order: identical fields in a
        different order produce a different band key, so an index built under one is not readable
        by the other."""
        minhash = MinHash(minhash_signature=list(range(64)), minhash_bits=8)
        forward = buildStorage(STORAGE_BAND_STRATEGY="explicit", STORAGE_BAND_PROJECTION=[[5, 17, 33, 60]])
        backward = buildStorage(STORAGE_BAND_STRATEGY="explicit", STORAGE_BAND_PROJECTION=[[60, 33, 17, 5]])
        self.assertNotEqual(forward.getBandHashesForMinHash(minhash), backward.getBandHashesForMinHash(minhash))


class RandomStrategyTestSuite(unittest.TestCase):
    """A process derives its projection once and bands every later minhash under it, so the
    minhash it happens to band first - an imported one, say - must not decide it."""

    def testProjectionDependsOnTheConfigurationAlone(self):
        reference = buildStorage(STORAGE_BAND_STRATEGY="random").createBandhashProjection(emptyMinHash())
        for length in (8, 63, 80):
            self.assertEqual(reference, buildStorage(STORAGE_BAND_STRATEGY="random").createBandhashProjection(emptyMinHash(length)), length)

    def testAWrongLengthFirstMinHashLeavesLaterBandHashesAlone(self):
        """A short first minhash re-keyed every band hash the process computed afterwards, a long
        one put positions past the signature into the projection, so that every later one raised."""
        minhash = MinHash(minhash_signature=list(range(64)), minhash_bits=8)
        expected = buildStorage().getBandHashesForMinHash(minhash)
        for length in (8, 80):
            storage = buildStorage()
            # whether the odd one is banded at all is not the point here
            with contextlib.suppress(IndexError):
                storage.getBandHashesForMinHash(MinHash(minhash_signature=[1] * length, minhash_bits=8))
            self.assertEqual(expected, storage.getBandHashesForMinHash(minhash), length)


if __name__ == "__main__":
    unittest.main()
