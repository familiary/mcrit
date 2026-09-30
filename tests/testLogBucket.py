#!/usr/bin/python3

import os
import unittest
from unittest import mock

import mcrit.shinglers.LogBucket as log_bucket_module
from mcrit.config.ShinglerConfig import ShinglerConfig
from mcrit.minhash.ShingleLoader import ShingleLoader
from mcrit.shinglers.LogBucket import LogBucket

from .context import config  # noqa: F401  (keeps the shared test bootstrap consistent)

CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(log_bucket_module.__file__))), "cache")


class LogBucketTest(unittest.TestCase):
    """Values outside the precomputed table must not abort indexing.

    LogBucket precomputes value -> bucket range for 0..max_value-1. FuzzyStatPairShingler buckets
    max_block_size, num_ins_C, num_ins_S and num_calls through it, and none of those are bounded -
    only stack_size is clamped, at the call site. A real corpus produced a basic block of 108,837
    bytes, and the resulting KeyError propagated out of the hashing pool and killed the whole
    indexing job, leaving the corpus half-hashed.
    """

    MAX_VALUE = 1024

    def setUp(self):
        self.buckets = LogBucket(self.MAX_VALUE, 1)
        self.highest = self.MAX_VALUE - 1
        self.lowest = 0

    def testValueAboveTableDoesNotRaise(self):
        # the exact shape that killed indexing: a basic block far larger than the table's top
        self.assertEqual(self.buckets.getLogBucketRange(self.highest * 106), self.buckets.getLogBucketRange(self.highest))

    def testFirstValueOutsideTableDoesNotRaise(self):
        # off-by-one at the boundary: the first key past the top was the one that was missing
        self.assertEqual(self.buckets.getLogBucketRange(self.highest + 1), self.buckets.getLogBucketRange(self.highest))

    def testNegativeValueDoesNotRaise(self):
        self.assertEqual(self.buckets.getLogBucketRange(-1), self.buckets.getLogBucketRange(self.lowest))

    def testValuesInsideTableAreUnchanged(self):
        """The clamp must not move any value that was already defined.

        This is what makes the fix safe to apply to an existing corpus: every MinHash computed
        before it stays computable to the same value, because only previously-raising inputs
        changed behaviour.
        """
        for buckets in (self.buckets, LogBucket()):
            for value, bucket_range in buckets._value_to_bucket_range.items():
                if buckets.getLogBucketRange(value) != bucket_range:
                    self.fail(f"value {value} of the {len(buckets._value_to_bucket_range)}-entry table moved bucket")

    def testClampTargetsAreTheTableEnds(self):
        # compared with the table itself, on a table whose two top and two bottom values differ:
        # near the top neighbouring values often share a range (1022 and 1023 do), so comparing
        # two clamped lookups with each other would not notice clamping to the wrong entry
        buckets = LogBucket(64, 1)
        table = buckets._value_to_bucket_range
        self.assertNotEqual(table[62], table[63])
        self.assertNotEqual(table[0], table[1])
        self.assertEqual(table[63], buckets.getLogBucketRange(64 * 106))
        self.assertEqual(table[0], buckets.getLogBucketRange(-1))

    def testClampTargetsAreRealKeys(self):
        """Guards the assumption the clamp relies on: both bounds are present in the table."""
        self.assertIn(self.highest, self.buckets._value_to_bucket_range)
        self.assertIn(self.lowest, self.buckets._value_to_bucket_range)


class LogBucketTableTest(unittest.TestCase):
    """The table must be the one for the parameters asked for (#202, #215).

    The cached file used to be named logbuckets.json whatever the parameters, and was loaded
    whenever it existed, so every instance got the table built first - in a shipped package that
    is the 100,000/1 default, which made SHINGLER_LOGBUCKETS and SHINGLER_LOGBUCKET_RANGE dead
    settings.

    Whole tables are compared with assertTrue(a == b): assertEqual would render a diff of
    100,000 entries on failure, which takes minutes.
    """

    def setUp(self):
        # every test starts from a process that has not loaded or built any table yet
        patcher = mock.patch.dict(LogBucket._tables, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def testTableCoversExactlyTheRequestedValues(self):
        for max_value, bucket_width in [(1024, 1), (16, 2), (1, 0)]:
            with self.subTest(max_value=max_value, bucket_width=bucket_width):
                self.assertEqual(sorted(LogBucket(max_value, bucket_width)._value_to_bucket_range), list(range(max_value)))

    def testBucketWidthIsHonoured(self):
        # every range holds the centre bucket and `width` buckets either side (padded with negative
        # values below the width); (100000, 2) shares max_value with the shipped table, so a lookup
        # that ignored the width would be served the width-1 table here
        for max_value, bucket_width in [(64, 0), (64, 1), (64, 2), (64, 4), (100000, 2)]:
            with self.subTest(max_value=max_value, bucket_width=bucket_width):
                table = LogBucket(max_value, bucket_width)._value_to_bucket_range
                widths = {len(table[value]) for value in range(max_value)}
                self.assertEqual({2 * bucket_width + 1}, widths)

    def testDefaultsAreServedFromTheShippedTable(self):
        with mock.patch.object(LogBucket, "_buildTable", side_effect=AssertionError("built the default table instead of loading it")):
            buckets = LogBucket()
        self.assertTrue(sorted(buckets._value_to_bucket_range) == list(range(100000)), "the shipped table does not cover 0..99999")

    def testShippedTableIsWhatTheBuilderProduces(self):
        """Renaming the shipped file must not change a single default MinHash.

        It also pins that a table built for other parameters comes from the same algorithm as
        the one every existing corpus was hashed with.
        """
        shipped = LogBucket()
        self.assertTrue(shipped._value_to_bucket_range == shipped._buildTable(), "the shipped table differs from a fresh build")

    def testEachParameterPairGetsItsOwnTable(self):
        narrow, default, wide = LogBucket(1024, 1), LogBucket(), LogBucket(1024, 2)
        self.assertEqual((1024, 100000, 1024), tuple(len(buckets._value_to_bucket_range) for buckets in (narrow, default, wide)))
        self.assertEqual((3, 5), (len(narrow.getLogBucketRange(500)), len(wide.getLogBucketRange(500))))
        # a second instance with the same parameters reuses the table instead of building it again
        self.assertIs(narrow._value_to_bucket_range, LogBucket(1024, 1)._value_to_bucket_range)

    def testBuildingOpensNoFile(self):
        """A table for other parameters stays in memory: the package directory may be read-only,
        and a file written there by one worker could be read half-written by the next."""
        before = sorted(os.listdir(CACHE_DIR))
        with mock.patch("builtins.open", side_effect=AssertionError("LogBucket opened a file")):
            LogBucket(512, 2)
        self.assertEqual(before, sorted(os.listdir(CACHE_DIR)))

    def testInvalidParametersRaise(self):
        for max_value, bucket_width in [(0, 1), (-5, 1), (1024, -1)]:
            with self.subTest(max_value=max_value, bucket_width=bucket_width), self.assertRaisesRegex(ValueError, "needs max_value >= 1 and bucket_width >= 0"):
                LogBucket(max_value, bucket_width)

    def testNonIntegerParametersRaise(self):
        # a float or bool would otherwise name a file that does not exist, or fail deep inside the build
        for max_value, bucket_width in [(100000.0, 1), (100000, 1.0), (100000, True), ("1024", 1)]:
            with self.subTest(max_value=max_value, bucket_width=bucket_width), self.assertRaisesRegex(TypeError, "needs an int"):
                LogBucket(max_value, bucket_width)

    def testWidthTheBuilderCannotServeIsRefused(self):
        # (2000, 5) and (2000, 8) left values without a range, whose lookup raised KeyError; (7, 5) gave
        # value 5 a wrong one-bucket range; (5, 4) gave value 4 eight buckets instead of nine; (3, 4)
        # made the builder itself raise IndexError
        for max_value, bucket_width in [(2000, 5), (2000, 8), (7, 5), (5, 4), (3, 4)]:
            with self.subTest(max_value=max_value, bucket_width=bucket_width), self.assertRaisesRegex(ValueError, "too wide"):
                LogBucket(max_value, bucket_width)

    def testEveryWidthUpToFourIsServed(self):
        for bucket_width in range(5):
            with self.subTest(bucket_width=bucket_width):
                self.assertEqual(2000, len(LogBucket(2000, bucket_width)._value_to_bucket_range))


class LogBucketSettingsTest(unittest.TestCase):
    """SHINGLER_LOGBUCKETS and SHINGLER_LOGBUCKET_RANGE must reach the shingler that hashes with them."""

    @staticmethod
    def _fuzzyStatPairShingler(shingler_config):
        return next(shingler for shingler in ShingleLoader(shingler_config).getShinglers() if shingler.getName() == "FuzzyStatPairShingler")

    def testConfiguredTableIsTheOneTheShinglerUses(self):
        log_buckets = self._fuzzyStatPairShingler(ShinglerConfig(SHINGLER_LOGBUCKETS=1024, SHINGLER_LOGBUCKET_RANGE=2))._log_buckets
        self.assertEqual(1024, len(log_buckets._value_to_bucket_range))
        self.assertEqual(5, len(log_buckets.getLogBucketRange(500)))

    def testDefaultConfigurationUsesTheShippedTable(self):
        log_buckets = self._fuzzyStatPairShingler(ShinglerConfig())._log_buckets
        self.assertEqual((100000, 3), (len(log_buckets._value_to_bucket_range), len(log_buckets.getLogBucketRange(500))))

    def testRefusedWidthFailsWhenTheShinglersAreLoaded(self):
        # ShingleLoader instantiates every shingler to learn its name, so this holds with a weight of 0 as well
        for weights in ({"FuzzyStatPairShingler": 1, "EscapedBlockShingler": 3}, {"FuzzyStatPairShingler": 0, "EscapedBlockShingler": 3}):
            with self.subTest(weights=weights), self.assertRaisesRegex(ValueError, "too wide"):
                ShingleLoader(ShinglerConfig(SHINGLER_LOGBUCKET_RANGE=5, SHINGLERS_WEIGHTS=weights)).getShinglers()


if __name__ == "__main__":
    unittest.main()
