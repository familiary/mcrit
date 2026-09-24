#!/usr/bin/python

import logging
import unittest

from mcrit.shinglers.AbstractShingler import AbstractShingler

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logging.disable(logging.CRITICAL)


class DummyShingler(AbstractShingler):
    def _generateByteSequences(self, function_object):
        return []


class MinHashingTestSuite(unittest.TestCase):
    """Run a full example on a memory dump"""

    def testBucketing(self):
        shingler = DummyShingler("dummy")
        test_pairs = {
            0: (0, 0, 1),
            2: (1, 2, 3),
            3: (2, 3, 4),
            4: (3, 4, 6),
            5: (4, 6, 8),
            11: (10, 12, 14),
            12: (10, 12, 14),
            16: (14, 16, 20),
            18: (16, 20, 24),
            29: (28, 32, 36),
            32: (28, 32, 36),
            33: (32, 36, 40),
            59: (56, 60, 64),
            89: (88, 96, 104),
            1197: (1184, 1216, 1248),
        }
        for input_value, expected in sorted(test_pairs.items()):
            self.assertEqual(expected, shingler._getLogBucketRange(input_value))


if __name__ == "__main__":
    unittest.main()
