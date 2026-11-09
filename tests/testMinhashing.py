#!/usr/bin/python

import logging
import os
import unittest

from smda.common.SmdaReport import SmdaReport

from mcrit.config.McritConfig import McritConfig
from mcrit.config.MinHashConfig import MinHashConfig
from mcrit.config.ShinglerConfig import ShinglerConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.minhash.MinHash import MinHash
from mcrit.minhash.MinHasher import MinHasher
from mcrit.storage.StorageInterface import StorageInterface

from .context import config

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logging.disable(logging.CRITICAL)


class MinHashingTestSuite(unittest.TestCase):
    """Run a full example on a memory dump"""

    def testMinHash(self):
        THIS_FILE_PATH = str(os.path.abspath(__file__))
        PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
        example_file_path = os.sep.join([PROJECT_ROOT, "tests", "example_report.smda"])
        smda_report = SmdaReport.fromFile(example_file_path)
        assert smda_report is not None
        # with open(example_file_path, "r") as fjson:
        #    smda_json = json.load(fjson)
        #    example_functions = [f for key, entry in smda_json["xcfg"].items()]
        shingler_test_config = ShinglerConfig()
        minhasher = MinHasher(config.MINHASH_CONFIG, shingler_test_config)
        # expected: sequence of (little endian) packed int8/32 values
        expected = {0: bytes.fromhex("7e71b7cd63682ed9"), 4424: bytes.fromhex("d018c68a829734ed")}
        tested = []
        for function in smda_report.getFunctions():  # example_functions:
            minhash = minhasher._calculateMinHash(function)
            print(function.offset, minhash.minhash.hex()[:16])
            if function.offset in expected:
                self.assertTrue(minhash.minhash.startswith(expected[function.offset]))
                tested.append(function.offset)
            if function.offset == 356:
                self.assertFalse(minhasher.isMinHashableFunction(function))
                tested.append(356)
        self.assertTrue(set(expected.keys()) <= set(tested))
        self.assertTrue(356 in tested)

    def testMatching(self):
        minhash_a = MinHash(function_id=1, minhash_signature=[0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39])
        minhash_b = MinHash(function_id=2, minhash_signature=[0x30, 0x31, 0x30, 0x33, 0x30, 0x30, 0x30, 0x37, 0x38, 0x39])
        minhash_score = MinHash.calculateMinHashScore(minhash_a.minhash, minhash_b.minhash)
        minhash_score_int = MinHash.calculateMinHashIntScore(minhash_a.minhash_int, minhash_b.minhash_int)
        self.assertEqual(60.0, minhash_score)
        self.assertEqual(minhash_score, minhash_score_int)

    def testBandingVariableSize(self):
        config = McritConfig()
        config.STORAGE_CONFIG = StorageConfig()
        config.STORAGE_CONFIG.STORAGE_BANDS = {2: 2, 3: 1}
        config.STORAGE_CONFIG.STORAGE_BAND_SEED = 0
        # the band projection is drawn from the configured signature length, not from the minhash
        config.MINHASH_CONFIG = MinHashConfig(MINHASH_SIGNATURE_LENGTH=10)
        minhash_a = MinHash(function_id=1, minhash_signature=[0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39])
        minhash_b = MinHash(function_id=2, minhash_signature=[0x30, 0x31, 0x30, 0x33, 0x30, 0x30, 0x30, 0x37, 0x38, 0x39])

        storage = StorageInterface(config)
        band_hashes_a = storage.getBandHashesForMinHash(minhash_a)
        band_hashes_b = storage.getBandHashesForMinHash(minhash_b)

        self.assertEqual(band_hashes_a[0], band_hashes_b[0])
        self.assertEqual(band_hashes_a[2], band_hashes_b[2])
        self.assertNotEqual(band_hashes_a[1], band_hashes_b[1])

    def testBandingVariableSize8Bits(self):
        config = McritConfig()
        config.STORAGE_CONFIG = StorageConfig()
        config.STORAGE_CONFIG.STORAGE_BANDS = {2: 2, 3: 1}
        config.STORAGE_CONFIG.STORAGE_BAND_SEED = 0
        config.MINHASH_CONFIG = MinHashConfig(MINHASH_SIGNATURE_LENGTH=10)
        minhash_a = MinHash(
            function_id=1,
            minhash_signature=[0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39],
            minhash_bits=8,
        )
        minhash_b = MinHash(
            function_id=2,
            minhash_signature=[0x30, 0x31, 0x30, 0x33, 0x30, 0x30, 0x30, 0x37, 0x38, 0x39],
            minhash_bits=8,
        )

        storage = StorageInterface(config)
        band_hashes_a = storage.getBandHashesForMinHash(minhash_a)
        band_hashes_b = storage.getBandHashesForMinHash(minhash_b)

        self.assertEqual(band_hashes_a[0], band_hashes_b[0])
        self.assertEqual(band_hashes_a[2], band_hashes_b[2])
        self.assertNotEqual(band_hashes_a[1], band_hashes_b[1])


if __name__ == "__main__":
    unittest.main()
