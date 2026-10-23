#!/usr/bin/python

import base64
import json
import logging
import os
import unittest

from mcrit.libs.utility import B85_SLICE_SIZE, b85encode_sliced, compress_encode, compress_encode_fragments, decompress_decode, generate_unique_pairs

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logging.disable(logging.CRITICAL)


class TestUtility(unittest.TestCase):
    """Run a full example on a memory dump"""

    def testUniquePairGenerator(self):
        input_data = [1, 2, 3, 4]
        expected_pairs = [(1, 2), (1, 3), (1, 4), (2, 3), (2, 4), (3, 4)]
        pairs = generate_unique_pairs(input_data)
        generated_pairs = [p for p in pairs]
        self.assertEqual(expected_pairs, generated_pairs)

    def testZip(self):
        content = {"test_key": "test data"}
        zip_b64 = compress_encode(json.dumps(content))
        decompressed = decompress_decode(zip_b64)
        self.assertEqual(content, json.loads(decompressed))

    def testZipFragments(self):
        # the export compresses a sample's entries as they are serialised, without joining them first
        fragments = [b"{", b'"1": {"name": "f\\u00e9"}', b', "2": [1, 2]', b"}"]
        self.assertEqual({"1": {"name": "f\u00e9"}, "2": [1, 2]}, json.loads(decompress_decode(compress_encode_fragments(fragments))))

    def testSlicedBase85IsBase85(self):
        for size in (0, 3, 4, 5, B85_SLICE_SIZE - 1, B85_SLICE_SIZE, B85_SLICE_SIZE + 3, 2 * B85_SLICE_SIZE + 6):
            with self.subTest(size=size):
                data = os.urandom(size)
                self.assertEqual(base64.b85encode(data).decode(), b85encode_sliced(data))


if __name__ == "__main__":
    unittest.main()
