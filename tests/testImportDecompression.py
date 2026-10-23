import base64
import io
import json
import logging
import os
import unittest
import zipfile
from unittest.mock import patch

import falcon
import falcon.testing
from smda.common.SmdaReport import SmdaReport

from mcrit.index.MinHashIndex import ImportRejected, MinHashIndex
from mcrit.libs.utility import MAX_DECOMPRESSION_RATIO, MIN_DECOMPRESSED_SIZE, compress_encode, decompress_decode
from mcrit.server.StatusResource import StatusResource

from .context import config

logging.disable(logging.CRITICAL)

EXAMPLE_REPORT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "example_report.smda")
EXAMPLE_REPORT_2 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "example_report_2.smda")


def zip_member(content, compress_type):
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", compress_type) as zip_file:
        zip_file.writestr("content", content)
    return base64.b85encode(zip_buffer.getvalue()).decode()


def deflated_at_ratio(content, ratio):
    """content deflated, padded with a stored member of random bytes until it inflates to `ratio` times the encoded blob"""

    def build(padding):
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr("content", content)
            if padding:
                zip_file.writestr(zipfile.ZipInfo("padding"), os.urandom(padding), compress_type=zipfile.ZIP_STORED)
        return base64.b85encode(zip_buffer.getvalue()).decode()

    return build(max(0, int((len(content) / ratio - len(build(0))) / 1.25) + 64))


def corrupted(encoded):
    """encoded content with the start of its deflate stream flipped, so that inflating it fails"""
    data = bytearray(base64.b85decode(encoded))
    with zipfile.ZipFile(io.BytesIO(bytes(data))) as zip_file:
        info = zip_file.getinfo("content")
    start = info.header_offset + 30 + len(info.filename) + len(info.extra)
    for offset in range(start, start + 40):
        data[offset] ^= 0xFF
    return base64.b85encode(bytes(data)).decode()


def encrypted(encoded):
    """encoded content with its member flagged as encrypted, which zipfile asks a password for"""
    data = bytearray(base64.b85decode(encoded))
    # general purpose flags of the local file header (offset 6) and central directory entry (offset 8)
    data[6] |= 1
    data[bytes(data).rfind(b"PK\x01\x02") + 8] |= 1
    return base64.b85encode(bytes(data)).decode()


class DecompressionLimitTestSuite(unittest.TestCase):
    """Compressed import content was inflated without a limit in the server process: a 1,773 byte
    POST /import with a bzip2 member grew the server by 1 GiB, and a few KB more exhausted any host"""

    def testExportContentStillRoundTrips(self):
        content = json.dumps({str(index): {"function_name": "sub_%x" % index, "offset": index} for index in range(1000)})
        self.assertEqual(content.encode(), decompress_decode(compress_encode(content)))
        self.assertEqual(b"", decompress_decode(compress_encode("")))

    def testOnlyDeflatedOrStoredContentIsInflated(self):
        # bzip2 and LZMA members inflate in one piece, which no read size can bound
        for compress_type in (zipfile.ZIP_BZIP2, zipfile.ZIP_LZMA):
            with self.subTest(compress_type=compress_type):
                with self.assertRaises(ValueError):
                    decompress_decode(zip_member("{}", compress_type))
        self.assertEqual(b"{}", decompress_decode(zip_member("{}", zipfile.ZIP_STORED)))

    def testContentInflatingBeyondTheRatioIsRefused(self):
        bomb = compress_encode(b"0" * (16 * 1024 * 1024))
        self.assertLess(len(bomb) * MAX_DECOMPRESSION_RATIO, 16 * 1024 * 1024)
        with self.assertRaises(ValueError):
            decompress_decode(bomb)

    def testRatioLeavesHeadroomForExportsOnly(self):
        # exports inflate 3-12x; at 100x, json.loads of [{},{},...] made a 0.5 MB import cost 1.3 GB
        content = b'"' + b"A" * (12 * 1024 * 1024) + b'"'
        with self.assertRaises(ValueError):
            decompress_decode(deflated_at_ratio(content, 25))
        self.assertEqual(content, decompress_decode(deflated_at_ratio(content, 15)))

    def testSmallContentIsNeverRefusedForItsRatio(self):
        # highly repetitive but small: below the floor the ratio does not matter
        content = b'"' + b"A" * (MIN_DECOMPRESSED_SIZE // 2) + b'"'
        encoded = compress_encode(content)
        self.assertGreater(len(content), MAX_DECOMPRESSION_RATIO * len(encoded))
        self.assertEqual(content, decompress_decode(encoded))
        beyond = compress_encode(b'"' + b"A" * (MIN_DECOMPRESSED_SIZE + 1024) + b'"')
        with self.assertRaises(ValueError):
            decompress_decode(beyond)

    def testUnderstatedSizeIsNoWayAround(self):
        # the member's stated size is the uploader's to choose: understated, zipfile stops reading
        # there and its CRC check refuses the rest, so nothing beyond it is inflated
        zip_buffer = io.BytesIO(base64.b85decode(compress_encode(b"0" * (16 * 1024 * 1024))))
        data = bytearray(zip_buffer.getvalue())
        with zipfile.ZipFile(io.BytesIO(bytes(data))) as zip_file:
            info = zip_file.getinfo("content")
        stated_size = (1024).to_bytes(4, "little")
        # local file header (offset 22) and central directory entry (offset 24 from its signature)
        data[info.header_offset + 22 : info.header_offset + 26] = stated_size
        central_directory = bytes(data).rfind(b"PK\x01\x02")
        data[central_directory + 24 : central_directory + 28] = stated_size
        with self.assertRaises(ValueError):
            decompress_decode(base64.b85encode(bytes(data)).decode())

    def testContentIsHeldToWhatRemainsOfTheImport(self):
        content = json.dumps({str(index): {"offset": index} for index in range(1000)}).encode()
        encoded = compress_encode(content)
        self.assertEqual(content, decompress_decode(encoded, max_size=len(content)))
        with self.assertRaisesRegex(ValueError, "left of the import's budget"):
            decompress_decode(encoded, max_size=len(content) - 1)
        # beyond the content's own cap, its own cap holds
        bomb = compress_encode(b"0" * (16 * 1024 * 1024))
        with self.assertRaisesRegex(ValueError, "times its size"):
            decompress_decode(bomb, max_size=10**12)

    def testContentThatDoesNotInflateIsRefusedAsValueError(self):
        # zlib.error and RuntimeError escaped as they were, and POST /import answered them with 500
        content = json.dumps({str(index): {"offset": index} for index in range(1000)})
        for unreadable in (corrupted(compress_encode(content)), encrypted(compress_encode(content)), compress_encode(content)[:-40]):
            with self.assertRaises(ValueError):
                decompress_decode(unreadable)


class ImportDecompressionTestSuite(unittest.TestCase):
    def _export(self):
        index = MinHashIndex(config)
        with open(EXAMPLE_REPORT) as fjson:
            report = SmdaReport.fromDict(json.load(fjson))
        assert report is not None
        index.getStorage().addSmdaReport(report)
        return index.getExportData(compress_data=True)

    def _post_import(self, index, body):
        request = falcon.Request(falcon.testing.create_environ(path="/import", method="POST", body=body))
        response = falcon.Response()
        StatusResource(index).on_post_import(request, response)
        return response

    def testCompressedExportStillImports(self):
        export = self._export()
        index = MinHashIndex(config)
        response = self._post_import(index, json.dumps(export))
        self.assertEqual(falcon.HTTP_200, response.status)
        self.assertEqual(1, json.loads(response.data)["data"]["num_samples_imported"])
        self.assertEqual(export["content"]["num_functions"], len(index.getStorage().getFunctions(0, 0)))

    def testExportWithoutValidVersionIsAnsweredAsBadInput(self):
        for version in ("invalid", None):
            export = self._export()
            export["config"]["version"] = version
            response = self._post_import(MinHashIndex(config), json.dumps(export))
            self.assertEqual(falcon.HTTP_400, response.status)
            self.assertIn("Import rejected", json.loads(response.data)["data"]["message"])
        export = self._export()
        del export["config"]["version"]
        response = self._post_import(MinHashIndex(config), json.dumps(export))
        self.assertEqual(falcon.HTTP_400, response.status)

    def testBombIsAnsweredAsBadInput(self):
        export = self._export()
        for sha256 in export["function_entries"]:
            export["function_entries"][sha256] = zip_member(b"0" * (4 * 1024 * 1024), zipfile.ZIP_BZIP2)
        index = MinHashIndex(config)
        response = self._post_import(index, json.dumps(export))
        self.assertEqual(falcon.HTTP_400, response.status)
        self.assertEqual("failed", json.loads(response.data)["status"])
        self.assertEqual([], index.getStorage().getFunctions(0, 0))
        self.assertEqual([], index.getStorage().getSampleIds())

    def testDenseJsonAtTheFormerRatioIsAnsweredAsBadInput(self):
        # one empty object per three bytes is 24 bytes of Python objects per byte once json.loads has it
        export = self._export()
        for sha256 in export["function_entries"]:
            export["function_entries"][sha256] = deflated_at_ratio(b"[" + b"{}," * (4 * 1024 * 1024) + b"{}]", 100)
        index = MinHashIndex(config)
        response = self._post_import(index, json.dumps(export))
        self.assertEqual(falcon.HTTP_400, response.status)
        self.assertEqual([], index.getStorage().getFunctions(0, 0))
        self.assertEqual([], index.getStorage().getSampleIds())

    def _twoSampleExport(self):
        index = MinHashIndex(config)
        for path in (EXAMPLE_REPORT, EXAMPLE_REPORT_2):
            with open(path) as fjson:
                report = SmdaReport.fromDict(json.load(fjson))
            assert report is not None
            index.getStorage().addSmdaReport(report)
        return json.loads(json.dumps(index.getExportData(compress_data=True)))

    def testRefusedMemberOfLaterSampleStoresNothing(self):
        for refused in (zip_member(b"0" * 1024, zipfile.ZIP_BZIP2), compress_encode(b'"' + b"0" * (MIN_DECOMPRESSED_SIZE + 1024) + b'"')):
            export = self._twoSampleExport()
            clean = json.loads(json.dumps(export))
            second_sha256 = list(export["function_entries"])[1]
            export["function_entries"][second_sha256] = refused
            index = MinHashIndex(config)
            response = self._post_import(index, json.dumps(export))
            self.assertEqual(falcon.HTTP_400, response.status)
            self.assertEqual([], index.getStorage().getSampleIds())
            self.assertEqual([], index.getStorage().getFunctions(0, 0))
            # the clean export imports fully afterwards
            response = self._post_import(index, json.dumps(clean))
            self.assertEqual(falcon.HTTP_200, response.status)
            self.assertEqual(2, json.loads(response.data)["data"]["num_samples_imported"])
            self.assertEqual(clean["content"]["num_functions"], len(index.getStorage().getFunctions(0, 0)))

    def testImportIsCappedAsAWhole(self):
        # each sample's content below its own cap, all of them together beyond the import's (#300):
        # four 70 KB blobs inflated to 32 MiB, and the cost grew with the number of samples
        export = self._twoSampleExport()
        inflated = sum(len(decompress_decode(content)) for content in export["function_entries"].values())
        with patch("mcrit.index.MinHashIndex.MIN_IMPORT_DECOMPRESSED_SIZE", 0):
            index = MinHashIndex(config)
            with self.assertRaisesRegex(ImportRejected, "left of the import's budget"):
                index.addImportData(json.loads(json.dumps(export)), import_size=(inflated - 1) // MAX_DECOMPRESSION_RATIO)
            self.assertEqual([], index.getStorage().getSampleIds())
            report = index.addImportData(json.loads(json.dumps(export)), import_size=-(-inflated // MAX_DECOMPRESSION_RATIO))
            self.assertEqual(2, report["num_samples_imported"])

    def testExportsInflatingAsTheLiveCorpusDoesPassTheWholeImportCap(self):
        # each sample's content at 12 times its size, beyond the 11.6 measured at most, and the body as
        # its size: an import is capped at 20 times that
        export = self._twoSampleExport()
        for sha256, content in export["function_entries"].items():
            function_entries = json.loads(decompress_decode(content))
            for function_entry in function_entries.values():
                function_entry["function_name"] = "A" * 100000
            export["function_entries"][sha256] = deflated_at_ratio(json.dumps(function_entries).encode(), 12)
        body = json.dumps(export)
        inflated = sum(len(decompress_decode(content)) for content in export["function_entries"].values())
        self.assertGreater(inflated, 11.5 * sum(len(content) for content in export["function_entries"].values()))
        with patch("mcrit.index.MinHashIndex.MIN_IMPORT_DECOMPRESSED_SIZE", 0):
            index = MinHashIndex(config)
            response = self._post_import(index, body)
        self.assertEqual(falcon.HTTP_200, response.status)
        self.assertEqual(2, json.loads(response.data)["data"]["num_samples_imported"])

    def testBodyThatIsNoJsonIsAnsweredAsBadInput(self):
        # b"\xff\xfe{}" is UTF-16 to json.loads, b"\x80{}" no Unicode at all; too deep a nesting is a RecursionError
        for body in ("{not json", b"\xff\xfe{}", b"\x80{}", "[" * 100000):
            with self.subTest(body=body):
                response = self._post_import(MinHashIndex(config), body)
                self.assertEqual(falcon.HTTP_400, response.status)


if __name__ == "__main__":
    unittest.main()
