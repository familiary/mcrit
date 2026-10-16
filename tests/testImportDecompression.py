import base64
import io
import json
import logging
import os
import unittest
import zipfile

import falcon
import falcon.testing
from smda.common.SmdaReport import SmdaReport

from mcrit.index.MinHashIndex import MinHashIndex
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
        with self.assertRaises((ValueError, zipfile.BadZipFile)):
            decompress_decode(base64.b85encode(bytes(data)).decode())


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

    def testBodyThatIsNoJsonIsAnsweredAsBadInput(self):
        response = self._post_import(MinHashIndex(config), "{not json")
        self.assertEqual(falcon.HTTP_400, response.status)


if __name__ == "__main__":
    unittest.main()
