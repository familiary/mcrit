import io
import json
import unittest
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, patch

import falcon
import falcon.testing

from mcrit.client.McritClient import McritClient, McritNotFound, McritServerError
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.server.SampleResource import SampleResource
from mcrit.Worker import Worker

from .context import config


class BinaryRouteTest(unittest.TestCase):
    """#95: /samples/{id}/binary answers the raw submission when the instance kept it"""

    @staticmethod
    def _request():
        return falcon.Request(falcon.testing.create_environ(path="/samples/1/binary"))

    @staticmethod
    def _servingIndex():
        index = MagicMock()
        index.isServingSampleBinaries.return_value = True
        return index

    def test_nothing_is_served_while_serving_is_off(self):
        """Keeping binaries and serving them are separate switches: with serving off the route
        refuses every sample alike, without looking up whether it exists or has a binary."""
        index = MagicMock()
        index.isServingSampleBinaries.return_value = False
        index.isSampleId.return_value = True
        index.openSampleBinary.return_value = io.BytesIO(b"MZ")
        resp = falcon.Response()
        SampleResource(index).on_get_binary(self._request(), resp, 1)
        self.assertEqual(falcon.HTTP_403, resp.status)
        self.assertIsNone(resp.stream)
        assert resp.data is not None
        self.assertEqual("failed", json.loads(resp.data)["status"])
        index.isSampleId.assert_not_called()
        index.openSampleBinary.assert_not_called()
        index.getSampleBinary.assert_not_called()

    def test_serving_is_off_by_default_and_follows_its_own_switch(self):
        self.assertFalse(StorageConfig().STORAGE_SERVE_SUBMITTED_BINARIES)
        self.assertFalse(MinHashIndex.isServingSampleBinaries(cast(Any, SimpleNamespace(_storage_config=StorageConfig(STORAGE_KEEP_SUBMITTED_BINARIES=True)))))
        serving = StorageConfig(STORAGE_SERVE_SUBMITTED_BINARIES=True)
        self.assertTrue(MinHashIndex.isServingSampleBinaries(cast(Any, SimpleNamespace(_storage_config=serving))))

    def test_the_stored_binary_is_served_as_octets(self):
        index = self._servingIndex()
        index.isSampleId.return_value = True
        index.openSampleBinary.return_value = io.BytesIO(b"MZ\x90\x00binary")
        resp = falcon.Response()
        SampleResource(index).on_get_binary(self._request(), resp, 1)
        self.assertEqual("application/octet-stream", resp.content_type)
        stream = resp.stream
        assert isinstance(stream, io.BytesIO)
        self.assertEqual(b"MZ\x90\x00binary", stream.read())

    def test_the_binary_is_streamed_rather_than_read_into_memory(self):
        """The point of serving through openSampleBinary: the response carries the handle, so
        the file is read out chunk by chunk by the server instead of being buffered whole."""
        index = self._servingIndex()
        index.isSampleId.return_value = True
        handle = io.BytesIO(b"MZ" * 100)
        index.openSampleBinary.return_value = handle
        resp = falcon.Response()
        SampleResource(index).on_get_binary(self._request(), resp, 1)
        index.getSampleBinary.assert_not_called()
        self.assertIs(handle, resp.stream)
        self.assertIsNone(resp.data)
        self.assertEqual(0, handle.tell(), "the responder must not have read the stream itself")

    def test_a_gridfs_handle_sets_the_content_length(self):
        """GridOut carries `length`; BytesIO does not, and an absent one must not be invented."""
        index = self._servingIndex()
        index.isSampleId.return_value = True
        index.openSampleBinary.return_value = SimpleNamespace(read=lambda size=-1: b"MZ", close=lambda: None, length=1234)
        resp = falcon.Response()
        SampleResource(index).on_get_binary(self._request(), resp, 1)
        content_length = resp.content_length  # falcon keeps it as the header string
        assert content_length is not None
        self.assertEqual(1234, int(content_length))
        index.openSampleBinary.return_value = io.BytesIO(b"MZ")
        resp = falcon.Response()
        SampleResource(index).on_get_binary(self._request(), resp, 1)
        self.assertIsNone(resp.content_length)

    def test_an_unknown_sample_and_a_sample_without_binary_are_404(self):
        index = self._servingIndex()
        index.isSampleId.return_value = False
        resp = falcon.Response()
        SampleResource(index).on_get_binary(self._request(), resp, 1)
        self.assertEqual(falcon.HTTP_404, resp.status)
        index.isSampleId.return_value = True
        index.openSampleBinary.return_value = None
        resp = falcon.Response()
        SampleResource(index).on_get_binary(self._request(), resp, 1)
        self.assertEqual(falcon.HTTP_404, resp.status)
        assert resp.data is not None
        self.assertEqual("failed", json.loads(resp.data)["status"])


class BinaryClientTest(unittest.TestCase):
    def test_the_client_hands_out_the_bytes_or_none(self):
        client = McritClient("http://mcrit.test")
        with patch("mcrit.client.McritClient.requests.get", return_value=MagicMock(status_code=200, content=b"bytes")) as get:
            self.assertEqual(b"bytes", client.getSampleBinary(3))
            self.assertIn("/samples/3/binary", get.call_args.args[0])
        with patch("mcrit.client.McritClient.requests.get", return_value=MagicMock(status_code=404)):
            self.assertIsNone(client.getSampleBinary(3))

    def test_the_client_error_modes_apply(self):
        failure = MagicMock(status_code=404, url="http://mcrit.test/samples/3/binary")
        failure.json.return_value = {"status": "failed", "data": {"message": "No binary kept for this sample."}}
        with patch("mcrit.client.McritClient.requests.get", return_value=failure):
            with self.assertRaises(McritNotFound):
                McritClient("http://mcrit.test", raise_client_errors=True).getSampleBinary(3)
        failure.status_code = 500
        with patch("mcrit.client.McritClient.requests.get", return_value=failure):
            with self.assertRaises(McritServerError):
                McritClient("http://mcrit.test", raise_server_errors=True).getSampleBinary(3)


class WorkerKeepsBinariesTest(unittest.TestCase):
    def _worker(self, keep):
        storage = MagicMock()
        storage.getSampleBySha256.return_value = None
        storage.storeSampleBinary.return_value = True
        worker = Worker(queue=MagicMock(), config=config, storage=storage)
        worker._storage_config = SimpleNamespace(STORAGE_KEEP_SUBMITTED_BINARIES=keep, STORAGE_MONGODB_CLEANUP_TTL=1)
        sample_entry = MagicMock()
        sample_entry.sample_id = 42
        sample_entry.toDict.return_value = {"sample_id": 42}
        setattr(worker, "_addReport", lambda report: sample_entry)
        return worker, storage

    def test_the_binary_is_kept_only_when_configured(self):
        with patch("mcrit.Worker.Disassembler") as disassembler:
            disassembler.return_value.disassembleUnmappedBuffer.return_value = MagicMock()
            worker, storage = self._worker(keep=True)
            result = worker.addBinarySample(b"MZ", "a.exe", "fam", "1", False, None, None)
            storage.storeSampleBinary.assert_called_once_with(42, b"MZ")
            self.assertTrue(result["binary_stored"])
            worker, storage = self._worker(keep=False)
            result = worker.addBinarySample(b"MZ", "a.exe", "fam", "1", False, None, None)
            storage.storeSampleBinary.assert_not_called()
            self.assertNotIn("binary_stored", result)

    def test_a_known_sample_without_a_stored_binary_gets_it_on_resubmission(self):
        """a retried job (sample committed, binary not) or a sample from before binaries were
        kept: the existing-sample path completes the storage instead of skipping it"""
        with patch("mcrit.Worker.Disassembler"):
            worker, storage = self._worker(keep=True)
            known = MagicMock()
            known.sample_id = 7
            known.toDict.return_value = {"sample_id": 7}
            storage.getSampleBySha256.return_value = known
            storage.hasSampleBinary.return_value = False
            result = worker.addBinarySample(b"MZ", "a.exe", "fam", "1", False, None, None)
            storage.storeSampleBinary.assert_called_once_with(7, b"MZ")
            self.assertTrue(result["binary_stored"])
            # the check asks whether a binary is there; it must not fetch one to find out
            storage.getSampleBinary.assert_not_called()
            # already stored: nothing to do
            storage.storeSampleBinary.reset_mock()
            storage.hasSampleBinary.return_value = True
            result = worker.addBinarySample(b"MZ", "a.exe", "fam", "1", False, None, None)
            storage.storeSampleBinary.assert_not_called()
            self.assertNotIn("binary_stored", result)


if __name__ == "__main__":
    unittest.main()
