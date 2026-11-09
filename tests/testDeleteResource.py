import json
import unittest
from unittest.mock import MagicMock

import falcon
import falcon.testing

from mcrit.server.FamilyResource import FamilyResource
from mcrit.server.SampleResource import SampleResource


def _delete(path):
    return falcon.Request(falcon.testing.create_environ(path=path, method="DELETE"))


class SampleDeleteTest(unittest.TestCase):
    """DELETE /samples/{id} queued a job for any id and answered 202, so an unknown sample's
    deletion only failed later, inside the worker"""

    def _delete_sample(self, known, job_id):
        index = MagicMock()
        index.isSampleId.return_value = known
        index.deleteSample.return_value = job_id
        resp = falcon.Response()
        SampleResource(index).on_delete(_delete("/samples/7"), resp, sample_id=7)
        return index, resp

    def test_a_known_sample_is_queued_for_deletion(self):
        index, resp = self._delete_sample(True, "65f0c0ffee")
        self.assertEqual(falcon.HTTP_202, resp.status)
        index.deleteSample.assert_called_once_with(7, force_recalculation=True, username=None)
        assert resp.data is not None
        self.assertEqual("65f0c0ffee", json.loads(resp.data)["data"])

    def test_an_unknown_sample_is_404_and_queues_nothing(self):
        index, resp = self._delete_sample(False, "65f0c0ffee")
        self.assertEqual(falcon.HTTP_404, resp.status)
        index.deleteSample.assert_not_called()
        assert resp.data is not None
        self.assertIn("don't have a sample", json.loads(resp.data)["data"]["message"])

    def test_a_deletion_that_could_not_be_queued_is_503(self):
        # an index that hands back no job id; the queue raises instead
        _, resp = self._delete_sample(True, None)
        self.assertEqual(falcon.HTTP_503, resp.status)


class FamilyDeleteTest(unittest.TestCase):
    def test_a_deletion_that_could_not_be_queued_is_503(self):
        index = MagicMock()
        index.getFamily.return_value = object()
        index.deleteFamily.return_value = None
        resp = falcon.Response()
        FamilyResource(index).on_delete(_delete("/families/3"), resp, family_id=3)
        self.assertEqual(falcon.HTTP_503, resp.status)

    def test_an_unknown_family_is_still_404(self):
        index = MagicMock()
        index.getFamily.return_value = None
        resp = falcon.Response()
        FamilyResource(index).on_delete(_delete("/families/3"), resp, family_id=3)
        self.assertEqual(falcon.HTTP_404, resp.status)
        index.deleteFamily.assert_not_called()


if __name__ == "__main__":
    unittest.main()
