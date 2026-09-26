import json
import unittest
from unittest.mock import MagicMock, patch

import falcon
import falcon.testing

from mcrit.client.McritClient import McritClient
from mcrit.server.FamilyResource import FamilyResource
from mcrit.storage.FamilyEntry import FamilyEntry


class FamilyEntryActors(unittest.TestCase):
    """#57: a family carries the actors it is attributed to"""

    def test_round_trip_and_defaults(self):
        entry = FamilyEntry(family_name="win.citadel", family_id=5, actors=["Actor A", "Actor B"])
        self.assertEqual(["Actor A", "Actor B"], entry.toDict()["actors"])
        self.assertEqual(["Actor A", "Actor B"], FamilyEntry.fromDict(entry.toDict()).actors)
        legacy = entry.toDict()
        del legacy["actors"]
        self.assertEqual([], FamilyEntry.fromDict(legacy).actors)
        self.assertEqual([], FamilyEntry(family_name="x").actors)

    def test_normalisation(self):
        self.assertEqual(["A", "B"], FamilyEntry.normalizeActors([" A ", "B", "A", "", "  "]))
        self.assertEqual([], FamilyEntry.normalizeActors(None))


class FamilyActorsRoute(unittest.TestCase):
    def _put(self, body, headers=None):
        payload = json.dumps(body)
        environ = falcon.testing.create_environ(path="/families/5", method="PUT", body=payload, headers={"Content-Type": "application/json", **(headers or {})})
        return falcon.Request(environ)

    def test_actors_are_validated_and_passed_on(self):
        index = MagicMock()
        index.modifyFamily.return_value = True
        resp = falcon.Response()
        # the requester travels with the job, as for every other modification (#37)
        FamilyResource(index).on_put(self._put({"actors": ["Actor A", "APT-1"]}, {"username": "alice"}), resp, 5)
        self.assertEqual(falcon.HTTP_202, resp.status)
        index.modifyFamily.assert_called_once_with(5, {"actors": ["Actor A", "APT-1"]}, force_recalculation=True, username="alice")
        # a comma separated string is accepted too, malformed names are not
        index.modifyFamily.reset_mock()
        FamilyResource(index).on_put(self._put({"actors": "one, two"}), falcon.Response(), 5)
        index.modifyFamily.assert_called_once_with(5, {"actors": ["one", " two"]}, force_recalculation=True, username=None)
        index.modifyFamily.reset_mock()
        resp = falcon.Response()
        FamilyResource(index).on_put(self._put({"actors": ["<script>"]}), resp, 5)
        self.assertEqual(falcon.HTTP_400, resp.status)
        index.modifyFamily.assert_not_called()
        resp = falcon.Response()
        FamilyResource(index).on_put(self._put({"actors": [1, 2]}), resp, 5)
        self.assertEqual(falcon.HTTP_400, resp.status)


class FamilyActorsClient(unittest.TestCase):
    def test_the_client_sends_actors_as_json(self):
        client = McritClient("http://mcrit.test")
        response = MagicMock(status_code=200)
        response.json.return_value = {"status": "successful", "data": {"message": "Family modified."}}
        with patch("mcrit.client.McritClient.requests.put", return_value=response) as put:
            client.modifyFamily(5, actors=["A", "B"])
        self.assertEqual({"actors": ["A", "B"]}, put.call_args.kwargs["json"])
        with patch("mcrit.client.McritClient.requests.put", return_value=response) as put:
            client.modifyFamily(5, family_name="new_name", is_library=True)
        self.assertEqual({"family_name": "new_name", "is_library": True}, put.call_args.kwargs["json"])


if __name__ == "__main__":
    unittest.main()
