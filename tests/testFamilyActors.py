import json
import os
import time
import unittest
from unittest.mock import MagicMock, patch

import falcon
import falcon.testing
import pymongo
import pytest
from smda.common.SmdaReport import SmdaReport

from mcrit.client.McritClient import McritClient
from mcrit.config.McritConfig import McritConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.server.FamilyResource import FamilyResource
from mcrit.storage.FamilyEntry import FamilyEntry
from mcrit.storage.StorageFactory import StorageFactory

from .context import config, getTestMongoServerAndPort

EXAMPLE_REPORT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "example_report.smda")


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


class FamilyActorsBounds(unittest.TestCase):
    """Actors had no count limit and were deduplicated with a list scan per name: 40,000 names held a
    worker for 10 s and a POST /import request thread for 19 s, growing quadratically"""

    def _put(self, body):
        environ = falcon.testing.create_environ(path="/families/5", method="PUT", body=json.dumps(body), headers={"Content-Type": "application/json"})
        return falcon.Request(environ)

    def test_normalisation_takes_linear_time(self):
        names = ["actor %d" % index for index in range(40000)]
        started = time.monotonic()
        self.assertEqual(names, FamilyEntry.normalizeActors(names + names))
        self.assertLess(time.monotonic() - started, 2)

    def test_the_route_refuses_more_than_the_cap(self):
        index = MagicMock()
        index.modifyFamily.return_value = True
        resp = falcon.Response()
        FamilyResource(index).on_put(self._put({"actors": ["Actor %d" % i for i in range(FamilyEntry.MAX_ACTORS + 1)]}), resp, 5)
        self.assertEqual(falcon.HTTP_400, resp.status)
        index.modifyFamily.assert_not_called()
        resp = falcon.Response()
        FamilyResource(index).on_put(self._put({"actors": ",".join("Actor %d" % i for i in range(FamilyEntry.MAX_ACTORS + 1))}), resp, 5)
        self.assertEqual(falcon.HTTP_400, resp.status)
        index.modifyFamily.assert_not_called()
        resp = falcon.Response()
        FamilyResource(index).on_put(self._put({"actors": ["Actor %d" % i for i in range(FamilyEntry.MAX_ACTORS)]}), resp, 5)
        self.assertEqual(falcon.HTTP_202, resp.status)

    def test_the_cap_counts_names_once(self):
        index = MagicMock()
        index.modifyFamily.return_value = True
        resp = falcon.Response()
        actors = ["Actor %d" % i for i in range(FamilyEntry.MAX_ACTORS)] + ["Actor 0"]
        FamilyResource(index).on_put(self._put({"actors": actors}), resp, 5)
        self.assertEqual(falcon.HTTP_202, resp.status)

    def test_an_import_adds_actors_up_to_the_cap(self):
        with open(EXAMPLE_REPORT) as fjson:
            report = SmdaReport.fromDict(json.load(fjson))
        assert report is not None
        report.family = "actors_family"
        source = MinHashIndex(config)
        sample_entry = source.getStorage().addSmdaReport(report)
        assert sample_entry is not None
        export_data = json.loads(json.dumps(source.getExportData()))
        export_data["family_actors"] = {str(sample_entry.family_id): ["Imported %d" % i for i in range(10000)]}
        target = MinHashIndex(config)
        target_family_id = target.getStorage().addFamily("actors_family")
        target.getStorage().modifyFamily(target_family_id, {"actors": ["Local A", "Imported 0"]})
        target.addImportData(export_data)
        actors = target.getStorage().getFamily(target_family_id).actors
        self.assertEqual(FamilyEntry.MAX_ACTORS, len(actors))
        self.assertEqual(["Local A", "Imported 0", "Imported 1", "Imported 2"], actors[:4])


class FamilyActorsRenameMerge(unittest.TestCase):
    """A rename that merges a family into another unioned both actor lists uncapped: one import of 640
    families with 64 actors each and 640 renames attributed 40,960 actors to one family"""

    mongo_db_name = None

    def setUp(self):
        mcrit_config = McritConfig()
        if self.mongo_db_name is None:
            mcrit_config.STORAGE_CONFIG = StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MEMORY)
        else:
            server, port = getTestMongoServerAndPort()
            mcrit_config.STORAGE_CONFIG = StorageConfig(
                STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB, STORAGE_SERVER=server, STORAGE_PORT=port, STORAGE_MONGODB_DBNAME=self.mongo_db_name
            )
        mcrit_config.QUEUE_CONFIG = config.QUEUE_CONFIG
        self.storage = MinHashIndex(mcrit_config).getStorage()
        self.storage.clearStorage()

    def _family(self, name, actors):
        family_id = self.storage.addFamily(name)
        self.storage.modifyFamily(family_id, {"actors": actors})
        return family_id

    def test_a_rename_merge_adds_actors_up_to_the_cap(self):
        own = ["Target %d" % i for i in range(40)]
        target_id = self._family("target", own)
        for feeder in range(3):
            feeder_id = self._family("feeder %d" % feeder, ["Feeder %d %d" % (feeder, i) for i in range(FamilyEntry.MAX_ACTORS)] + ["Target 0"])
            self.storage.modifyFamily(feeder_id, {"family_name": "target"})
        actors = self.storage.getFamily(target_id).actors
        self.assertEqual(own + ["Feeder 0 %d" % i for i in range(FamilyEntry.MAX_ACTORS - 40)], actors)

    def test_a_family_stored_with_more_keeps_its_own(self):
        # stored before the cap: a merge drops none of the target's, and adds none
        own = ["Legacy %d" % i for i in range(FamilyEntry.MAX_ACTORS + 36)]
        target_id = self._family("legacy", own)
        feeder_id = self._family("feeder", ["Feeder %d" % i for i in range(10)])
        self.storage.modifyFamily(feeder_id, {"family_name": "legacy"})
        self.assertEqual(own, self.storage.getFamily(target_id).actors)


@pytest.mark.mongo
class MongoFamilyActorsRenameMerge(FamilyActorsRenameMerge):
    mongo_db_name = "test_family_actors_merge_mcrit"

    @classmethod
    def tearDownClass(cls):
        server, port = getTestMongoServerAndPort()
        pymongo.MongoClient(server, int(port)).drop_database(cls.mongo_db_name)


class FamilyActorsClient(unittest.TestCase):
    def test_the_client_sends_actors_as_json(self):
        client = McritClient("http://mcrit.test")
        response = MagicMock(status_code=200)
        response.json.return_value = {"status": "successful", "data": {"message": "Family modified."}}
        with patch("mcrit.client.McritClient.requests.Session.put", return_value=response) as put:
            client.modifyFamily(5, actors=["A", "B"])
        self.assertEqual({"actors": ["A", "B"]}, put.call_args.kwargs["json"])
        with patch("mcrit.client.McritClient.requests.Session.put", return_value=response) as put:
            client.modifyFamily(5, family_name="new_name", is_library=True)
        self.assertEqual({"family_name": "new_name", "is_library": True}, put.call_args.kwargs["json"])


if __name__ == "__main__":
    unittest.main()
