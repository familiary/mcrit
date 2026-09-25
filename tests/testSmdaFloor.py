"""What raising the smda floor to 4.8.0 relies on, held as tests so a later smda release that breaks
it fails the suite rather than a corpus.

* An xcfg an older smda stored still loads. SmdaFunction.fromDict checks for
  REQUIRED_FUNCTION_FIELDS and REQUIRED_FUNCTION_METADATA; those sets are the same in every
  release from 4.4.5 through 4.8.0, earlier releases indexed the same keys directly, and
  SmdaFunction.toDict has written all of them since smda 1.2. The reports under tests/ were
  written by smda 1.5.12 and 4.2.16, the oldest stored shape at hand, so the canary below fails
  the day an smda release requires a field they lack.
* One staleness threshold still covers every architecture. MongoDbStorage.recalculateAllPicHashes
  and Worker.repairMinHashes treat a sample as stale by smda's ESCAPER_DOWNWARD_COMPATIBILITY
  alone, while smda gates its own pic_hash recalculation per architecture.
* The smda_version IdaReportProducer writes, "MCRIT4IDA cli via SMDA <version>", parses as a
  version recalculateAllPicHashes takes as current.
"""

import glob
import json
import os
import re
import unittest
from unittest.mock import patch

import pytest
import smda.common.SmdaFunction as smda_function_module
from packaging import version
from smda.common.BinaryInfo import BinaryInfo
from smda.common.SmdaFunction import REQUIRED_FUNCTION_FIELDS, REQUIRED_FUNCTION_METADATA, SmdaFunction
from smda.common.SmdaReport import SmdaReport
from smda.SmdaConfig import SmdaConfig

from mcrit.config.McritConfig import McritConfig
from mcrit.config.MinHashConfig import MinHashConfig
from mcrit.config.ShinglerConfig import ShinglerConfig
from mcrit.storage.StorageFactory import StorageFactory

from .testStorage import buildMongoStorageConfig

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
EXAMPLE_REPORT = os.path.join(TESTS_DIR, "example_report.smda")


def _storedReports():
    for path in sorted(glob.glob(os.path.join(TESTS_DIR, "*.smda")) + glob.glob(os.path.join(TESTS_DIR, "fixtures", "*.smda"))):
        with open(path) as handle:
            yield path, json.load(handle)


class OlderSmdaXcfgTest(unittest.TestCase):
    def test_the_stored_shape_of_older_smda_loads_under_the_installed_smda(self):
        versions = set()
        for path, report in _storedReports():
            versions.add(report["smda_version"])
            binary_info = BinaryInfo(b"")
            binary_info.architecture = report["architecture"]
            for offset, function_dict in report["xcfg"].items():
                with self.subTest(report=os.path.basename(path), function=offset):
                    self.assertEqual(set(), REQUIRED_FUNCTION_FIELDS.difference(function_dict))
                    self.assertEqual(set(), REQUIRED_FUNCTION_METADATA.difference(function_dict["metadata"]))
                    self.assertIsNotNone(SmdaFunction.fromDict(function_dict, binary_info=binary_info))
        # the evidence is only as old as the oldest reports it covers
        self.assertLessEqual({"1.5.12", "4.2.16"}, versions)


class SmdaVersionThresholdTest(unittest.TestCase):
    """MongoDbStorage.recalculateAllPicHashes and Worker.repairMinHashes treat a sample as stale by
    one number, smda's ESCAPER_DOWNWARD_COMPATIBILITY (4.4.5 in 4.5.0 through 4.8.0). smda itself
    gates its pic_hash recalculation per architecture; one threshold only works while it is at
    least as new as every one of those gates."""

    def test_the_escaper_threshold_covers_every_architecture_escape_change(self):
        threshold = version.parse(SmdaConfig().ESCAPER_DOWNWARD_COMPATIBILITY)
        gates = {name: value for name, value in vars(smda_function_module).items() if name.endswith("_PIC_HASH_ESCAPE_VERSION")}
        self.assertGreaterEqual(len(gates), 4, sorted(gates))
        for name, gate in gates.items():
            with self.subTest(gate=name):
                self.assertLessEqual(version.parse(".".join(str(part) for part in gate)), threshold)


@pytest.mark.mongo
class IdaReportVersionTest(unittest.TestCase):
    def setUp(self):
        mongo_config = McritConfig()
        mongo_config.STORAGE_CONFIG = buildMongoStorageConfig("test_smda_floor_mcrit")
        mongo_config.MINHASH_CONFIG = MinHashConfig()
        mongo_config.SHINGLER_CONFIG = ShinglerConfig()
        self.storage = StorageFactory.getStorage(mongo_config)
        self.storage.clearStorage()
        self.sample_entry = self.storage.addSmdaReport(SmdaReport.fromFile(EXAMPLE_REPORT))

    def tearDown(self):
        self.storage.clearStorage()

    def _recalculateAndReport(self):
        """The "Found N outdated samples" summary recalculateAllPicHashes logs, whichever info call it is."""
        with patch("mcrit.storage.MongoDbStorage.LOGGER") as logger:
            self.storage.recalculateAllPicHashes()
        messages = [str(call.args[0]) for call in logger.info.call_args_list if call.args]
        summaries = [message for message in messages if re.match(r"Found \d+ outdated samples", message)]
        self.assertEqual(1, len(summaries), messages)
        return summaries[0]

    def test_pichash_recalculation_takes_an_ida_report_of_this_smda_as_current(self):
        # the smda_version IdaReportProducer writes; recalculateAllPicHashes reads its last token
        self.storage._getDb().samples.update_one({"sample_id": self.sample_entry.sample_id}, {"$set": {"smda_version": f"MCRIT4IDA cli via SMDA {SmdaConfig().VERSION}"}})
        self.assertTrue(self._recalculateAndReport().startswith("Found 0 outdated samples"))

    def test_pichash_recalculation_takes_the_example_report_as_outdated(self):
        # the control for the test above: written by smda 1.5.12, the same sample is outdated
        self.assertTrue(self._recalculateAndReport().startswith("Found 1 outdated samples"))


if __name__ == "__main__":
    unittest.main()
