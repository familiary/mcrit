"""#270: the read-only corpus check finds what older releases let into a corpus."""

import contextlib
import hashlib
import io
import json
import os
from typing import Any, Dict, List, Tuple
from unittest import TestCase
from unittest.mock import MagicMock, patch

import pytest
from bson import Binary
from smda.common.SmdaReport import SmdaReport

from mcrit.config.McritConfig import McritConfig
from mcrit.config.MinHashConfig import MinHashConfig
from mcrit.config.QueueConfig import QueueConfig
from mcrit.config.ShinglerConfig import ShinglerConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.migrations import verify_corpus
from mcrit.storage.SampleEntry import SampleEntry
from mcrit.storage.StorageFactory import StorageFactory
from mcrit.Worker import Worker

from .context import getTestMongoServerAndPort

TESTS = os.path.dirname(os.path.abspath(__file__))
BINARY = b"MZ\x90\x00 bytes kept for the first sample"


@pytest.mark.mongo
class VerifyCorpusTest(TestCase):
    def setUp(self):
        self.server, self.port = getTestMongoServerAndPort()
        self.db_name = f"test_verify_corpus_{os.getpid()}"
        self.config = McritConfig()
        # an instance attribute: the class-level StorageConfig, shared by every McritConfig, stays as it is
        self.config.STORAGE_CONFIG = StorageConfig(
            STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB, STORAGE_SERVER=self.server, STORAGE_PORT=self.port, STORAGE_MONGODB_DBNAME=self.db_name
        )
        self.config.MINHASH_CONFIG = MinHashConfig()
        self.config.MINHASH_CONFIG.MINHASH_POOL_INDEXING = False
        self.config.SHINGLER_CONFIG = ShinglerConfig()
        self.config.QUEUE_CONFIG = QueueConfig()
        self.storage = StorageFactory.getStorage(self.config)
        self.db = self.storage._getDb()
        worker = Worker(queue=MagicMock(), config=self.config, storage=self.storage)
        reported = SmdaReport.fromFile(os.path.join(TESTS, "example_report.smda"))
        assert reported is not None
        reported.sha256 = hashlib.sha256(BINARY).hexdigest()
        uploaded = SmdaReport.fromFile(os.path.join(TESTS, "example_report_2.smda"))
        assert uploaded is not None
        reported_entry = worker._addReport(reported, sha256_verified=False)
        uploaded_entry = worker._addReport(uploaded, sha256_verified=True)
        assert reported_entry is not None and uploaded_entry is not None
        self.reported: SampleEntry = reported_entry
        self.uploaded: SampleEntry = uploaded_entry
        self.storage.storeSampleBinary(self.reported.sample_id, BINARY)

    def tearDown(self):
        self.db.client.drop_database(self.db_name)

    def _check(self) -> Tuple[int, Dict[str, Any]]:
        argv: List[str] = ["--host", self.server, "--port", self.port, "--db", self.db_name, "--hash-binaries"]
        argv += ["--signature-length", str(self.config.MINHASH_CONFIG.MINHASH_SIGNATURE_LENGTH), "--signature-bits", str(self.config.MINHASH_CONFIG.MINHASH_SIGNATURE_BITS)]
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            exit_code = verify_corpus.main(argv)
        return exit_code, json.loads(output.getvalue())

    def testACleanCorpusPasses(self):
        exit_code, result = self._check()
        self.assertEqual([], result["problems"])
        self.assertEqual(0, exit_code)
        # example_report.smda has 10 functions, example_report_2.smda 9
        self.assertEqual((2, 19, 1), (result["num_samples"], result["num_functions"], result["num_binaries"]))
        self.assertEqual({"true": 1, "false": 1, "null": 0}, result["samples_by_sha256_verified"])
        self.db.samples.update_one({"sample_id": self.uploaded.sample_id}, {"$unset": {"sha256_verified": ""}})
        self.assertEqual({"true": 0, "false": 1, "null": 1}, self._check()[1]["samples_by_sha256_verified"])

    def testEachSeededProblemIsReported(self):
        db = self.db
        reported_id, uploaded_id = self.reported.sample_id, self.uploaded.sample_id
        # a report whose statistics claim functions it does not have
        db.samples.update_one({"sample_id": uploaded_id}, {"$set": {"statistics.num_functions": 99}})
        # a minhash of another signature, as BSON binary the way minhashes are stored since #256
        db.functions.update_one({"sample_id": reported_id, "minhash": {"$ne": ""}}, {"$set": {"minhash": Binary(b"\x00\xff")}})
        # a function an import attached to a sample id that is not there
        db.functions.insert_one({"function_id": 10**6, "sample_id": 4242, "family_id": 0, "offset": 0, "minhash": ""})
        # a family whose counter drifted
        db.families.update_one({"family_id": self.uploaded.family_id}, {"$inc": {"num_functions": 5}})
        # a binary that is not the sample's
        self.storage.storeSampleBinary(uploaded_id, b"other bytes")
        # stored bytes that are not what their metadata says
        db["sample_binaries.files"].update_one({"metadata.sample_ids": reported_id}, {"$set": {"metadata.sha256": 64 * "0"}})
        # an upload's replacement that died midway
        db.samples.update_one({"sample_id": reported_id}, {"$set": {"verification": {"token": "dead", "staging_id": 77, "swap": {"started": {}}}}})
        exit_code, result = self._check()
        self.assertEqual(1, exit_code)
        checks = result["checks"]
        self.assertEqual([{"sample_id": uploaded_id, "statistics_num_functions": 99, "stored_functions": 9}], checks["function_count_mismatches"]["examples"])
        self.assertEqual({"count": 1, "examples": [{"sample_id": reported_id, "num_functions": 1}], "num_functions": 1}, checks["samples_with_wrong_length_minhashes"])
        self.assertEqual({"count": 1, "examples": [{"sample_id": 4242, "num_functions": 1}]}, checks["functions_without_sample"])
        self.assertEqual(1, checks["family_counter_mismatches"]["count"])
        self.assertEqual(self.uploaded.family_id, checks["family_counter_mismatches"]["examples"][0]["family_id"])
        # the content of one binary and its metadata disagree, the other binary is not its sample's
        self.assertEqual(2, checks["binary_sha256_mismatches"]["count"])
        self.assertIn(
            {"sample_id": uploaded_id, "sample_sha256": self.uploaded.sha256, "binary_sha256": hashlib.sha256(b"other bytes").hexdigest()},
            checks["binary_sha256_mismatches"]["examples"],
        )
        self.assertEqual({"count": 1, "examples": [reported_id]}, checks["unfinished_replacements"])
        self.assertEqual(1, result["pending_verifications"]["count"])
        for name in (
            "function_count_mismatches",
            "samples_with_wrong_length_minhashes",
            "functions_without_sample",
            "family_counter_mismatches",
            "binary_sha256_mismatches",
            "unfinished_replacements",
        ):
            self.assertIn(name, " ".join(result["problems"]))

    def testTheChecksOfFamiliesBinariesAndStaging(self):
        db = self.db
        reported_id, uploaded_id = self.reported.sample_id, self.uploaded.sample_id
        # a function left in another family than its sample's
        moved = db.functions.find_one({"sample_id": reported_id})
        assert moved is not None
        db.functions.update_one({"function_id": moved["function_id"]}, {"$set": {"family_id": 4343}})
        # a sample, with its functions, in a family without a document
        db.samples.update_one({"sample_id": uploaded_id}, {"$set": {"family_id": 5151}})
        db.functions.update_many({"sample_id": uploaded_id}, {"$set": {"family_id": 5151}})
        # a binary whose sample is gone
        db["sample_binaries.files"].update_one({"metadata.sample_ids": reported_id}, {"$set": {"metadata.sample_ids": [9999]}})
        # staged documents of a replacement no sample records, beside one a sample still does
        db.functions_staging.insert_many([{"staging_id": 31, "function_id": 10**6}, {"staging_id": 32, "function_id": 10**6 + 1}])
        db.xcfg_staging.insert_one({"_id": 10**6, "staging_id": 31, "_xcfg": "{}"})
        db.samples.update_one({"sample_id": reported_id}, {"$set": {"verification": {"token": "running", "staging_id": 32}}})
        # and a deletion that died after claiming its sample
        db.samples.update_one({"sample_id": uploaded_id}, {"$set": {"verification": {"token": "deleteSample:x", "deleting": True}}})
        exit_code, result = self._check()
        self.assertEqual(1, exit_code)
        checks = result["checks"]
        self.assertEqual(
            {"count": 1, "examples": [{"sample_id": reported_id, "sample_family_id": self.reported.family_id, "function_family_id": 4343, "num_functions": 1}]},
            checks["functions_of_another_family"],
        )
        self.assertEqual({"count": 1, "examples": [5151]}, checks["families_without_document"])
        self.assertEqual({"count": 1, "examples": [{"file_id": checks["binaries_without_sample"]["examples"][0]["file_id"], "sample_id": 9999}]}, checks["binaries_without_sample"])
        self.assertEqual(
            {
                "count": 2,
                "examples": [{"collection": "functions_staging", "staging_id": 31, "num_documents": 1}, {"collection": "xcfg_staging", "staging_id": 31, "num_documents": 1}],
            },
            checks["orphaned_staging"],
        )
        self.assertEqual({"count": 1, "examples": [uploaded_id]}, checks["unfinished_deletions"])
        # the running claim is no problem, only reported
        self.assertEqual(1, result["pending_verifications"]["count"])
        for name in ("functions_of_another_family", "families_without_document", "binaries_without_sample", "orphaned_staging", "unfinished_deletions"):
            self.assertIn(name, " ".join(result["problems"]))

    def testAJobStagingDuringTheCheckIsNotReportedOrphaned(self):
        db = self.db
        reported_id = self.reported.sample_id

        class StagingMeanwhile:
            """The samples collection, with a job staging just as the check reads it."""

            def __init__(self, samples):
                self.samples = samples

            def find(self, *args, **kwargs):
                yield from self.samples.find(*args, **kwargs)
                # right after the check has read the samples, a job stages as stageSampleAnalysis
                # does it: the staging id recorded first, then the documents written
                self.samples.update_one({"sample_id": reported_id}, {"$set": {"verification": {"token": "job", "staging_id": 41}}})
                db.functions_staging.insert_one({"staging_id": 41, "function_id": 10**6})

        class Db:
            def __getattr__(self, name):
                return StagingMeanwhile(db.samples) if name == "samples" else getattr(db, name)

            def __getitem__(self, name):
                return db[name]

        bucket = verify_corpus.gridfs.GridFS(db, collection=verify_corpus.BINARIES_BUCKET)
        with patch.object(verify_corpus.gridfs, "GridFS", return_value=bucket):
            result = verify_corpus.verify_corpus(Db(), self.config.MINHASH_CONFIG.MINHASH_SIGNATURE_LENGTH, self.config.MINHASH_CONFIG.MINHASH_SIGNATURE_BITS)
        self.assertEqual(0, result["checks"]["orphaned_staging"]["count"])
        # and once it is in, it is recorded
        self.assertEqual(
            0,
            verify_corpus.verify_corpus(db, self.config.MINHASH_CONFIG.MINHASH_SIGNATURE_LENGTH, self.config.MINHASH_CONFIG.MINHASH_SIGNATURE_BITS)["checks"]["orphaned_staging"][
                "count"
            ],
        )
