import copy
import dataclasses
import json
import logging
import os
import unittest

import pymongo
import pytest
from smda.common.SmdaReport import SmdaReport

from mcrit.config.McritConfig import McritConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.storage.StorageFactory import StorageFactory

from .context import config, getTestMongoServerAndPort

logging.disable(logging.CRITICAL)

THIS_FILE_PATH = str(os.path.abspath(__file__))
PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", ".."])))
EXAMPLE_REPORT = os.sep.join([PROJECT_ROOT, "tests", "example_report.smda"])


def mongo_config(db_name):
    server, port = getTestMongoServerAndPort()
    mcrit_config = McritConfig()
    mcrit_config.STORAGE_CONFIG = StorageConfig(
        STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB,
        STORAGE_SERVER=server,
        STORAGE_PORT=port,
        STORAGE_MONGODB_DBNAME=db_name,
        STORAGE_DROP_DISASSEMBLY=False,
    )
    # the same hashing configuration as the in-memory test config: an import is refused
    # between instances whose minhash or shingler configuration hashes differ
    mcrit_config.MINHASH_CONFIG = config.MINHASH_CONFIG
    mcrit_config.SHINGLER_CONFIG = config.SHINGLER_CONFIG
    # the in-process queue, so the hashing job runs when its result is fetched
    mcrit_config.QUEUE_CONFIG = config.QUEUE_CONFIG
    return mcrit_config


def by_offset(function_entries):
    return {entry.offset: entry for entry in function_entries}


class ExportImportRoundTripMixin(unittest.TestCase):
    """What an export -> import changes, and what it must not (fkie-cad/mcritweb#67 asked):
    besides the remapped ids, nothing. In particular the disassembly (xcfg), the pichashes,
    the picblockhashes and the minhashes arrive as they left."""

    def _index_with_report(self, mcrit_config):
        index = MinHashIndex(mcrit_config)
        with open(EXAMPLE_REPORT) as fjson:
            report = SmdaReport.fromDict(json.load(fjson))
        assert report is not None
        sample_entry = index.getStorage().addSmdaReport(report)
        assert sample_entry is not None
        # a job on the local queue: fetching the result runs it
        index.getResultForJob(index.updateMinHashesForSample(sample_entry.sample_id, force_recalculation=True))
        return index, sample_entry

    def _roundTrip(self, source_config, target_config, compress_data):
        source, sample_entry = self._index_with_report(source_config)
        export_data = source.getExportData(compress_data=compress_data)
        # the export is JSON on the wire; a dict that is not JSON-clean would break the client
        export_data = json.loads(json.dumps(export_data))
        target = MinHashIndex(target_config)
        report = target.addImportData(export_data)
        self.assertEqual(1, report["num_samples_imported"])
        self.assertEqual(sample_entry.statistics["num_functions"], report["num_functions_imported"])
        self.assertEqual(0, report["num_functions_skipped"])
        imported = target.getStorage().getSampleBySha256(sample_entry.sha256)
        assert imported is not None
        self.assertEqual(sample_entry.family, imported.family)
        self.assertEqual(sample_entry.filename, imported.filename)
        self.assertEqual(sample_entry.statistics, imported.statistics)
        source_functions = by_offset(source.getStorage().getFunctionsBySampleId(sample_entry.sample_id))
        target_functions = by_offset(target.getStorage().getFunctionsBySampleId(imported.sample_id))
        self.assertEqual(set(source_functions), set(target_functions))
        num_with_xcfg = num_with_minhash = num_with_blocks = 0
        for offset, before in source_functions.items():
            after = target_functions[offset]
            self.assertEqual(before.pichash, after.pichash, offset)
            self.assertEqual(before.picblockhashes, after.picblockhashes, offset)
            # the export is JSON: block offsets keyed by int in memory arrive as strings
            self.assertEqual(json.loads(json.dumps(before.xcfg)), json.loads(json.dumps(after.xcfg)), offset)
            self.assertEqual(before.minhash, after.minhash, offset)
            self.assertEqual(before.function_name, after.function_name, offset)
            self.assertEqual(before.num_instructions, after.num_instructions, offset)
            num_with_xcfg += bool(after.xcfg)
            num_with_minhash += bool(after.minhash)
            num_with_blocks += bool(after.picblockhashes)
        # the assertions above are only worth something if the data was there to begin with
        self.assertEqual(len(source_functions), num_with_xcfg)
        self.assertGreater(num_with_minhash, 0)
        self.assertGreater(num_with_blocks, 0)
        # the target's own indexes know the imported functions: the pichash lookup finds every
        # imported function under its new ids, and the band index yields a minhashed function as
        # a candidate for its own minhash
        target_storage = target.getStorage()
        minhash_bits = target_config.MINHASH_CONFIG.MINHASH_SIGNATURE_BITS
        num_candidates_checked = 0
        for offset, after in target_functions.items():
            self.assertIn((imported.family_id, imported.sample_id, after.function_id), target_storage.getMatchesForPicHash(after.pichash), offset)
            if after.minhash:
                self.assertIn(after.function_id, target_storage.getCandidatesForMinHash(after.getMinHash(minhash_bits=minhash_bits)), offset)
                num_candidates_checked += 1
        self.assertEqual(num_with_minhash, num_candidates_checked)
        target_status = target.getStatus(with_pichash=True)["status"]
        source_status = source.getStatus(with_pichash=True)["status"]
        self.assertEqual(source_status["num_functions"], target_status["num_functions"])
        self.assertEqual(source_status["num_pichashes"], target_status["num_pichashes"])
        # a second import of the same export is a no-op
        report = target.addImportData(export_data)
        self.assertEqual(1, report["num_samples_skipped"])
        self.assertEqual(sample_entry.statistics["num_functions"], target.getStatus(with_pichash=False)["status"]["num_functions"])
        return source, target

    def _storedComposition(self, storage, function_id):
        """the shingle composition as the storage holds it for the function, not as an entry reads it"""
        raise NotImplementedError

    def _importKeepsCompositions(self, target_config):
        """The import stores each function's shingle composition with its entry and then bands the
        minhashes; banding them wrote an empty composition over the one just stored."""
        # compositions are only recorded where shingles are tracked, which is not the default
        source_config = copy.deepcopy(config)
        source_config.MINHASH_CONFIG = dataclasses.replace(config.MINHASH_CONFIG, MINHASH_TRACK_SHINGLES=True)
        source, sample_entry = self._index_with_report(source_config)
        source_functions = by_offset(source.getStorage().getFunctionsBySampleId(sample_entry.sample_id))
        compositions = {offset: entry.minhash_shingle_composition for offset, entry in source_functions.items() if entry.minhash}
        self.assertGreater(len(compositions), 0)
        self.assertTrue(all(compositions.values()))
        target = MinHashIndex(target_config)
        target.addImportData(json.loads(json.dumps(source.getExportData())))
        target_storage = target.getStorage()
        imported = target_storage.getSampleBySha256(sample_entry.sha256)
        assert imported is not None
        target_functions = by_offset(target_storage.getFunctionsBySampleId(imported.sample_id))
        for offset, composition in compositions.items():
            function_id = target_functions[offset].function_id
            self.assertEqual(composition, self._storedComposition(target_storage, function_id), offset)
            function_entry = target_storage.getFunctionById(function_id)
            assert function_entry is not None
            self.assertEqual(composition, function_entry.minhash_shingle_composition, offset)
            self.assertEqual(composition, target_functions[offset].minhash_shingle_composition, offset)

    def _importWrongLengthMinHashes(self, mcrit_config):
        """An export is data from elsewhere: a minhash of another length than this instance's
        signatures is dropped at import. Kept, it was banded beside the code it was copied from
        and failed every matching job that drew it as a candidate, or did not fit the band
        projection at all. The function itself is imported, and a repair hashes it again."""
        index, sample_entry = self._index_with_report(mcrit_config)
        export_data = json.loads(json.dumps(index.getExportData(compress_data=False)))
        # a renamed copy of the indexed sample, so that it lands next to the code it copies
        copy_sha256 = "ab" * 32
        sample_dict = export_data["sample_entries"].pop(sample_entry.sha256)
        sample_dict["sha256"] = copy_sha256
        export_data["sample_entries"][copy_sha256] = sample_dict
        functions = export_data["function_entries"].pop(sample_entry.sha256)
        export_data["function_entries"][copy_sha256] = functions
        hashed = sorted((function_id for function_id, function in functions.items() if function["minhash"]), key=int)
        self.assertGreater(len(hashed), 2)
        # the first keeps its signature, the second is cut short, the others carry a byte too many
        functions[hashed[1]]["minhash"] = functions[hashed[1]]["minhash"][:16]
        for function_id in hashed[2:]:
            functions[function_id]["minhash"] += "00"
        report = index.addImportData(export_data)
        self.assertEqual(1, report["num_samples_imported"])
        self.assertEqual(len(functions), report["num_functions_imported"])
        storage = index.getStorage()
        imported = storage.getSampleBySha256(copy_sha256)
        assert imported is not None
        minhash_config = mcrit_config.MINHASH_CONFIG
        signature_bytes = minhash_config.MINHASH_SIGNATURE_LENGTH * (1 if minhash_config.MINHASH_SIGNATURE_BITS <= 8 else 4)
        imported_functions = storage.getFunctionsBySampleId(imported.sample_id)
        self.assertEqual([signature_bytes], [len(entry.minhash) for entry in imported_functions if entry.minhash])
        kept = next(entry for entry in imported_functions if entry.minhash)
        self.assertIn(kept.function_id, storage.getCandidatesForMinHash(kept.getMinHash(minhash_bits=minhash_config.MINHASH_SIGNATURE_BITS)))
        # imported samples record no minhash version, so a repair hashes the dropped ones again
        repair = index.getResultForJob(index.repairMinHashes(force_recalculation=True))
        self.assertEqual(len(hashed), repair["num_functions_rehashed"])
        source_minhashes = sorted(entry.minhash for entry in storage.getFunctionsBySampleId(sample_entry.sample_id) if entry.minhash)
        self.assertEqual(source_minhashes, sorted(entry.minhash for entry in storage.getFunctionsBySampleId(imported.sample_id) if entry.minhash))


class MemoryExportImportRoundTrip(ExportImportRoundTripMixin):
    def test_plain_export(self):
        self._roundTrip(config, config, compress_data=False)

    def test_compressed_export(self):
        self._roundTrip(config, config, compress_data=True)

    def test_wrong_length_minhashes_are_dropped(self):
        self._importWrongLengthMinHashes(config)

    def _storedComposition(self, storage, function_id):
        return storage._functions[function_id].toDict()["minhash_shingle_composition"]

    def test_shingle_compositions_survive_the_import(self):
        self._importKeepsCompositions(config)


@pytest.mark.mongo
class MongoExportImportRoundTrip(ExportImportRoundTripMixin):
    """The MongoDB backend splits the disassembly into its own collection; the import has to
    write it there as well, or the CFG view of an imported function stays empty (#67)."""

    SOURCE_DB = "test_export_source_mcrit"
    TARGET_DB = "test_export_target_mcrit"

    def setUp(self):
        server, port = getTestMongoServerAndPort()
        self.client = pymongo.MongoClient(server, int(port))
        for name in (self.SOURCE_DB, self.TARGET_DB):
            self.client.drop_database(name)

    def tearDown(self):
        for name in (self.SOURCE_DB, self.TARGET_DB):
            self.client.drop_database(name)

    def test_memory_export_into_mongo(self):
        _, target = self._roundTrip(config, mongo_config(self.TARGET_DB), compress_data=False)
        db = target.getStorage()._getDb()
        self.assertEqual(db.functions.count_documents({}), db.xcfg.count_documents({}))

    def test_mongo_export_into_mongo(self):
        source, target = self._roundTrip(mongo_config(self.SOURCE_DB), mongo_config(self.TARGET_DB), compress_data=True)
        self.assertEqual(source.getStorage()._getDb().xcfg.count_documents({}), target.getStorage()._getDb().xcfg.count_documents({}))

    def test_wrong_length_minhashes_are_dropped(self):
        self._importWrongLengthMinHashes(mongo_config(self.TARGET_DB))

    def _storedComposition(self, storage, function_id):
        return storage._getDb().functions.find_one({"function_id": function_id})["minhash_shingle_composition"]

    def test_shingle_compositions_survive_the_import(self):
        self._importKeepsCompositions(mongo_config(self.TARGET_DB))

    def test_disassembly_dropped_at_the_source_stays_dropped(self):
        """what mcritweb#67 saw: not the export dropping the xcfg, but STORAGE_DROP_DISASSEMBLY
        having deleted it before the export - the export then carries {} and the import keeps {}"""
        source_config = mongo_config(self.SOURCE_DB)
        source_config.STORAGE_CONFIG.STORAGE_DROP_DISASSEMBLY = True
        source, sample_entry = self._index_with_report(source_config)
        self.assertTrue(all(not entry.xcfg for entry in source.getStorage().getFunctionsBySampleId(sample_entry.sample_id)))
        target = MinHashIndex(mongo_config(self.TARGET_DB))
        target.addImportData(json.loads(json.dumps(source.getExportData())))
        imported = target.getStorage().getSampleBySha256(sample_entry.sha256)
        assert imported is not None
        functions = target.getStorage().getFunctionsBySampleId(imported.sample_id)
        self.assertEqual(sample_entry.statistics["num_functions"], len(functions))
        self.assertTrue(all(not entry.xcfg for entry in functions))
        # ... while everything that survives the drop still round-trips
        self.assertTrue(all(entry.pichash and entry.minhash for entry in functions if entry.num_instructions >= 10))


if __name__ == "__main__":
    unittest.main()
