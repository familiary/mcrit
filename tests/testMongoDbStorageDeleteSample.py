from types import MethodType, SimpleNamespace
from unittest import TestCase

from mcrit.config.McritConfig import McritConfig
from mcrit.config.MinHashConfig import MinHashConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.minhash.MinHash import MinHash
from mcrit.storage.MongoDbStorage import MongoDbStorage


class FakeCollection:
    def __init__(self, documents=None):
        self.documents = list(documents or [])
        self.find_calls = []
        self.delete_many_calls = []
        self.delete_one_calls = []

    def find(self, query, projection):
        self.find_calls.append((query, projection))
        return list(self.documents)

    def _matching(self, query):
        return [document for document in self.documents if all(document.get(key) == value for key, value in query.items())]

    def find_one(self, query, projection=None):
        return self.documents[0] if self.documents else None

    def delete_many(self, query):
        self.delete_many_calls.append(query)
        matching = self._matching(query)
        self.documents = [document for document in self.documents if document not in matching]
        return SimpleNamespace(deleted_count=len(matching))

    def delete_one(self, query):
        self.delete_one_calls.append(query)
        matching = self._matching(query)[:1]
        self.documents = [document for document in self.documents if document not in matching]
        return SimpleNamespace(deleted_count=len(matching))

    def count_documents(self, query, limit=None):
        return len(self._matching(query))


class FakeDb(dict):
    def __getattr__(self, name):
        return self[name]


class MongoDbStorageDeleteSampleTest(TestCase):
    def setUp(self):
        config = McritConfig()
        config.STORAGE_CONFIG = StorageConfig()
        config.MINHASH_CONFIG = MinHashConfig()
        config.MINHASH_CONFIG.MINHASH_SIGNATURE_BITS = 32
        config.MINHASH_CONFIG.MINHASH_SIGNATURE_LENGTH = 10
        self.storage = MongoDbStorage(config)

    def testDeleteSampleUsesProjectedFunctionFields(self):
        functions = FakeCollection(
            [
                {"sample_id": 7, "function_id": 10, "minhash": bytes(range(40)).hex()},
                {"sample_id": 7, "function_id": 11, "minhash": bytes(range(40, 80)).hex()},
            ]
        )
        samples = FakeCollection([{"sample_id": 7, "family_id": 1}])
        families = FakeCollection()
        # deleting a sample also removes the disassembly split out of the function documents (#137)
        xcfg = FakeCollection()
        query_xcfg = FakeCollection()
        # deleteSample also drops the sample from the picblockhash index, which first asks settings
        # whether that index is trusted. Empty here, so the hook returns before touching functions -
        # which is what keeps the find_calls assertion below about the #137 projection alone.
        settings = FakeCollection()
        setattr(self.storage, "_database", FakeDb(functions=functions, samples=samples, families=families, xcfg=xcfg, query_xcfg=query_xcfg, settings=settings))
        setattr(
            self.storage,
            "getSampleById",
            MethodType(
                lambda _self, sample_id: SimpleNamespace(
                    family_id=1,
                    statistics={"num_functions": 2},
                    is_library=False,
                ),
                self.storage,
            ),
        )
        setattr(
            self.storage,
            "isSampleId",
            MethodType(
                lambda _self, sample_id: (_ for _ in ()).throw(AssertionError("deleteSample should not re-check sample existence")),
                self.storage,
            ),
        )
        band_updates = []
        family_updates = []
        setattr(
            self.storage,
            "_updateBands",
            MethodType(
                lambda _self, band_hashes, method="push": band_updates.append((band_hashes, method)),
                self.storage,
            ),
        )
        setattr(
            self.storage,
            "_updateFamilyStats",
            MethodType(
                lambda _self, family_id, num_samples, num_functions, num_library_samples: family_updates.append((family_id, num_samples, num_functions, num_library_samples)),
                self.storage,
            ),
        )
        setattr(
            self.storage,
            "getFamily",
            MethodType(
                lambda _self, family_id: SimpleNamespace(num_samples=1, family_id=family_id),
                self.storage,
            ),
        )
        setattr(self.storage, "_updateDbState", MethodType(lambda _self: None, self.storage))
        # the stored-binary bucket is GridFS, which the fake database cannot provide (#95)
        deleted_binaries = []
        setattr(self.storage, "deleteSampleBinary", MethodType(lambda _self, sample_id: deleted_binaries.append(sample_id), self.storage))

        result = self.storage.deleteSample(7)

        self.assertTrue(result)
        self.assertEqual(
            [({"sample_id": 7}, {"_id": 0, "function_id": 1, "minhash": 1})],
            functions.find_calls,
        )
        self.assertEqual([{"sample_id": 7}], functions.delete_many_calls)
        self.assertEqual([{"sample_id": 7}], samples.delete_one_calls)
        self.assertEqual([7], deleted_binaries)
        # decremented by what was deleted (two function documents, one sample), and the family
        # deleted because no sample references it any more (#151)
        self.assertEqual([(1, -1, -2, 0)], family_updates)
        self.assertEqual([{"family_id": 1}], families.delete_one_calls)
        self.assertEqual(1, len(band_updates))
        self.assertEqual("pull", band_updates[0][1])

    def testMinHashRoundTripsFromBytes(self):
        signature = [0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39]
        minhash = MinHash(function_id=1, minhash_signature=signature, minhash_bits=8)

        restored = MinHash(function_id=1, minhash_bytes=minhash.getMinHash(), minhash_bits=8)

        self.assertEqual(signature, restored.getMinHashInt())
