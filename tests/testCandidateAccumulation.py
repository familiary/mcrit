"""The numpy band-hit accumulator must carry function ids past the 32-bit range unchanged.

Function ids come from a counter that never reuses an id, deleted samples included. On the
Malpedia corpus that counter advances by about 1,660 per sample kept, which crosses 2**31 - 1
at around 1.3 million samples. These tests stand a small fake in for the band collections, so
they need no MongoDB and exercise only the accumulation itself.
"""

from unittest import TestCase, main

import numpy as np

from mcrit.config.McritConfig import McritConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.storage.MongoDbStorage import MongoDbStorage

INT32_MAX = 2**31 - 1


class FakeBandCollection:
    def __init__(self, hits):
        self._hits = hits

    def aggregate(self, pipeline):
        return iter(self._hits)


class FakeDatabase:
    """Answers db["band_N"] with that band's canned posting lists."""

    def __init__(self, hits_per_band):
        self._hits_per_band = hits_per_band

    def __getitem__(self, name):
        return FakeBandCollection(self._hits_per_band.get(int(name.split("_")[1]), []))


class CannedBandStorage(MongoDbStorage):
    """Band lookups answer from canned posting lists, every hit claimed by query function -1."""

    def __init__(self, config, hits_per_band):
        super().__init__(config)
        self._canned_database = FakeDatabase(hits_per_band)
        num_bands = config.STORAGE_CONFIG.STORAGE_NUM_BANDS
        self._targets = {band_number: set() for band_number in range(num_bands)}
        self._claimed_by = {band_number: {} for band_number in range(num_bands)}
        for band_number, hits in hits_per_band.items():
            for hit in hits:
                self._targets[band_number].add(hit["band_hash"])
                self._claimed_by[band_number][hit["band_hash"]] = {-1}

    def _getDb(self):
        return self._canned_database

    def _collectBandHashTargets(self, function_id_to_minhash, skips=None):
        return self._targets, self._claimed_by


class CandidateAccumulationTest(TestCase):
    def _storage(self, hits_per_band):
        config = McritConfig()
        config.STORAGE_CONFIG = StorageConfig(STORAGE_CANDIDATE_ACCUMULATION="numpy")
        return CannedBandStorage(config, hits_per_band)

    def test_function_ids_above_the_32_bit_range_come_back_unchanged(self):
        large_ids = [INT32_MAX + 1, INT32_MAX + 12345, 2**40 + 7]
        storage = self._storage({0: [{"band_hash": 11, "function_ids": [5] + large_ids}]})
        self.assertEqual(storage.getCandidatesForMinHashes({-1: None}), {-1: {5, *large_ids}})
        arrays = storage.getCandidateArraysForMinHashes({-1: None})
        self.assertEqual(arrays[-1].dtype, np.int64)
        self.assertEqual(arrays[-1].tolist(), sorted([5] + large_ids))

    def test_band_matches_are_counted_exactly_above_the_32_bit_range(self):
        # 2**32 + 3 wraps to 3 in 32 bits, so a narrowing accumulator would count it as a
        # second hit on function 3 and let both through a two-band requirement
        storage = self._storage(
            {
                0: [{"band_hash": 21, "function_ids": [3, INT32_MAX + 1]}],
                1: [{"band_hash": 22, "function_ids": [2**32 + 3, INT32_MAX + 1]}],
            }
        )
        self.assertEqual(storage.getCandidatesForMinHashes({-1: None}, band_matches_required=2), {-1: {INT32_MAX + 1}})


if __name__ == "__main__":
    main()
