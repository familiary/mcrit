#!/usr/bin/env python3

import logging
import random
from collections import Counter
from typing import Dict, List, Tuple

from mcrit.libs.utility import generate_segmented_sequence
from mcrit.minhash.MinHash import MinHash
from mcrit.minhash.ShingleLoader import ShingleLoader

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

# Bumped by every change that alters what a shingler produces for some architecture, and recorded
# per sample next to the smda version its minhashes were escaped with (#238).
MINHASH_SHINGLER_REVISION = 1
# Per architecture whose shingling changed, the first revision whose minhashes are current. A sample
# of that architecture recorded at an older revision, or at none, holds stale minhashes, which
# repairMinHashes rehashes and /status counts. 1: FuzzyStatPairShingler reads AArch64 frame sizes.
SHINGLER_REVISION_SINCE = {"aarch64": 1}


class MinHasher:
    MINHASH_STRATEGY_HASH_ALL = 1
    MINHASH_STRATEGY_XOR_ALL = 2
    MINHASH_STRATEGY_SEGMENTED = 3

    def __init__(self, minhash_config, shingler_config):
        self._minhash_config = minhash_config
        self._shingler_config = shingler_config
        self._minhash_seeds = []
        self._minhash_permutation = []
        self._signature_segments = []
        self._shinglers = self._loadShinglers()
        self._shingler_names = [shingler.getName() for shingler in self._shinglers]
        self._initMinhashing()

    def _initMinhashing(self):
        random.seed(self._minhash_config.MINHASH_SEED)
        # initiate sequence of seeds. If XOR or SEGMENTED strategy, this serves as sequence of XOR values instead
        self._minhash_seeds = [random.randint(0, MinHash.getHashMax()) for _ in range(self._minhash_config.MINHASH_SIGNATURE_LENGTH)]
        if self._minhash_config.MINHASH_STRATEGY == MinHasher.MINHASH_STRATEGY_SEGMENTED:
            self._initSegmentedMinHashing()

    def _initSegmentedMinHashing(self):
        # for weighing by segments, init segment fields
        weights = {shingler.getName(): shingler.getWeight() for shingler in self._shinglers}
        self._signature_segments = generate_segmented_sequence(weights, self._minhash_config.MINHASH_SIGNATURE_LENGTH)

    def _loadShinglers(self):
        shingle_loader = ShingleLoader(self._shingler_config)
        return shingle_loader.getShinglers()

    def isMinHashableFunction(self, smda_function):
        is_hashable = False
        if self._minhash_config.MINHASH_FN_MIN_BLOCKS and smda_function.num_blocks > self._minhash_config.MINHASH_FN_MIN_BLOCKS:
            is_hashable = True
        if self._minhash_config.MINHASH_FN_MIN_INS and smda_function.num_instructions > self._minhash_config.MINHASH_FN_MIN_INS:
            is_hashable = True
        return is_hashable

    def _calculateMinHash(self, smda_function):
        if self._minhash_config.MINHASH_STRATEGY == MinHasher.MINHASH_STRATEGY_HASH_ALL:
            return self._calculateMinHashAllSeeds(smda_function)
        elif self._minhash_config.MINHASH_STRATEGY == MinHasher.MINHASH_STRATEGY_XOR_ALL:
            return self._calculateMinHashAllXored(smda_function)
        elif self._minhash_config.MINHASH_STRATEGY == MinHasher.MINHASH_STRATEGY_SEGMENTED:
            return self._calculateMinHashSegmented(smda_function)
        raise NotImplementedError("Unknown MinHasher strategy.")

    def calculateMinHashFromStorage(self, function_tuple):
        function_id, smda_function = function_tuple
        minhash = self._calculateMinHash(smda_function)
        minhash.function_id = function_id
        return minhash

    def calculateMinHashesFromStorage(self, function_tuples):
        minhashes = []
        for function_tuple in function_tuples:
            function_id, smda_function = function_tuple
            minhash = self._calculateMinHash(smda_function)
            minhash.function_id = function_id
            minhashes.append(minhash)
        return minhashes

    def calculateAggregatedScoresFromPackedTuples(
        self,
        packed_tuples: List[Tuple[int, int, bytes, int, int, bytes]],
        ignore_threshold=False,
        minhash_threshold=None,
    ) -> Dict[Tuple[int, int, int], Tuple[int, float]]:
        results: Dict[Tuple[int, int, int], Tuple[int, float]] = {}
        if minhash_threshold is None:
            minhash_threshold = self._minhash_config.MINHASH_MATCHING_THRESHOLD
        for minhash_tuple in packed_tuples:
            sample_id_a, function_id_a, minhash_a, sample_id_b, function_id_b, minhash_b = minhash_tuple
            score = MinHash.calculateMinHashScore(minhash_a, minhash_b, minhash_bits=self._minhash_config.MINHASH_SIGNATURE_BITS)
            if ignore_threshold or score > minhash_threshold:
                key = (sample_id_a, function_id_a, sample_id_b)
                if key not in results or score > results[key][1]:
                    results[key] = (function_id_b, score)
        return results

    def calculateScoresFromPackedTuples(
        self,
        packed_tuples: List[Tuple[int, int, bytes, int, int, bytes]],
        ignore_threshold=False,
        minhash_threshold=None,
    ) -> List[Tuple[int, int, int, int, float]]:
        results = []
        if minhash_threshold is None:
            minhash_threshold = self._minhash_config.MINHASH_MATCHING_THRESHOLD
        for minhash_tuple in packed_tuples:
            sample_id_a, function_id_a, minhash_a, sample_id_b, function_id_b, minhash_b = minhash_tuple
            score = MinHash.calculateMinHashScore(minhash_a, minhash_b, minhash_bits=self._minhash_config.MINHASH_SIGNATURE_BITS)
            if ignore_threshold or score > minhash_threshold:
                results.append((sample_id_a, function_id_a, sample_id_b, function_id_b, score))
        return results

    def _calculateMinHashAllSeeds(self, smda_function):
        """Calculate hash function every time, then take minimum shingle per shingler"""
        minhash_result = MinHash(minhash_bits=self._minhash_config.MINHASH_SIGNATURE_BITS)
        minhash_signature = []
        shingler_composition = Counter()
        for hash_seed in self._minhash_config.HASH_SEEDS:
            minimum_shingles = []
            shingler_outputs = {}
            for shingler in self._shinglers:
                shingler_outputs[shingler.getName()] = shingler.process(smda_function, hash_seed)
            for shingler, outputs in shingler_outputs.items():
                merged_outputs = []
                for output in outputs:
                    merged_outputs.extend(output)
                minimum_shingle = merged_outputs[0]
                if len(merged_outputs) > 1:
                    minimum_shingle = min(merged_outputs)
                minimum_shingles.append(minimum_shingle)
            if self._minhash_config.MINHASH_TRACK_SHINGLES:
                shingler_composition[self._shingler_names[minimum_shingles.index(min(minimum_shingles))]] += 1
            minhash_value = min(minimum_shingles)
            if self._minhash_config.MINHASH_SIGNATURE_BITS < 32:
                minhash_value = minhash_value % (2**self._minhash_config.MINHASH_SIGNATURE_BITS)
            minhash_signature.append(minhash_value)
        minhash_result.setMinHash(minhash_signature)
        minhash_result.shingler_composition = dict(shingler_composition)
        return minhash_result

    def _calculateMinHashAllXored(self, smda_function):
        """Calculate group of shingles per shingler, then XOR all shingles and pick minimum across all combined.
        For multi byte-sequence shinglers, this will always pick the respective minimum shingle"""
        minhash_result = MinHash(minhash_bits=self._minhash_config.MINHASH_SIGNATURE_BITS)
        minhash_signature = []
        shingler_composition = {name: {"count": 0, "size": 0} for name in self._shingler_names}
        shingler_outputs = {}
        for shingler in self._shinglers:
            shingler_outputs[shingler.getName()] = shingler.process(smda_function, 0)
            shingler_composition[shingler.getName()]["size"] = sum([len(item) for item in shingler_outputs[shingler.getName()]])
        for hash_seed in self._minhash_seeds:
            xored_shingles = []
            for shingler, outputs in sorted(shingler_outputs.items()):
                xored_outputs = []
                for output in outputs:
                    xored_outputs.extend([shingle ^ hash_seed for shingle in output])
                minimum_shingle = xored_outputs[0]
                if len(xored_outputs) > 1:
                    minimum_shingle = min(xored_outputs)
                xored_shingles.append(minimum_shingle)
            if self._minhash_config.MINHASH_TRACK_SHINGLES:
                shingler_composition[self._shingler_names[xored_shingles.index(min(xored_shingles))]]["count"] += 1
            minhash_value = min(xored_shingles)
            if self._minhash_config.MINHASH_SIGNATURE_BITS < 32:
                minhash_value = minhash_value % (2**self._minhash_config.MINHASH_SIGNATURE_BITS)
            minhash_signature.append(minhash_value)
        minhash_result.setMinHash(minhash_signature)
        minhash_result.shingler_composition = dict(shingler_composition)
        return minhash_result

    def _calculateMinHashSegmented(self, smda_function):
        """Split the whole signature into segments according to weights and have shingles of the shingler
        only compete within the segment. Use XOR on all shingles to speed up the procedure"""
        minhash_result = MinHash(minhash_bits=self._minhash_config.MINHASH_SIGNATURE_BITS)
        minhash_signature = []
        shingler_composition = {}
        shingler_outputs = {}
        for shingler in self._shinglers:
            shingler.setUseWeights(False)
            shingler_name = shingler.getName()
            shingler_output = shingler.process(smda_function, 0)
            shingler_outputs[shingler_name] = shingler_output[0]
            shingler_size = len(shingler_output[0])
            if self._minhash_config.MINHASH_TRACK_SHINGLES:
                shingler_count = self._signature_segments.count(shingler_name)
                shingler_composition[shingler_name] = {"size": shingler_size, "count": shingler_count}
        for index, shingler_name in enumerate(self._signature_segments):
            hash_seed = self._minhash_seeds[index]
            xored_outputs = [shingle ^ hash_seed for shingle in shingler_outputs[shingler_name]]
            minhash_value = min(xored_outputs)
            if self._minhash_config.MINHASH_SIGNATURE_BITS < 32:
                minhash_value = minhash_value % (2**self._minhash_config.MINHASH_SIGNATURE_BITS)
            minhash_signature.append(minhash_value)
        minhash_result.setMinHash(minhash_signature)
        minhash_result.shingler_composition = dict(shingler_composition)
        return minhash_result
