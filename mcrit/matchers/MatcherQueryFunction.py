from typing import TYPE_CHECKING, Dict, Set, Tuple

from mcrit.matchers.MatcherInterface import MatcherInterface, add_duration
from mcrit.storage.FunctionEntry import FunctionEntry
from mcrit.storage.SampleEntry import SampleEntry

if TYPE_CHECKING:
    from smda.common.SmdaReport import SmdaReport


class MatcherQueryFunction(MatcherInterface):
    def _additional_setup(self):
        self.matcher_type = "MatcherQueryFunction"
        self._sample_entry = None
        self._sample_id = -1
        # initialize with query object
        self._sample_to_lib_info = {-1: False}
        self._sample_id_to_entry = {-1: 0}

    @add_duration
    def getMatchesForSmdaFunction(self, smda_report: "SmdaReport"):
        smda_function = None
        for fun in smda_report.getFunctions():
            smda_function = fun
        if smda_function is None:
            raise ValueError("SmdaReport does not contain any function.")
        if not self._worker.minhasher.isMinHashableFunction(smda_function):
            raise ValueError("SmdaFunction is not MinHashable.")
        # create temporary objects similar to the stored samples/functions
        self._sample_entry = SampleEntry(smda_report)
        existing_sample_entry = self._storage.getSampleBySha256(smda_report.sha256)
        if existing_sample_entry and self._exclude_self_matches:
            self._sample_entry.sample_id = existing_sample_entry.sample_id
        self._sample_id = self._sample_entry.sample_id
        self._sample_id_to_entry[self._sample_entry.sample_id] = self._sample_entry
        self._sample_info = self._sample_entry.toDict()
        # calculate MinHash for function
        minhash = self._worker.minhasher._calculateMinHash(smda_function)
        function_entry = FunctionEntry(self._sample_entry, smda_function, -1, minhash)
        self._function_entries = [function_entry]
        match_report = self._getMatchesRoutine()
        match_report["info"]["type"] = "matcher_query_function"
        return match_report

    def _getPicHashMatches(self) -> Dict[int, Set[Tuple[int, int, int]]]:
        return self._getCorpusPicHashMatchesForFunctionEntries()

    def _createMatchingCache(self, candidate_groups):
        function_ids_from_storage = set()
        for own_function_id, other_function_ids in candidate_groups.items():
            for function_id in other_function_ids:
                if function_id >= 0:
                    function_ids_from_storage.add(function_id)
        cache = self._storage.createMatchingCache(function_ids_from_storage, allow_self_return=True, previous=self._matching_cache)
        self._matching_cache = cache
        cache.addFunctionEntriesToCache(self._function_entries)
        return cache
