from typing import TYPE_CHECKING, Dict, Set, Tuple

from mcrit.matchers.MatcherInterface import MatcherInterface, add_duration

if TYPE_CHECKING:
    from smda.common.SmdaReport import SmdaReport


class MatcherQuery(MatcherInterface):
    def _additional_setup(self):
        self.matcher_type = "MatcherQuery"
        self._sample_entry = None
        self._sample_id = -1
        # initialize with query object
        self._sample_to_lib_info = {-1: False}
        self._sample_id_to_entry = {-1: 0}

    @add_duration
    def getMatchesForSmdaReport(self, smda_report: "SmdaReport"):
        # create temporary objects similar to the stored samples/functions
        self._sample_entry = self._storage.addSmdaReport(smda_report, isQuery=True)
        self._sample_id = self._sample_entry.sample_id
        self._sample_id_to_entry[self._sample_entry.sample_id] = self._sample_entry
        self._sample_info = self._sample_entry.toDict()
        tmp_function_entries_dict = {}
        for function_entry in self._storage.getFunctionsBySampleId(self._sample_entry.sample_id):
            tmp_function_entries_dict[function_entry.function_id] = function_entry

        minhashes = self._worker.calculateMinHashes(tmp_function_entries_dict.values())
        for minhash in minhashes:
            tmp_function_entries_dict[minhash.function_id].minhash = minhash.getMinHash()
            tmp_function_entries_dict[minhash.function_id].minhash_shingle_composition = minhash.getComposition()
        self._function_entries = list(tmp_function_entries_dict.values())

        match_report = self._getMatchesRoutine()
        match_report["info"]["type"] = "matcher_query"
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
