from typing import Dict, Set, Tuple

from mcrit.matchers.MatcherInterface import MatcherInterface, add_duration


class MatcherVs(MatcherInterface):
    def _additional_setup(self):
        self.matcher_type = "MatcherVs"
        self._function_entries_b = []
        self._sample_to_lib_info = {}
        self._sample_id_to_entry = {}
        self._sample_id = None

    def _getShortlistSize(self) -> int:
        # the match is restricted to the samples it names already; a shortlist ranked over the
        # whole corpus could only drop some of them
        return 0

    def _takesShortlist(self) -> bool:
        return False

    @add_duration
    def getMatchesForSample(self, sample_id: int, other_sample_id: int):
        self._function_entries = self._storage.getFunctionsBySampleId(sample_id)
        self._function_entries_b = self._storage.getFunctionsBySampleId(other_sample_id)
        self._sample_id = sample_id
        sample_entry = self._storage.getSampleById(sample_id)
        self._sample_info = sample_entry.toDict()

        matching_report = self._getMatchesRoutine()

        other_sample_entry = self._storage.getSampleById(other_sample_id)
        other_sample_info = other_sample_entry.toDict()
        matching_report["other_sample_info"] = other_sample_info
        matching_report["info"]["type"] = "matcher_vs"
        return matching_report

    def _getPicHashMatches(self) -> Dict[int, Set[Tuple[int, int, int]]]:
        by_pichash: Dict[int, Set[Tuple[int, int, int]]] = {}
        for function_entry in self._function_entries:
            by_pichash.setdefault(function_entry.pichash, set()).add((function_entry.family_id, function_entry.sample_id, function_entry.function_id))
        for function_entry in self._function_entries_b:
            if function_entry.pichash in by_pichash:
                by_pichash[function_entry.pichash].add((function_entry.family_id, function_entry.sample_id, function_entry.function_id))
        return by_pichash

    def _createMinHashCandidateGroups(self, start=0, end=None) -> Dict[int, Set[int]]:
        # find candidates based on bands
        candidate_groups = super()._createMinHashCandidateGroups(start, end)

        allowed_function_ids = set([entry.function_id for entry in self._function_entries_b])
        # NOTE Also include function ids of entry a to allow self-matches
        allowed_function_ids.update([entry.function_id for entry in self._function_entries])
        for fid, candidates in candidate_groups.items():
            candidate_groups[fid] = candidates.intersection(allowed_function_ids)
        return candidate_groups
