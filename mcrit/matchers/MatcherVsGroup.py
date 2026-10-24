import logging
from typing import Dict, List, Set, Tuple

from mcrit.matchers.MatcherInterface import MatcherInterface, add_duration

# Only do basicConfig if no handlers have been configured
if not logging.root.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
LOGGER = logging.getLogger(__name__)


class MatcherVsGroup(MatcherInterface):
    """Matcher to compare functions from one sample against functions from a group of other samples."""

    def _additional_setup(self):
        self.matcher_type = "MatcherVsGroup"
        # what matching reads of the other samples: their function ids, and by PicHash their functions
        # sharing one with an own function
        self._other_function_ids: Set[int] = set()
        self._other_pichash_holders: Dict[int, Set[Tuple[int, int, int]]] = {}
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
    def getMatchesForSample(self, sample_id: int, other_sample_ids: List[int]):
        # without disassembly, which matching never reads
        self._function_entries = self._storage.getFunctionsBySampleId(sample_id, with_xcfg=False)
        # 0 is what a function without a PicHash is stored with, not a hash it shares
        own_pichashes = {function_entry.pichash for function_entry in self._function_entries if function_entry.pichash}
        # The other samples one at a time and each once, keeping only what matching reads of them. A
        # child of a cross compare is matched against every other sample named, and holding all of
        # them with their disassembly made its memory grow with the samples named, and with every
        # repetition of one.
        for other_sample_id in dict.fromkeys(other_sample_ids):
            for function_entry in self._storage.getFunctionsBySampleId(other_sample_id, with_xcfg=False):
                self._other_function_ids.add(function_entry.function_id)
                if function_entry.pichash in own_pichashes:
                    holder = (function_entry.family_id, function_entry.sample_id, function_entry.function_id)
                    self._other_pichash_holders.setdefault(function_entry.pichash, set()).add(holder)
        self._sample_id = sample_id
        sample_entry = self._storage.getSampleById(sample_id)
        self._sample_info = sample_entry.toDict()

        LOGGER.info(
            "Performing matching of sample %d against %d other samples, with %d functions total.",
            sample_id,
            len(other_sample_ids),
            len(self._function_entries) + len(self._other_function_ids),
        )

        matching_report = self._getMatchesRoutine()

        matching_report["info"]["type"] = "matcher_vs_group"
        matching_report["other_sample_infos"] = []
        for other_sample_id in other_sample_ids:
            other_sample_entry = self._storage.getSampleById(other_sample_id)
            other_sample_info = other_sample_entry.toDict()
            matching_report["other_sample_infos"].append(other_sample_info)
        return matching_report

    def _getPicHashMatches(self) -> Dict[int, Set[Tuple[int, int, int]]]:
        by_pichash: Dict[int, Set[Tuple[int, int, int]]] = {}
        for function_entry in self._function_entries:
            # 0 is what a function without a PicHash is stored with, not a hash it shares
            if function_entry.pichash:
                by_pichash.setdefault(function_entry.pichash, set()).add((function_entry.family_id, function_entry.sample_id, function_entry.function_id))
        for pichash, holders in self._other_pichash_holders.items():
            by_pichash[pichash].update(holders)
        return by_pichash

    def _createMinHashCandidateGroups(self, start=0, end=None) -> Dict[int, Set[int]]:
        # find candidates based on bands
        candidate_groups = super()._createMinHashCandidateGroups(start, end)

        allowed_function_ids = set(self._other_function_ids)
        # NOTE Also include function ids of entry a to allow self-matches
        allowed_function_ids.update([entry.function_id for entry in self._function_entries])
        for fid, candidates in candidate_groups.items():
            candidate_groups[fid] = candidates.intersection(allowed_function_ids)
        return candidate_groups
