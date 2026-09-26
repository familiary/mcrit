import logging
import math
from operator import attrgetter
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from smda.common.BinaryInfo import BinaryInfo

import mcrit.matchers.MatcherFlags as MatcherFlags
from mcrit.libs.graph import Graph
from mcrit.storage.FunctionEntry import smdaFunctionFromXcfg
from mcrit.storage.MatchedFunctionEntry import MatchedFunctionEntry
from mcrit.storage.MatchedSampleEntry import MatchedSampleEntry
from mcrit.storage.SampleEntry import SampleEntry

if TYPE_CHECKING:  # pragma: no cover
    from mcrit.storage.SampleEntry import SampleEntry

LOGGER = logging.getLogger(__name__)

# Dataclass, post init
# constructor -> .fromSmdaFunction
# assume sample_entry, smda_function always available


class MatchingResult:
    reference_sample_entry: "SampleEntry"
    other_sample_entry: Optional["SampleEntry"]
    match_aggregation: Dict
    sample_matches: List["MatchedSampleEntry"]
    function_matches: List["MatchedFunctionEntry"]
    # filtered versions - lazily materialized shallow copies, see the properties below
    # function_id -> [(family_id, sample_id), ...]
    library_matches: Dict
    # function_id -> {family_id_a, family_id_b, ...}
    function_id_to_family_ids_matched: Dict
    unique_family_scores_per_sample: Optional[Dict]
    family_id_to_name_map: Optional[Dict]
    is_family_filtered: bool
    is_sample_filtered: bool
    is_function_filtered: bool
    is_score_filtered: bool
    is_sample_count_filtered: bool
    is_family_count_filtered: bool
    is_library_filtered: bool
    is_pic_filtered: bool
    is_query: bool
    filter_values: Dict

    def __init__(self, sample_entry: "SampleEntry") -> None:
        self.reference_sample_entry = sample_entry
        self.unique_family_scores_per_sample = None
        self.family_id_to_name_map = None
        self.is_query = False
        self.filter_values = {}
        self.function_id_to_family_ids_matched = {}
        self._filtered_sample_matches = None
        self._filtered_function_matches = None

    @property
    def num_original_function_matches(self):
        return len(self.function_matches)

    @property
    def num_original_sample_matches(self):
        return len(self.sample_matches)

    @property
    def num_original_family_matches(self):
        return len(set([sample.family_id for sample in self.sample_matches if not sample.is_library]))

    @property
    def num_original_library_matches(self):
        return len(set([sample.family_id for sample in self.sample_matches if sample.is_library]))

    # The filtered_* lists are materialized lazily on first access, as shallow copies of the
    # originals. Building them eagerly (as a deepcopy) in fromDict used to dominate the whole
    # load cost, while the common case - rendering a report without any filter - never needs
    # them to be independent. Filters only ever rebind these lists (never mutate the entries),
    # so sharing the entry objects with the originals is safe.
    # NOTE: getLinkHuntResults does write to matched_family / matched_link_score /
    # matched_unique on the original entries, so those three fields are shared. They are
    # additive-only, absent from getMatchTuple()/toDict(), and read exclusively by the
    # link-hunt views, hence not observable through the filtered_* lists.
    @property
    def filtered_sample_matches(self) -> List["MatchedSampleEntry"]:
        if self._filtered_sample_matches is None:
            self._filtered_sample_matches = list(self.sample_matches)
        return self._filtered_sample_matches

    @filtered_sample_matches.setter
    def filtered_sample_matches(self, value):
        self._filtered_sample_matches = value

    @property
    def filtered_function_matches(self) -> List["MatchedFunctionEntry"]:
        if self._filtered_function_matches is None:
            self._filtered_function_matches = list(self.function_matches)
        return self._filtered_function_matches

    @filtered_function_matches.setter
    def filtered_function_matches(self, value):
        self._filtered_function_matches = value

    def resetFilters(self):
        """Drop all filtering, so that the filtered_* lists are re-derived from the originals.

        Needed whenever a MatchingResult is reused across several filter runs, because
        applyFilterValues() narrows the filtered_* lists cumulatively and never resets them.
        """
        self._filtered_sample_matches = None
        self._filtered_function_matches = None

    @property
    def num_function_matches(self):
        return len(self.filtered_function_matches)

    @property
    def num_sample_matches(self):
        return len(self.filtered_sample_matches)

    @property
    def num_family_matches(self):
        return len(set([sample.family_id for sample in self.filtered_sample_matches if not sample.is_library]))

    @property
    def num_library_matches(self):
        return len(set([sample.family_id for sample in self.filtered_sample_matches if sample.is_library]))

    def setFilterValues(self, filter_dict):
        self.filter_values = filter_dict

    def getFilterValue(self, filter_name):
        return self.filter_values[filter_name] if filter_name in self.filter_values else 0

    def applyFilterValues(self):
        """use the filter_values that have been set before to reduce the data"""
        # filter family/sample
        if self.filter_values.get("filter_unique_only", None):
            self.filterToUniqueMatchesOnly()
        if self.filter_values.get("filter_direct_min_score", None):
            self.filterToDirectMinScore(self.filter_values["filter_direct_min_score"])
        if self.filter_values.get("filter_direct_nonlib_min_score", None):
            self.filterToDirectMinScore(self.filter_values["filter_direct_nonlib_min_score"], nonlib=True)
        if self.filter_values.get("filter_frequency_min_score", None):
            self.filterToFrequencyMinScore(self.filter_values["filter_frequency_min_score"])
        if self.filter_values.get("filter_frequency_nonlib_min_score", None):
            self.filterToFrequencyMinScore(self.filter_values["filter_frequency_nonlib_min_score"], nonlib=True)
        if self.filter_values.get("filter_exclude_own_family", None):
            self.excludeOwnFamily()
        if self.filter_values.get("filter_family_name", None):
            self.filterByFamilyName(self.filter_values["filter_family_name"])
        # filter functions
        if self.filter_values.get("filter_exclude_library", None):
            self.excludeLibraryMatches()
        if self.filter_values.get("filter_library_min_score", None):
            self.filterToFunctionScore(min_score=self.filter_values["filter_library_min_score"], library_only=True)
        if self.filter_values.get("filter_max_num_families", None):
            self.filterToFamilyCount(self.filter_values["filter_max_num_families"])
        if self.filter_values.get("filter_min_num_samples", None):
            self.filterToSampleCount(min_samples=self.filter_values["filter_min_num_samples"])
        if self.filter_values.get("filter_max_num_samples", None):
            self.filterToSampleCount(max_samples=self.filter_values["filter_max_num_samples"])
        if self.filter_values.get("filter_function_min_score", None):
            self.filterToFunctionScore(min_score=self.filter_values["filter_function_min_score"])
        if self.filter_values.get("filter_function_max_score", None):
            self.filterToFunctionScore(max_score=self.filter_values["filter_function_max_score"])
        if self.filter_values.get("filter_function_offset", None):
            self.filterToFunctionOffset(self.filter_values["filter_function_offset"])
        if self.filter_values.get("filter_exclude_pic", None):
            self.excludePicMatches()
        if self.filter_values.get("filter_func_unique", None):
            self.filterToUniqueFunctionMatchesOnly()

    def getFamilyNameByFamilyId(self, family_id):
        if self.family_id_to_name_map is None:
            self.family_id_to_name_map = {}
            for sample_match in self.sample_matches:
                self.family_id_to_name_map[sample_match.family_id] = sample_match.family
        return self.family_id_to_name_map[family_id] if family_id in self.family_id_to_name_map else ""

    def getFamilyIdsMatchedByFunctionId(self, function_id):
        if function_id not in self.function_id_to_family_ids_matched:
            return 0
        return self.function_id_to_family_ids_matched[function_id]

    def filterByFamilyName(self, filter_term):
        """reduce families and samples to those where family_name is part of the family_name"""
        filtered_sample_matches = []
        for sample_match in self.filtered_sample_matches:
            if filter_term in sample_match.family:
                filtered_sample_matches.append(sample_match)
        self.filtered_sample_matches = filtered_sample_matches

    def filterToDirectMinScore(self, min_score, nonlib=False):
        """reduce aggregated sample matches to those with direct score of min_score or higher, but nonlib flag is not applied to library samples"""
        filtered_sample_matches = []
        for sample_match in self.filtered_sample_matches:
            if nonlib:
                if sample_match.is_library:
                    filtered_sample_matches.append(sample_match)
                elif sample_match.matched_percent_nonlib_score_weighted >= min_score:
                    filtered_sample_matches.append(sample_match)
            else:
                if sample_match.matched_percent_score_weighted >= min_score:
                    filtered_sample_matches.append(sample_match)
        self.filtered_sample_matches = filtered_sample_matches

    def filterToFrequencyMinScore(self, min_score, nonlib=False):
        """reduce aggregated sample matches to those with frequency score of min_score or higher, but nonlib flag is not applied to library samples"""
        filtered_sample_matches = []
        for sample_match in self.filtered_sample_matches:
            if nonlib:
                if sample_match.is_library:
                    filtered_sample_matches.append(sample_match)
                elif sample_match.matched_percent_nonlib_frequency_weighted >= min_score:
                    filtered_sample_matches.append(sample_match)
            else:
                if sample_match.matched_percent_frequency_weighted >= min_score:
                    filtered_sample_matches.append(sample_match)
        self.filtered_sample_matches = filtered_sample_matches

    def filterToUniqueMatchesOnly(self):
        """reduce aggregated sample matches to those with unique matches only"""
        filtered_sample_matches = []
        for sample_match in self.filtered_sample_matches:
            unique_info = self.getUniqueFamilyMatchInfoForSample(sample_match.sample_id)
            if unique_info["unique_score"] > 0 or sample_match.is_library:
                filtered_sample_matches.append(sample_match)
        self.filtered_sample_matches = filtered_sample_matches

    def filterToUniqueFunctionMatchesOnly(self):
        """reduce function matches to those with unique matches (with respect to the family) only"""
        aggregated = self.getAggregatedFunctionMatches()
        filtered_function_matches = []
        unique_info_by_function_id = {entry["function_id"]: entry["num_families_matched"] == 1 for entry in aggregated}
        for function_match in self.filtered_function_matches:
            if unique_info_by_function_id[function_match.function_id]:
                filtered_function_matches.append(function_match)
        self.filtered_function_matches = filtered_function_matches

    def filterToFunctionOffset(self, offset):
        """reduce function matches to those that match a specific offset"""
        filtered_function_matches = []
        for function_match in self.filtered_function_matches:
            if function_match.offset == offset:
                filtered_function_matches.append(function_match)
        self.filtered_function_matches = filtered_function_matches

    def excludeOwnFamily(self):
        """remove all sample matches with the same family_id as the reference samples"""
        filtered_sample_matches = []
        for sample_match in self.filtered_sample_matches:
            if sample_match.family_id != self.reference_sample_entry.family_id:
                filtered_sample_matches.append(sample_match)
        self.filtered_sample_matches = filtered_sample_matches

    def excludeLibraryMatches(self):
        """reduce contained matches to those where none of the matches is with a library (transitive library identification)"""
        library_matches = [matches for matches in self.library_matches.values() if matches]
        library_samples = set([match[1] for match_list in library_matches for match in match_list])
        library_matched_functions = [key for key in self.library_matches if self.library_matches[key]]
        self.filtered_sample_matches = [sample_match for sample_match in self.filtered_sample_matches if sample_match.sample_id not in library_samples]
        self.filtered_function_matches = [function_match for function_match in self.filtered_function_matches if function_match.function_id not in library_matched_functions]
        self.is_library_filtered = True

    def excludePicMatches(self):
        """reduce contained matches to those which are not identified as quasi-identical via PIC matching"""
        self.filtered_function_matches = [function_match for function_match in self.filtered_function_matches if not function_match.match_is_pichash]
        self.is_pic_filtered = True

    def filterToSampleCount(self, min_samples=None, max_samples=None):
        """reduce contained matches to those with a maximum of <max_sample_count> matched samples"""
        matched_samples_by_function_id = {}
        for function_match in self.filtered_function_matches:
            if function_match.function_id not in matched_samples_by_function_id:
                matched_samples_by_function_id[function_match.function_id] = []
            if function_match.matched_sample_id not in matched_samples_by_function_id[function_match.function_id]:
                matched_samples_by_function_id[function_match.function_id].append(function_match.matched_sample_id)
        if min_samples is not None:
            self.filtered_function_matches = [
                function_match for function_match in self.filtered_function_matches if len(matched_samples_by_function_id[function_match.function_id]) >= min_samples
            ]
        if max_samples is not None:
            self.filtered_function_matches = [
                function_match for function_match in self.filtered_function_matches if len(matched_samples_by_function_id[function_match.function_id]) <= max_samples
            ]
        self.is_sample_count_filtered = True

    def filterToFamilyCount(self, max_family_count):
        """reduce contained matches to those with a maximum of <max_family_count> matched families"""
        matched_families_by_function_id = {}
        for function_match in self.filtered_function_matches:
            if function_match.function_id not in matched_families_by_function_id:
                matched_families_by_function_id[function_match.function_id] = []
            if function_match.matched_family_id not in matched_families_by_function_id[function_match.function_id]:
                matched_families_by_function_id[function_match.function_id].append(function_match.matched_family_id)
        self.filtered_function_matches = [
            function_match for function_match in self.filtered_function_matches if len(matched_families_by_function_id[function_match.function_id]) <= max_family_count
        ]
        self.is_family_count_filtered = True

    def filterToFunctionScore(self, min_score=None, max_score=None, library_only=False):
        """reduce contained matches to those with a minimum score of <threshold>"""
        filtered_function_matches = []
        for function_match in self.filtered_function_matches:
            if library_only and not function_match.match_is_library:
                filtered_function_matches.append(function_match)
            else:
                if (min_score is not None) and (max_score is not None):
                    if min_score <= function_match.matched_score <= max_score:
                        filtered_function_matches.append(function_match)
                else:
                    # no duplicates expected as per above condition only one case can be true
                    if min_score is not None and function_match.matched_score >= min_score:
                        filtered_function_matches.append(function_match)
                    if max_score is not None and function_match.matched_score <= max_score:
                        filtered_function_matches.append(function_match)
        self.filtered_function_matches = filtered_function_matches
        self.is_score_filtered = True

    def filterToFamilyId(self, family_id):
        """reduce contained matches to chosen family_id by deleting the other sample and function matches"""
        self.filtered_sample_matches = [sample_match for sample_match in self.filtered_sample_matches if sample_match.family_id == family_id]
        self.filtered_function_matches = [function_match for function_match in self.filtered_function_matches if function_match.matched_family_id == family_id]
        self.is_family_filtered = True

    def filterToSampleId(self, sample_id):
        """reduce contained matches to chosen sample_id by deleting the other sample and function matches"""
        self.filtered_sample_matches = [sample_match for sample_match in self.filtered_sample_matches if sample_match.sample_id == sample_id]
        self.filtered_function_matches = [function_match for function_match in self.filtered_function_matches if function_match.matched_sample_id == sample_id]
        self.is_sample_filtered = True

    def filterToFunctionId(self, function_id):
        """reduce contained matches to chosen function_id by deleting the other sample and function matches"""
        self.filtered_function_matches = [function_match for function_match in self.filtered_function_matches if function_match.function_id == function_id]
        self.is_function_filtered = True

    def hasLibraryMatch(self, function_id):
        return function_id in self.library_matches and self.library_matches[function_id]

    def getBestSampleMatchesPerFamily(self, start=None, limit=None, unfiltered=False, library_only=False, malware_only=False):
        by_family: Dict[str, Dict[str, Any]] = {}
        source_matches = self.sample_matches if unfiltered else self.filtered_sample_matches
        for sample_match in source_matches:
            if library_only and not sample_match.is_library:
                continue
            if malware_only and sample_match.is_library:
                continue
            if sample_match.family not in by_family:
                by_family[sample_match.family] = {"score": sample_match.matched_percent_frequency_weighted, "report": sample_match}
            elif sample_match.matched_percent_frequency_weighted > by_family[sample_match.family]["score"]:
                by_family[sample_match.family]["score"] = sample_match.matched_percent_frequency_weighted
                by_family[sample_match.family]["report"] = sample_match
        result_list = []
        for family, score_entry in sorted(by_family.items(), key=lambda e: e[1]["score"], reverse=True):
            result_list.append(score_entry["report"])
        if start is not None:
            result_list = result_list[start:]
        if limit is not None:
            result_list = result_list[:limit]
        return result_list

    def getUniqueFamilyMatchInfoForSample(self, sample_id):
        if self.unique_family_scores_per_sample is None:
            self.unique_family_scores_per_sample = {entry.sample_id: {"functions_matched": 0, "bytes_matched": 0, "unique_score": 0} for entry in self.sample_matches}
            families_matched_by_function_id = {}
            samples_matched_by_function_id = {}
            weighted_bytes_per_function_and_sample = {}
            for function_match_summary in self.function_matches:
                if function_match_summary.function_id not in families_matched_by_function_id:
                    families_matched_by_function_id[function_match_summary.function_id] = set()
                    samples_matched_by_function_id[function_match_summary.function_id] = set()
                families_matched_by_function_id[function_match_summary.function_id].add(function_match_summary.matched_family_id)
                samples_matched_by_function_id[function_match_summary.function_id].add(function_match_summary.matched_sample_id)
                # matches should be weighted by match score - the best one the function has in
                # that sample, not whichever match happened to be iterated last (#157)
                key = (function_match_summary.function_id, function_match_summary.matched_sample_id)
                weighted_bytes_per_function_and_sample[key] = max(
                    weighted_bytes_per_function_and_sample.get(key, 0), function_match_summary.num_bytes * function_match_summary.matched_score / 100.0
                )
            for function_id in families_matched_by_function_id:
                if len(families_matched_by_function_id[function_id]) == 1:
                    for sid in samples_matched_by_function_id[function_id]:
                        self.unique_family_scores_per_sample[sid]["functions_matched"] += 1
                        self.unique_family_scores_per_sample[sid]["bytes_matched"] += weighted_bytes_per_function_and_sample[(function_id, sid)]
            for sid in self.unique_family_scores_per_sample:
                self.unique_family_scores_per_sample[sid]["unique_score"] = (
                    100.0 * self.unique_family_scores_per_sample[sid]["bytes_matched"] / self.reference_sample_entry.binweight
                )
        if sample_id in self.unique_family_scores_per_sample:
            return self.unique_family_scores_per_sample[sample_id]
        else:
            return {"functions_matched": 0, "bytes_matched": 0, "unique_score": 0}

    def getSampleMatches(self, start=None, limit=None, unfiltered=False, library_only=False, malware_only=False):
        by_sample_id = {}
        source_matches = self.sample_matches if unfiltered else self.filtered_sample_matches
        for sample_match in source_matches:
            if library_only and not sample_match.is_library:
                continue
            if malware_only and sample_match.is_library:
                continue
            by_sample_id[sample_match.sample_id] = {"score": sample_match.matched_percent_frequency_weighted, "report": sample_match}
        result_list = []
        for sample_id, score_entry in sorted(by_sample_id.items(), key=lambda e: e[1]["score"], reverse=True):
            result_list.append(score_entry["report"])
        if start is not None:
            result_list = result_list[start:]
        if limit is not None:
            result_list = result_list[:limit]
        return result_list

    def getFunctionMatches(self, start=None, limit=None, unfiltered=False):
        source_matches = self.function_matches if unfiltered else self.filtered_function_matches
        result_list = source_matches
        if start is not None:
            result_list = result_list[start:]
        if limit is not None:
            result_list = result_list[:limit]
        return result_list

    def getAggregatedFunctionMatches(self, start=None, limit=None, unfiltered=False):
        by_function_id: Dict[int, Dict[str, Any]] = {}
        source_matches = self.function_matches if unfiltered else self.filtered_function_matches
        for function_match in source_matches:
            if function_match.function_id not in by_function_id:
                by_function_id[function_match.function_id] = {
                    "function_id": function_match.function_id,
                    "num_bytes": 0,
                    "offset": 0,
                    "best_score": 0,
                    "num_families_matched": 0,
                    "family_ids_matched": set([]),
                    "families_matched": set([]),
                    "num_samples_matched": 0,
                    "sample_ids_matched": set([]),
                    "num_functions_matched": 0,
                    "function_ids_matched": set([]),
                    "minhash_matches": 0,
                    "pichash_matches": 0,
                    "library_matches": 0,
                }
            by_function_id[function_match.function_id]["num_bytes"] = function_match.num_bytes
            by_function_id[function_match.function_id]["offset"] = function_match.offset
            by_function_id[function_match.function_id]["best_score"] = max(function_match.matched_score, by_function_id[function_match.function_id]["best_score"])
            by_function_id[function_match.function_id]["family_ids_matched"].add(function_match.matched_family_id)
            by_function_id[function_match.function_id]["families_matched"].add(self.getFamilyNameByFamilyId(function_match.matched_family_id))
            by_function_id[function_match.function_id]["sample_ids_matched"].add(function_match.matched_sample_id)
            by_function_id[function_match.function_id]["function_ids_matched"].add(function_match.matched_function_id)
            by_function_id[function_match.function_id]["num_families_matched"] = len(by_function_id[function_match.function_id]["family_ids_matched"])
            by_function_id[function_match.function_id]["num_samples_matched"] = len(by_function_id[function_match.function_id]["sample_ids_matched"])
            by_function_id[function_match.function_id]["num_functions_matched"] = len(by_function_id[function_match.function_id]["function_ids_matched"])
            by_function_id[function_match.function_id]["minhash_matches"] += 1 if function_match.match_is_minhash else 0
            by_function_id[function_match.function_id]["pichash_matches"] += 1 if function_match.match_is_pichash else 0
            by_function_id[function_match.function_id]["library_matches"] += 1 if function_match.match_is_library else 0
            by_function_id[function_match.function_id]["is_family_unique"] = True if len(by_function_id[function_match.function_id]["family_ids_matched"]) == 1 else False
        aggregated_matched = [v for k, v in sorted(by_function_id.items())]
        if start is not None:
            aggregated_matched = aggregated_matched[start:]
        if limit is not None:
            aggregated_matched = aggregated_matched[:limit]
        return aggregated_matched

    def getFunctionsSlice(self, start, limit, unfiltered=False):
        return self.filtered_function_matches[start : start + limit]

    def getLinkHuntResults(
        self,
        min_score=None,
        min_lib_score=None,
        min_size=None,
        min_offset=None,
        max_offset=None,
        unpenalized_family_count=3,
        exclude_family_ids=None,
        exclude_sample_ids=None,
        strongest_per_family=True,
    ):
        """Returns the most promising matches for finding inter-family relationship as a list"""
        # run over all function_matches while applying filters, collect aggregate data needed to calculate scores
        by_function_id: Dict[int, Dict[str, Any]] = {}
        link_matches = []
        droppable_fids = set()
        for function_match in sorted(self.function_matches, key=lambda x: (x.matched_score, x.matched_family_id), reverse=True):
            if min_offset is not None and function_match.offset < min_offset:
                continue
            if max_offset is not None and max_offset < function_match.offset:
                continue
            if min_lib_score is not None and function_match.match_is_library and function_match.matched_score < min_lib_score:
                continue
            if min_score is not None and function_match.matched_score < min_score:
                continue
            if min_size is not None and function_match.num_bytes < min_size:
                continue
            if exclude_family_ids is not None and function_match.matched_family_id in exclude_family_ids:
                continue
            if exclude_sample_ids is not None and function_match.matched_sample_id in exclude_sample_ids:
                continue
            if function_match.function_id not in by_function_id:
                by_function_id[function_match.function_id] = {
                    "function_id": function_match.function_id,
                    "num_bytes": 0,
                    "offset": 0,
                    "position": len(by_function_id),
                    "num_families_matched": 0,
                    "family_ids_matched": set([]),
                    "families_matched": set([]),
                    "library_matches": 0,
                }
            # only keep the first / strongest per function_id and family_id
            if strongest_per_family and function_match.matched_family_id in by_function_id[function_match.function_id]["family_ids_matched"]:
                continue
            by_function_id[function_match.function_id]["num_bytes"] = function_match.num_bytes
            by_function_id[function_match.function_id]["offset"] = function_match.offset
            by_function_id[function_match.function_id]["family_ids_matched"].add(function_match.matched_family_id)
            by_function_id[function_match.function_id]["num_families_matched"] = len(by_function_id[function_match.function_id]["family_ids_matched"])
            family_name = self.getFamilyNameByFamilyId(function_match.matched_family_id)
            function_match.matched_family = family_name
            by_function_id[function_match.function_id]["families_matched"].add(family_name)
            by_function_id[function_match.function_id]["library_matches"] += 1 if function_match.match_is_library else 0
            # these will be discarded later on anyway
            if function_match.match_is_library:
                droppable_fids.add(function_match.function_id)
            link_matches.append(function_match)
        filtered_link_matched = []
        for match in link_matches:
            if match.function_id in droppable_fids:
                continue
            filtered_link_matched.append(match)
        link_matches = filtered_link_matched
        for match in link_matches:
            num_families_matched = by_function_id[match.function_id]["num_families_matched"]
            family_adjustment_value = 1 if num_families_matched < unpenalized_family_count else 1 + int(math.log(1 + num_families_matched - unpenalized_family_count, 2))
            # for now, we use an interpolated polynome that is flatter than linear initially and crosses upwards once past ~10 families matched
            family_adjustment_value = (
                1 if num_families_matched < unpenalized_family_count else max(1, math.floor(0.0545793 * num_families_matched**2 + 0.370159 * num_families_matched - 0.170796))
            )
            # magic formula:
            # do a compound score that is based on
            # 10% position, front of binary preferred
            # 20% size, functions larger 100bytes preferred
            # 70% score, as originally produced by minhash
            position_score = 100 * (1 - (by_function_id[match.function_id]["position"] / len(by_function_id)))
            size_score = 100 * (min(300, by_function_id[match.function_id]["num_bytes"]) / 300)
            # scale the whole compound score with frequency weight
            match.matched_link_score = (0.1 * position_score + 0.2 * size_score + 0.7 * match.matched_score) / family_adjustment_value
            match.matched_unique = num_families_matched == 1
            link_matches.sort(key=lambda x: (x.matched_link_score, x.function_id), reverse=True)
        return link_matches

    def clusterLinkHuntResult(self, function_entries, link_hunt_matches):
        """
        Clustering is based on ICFG relationships, so we need to pass in that information in the form of function_entries
        """
        cluster_results = []
        # build some helper lookup structures
        function_offsets = [f.offset for f in function_entries]
        candidate_links = {}
        family_id_to_name = {}
        families_per_offset = {}
        for fm in link_hunt_matches:
            # map names
            family_id_to_name[fm.matched_family_id] = fm.matched_family
            # filter to some minimal link score (remove super common functions to break up huge clusters)
            if fm.matched_family_id not in candidate_links:
                candidate_links[fm.matched_family_id] = []
            if fm.matched_link_score > 1:
                candidate_links[fm.matched_family_id].append(fm)
            # make unique matches identifiable
            if fm.offset not in families_per_offset:
                families_per_offset[fm.offset] = set()
            families_per_offset[fm.offset].add(fm.matched_family_id)
        # extract code references from function_entries via SmdaFunctions
        binfo = BinaryInfo(b"")
        all_function_links = {}
        without_disassembly = 0
        for function_entry in function_entries:
            binfo.architecture = function_entry.architecture
            # None for an entry loaded without its xcfg, and for one whose disassembly was dropped,
            # which reads back as {}
            smda_function = smdaFunctionFromXcfg(function_entry.xcfg, binfo)
            if smda_function is None:
                without_disassembly += 1
                continue
            for _, to_offsets in (smda_function.outrefs or {}).items():
                for to_offset in [o for o in to_offsets if o in function_offsets]:
                    from_offset = smda_function.offset
                    if from_offset not in all_function_links:
                        all_function_links[from_offset] = set()
                    all_function_links[from_offset].add(to_offset)
                    if to_offset not in all_function_links:
                        all_function_links[to_offset] = set()
                    all_function_links[to_offset].add(from_offset)
        if without_disassembly:
            # their own code references are unknown, so clusters can come out split - say so once
            LOGGER.warning("Link hunt clustering: %d function entries have no disassembly and are skipped; their code references are not followed.", without_disassembly)
        # turn into proper (undirected) ICFG and DFS all clusters
        for fam_id, links in candidate_links.items():
            offset_to_link = {}
            g = Graph()
            for index, link_a in enumerate(links):
                g.addNode(link_a.offset)
                offset_to_link[link_a.offset] = link_a
                for link_b in links[index:]:
                    if link_a.offset in all_function_links and link_b.offset in all_function_links[link_a.offset]:
                        g.addEdge(link_a.offset, link_b.offset)
                        g.addEdge(link_b.offset, link_a.offset)
            queue = set([k for k in g.graph.keys()])
            while queue:
                start = queue.pop()
                reachable = g.DFS(start)
                subgraph_score = 0
                max_score = 0
                for offset in reachable:
                    subgraph_score += offset_to_link[offset].matched_link_score
                    max_score = max(max_score, offset_to_link[offset].matched_link_score)
                subgraph_result = {
                    "family_id": fam_id,
                    "family": family_id_to_name[fam_id],
                    "links": [offset_to_link[offset] for offset in reachable],
                    "unique_matches": [offset_to_link[offset] for offset in reachable if len(families_per_offset[offset]) == 1],
                    "score": subgraph_score,
                    "max_score": max_score,
                }
                cluster_results.append(subgraph_result)
                queue.difference_update(reachable)
        return cluster_results

    def toDict(self):
        # the wire format groups the pairs by own function, in the order of function_matches (which
        # is sorted by function_id), as a list of summaries - the same shape the matchers produce
        # and fromDict reads back
        summaries: List[Dict[str, Any]] = []
        current_function_id = None
        current_matches: List[Any] = []
        # a query result carries its (temporary) function ids negated; fromDict reads the sign
        # into is_query and stores the absolute id, so the sign goes back on here
        fid_sign = -1 if self.is_query else 1
        for function_match_entry in self.function_matches:
            if function_match_entry.function_id != current_function_id:
                current_function_id = function_match_entry.function_id
                current_matches = []
                summaries.append(
                    {"num_bytes": function_match_entry.num_bytes, "offset": function_match_entry.offset, "fid": fid_sign * current_function_id, "matches": current_matches}
                )
            current_matches.append(function_match_entry.getMatchTuple())
        # build the dictionary
        matching_entry = {
            "info": {"sample": self.reference_sample_entry.toDict()},
            "matches": {"aggregation": self.match_aggregation, "functions": summaries, "samples": [match.toDict() for match in self.sample_matches]},
        }
        if self.other_sample_entry is not None:
            matching_entry["other_sample_info"] = self.other_sample_entry.toDict()
        return matching_entry

    @classmethod
    def fromDict(cls, entry_dict):
        matching_entry = cls(SampleEntry(None))
        matching_entry.reference_sample_entry = SampleEntry.fromDict(entry_dict["info"]["sample"])
        if "other_sample_info" in entry_dict:
            matching_entry.other_sample_entry = SampleEntry.fromDict(entry_dict["other_sample_info"])
        else:
            matching_entry.other_sample_entry = None
        matching_entry.match_aggregation = entry_dict["matches"]["aggregation"]
        matching_entry.sample_matches = [MatchedSampleEntry.fromDict(entry) for entry in entry_dict["matches"]["samples"]]
        # expand function matches into individual entries. This is the hot loop for large results
        # (#44): a local append, set-backed membership tests instead of list scans, and no
        # per-pair attribute lookups on the result object
        list_of_function_matches: List[MatchedFunctionEntry] = []
        append_function_match = list_of_function_matches.append
        function_id_to_family_ids_matched = matching_entry.function_id_to_family_ids_matched
        library_flag = MatcherFlags.IS_LIBRARY_FLAG
        if "functions" in entry_dict["matches"]:
            function_summaries = entry_dict["matches"]["functions"]
            library_matches: Dict[int, List[Tuple[int, int]]] = {abs(entry["fid"]): [] for entry in function_summaries}
            matching_entry.library_matches = library_matches
            matching_entry.unique_family_scores_per_sample = None
            for function_match_summary in function_summaries:
                num_bytes = function_match_summary["num_bytes"]
                offset = function_match_summary["offset"]
                function_id = function_match_summary["fid"]
                # ensure that we have all function_ids in increasing order, regardless of whether they come from a query or regular match.
                matching_entry.is_query = function_id < 0
                function_id = abs(function_id)
                family_ids_matched = function_id_to_family_ids_matched.setdefault(function_id, [])
                families_seen = set(family_ids_matched)
                function_library_matches = library_matches[function_id]
                library_seen = set(function_library_matches)
                for match_tuple in function_match_summary["matches"]:
                    append_function_match(MatchedFunctionEntry(function_id, num_bytes, offset, match_tuple))
                    matched_family_id = match_tuple[0]
                    if matched_family_id not in families_seen:
                        families_seen.add(matched_family_id)
                        family_ids_matched.append(matched_family_id)
                    if match_tuple[4] & library_flag:
                        library_match = (matched_family_id, match_tuple[1])
                        if library_match not in library_seen:
                            library_seen.add(library_match)
                            function_library_matches.append(library_match)
        list_of_function_matches.sort(key=attrgetter("function_id"))
        matching_entry.function_matches = list_of_function_matches
        # the filtered_* lists are derived lazily on first access, see their properties
        matching_entry.resetFilters()
        return matching_entry

    def __str__(self):
        return "Matched: Samples: {} (unfiltered: {}) Functions: {} (unfiltered: {})".format(
            len(self.filtered_sample_matches),
            len(self.sample_matches),
            len(self.filtered_function_matches),
            len(self.function_matches),
        )
