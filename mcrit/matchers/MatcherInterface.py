import datetime
import functools
import logging
import math
from collections import Counter, defaultdict
from timeit import default_timer as timer
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import numpy as np
import tqdm

import mcrit.matchers.MatcherFlags as MatcherFlags
from mcrit.libs.parallel import create_process_pool
from mcrit.queue.QueueRemoteCalls import NoProgressReporter

if TYPE_CHECKING:  # pragma: no cover
    from mcrit.storage.FunctionEntry import FunctionEntry
    from mcrit.storage.MatchingCache import MatchingCache
    from mcrit.storage.MemoryStorage import MemoryStorage
    from mcrit.Worker import Worker


# Only do basicConfig if no handlers have been configured
if not logging.root.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
LOGGER = logging.getLogger(__name__)

# function_id_own, sample_id_foreign, function_id_foreign, score, is_pic_match, is_min_match
HarmonizedMatches = Dict[Tuple[int, int, int], Tuple[float, bool, bool]]
PichashMatches = Dict[int, Set[Tuple[int, int, int]]]
MinhashMatches = List[Tuple[int, int, int, int, float]]


def build_method_str_from_args(args):
    # extract the job parameters
    method_str = ""
    try:
        method_type = f"{type(args[0])}"[8:-2]
        method_str = method_type.split(".")[-1]
        if len(args) > 1:
            method_str += "(" + ", ".join([str(i) for i in list(args)[1:]]) + ")"
    except (IndexError, TypeError):
        pass
    return method_str


def add_duration(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = timer()
        result = func(*args, **kwargs)
        time = timer() - start
        result["info"]["job"] = {"duration": time, "timestamp": datetime.datetime.now().isoformat(), "parameters": build_method_str_from_args(args)}
        return result

    return wrapper


class MatcherInterface:
    # how many band votes one exact (PicHash) match is worth when ranking the shortlist
    _PICHASH_SHORTLIST_VOTE_WEIGHT = 4

    def __init__(self, worker: "Worker", minhash_threshold=None, pichash_size=None, band_matches_required=None, exclude_self_matches=False, progress_reporter=NoProgressReporter()):
        self.matcher_type = "MatcherInterface"
        # Extended by Query, VS, Sample
        self._worker: Worker = worker
        self._storage = worker.getStorage()
        self._function_entries: List[FunctionEntry] = []
        self._sample_info: Optional[Dict] = None
        self._sample_id: Optional[int] = None
        self._progress_reporter = progress_reporter
        self._num_batches = None
        # job-scoped MatchingCache handle: with STORAGE_MATCHING_CACHE_PERSIST the cache is
        # retained across this job's batches by passing it back into createMatchingCache as
        # previous=. It lives on the matcher, never on the shared storage object, so
        # concurrent jobs/requests cannot see each other's entries (#110).
        self._matching_cache = None
        # stage-1 result for this job: the corpus samples the exact stage may look at, or None
        # when shortlisting is disabled/unavailable. Job-scoped like the matching cache, so
        # concurrent jobs cannot see each other's shortlists.
        self._sample_shortlist: Optional[Set[int]] = None
        # candidate groups produced by the stage-1 banding pass, served back to the batch loop
        self._shortlist_candidate_cache: Optional[Dict[int, Tuple[np.ndarray, np.ndarray]]] = None
        # the shortlist as a sorted array, for vectorised membership tests
        self._shortlist_array: Optional[np.ndarray] = None
        self._minhash_threshold = minhash_threshold
        self._exclude_self_matches = exclude_self_matches
        if pichash_size is None:
            pichash_size = self._worker.config.MINHASH_CONFIG.PICHASH_SIZE
        if band_matches_required is None:
            band_matches_required = self._worker.config.MINHASH_CONFIG.BAND_MATCHES_REQUIRED
        self._band_matches_required = band_matches_required
        self._pichash_size = pichash_size
        self._additional_setup()
        self._sample_to_lib_info: Dict[int, bool]
        # NOTE: query matchers seed this with a sentinel entry for the query sample itself
        self._sample_id_to_entry: Dict[int, Any]

    def _additional_setup(self):
        pass

    def getMatchesForSample(self, sample_id, other_sample_id, minhash_threshold=None):
        # VS, Sample
        raise NotImplementedError

    def getMatchesForSmdaReport(self, smda_report, minhash_threshold=None):
        # Query
        raise NotImplementedError

    def _getPicHashMatches(self) -> Dict[int, Set[Tuple[int, int, int]]]:
        # Query, VS, Sample
        # Every Matcher has own implementation
        raise NotImplementedError

    def _createMinHashCandidateGroups(self, start=0, end=None) -> Dict[int, Set[int]]:
        # Query, VS, Sample
        # Sample , Query have this plain version
        # VS adds another intersection step to this version
        # find candidates based on bands
        if end is None:
            end = len(self._function_entries)
        if self._shortlist_candidate_cache is not None:
            # stage 1 already banded every query function; reuse its result rather than asking
            # the index the same question twice
            wanted = {function_entry.function_id for function_entry in self._function_entries[start:end]}
            candidate_groups = {function_id: candidates for function_id, candidates in self._shortlist_candidate_cache.items() if function_id in wanted}
        else:
            function_id_to_minhash = {}
            for function_entry in self._function_entries[start:end]:
                function_id_to_minhash[function_entry.function_id] = function_entry.getMinHash(minhash_bits=self._worker._minhash_config.MINHASH_SIGNATURE_BITS)
            candidate_groups = self._storage.getCandidatesForMinHashes(function_id_to_minhash, band_matches_required=self._band_matches_required)
        return self._restrictToShortlist(candidate_groups)

    # ---- two-stage matching: shortlist the corpus samples worth matching exactly ----------

    def _getShortlistSize(self) -> int:
        return getattr(self._worker.config.MINHASH_CONFIG, "MINHASH_MATCHING_SHORTLIST_SIZE", 0)

    def _resolveSampleIds(self, function_ids: np.ndarray) -> Optional[np.ndarray]:
        """Sample id per candidate function id, or None when storage cannot answer cheaply."""
        resolver = getattr(self._storage, "getSampleIdsForFunctionIdArray", None)
        is_complete = getattr(self._storage, "isFunctionRangeIndexComplete", None)
        if resolver is None or is_complete is None or not is_complete():
            return None
        return resolver(function_ids)

    def _computeSampleShortlist(self, pichash_matches: Optional[HarmonizedMatches] = None) -> Optional[Set[int]]:
        """Stage 1: the corpus samples that the exact stage will be restricted to.

        Each query function votes at most once per corpus sample, so a sample is ranked by how
        many *distinct* query functions found it - not by how many candidate pairs it
        contributed, which would just re-rank by sample size. Ties are broken towards the lower
        sample id so a shortlist is reproducible rather than dict-order dependent.

        PicHash evidence votes too, and that is not a refinement: a sample can be reported
        purely through exact (PicHash) matches, and ranking it on band votes alone dropped
        samples the unrestricted matcher put in its top ten (measured: top-10 recall 0.60 before
        this was counted, 1.00 after). An exact match is also stronger evidence than a band
        collision, so it is weighted above one.

        The banding pass done here is *kept* (see _shortlist_candidate_cache) and served back to
        the batch loop, so two-stage matching bands the query once, exactly like the one-stage
        path - the shortlist costs the vote accounting, not a second index traversal.

        Returns None when shortlisting is off or unavailable, which every caller treats as
        "do not restrict anything" - the unbounded behaviour.
        """
        shortlist_size = self._getShortlistSize()
        if shortlist_size <= 0 or not self._function_entries:
            return None
        signature_bits = self._worker._minhash_config.MINHASH_SIGNATURE_BITS
        votes: Counter = Counter()
        # candidates are kept as arrays, together with the sample each one belongs to, so the
        # restriction pass can be a boolean mask instead of resolving and re-boxing every
        # candidate a second time. Measured on a 10k-sample corpus, doing this per candidate in
        # Python cost 1.2 s of voting plus 1.1 s of restriction on a 361k-pair query.
        cached_groups: Dict[int, Tuple[np.ndarray, np.ndarray]] = {}
        per_function_votes: List[np.ndarray] = []
        chunk_size = max(1, min(self._worker.config.MINHASH_CONFIG.MINHASH_MATCHING_FUNCTION_BATCH_SIZE, 1000))
        for start_index in range(0, len(self._function_entries), chunk_size):
            function_id_to_minhash = {}
            for function_entry in self._function_entries[start_index : start_index + chunk_size]:
                function_id_to_minhash[function_entry.function_id] = function_entry.getMinHash(minhash_bits=signature_bits)
            # ask for arrays where storage can produce them: the accumulation is already
            # numpy, and boxing it into sets only to unbox it here is pure overhead
            array_getter = getattr(self._storage, "getCandidateArraysForMinHashes", None)
            if array_getter is not None:
                candidate_groups = array_getter(function_id_to_minhash, band_matches_required=self._band_matches_required)
            else:
                candidate_groups = self._storage.getCandidatesForMinHashes(function_id_to_minhash, band_matches_required=self._band_matches_required)
            for function_id, candidate_ids in candidate_groups.items():
                if not len(candidate_ids):
                    continue
                candidates = candidate_ids if isinstance(candidate_ids, np.ndarray) else np.fromiter(candidate_ids, dtype=np.int64, count=len(candidate_ids))
                sample_ids = self._resolveSampleIds(candidates)
                if sample_ids is None:
                    LOGGER.warning("Shortlisting requested but the function range index is unavailable; matching against the whole corpus instead.")
                    return None
                cached_groups[function_id] = (candidates, sample_ids)
                # np.unique per query function is what makes this one vote per sample per
                # query function rather than one per candidate pair
                resolved = sample_ids[sample_ids >= 0]
                if resolved.size:
                    per_function_votes.append(np.unique(resolved))
        if per_function_votes:
            voted_samples, vote_counts = np.unique(np.concatenate(per_function_votes), return_counts=True)
            votes = Counter(dict(zip(voted_samples.tolist(), vote_counts.tolist())))
        # an exact match is worth more than a band collision; the weight only has to be big
        # enough that a pichash-only sample outranks a sample carried by a single band hit
        for _own_function_id, foreign_sample_id, _foreign_function_id in pichash_matches or {}:
            votes[foreign_sample_id] += self._PICHASH_SHORTLIST_VOTE_WEIGHT
        # the query's own sample must never be shortlisted away - self-matches are how a
        # sample-vs-corpus result reports its own functions
        votes.pop(self._sample_id, None)
        shortlist = self._rankShortlist(votes, shortlist_size)
        if self._sample_id is not None:
            shortlist.add(self._sample_id)
        self._shortlist_candidate_cache = cached_groups
        LOGGER.info("Shortlisted %d of %d voted samples for exact matching", len(shortlist), len(votes))
        return shortlist

    def _rankShortlist(self, votes: "Counter", shortlist_size: int) -> Set[int]:
        """Pick the samples to keep, by absolute *and* by relative evidence.

        Ranking on vote count alone is the wrong proxy, and measurably so: MCRIT scores a
        matched sample by the *percentage* of it that matched, so a small sample whose handful
        of functions all match outranks a large one sharing the same handful. A count-ranked
        shortlist drops exactly those, and it showed up as top-10 recall of 0.60 against the
        unrestricted result.

        Neither ranking alone is right either - coverage alone would fill the shortlist with
        one-function samples at 100%. So the two rankings are interleaved, and the shortlist is
        the first `shortlist_size` distinct samples of that merge: whichever ranking a sample
        belongs in, it only has to be near the top of *one* of them to survive.
        """
        function_counts = {}
        counts_getter = getattr(self._storage, "getSampleFunctionCounts", None)
        if counts_getter is not None:
            # only the samples that actually received a vote - at most a few thousand. Asking for
            # the whole corpus was the last place per-query cost still grew with corpus size.
            try:
                function_counts = counts_getter(votes.keys())
            except TypeError:  # a storage that predates the argument
                function_counts = counts_getter()
        by_count = sorted(votes.items(), key=lambda item: (-item[1], item[0]))
        by_coverage = sorted(votes.items(), key=lambda item: (-(item[1] / max(1, function_counts.get(item[0], 1))), -item[1], item[0]))
        shortlist: Set[int] = set()
        for position in range(max(len(by_count), len(by_coverage))):
            for ranking in (by_count, by_coverage):
                if position < len(ranking):
                    shortlist.add(ranking[position][0])
                    if len(shortlist) >= shortlist_size:
                        return shortlist
        return shortlist

    def _restrictToShortlist(self, candidate_groups: Dict[int, Any]) -> Dict[int, Set[int]]:
        """Drop candidates outside the shortlist, so the exact stage is bounded by it.

        Entries arrive as the (candidates, sample_ids) arrays stage 1 already produced, so this
        is a boolean mask per query function and the only Python-level work left is boxing the
        *surviving* candidates - which is the small set, by construction.
        """
        if self._sample_shortlist is None:
            return candidate_groups
        if self._shortlist_array is None:
            self._shortlist_array = np.fromiter(sorted(self._sample_shortlist), dtype=np.int64, count=len(self._sample_shortlist))
        restricted = {}
        for function_id, entry in candidate_groups.items():
            if isinstance(entry, tuple):
                candidates, sample_ids = entry
            else:  # a caller that did not come through stage 1's cache
                if not entry:
                    continue
                candidates = np.fromiter(entry, dtype=np.int64, count=len(entry))
                resolved = self._resolveSampleIds(candidates)
                if resolved is None:
                    return entry
                sample_ids = resolved
            if not candidates.size:
                continue
            surviving = candidates[np.isin(sample_ids, self._shortlist_array)]
            if surviving.size:
                restricted[function_id] = set(surviving.tolist())
        return restricted

    def _createMatchingCache(self, candidate_groups):
        # Query, VS, Sample
        # This version is used by VS, Sample
        # Own version by Query
        cache_function_ids = set()
        for candidate_key, candidate_functions in candidate_groups.items():
            cache_function_ids.add(candidate_key)
            cache_function_ids.update(candidate_functions)
        LOGGER.info("creating cache for %d functions", len(cache_function_ids))
        self._matching_cache = self._storage.createMatchingCache(cache_function_ids, previous=self._matching_cache)
        return self._matching_cache

    ######## Below this line, nothing will be overwritten by Subclasses #########

    def filter_pichashes_from_candidate_groups(self, matching_cache, candidate_groups, pichash_matches):
        finished_tuples = set()
        for pichash_match in pichash_matches:
            own_fid, foreign_sid, foreign_fid = pichash_match
            if own_fid in candidate_groups:
                # remove all candidates of the respective sample instead
                if (own_fid, foreign_sid) not in finished_tuples:
                    if matching_cache.isSampleId(foreign_sid):
                        pichash_match_sid_fids = matching_cache.getFunctionIdsBySampleId(foreign_sid)
                        candidate_groups[own_fid].difference_update(pichash_match_sid_fids)
                        finished_tuples.add((own_fid, foreign_sid))
        return candidate_groups

    def _getMatchesRoutine(self):
        # Query, VS, Sample
        # All use this version
        try:
            return self._getMatchesRoutineInner()
        finally:
            # the matching cache may be retained across batches; the job is over either way,
            # so drop it even if the job raised - a cancelled job (JobProgressReporter raises
            # from inside the batch loop) would otherwise keep up to
            # STORAGE_MATCHING_CACHE_MAX_ENTRIES minhashes pinned for this matcher's lifetime.
            self._matching_cache = None
            self._shortlist_candidate_cache = None
            self._shortlist_array = None

    def _iterCandidateGroupBatches(self):
        """Yield candidate_groups dicts sized for one cache+scoring pass each.

        With MINHASH_MATCHING_MAX_PAIRS > 0, query functions are banded in small chunks and
        packed into batches by their actual candidate volume, flushing whenever the pending
        pair count reaches the budget. Peak matcher memory is then bounded by the budget (plus
        the widest single candidate group) instead of scaling with whatever a fixed count of
        query functions happens to drag in - candidate volume per query function spans five
        orders of magnitude on real corpora, so a fixed count cannot bound both sides.

        With the budget disabled (0), the legacy fixed-size batching is preserved verbatim.
        Progress is reported per banding chunk in budget mode (flush points are data-dependent),
        per batch in legacy mode.
        """
        batch_size = self._worker.config.MINHASH_CONFIG.MINHASH_MATCHING_FUNCTION_BATCH_SIZE
        max_pairs = getattr(self._worker.config.MINHASH_CONFIG, "MINHASH_MATCHING_MAX_PAIRS", 0)
        num_functions = len(self._function_entries)
        if max_pairs <= 0:
            quotient, remainder = divmod(num_functions, batch_size)
            self._num_batches = quotient + int(bool(remainder))  # always round up
            self._progress_reporter.set_total(self._num_batches)
            for start_index in range(0, num_functions, batch_size):
                yield self._createMinHashCandidateGroups(start=start_index, end=start_index + batch_size)
                if self._num_batches > 1:
                    self._progress_reporter.step()
            return
        # banding itself is index lookups whose transient cost scales with the chunk, so the
        # chunk only needs to be small enough that accumulation stays cheap - it is not the
        # memory-relevant knob, the pair budget is.
        band_chunk = min(batch_size, 1000)
        quotient, remainder = divmod(num_functions, band_chunk)
        self._num_batches = quotient + int(bool(remainder))
        self._progress_reporter.set_total(self._num_batches)
        pending_groups = {}
        pending_pairs = 0
        for start_index in range(0, num_functions, band_chunk):
            chunk_groups = self._createMinHashCandidateGroups(start=start_index, end=start_index + band_chunk)
            for function_id, candidates in chunk_groups.items():
                pending_groups[function_id] = candidates
                pending_pairs += len(candidates)
                # The configured function batch size stays an upper bound on how many query functions
                # share a batch. The pair budget is the better memory bound and binds first on wide
                # candidate sets, but it is a single global default: without this ceiling, a deployment
                # that lowered MINHASH_MATCHING_FUNCTION_BATCH_SIZE to fit a small host would silently
                # get batches packed all the way to the budget instead (measured: 5000 query functions
                # at 100 candidates each and batch size 200 went from 25 batches of 20k pairs to one
                # batch of 500k). Honouring both keeps the knob documented in docs/TUNING.md meaningful
                # while the budget still guards the tail.
                if pending_pairs >= max_pairs or len(pending_groups) >= batch_size:
                    limit_reached = "pair budget" if pending_pairs >= max_pairs else "function batch size"
                    LOGGER.info("Flushing batch, %s reached (%d pairs, %d query functions)", limit_reached, pending_pairs, len(pending_groups))
                    yield pending_groups
                    pending_groups = {}
                    pending_pairs = 0
            if self._num_batches > 1:
                self._progress_reporter.step()
        if pending_groups:
            yield pending_groups

    def _getMatchesRoutineInner(self):
        pichash_matches = self._harmonizePicHashMatches(self._getPicHashMatches())
        LOGGER.info("Calculated PicHash matches")
        all_minhash_matches = {}
        # only do minhashing, if we have bands to evaluate
        if self._band_matches_required > 0:
            # stage 1 runs once for the whole query, not per batch: a shortlist that differed
            # between batches would make the reported matches depend on batch boundaries
            self._sample_shortlist = self._computeSampleShortlist(pichash_matches)
            for candidate_groups in self._iterCandidateGroupBatches():
                LOGGER.info("Created candidate groups from MinHash bands")
                matching_cache = self._createMatchingCache(candidate_groups)
                LOGGER.info("Created MatchingCache")
                if self._worker._minhash_config.PICHASH_IMPLIES_MINHASH_MATCH:
                    LOGGER.info("Removing PicHash matches")
                    candidate_groups = self.filter_pichashes_from_candidate_groups(matching_cache, candidate_groups, pichash_matches)
                    LOGGER.info("Removed PicHash matches from CandidateGroups")
                LOGGER.info("Now starting MinHash matching")
                minhash_matches = self._harmonizeMinHashMatches(self._sample_id, self._performMinHashMatching(candidate_groups, matching_cache))
                all_minhash_matches.update(minhash_matches)
            LOGGER.info("Calculated MinHash matches.")
        matching_report = self._craftResultDict(pichash_matches, all_minhash_matches)
        LOGGER.info("Returning aggregated match report.")
        return matching_report

    # Reports PROGRESS
    @staticmethod
    def _minMatchesForThreshold(signature_length: int, minhash_threshold: float) -> int:
        """Smallest number of matching signature fields that still passes `score > threshold`.

        Derived by evaluating the very expression the scalar path uses
        (`100.0 * matches / signature_length > threshold`) over every possible match count, so
        the integer predicate is exact by construction rather than by a rounding argument.
        """
        for matches in range(signature_length + 1):
            if 100.0 * matches / signature_length > minhash_threshold:
                return matches
        return signature_length + 1

    # Reports PROGRESS
    # Reports PROGRESS
    def _performMinHashMatchingVectorized(self, candidate_groups: Dict[int, Set[int]], cache) -> List[Tuple[int, int, int, int, float]]:
        """B2: score one query function against all of its candidates as an (C, L) block.

        Equivalent to _performMinHashMatching's single-process path, pair for pair:
        candidates are visited in ascending function_id_b, and a strictly greater score is
        required to displace the incumbent - so ties keep the smallest function_id_b, exactly
        as `max(..., key=score)` does when it keeps the first element.

        The point is not only speed: pairs are never materialised as tuples, so peak memory
        goes from O(pairs) to O(candidates of the widest single query function).
        """
        minhash_config = self._worker.config.MINHASH_CONFIG
        signature_length = minhash_config.MINHASH_SIGNATURE_LENGTH
        min_matches = self._minMatchesForThreshold(signature_length, minhash_config.MINHASH_MATCHING_THRESHOLD)
        sorted_ids, rows, matrix = cache.getSignatureMatrix(signature_length=signature_length, signature_bits=minhash_config.MINHASH_SIGNATURE_BITS)
        func_id_to_sample_id = cache._func_id_to_sample_id
        organized_matching_results: Dict[Tuple[int, int, int], Tuple[int, float]] = {}
        score_counts = np.zeros(signature_length + 1, dtype=np.int64)
        block_limit = max(1, minhash_config.MINHASH_MATCHING_CANDIDATE_WORKPACK_SIZE)
        num_pairs = sum(len(candidate_ids) for candidate_ids in candidate_groups.values())
        if self._num_batches == 1:
            self._progress_reporter.set_total(len(candidate_groups))
        for function_id_a, candidate_ids in tqdm.tqdm(sorted(candidate_groups.items()), total=len(candidate_groups)):
            if self._num_batches == 1:
                self._progress_reporter.step()
            if not candidate_ids:
                continue
            candidates = np.fromiter(candidate_ids, dtype=np.int64, count=len(candidate_ids))
            candidates.sort()
            candidates = candidates[candidates != function_id_a]
            if not candidates.size:
                continue
            query_row = cache.getRowsForFunctionIds(np.array([function_id_a], dtype=np.int64), sorted_ids, rows)
            query_signature = matrix[query_row[0]]
            candidate_rows = cache.getRowsForFunctionIds(candidates, sorted_ids, rows)
            sample_id_a = func_id_to_sample_id[function_id_a]
            # chunked so the gathered block stays bounded even for a query function sitting in
            # a huge equivalence class
            for offset in range(0, candidates.size, block_limit):
                chunk_rows = candidate_rows[offset : offset + block_limit]
                chunk_ids = candidates[offset : offset + block_limit]
                # candidates sharing a signature share a matrix row, so compare each distinct
                # row once and fan the result back out. Same scores, fewer comparisons - and
                # the gathered block is the deduplicated one, so peak memory drops with it.
                unique_rows, inverse = np.unique(chunk_rows, return_inverse=True)
                matches = (matrix[unique_rows] == query_signature).sum(axis=1, dtype=np.int16)[inverse]
                score_counts += np.bincount(matches, minlength=signature_length + 1)
                surviving = np.flatnonzero(matches >= min_matches)
                if not surviving.size:
                    continue
                surviving_ids = chunk_ids[surviving]
                surviving_scores = 100.0 * matches[surviving] / signature_length
                for function_id_b, score in zip(surviving_ids.tolist(), surviving_scores.tolist()):
                    key = (sample_id_a, function_id_a, func_id_to_sample_id[function_id_b])
                    incumbent = organized_matching_results.get(key)
                    if incumbent is None or score > incumbent[1]:
                        organized_matching_results[key] = (function_id_b, score)
        LOGGER.info("Minhash Signature Field Match Counts: " + ", ".join("(%s: %d)" % (float(matches), int(count)) for matches, count in enumerate(score_counts) if count))
        LOGGER.info("Vectorized matching over %d pairs in %d candidate groups", num_pairs, len(candidate_groups))
        matching_results: MinhashMatches = [(key[0], key[1], key[2], value[0], value[1]) for key, value in organized_matching_results.items()]
        return matching_results

    # Reports PROGRESS
    def _performMinHashMatching(self, candidate_groups: Dict[int, Set[int]], cache) -> List[Tuple[int, int, int, int, float]]:
        # Query, VS, Sample
        # All use the same version
        """perform matching between candidates, provided as candidate_groups in the form of a dict: {function_id: [function_ids]}"""
        if getattr(self._worker.config.MINHASH_CONFIG, "MINHASH_MATCHING_VECTORIZED", False):
            try:
                return self._performMinHashMatchingVectorized(candidate_groups, cache)
            except (NotImplementedError, KeyError) as error:
                # NotImplementedError: StorageBackedMatchingCache serves minhashes lazily.
                # KeyError: an id the matrix does not cover (e.g. a function whose minhash was
                # dropped). Either way the scalar path can still do the work.
                LOGGER.warning("Vectorised scoring unavailable (%s: %s), scoring per pair instead", type(error).__name__, error)
        # result format: sample_id_a, function_id_a, sample_id_b, function_id_b, score
        matching_results: List[Tuple[int, int, int, int, float]] = []
        organized_matching_results: Dict[Tuple[int, int, int], Tuple[int, float]] = defaultdict(lambda: (-1, -1.0))
        packed_tuples = self._unrollGroupsAsPackedTuples(cache, candidate_groups)
        num_packed_tuples = self._countPackedTuples(candidate_groups)
        # progress reporter counts here in case calling function does not count
        single_batch = False
        if self._num_batches == 1:
            self._progress_reporter.set_total(num_packed_tuples)
            single_batch = True
        calculation_function = functools.partial(self._worker.minhasher.calculateScoresFromPackedTuples, ignore_threshold=True, minhash_threshold=self._minhash_threshold)
        packed_tuples = [p for p in packed_tuples]
        counted_scores = Counter()
        if self._worker._minhash_config.MINHASH_POOL_MATCHING:
            with create_process_pool() as pool:
                for pool_result in tqdm.tqdm(
                    pool.imap_unordered(calculation_function, packed_tuples),
                    total=num_packed_tuples,
                ):
                    # for key, new_value in pool_result.items():
                    for single_result in pool_result:
                        sample_id_a, function_id_a, sample_id_b, function_id_b, score = single_result
                        counted_scores[score] += 1
                        if score > self._worker.config.MINHASH_CONFIG.MINHASH_MATCHING_THRESHOLD:
                            key = (sample_id_a, function_id_a, sample_id_b)
                            new_value = (function_id_b, score)
                            original_value = organized_matching_results[key]
                            # tie-break on the smaller function_id_b so the reported match does
                            # not depend on worker arrival order (imap_unordered); this matches
                            # what the single-process path below produces by iterating candidate
                            # ids in ascending order
                            organized_matching_results[key] = max([original_value, new_value], key=lambda x: (x[1], -x[0]))
                    if single_batch:
                        self._progress_reporter.step()
        else:
            packed_tuple: List[Tuple[int, int, bytes, int, int, bytes]]
            for packed_tuple in tqdm.tqdm(packed_tuples, total=num_packed_tuples):
                pool_result = calculation_function(packed_tuple)
                for single_result in pool_result:
                    sample_id_a, function_id_a, sample_id_b, function_id_b, score = single_result
                    counted_scores[score] += 1
                    if score > self._worker.config.MINHASH_CONFIG.MINHASH_MATCHING_THRESHOLD:
                        key = (sample_id_a, function_id_a, sample_id_b)
                        new_value = (function_id_b, score)
                        original_value = organized_matching_results[key]
                        organized_matching_results[key] = max([original_value, new_value], key=lambda x: (x[1], -x[0]))
                if single_batch:
                    self._progress_reporter.step()
        full_score_counts = sorted([(item[0] * 64 / 100, item[1]) for item in dict(counted_scores).items()])
        LOGGER.info("Minhash Signature Field Match Counts: " + ", ".join([f"({i[0]}: {i[1]})" for i in full_score_counts]))
        matching_results: MinhashMatches = [(key[0], key[1], key[2], value[0], value[1]) for key, value in organized_matching_results.items()]
        return matching_results

    def _countPackedTuples(self, candidate_pairs) -> int:
        count = 0
        for candidate_ids in candidate_pairs.values():
            count += len(candidate_ids)
        quotient, remainder = divmod(count, self._worker.config.MINHASH_CONFIG.MINHASH_MATCHING_CANDIDATE_WORKPACK_SIZE)
        LOGGER.info("Processing a total of %d candidates.", count)
        return quotient + int(bool(remainder))  # always round up

    def _unrollGroupsAsPackedTuples(self, cache: Union["MatchingCache", "MemoryStorage"], candidate_pairs) -> Iterable[List[Tuple[int, int, bytes, int, int, bytes]]]:
        # Query, VS, Sample
        # All were identical
        packed_tuples: List[List[Tuple[int, int, bytes, int, int, bytes]]] = []
        current_pack: List[Tuple[int, int, bytes, int, int, bytes]] = []
        count = 0
        for function_id_a, candidate_ids in sorted(candidate_pairs.items()):
            for function_id_b in sorted(candidate_ids):
                if function_id_a == function_id_b:
                    continue
                minhash_a = cache.getMinHashByFunctionId(function_id_a)
                minhash_b = cache.getMinHashByFunctionId(function_id_b)
                sample_id_a = cache.getSampleIdByFunctionId(function_id_a)
                sample_id_b = cache.getSampleIdByFunctionId(function_id_b)
                # the candidate groups only contain function_ids that are present in the cache
                assert minhash_a is not None and minhash_b is not None and sample_id_a is not None and sample_id_b is not None
                current_pack.append((sample_id_a, function_id_a, minhash_a, sample_id_b, function_id_b, minhash_b))
                if count < self._worker.config.MINHASH_CONFIG.MINHASH_MATCHING_CANDIDATE_WORKPACK_SIZE:
                    count += 1
                else:
                    yield current_pack
                    current_pack = []
                    count = 0
        if current_pack:
            yield current_pack
        return packed_tuples

    def _harmonizePicHashMatches(self, pichash_matches: PichashMatches) -> HarmonizedMatches:
        # Query, VS, Sample
        # all use this version
        pichash_match_mapping: HarmonizedMatches = {}
        cached_function_id_to_sample_id: Dict[int, int] = {}
        # all_own_function_ids = {entry.function_id for entry in self._function_entries}
        sample_function_id_to_size = {entry.function_id: entry.num_instructions for entry in self._function_entries}
        for _, pichash_tuples in pichash_matches.items():
            current_own_function_ids: List[int] = []
            current_foreign_function_ids: List[int] = []
            function_size = 0
            for pichash_tuple in pichash_tuples:
                bh_family_id, bh_sample_id, bh_function_id = pichash_tuple
                cached_function_id_to_sample_id[bh_function_id] = bh_sample_id
                if bh_function_id in sample_function_id_to_size:
                    current_own_function_ids.append(bh_function_id)
                    function_size = sample_function_id_to_size[bh_function_id]
                else:
                    current_foreign_function_ids.append(bh_function_id)
            if function_size < self._pichash_size:
                continue
            # eval matches for this set of tuples
            if len(pichash_tuples) > 1:
                for own_function_id in current_own_function_ids:
                    for foreign_function_id in current_foreign_function_ids:
                        foreign_sample_id = cached_function_id_to_sample_id[foreign_function_id]
                        foreign_tuple = (foreign_sample_id, foreign_function_id)
                        pichash_match_mapping[(own_function_id, *foreign_tuple)] = (100, True, False)
                    # include self matches
                    for own_function_id_2 in current_own_function_ids:
                        if own_function_id == own_function_id_2:
                            continue
                        foreign_sample_id = cached_function_id_to_sample_id[own_function_id_2]
                        foreign_tuple = (foreign_sample_id, own_function_id_2)
                        pichash_match_mapping[(own_function_id, *foreign_tuple)] = (100, True, False)
        return pichash_match_mapping

    def _harmonizeMinHashMatches(self, sample_id, minhash_matches: MinhashMatches) -> HarmonizedMatches:
        # Query, VS, Sample
        # all use this version
        # handle minhashes
        minhash_mapping: HarmonizedMatches = {}

        for match in minhash_matches:
            sample_id_a, function_id_a, sample_id_b, function_id_b, minhash_score = match
            if sample_id_a == sample_id:
                own_function_id = function_id_a
                foreign_function_id = function_id_b
                foreign_sample_id = sample_id_b
                minhash_mapping[(own_function_id, foreign_sample_id, foreign_function_id)] = (
                    minhash_score,
                    False,
                    True,
                )
            if sample_id_b == sample_id:
                own_function_id = function_id_b
                foreign_function_id = function_id_a
                foreign_sample_id = sample_id_a
                minhash_mapping[(own_function_id, foreign_sample_id, foreign_function_id)] = (
                    minhash_score,
                    False,
                    True,
                )

        return minhash_mapping

    def _getCorpusPicHashMatchesForFunctionEntries(self) -> Dict[int, Set[Tuple[int, int, int]]]:
        """PicHash matches of functions that are not in the corpus themselves (query input), from one lookup.

        Each pichash some corpus function holds maps to its holders plus the query functions with
        it; this used to take one query per distinct pichash, uncapped by MINHASH_PICHASH_MAX_MATCHES (#69).
        """
        own_functions_by_pichash: Dict[int, List[Tuple[int, int, int]]] = {}
        for function_entry in self._function_entries:
            if function_entry.pichash:
                own_functions_by_pichash.setdefault(function_entry.pichash, []).append((function_entry.family_id, function_entry.sample_id, function_entry.function_id))
        pichash_matches = self._storage.getMatchesForPicHashes(list(own_functions_by_pichash))
        for pichash, holders in pichash_matches.items():
            holders.update(own_functions_by_pichash[pichash])
        return pichash_matches

    # summarizing, formatting starts here:

    def _summarizeMatches(self, sample_id, matches: HarmonizedMatches, aggregation_only: bool) -> Tuple[List, Dict, Dict, float]:
        # Query, VS, Sample
        # all use this version
        sample_fid_to_binweight = {entry.function_id: entry.binweight for entry in self._function_entries}
        # prefetch every foreign sample entry in one $in instead of two find_ones per
        # distinct sample inside the loop below (N+1, #111). The lazy per-id lookups stay
        # as the fallback for anything the batch did not return.
        missing_sample_ids = {match_ids[1] for match_ids in matches if match_ids[1] != sample_id and match_ids[1] not in self._sample_id_to_entry}
        if missing_sample_ids and hasattr(self._storage, "getSampleEntriesByIds"):
            for foreign_sample_id, entry in self._storage.getSampleEntriesByIds(list(missing_sample_ids)).items():
                self._sample_id_to_entry.setdefault(foreign_sample_id, entry)
                # same predicate as getLibraryInfoForSampleId(...) is not None
                self._sample_to_lib_info.setdefault(foreign_sample_id, bool(entry.is_library))
        aggregation = {
            "num_own_functions_matched": 0,
            "num_foreign_functions_matched": 0,
            "num_own_functions_matched_as_library": 0,
            "num_self_matches": 0,
            "bytes_matched": 0.0,
        }
        # handle minhashes
        match_function_mapping: Dict[int, Dict] = {}
        # from here on structure the stuff in the same way as pichash result
        self_matched_functions: Set[int] = set()
        own_functions_with_matches: Set[int] = set()
        own_functions_with_library_matches: Set[int] = set()
        foreign_functions_matched: Set[int] = set()

        # create entries for all functions, to later also show unmatched functions and calculate statistics based on function sizes
        for function_entry in self._function_entries:
            if function_entry.function_id not in match_function_mapping:
                match_function_mapping[function_entry.function_id] = {
                    "num_bytes": function_entry.binweight,
                    "num_instructions": function_entry.num_instructions,
                    "offset": function_entry.offset,
                    "matches": [],
                }

        for match_ids, match_strength in matches.items():
            own_function_id, foreign_sample_id, foreign_function_id = match_ids
            minhash_score, is_pichash_match, is_minhash_match = match_strength

            if foreign_sample_id == sample_id:
                self_matched_functions.add(own_function_id)
                self_matched_functions.add(foreign_function_id)
            else:
                if foreign_sample_id not in self._sample_to_lib_info:
                    self._sample_to_lib_info[foreign_sample_id] = self._storage.getLibraryInfoForSampleId(foreign_sample_id) is not None
                has_libinfo = self._sample_to_lib_info[foreign_sample_id]
                foreign_functions_matched.add(foreign_function_id)
                own_functions_with_matches.add(own_function_id)
                if has_libinfo:
                    own_functions_with_library_matches.add(own_function_id)
                if aggregation_only:
                    # the totals are all that is asked for; building, sorting and keeping a
                    # tuple per match is what grows with the number of matches (#69)
                    continue

                if foreign_sample_id not in self._sample_id_to_entry:
                    foreign_sample_entry = self._storage.getSampleById(foreign_sample_id)
                    assert foreign_sample_entry is not None
                    self._sample_id_to_entry[foreign_sample_id] = foreign_sample_entry
                foreign_family_id = self._sample_id_to_entry[foreign_sample_id].family_id

                flags = is_pichash_match * MatcherFlags.IS_PICHASH_FLAG + is_minhash_match * MatcherFlags.IS_MINHASH_FLAG + has_libinfo * MatcherFlags.IS_LIBRARY_FLAG
                match_function_mapping[own_function_id]["matches"].append(
                    (
                        foreign_family_id,
                        foreign_sample_id,
                        foreign_function_id,
                        minhash_score,
                        flags,
                    )
                )

        # Anti-Value-Key post processing
        matches_function_list = []
        if not aggregation_only:
            for function_id, dict in match_function_mapping.items():
                dict["fid"] = function_id
                # this creates additional stability for tests and processing
                dict["matches"] = sorted(dict["matches"])
                matches_function_list.append(dict)

        aggregation["num_self_matches"] = len(self_matched_functions)
        aggregation["num_own_functions_matched"] = len(own_functions_with_matches)
        aggregation["bytes_matched"] = sum([sample_fid_to_binweight[own_function_id] for own_function_id in own_functions_with_matches])
        aggregation["num_foreign_functions_matched"] = len(foreign_functions_matched)
        aggregation["num_own_functions_matched_as_library"] = len(own_functions_with_library_matches)

        num_library_match_bytes = sum([sample_fid_to_binweight[function_id] for function_id in own_functions_with_library_matches])

        return matches_function_list, match_function_mapping, aggregation, num_library_match_bytes

    def _isMatchableFunction(self, function_entry) -> bool:
        """Whether this request can match the function at all, i.e. whether it belongs in the
        denominator of the percentages: by PicHash when it reaches the request's pichash_size,
        or by MinHash when the minhasher produces a signature for it (#156).

        Both thresholds are the ones the numerator uses - pichash_size is per request, the
        MinHash limits are the configured ones - so a request with a small pichash_size
        widens the denominator along with the matches instead of exceeding 100 %.
        """
        if function_entry.num_instructions >= self._pichash_size:
            return True
        minhash_config = self._worker._minhash_config
        if minhash_config.MINHASH_FN_MIN_INS and function_entry.num_instructions > minhash_config.MINHASH_FN_MIN_INS:
            return True
        if minhash_config.MINHASH_FN_MIN_BLOCKS and function_entry.num_blocks > minhash_config.MINHASH_FN_MIN_BLOCKS:
            return True
        return False

    def _get_family_adjustment(self, match_report) -> Dict[int, float]:
        adjustments: Dict[int, float] = {}
        for fid, function_data in match_report.items():
            families = {match[0] for match in function_data["matches"]}
            family_adjustment_value = 1 if len(families) < 3 else 1 + int(math.log(len(families), 2))
            adjustments[fid] = family_adjustment_value
        return adjustments

    def _aggregateMatchSampleSummary(self, match_report, own_sample_info, num_library_bytes):
        # calculate number of bytes that are actuall matchable (i.e. functions whose size exceeds threshold)
        own_sample_num_matchable_bytes = 0
        if self.matcher_type in ["MatcherQueryFunction", "MatcherQuery"]:
            # rely on dynamically created function entries
            own_sample_function_entries = self._function_entries
        else:
            own_sample_function_entries = self._storage.getFunctionsBySampleId(own_sample_info["sample_id"])
        for function_entry in own_sample_function_entries:
            if self._isMatchableFunction(function_entry):
                own_sample_num_matchable_bytes += function_entry.binweight
        own_sample_num_nonlibrary_bytes = own_sample_num_matchable_bytes - num_library_bytes
        # aggregate sample byte sizes
        function_num_bytes = {}
        for own_function_id, function_data in match_report.items():
            function_num_bytes[own_function_id] = function_data["num_bytes"]
        # summarize samples
        matches_per_sample = self._aggregateMatchesPerSample(match_report)
        sample_summary: Dict[int, Dict[str, Any]] = {}

        family_adjustment = self._get_family_adjustment(match_report)

        for foreign_sample_id in matches_per_sample:
            if foreign_sample_id not in self._sample_id_to_entry:
                foreign_sample_entry = self._storage.getSampleById(foreign_sample_id)
                assert foreign_sample_entry is not None
                self._sample_id_to_entry[foreign_sample_id] = foreign_sample_entry
            sample_info = self._sample_id_to_entry[foreign_sample_id]
            sample_summary[foreign_sample_id] = {
                "family": sample_info.family,
                "family_id": sample_info.family_id,
                "version": sample_info.version,
                "bitness": sample_info.bitness,
                "sha256": sample_info.sha256,
                "filename": sample_info.filename,
                "sample_id": foreign_sample_id,
                "num_bytes": sample_info.binweight,
                "is_library": sample_info.is_library,
                "num_functions": sample_info.statistics["num_functions"],
                "matched": {
                    "functions": {
                        "minhashes": 0,
                        "pichashes": 0,
                        "combined": 0,
                        "library": 0,
                    },
                    "bytes": {
                        "unweighted": 0,
                        "score_weighted": 0,
                        "frequency_weighted": 0,
                        "nonlib_unweighted": 0,
                        "nonlib_score_weighted": 0,
                        "nonlib_frequency_weighted": 0,
                    },
                    "percent": {
                        "unweighted": 0,
                        "score_weighted": 0,
                        "frequency_weighted": 0,
                        "nonlib_unweighted": 0,
                        "nonlib_score_weighted": 0,
                        "nonlib_frequency_weighted": 0,
                    },
                },
            }
            current_matched = sample_summary[foreign_sample_id]["matched"]
            for own_function_id, matches in matches_per_sample[foreign_sample_id].items():
                current_matched["functions"]["combined"] += 1
                has_library_match = "library" in [match[0] for match in matches]
                current_matched["functions"]["library"] += 1 if has_library_match else 0
                current_matched["functions"]["minhashes"] += 1 if "minhash" in [match[0] for match in matches] else 0
                current_matched["functions"]["pichashes"] += 1 if "pichash" in [match[0] for match in matches] else 0

                match_max = max([match[1] for match in matches])
                unweighted_inc = function_num_bytes[own_function_id]
                score_weighted_inc = 1.0 * unweighted_inc * match_max / 100.0
                frequency_weighted_inc = score_weighted_inc / family_adjustment[own_function_id]

                current_matched["bytes"]["unweighted"] += unweighted_inc
                current_matched["bytes"]["score_weighted"] += score_weighted_inc
                current_matched["bytes"]["frequency_weighted"] += frequency_weighted_inc
                if not has_library_match:
                    current_matched["bytes"]["nonlib_unweighted"] += unweighted_inc
                    current_matched["bytes"]["nonlib_score_weighted"] += score_weighted_inc
                    current_matched["bytes"]["nonlib_frequency_weighted"] += frequency_weighted_inc

            for kind in "unweighted", "score_weighted", "frequency_weighted":
                current_matched["percent"][kind] = 100.0 * current_matched["bytes"][kind] / own_sample_num_matchable_bytes if own_sample_num_matchable_bytes > 0 else 0.0
                current_matched["percent"]["nonlib_" + kind] = (
                    100.0 * current_matched["bytes"]["nonlib_" + kind] / own_sample_num_nonlibrary_bytes if own_sample_num_nonlibrary_bytes > 0 else 0.0
                )

        return list(sample_summary.values())

    def _aggregateMatchesPerSample(self, match_report):
        matches_per_sample: Dict[int, Dict[int, List[Tuple[str, float]]]] = defaultdict(lambda: defaultdict(list))
        for own_function_id, function_data in match_report.items():
            function_has_libinfo = any([bool(match[-1] & MatcherFlags.IS_LIBRARY_FLAG) for match in function_data["matches"]])
            for match in function_data["matches"]:
                (
                    foreign_family_id,
                    foreign_sample_id,
                    foreign_function_id,
                    minhash_score,
                    flags,
                ) = match
                is_pichash_match = flags & MatcherFlags.IS_PICHASH_FLAG
                is_minhash_match = flags & MatcherFlags.IS_MINHASH_FLAG
                match_types = []
                if is_pichash_match:
                    match_types.append("pichash")
                if is_minhash_match:
                    match_types.append("minhash")
                for match_type in match_types:
                    matches_per_sample[foreign_sample_id][own_function_id].append((match_type, minhash_score))
                if function_has_libinfo:
                    matches_per_sample[foreign_sample_id][own_function_id].append(("library", 0))
        return matches_per_sample

    def _craftResultDict(self, pichash_matches: HarmonizedMatches, minhash_matches: HarmonizedMatches, num_matches=None) -> Dict:
        # Query, VS, Sample
        # All use this version
        if self._worker._minhash_config.PICHASH_IMPLIES_MINHASH_MATCH:
            for key, new_entry in pichash_matches.items():
                if key not in minhash_matches:
                    minhash_matches[key] = (100.0, False, True)
        all_matches = minhash_matches.copy()
        for key, new_entry in pichash_matches.items():
            if key in all_matches:
                old_entry = all_matches[key]
                # update values: score, is_pic, is_min
                all_matches[key] = (max(old_entry[0], new_entry[0]), old_entry[1] or new_entry[1], old_entry[2] or new_entry[2])
            else:
                all_matches[key] = new_entry

        _, _, pichash_aggregation, _ = self._summarizeMatches(self._sample_id, pichash_matches, True)
        # Assume that every pichash is also a min hash:
        _, _, minhash_aggregation, _ = self._summarizeMatches(self._sample_id, minhash_matches, True)

        (all_functions_summary, all_functions_list, all_aggregation, num_library_bytes) = self._summarizeMatches(self._sample_id, all_matches, False)
        sample_summary = self._aggregateMatchSampleSummary(all_functions_list, self._sample_info, num_library_bytes)
        summary = {
            "info": {
                "job": None,
                "sample": self._sample_info,
                "type": "",
            },
            "matches": {
                "aggregation": {
                    # assume all pichashes are also minhashes
                    "minhash": minhash_aggregation,
                    "pichash": pichash_aggregation,
                },
                "functions": all_functions_summary,
                "samples": sample_summary,
            },
        }
        return summary
