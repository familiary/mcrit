import logging
import os
from dataclasses import dataclass
from typing import Dict, List, Optional

from mcrit.config.BandPresets import BAND_PRESETS
from mcrit.config.ConfigInterface import ConfigInterface, default_field
from mcrit.storage.StorageFactory import StorageFactory

LOGGER = logging.getLogger(__name__)

# measured resident cost per retained MatchingCache function (minhash bytes + dict/index
# overhead), used to convert STORAGE_MATCHING_CACHE_MAX_BYTES into an entry ceiling
MATCHING_CACHE_BYTES_PER_ENTRY = 314


@dataclass
class StorageConfig(ConfigInterface):
    # storage configuration, use "memory" for local testing or "mongodb" when working with larger data
    # STORAGE_METHOD = StorageFactory.STORAGE_METHOD_MEMORY
    STORAGE_METHOD: str = StorageFactory.STORAGE_METHOD_MONGODB
    # Use this as endpoint for our server
    STORAGE_SERVER: str = "127.0.0.1"
    STORAGE_PORT: str = "27017"
    # By default, MongoDbStorage's DB's name and MongoQueue's DB's name are both "mcrit"
    # Changing one DB name here or at runtime DOES NOT change the other name!
    STORAGE_MONGODB_DBNAME: str = "mcrit"
    STORAGE_MONGODB_USERNAME: Optional[str] = None
    STORAGE_MONGODB_PASSWORD: Optional[str] = None
    STORAGE_MONGODB_FLAGS: str = ""
    # Enable periodic deletion of queried samples and their results after a given time
    STORAGE_MONGODB_ENABLE_CLEANUP: bool = False
    STORAGE_MONGODB_CLEANUP_DELTA: int = 60 * 60 * 24 * 7
    STORAGE_MONGODB_CLEANUP_TTL: int = 60 * 60 * 24 * 7
    # Once MinHashes have been calculated, discard disassembly from function entries
    STORAGE_DROP_DISASSEMBLY: bool = False
    # Keep the raw binary of every sample submitted through /samples/binary; off by default,
    # since it stores every submitted file in full (#95)
    STORAGE_KEEP_SUBMITTED_BINARIES: bool = False
    # Serve the kept binaries over the REST API at /samples/{id}/binary. A separate decision from
    # keeping them: it hands malware out to anyone who can reach the API, so it is off by default
    # and the route answers 403 until it is turned on (#95)
    STORAGE_SERVE_SUBMITTED_BINARIES: bool = False
    # supported strategies:
    #  * random: randomly sample from minhash fields, possibly more fuzziness likely won't use all minhash fields
    #  * linear: use a sequential selection of minhash fields, requires size*number=MINHASH_SIGNATURE_LENGTH
    #  * explicit: state each band's signature offsets directly, via STORAGE_BAND_PROJECTION or
    #    STORAGE_BAND_PRESET. This subsumes both strategies above and allows bands of differing
    #    size, overlapping bands, and bands confined to one shingler's segment of the signature.
    STORAGE_BAND_STRATEGY: str = "random"
    # random seed to be used when deriving sequences used as bands
    # NOTE: only "random" consults this. Under "linear" and "explicit" the seed has no effect,
    # so changing it there is silently a no-op.
    STORAGE_BAND_SEED: int = 0xDEADBEEF
    # For STORAGE_BAND_STRATEGY = "explicit": either name a preset from BAND_PRESETS, or give the
    # projection outright as a list of lists of signature offsets. STORAGE_BAND_PRESET wins if both
    # are set. Under "explicit", STORAGE_BANDS is ignored and STORAGE_NUM_BANDS derives from the
    # projection.
    STORAGE_BAND_PRESET: str = ""
    STORAGE_BAND_PROJECTION: List[List[int]] = default_field([])
    # Banding supports:
    #  * MemoryStorage: arbitrary banding configuration, multiple lengths
    #  * MongoDbStorage: arbitrary banding configuration, multiple lengths
    # configuration for bands, dict with size:number as structure - we allow mixed sizes to increase scatter effect and randomness
    _default_storage_bands = {4: 20}
    STORAGE_BANDS: Dict[int, int] = default_field(_default_storage_bands)
    # use a hashmap to cache all banding data - very memory intensive, but great speedups.
    STORAGE_CACHE: bool = False
    # Reuse the MatchingCache across the batches of a single matching job instead of
    # rebuilding it per batch. Measured redundancy of the per-batch rebuild on real data:
    # 1.54x (citadel) to 12.21x (merlin). Costs ~314 B resident per retained function.
    STORAGE_MATCHING_CACHE_PERSIST: bool = True
    # Memory budget for retained functions, in bytes (converted at ~314 B resident per
    # retained function, measured). Least-recently-needed entries are evicted first, never
    # those required by the batch currently being served, and every eviction is logged.
    # 0 disables the byte budget.
    STORAGE_MATCHING_CACHE_MAX_BYTES: int = 512 * 1024 * 1024
    # Ceiling on retained functions as an entry count. 0 (the default) derives it from
    # STORAGE_MATCHING_CACHE_MAX_BYTES at config load and logs the resolved value; an
    # explicit value here overrides the byte budget. Setting both this and
    # STORAGE_MATCHING_CACHE_MAX_BYTES to 0 disables the ceiling entirely.
    STORAGE_MATCHING_CACHE_MAX_ENTRIES: int = 0
    # How getCandidatesForMinHashes accumulates band hits:
    #  * "numpy": per-query-function int64 hit arrays + np.unique(return_counts) (default)
    #  * "dict":  dict[query_fid][candidate_fid] -> count  (legacy fallback, deprecated)
    # Same results either way; "numpy" avoids the ~100 B/pair Python dict and the O(pairs) loop.
    STORAGE_CANDIDATE_ACCUMULATION: str = "numpy"
    # Fetch candidate signatures for the MatchingCache with this many concurrent queries.
    # The call is latency-bound (186 B returned per ~4 kB document read), so concurrency
    # overlaps mongod's disk reads: measured 6.81x warm / 15.3x cold at 8 threads on 500k ids,
    # byte-identical results. 1 keeps the sequential behaviour. 0 (the default) derives
    # min(4, max(1, cpu_count // 2)) at config load and logs the resolved value - a
    # conservative half-step that captures most of the win (2.16x at 2 threads, 3.36x at 4
    # on the measured fetch) without turning a shared mongod into a contention point.
    # With N concurrent jobs sharing one mongod, keep N x threads <= pymongo's maxPoolSize
    # (default 100).
    STORAGE_CACHE_FETCH_THREADS: int = 0
    # function_ids per $in query. Must stay well under Mongo's 16 MB command limit; smaller
    # slices also give the thread pool something to overlap.
    STORAGE_CACHE_FETCH_SLICE_SIZE: int = 500000
    # Skip band hashes whose posting list is longer than this when generating candidates.
    # 0 (the default) keeps every posting list, i.e. the behaviour this knob was added to.
    #
    # A band hash held by a large fraction of the corpus says almost nothing about *which*
    # samples resemble the query - it is the binary-similarity equivalent of a stopword, and
    # it is also exactly the posting list that is expensive to read and turns into candidate
    # pairs. Measured on 257 real Malpedia samples: band posting lists are p50=1, p99=42, but
    # max=3598, and that tail grows with the corpus while the median does not. Capping it
    # bounds candidate volume by (query functions x bands x cutoff) instead of by corpus size.
    #
    # Measured starting point: 200. At 12,500 samples that is where the traversal stops scaling
    # with the corpus while top-10 and top-25 sample recall against the uncapped result are still
    # 1.000 (median 1.172 s at cutoff 1000 -> 0.374 s at 200; tightening to 100 buys little more).
    #
    # This is a recall/latency trade: a match findable *only* through a band hash that common
    # is no longer found by the fuzzy path. It is not a silent one - PicHash matching is
    # unaffected, and benchmarks/compare_quality.py measures what a given cutoff costs against
    # the uncapped result on a real corpus. See docs/scaling/ for measured numbers.
    STORAGE_BAND_DF_CUTOFF: int = 0
    # Cap on how many function ids one band document may hold, splitting a posting list across
    # (band_hash, bucket) documents once it would exceed that. 0 keeps the single-document shape.
    #
    # This exists because MongoDB caps a document at 16 MB and a posting list is an array inside
    # one. Measured directly, a document holds about 1.35 million ids while they fit in 32 bits
    # and about 1.05 million once they need BSON int64. On a 7,244-sample real corpus the longest
    # posting list across all 20 bands held 36,183 ids, which puts the wall near 270,000 samples
    # if it grows linearly. Past it the $push does not slow down, it
    # fails ("BSONObj size ... is invalid"), and indexing stops for any sample holding a function
    # whose band hash is already at the cap. Sharding does not move this: a document cannot span
    # shards.
    #
    # 100,000 leaves a wide margin under the cap even if postings grow heavier than measured, and
    # keeps a single document small enough to be cheap to ship. It must stay comfortably above
    # STORAGE_BAND_DF_CUTOFF: the cutoff selects hashes by the total df stored on bucket 0, and
    # that stays exact only while an under-cutoff posting list still fits in one bucket, so
    # MongoDbStorage refuses to start with a cutoff above the bucket size.
    STORAGE_BAND_BUCKET_SIZE: int = 0
    # How many index keys one partition of an offline index rebuild reads. 0 (the default)
    # keeps the single-pass rebuild that groups over the whole collection server-side, i.e. the
    # behaviour this knob was added to. 500,000 is the measured recommendation.
    #
    # The pichash count rebuild used to be one `$group` over every pichash in the corpus. That
    # is a blocking stage whose accumulator holds one entry per *distinct* hash, so its memory
    # follows the corpus; past `internalDocumentSourceGroupMaxMemoryBytes` (100 MB by default)
    # it spills to disk and pays external merge I/O on top of the scan. Measured: k ~ +2.2 over
    # a 1.38x corpus increase, 211.9 s at 5,243 samples against 437.1 s at 7,244.
    #
    # The partitioned rebuild reads the same index in bounded slices and counts runs of equal
    # keys as it goes, so its memory is constant and its cost is one pass over the index no
    # matter how large the vocabulary grows. Results are identical - the rebuild verifies its
    # own total against an independent count and falls back to the grouped path if they
    # disagree - so there is no recall or accuracy trade here, only time and memory.
    #
    # 500,000 keys is about 40 MB of BSON in flight per partition and few enough partitions
    # that the per-partition round trip is noise. Lower it if the rebuild has to share a small
    # machine; raising it buys nothing once the round trip has stopped mattering.
    #
    # It defaults to off despite there being no measured trade, for the same reason as the other
    # knobs added by this work: a rebuild is the operation an operator reaches for when something
    # is already wrong, and changing what it does underneath an existing deployment is not a
    # change to make silently on upgrade.
    STORAGE_REBUILD_PARTITION_SIZE: int = 0
    # limit maximum export size to protect the system against running OOM, default: 1 GB
    STORAGE_MAX_EXPORT_SIZE = 1024 * 1024 * 1024

    def __post_init__(self):
        super().__post_init__()
        # resolve derived defaults once, here, so every consumer sees a concrete value and
        # the resolved value is visible in the log - a silently-binding derived default is
        # indistinguishable from a fix that does not work
        if not self.STORAGE_CACHE_FETCH_THREADS:
            cpu_count = os.cpu_count() or 1
            self.STORAGE_CACHE_FETCH_THREADS = min(4, max(1, cpu_count // 2))
            LOGGER.info(
                "STORAGE_CACHE_FETCH_THREADS resolved to %d (derived from cpu_count=%d)",
                self.STORAGE_CACHE_FETCH_THREADS,
                cpu_count,
            )
        if not self.STORAGE_MATCHING_CACHE_MAX_ENTRIES and self.STORAGE_MATCHING_CACHE_MAX_BYTES:
            self.STORAGE_MATCHING_CACHE_MAX_ENTRIES = max(1, self.STORAGE_MATCHING_CACHE_MAX_BYTES // MATCHING_CACHE_BYTES_PER_ENTRY)
            LOGGER.info(
                "STORAGE_MATCHING_CACHE_MAX_ENTRIES resolved to %d (derived from STORAGE_MATCHING_CACHE_MAX_BYTES=%d at %d B/entry)",
                self.STORAGE_MATCHING_CACHE_MAX_ENTRIES,
                self.STORAGE_MATCHING_CACHE_MAX_BYTES,
                MATCHING_CACHE_BYTES_PER_ENTRY,
            )

    @property
    def STORAGE_NUM_BANDS(self):
        if getattr(self, "STORAGE_BAND_STRATEGY", "random") == "explicit":
            return len(self.getBandProjection())
        num_bands = 0
        if self.STORAGE_BANDS:
            num_bands = sum([value for value in self.STORAGE_BANDS.values()])
        return num_bands

    def getBandProjection(self) -> List[List[int]]:
        """Resolve the explicit projection: a named preset if given, else STORAGE_BAND_PROJECTION."""
        preset = getattr(self, "STORAGE_BAND_PRESET", "")
        if preset:
            if preset not in BAND_PRESETS:
                raise AttributeError("Unknown STORAGE_BAND_PRESET %r - available: %s" % (preset, ", ".join(sorted(BAND_PRESETS))))
            return [list(band) for band in BAND_PRESETS[preset]]
        return [list(band) for band in (getattr(self, "STORAGE_BAND_PROJECTION", None) or [])]
