# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) over **the REST API, the
configuration surface and the stored data shape**. Python object internals - the classes in
`mcrit.storage` and friends - may change in a minor release; where that is expected to break a
consumer, the entry says so.

Entries carry the measurement, the caveat and the failure mode, not just the change: a line that
says what moved without saying what it cost, what it cannot see, or how it would be noticed is
worth less than the space it takes. Add yours to `[Unreleased]` when the change merges, while the
reasoning is still at hand, rather than reconstructing it from the commit log at release time.

## [Unreleased]

### Fixed

- **A repeated request for a match report, cross compare or unique-blocks result is no longer
  answered with a job computed before an upgrade that changed such results.** A job's cache key
  held the method, its parameters and the hashes of uploaded files, so after an upgrade that
  changes what these reports hold, asking again returned the job the old code computed until
  someone passed `force_recalculation`, or deleted the job for unique blocks, whose routes do not
  take that flag. These jobs now record a results version
  (`mcrit.Worker.RESULTS_VERSION`, appended to the job descriptor) and are only reused within one.
  NOTE that after upgrading to this release, and after any later release that bumps the version,
  the first repeat of each request (the same method and parameters, or the same uploaded file for
  a query) is computed again; the old jobs stay listed. Jobs that add, change or delete data keep
  their cache as before ([#241]).

## [1.12.0] - 2026-09-25

### Added

- **`GET /jobs` and `GET /jobs/count` select jobs by `sample_ids` (with `method`) and by
  `job_ids`**, applied in the query before paging, and `McritClient.getQueueData` /
  `getQueueCount` pass them on. Each sample id becomes two anchored regexes on
  `payload.descriptor` that are literal to their end, so each bounds one range of the existing
  index: on a 60,000-job queue the jobs of 25 samples read 102-124 index keys in under 2 ms,
  where one regex with an alternation read all 60,000 documents in ~100 ms. NOTE that
  `sample_ids` matches the first positional argument only, answers 400 without `method`, and a
  selector that keeps no parseable id selects nothing, never everything ([#210]).
- **`POST /samples/ids` and `POST /families/ids`, with `McritClient.getSamplesByIds` and
  `getFamiliesByIds`, answer several entries in one request** - one `$in` query per collection
  instead of a round trip per id. All 66 samples of a corpus took 5.2 ms in one request against
  206.9 ms in 66, and 16 families 2.6 ms against 40.1 ms. The body is a comma-separated id list,
  as for `POST /functions`; unknown ids are left out, and an empty or malformed body answers 400.
  Family entries carry no sample lists ([#207]).

### Fixed

- **`McritClient` waited forever on a server that did not answer.** None of its requests passed
  a timeout, and requests has none by default, so a server that was down behind a firewall, or up
  but hung, blocked the caller for good: against a socket that accepts and never replies, a
  `getVersion()` was still waiting after 15 s and would have waited indefinitely. In MCRITweb that
  is a gunicorn request thread, which gunicorn's own `-t` does not reclaim under the `gthread`
  worker. Every request now passes `timeout=`, from a new `timeout` argument, also settable as
  `client.timeout`, that defaults to `(10, None)`: the connect is bounded at 10 s, and the read is
  left open, because `/import`, `/export` and `/status` on a large corpus answer only once their
  work is done. A caller that knows its bound sets one; MCRITweb, behind an NGINX that gives up
  after 300 s, should. A request that runs out raises `requests.exceptions.ConnectTimeout` or
  `ReadTimeout`, as a refused connection already raised `ConnectionError`. A test reads the
  client's source and fails for any request added without a timeout.

## [1.11.0] - 2026-09-25

### Added

- **Band posting lists can be split across documents**, behind `STORAGE_BAND_BUCKET_SIZE`, which
  defaults to `0` (off) and keeps the single-document shape byte for byte.

  A posting list is a `function_ids` array inside one document and MongoDB caps a document at
  16 MB. Measured directly by pushing ids into one document until the write is refused: it holds
  about **1.35 million ids** while they fit in 32 bits (12.2 bytes each) and about **1.05
  million** once they need BSON int64 (15.9 bytes each), after which `$push` raises `BSONObj
  size ... is invalid`. On a 7,244-sample real corpus the longest posting list across all 20
  bands held **36,183 ids** (in `band_14`), so extrapolating it linearly puts the wall near
  **270,000 samples**. The write **fails** rather than slowing down, so indexing stops for any sample holding a function
  whose band hash is already at the cap. **Sharding does not move this**: a document cannot span
  shards.

  Bucket 0 carries the bookkeeping for the whole hash - `df` as the total across every bucket,
  plus `tail`/`tail_n` for placement - and higher buckets carry only postings. That is what keeps
  the cutoff filter and its `(band_hash, df)` index unchanged: a hash under the cutoff is far
  below one bucket's worth so it never spills, and a hash that spilled has a `df` that rejects it.
  Buckets fill in order rather than by hashing the function id, so a short posting list stays in
  one document instead of being scattered across many.

  **Migration**: enabling the knob on an existing database requires running
  `rebuild_band_df_index` before the next write. Documents written earlier have no `bucket` field,
  so the upsert filter `{band_hash, bucket: 0}` would not match them and would insert a *second*
  document for the hash, splitting the posting list invisibly. The rebuild stamps `bucket: 0` and
  is what makes them addressable. Matching results are unchanged either way - the tests assert
  identical matches with bucketing on and off, against a corpus where the split is forced.

  Deleting a sample reaches every bucket of a hash, and keeps bucket 0 (the only holder of
  `df`/`tail`/`tail_n`) for as long as any other bucket of that hash still holds postings.
  `STORAGE_BAND_DF_CUTOFF` above `STORAGE_BAND_BUCKET_SIZE` is refused at startup, since only
  bucket 0 carries `df` and such a cutoff would serve a spilled hash as bucket 0 alone.

- **`STORAGE_REBUILD_PARTITION_SIZE`**, defaulting to `0` (off), which rebuilds the PicHash count
  index from a partitioned scan of the `_pichash` index instead of one server-side `$group`
  followed by an upsert per distinct hash. `500000` is the measured recommendation. The rebuild
  is offline and never touches query latency, but it was the last operation whose cost followed
  corpus size rather than request size.
  - The old rebuild held two structures shaped like the corpus: a `$group` accumulator with one
    entry per *distinct* hash, which crosses MongoDB's 100 MB limit and spills (4 spills, 36.7 MB
    at 7,244 samples), and an upsert per hash arriving in group order rather than key order, so
    each landed at a random position in a growing index. A covered index scan already arrives
    sorted, which the old code discarded; counting runs of equal keys makes the intermediate
    state two local variables, and makes the writes ascending inserts.
  - Measured on corpora projected from a 7,244-sample real corpus, three repeats, medians:
    **51.9 s -> 10.8 s** at 1,000 samples and **301.6 s -> 73.1 s** at 7,244 (4.81x to 4.12x).
    At the largest size the old rebuild spends 32.6 s reading and 269.0 s writing - 8,690
    upserts/s against 38,765 inserts/s.
  - **Result-preserving**, and verified rather than assumed: the rebuild checks the holders it
    counted against an independent count of the functions carrying a pichash and falls back to
    the old implementation if they disagree. This matters because keyset paging brackets by BSON
    type, so a pichash that was not a string would silently truncate the index - and a missing
    count document is *excluded* by the cutoff filter, i.e. exact matches would quietly stop
    being found. The tests compare the full `_pichash -> df` map from both implementations.
  - **Caveat on the scaling claim**: the measured corpora are reduced to the one field the
    rebuild reads, so they stay inside the WiredTiger cache and both implementations measured
    *linear* there - the superlinear exponent (k ~ +2.2) seen earlier on full-fidelity corpora
    did not reproduce. What is demonstrated is a 4.1x constant factor and a memory shape
    independent of the corpus, not a repaired exponent. `rebuildPicBlockHashIndex` and the band
    bookkeeping rebuild share the `$group` shape and are unchanged and unmeasured.

- `docs/scaling/` - the architecture before and after, the comparison of indexing approaches
  considered and why most of the field is eliminated before latency is even discussed (MCRIT
  compares MinHash signatures field-for-field and estimates Jaccard; a cosine/L2 ANN index
  answers a different question), the full research log, and the measured results.
- `benchmarks/` - the harness behind every number above: Malpedia fetch, SMDA report cache,
  per-stage 1-vs-N timing, corpus-structure analysis, Heaps' law fit, synthetic corpus growth
  fitted to a real corpus, quality comparison, and a scaling sweep.

### Changed

- The **matching-cache fetch decodes one MinHash per distinct signature**, not one per candidate
  function, and every function carrying a signature shares that one decoded object. Exact, not
  approximate: the decode is a pure function of the stored hex string. Candidate sets repeat
  signatures far more than the corpus does, because they are assembled by band collision -
  measured **3.99x to 29.59x** on the candidate sets of three query samples against a 7,244-sample
  real corpus, where the corpus-wide figure is 2.46x. The fetch logs its own factor. In isolation
  this is 7%-50% off the fetch and 0%-24% off its allocation, growing with the candidate set;
  **end to end it is not measurable** at this corpus size (fetch stage 10.193 s -> 10.487 s with
  the two-stage knobs off, 0.269 s -> 0.270 s with them on, summed over three queries, three
  repeats - a run-to-run spread several times larger than the effect), because the stage is
  dominated by per-function cache-object construction that this does not touch. It is worth having
  as a reduction in work proportional to the candidate set, which is what grows with the corpus,
  and not as a speed-up anybody will notice today. The fetch still *reads* one document per
  candidate function: each carries per-function attribution (`sample_id`), and reading fewer would
  need a signature-keyed index, i.e. a schema change. Match reports are unchanged, asserted by a
  test that replays a query with the deduplication defeated and compares the whole report.

## [1.10.0] - 2026-09-25

### Added

- Pushing a `vX.Y.Z` tag now publishes the release. The workflow refuses to continue unless the tag
  matches `pyproject.toml` and `McritConfig.VERSION`, `CHANGELOG.md` has a section for it, the commit
  is on `main` and CI passed there; it then builds the sdist and wheel in an isolated environment,
  installs the wheel into a clean environment to import it and run `mcrit --help`, uploads to PyPI
  through trusted publishing with signed provenance, and creates the GitHub release from that
  version's changelog section with the generated contributor list appended. Pre-release tags
  (`v1.10.0rc1`) are marked as such, and a manual run rehearses the same path against TestPyPI.
  Before, publishing was `make publish` with an API token, GitHub releases stopped at v1.3.0, and
  nothing checked that the three version strings agreed. See `RELEASING.md`; the trusted publisher
  and the `pypi` and `testpypi` environments are configured once by a maintainer.
- A pull request that changes `mcrit/` or `pyproject.toml` has to add a `CHANGELOG.md` entry or
  carry the `no-changelog` label; CI checks it.

### Changed

- Pairwise scoring now compares each **distinct** MinHash signature once rather than once per
  function holding it. This is exact, not approximate: a score depends only on the two
  signatures, so functions sharing one score identically against any query. Worth 2.46x on 257
  real Malpedia samples (185,387 hashed functions over 75,323 distinct signatures) and a
  projected ~24x at a million samples from the fitted Heaps' law V(n) = 1412.8 * n^0.7247. Peak
  matcher memory falls with the matrix by the same factor. Verified against the existing
  golden-result suites, which pass unchanged.

- `getSampleFunctionCounts` takes the sample ids to answer for. The shortlist ranking needs a
  function count per *candidate*, and asked for every sample in the corpus - once per matching
  job. At a few thousand samples that map is free, which is why four benchmark points across
  3.59x of corpus growth show no trace of it; at 10^9 samples it is a 10^9-entry dict per query.
  It is now an indexed lookup of the samples that received a vote (a few thousand at most).
  Callers passing nothing still get the whole-corpus map, so no consumer breaks. **Ranking
  behaviour is unchanged.**

### Removed

- **Python 3.11 is no longer supported**; `requires-python` is `>=3.12`. Nothing in MCRIT needed
  3.12 - the MCRIT ecosystem now shares a 3.12 floor so one interpreter serves every component. The
  reference `docker-mcrit` deployment already runs 3.12.
- **Two-stage 1-vs-N matching**, behind two knobs that both default to `0` (off), so an upgraded
  instance is bit-identical until it opts in. Every stage of a 1-vs-N query grew with corpus
  size, and so did the answer - a query whose result names 5,930 matched samples is not an
  answer anybody reads, and bounding the answer is the only thing that bounds the work.
  - `MINHASH_MATCHING_SHORTLIST_SIZE` ranks candidate samples cheaply (one vote per distinct
    query function, plus weighted PicHash evidence, ranked by vote count *and* by coverage
    because MCRIT scores a matched sample by the percentage of it that matched) and runs the
    existing exact matching against only the best N.
  - `STORAGE_BAND_DF_CUTOFF` skips band hashes whose posting list is longer than the cutoff. A
    band hash held by much of the corpus is a stopword: expensive to read, uninformative about
    *which* samples match.
  - Measured over a **48.6x** growth in corpus size (257 -> 12,500 samples), fixed query set,
    warm cache, repeated runs, at shortlist 100 / cutoff 200: one-stage median went
    0.429 s -> 4.427 s (latency ~ corpus^0.60) while **two-stage went 0.645 s -> 0.374 s**, with
    mean -4% and max +7% - no measurable growth. Extrapolated to a million samples: ~62 s
    against ~0.4 s.
  - **NOTE that unlike the tuning knobs, these two are not result-preserving.** Matching *within*
    a shortlisted sample is unchanged - same candidates, same scores - and top-10 and top-25
    sample recall against the unrestricted result measured 1.000 at every corpus size tested,
    with 0.9936-1.000 of surviving function matches keeping a bit-identical score. What a
    shortlist costs is tail samples: overall sample recall at 12,500 samples was 0.67. PicHash
    matching is unaffected and stays exact. See `docs/TUNING.md`.
- `function_ranges` index and `GET /rebuild_function_range_index`, mapping a function id back to
  its sample without reading the function - the shortlist has to do that per candidate, which is
  the cost it exists to avoid. Stored as one span per contiguous id run, so it is exact whether
  or not a sample's ids happen to be dense (an import adding functions later, or concurrent
  writers interleaving counter reservations, makes them not be). Read only when a completeness
  flag vouches for it; until then matching falls back to the whole corpus.
- `df` on band documents plus a `(band_hash, df)` index, and `GET /rebuild_band_df_index`.
  Filtering the cutoff on `$size` instead was measured to save nothing worth having - mongod
  reads the document to measure it - at 12,500 samples, 1.172 s at cutoff 1000 against 0.374 s
  once df is indexed at cutoff 200.
### Fixed

- **`McritClient`'s error modes reach the three maintenance jobs.** `rebuildPicBlockHashIndex`,
  `repairMinHashes` and `recomputeFamilyStats` parsed their answer with `handle_response`
  directly instead of `self._handle`, so a client built with `raise_client_errors` or
  `raise_server_errors` still got `None` from them - a refused or failed job request that looked
  like one nothing had answered. They landed while the modes were being written, which is how
  they were missed. `testClientErrors` now fails on any method that parses outside the client's
  mode, not only on these three.
- **`LogBucket` raised `KeyError` for any value past its precomputed table**, which aborts the
  whole indexing job. The table covers `0..SHINGLER_LOGBUCKETS-1` (100,000 by default) and
  `FuzzyStatPairShingler` buckets `max_block_size`, `num_ins_C`, `num_ins_S` and `num_calls`
  through it without bounding any of them - only `stack_size` is clamped, at its own call site.
  A single basic block of 108,837 bytes in a real corpus was enough to make that corpus
  unindexable, and the failure gets *likelier* as corpora grow. Values outside the table are now
  clamped to its bounds. **No MinHash changes**: only inputs that previously raised behave
  differently, asserted across the whole table.
- **`Worker.updateMinHashes` raised `UnboundLocalError` when there was nothing left to hash.**
  `minhashes` was bound only inside the batch loop, so a run with an empty backlog failed exactly
  like a crash - and that is the normal state of a *resumed* index, which is where it was hit.
  The same statement also returned the size of the **last batch** rather than the total, silently
  under-reporting any run longer than one workpack (a 238,991-function backlog across 24 batches
  reported whatever the final batch held). Every caller reads it as a total, so it now
  accumulates. **This changes the number returned**, toward what `recalculateMinHashes`,
  `updateMinHashesForSample` and `/status` already meant by it.
- `getSampleFunctionCounts` summed nothing when a sample owned several non-contiguous function-id
  runs - it assigned each run's size in turn, keeping only the last. Such samples were
  undercounted, distorting their coverage ranking in the shortlist. Both the whole-corpus and the
  per-sample paths now sum.
- **Renaming a family to its own name deleted it on MongoDB**, while its samples and functions
  kept its id, and raised `KeyError` on MemoryStorage; for family 0, named `""`, it doubled the
  counters. `modifyFamily` merges into whatever family the new name resolves to, which here was
  the family itself. The rename now runs only when the name differs from the stored one -
  compared, not looked up, since names are not unique in storage - and the rest of the update
  still applies. MemoryStorage also failed an ordinary rename with `KeyError` whenever the
  renamed family's samples were not the last ones stored. NOTE that a same-name rename now writes
  nothing on MongoDB and so no longer advances `db_state` there ([#208]).
- **`PUT /samples/<id>` and `PUT /families/<id>` refused `""` and every one-character family
  name**, although their messages allow 0-64 characters, so a version or component could not be
  cleared once set and no sample could be moved into family 0, whose name is `""`. The patterns
  now accept what the messages describe, and end in `\Z` rather than `$`, which also matched
  before a trailing newline: `"ab\n"` as a family name and `"1.0\n"` as a version are now
  refused. Checked over 37,210 generated strings against the old patterns: nothing else changes.
  **Needs the same-name family rename fix ([#208])** - with `""` accepted, renaming family 0 to
  its own name would otherwise double its counters ([#209]).
- **A repeated request could be served by a queued or running force rematch** instead of the
  finished job whose result it could use, because the cache picked the newest job with the same
  descriptor whatever its state. Both queues now prefer a finished job, then the newest, and
  never reuse a failed or terminated one. NOTE that this changes which job answers: a pending
  forced rematch no longer shadows an earlier finished result, verified against a running
  instance ([mcritweb#47]).
- **Searches sorted by anything but the id had no index to be served from**, so MongoDB sorted
  every filtered document in memory. A compound `(field, id)` index now exists for every field
  MCRITweb sorts families, samples and functions by, and the tie-break follows the sort
  direction so one index serves both; `explain()` on a real database went from
  `SORT -> FETCH -> IXSCAN` to `LIMIT -> FETCH -> IXSCAN`. NOTE that the first start after
  upgrading builds these indexes - six of them on `functions` - which on a large corpus takes
  noticeable time before the server is ready (for scale: one instance holds 11.6M function
  documents and 2.38 GB of indexes). Also fixed: **paging stopped early whenever a page ended on
  id 0** (function 0, sample 0, the unknown family), as the cursor was tested for truthiness
  ([mcritweb#59]).
- **A function name search that found nothing examined every function document** - the
  reported ~30 s on larger databases - since an unanchored case-insensitive regex cannot bound an
  index. `findFunctionByString` now lists the distinct names over the `function_name` index,
  matches the term against them in Python and hands MongoDB an `$in` / `$nin`. On two million
  functions with 5,000 distinct names, a no-result search went 4.2 s -> 22 ms and a sorted
  search 1.5 s -> 62 ms. NOTE that a common term at the default sort got slower by tens of
  milliseconds (`main` 15 ms -> 78 ms), and above 10,000 distinct names the search keeps the
  regex, unbounded as before ([mcritweb#76]). Finding out that a corpus is past that cap is not
  free - on 11.6M functions with 314,144 distinct names the capped scan takes ~0.9 s - so each
  process remembers the over-cap verdict for an hour instead of rescanning on every search. Only
  that verdict is kept, never the names, so writes need not invalidate it.
- **A document over MongoDB's 16 MiB limit lost the whole sample behind a bare
  `ValueError("Database insert failed.")`** that named nothing - reported 4 times in 120k files,
  typically one giant function's `xcfg` blob. `_dbInsertMany` now recognises both shapes of the
  error (pymongo's `DocumentTooLarge` and the server's write error after an ordered insert) and
  logs the offending documents with their ids and byte sizes. An oversized `xcfg` / `query_xcfg`
  blob is dropped with a warning and the rest stored, so the sample survives; NOTE that the
  affected function then has no disassembly and so no MinHash. An oversized document in any other
  collection still fails, now naming it ([#42]).


## [1.9.0] - 2026-09-08

Correctness and operator-recovery release, plus a large `getUniqueBlocks` speedup. **Matching
results are unchanged at default configuration** - a reference digest over an 11.6M-function corpus
reproduced byte-for-byte against v1.8.1 (8,125,782 bytes of match payload, 5,523 sample matches) -
and the database shape is unchanged, so no migration is required. Two fixes below *can* change
reported numbers; see `Changed`.

### Added

- `explicit` band projection strategy, stating each band's signature offsets directly via
  `STORAGE_BAND_PROJECTION` or a name from `STORAGE_BAND_PRESET`, instead of deriving them from a
  seed (`random`) or a stride (`linear`). It allows what neither can express: bands of differing
  size, overlapping bands, and bands confined to one shingler's segment of the signature. That last
  is the point - `generate_segmented_sequence` puts metric fields at offsets 0-15 and block fields
  at 16-63, a band matches only if *all* its fields match, and the two segments move independently:
  recompilation shifts metrics while block structure survives, and the SMDA 4.4.5 escaper change
  moved block fields on ~20% of a real corpus while metrics stayed put. `random` remains the
  default, and `legacy-random-20x4` / `legacy-linear-16x4` reproduce the derived strategies exactly,
  so an instance can adopt `explicit` without reindexing - asserted in tests and verified on an
  11.6M-function corpus where a reference digest reproduced byte-for-byte against an index built
  under the seed-derived projection ([#147]).
- `getBandProjectionFingerprint`, identifying the projection an index was built under. Band keys are
  derived data exactly like minhashes, so changing the projection without rebuilding leaves an index
  that still resolves, still returns candidates, and is quietly wrong. NOTE that offset *order
  within a band* is significant - a band hash concatenates field values in projection order - so
  neither the presets nor the fingerprint normalise it ([#147]).
- Inverted `picblockhashes` collection answering `getUniqueBlocks`: **92 s -> 0.11 s for one sample
  and 107 s -> 2.9 s for five** on the reference corpus, reading one document per candidate hash
  instead of every function carrying block hashes (9,088,495 documents, 51,364,030 block entries,
  and a cost independent of how many samples were requested). It is read only when a completeness
  flag vouches for it, so an existing database keeps using the old scan - retained, not deleted -
  until `rebuild_picblockhash_index` has run once: upgrading changes performance, never results
  ([#154]).
- `POST /repair_minhashes`, rehashing only the samples an older SMDA escaper hashed, one sample at a
  time and with the index serving throughout, where `recalculateMinHashes` drops every band
  collection and rehashes the whole corpus. Each sample records which SMDA version escaped its
  minhashes, and `/status` reports `minhash_compatibility_threshold` and
  `num_samples_with_stale_minhashes` ([#142]).
- `POST /recompute_family_stats`, setting every family's counters from the collections, recreating
  family documents that samples reference but no document describes, and reporting every correction
  ([#151]).
- `GET /rebuild_picblockhash_index` ([#154]) and `purgeEmptyBandDocuments()` ([#149]), both for an
  operator to run once after upgrading.

### Changed

- **BREAKING for Python consumers:** `MatchedFunctionEntry.match_is_minhash`, `match_is_pichash` and
  `match_is_library` are now `bool` properties over the stored flag integer rather than the masked
  integer itself. Truthiness is unaffected - `if entry.match_is_pichash` behaves as before - but
  code comparing them to `2` or `4` must change. The wire format is unchanged ([#155]).
- The percentage denominator is sized by what the request can actually match. This differs from
  before **only** when a request passes a `pichash_size` other than the configured
  `MINHASH_FN_MIN_INS`; where the two agree, reported percentages are unchanged ([#156]).
- A function unique to one family is weighted by its best match rather than whichever match came
  last ([#157]).
- Stored results are served as the bytes they were stored with rather than parsed and immediately
  re-serialised, which was **0.66 s of a 1.03 s report open** on an 8 MB report. `compact=true`
  keeps the parsing path, since it edits the result ([#152]).
- `getUnhashedFunctions` selects in the database and skips functions that can never be hashed. Its
  work set was "minhash is empty", which on a real corpus included 4.16M functions below
  `MINHASH_FN_MIN_INS` whose disassembly it re-fetched on every invocation - which is what made a
  minhash repair impractical to run at all.
- `MatchedFunctionEntry` uses `__slots__`: 217 -> 145 bytes retained per matched pair, measured on a
  240k-pair report ([#44]).
- `rebuildMinhashBandIndex` drops every `band_*` collection present, not only
  `band_0..STORAGE_NUM_BANDS-1`; lowering the band count previously stranded the surplus
  collections, invisible to matching and consuming disk ([#147]).

### Removed

- The `_picblockhashes.offset` index is no longer created. No query can use it - the only reference
  to that field is a `$project`, which an index cannot serve - and it held 0.678 GB, **27% of the
  `functions` collection's index footprint**, plus a multikey write per block per function on every
  insert. Existing instances keep their copy until it is dropped explicitly; see `Upgrading`.

### Fixed

- `getMatchTuple()` corrupted the match flags on a served route, multiplying already-masked bits by
  their flag a second time: a pichash-only match round-tripped as `IS_LIBRARY` (2 -> 4) and a
  library match lost its flag entirely (4 -> 16) ([#155]).
- `MatchingResult.toDict()` emitted `matches.functions` as a dict keyed by function id where
  `fromDict` reads a list, so `fromDict(toDict())` raised `TypeError` ([#44]).
- A worker killed without unwinding - the OOM killer's way - stranded its job forever: `next()` only
  hands out unlocked jobs and nothing cleared the lock, and `get_cached_job_id()` then served that
  dead job's id to every identical resubmission, which waited on work that could never run. Workers
  now carry a heartbeat, a registered worker whose heartbeat is older than the queue timeout is
  treated as dead and its unfinished jobs are reclaimed with one attempt fewer, and the cached-job
  lookup only serves a job that is finished, waiting, or in flight on a live worker ([#150]).
- The per-family counters `/status` sums drifted from the collections they summarise: increments
  against a missing family were silently discarded, imports did not create the family document their
  sample references, `modifySample` used read-modify-write where concurrent relabels lose an update,
  `deleteSample` decremented by the sample's stored statistics rather than by what it removed, and
  family deletion was judged by a counter that may have drifted rather than by whether any sample
  still references the family ([#151]).
- `deleteSample` left an emptied band posting list behind, and its `upsert` on the pull path could
  *create* band documents - so a delete could grow the index ([#149]).
- A sample search for an unknown sha256 answers with no match instead of HTTP 500 ([#158]).
- `renderRule` tested the `wrap_string` *function* for truthiness where it meant the `wrap_at`
  parameter, so `wrap_at` was never read: YARA rules were always wrapped, always at a hardcoded 80
  columns, and the single-line branch was unreachable. MCRITweb had been asking for `wrap_at=40` and
  silently getting 80. `wrap_at` is now both the switch and the width - `0` keeps the hex on one
  line, `N` wraps at `N` - and its default moves from `0` to `80`, the width callers were already
  getting, so output changes only for callers that ask ([#186]).
- `STORAGE_BAND_STRATEGY` carried no type annotation, so it was not a dataclass field and could not
  be set through `StorageConfig(...)` like every neighbouring setting ([#147]).

### Upgrading

No database-shape change, so **no migration is required**. Five one-time operator actions are
available, none of them automatic and none required for correct serving:

| action | effect |
|---|---|
| `rebuild_picblockhash_index` | enables the fast `getUniqueBlocks`; until it runs, the previous full scan is used |
| `recompute_family_stats` | corrects counters that have already drifted |
| `repair_minhashes` | rehashes only the samples an older escaper hashed - **read the note below first** |
| `purgeEmptyBandDocuments()` | clears the band tombstones older deletions left |
| `db.functions.dropIndex("_picblockhashes.offset_1")` | reclaims the index above on an existing instance |

The new endpoints mean MCRITweb needs a matching release to *use* them; MCRITweb works unchanged
against this version either way.

**A corpus that upgrades into this version reports every one of its samples as having stale
minhashes, and that is usually wrong.** Staleness is decided by a per-sample `minhash_smda_version`
that did not exist before 1.9.0, and a sample without one counts as stale - so `/status` shows
`num_samples_with_stale_minhashes` equal to the whole corpus on the first look, whatever the true
state. Running `repair_minhashes` in response rehashes everything, which on a multi-million-function
corpus is hours of work and a long stretch of degraded matching for no gain.

Check first whether the minhashes actually are stale - `escaper_fingerprint` in `/status` against
what produced them, or a sample rehashed by hand and compared. If they are current, record that
instead of recomputing it: `setMinHashVersionForSamples(<running smda version>)` sets the field for
every sample in one update (storage-level; there is no route for it, since it asserts something only
an operator can know). If they genuinely are stale, `repair_minhashes` is the cheap way to fix them
and the reason it exists.

## Older releases

Entries below predate this format and are kept verbatim, newest first. Each carries the release
date, the version, and what changed.

 * 2026-08-25 v1.8.1:  Declares `packaging` as a dependency, which `MongoDbStorage` has imported all along without it ever being listed - not in `pyproject.toml` and not in the `requirements.txt` it replaced. It was satisfied by accident, because that file also listed `pytest`, which depends on packaging; 1.8.0 correctly moved the test tooling to the `dev` extra and took packaging with it, so **`mcrit server` and `mcrit worker` cannot start on a clean 1.8.0 install** - `ModuleNotFoundError: No module named 'packaging'` from `MongoDbStorage.py`. Anyone on 1.8.0 should upgrade; an environment that happens to carry packaging (a dev checkout, or anything with pytest installed) is unaffected either way. The CI build job now installs the built wheel into a venv with nothing else in it and imports the server, the storage factory and the CLI from outside the checkout, so an undeclared runtime dependency or a wheel missing code fails the build - every other job installs the `dev` extra and can therefore never see it.
 * 2026-08-25 v1.8.0:  Packaging, tooling and latent-bug release; matching results, configuration and database shape are unchanged from v1.7.1, and no re-index or migration is required. BREAKING (build only): `requirements.txt` and `requirements-dev.txt` are gone - dependencies live in `pyproject.toml`, so install with `pip install -e .` and `pip install -e ".[dev]"` for the development tooling (docker-mcrit is updated to match). `setup.py`, `pytest.ini`, `.coveragerc`, `ruff.toml` and `.pylintrc` collapse into that one file as well, which also stops `pytest`, `pytest-cov` and `coverage` from being installed as *runtime* dependencies of the wheel (#101, THX to @r0ny123!). CI pins every action to a commit SHA, drops its token permissions to the minimum per job, adds timeouts and Dependabot, tests 3.11 through 3.14, and gates on the `ty` type checker next to `ruff`. Adopting `ty` is what makes this more than a tooling release: it surfaced eleven latent bugs, all fixed here (#102, THX to @r0ny123!). Five `MemoryStorage` methods raised on every call - `modifyFamily(is_library=...)`, `deleteFamily(keep_samples=True)`, `getUniqueBlocks`, `getMatchesForPicBlockHash` and the query-sample accessors, which did not accept `is_query` and so broke `Worker`'s query-sample cleanup on that backend; `addStorageContent` wrote a 2-tuple into the pichash index where every other site writes `(family_id, sample_id, function_id)`, silently corrupting it on import. `mcrit server` could not start where gunicorn is absent (i.e. on Windows, where the dependency marker excludes it), `MongoDbStorage.deleteSample` reported a successful query-sample deletion as a failure, `getCandidatesForMinHash` returned `None` where the memory backend returns an empty set, and `LocalQueue._file_to_grid` raised on `str` input. The `AUTH_TOKEN` can now be supplied via the `MCRIT_AUTH_TOKEN` environment variable instead of being edited into the installed config, the API token is compared in constant time, and a server that serves an unprotected API says so in its log (#96, THX to @r0ny123!). Malformed search queries - an unbalanced parenthesis, a dangling operator - are answered with HTTP 400 and the parser's message instead of a 500 and a traceback (#146). `getUniqueBlocks` reports `yara_covers` for real rather than always `0`, and its block entries now carry the same fields on both storage backends, which is what the block cover needs to run on the memory backend at all (#144).
 * 2026-08-24 v1.7.1:  Bugfix release; matching results, configuration and database shape are unchanged from v1.7.0. Search conditions on `pichash` no longer crash the Mongo search transpiler: every condition used to be passed through an `_encodePichash(None, ...)` call that could never work, so a condition on `pichash` raised instead of querying. Conditions now behave per operator - `:`, `=` and `!=` hex-encode the value, `?` and `!?` match a regex against the stored hex representation, and the four range operators are rejected with an explicit error, because the stored form is a variable-length hex string on which comparisons are not meaningful (#100, THX to @r0ny123!; #145 tracks the zero-padded encoding that would make them work). Consequently, sorting function search results by `pichash` is unsupported on the MongoDB backend and now says so: it never worked, since the search cursor pages by comparing the sort field, and the sort named `pichash` where the document stores `_pichash` - so a first page that looked fine was mis-sorted and the second page crashed. Search endpoints answer an unsupported field, operator or sort field with `{"status": "failed"}` and HTTP 400 instead of a 500 and a traceback, and the `sort_by` whitelists raise instead of asserting. Cross-compare reports no longer emit phantom entries for samples outside the requested set, which could leave `matching_percent` with rows of inconsistent width and break the downstream clustering (#98, resolving a TODO from 2022, also THX to @r0ny123!). Minhash storage is now query-function aware: `addMinHash`/`addMinHashes` route negative function ids to `query_functions`, skip banding for them (they are query input only, never index material), and warn instead of silently dropping minhashes whose function does not exist (#97, THX to @r0ny123!) - no production path feeds query functions into either method today, as `MatcherQuery` hashes them in memory, so this hardens a latent case rather than fixing an observed one. Regex literals across the server resources use raw strings and ruff now enforces W605 to keep it that way (#103), plus tests for the serialization helpers (#99), the search transpiler, the search responders and the query-function minhash routing.
 * 2026-08-24 v1.7.0:  BREAKING (database shape): disassembly (`_xcfg`) now lives in dedicated `xcfg`/`query_xcfg` collections keyed by function id, instead of inline in every function document (#137). Matching results are unchanged - reference digests were reproduced byte-for-byte on both the old and the new shape. Upgrading does not require a migration window: every reader falls back to an inline `_xcfg` when no blob document exists, so serving stays correct before, during and after the split - deploy now, migrate when convenient. Moving the blobs out of the hot collection is where the benefit comes from: on the reference corpus (11.6M functions, 37.6 GB of disassembly) a full scan of `functions` fell from 191 s to 9 s, and a freshly written copy of the collection shrinks from 18,353 MB to 3,130 MB. NOTE: an **in-place** migration reaches the same document shape but not the same file size - MongoDB keeps the freed extents - so it temporarily costs about +15 GB for the new `xcfg` collection until you `compact` the `functions` collection; plan disk accordingly. To complete the split, run the bundled migration: `--mode copy --target <rehearsal_db>` for a read-only rehearsal plus `--mode verify` for byte-for-byte comparison, then `--mode inplace` (resumable, writes each blob before unsetting it; 16.6 min at ~11,700 functions/s on the reference corpus), and `--mode unsplit` to roll back. Note that byte-for-byte verification is only possible against a copy or a dump, since an in-place run leaves no original blobs to compare - `verify` then reports coverage and counts only, and labels itself as such. IMPORTANT: after migrating, older MCRIT versions can no longer read the database and fail *quietly* - they start up normally and then see no disassembly at all - so run `--mode unsplit` before downgrading. `/status` reports `inline_xcfg_remaining`, so a half-migrated instance announces itself instead of quietly staying on the old shape. See the [migration guide](https://github.com/danielplohmann/mcrit/blob/main/docs/migration-v1.7.0.md). With the blobs split out, `STORAGE_DROP_DISASSEMBLY` now genuinely reclaims space by dropping the collection where blanking in place did not - but be aware that discarding disassembly also gives up the ability to recalculate MinHashes without re-submitting samples, which is precisely what an SMDA escaper change requires. Minhash exports and `/status` additionally record which SMDA escaper produced their minhashes, as a per-architecture fingerprint (#142): since MinHashes derive from SMDA's escaped instruction representation, an escaper change (e.g. SMDA 4.4.5 reclassifying segment-qualified memory operands, which silently altered ~20% of stored MinHashes on a real corpus) previously made imported signatures silently under-report matches; imports now warn per architecture on escaper mismatch instead.
 * 2026-08-20 v1.6.2:  Reliability release for long-running deployments; matching results are unchanged. The worker now survives a transient mongod outage with capped backoff instead of exiting, a child process that dies without a result fails its job instead of reporting it finished, and a job left half-locked by a progress heartbeat racing a release is claimable again (#106). Queue polling is served by a compound index instead of scanning the whole queue collection (#117; 28 documents examined per poll instead of 11,301 on the instance measured). Storage gains indexes for the `sha256`, `family_id`, `family_name` and `function_name` lookups, drops a full-collection count on the submit path, and pages the band-index rebuild by keyset instead of `skip()` (#115). Counter initialisation is idempotent behind a unique index on `counters.name`, with existing duplicates cleaned up at startup (#105), and the lazy database init is synchronised so server and worker can bootstrap the same database concurrently (#109). Matching batches are packed to a candidate-pair budget (`MINHASH_MATCHING_MAX_PAIRS`, default 50 M) so peak memory is bounded by pairs in flight rather than by a fixed function count, while `MINHASH_MATCHING_FUNCTION_BATCH_SIZE` stays an upper bound (#107); see the new `docs/TUNING.md` for deployment sizing. `/status` reports `num_pichashes` as `null` when the aggregation was skipped rather than a misleading `0` (#108), and the matcher tests no longer pin a specific SMDA version (#134). NOTE: the first startup after upgrading builds the new `functions` indexes, which blocks for minutes on a multi-million function corpus - plan it accordingly.
 * 2026-08-11 v1.6.1:  The v1.6.0 performance features are now on by default: numpy candidate accumulation, vectorised scoring (matching now runs single-process; same results), concurrent signature fetch (thread count derived from CPU count, logged at startup), and the persistent per-job MatchingCache, now with a byte-denominated memory budget (`STORAGE_MATCHING_CACHE_MAX_BYTES`, default 512 MiB; an explicit `STORAGE_MATCHING_CACHE_MAX_ENTRIES` still overrides it). All results remain byte-identical to stock. To restore v1.6.0 behaviour: `STORAGE_CANDIDATE_ACCUMULATION="dict"`, `MINHASH_MATCHING_VECTORIZED=False`, `STORAGE_CACHE_FETCH_THREADS=1`, `STORAGE_MATCHING_CACHE_PERSIST=False`.
 * 2026-08-11 v1.6.0:  Major matching-performance release: four new opt-in optimisations (numpy candidate accumulation, vectorised minhash scoring, concurrent signature fetch, persistent per-job MatchingCache with LRU cap) measured at 4.4x aggregate / up to 10x on large samples when combined, with byte-identical results; pooled matching is now deterministic (#104); two N+1 query patterns removed (#111); minhash score computation micro-optimised (THX to @shaurya703!). All new options default to off - see StorageConfig/MinHashConfig comments for tuning guidance.
 * 2026-08-04 v1.5.3:  Matching reports now load ~7x faster, as MatchingResult.fromDict no longer deep-copies the match lists for filtering (they are derived lazily instead). NOTE: filtered_function_matches / filtered_sample_matches now share their entry objects with function_matches / sample_matches, so consumers must not mutate entries in place; requires mcritweb >= 1.4.2.
 * 2026-07-29 v1.5.2:  More packaging adjustments.
 * 2026-07-29 v1.5.1:  Minor packaging bugfix to include shingler dir in PyPI packages, Dalvik capability upgrade.
 * 2026-07-16 v1.5.0:  MCRIT now supports experimental minhash matching for CIL and Aarch64 binaries.
 * 2026-07-10 v1.4.7:  Major linting and CI/CD overhaul. (THX to @r0ny123!)
 * 2026-01-13 v1.4.6:  Introduced linear banding strategy, to be used as default in future releases (will require re-creation of the whole index).
 * 2025-12-22 v1.4.5:  Fixed a bug due to early conversion when fetching many FunctionEntries at once, which would crash if one function ID does not exist.
 * 2025-12-22 v1.4.4:  No changes, just moved plugins to their own repo located at [mcrit-plugins](https://github.com/danielplohmann/mcrit-plugins).
 * 2025-12-08 v1.4.3:  Major improvements to MCRIT IDA plugin UI, backend now supports faster cross matching jobs only matching among selected samples, minor bugfixes.
 * 2025-09-12 v1.4.2:  QoL improvements and bugfixes to console client (proper markdown for result tables in queries and force recalculation option, faster skipping for dir mode submission).
 * 2025-07-30 v1.4.1:  Filtering for unique matches now takes precedence over scores.
 * 2025-06-13 v1.4.0:  Changed the way how percentages for matching are calculated, now using only matchable code vs. all code as baseline. Minor IDA plugin fixes.
 * 2025-05-22 v1.3.22: McritCLI now supports ENV variables (`MCRIT_CLI_SERVER` and `MCRIT_CLI_APITOKEN`) and a `.env` file for setting server and apitoken  - THX to @r0ny123 for the suggestion!
 * 2025-03-11 v1.3.21: McritCLI now supports submissions with a a spawned worker (requires --worker flag).
 * 2025-02-26 v1.3.20: Fixed a bug where crashing SpawningWorker would not be properly handled - THX to @yankovs!.
 * 2025-02-26 v1.3.18: Added server and API token support for the CLI.
 * 2024-06-20 v1.3.17: Job deletion and cleanup are now [more robust](https://github.com/danielplohmann/mcrit/pull/77) and won't accidentally purge samples unwantedly - @yankovs - THX!!
 * 2024-05-10 v1.3.16: Queue cleanup has been extended to also purge files uploaded during all 3 types of queries (mapped, unmapped, smda).
 * 2024-04-17 v1.3.15: Worker type `spawningworker` will now terminate children after QueueConfig.QUEUE_SPAWNINGWORKER_CHILDREN_TIMEOUT seconds.
 * 2024-04-02 v1.3.14: Experimental: Introduction of new worker type `spawningworker` - this variant will consume jobs from the queue as usual but defer the actual job execution into a separate (sub)process, which should reduce issues with locked memory allocations.
 * 2024-04-02 v1.3.13: When cleaning up the queue, now also [delete all failed jobs](https://github.com/danielplohmann/mcrit/pull/70) @yankovs - THX!!
 * 2024-03-06 v1.3.12: Fixed a bug where protection of recent samples from queue cleanup would lead to key errors as reported by @yankovs - THX!!
 * 2024-02-21 v1.3.10: Bump SMDA to 1.13.16, which covers another 200 instructions in a better escaped category (affects MinHashes).
 * 2024-02-16 v1.3.9:  Finished and integrated automated queue cleanup feature (disabled by default) proposed by @yankovs - THX!!
 * 2024-02-15 v1.3.8:  Bump SMDA to address issues with version recognition in SmdaFunction, fixed exception prints in IDA plugin's McritInterface (THX to @malwarefrank!!).
 * 2024-02-12 v1.3.5:  Recalculating minhashes will now show correct percentages (THX to @malwarefrank!!).
 * 2024-02-02 v1.3.4:  Mini fix in the IDA plugin to avoid referencing a potentially uninitialized object (THX to @r0ny123!!).
 * 2024-02-01 v1.3.2:  FIX: Non-parallelized matching now outputs the [same data format](https://github.com/danielplohmann/mcrit/pull/63) (THX to @dannyquist!!).
 * 2024-01-30 v1.3.1:  The connection to MongoDB is now fully [configurable](https://github.com/danielplohmann/mcrit/pull/61) (THX to @dannyquist!!).
 * 2024-01-24 v1.3.0:  BREAKING: Milestone release with indexing improvements for PicHash and MinHash. To ensure full backward compatibility, recalculation of all hashes is recommended. Check this [migration guide](https://github.com/danielplohmann/mcrit/blob/main/docs/migration-v1.3.0.md).
 * 2024-01-23 v1.2.26: Pinning lief to 0.13.2 in order to ensure that the pinned SMDA remains compatible.
 * 2024-01-09 v1.2.25: Ensure that we can deliver system status regardless of whether there is a `db_state` and `db_timestamp` or not.
 * 2024-01-05 v1.2.24: Now supporting "query" argument in CLI, as well as compact MatchingResults (without function match info) to reduce file footprint.
 * 2024-01-03 v1.2.23: Limit maximum export size to protect the system against OOM crashes.
 * 2024-01-02 v1.2.22: Introduced data class for UniqueBlocksResult with convenience functionality.
 * 2023-12-28 v1.2.21: McritClient now doing passthrough for binary query matching.
 * 2023-12-28 v1.2.20: Status now provides timestamp of last DB update.
 * 2023-12-13 v1.2.18: Bounds check versus sample_ids passed to getUniqueBlocks.
 * 2023-12-05 v1.2.15: Added convenience functionality to Job objects, version number aligned with mcritweb.
 * 2023-11-24 v1.2.11: SMDA pinned to version 1.12.7 before we upgrade SMDA and introduce a database migration to recalculate pic + picblock hashes with the improved generalization.
 * 2023-11-17 v1.2.10: Added ability to set an authorization token for the server via header field: `apitoken`; added ability to filter by job groups; added ability to fail orphaned jobs.
 * 2023-10-17 v1.2.8:  Minor fix in job groups.
 * 2023-10-16 v1.2.6:  Summarized queue statistics, refined Job classification.
 * 2023-10-13 v1.2.4:  Exposed Queue/Job Deletion to REST interface, improved query speed for various queue lookups via indexing and parameterized mongodb queries.
 * 2023-10-13 v1.2.3:  Workers will now de-register from in-progress jobs in case they crash (THX to @yankovs for the code template).
 * 2023-10-03 v1.2.2:  MatchingResult filtering for min/max num samples (incl. fix).
 * 2023-10-02 v1.2.0:  Milestone release for Virus Bulletin 2023.
 * 2023-09-18 v1.1.7:  Bugfix: Tasking matching with 0 bands now deactivates minhash matching as it was supposed to be before. Also matching job progress percentage fixed.
 * 2023-09-15 v1.1.6:  Bugfix in BlockMatching, convenience functionality for interacting with Job objects.
 * 2023-09-14 v1.1.5:  Deactivated gunicorn as default WSGI handler for the time being due to issues with non-returning calls when handling compute-heavy calls.
 * 2023-09-14 v1.1.4:  BUGFIX: Added `requirements.txt` to `data_files` in `setup.py` to ensure it's available for the package.
 * 2023-09-13 v1.1.3:  Extracted some performance critical constants into parameters configurable in MinHashConfig and StorageConfig, fixed progress reporting for batched matching, BUGFIX: usage of GunicornConfig to proper dataclass.
 * 2023-09-13 v1.1.1:  Streamlined requirements / setup, excluded `gunicorn` for Windows (THX to @yankovs!!).
 * 2023-09-12 v1.1.0:  For Linux deployments, MCRIT now uses `gunicorn` instead of `waitress` as WSGI server because of [much better performance](https://github.com/danielplohmann/mcrit/pull/39). As gunicorn needs its own config, this required bumping the minor versions (THX to @yankovs!!).
 * 2023-09-08 v1.0.21: All methods of McritClient now forward apitokens/usernames to the backend.
 * 2023-09-05 v1.0.20: Use two-complement to represent addresses in SampleEntry, FunctionEntry when storing in MongoDB to address BSON limitations (THX to @yankovs).
 * 2023-09-05 v1.0.19: Statistics are now using the internal counters that had been created a while ago (THX to @yankovs).
 * 2023-08-30 v1.0.18: Refined LinkHunt scoring and clustering of results via ICFG relationship.
 * 2023-08-24 v1.0.15: Integrated first attempt at link hunting capability in MatchingResult.
 * 2023-08-24 v1.0.13: Rebuilding the minhash bands will no longer explode RAM usage. Removed redundant path checks (THX to @yankovs).
 * 2023-08-23 v1.0.12: Added the ability to rebuild the minhash bands used for indexing.
 * 2023-08-22 v1.0.11: Fixed a bug where when importing bulk data, the `function_name` was not also added as a `function_label`.
 * 2023-08-11 v1.0.10: Fixed a bug where when importing bulk data, the function_id would not be adjusted prior to adding MinHashes to bands, possibly leading to non-existing function_ids.
 * 2023-08-02 v1.0.9:  IDA plugin can now filter by block size and minhash score, optimized layout and user experience (THX for the feedback to @r0ny123!!)
 * 2023-07-28 v1.0.8:  IDA plugin can now display colored graphs for remote functions and do queries for PicBlockHashes (for basic blocks) for the currently viewed function.
 * 2023-06-06 v1.0.7:  Extended filtering capabilities on MatchingResult.
 * 2023-06-02 v1.0.6:  IDA plugin can now task matching jobs, show their results and batch import labels. Harmonization of MatchingResult.
 * 2023-05-22 v1.0.3:  More robustness for path verification when using MCRIT CLI on Malpedia repo folder.
 * 2023-05-12 v1.0.1:  Some progress on label import for the IDA plugin. Reflected API extension of MCRITweb in McritClient.
 * 2023-04-10 v1.0.0:  Milestone release for Botconf 2023.
 * 2023-04-10 v0.25.0: IDA plugin can now do function queries for the currently viewed function.
 * 2023-03-24 v0.24.2: McritClient can forward username/apitoken, addJsonReport is now forwardable.
 * 2023-03-21 v0.24.0: FunctionEntries now can store additional FunctionLabelEntries, along submitting user/date.
 * 2023-03-17 v0.23.0: It is now possible to query matches for single SmdaFunctions (synchronously).
 * 2023-03-15 v0.22.0: McritClient now supports apitokens and raw responses for a subset of functionality.
 * 2023-03-14 v0.21.0: Backend support for more fine grained filtering.
 * 2023-03-13 v0.20.6: Backend support for filtering family/sample by score in MatchResult.
 * 2023-02-22 v0.20.4: Bugfix for calculating unique scores and accessing these results.
 * 2023-02-21 v0.20.3: Supporting frontend capabilities with result presentation.
 * 2023-02-17 v0.20.2: Extended match report object to support frontend improvements.
 * 2023-02-14 v0.20.0: Overhauled console client to simplify shell-based interactions with the backend.
 * 2023-01-12 v0.19.4: Additional filtering capabilities for MatchingResults.
 * 2022-12-13 v0.19.1: It is now possible to require specific (higher) amounts of band matches for candidates (i.e. reduce fuzziness of matching).
 * 2022-12-13 v0.18.x: Enable matching of arbitrary function IDs.
 * 2022-11-25 v0.18.9: Accelerated Query matching.
 * 2022-11-18 v0.18.8: Harmonized handling of deletion and modifications, minor fixes.
 * 2022-11-13 v0.18.7: Drastically accelerated sample deletion.
 * 2022-11-13 v0.18.6: Added functionality to modify existing sample and family information.
 * 2022-11-11 v0.18.2: Upgrading matching procedure, should now be able to handle larger binaries more robustly and efficiently.
 * 2022-11-03 v0.18.1: Minor fixes.
 * 2022-11-03 v0.18.0: Unique block isolation now also generates a proposal for a YARA rule, restructured result output.
 * 2022-10-24 v0.17.4: Harmonized setup.py with requirements, improved memory efficiency for processing cross jobs.
 * 2022-10-18 v0.17.3: Added a convenience script to recursively produce SMDA reports from a semi-structured folder.
 * 2022-10-13 v0.17.2: Fixed potential OOM issues during MinHash calculation by processing functions to be hashed in smaller batches.
 * 2022-10-12 v0.17.1: Added a function to schedule a job that will ensure minhashes have been calculated for all samples/functions.
 * 2022-10-11 v0.17.0: Search for unique blocks is now an asychronous job through the Worker.
 * 2022-10-11 v0.16.0: Samples from MatchQuery jobs will now be stored with their Sample/FunctionEntries to allow better post processing.
 * 2022-10-04 v0.15.4: Server can now display its version.
 * 2022-09-28 v0.15.3: Addressing performance issues for bigger instances, generating escaped instruction sequence for unique blocks.
 * 2022-09-26 v0.15.0: CrossJobs now in backend, started to provide functionality to identify unique basic blocks in samples.
 * 2022-08-29 v0.14.2: Minor fixes for deployment.
 * 2022-08-22 v0.14.0: Jobs can now depend on other jobs (preparation for moving crossjobs to backend), QoL improvements to job handling.
 * 2022-08-17 v0.13.1: Added commandline option for profiling (requires cProfile).
 * 2022-08-09 v0.13.0: Can now do efficient direct queries for PicHash and PicBlockHash matches.
 * 2022-08-09 v0.12.3: Bugfix for FamilyEntry
 * 2022-08-08 v0.12.2: Bugfix for delivery of XCFG data, added missing dependency.
 * 2022-08-08 v0.12.0: Integrated Advanced Search syntax.
 * 2022-08-03 v0.11.0: (BREAKING) Families are now represented with a FamilyEntry.
 * 2022-08-03 v0.10.3: Now leaving function xcfg data by default in DB, exposed access to it via REST API and McritClient.
 * 2022-07-29 v0.10.2: Added ability to delete families - now also keeping XCFG info for all functions by default.
 * 2022-07-12 v0.10.1: Improved performance.
 * 2022-07-12 v0.10.0: (BREAKING) Job handling simplified.
 * 2022-05-13  v0.9.4: Bug fix for receiving submitted files.
 * 2022-05-13  v0.9.3: Further updates to MatchingResults.
 * 2022-05-13  v0.9.2: Added another field and more convenience functions in MatchingResult for better access - those are breaking changes for previously created MatchingResults.
 * 2022-05-05  v0.9.1: Processing of binary submissions, minor fixes for minhash queuing - INITIAL RELEASE.
 * 2022-02-09  v0.9.0: Added PicBlocks to MCRIT.
 * 2022-01-19  v0.8.0: Migrated the client and the examples into the primary MCRIT repository.
 * 2021-12-16  v0.7.0: Initial private release.

[Unreleased]: https://github.com/danielplohmann/mcrit/compare/v1.9.0...HEAD
[1.9.0]: https://github.com/danielplohmann/mcrit/compare/v1.8.1...v1.9.0
[#44]: https://github.com/danielplohmann/mcrit/issues/44
[#142]: https://github.com/danielplohmann/mcrit/issues/142
[#147]: https://github.com/danielplohmann/mcrit/pull/147
[#149]: https://github.com/danielplohmann/mcrit/issues/149
[#150]: https://github.com/danielplohmann/mcrit/issues/150
[#151]: https://github.com/danielplohmann/mcrit/issues/151
[#152]: https://github.com/danielplohmann/mcrit/issues/152
[#154]: https://github.com/danielplohmann/mcrit/pull/154
[#155]: https://github.com/danielplohmann/mcrit/issues/155
[#156]: https://github.com/danielplohmann/mcrit/issues/156
[#157]: https://github.com/danielplohmann/mcrit/issues/157
[#158]: https://github.com/danielplohmann/mcrit/issues/158
[#186]: https://github.com/danielplohmann/mcrit/issues/186
[#208]: https://github.com/danielplohmann/mcrit/issues/208
[#209]: https://github.com/danielplohmann/mcrit/issues/209
[mcritweb#47]: https://github.com/fkie-cad/mcritweb/issues/47
[mcritweb#59]: https://github.com/fkie-cad/mcritweb/issues/59
[mcritweb#76]: https://github.com/fkie-cad/mcritweb/issues/76
[#42]: https://github.com/danielplohmann/mcrit/issues/42
[#207]: https://github.com/danielplohmann/mcrit/issues/207
[#210]: https://github.com/danielplohmann/mcrit/issues/210
[#241]: https://github.com/danielplohmann/mcrit/issues/241
