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

### Added

- **Families, samples and functions carry `tags`**, free labels such as `packed`, `reviewed` or
  the namespaced `source:vt`. `POST /{families|samples|functions}/{id}/tags` adds and `DELETE` on
  the same path removes the tags named in a JSON body `{"tags": [...]}`; both answer the entity's
  resulting tags, 404 for an unknown id (query samples and functions carry no tags) and 400 for
  a malformed body or an invalid tag, without writing any of the list. There is deliberately no
  replace-all, so two analysts tagging the same entity at once cannot erase each other's tags,
  and no per-tag author. `GET /tags?entity=family|sample|function` answers the distinct tags with
  how many entities carry each. `McritClient.addTags(entity, entity_id, tags)`,
  `removeTags(...)` and `getTags(entity)` wrap them, and refuse an unknown entity before sending.
  - A tag is stripped and lower-cased, then has to be 1-64 letters, digits, spaces, dots, colons,
    underscores or dashes starting with a letter or digit - so `$where` or an empty tag is
    refused rather than stored.
  - One request names at most 100 tags, counted as sent and before any is looked at, and an
    entity carries at most 256; both answer 400 naming the cap, and an add past the entity cap
    adds none of its tags. MongoDB checks the entity cap in the write that adds the tags (a
    `$expr` on the size of the union of stored and new tags in the update's filter), so two
    concurrent adds cannot pass it together, and re-adding tags an entity at the cap carries is
    not refused. The caps are `MAX_TAGS_PER_REQUEST` and `MAX_TAGS_PER_ENTITY` in
    `mcrit.libs.tags`. Without them one POST of 60,000 tags took ~30 s, since the list was
    deduplicated in quadratic time, and grew the document towards MongoDB's 16 MB limit; the
    deduplication is linear now, and the request log names the number of tags, not the tags.
  - `GET /tags` is not paginated: the answer holds one entry per distinct tag, so it grows with
    the tag vocabulary (at most 64 characters a tag), not with the number of tagged entities,
    and every page would have to count all tagged entities again. The entities carrying one tag
    are found by the paginated search `tags:<tag>`.
  - The search takes `tags:packed` (or `tag:packed`) for "some tag is packed", `tags:!=packed`
    (or `NOT tag:packed`) for "no tag is packed" and `tags:?pack` for a substring, MongoDB's own
    semantics for an array field, which the in-memory storage now follows for list fields too.
    The value is normalised like a tag, so `tag:Packed` finds `packed`. A plain search term does
    not look at tags, so existing searches find what they found before.
  - A rename takes the family's tags along, as it does its actors: onto an unused name they move
    with it, onto an existing family they are merged into that family's, behind its own. Renaming
    family 0 (the unknown family) copies them instead, because family 0 stays and keeps its tags.
    A merge is not capped at 256, since a rename is no tagging request to refuse and dropping
    tags would lose them; a family merged past the cap takes no new tag until some are removed.
    Moving a sample to another family moves none: family tags stay on the family, and go with it
    when its last sample leaves and it is deleted, as its actors do.
  - Exports carry sample and function tags in their entries and family tags as `family_tags`,
    which an import merges into what the target knows; an older importer ignores the key. Tags
    of a sample the target already holds are not merged, because the import skips that sample
    as a whole. An imported tag this instance would not accept is dropped, not the import, and
    so are those past the entity cap: an entity keeps the first that fit, in the exported order.
  - A function's dict - its REST answer, an export entry, a report - leaves `tags` out while the
    function carries none, instead of an empty list in every function of a whole-corpus export
    (~12 bytes each). Read it with a default, as `FunctionEntry.fromDict` does. Families and
    samples always carry the key.
  - **Stored data**: entities stored before read as untagged, so there is no migration. MongoDB
    gets a `tags` index on `families`, `samples` and `functions`, built by the first start after
    the upgrade, which waits for it. The one on `functions` is sparse and function documents leave
    the field out while it is empty, so the index holds the tagged functions only; its first build
    on an existing corpus still reads every function document once, but adds no entries.
    `tags:!=x` has to look at every entity on either backend, as a negated condition on any field
    does ([#53]).

### Fixed

- **The in-memory search compares a list field element-wise, as MongoDB does.** `actors:APT28`
  compared the whole `actors` list with the string, so it and `actors:?apt` found nothing on
  `MemoryStorage` while MongoDB found every family with that actor, and `actors:!=APT28` found
  every family rather than those without it. A condition now holds when some element satisfies
  it, and a negated one when none does, for every list field; a test on `actors` runs against
  both backends ([#53]).

## [1.16.0] - 2026-10-16

**Minor release: bounded search and listing, the SMDA report rebuilt from storage, and correct
bookkeeping when samples are deleted, imported or retried.** Nothing changes matching results at
default configuration. There are no repair jobs, but the API now caps what one request can ask for.

- **Behaviour changes for API callers (#271):**
  - `GET /functions` and `GET /search/{families,samples,functions}` answer at most 1,000 entries per
    request. Page with `start` or the `cursor`.
  - `POST /functions` takes at most 10,000 ids. `McritClient.getFunctionsByIds` splits larger
    requests itself; a client from an earlier release gets `{}` back for more than that.
  - A search query longer than 16,384 characters is refused with 400.
  - Search cursors name the sort they belong to, and one replayed under another sort is a 400.
    Cursors from an older server are still accepted.
- **New setting:** `STORAGE_MONGODB_SEARCH_MAX_TIME_MS` (30 s, 0 = no limit) bounds the MongoDB scans of
  a search. A deployment that ships its own config modules needs it added; without it the default
  applies.
- **New route:** `GET /samples/{id}/smda` rebuilds a sample's SMDA report. Its extras live in two new
  collections, `smda_extras` and `query_smda_extras`, filled for samples submitted from now on.
- **First start** creates the indexes of those two collections, and the `pichash_counts` and
  `function_ranges` indexes where no rebuild had created them yet (#262).
- **After upgrading:** a corpus that deleted samples before this release still carries surplus pichash
  counts, which only matter with `MINHASH_PICHASH_MAX_MATCHES` set. `rebuildPicHashCountIndex()`
  clears them, and is also owed after any `recalculatePicHashes` that changes a pichash (#262).

### Added

- **`GET /samples/{id}/smda` and `McritClient.getSmdaReportForSample` rebuild a sample's SMDA
  report from storage.** The report's top-level fields and metadata that `SampleEntry` holds no
  field for - including smda's whole-binary `xdata_refs_from`/`xdata_refs_to` and `xmetadata` -
  are kept in new `smda_extras`/`query_smda_extras` collections keyed by sample id, never in the
  sample document: `SampleEntry.toDict()` is what `/samples`, job results and exports carry, and
  those extras grow with the binary. They are stored as a JSON blob split into documents of at
  most 8 MiB, so a report past the 16 MiB document limit still stores (a 25 MB test report takes
  4 documents, its sample document stays under 1 KB). The disassembly comes from the functions'
  `xcfg` blobs in one batched fetch, and the example report round-trips byte for byte. smda's
  `buffer`, the analysed binary itself in a report made with `STORE_BUFFER`, is never kept, so this
  route can't hand out a binary that `STORAGE_SERVE_SUBMITTED_BINARIES` keeps back. The
  response is `{"smda_report", "complete", "incomplete_reasons",
  "num_functions_without_disassembly"}`: `extras_missing` flags a sample stored before this (its
  extras fall back to an empty report's defaults), `disassembly_missing` counts functions without
  stored `xcfg` (`STORAGE_DROP_DISASSEMBLY`, or a blob over the limit), which are absent from the
  rebuilt report. NOTE that exports do not carry the extras, so an imported sample rebuilds as
  `extras_missing`; deleting a sample, its family or a query sample (also by the query cleanup)
  deletes its extras, `STORAGE_MONGODB_COMPACT_AFTER_CLEANUP` compacts `query_smda_extras` too,
  and `STORAGE_DROP_DISASSEMBLY` keeps them ([#94]). Rebuilding a large sample's report takes
  seconds and hundreds of MiB in the request thread, so a server process runs one rebuild at a
  time; a request that arrives during one answers 503 with `Retry-After` instead of tying up
  another thread. `getSmdaReportForSample` waits that long (at most 30 s, and 5 s for a value that
  isn't a usable number of seconds) and asks again, three requests in all.

### Changed

- Every start creates the `(_pichash, df)` index on `pichash_counts` and the `sample_id` and
  `first_function_id` indexes on `function_ranges`, the specs their rebuilds create ([#261]). Only
  the rebuilds created them before, so a database that started empty, and so never needed a
  rebuild, had neither: the upserts of every add, the cutoff's count lookups and the deletes above
  each scanned the collection. Since `clearStorage` now drops both collections, that would also
  have been the state after every respawn. Where a rebuild already ran they exist, and nothing
  changes.

### Fixed

- **Deleting a sample left it in the pichash counts and the function-range index** ([#261]).
  `deleteSample` took the sample out of the bands and the picblockhash index but not out of
  `pichash_counts` or `function_ranges`, which adding a sample fills, so a deleted sample stayed
  counted and a deleted and re-added one was counted twice. Measured on a 7,244-sample corpus after
  475 samples were deleted and added back: the counts summed to 9,050,959 for 8,657,357 functions,
  306 of 5,000 sampled counts wrong. Only `MINHASH_PICHASH_MAX_MATCHES` reads the counts, so a
  deployment that leaves it at 0, the default, saw nothing; one that sets it could lose exact
  matches whose pichash an inflated count pushed over the cap. A corpus that deleted samples
  before this release still carries the surplus. `MongoDbStorage.rebuildPicHashCountIndex()`
  clears it, though no route or client method runs it yet; `rebuild_function_range_index`
  (`McritClient.rebuildFunctionRangeIndex()`) drops the stale ranges, which are harmless after a
  delete since function ids are never reused. `clearStorage` (`POST /respawn`,
  `McritClient.respawn()`) left both collections behind too, and there they did harm: it resets
  the id counters and vouches for both indexes on the emptied database, so re-added samples were
  counted twice, and with the matching shortlist on (`MINHASH_MATCHING_SHORTLIST_SIZE`, off by
  default) their function ids could resolve to a cleared sample's range. It now drops both.
  Deleting a sample now first sets `pichash_counts_removed: true` on its sample document and takes
  it out of the counts only if that set changed the document, so a retried or overlapping delete
  decrements once. The field is new in the stored data shape and normally lasts only as long as
  the delete, as the sample document is removed with it; a delete that fails partway leaves it on
  the sample until a retry completes the delete.
- **`recalculatePicHashes` left the pichash counts keyed by the old hashes** ([#261]). It rewrites
  `_pichash` in place and marked the picblockhash index incomplete, but not the counts, which went
  on counting hashes the functions no longer carry and missed the new ones. Under
  `MINHASH_PICHASH_MAX_MATCHES` a hash without a count document is dropped, so a rehashed function
  whose new hash no other function held lost its exact matches; and with the decrement above,
  deleting a rehashed sample would take its new hashes away from other samples holding them. A
  recalculation that changes any pichash now marks the count index incomplete before it writes,
  and logs that it did. **Run `MongoDbStorage.rebuildPicHashCountIndex()` afterwards**;
  until then the cutoff counts from the functions collection, which is correct but pays the cost
  the counts exist to avoid, and adds and deletes leave the counts alone. A run that changes no
  pichash leaves them complete.
- **`McritClient` sent its token and request bodies wherever a redirect pointed.** requests drops
  an `Authorization` header when a redirect leads to another host, but not the `apitoken` header
  MCRIT uses, and on a 307 or 308 it sends the body again, so a redirect from the server, or from
  whatever answered in its place, handed the token and any submitted binary or report to the host
  it named. A redirect to another origin now gets neither. One within the server's origin, or
  from http to https on the same host, keeps both, as before.
- **`DELETE /jobs` deleted every job when a date filter didn't parse.** A `created_before` or
  `finished_before` that wasn't `YYYY-MM-DD` or `YYYY-MM-DDTHH:MM:SS` was dropped, and with it the
  only condition of the request, so a typo deleted every job and stored result. It is now a 400 and
  deletes nothing. `MongoQueue.delete_jobs` also dropped `finished_before` whenever `created_before`
  came with it, which deleted queued and running jobs along with the finished ones; both now apply.
  The memory queue's `delete_jobs` compared both dates with `finished_before`, raised a TypeError
  without one, and deleted nothing without a method. It now filters like MongoDB. A repeated date parameter is a 400 as well, where it was a 500.
- **Deleting a cross compare's child job left its combine job queued forever.** The combine job
  waits for its children, and one deleted before it finished never let it go, so a client awaiting
  the cross compare polled indefinitely. Deleting a job now releases whatever waits on it, the way
  a failed job does: the combine job runs and fails, saying a child job failed.
- **A job could be finished, failed or released by a worker that no longer held it.**
  `Job.complete`, `error` and `release` wrote to the job whatever had happened to it since. Under
  `mcrit spawningworker`, a failing job process errored the job and the spawning worker errored it
  again: each failure cost two attempts, a job ended at `attempts_left` -1 and showed as queued
  forever, and the second error could unlock the job under a worker that had taken it in between,
  so two workers ran it at once. A finished job process was also counted as finished twice. These
  calls now act only while the job is unfinished and held by whoever took it (a job process acts
  for its spawning worker), so each failure costs one attempt and a late write from a former
  holder changes nothing; a late completion is logged. The spawning worker's completion still
  releases the jobs waiting on one its job process finished, in case that process died before it
  could.
- **Worker liveness depended on the time zone.** Heartbeats were written and compared in each
  host's local time, so a worker on a host west of the one checking looked dead and had its jobs
  reclaimed, one east of it looked alive for hours after dying, and a DST change shifted every
  worker by an hour. Heartbeats are now also written in UTC (`heartbeats_utc` next to `heartbeats`
  in the queue's `workers` counter) and judged by that. A worker of an older release writes only
  the local one, which is compared in local time as before, so a rolling upgrade judges both alike.
- **`POST /respawn` left every job result and query binary in GridFS.** `MongoQueue.clear()`
  dropped a collection named `fs`, which GridFS doesn't use. It now removes the files no job refers
  to once the queue is gone, as `POST /delete_orphaned_queue_files` does, which also leaves the
  files of other queues sharing the database alone. Results left behind by an earlier respawn go
  with the next one, or with that cleanup.
- **An import or submission that failed part way left samples without their functions.** A sample
  is written before its functions, so `POST /import` failing on a later entry, or on a function
  MongoDB refused, kept the samples written so far without functions, and the retry of the
  corrected export skipped them as present already. A submitted report whose function insert
  failed did the same, and every resubmission was answered that the sample existed. A failed
  import now removes the samples it added, with their functions and family counters, and on
  MongoDB a submission whose function insert failed removes its sample, before the error is
  answered. Samples that were there before
  are left alone, and so are families, which may have been there before as well. So is a sample
  another import stored while this one ran: the import skips it, attaches none of its functions
  to it, and does not remove it if it fails later. Minhashes are
  added after this, and a failure there still leaves the samples in place, as before. Should an
  index write fail after the functions were stored, the samples' picblockhashes are taken out
  again, but their pichash counts stay raised until `rebuildPicHashCountIndex` runs.
- **Imported functions were missing from the function range index.** `addSmdaReport` records which
  function ids each sample owns, but the import didn't, so a shortlisted match never attributed an
  imported function to its sample and dropped its minhash matches. Imports now record them, and
  a range lookup cached while the import ran is not used past it. Functions imported before this
  fix are picked up by `rebuildFunctionRangeIndex`.
- **Resubmitting a report added its function names as labels unchecked.** `POST /samples` for a
  sample that exists already adds the report's function names as the submitter's labels, without
  the limits `PUT /functions/{id}` applies, to a sample that may well be someone else's. Such a
  label now has to be what a rename could set, 1 to 256 printable ASCII characters once stripped,
  and others are left out. The first report of a sample keeps every name it holds, long mangled
  and non-ASCII ones included, as before. On MongoDB, a label for a function deleted meanwhile no
  longer recreates it as a document holding only that label.
- **Search answered 500 for input it couldn't serve.** Only a `ValueError` was answered as a client
  error, so each of these failed with a traceback, on MongoDB, in memory, or both:
  - an integer beyond 63 bits for an `_id` or `num_` field, or beyond 64 bits for an `offset`;
  - a `pichash` that wasn't a number;
  - a numeric field compared with text;
  - a query nested fifty levels deep.

  They are 400s now, with a message saying what's wrong. A number in a query may still be `0x`
  hex, and now also decimal with leading zeros, which was compared as text and found nothing, but
  no longer a fraction or a negative offset. On MongoDB, an
  `offset` of 2^63 and above, where 64-bit kernel code sits, now finds the function stored there;
  it was a 500. MongoDB stores such an offset as a negative number and sorts it below every other
  one, so a range comparison with one (`offset:>=0xffffffff80000000`) is a 400 there rather than
  a wrong answer; in memory it works.
- **`GET /recalculate_minhashes` lost the minhashes it couldn't compute again.** It dropped every
  minhash first and then hashed the functions that have disassembly, so the minhashes of functions
  whose disassembly is gone (`STORAGE_DROP_DISASSEMBLY`, or never stored) were gone for good, and their samples were recorded as hashed by the running smda all the same.
  Those minhashes are now kept and put into the fresh band index, and only the samples hashed
  again are recorded as current. `repairMinHashes` dropped them the same way for a sample only some
  of whose functions still have disassembly; it keeps them now, and such a sample stays listed as
  stale, so each repair hashes its other functions again and `/status` keeps counting it: that
  count no longer reaches zero while such samples are left. The fresh band index also gets its
  `(band_hash, df)` and `(band_hash, bucket)` indexes right away; they used to come back only with
  the next start.
- **A sample submitted without a timestamp broke the sample listing on MongoDB.** smda reads an
  empty `timestamp` as none, MCRIT stored it as null, and reading the sample back raised a
  TypeError. From then on every route that read it answered 500, `GET /samples` and sample search
  included. Such a sample now reads back with no timestamp.
- **Sample search sorted by `timestamp` answered 500 whenever there was a next page**, on both
  backends: the page's cursor held a datetime, which JSON can't encode. The cursor now holds the
  timestamp as it's stored. Paging through samples without a timestamp also stopped at the last of
  them, since nothing compares greater than a missing value; the cursor now steps past them, and
  over them backwards and in descending order.
- **On the memory backend, sorting samples by `timestamp` failed when a store mixed time-zone-aware
  and naive timestamps**, as it does when smda runs in the same process. It now sorts them as the
  text MongoDB stores, which leaves out the time zone, so both backends order them the same way.
  The query cleanup job raised a TypeError on such a query sample in the same way; it now compares
  the timestamp without its time zone too. Past that, it still couldn't delete a stale query sample
  on the memory backend, which looked for the sample's functions among the regular ones and raised
  a KeyError; it now deletes the query sample and its functions.
- **The query cleanup job aged query samples by the server's local time**, while smda stamps them
  in UTC. On a server east of UTC a query sample was deleted early by the offset, and west of UTC
  it was kept that much longer. Query samples are now compared with a cutoff in UTC; query jobs,
  which the queue stamps in local time, keep a local one. The schedule that starts the cleanup had
  the same offset, since the storage records the last cleanup in UTC, and now reads it as UTC too.
- **`sort_by=function_id` on sample search is refused with a 400.** Samples have no function id:
  MongoDB sorted every sample as null, and the memory backend answered 500 from the second page on.
- **`MemoryStorage` deletes query samples and families with samples again.** Deleting a query
  sample raised `KeyError` (it looked in the corpus dicts), and `deleteFamily` removed the family
  before its samples, so the first sample's count update failed its assertion. MongoDB was not
  affected ([#94]).

### Security

- **A search cursor's values reached MongoDB as query operators.** `GET
  /search/{families,samples,functions}` decoded the `cursor` it hands out and put its values into
  the query unchecked, so a JSON object there became an operator on the sort field (for example
  `$regex` on `sort_by=function_name`), evaluated by mongod against every entry. A cursor has to be
  `[[value, ...], 0 or 1]` now, with one string, number (up to 8 bytes), boolean or null per sort
  field; anything else is answered 400, as is a cursor with a different number of values than the
  sort has fields. Cursors the server issues name the sort fields and directions they belong to, and
  one replayed under another sort field or direction is answered 400 on both storages (before, it
  gave an arbitrary page on both, or a 500 in memory when the values could not be compared).
  Cursors an older server issued carry no sort and are accepted as before, checked by the number of
  values only. The MongoDB scans of a search now have a time limit,
  `STORAGE_MONGODB_SEARCH_MAX_TIME_MS` (30 s, 0 = none); the lookups of a single id or sha256 do
  not. A search that runs out is answered 400 with the advice to narrow the search term.
- **A search query with a few unbalanced parentheses held a server thread for minutes.** The query
  grammar retried alternatives that began with the same operand, so every nesting level multiplied
  the work: `((((((a` took about 50 s on `GET /search/families` before it was answered 400, and a
  handful of such requests occupied every server thread. AND and OR chains are loops now, a NOT
  whose operand fails is no longer re-read as a search term, and a quoted term no longer matches an
  escaped quote two ways (an unterminated `"\"\"\"...` cost 2^n steps). Queries parse to the same
  trees as before, in milliseconds for ordinary queries; the slowest shapes at the new
  16,384-character limit take about 1.3 s. A query longer than that, or nested deeper than the
  stack allows (about 47 levels of parentheses, 33 of `(NOT `), is answered 400 instead of 500. A
  chain of terms used to run out of stack at about 75 terms (implicit AND) or 105 (AND, OR); now
  chains up to the length limit parse (about 220 OR'd `sha256:` terms, where main stopped at about
  105). Behaviour change: a query over 16,384 characters is refused even where main answered it,
  for instance a single very long term. Note that `"a\\\"b"` searches for `a\"b` now; the
  escaped quote used to be unescaped a second time, into `a"b`.
- **`GET /functions` loaded the whole corpus, disassembly included, into the server process.**
  Without `limit` (and `McritClient.getFunctions()` sends 0) every function was read and serialised
  in one response, about ten times its stored size, and `GET /search/{families,samples,functions}`
  did the same with `limit=-1` or a very large `limit`. Both answer at most 1,000 entries per
  request now; page with `start` (a position in the function collection, not a function id) or the
  `cursor`. `POST /functions` looks each id up once, in one query per collection, and answers 400 for
  more than 10,000 ids; `McritClient.getFunctionsByIds` sends its ids in requests of at most that
  many and merges the answers, so it works with any number of ids against old and new servers
  (with `raw_responses` it still sends one request, and the server's answer is returned as it is).
  Behaviour change: a caller that relied on either listing to return every entry receives the
  first 1,000, and one that posts ids to `/functions` itself can send at most 10,000 per request; a `McritClient` from an earlier release sends all ids in one request and gets `{}` back for more than that.

## [1.15.0] - 2026-10-12

**Minor release: a cheaper PicHash recalculation, client fixes, family actors, an optional binary
store and a query cleanup that keeps working.** Nothing changes results. There are no repair jobs,
and every new setting is off by default.

- **The first start builds five new indexes**: three on `samples` for the recalculation stamps
  (#250), and two for the `sample_binaries` GridFS bucket, which every instance creates whether or
  not it keeps binaries (#178).
- **The first `recalculatePicHashes` after upgrading is still about a full pass**, since no sample
  carries the new stamp yet. Each run after that selects only what changed (see Fixed).
- **Client callers:**
  - `McritClient.awaitResult` now raises `JobFailedError` for a failed job. It used to return
    `None` by default, after a request to `/results/None`.
  - `modifyFamily` sends JSON, and in raw mode it answers the `Response`.
  - The client now reuses one connection per instance (see Changed).
- **New optional stored fields, no migration:**
  - on samples: `pichash_smda_version` and `pichash_unrehashable_smda_version`
  - on families: `actors`
  - on query samples: `num_query_functions`
- **New settings, all off by default:** `STORAGE_KEEP_SUBMITTED_BINARIES`,
  `STORAGE_SERVE_SUBMITTED_BINARIES` and `STORAGE_MONGODB_COMPACT_AFTER_CLEANUP`. A deployment that
  ships its own config modules needs them added (docker-mcrit warns at startup about missing
  settings).

### Added

- `STORAGE_MONGODB_COMPACT_AFTER_CLEANUP`, default `False`, runs MongoDB's `compact` on
  `query_samples`, `query_functions` and `query_xcfg` after every query cleanup (`DbCleanup`), so
  the space the cleanup freed goes back to the file system instead of staying inside the collection
  files for reuse. It needs the `compact` privilege, which `readWrite` does not carry; a refusal is
  reported per collection in the cleanup's result rather than failing the job. The queue's GridFS
  (`fs.files`, `fs.chunks`), where the results of the deleted query jobs were, is compacted too when
  the queue shares the storage database, as it does by default, and left alone when the queue has a
  database of its own.

- **Families carry `actors`, the names they are attributed to**, set through
  `PUT /families/{id}` and `McritClient.modifyFamily(family_id, actors=[...])` (an empty list
  clears them), kept through a rename, and carried by exports as `family_actors`, which imports
  merge into what the target already knows. Names are 1-64 characters of letters, digits,
  spaces, dots, dashes and underscores; anything else answers 400 without touching the family.
  Families stored before read as an empty list, so no migration. NOTE that
  `McritClient.modifyFamily` now sends its update as JSON, since a list does not survive form
  encoding; the route accepts both, and with `raw_responses=True` it now answers the `Response`
  like the other client methods, instead of parsed data ([#57]).

- `STORAGE_KEEP_SUBMITTED_BINARIES` keeps the raw binary a sample was submitted as, in the
  GridFS bucket `sample_binaries` beside the corpus, and `GET /samples/{sample_id}/binary` serves
  it back (`McritClient.getSampleBinary`) once `STORAGE_SERVE_SUBMITTED_BINARIES` allows it. Both
  are off by default. Keeping stores the sample a second time, so a corpus that turns it on grows
  by the size of its submissions. Serving is its own switch because it is its own decision: the
  binaries are, on most instances, malware, and serving makes the API a place to download them
  from; until it is turned on, the route answers 403 for every sample, without saying whether
  one exists or has a binary kept. A sample submitted before keeping was turned on has no binary
  until it is submitted again, which the existing-sample path now completes instead of skipping.
  A binary is stored once per content, keyed by `metadata.sha256`, with `metadata.sample_ids`
  listing the samples it belongs to, so that the job queue's file parameters, which it already
  shares by sha256, could later refer to the same file without a data migration. Deleting a sample
  takes it off its binary and deletes the binary once no sample is left on it; a submission of the
  same bytes racing that deletion stores a fresh copy rather than linking to the one being deleted.
  A process dying between those steps can leave a file no sample refers to; nothing collects such
  files yet. NOTE that exports do not carry kept binaries, so an imported corpus has none, and
  that every instance creates the bucket's two indexes on first start, whether keeping is on or
  not ([#95]).

- `StorageInterface.hasSampleBinary()` and `.openSampleBinary()`, so neither asking whether a
  binary is stored nor serving one has to read the file. `openSampleBinary()` answers with a
  stream (a GridFS `GridOut`, or a `BytesIO` from `MemoryStorage`) rather than bytes, and the
  binary route serves through it: peak allocation while serving is bounded by the driver's
  cursor batch instead of the sample size - measured flat at ~34 MiB for 32, 128 and 256 MiB
  samples, against 64, 256 and 512 MiB for `getSampleBinary()`, which costs twice the file
  because `GridOut.read()` joins the chunks it has collected. The resubmission check in
  `Worker.addBinarySample` used `getSampleBinary(...) is None` and so streamed the whole file
  to answer a yes/no question; it uses `hasSampleBinary()` now, which reads one metadata
  document. `getSampleBinary()` is unchanged for callers that want the bytes.

### Changed

- `McritClient` sends all requests through one `requests.Session`, so consecutive calls reuse a
  connection instead of opening a new one, with a new TLS handshake on HTTPS, every time. Against
  mcrit.malpedia.io, 166 sequential block-hash queries took 46 s instead of 166 s. A session keeps
  the cookies a server sets for the life of the client, is not formally thread-safe, and can hit a
  pooled connection the server already closed, which surfaces as a connection error on that call.
  Tests patch `requests.Session` methods instead of the module-level `requests` functions. (#254)

### Fixed

- **`McritClient` raised a `JSONDecodeError` for a 2xx whose body is not JSON** ([#257]). A reverse
  proxy's error or login page, or an empty answer, with a 200 or 202 status went through
  `response.json()` unguarded and came out of the client as a `ValueError` in either error mode,
  past a caller that relies on `None` by default or catches `McritClientError` under
  `raise_server_errors`. Such an answer is now a failed one like any other: `None` by default,
  `McritServerError` with an empty message under `raise_server_errors`, and a warning in the log
  naming the status, which is where to start looking for the proxy. A JSON body that is not an
  object was already handled this way. The IDA and Binary Ninja plugin vendors this client and gets
  the same once it takes this version up.

- `McritClient.awaitResult` asked the server for the result of a job that failed, instead of
  reporting the failure it had just waited for ([#252]). A job whose attempts ran out keeps
  `result` None, and the wait had only two outcomes - terminated raises, anything else fetches
  `job.result` - so the client went on to request `/results/None` and the server answered 400
  (`Valid ResultIDs are hexstrings with 24 characters.`): as `None` by default, as that message
  with `raise_client_errors=True`, and in neither case naming the job or its `last_error`.
  docker-mcrit's `repair.sh` died that way when a production `recalculatePicHashes` lost its
  attempts to its worker's child timeout plus a lock expiry counted as the last one. The wait now
  raises a dedicated `JobFailedError`, carrying the job id and the queue's `last_error` when one
  is recorded, next to `JobTerminatedError` and raised whatever the client's error mode, because a
  caller that waited wants to know the job failed rather than receive a `None` it cannot tell
  from other failures. NOTE that a job which finished despite a reclaimed lock still answers its
  result: the failure is decided by the missing result, not by `attempts_left`. `Job.last_error`
  reads a missing field as None now - the client wraps server answers in that class, and a job
  that never errored does not carry one.

- **`recalculatePicHashes` picked most of the corpus again on every run.** It chose samples by
  their report's `smda_version` and only overwrote that field on samples where some hash changed, so
  every sample whose hashes came out the same stayed selected - 8,691, then 7,096 of 8,699 samples
  on a real corpus, each run a near-full pass ([#249]). It now records the smda it rehashed a sample
  with in a new per-sample field, `pichash_smda_version`, for every sample whose functions were all
  rehashed, changed or not, and selects by that; a sample without the field is judged by its report's
  `smda_version` as before, so a corpus the old code already bumped is not revisited, and one without
  any `smda_version` is picked. A newly added report is stamped on insert, as its hashes are computed
  then; an imported sample is not, and is judged by its report's version. A second run now selects
  nothing. NOTE that `smda_version` is no longer overwritten: it keeps naming the smda
  that produced the report. A sample with a function whose disassembly is gone (e.g. dropped with
  `STORAGE_DROP_DISASSEMBLY`) cannot be rehashed, and would otherwise be picked again on every run,
  by this selection and by the non-Intel block hash one of [#240]: it gets neither stamp, but is
  marked with the running smda in `pichash_unrehashable_smda_version`, and neither selection picks
  it again while that smda runs. The result counts the samples marked in a run in
  `samples_skipped_xcfg_missing`. `/status` reports the samples still pending in
  `num_samples_with_stale_pichashes` and the marked ones apart in
  `num_samples_pichash_unrehashable`; a marked sample no longer counts in
  `num_samples_with_stale_picblockhashes` either. NOTE that a marked sample is retried once when
  smda changes, and unmarked if it then rehashes completely; one whose disassembly was restored
  before that is retried by unsetting its `pichash_unrehashable_smda_version`. The counts read
  three new `samples` indexes, built on first start: one on `smda_version` for the distinct report
  versions, one on `pichash_smda_version` and `smda_version` for the stamps, and a sparse one on
  `pichash_unrehashable_smda_version`. A run killed midway loses at most the sample it was on.
  NOTE that the first run after upgrading is still about as long as before: samples the old code
  never bumped carry no stamp and an old `smda_version`, so they are selected once more and stamped
  then - on a 8,711-sample / 12M-function corpus a near-full pass of about 1 h 45 min. Runs after
  that select only what changed.

- The query cleanup (`STORAGE_MONGODB_ENABLE_CLEANUP`) failed on the first query job that left no
  result - one that failed before matching - because it read the query sample
  out of every job's result, and so never deleted anything once such a job existed ([#68]). It now
  deletes an old job without a result and moves on, and it also reaches failed
  `getMatchesForSmdaReport` jobs, which it did not look at. Its result says how many query
  samples and jobs it deleted. NOTE that terminated query jobs are still not collected: the
  cleanup looks at finished and failed ones only.

- The query data nothing refers to any more is deleted by the same cleanup ([#68]): query functions
  whose query sample is gone, and query disassembly whose function is gone. Both were left behind
  for good by an interrupted deletion or insert. Each is judged only behind a boundary taken
  before the walk, in batches, so that a query inserted meanwhile is never looked at. The
  disassembly is written before its functions, so it is judged only while no query sample is
  short of its functions: a query sample now records how many functions its insert writes
  (`num_query_functions`), and one still missing some is an insert in flight - or one that died,
  whose disassembly then goes in the run that deletes its sample. Without that check, two queries
  inserted at once that finished out of order had the disassembly of the one still writing its
  functions deleted under it. Query samples written before this release have no count and hold
  the disassembly judgment back until the last of them has expired.

## [1.14.0] - 2026-10-09

**Minor release: one addition, fixes, and nothing to run on a deployment that keeps the defaults.**
The band df cutoff coverage report (#201) is new API, hence 1.14 rather than a patch. No new
indexes, no change to `RESULTS_VERSION`, no repair jobs.

- **A deployment that set `SHINGLER_LOGBUCKETS` or `SHINGLER_LOGBUCKET_RANGE` away from its default
  needs a full re-index after upgrading**, and its exports regenerated afterwards: it was hashing
  with the default table all along (see Fixed). On the defaults, MinHashes are unchanged.
- Under `STORAGE_BAND_BUCKET_SIZE`, run `rebuild_band_df_index` once if band documents were ever
  deleted: it recreates a bucket 0 that went missing, which the cutoff and the coverage report
  otherwise cannot see (see Fixed).

### Added

- `docs/limitations.md` records what the data model deliberately leaves out, starting with
  functions whose body a linker folded under several names: MCRIT keeps one of them, which bounds
  attribution by name and understates it when scored against a single expected name ([#126]).

- **What `STORAGE_BAND_DF_CUTOFF` skips is measurable** ([#201]), as a job:
  `GET /band_df_cutoff_coverage` (optionally `?band_df_cutoff=N`, refused with a 400 unless an
  integer from 0 to 2^63 - 1, the largest a BSON integer holds) answers a job id, and
  `McritClient.requestBandDfCutoffCoverage()` does the same. Its result gives per band and in total
  the band hashes, the postings (sum of df), how many of each are over the cutoff and the
  fractions, plus the same totals at the reference cutoffs 50, 100, 200, 500 and 1000 - so a cutoff
  of `0` still shows what one would skip, and two reports compare whatever cutoff each asked about.
  The worker logs the headline at INFO.

  The cutoff is a fixed number while posting lists lengthen with the corpus (band-hash vocabulary
  follows Heaps' law, V(n) = 1412.8 · n^0.7247), so the share it skips grows silently. On a
  7,244-sample real corpus, at 200, **46.4 % of band postings (51.8 M of 111.8 M) sit
  in 0.97 % of band hashes (83,235 of 8,538,312)**. The count runs off the query path on purpose:
  a per-lookup count would roughly double each band lookup's index work. On MongoDB each band is
  one `$group` over a covered scan of the `(band_hash, df)` index (hinted, no band document
  fetched, constant memory, no `allowDiskUse`); on that corpus (MongoDB 7.0) a single index-only
  `$group` of exactly this shape took 39.3 s for all 20 bands, about 1.1 to 2 s per band.

  **Caveats**: the share is a leading indicator, not recall - re-measure recall with
  `benchmarks/compare_quality.py` when it moves. A database whose df is not trusted yet (built
  before df, until `rebuild_band_df_index` has run) gets `available: false` and a message saying
  so instead of numbers; df-less documents are never counted as empty posting lists. Under
  `STORAGE_BAND_BUCKET_SIZE` a spilled hash counts once with its bucket-0 total; a hash whose
  bucket 0 is missing is not counted (nor served under the cutoff) until `rebuild_band_df_index`
  repairs it (see Fixed). MemoryStorage
  counts the same numbers and, since [#217], applies the cutoff when matching as MongoDbStorage
  does, so both report `backend_applies_cutoff: true`; a backend that did not apply it would say
  so in the headline.
  WAND/MaxScore pruning was not built: it needs posting lists sorted by function id, which the
  fill-order buckets of `STORAGE_BAND_BUCKET_SIZE` are not.

### Fixed

- **Unique blocks in memory mode failed for any subset of the stored samples.**
  `MemoryStorage.getUniqueBlocks` raised `KeyError` whenever the store held samples other than the
  ones asked about: it collected the blocks of every stored function and then counted them against
  the requested samples only. It now reads the blocks of the requested samples, as `MongoDbStorage`
  does, and answers the same.

- `SHINGLER_LOGBUCKETS` and `SHINGLER_LOGBUCKET_RANGE` take effect. The logbucket table was cached as
  `mcrit/cache/logbuckets.json` whatever its parameters and loaded whenever that file existed, and
  the package ships it, so every installed package hashed with the 100,000/1 default table
  regardless of what it configured - `LogBucket(1024, 1)` answered with 100,000 entries ([#202],
  [#215]); a source checkout whose copy of the file had been deleted hashed with whichever table it
  built next, and needs a re-index as well unless that table was built for the values it is
  configured with. The shipped file is now `logbuckets_100000_1.json`, byte for byte the same table, so **a
  deployment on the defaults hashes exactly as before and needs nothing**. One that set either
  value away from its default was hashing with the default table all along; its MinHashes now
  follow its settings and no longer agree with what is stored, so it needs a full re-index after
  upgrading. Its exports have to be regenerated after that re-index, too: their `config.shingler`
  hash already encodes the non-default values, so an upgraded instance with the same settings
  would accept them without complaint while their MinHashes came from the default table. A table
  for other parameters is built in memory once per process (0.2 s at 100,000 entries) and never
  written to disk, where the old code wrote one into the package directory. The builder cannot
  produce a proper range for the lowest values once `SHINGLER_LOGBUCKET_RANGE` reaches 5, or when
  `SHINGLER_LOGBUCKETS` is too small for the range (below 6 for a range of 4), which an installed
  package never reached, since it always loaded the default table: such a
  setting now raises `ValueError` when the shinglers are loaded - even with
  `FuzzyStatPairShingler` weighted 0, as `ShingleLoader` instantiates every shingler - instead of
  `KeyError` in the middle of indexing. A `SHINGLER_LOGBUCKETS` below 1 or a negative range raises
  `ValueError` as well, and a non-int value of either `TypeError`.

- **With `STORAGE_BAND_BUCKET_SIZE` set, a band hash that deletions shrank back under
  `STORAGE_BAND_DF_CUTOFF` lost the candidates in its upper buckets.** The df-indexed lookup
  matches bucket 0 alone, the only document carrying df. For a hash that never spilled that is all
  of it, and a spilled hash has a df the cutoff rejects - but pulls can bring a spilled hash's df
  back under the cutoff while its surviving postings sit in buckets above 0. The lookup then
  returned bucket 0's postings only, often none, and matching treated the hash as one without
  candidates, with no error. A lookup now also fetches the upper buckets of every admitted hash
  whose bucket 0 names a tail above 0, in one indexed query per band; only deletions produce such
  a hash, so the query is normally never made, and the df index flag is now read once per lookup
  rather than once per band. This covers the df-indexed lookup; the `$size` fallback, used only
  until `rebuild_band_df_index` has run once after enabling bucketing, still measures bucket 0
  alone. The df match now admits bucket 0 alone, so a stray df on an upper bucket (only switching
  bucketing back off, which is unsupported, stamps one) cannot admit that bucket twice, and a
  lookup reads a bucket 0 without a posting list as an empty one instead of failing the job with
  `KeyError: 'function_ids'`. `getCandidatesForMinHash`, the single-function lookup no matcher
  uses but the storage interface offers, read only the first document the lookup returned, so
  under bucketing it missed every bucket but one even without a cutoff; it reads them all now.
  Whether the upper buckets are fetched is decided from the cutoff the job applies, so a
  per-request `band_df_cutoff` neither counts a hash twice nor misses a shrunk hash's buckets.

- **One function stored without its disassembly failed every function hashed beside it.** A
  function's `xcfg` reads back as `{}` once `STORAGE_DROP_DISASSEMBLY` removed it, once a blob over
  MongoDB's 16 MiB limit was dropped at insert ([#42]), or after importing an export of such an
  instance, and smda rejects `{}` ("serialized function is incomplete"; smda before 4.4.5 raised
  `KeyError`). `Worker.calculateMinHashes` handed it over anyway, so the minhashing job of that
  sample - and every `complete_minhashes` batch of 10,000 functions it fell into - failed, on
  every retry; `recalculateAllPicHashes` stopped on a stored `{}` and link-hunt clustering
  (`MatchingResult.clusterLinkHuntResult`) on an entry whose disassembly was dropped. All of them now
  rebuild through one helper, `FunctionEntry.smdaFunctionFromXcfg`, skip such a function and log
  one warning per call with the number skipped; `recalculateAllPicHashes` no longer counts a
  skipped function's old block hashes in `picblockhashes_updatable`.
  `FunctionEntry.toSmdaFunction` answers `None` for it, so a caller that used its result
  unchecked now has to handle `None`. An `xcfg` that is present but lacks a field smda requires
  still raises, now naming the fields. The cause does not depend on the smda version: every smda
  release MCRIT supports requires the same fields (4.4.5 and newer check for them, older ones read
  them unconditionally), and none can rebuild a function from `{}`. A skipped function stays
  without a minhash, and the warning with the count is the only trace of it.

  The unique-blocks job (`getUniqueBlocks`) failed the same way, with `KeyError: 'blocks'`, when a
  candidate block's function had no disassembly: MemoryStorage only guarded against `None`, and
  MongoDbStorage decoded a missing or `{}` blob to `{}` and indexed it anyway. Such blocks are now
  reported without instructions (an empty `instructions` list and `escaped_sequence`) on both
  backends, as MemoryStorage already did for a block offset its xcfg lacks, instead of failing the
  job. The job leaves them out of its result and counts them in
  `statistics["blocks_without_instructions"]`: with no instructions to show and no bytes to match
  on, a block cover that picked them claimed a complete rule that then failed to render (`max()` of
  no instructions) or rendered an empty, invalid string, and MCRITweb's block table fails on a
  block without instructions the same way. `UniqueBlocksResult.generateBlockCover` skips such a
  block too, for results stored before. A sample hashed under `STORAGE_DROP_DISASSEMBLY` therefore
  completes the job with no unique blocks to show and no YARA rule, and says why in that count.

- With memory storage and the fake queue (`STORAGE_METHOD = "memory"`, `QUEUE_METHOD = "fake"`), no
  job or result could be fetched by id: `LocalQueue` minted `uuid4` ids, and `/jobs/{id}`,
  `/jobs/{id}/result`, `/results/{id}` and `/results/{id}/job`, as well as `DELETE /jobs/{id}`,
  accept only the 24 hex characters of an ObjectId, so every one of them answered 400 ([#203]).
  `LocalQueue` now mints ObjectIds as `MongoQueue` does; the accepted id format is unchanged. The id
  check is also anchored at the end: an id that merely started with 24 hex characters used to pass
  it and then fail as an invalid ObjectId inside `MongoQueue`, which the client saw as a 500
  instead of the 400 it now gets. Ids from either queue have exactly 24, so no valid request is
  affected, and the routes hand them on in lower case, the form both queues store, so an id in
  upper case now finds its job in `LocalQueue` too, as it always did in `MongoQueue` - and so does
  one in a `GET /jobs?job_ids=...` selection. Reaching these
  routes in that mode exposed `LocalQueue` indexing its file tables with ids they did not hold.
  `/results/{id}/job` for an unknown id and `DELETE /jobs/{id}` for a job without a result (failed
  or terminated; this one also answered 500 and left the job half deleted) each left an empty entry
  behind, on which the next periodic clean-up, and from then on every new job, failed with a
  `TypeError`; `/results/{id}?compact=true` for an unknown id answered 500. All three now answer
  `null`, or delete the job, and leave the tables alone. `MongoQueue` answered every one of the
  result routes for an unknown result id with a 500 (GridFS raising `NoFile` for its metadata) and
  now answers `null` as well.

- **`rebuild_band_df_index` recreates a missing bucket 0** under `STORAGE_BAND_BUCKET_SIZE`. It
  only updated an existing bucket 0, so a hash whose bucket 0 was gone while higher buckets
  survived kept no df anywhere: the cutoff never served its postings and the coverage report
  ([#201]) could not count them, with no error either way. The rebuild now upserts bucket 0 while
  postings survive, as the recompute after a deletion already did. Both now create it with an
  empty posting list: the recompute's upsert left `function_ids` out, so the next candidate lookup
  that returned the recreated document raised `KeyError: 'function_ids'` and failed the job.

## [1.13.0] - 2026-09-29

**Results change in this release, and the upgrade has an order.** Matching stays within one
architecture (#230), a request's `minhash_score` now applies (#235), and non-Intel block hashes and
AArch64 minhashes are recomputed (#244, #245). Cached jobs are keyed on the values they run with and
on `RESULTS_VERSION` 2 (#235, #243), so every cached match is recomputed once on its next request.
Rehearsed on a copy of an 8,699-sample / 11.7M-function corpus whose reports go back to smda 1.9,
with this release, smda 4.9.0 and picblocks 2.1.0:

1. **Upgrade server and workers together.** The server now resolves the matching defaults, and an
   older worker fails matching jobs on arguments it does not know. Rebuild images: picblocks 2.1.0 is
   the floor (#230). smda 4.9.0 escapes Intel, AArch64, CIL and Dalvik exactly as 4.5.0 did, so its
   fingerprints and every rehashed minhash and PicHash are unchanged.
2. **The first start builds new indexes before storage answers** - 34 of them from 1.9.x, in
   **7.5 min** on that corpus.
3. **`recalculatePicHashes`**, then **`rebuildPicBlockHashIndex`**, then **`repairMinHashes`** - in
   **1 h 46 min**, **5 min** and **6 s** there. `recalculatePicHashes` revisits every sample whose
   report predates smda's escaper compatibility version (4.4.5), whatever its architecture, so on an
   older corpus it is a full pass; most of it is the recompute, and the final `reIndex` of `functions`
   took 8 min. NOTE that it is worth it beyond non-Intel samples: on that corpus it rewrote **435,122
   function PicHashes**, 357,474 of them in the 376 Intel samples whose reports came from smda 1.9.x
   (62% of their functions), whose minhashes had been repaired before but whose PicHashes never were.
   Exact matches against such samples come back, e.g. 6,565 -> 24,824 foreign PicHash matches for
   one of them.
4. Serve matching after that. Job caches do not key on corpus data, so a match computed between the
   upgrade and the end of the repairs would hold pre-repair PicHashes.

### Added

- `QUEUE_SPAWNINGWORKER_CHILD_MAX_MEMORY` bounds the memory of each job a spawning worker runs
  ([#69]). Off by default. The worker measures the resident size of the job's process tree once a
  second and kills a job over the limit, which then fails like any other failing job instead of
  taking the host's memory from the workers beside it - the failure reported, one query growing a
  worker to tens of GB and starving the rest. Linux only (it reads `/proc`); see `docs/TUNING.md`
  for sizing.

- A worker running jobs in its own process returns the memory a finished job freed to the operating
  system (`malloc_trim`, glibc only), instead of holding on to its largest job's peak while idle
  ([#69]). Sample matching on a 7,244-sample corpus left an idle worker at 1.0-2.3 GiB before and at
  0.45-0.58 GiB after, for 30-120 ms per job; the reports are identical.

- **`recalculatePicHashes` also redoes the block hashes of non-Intel samples that a picblocks
  before 2.1.0 computed**, which escaped every block as Intel code, and `/status` counts them as
  `num_samples_with_stale_picblockhashes` ([#240]). Samples stored from now on record the
  picblocks their block hashes came from (`picblockhash_version`); a non-Intel sample without that
  record, or with an older one, is rehashed and then recorded. That covers every sample stored
  before, and every imported one, whose block hashes another instance computed. Intel samples are
  left to the existing SMDA version check, since their block hashes did not change. MongoDB storage
  only; the in-memory storage has no recalculation and leaves the count out of `/status`.

  NOTE that a sample with a function whose disassembly is gone (e.g. dropped with
  `STORAGE_DROP_DISASSEMBLY`) cannot be rehashed completely, so it stays counted until it is
  deleted and submitted again. Rewritten block hashes mark the picblockhash index incomplete until
  `rebuildPicBlockHashIndex` runs, as any recalculation that changes block hashes does, and
  unique-blocks results computed before stay in the job cache until their job is deleted
  (`DELETE /jobs/<job_id>`), since the unique-blocks routes do not take `force_recalculation`.

- `POST /delete_orphaned_queue_files` (`McritClient.deleteOrphanedQueueFiles`) schedules a job that
  deletes the queue's GridFS data no job refers to any more: results whose job is gone, submitted
  files no existing job uses and no submission holds, and chunks whose file document is gone. Its
  result says how many of each it deleted; with `dry_run=true` it deletes nothing and says how many
  it would have, which a real run can undercut when a submission claims one of the files in between.
  This is what the deletion paths fixed below left behind, so an instance that has deleted jobs
  before - including through the query-sample cleanup - should run it once. MongoDB reuses the space
  freed; returning it to the operating system still takes a `compact`. A job is looked for in every
  queue of the database, since they share its GridFS. `dry_run=true` or `dry_run=false` has to be
  given in the query string; a request without it, or with any other value, is answered with 400
  rather than taken as a real run. Chunks younger than an hour are left alone, as GridFS writes a
  file's chunks before its document, and so is a file still claimed by a submission that died before
  creating its job. The submitted files it deletes include binaries of indexed samples that an
  earlier bulk deletion left in the queue: MCRIT never serves those, but they may be a deployment's
  only copy of a binary, so take one first if that matters; the dry run counts them among the file
  parameters.

- `/status` reports `escaper_fingerprints`, and exports record, a fingerprint of how smda escapes
  AArch64, ARM (A32/Thumb), CIL and Dalvik code next to the Intel one ([#93]), so that a change in
  how smda escapes any architecture MCRIT computes MinHashes for shows, not only an Intel one. ARM
  reads `unavailable` under an smda that has no ARM escaper yet, as PyPI's 4.8.0 does. The Intel
  fingerprint, and `escaper_fingerprint` in `/status`, are unchanged; an import compares only the
  architectures the export holds samples of. An export made before carries the Intel fingerprint
  alone: it is compared as before when it holds Intel samples, and otherwise logs that it has
  nothing to compare.

- **`shortlist_size` and `band_df_cutoff` can be set per matching request** ([#217]), overriding
  `MINHASH_MATCHING_SHORTLIST_SIZE` and `STORAGE_BAND_DF_CUTOFF` for that job alone: as query
  parameters of the `/matches/sample/...` and `/query/...` endpoints, and as keyword arguments of
  `McritClient.requestMatchesForSample`, `getMatchesForSmdaFunction` and the three
  `requestMatchesFor...` query methods (`requestMatchesForSampleVs` and `requestMatchesCross` take
  `band_df_cutoff` only). Both change which matches are reported, so they are a choice per request
  rather than per deployment. `0` switches either off. A value that is not an integer from 0 to
  2^63 - 1 (the largest integer a MongoDB query takes; the df cutoff goes into one) is refused with
  a 400 - a repeated parameter as well - as is, with band bucketing on, a `band_df_cutoff` above
  `STORAGE_BAND_BUCKET_SIZE` - the check the storage makes for a configured cutoff at startup
  ([#196]) - and a `shortlist_size` on a match restricted to the samples it names: one sample
  against another, within a group (`sample_group_only`) or across several (a cross compare).
  Refused rather than replaced or dropped, unlike the older options, because a changed value
  answers a question the caller did not ask and nothing in the response would say so.

- **Matching presets, `hunt` and `identification`** ([#217]), per request as `preset=` on the
  `/matches/sample/...` and `/query/...` endpoints, and as `preset` on `McritClient`'s matching
  methods and `MinHashIndex`'s matching jobs. Both use `band_matches_required=1`, below the default
  of 2: in the measurements on [#217] turning the shortlist on never moved top-10 or top-25 recall,
  while every higher value did. `hunt` turns the shortlist off - the most exhaustive and slowest of
  the measured configurations, the baseline the others were compared with. `identification` turns
  it on, at the configured `MINHASH_MATCHING_SHORTLIST_SIZE` or else 100, the size measured on
  [#195]; k=1 with the shortlist on was the only non-baseline combination that held recall at 1.000
  on both queries measured, at about 3x the speed of k=1 without it. A preset only fills in the
  knobs a request leaves out (the df cutoff keeps its configured value unless the request sets one)
  and is expanded into their values before the job is submitted, so the job is keyed on what it
  runs with, shares its result with the equivalent explicit request, and its report shows the
  values rather than the preset's name. On a match restricted to the samples it names it applies
  all but the shortlist. An unknown or repeated preset is refused with a 400. The "fast" preset the
  issue floated is left out until a measurement defines it. `docs/TUNING.md` has the table.

- **Every match report records its knobs under `info.matching`** ([#217]): `requested` (the values
  the job was submitted with; the server and `MinHashIndex` fill in the configured value of every
  knob a caller leaves out, so `null` appears for a knob the job does not take - the shortlist of a
  match restricted to named samples - for a job handed to a `Worker` directly, and for every knob of
  a job queued before this version), `applied`
  (what it ran with; `null` for a knob with nothing to act on - the shortlist of a match restricted to
  named samples, and both shortlist and df cutoff when `band_matches_required` is 0) and `fallbacks`
  (knob to reason, for any that could not be applied). The one fallback so far is a shortlist while
  the function range index is incomplete or unsupported (`function_range_index_incomplete` /
  `function_range_index_unsupported`): the job matches against the whole corpus, as before, and now
  says so instead of only logging it. `MatchingResult` keeps the block as `matching_info` through
  `fromDict`/`toDict`.

### Changed

- Query matching (a binary or SMDA report matched without being stored) looks up PicHash matches in
  one query for all of the query's hashes instead of one query per hash, and applies
  `MINHASH_PICHASH_MAX_MATCHES` as sample matching already did, so a hash held by much of the
  corpus no longer returns one tuple per holder there either ([#69]). With the knob at its default
  of 0, the matches are exactly the ones reported before. `MemoryStorage` applies the cutoff too;
  it ignored it for sample matching as well.

- The pichash and minhash totals of a match report are computed without building the per-function
  match lists they used to build and throw away, which were two of the three full passes over
  every match ([#69]). The report is unchanged: tests compare both ways on every call during sample
  and query matching.

- **`FuzzyStatPairShingler` reads the frame size of AArch64 functions**, from `sub sp, sp, #imm`
  and pre-indexed pushes such as `stp x29, x30, [sp, #-0x20]!` in the first ten instructions of the
  entry block, as it reads `sub esp/rsp` for Intel, where it took 0 for every non-Intel function
  before ([#238]). Matched through MCRIT's own banding on smda's AArch64 Mach-O corpus (12
  families, one sample each) plus the FlexibleFerret/FrostyFerret test fixtures, MinHash-only
  matches across families fell from 384 to 289, and those within the ferret family went from 109 to
  111; one of the five confirmed ferret matches (score 56.25 now) shares one band with its partner
  instead of two and is no longer found. CIL and Dalvik have no frame to read and keep 0.
  `RESULTS_VERSION` goes up with this ([#241]), so match reports cached before are recomputed on
  their next request rather than handed out with the old AArch64 matches.

  NOTE that this changes the MinHashes of every AArch64 function. Samples now record the shingler
  revision their MinHashes were computed at (`minhash_shingler_revision`), AArch64 samples hashed
  before count in `num_samples_with_stale_minhashes` on `/status`, and `repairMinHashes` rehashes
  them. A sample with no function large enough to hash is recorded as current instead of being
  skipped; one whose disassembly was dropped with `STORAGE_DROP_DISASSEMBLY` still is, and has to be
  deleted and submitted again. Until the repair has run, AArch64 samples indexed before and after
  the upgrade match each other less well, and AArch64 match reports cached in that window keep
  their results until requested with `force_recalculation`. Upgrade the workers before running the
  repair: an older worker rehashes with the old shingler.

### Fixed

- A spawning worker started its job processes as `python` from `PATH`, which need not be the
  interpreter MCRIT runs in; it now uses the same one.

- **A query parameter given twice answered 500, or quietly matched nothing.** falcon hands a
  repeated parameter over as a list, and the responders read single values. On `/jobs` and
  `/jobs/count` a repeated `sample_ids` or `job_ids` ([#210]) failed in the comma-list parsers, a
  repeated `start`, `limit`, `ascending`, `state` or `filter` failed converting or matching it,
  and a repeated `method` or `username` reached the queue as a list and matched no job; the same
  `.lower()` and `int()` calls failed on a repeated `compact` of the result routes, `with_refresh`
  of `/jobs/stats`, and the flags and paging of the family, sample, function and status routes,
  while the matching routes dropped a repeated `minhash_score` and friends for the configured
  value. A middleware now refuses any repeated parameter with a 400 naming it, after routing (an
  unknown route still answers 404) and before a responder runs - which of two values was meant is
  unknowable - and logs it. `sample_ids` and `job_ids` are the exception: a repeat reads as its
  comma-joined form.

- **`McritClient.getJobCount` and `deleteQueueData` spliced their values into the URL unencoded**,
  so a `filter` or `method` holding `&` or `#` became a second parameter or cut off the ones after
  it. They are passed as request parameters now, which encodes them. `addBinarySample` keeps
  splicing `filename`, `family` and `version` as given, since its caller encodes them (MCRITweb
  does, and would otherwise see every name with a space or bracket encoded twice); its docstring
  now says so.

- Deleting a job left its data in GridFS ([#80]). `DELETE /jobs/{id}` and the query-sample cleanup
  removed only the `fs.files` document of the job's result and of the files it was given, never
  their `fs.chunks`, so every submitted binary and every result stayed in the database for good,
  unreachable. Deleting jobs in bulk (`DELETE /jobs` by method or age) removed the result with its
  chunks, but left the submitted files linked to the deleted jobs, so nothing could ever delete
  those. Both now delete through GridFS, which takes the chunks along, and release the jobs' files,
  deleting each one no other job uses. A file is only deleted after clearing its hash in the same
  update that checks it is still unused, as `clean()` already did, because a new submission of the
  same binary claims the file by that hash; before, a submission arriving between the check and the
  deletion could be handed a file that was deleted under it. Bulk deletion also deletes exactly the
  jobs it listed, rather than evaluating its filter a second time, which also caught a matching job
  submitted in between without releasing its files and then failed on the count; it works through
  them in chunks of 10,000, and it now lowers the queue counters for what it deleted, which it
  computed and never applied, so `/jobs/stats` went on counting bulk-deleted jobs until asked
  `with_refresh=true` - and even that left the counts of a method with no jobs left as they were,
  which it now sets to zero. Both lower a job's count from its state just before it is deleted
  rather than from one read long before, so the counts rarely drift; `with_refresh=true` corrects
  them when they do. Either deletion removes the job before its result and files, so an interruption
  leaves only files no job refers to, which the sweep above takes, rather than a finished job
  without its result, which a repeated request would be handed. The memory queue deletes a job's
  unused files with it too, and deletes a job without a result.

- Sample, query, function query, vs, vs-group and cross match reports included matches against
  samples of another architecture ([#93]). A PicHash or MinHash only means the same thing for two
  functions escaped by one instruction set's rules; across architectures, shingles still collide in
  bands now and then. On a corpus of 6,315 Intel, 458 CIL, 14 Dalvik and 13 AArch64 samples (and 444
  SMDA could not disassemble), sample matching reported them at scores of 51 to 61, just over the
  threshold - one Dalvik sample was reported against 84 samples of other architectures next to 13 of
  its own. Such matches are now left out, and with `MINHASH_MATCHING_SHORTLIST_SIZE` set, samples of
  another architecture no longer take places on the shortlist. A corpus of one architecture is
  matched as before, with the same lookups (with the shortlist on, the entries of the samples voted
  for are fetched in one batch as well). A sample whose architecture is unknown (an empty string, as
  a sample SMDA could not disassemble has) is not taken as another one. ARM (A32/Thumb) and AArch64
  are different architectures here. What is left out was not signal: on the Lazarus backdoor that
  McAfee tied from Windows (two PE32 builds) to Android (three ARM ELFs) by its C2 protocol, sample
  matching found no match between the PEs and the ELFs, while it matched the PEs at 100 and the ELFs
  at 99 among themselves. Across all PE and ELF function pairs the best was 39, and the one pair
  known to be the same function (the receive routine that checks for 0x301) scored 3.
  `RESULTS_VERSION` goes to 2 with this, so reports the job cache kept from before, which still
  hold such matches, are recomputed on their next request rather than handed out again ([#241]).

- Block hashes of non-Intel code are computed with that architecture's escaper: MCRIT now requires
  picblocks 2.1.0, which escaped every block as Intel code before ([#93]). picblocks was unpinned
  above 1.1.2, so installations set up since its 2.1.0 release on 2026-09-13 compute the new hashes
  already; this makes it the floor. Intel block hashes are unchanged. Non-Intel samples indexed
  before keep the block hashes they were stored with, so their unique blocks compare correctly only
  with samples indexed before, until `recalculatePicHashes` redoes them ([#240]).

- **`shortlist_size` and `band_df_cutoff` can be set per matching request** ([#217]), overriding
  `MINHASH_MATCHING_SHORTLIST_SIZE` and `STORAGE_BAND_DF_CUTOFF` for that job alone: as query
  parameters of the `/matches/sample/...` and `/query/...` endpoints, and as keyword arguments of
  `McritClient.requestMatchesForSample`, `getMatchesForSmdaFunction` and the three
  `requestMatchesFor...` query methods (`requestMatchesForSampleVs` and `requestMatchesCross` take
  `band_df_cutoff` only). Both change which matches are reported, so they are a choice per request
  rather than per deployment. `0` switches either off. A value that is not an integer from 0 to
  2^63 - 1 (the largest integer a MongoDB query takes; the df cutoff goes into one) is refused with
  a 400 - a repeated parameter as well - as is, with band bucketing on, a `band_df_cutoff` above
  `STORAGE_BAND_BUCKET_SIZE` - the check the storage makes for a configured cutoff at startup
  ([#196]) - and a `shortlist_size` on a match restricted to the samples it names: one sample
  against another, within a group (`sample_group_only`) or across several (a cross compare).
  Refused rather than replaced or dropped, unlike the older options, because a changed value
  answers a question the caller did not ask and nothing in the response would say so.

- **Matching presets, `hunt` and `identification`** ([#217]), per request as `preset=` on the
  `/matches/sample/...` and `/query/...` endpoints, and as `preset` on `McritClient`'s matching
  methods and `MinHashIndex`'s matching jobs. Both use `band_matches_required=1`, below the default
  of 2: in the measurements on [#217] turning the shortlist on never moved top-10 or top-25 recall,
  while every higher value did. `hunt` turns the shortlist off - the most exhaustive and slowest of
  the measured configurations, the baseline the others were compared with. `identification` turns
  it on, at the configured `MINHASH_MATCHING_SHORTLIST_SIZE` or else 100, the size measured on
  [#195]; k=1 with the shortlist on was the only non-baseline combination that held recall at 1.000
  on both queries measured, at about 3x the speed of k=1 without it. A preset only fills in the
  knobs a request leaves out (the df cutoff keeps its configured value unless the request sets one)
  and is expanded into their values before the job is submitted, so the job is keyed on what it
  runs with, shares its result with the equivalent explicit request, and its report shows the
  values rather than the preset's name. On a match restricted to the samples it names it applies
  all but the shortlist. An unknown or repeated preset is refused with a 400. The "fast" preset the
  issue floated is left out until a measurement defines it. `docs/TUNING.md` has the table.

- **Every match report records its knobs under `info.matching`** ([#217]): `requested` (the values
  the job was submitted with; the server and `MinHashIndex` fill in the configured value of every
  knob a caller leaves out, so `null` appears for a knob the job does not take - the shortlist of a
  match restricted to named samples - for a job handed to a `Worker` directly, and for every knob of
  a job queued before this version), `applied`
  (what it ran with; `null` for a knob with nothing to act on - the shortlist of a match restricted to
  named samples, and both shortlist and df cutoff when `band_matches_required` is 0) and `fallbacks`
  (knob to reason, for any that could not be applied). The one fallback so far is a shortlist while
  the function range index is incomplete or unsupported (`function_range_index_incomplete` /
  `function_range_index_unsupported`): the job matches against the whole corpus, as before, and now
  says so instead of only logging it. `MatchingResult` keeps the block as `matching_info` through
  `fromDict`/`toDict`.

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

- **A matching job's cached result could be served for different settings** ([#217]). A job is reused
  for any later request with the same arguments, and a request that left an option out was keyed
  on its absence, not on the value the worker then filled in from its configuration. So after a
  change to `MINHASH_MATCHING_THRESHOLD`, `PICHASH_SIZE`, `BAND_MATCHES_REQUIRED`,
  `MINHASH_MATCHING_SHORTLIST_SIZE` or `STORAGE_BAND_DF_CUTOFF`, every request relying on the default
  was served the result computed under the old value. `MinHashIndex` - behind the server, and for
  any direct caller - now puts the value of all five into each matching job's arguments, the
  configured one where the caller names none (`mcrit.index.MatchingParameters`), in a fixed order so
  a job listing shows each knob in the same position. Jobs from before the upgrade were keyed
  without them, so the first repeat of each matching request after the upgrade computes a fresh
  result instead of reusing the old one - a one-off recompute, not a correctness change.
  **Upgrade server and workers together:** the server's configuration now decides these defaults for
  the jobs it submits (it used to be each worker's), and a worker from before this change fails
  every matching job on the `shortlist_size` and `band_df_cutoff` arguments it does not know.
  A job submitted while the shortlist cannot be applied is marked so in its arguments
  (`shortlist_unavailable`), so its whole-corpus result has a cache key of its own and is not served
  for the same request once the index is complete again. A job whose shortlist became unavailable
  only after submission - a rebuild of the function range index started in between, or during the
  run - is marked `cacheable: false` on the job document before it completes, on every worker
  (`mcrit worker` and each job `mcrit spawningworker` runs), and a request made after that is not
  handed its result: a job method can return its result as a `QueueRemoteCalls.UncacheableResult`,
  and both queues' cache lookups skip such jobs. A request that attached to the job while it was
  still queued or running does receive that result, with the fallback named in
  `info.matching.fallbacks`. Knob values are normalised in the key (`50.0` and `50`, `True` and `1`
  are one job).

- **A matching request's `minhash_score` had no effect on the result** ([#217]). The matchers filtered
  candidate pairs on the configured `MINHASH_MATCHING_THRESHOLD` alone; the requested threshold was
  handed to the scoring call only together with `ignore_threshold=True`. It now decides which
  MinHash matches are reported, on both the vectorized and the pairwise path. Requests that do not
  set it, or set the configured value, get the same result as before; the others get what they
  asked for. The same holds on the process-pool path. `/query/function` also passed the requested
  `minhash_score` and `pichash_size` on as `None`, hard-coded `exclude_self_matches` to `False`
  although `McritClient.getMatchesForSmdaFunction` sends it, and failed on `force_recalculation`;
  it applies all three now and accepts the last.

- **`MemoryStorage` ignored `STORAGE_BAND_DF_CUTOFF`** ([#217]); it applies it now, and a job's own
  `band_df_cutoff`.

- **A match between named samples could leave some of them out** ([#217]). Matching one sample
  against another (`/matches/sample/{a}/{b}`), within a group
  (`sample_group_only`) or across several (`/matches/sample/cross/...`) with
  `MINHASH_MATCHING_SHORTLIST_SIZE` set could leave out the very samples it was asked about: the
  shortlist is ranked over the whole corpus, and a named sample outside its top entries was not
  matched - in a cross compare its pair read as 0 %. None of them takes a shortlist any more: one
  asked for is refused, with a 400 on the routes and a `TypeError` for a direct `MinHashIndex`
  caller, and a cross compare asks its 1-vs-corpus jobs for `shortlist_size=0` explicitly, since a
  job left without one would take the configured shortlist.

- **The df-cutoff fallback served a spilled band hash as bucket 0 alone** ([#196]). With band
  bucketing on and the df index not yet trusted (before `rebuild_band_df_index` has run), the
  fallback measures bucket 0's list, and a full bucket 0 is exactly `STORAGE_BAND_BUCKET_SIZE`
  long, so at a cutoff equal to the bucket size a spilled hash passed and a truncated posting list
  was served. The fallback no longer serves a hash whose bucket 0 names a tail above 0; one that
  pulls shrank below the cutoff is therefore left out by it rather than served truncated. (The
  df-indexed lookup reads such a shrunk hash's upper buckets only with the separate fix for it.)

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
[#94]: https://github.com/danielplohmann/mcrit/issues/94
[#241]: https://github.com/danielplohmann/mcrit/issues/241
[#93]: https://github.com/danielplohmann/mcrit/issues/93
[#196]: https://github.com/danielplohmann/mcrit/pull/196
[#217]: https://github.com/danielplohmann/mcrit/issues/217
[#195]: https://github.com/danielplohmann/mcrit/pull/195
[#80]: https://github.com/danielplohmann/mcrit/issues/80
[#238]: https://github.com/danielplohmann/mcrit/issues/238
[#240]: https://github.com/danielplohmann/mcrit/issues/240
[#69]: https://github.com/danielplohmann/mcrit/issues/69
[#126]: https://github.com/danielplohmann/mcrit/issues/126
[#201]: https://github.com/danielplohmann/mcrit/issues/201
[#202]: https://github.com/danielplohmann/mcrit/issues/202
[#203]: https://github.com/danielplohmann/mcrit/issues/203
[#215]: https://github.com/danielplohmann/mcrit/issues/215
[#261]: https://github.com/danielplohmann/mcrit/issues/261
[#257]: https://github.com/danielplohmann/mcrit/issues/257
[#252]: https://github.com/danielplohmann/mcrit/issues/252
[#249]: https://github.com/danielplohmann/mcrit/issues/249
[#57]: https://github.com/danielplohmann/mcrit/issues/57
[#53]: https://github.com/danielplohmann/mcrit/issues/53
[#68]: https://github.com/danielplohmann/mcrit/issues/68
[#95]: https://github.com/danielplohmann/mcrit/issues/95
