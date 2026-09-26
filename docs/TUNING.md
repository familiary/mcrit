# Tuning an MCRIT deployment

> **Canonical copy:** `docs/TUNING.md` in the [mcrit](https://github.com/danielplohmann/mcrit)
> repository, which is where the config classes whose defaults this document describes live. The
> copy in [docker-mcrit](https://github.com/danielplohmann/docker-mcrit) is a verbatim mirror:
> edit mcrit's, then copy the file over, and never the other way round. A change to a default in
> `MinHashConfig` or `StorageConfig` should update this document in the same commit.

These recommendations come from a benchmarking campaign against a real Malpedia corpus
(11,697,468 functions / 8,483 samples / 2,175 families, 20 bands `{4:20}`, threshold 50) on an
8-core / 32 GiB host with mongod 5.0. Roughly 200 full matching jobs were measured across 81
distinct samples, spanning 50 to 5,482 functions and both `BAND_MATCHES_REQUIRED` regimes.
Every configuration change was verified result-preserving by comparing a SHA-256 digest of the
complete match report, so the settings below trade speed and memory only — never matches.

Two findings drive the sizing. First, **matching memory scales with how much a sample pulls
out of the corpus, not with how large the sample is**: peak RSS correlates 0.98 with bytes
fetched from MongoDB and only 0.62 with the sample's function count, so a 2,100-function
sample can cost more RAM than a 5,500-function one. Second, **matching is bound by MongoDB
read latency, not by CPU**: the hot query returns a 186-byte projection out of 4,188-byte
documents, so it spends its time waiting rather than computing. Spare cores are therefore worth
much more on I/O concurrency than on additional matching processes, and RAM is best spent on
mongod's cache and on retaining fetched signatures between batches.

## Recommended settings

Values are **per concurrent matching job** — multiply the worker budget if you run several.

| host | mongod WiredTiger cache | `MINHASH_MATCHING_FUNCTION_BATCH_SIZE` | `STORAGE_MATCHING_CACHE_MAX_BYTES` | `STORAGE_CACHE_FETCH_THREADS` | expected worker peak |
|---|---|---|---|---|---|
| 8 GiB / 2 cores | 3 GiB | 200 | 300 MiB (~1.0 M functions) | 2 | ~1.5–2 GiB |
| 16 GiB / 4 cores | 6 GiB | 500 | 640 MiB (~2.1 M functions) | 4 | ~2–3 GiB |
| 32 GiB / 8 cores | 12–15 GiB | 1000 | 1.5 GiB (~5.0 M functions) | 8 | ~3–5 GiB |
| 64 GiB / 16 cores | 24 GiB | 1000–5000 | 1.5 GiB | 8 | ~5–8 GiB, room for 2–3 jobs |

`STORAGE_MATCHING_CACHE_MAX_BYTES` is set as a plain byte count (`536870912` for the 512 MiB
default). It and `STORAGE_CACHE_FETCH_THREADS` are only worth setting where the table differs
from the shipped default: as of MCRIT 1.6.1 the budget defaults to 512 MiB, and fetch threads
defaults to `min(4, cpu_count / 2)` — deliberately below the values above, which assume mongod
is not shared with another workload. Both resolved values are logged at startup.

Always keep `WiredTiger cache + Σ worker peaks + 2 GiB` inside physical RAM, and set a
`mem_limit` on the worker service in `docker-compose.yml` — an unlimited container leaves any
overcommit to the host OOM killer, which may choose mongod.

These settings are strictly better and involve no trade-off. **As of MCRIT 1.6.1 they are the
defaults**, so on 1.6.1 or later there is nothing to set — they are listed here for what they
buy, and because the value is what you would restore to on 1.6.0:

| setting | value | effect |
|---|---|---|
| `STORAGE_CANDIDATE_ACCUMULATION` | `"numpy"` | 1.5–5.9x faster candidate retrieval, several hundred MB less peak memory |
| `MINHASH_MATCHING_VECTORIZED` | `True` | 27–29x faster scoring (59,500 → 1.59 M pairs/s) |
| `STORAGE_MATCHING_CACHE_PERSIST` | `True` | drops a 1.5–12.2x redundant per-batch cache rebuild |

`MINHASH_POOL_MATCHING` no longer needs setting either: with vectorised scoring the matching
phase runs single-process and ignores the flag, which also removes the run-to-run
non-determinism pooled matching caused. Indexing still honours `MINHASH_POOL_INDEXING`.

Notes on the individual knobs:

* **Batch size** trades memory for speed: `1000` is ~1.5x faster than `200` for ~1.5x the peak
  memory, while `10000` is no faster than `5000` and costs 2.4–3.7x the memory of `200`.
* **Pair budget** (`MINHASH_MATCHING_MAX_PAIRS`, default 50,000,000, new in 1.6.2) caps how many
  candidate pairs a batch accumulates before it is scored, at roughly ~250 B resident per pair.
  Candidate volume per query function spans five orders of magnitude, so a function count alone
  cannot bound the tail; the budget does, and at the default it only binds on runaway jobs. The
  batch size above stays an upper bound on query functions per batch, so both limits apply and
  lowering either one lowers peak memory. Lowering the budget buys memory with wall time - measured
  on a 53 M-pair sample: 10 M cost +68 % wall for -40 % peak, 2 M cost +203 % for -49 %, and most of
  that penalty is batches evicting each other from the MatchingCache when the budget sits far below
  the candidate union. Set it to `0` to restore fixed-size batches.
* **Cache ceiling** costs ~314 bytes per retained function. Size it above your largest sample's
  candidate set; when it binds it roughly halves the benefit. Since 1.6.1 evictions are logged,
  so a ceiling that binds is visible in the worker log rather than silent — entries needed by
  the batch being served are never evicted.
* **Fetch threads** overlap MongoDB read latency: 2.2x at 2 threads, 3.4x at 4, 6.8x at 8 on an
  isolated fetch. Where mongod is shared with other workloads, treat this as a politeness knob.
* **`BAND_MATCHES_REQUIRED` is not a tuning knob** — it changes results. Raising it from 2 to 3
  removed 62 % of the scoring work at a cost of 9.5 % of matches on the sample tested. Choose
  it for analysis quality, then tune around it.

## Two-stage matching: making 1-vs-N stop scaling with the corpus

Everything above lowers the *slope* of 1-vs-N cost. Two knobs change its *shape*, because every
stage of a 1-vs-N query - and the result set itself - otherwise grows with the corpus.

| knob | default | what it does |
|---|---|---|
| `MINHASH_MATCHING_SHORTLIST_SIZE` | `0` (off) | how many corpus samples the exact matching stage may look at. A cheap stage ranks candidate samples first; only the best N are matched exactly |
| `STORAGE_BAND_DF_CUTOFF` | `0` (off) | skip band hashes whose posting list is longer than this. A band hash held by much of the corpus is a stopword: expensive to read, uninformative about *which* samples match |
| `MINHASH_PICHASH_MAX_MATCHES` | `0` (off) | skip PicHashes held by more than this many corpus functions. Same argument for the exact-match path, which the shortlist does not bound: a hash covering a common library function returns one tuple per holder |

**Both default to off, so an upgrade changes nothing until you opt in.** Two indexes need one
build each before they take effect, and neither is read until a completeness flag vouches for
it — so the old behaviour holds until they exist:

```python
storage.rebuildFunctionRangeIndex()  # required for shortlisting
storage.rebuildBandDfIndex()  # makes the df cutoff skip from the index, not after it
```

Measured at 12,500 samples / ~10.2M functions: 145 s and 147 s respectively.

### Suggested starting point

```
MINHASH_MATCHING_SHORTLIST_SIZE = 100
STORAGE_BAND_DF_CUTOFF = 200
MINHASH_PICHASH_MAX_MATCHES = 0      # raise from 0 only once PicHash lookup shows up in timings
```

Measured on a fixed query set at 257 and 12,500 samples — a **48.6x** growth in corpus size,
warm cache, repeated runs:

| | 257 samples | 12,500 samples | growth |
|---|---|---|---|
| one-stage median | 0.429 s | 4.427 s | 10.32x (latency ~ corpus^0.60) |
| two-stage median | 0.645 s | 0.374 s | 0.58x |
| two-stage mean | 0.868 s | 0.835 s | 0.96x |
| two-stage max | 1.695 s | 1.810 s | 1.07x |

One-stage latency grows with the corpus; two-stage does not. Note the first row of the
two-stage column: **on a small corpus two-stage is slower**, because the ranking stage costs
something and there is nothing yet to save. It is worth enabling when queries have started to
hurt, not before.

### What it costs

Unlike everything else in this document, these two knobs are **not** result-preserving, so they
are quoted against the unrestricted result rather than a digest:

| corpus | top-10 sample recall | top-25 sample recall | surviving function matches with identical score |
|---|---|---|---|
| 257 | 1.000 | 1.000 | 1.000 |
| 10,000 | 1.000 | 1.000 | 0.9945 |
| 12,500 | 1.000 | 1.000 | 0.9936 |

Matching *within* a shortlisted sample is unchanged — same candidates, same scores. What a
shortlist can cost is a sample not being ranked into it: overall sample recall at 12,500 samples
is 0.67, because a query whose unrestricted answer names 5,930 matched samples gets 99. Raise
`MINHASH_MATCHING_SHORTLIST_SIZE` if you need more of the tail; cost grows with it roughly
linearly. PicHash matching is unaffected and stays exact, so exact matches are still reported
whether or not their sample made the shortlist.

Tuning the cutoff at 12,500 samples, shortlist held at 100: cutoff 1000 gives a 1.172 s median,
200 gives 0.374 s, 100 gives 0.332 s — all three at top-10 and top-25 recall of 1.000. 200 is
where the traversal stops scaling; below that there is little left to win.

## Growing past ~270,000 samples: `STORAGE_BAND_BUCKET_SIZE`

Separate from latency, and a hard stop rather than a slowdown. A band posting list is a
`function_ids` array inside one document, and MongoDB caps a document at 16 MB. Measured by
pushing ids into one document until the write is refused, a document holds about **1.35 million
ids** while they fit in 32 bits and about **1.05 million** once they need BSON int64. On a
7,244-sample real corpus the longest posting list across all 20 bands held 36,183 ids, so
extrapolating it linearly puts the ceiling near **270,000 samples**.

What happens there is not gradual: `$push` raises `BSONObj size ... is invalid` and the write
fails, so indexing stops for any sample containing a function whose band hash is already at the
cap. **Adding machines does not help** - a document cannot span shards, so this is not something
sharding fixes.

    STORAGE_BAND_BUCKET_SIZE = 100000

splits a hash across `(band_hash, bucket)` documents once it would exceed that. `0` (the default)
keeps the single-document shape. Set it comfortably above `STORAGE_BAND_DF_CUTOFF`: the cutoff
selects hashes by the total `df` stored on bucket 0, and that stays exact only while an
under-cutoff posting list still fits in a single bucket. At the suggested values (100,000 against
a cutoff of 200) there is a 500x margin.

**Enabling it on an existing database requires a rebuild first:**

    curl http://localhost:8000/rebuild_band_df_index

Documents written before the knob was on have no `bucket` field, so the upsert filter
`{band_hash, bucket: 0}` will not match them - it would insert a second document for the hash and
split the posting list invisibly, which no error would report. The rebuild stamps `bucket: 0` and
is what makes them addressable. Run it after setting the knob and before the next ingest.
**Switching it back to `0` on a database that has spilled is not supported:** the single-document
code paths neither read nor maintain the buckets above 0, so deletions and lookups made that way
leave them out of step with bucket 0.

Matching results are unchanged with it on or off; the tests assert identical matches against a
corpus where the split is forced.

The corpus that hits this ceiling depends on more than sample count. Malpedia is curated and
deduplicated; a collection carrying many near-duplicate packed variants concentrates `df` faster
and would reach the cap sooner. `df` on bucket 0 is worth watching:

    db.band_0.find({}, {band_hash: 1, df: 1}).sort({df: -1}).limit(5)

## Rebuilding the PicHash counts on a large corpus: `STORAGE_REBUILD_PARTITION_SIZE`

This one is about an offline operation, not about query latency. All the indexes are maintained
incrementally on write; a full rebuild is what you run after a bulk import, a schema change, or
a repair. On a large corpus it is the operation that takes longest, and it is the one you are
running when something is already wrong.

    STORAGE_REBUILD_PARTITION_SIZE = 500000

`0` (the default) keeps the original rebuild: one server-side `$group` over every pichash, then
one upsert per distinct hash. A positive value switches to a partitioned scan that reads the
`_pichash` index in slices of that many keys, counts runs of equal keys as it goes, and writes
the counts as plain inserts in ascending key order.

**The result is identical, and checked rather than assumed.** The rebuild verifies the holders
it counted against an independent count of the functions carrying a pichash, and falls back to
the original implementation if they disagree. The test suite compares the full
`_pichash -> df` map produced by both implementations, entry for entry, at partition sizes small
enough that boundary cases actually occur.

Measured on corpora projected from a 7,244-sample real corpus, three repeats, medians:

| samples | distinct hashes | default (`0`) | at `500000` | speedup |
|---|---|---|---|---|
| 1,000 | 393,858 | 51.9 s | 10.8 s | 4.81x |
| 2,000 | 722,815 | 91.2 s | 20.8 s | 4.38x |
| 4,000 | 1,486,935 | 188.8 s | 43.6 s | 4.33x |
| 7,244 | 2,337,173 | 301.6 s | 73.1 s | 4.12x |

Where the time goes: at the largest size the original spends 32.6 s producing the counts and
269.0 s writing them, because upserts in the group's output order land at random positions in a
growing index (8,690/s against 38,765/s for ascending inserts). The partitioned scan also holds
its intermediate state in two local variables instead of a table with one entry per distinct
hash, which is what makes the rebuild's memory independent of the corpus - the original's
accumulator crosses MongoDB's 100 MB `$group` limit between 1,000 and 2,000 samples on this
corpus and spills to disk from there on (4 spills, 36.7 MB at 7,244 samples).

**What the measurement does not show.** It was taken on corpora reduced to the single field the
rebuild reads, so they fit in the WiredTiger cache where a full corpus of that size does not.
Both implementations measured *linear* there, against an earlier full-fidelity measurement of the
original rebuild that had it growing superlinearly (437.1 s at 7,244 samples against 211.9 s at
5,243). Treat the 4.1x as solid and the scaling behaviour as unsettled: on a corpus large enough
to leave cache, the gap is expected to be wider, not narrower, but that has not been measured.
The full accounting - the harness, the raw numbers and the write-up - is on the
`research/scaling-notes` branch under `docs/scaling/`.

Sizing the knob: 500,000 keys is roughly 40 MB of BSON in flight per partition, and few enough
partitions that the per-partition round trip is noise. Lower it if the rebuild shares a small
machine; raising it buys nothing once the round trip has stopped mattering.

## Caveats

* Constants are measured on one corpus and one host. The relationships generalise; the specific
  numbers are worth re-checking if your corpus differs substantially in size or in family
  composition.
* `STORAGE_CANDIDATE_ACCUMULATION`, `MINHASH_MATCHING_VECTORIZED`, `STORAGE_CACHE_FETCH_THREADS`
  and `STORAGE_MATCHING_CACHE_PERSIST` shipped opt-in (default off) in MCRIT 1.6.0 and are on by
  default from 1.6.1. `MINHASH_MATCHING_MAX_PAIRS` is 1.6.2 and later. `STORAGE_MATCHING_CACHE_MAX_BYTES` is 1.6.1 and later; on 1.6.0 size the
  cache with `STORAGE_MATCHING_CACHE_MAX_ENTRIES` instead, at ~314 B per entry. On 1.5.3 and
  earlier only the batch size, `BAND_MATCHES_REQUIRED` and the mongod cache size apply.
* All measurements used single-process matching; comparisons against a pooled configuration
  will differ.
* The two-stage numbers come from a different, smaller campaign than the rest of this document:
  257 real Malpedia samples grown to 12,500 with synthetic samples drawn from a process fitted
  to that corpus (functions-per-sample from its empirical distribution, signatures from a
  preferential-attachment urn calibrated to its measured Heaps exponent). They were measured on
  a 4-core / 16 GiB host with mongod 7.0. The shape of the result - one-stage grows with the
  corpus, two-stage does not - is the finding; the absolute seconds are host-specific.
