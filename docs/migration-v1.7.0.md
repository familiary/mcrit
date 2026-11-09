# MCRIT Migration Guide for v1.7.0

With v1.7.0, disassembly moves out of the function documents. Until now every entry in the
`functions` collection carried its own `_xcfg` blob inline, which meant that every query touching a
function - fetching a MinHash, filtering by PicHash, assembling a match report - had to read past
that blob to reach the few fields it actually wanted. On a corpus of 11.6M functions that is 37.6 GB
of disassembly, and a copy of `functions` written without it measures 3,130 MB against 18,353 MB
with it - so the hot path was reading roughly six times more data than it needed.

The blobs now live in their own `xcfg` collection (and `query_xcfg` for query functions), keyed by
function id. Matching results are unchanged - the release was verified to reproduce recorded
reference digests byte-for-byte on both the old and the new shape.

**You do not have to migrate immediately.** Every reader falls back to an inline `_xcfg` when no blob
document exists, so v1.7.0 serves correctly on an un-migrated database. Upgrading the code and
completing the split are separate decisions; take them on separate days if that suits you.

## Before you start

Check the disk first. An in-place migration *adds* the `xcfg` collection while MongoDB keeps the
space freed inside `functions`, so it temporarily needs room for a second copy of the disassembly:

| | before | after in-place migration |
|---|---|---|
| `functions` | 18,353 MB | 18,521 MB (unchanged) |
| `xcfg` | - | 13,598 MB |
| free disk | 51 GB | 36 GB |

Reserve roughly the size of your disassembly, and see [Reclaiming the space](#reclaiming-the-space)
below for how to get it back afterwards.

For reference, the same corpus migrated in **16.6 minutes** at about 11,700 functions per second.
The migration is resumable, so an interrupted run costs only the batch in flight, and re-running it
after completion does nothing.

## Which path: do you need byte-for-byte proof?

This is the one decision that cannot be revisited later. An in-place migration writes each blob and
then unsets it from the function document, so once it has run **the original inline blobs are gone**
and no tool can compare old against new content any more. Verification afterwards can confirm that
every function has a blob and that the counts line up, but not that the bytes are identical.

If that proof matters for your deployment, rehearse into a second database first (or take a dump).
If it does not - the migration is resumable, digest-verified and reversible - go straight to
[in place](#migrating-in-place).

## Rehearsing first

`copy` mode reads your database **read-only** and writes the migrated shape into a separate target,
so you can verify the result and even run matching against it before touching production:

```bash
$ python -m mcrit.migrations.migrate_xcfg_split --mode copy --target mcrit_rehearsal
$ python -m mcrit.migrations.migrate_xcfg_split --mode verify --target mcrit_rehearsal
```

Because the source still holds its blobs, `verify` here compares the actual bytes for a sample of
functions and reports any that differ or are missing. This is the only configuration in which it can
do so.

Note that a freshly written collection is also where the "collection shrinks from 18,353 MB to
3,130 MB" figure comes from: the copy target is compact because it was written from scratch. An
in-place migration reaches the same document shape but not the same file size - see below.

## Migrating in place

```bash
$ python -m mcrit.migrations.migrate_xcfg_split --mode inplace
```

Each batch writes its blob documents first and only unsets `_xcfg` from the function documents whose
blobs are confirmed present, so an interruption at any point leaves the database readable - functions
either have their blob or still have their inline copy, and the readers handle both.

You can watch progress from the outside; the `xcfg` collection grows towards the number of functions
in your database:

```bash
$ python -c "from mcrit.client.McritClient import McritClient; print(McritClient().getStatus())"
```

`/status` reports `inline_xcfg_remaining`, so a half-migrated instance says so rather than quietly
staying on the old shape. Once the migration completes it answers from a marker the migration writes,
so this stays cheap to poll.

## Verifying afterwards

```bash
$ python -m mcrit.migrations.migrate_xcfg_split --mode verify
```

After an in-place run this reports coverage and counts rather than content, and labels itself
accordingly - `checked: 0` together with an `established` field explaining that the source no longer
holds blobs to compare. What it does establish is that every sampled function has a blob document and
that `functions`, the migrated function count and the blob count all agree. On the reference corpus
that came out at zero gaps across 200,000 sampled ids and 11,665,054 documents on all three counts.

If you want content-level proof after the fact, verify a dump taken before the migration against the
migrated database.

## Reclaiming the space

MongoDB does not return the space freed by unsetting a field to the operating system; the extents stay
inside the collection file and get reused by later writes. The performance benefit does not depend on
this - documents are narrow now, so queries read less regardless - but the file will not shrink until
you compact it:

```bash
$ mongosh mcrit --eval 'db.runCommand({compact: "functions"})'
```

Check your MongoDB version's documentation before running this: `compact` blocks operations on the
collection in some versions.

## Rolling back

`unsplit` writes the blobs back into the function documents and removes the `xcfg` collection once no
blob is left orphaned:

```bash
$ python -m mcrit.migrations.migrate_xcfg_split --mode unsplit
```

**Run this before downgrading MCRIT.** Older versions do not know about the `xcfg` collection, and
they fail quietly rather than loudly: the instance starts up normally and only then behaves as though
every function had no disassembly - MinHash recalculation errors out, match reports lose their code
references, and unique-block queries come back empty. The database is not damaged, but a downgrade
without `unsplit` looks healthy while being broken.

## A note on STORAGE_DROP_DISASSEMBLY

With the blobs in their own collection, `STORAGE_DROP_DISASSEMBLY` genuinely reclaims space - it
drops the collection, where blanking the field in place did not.

Consider what you give up, though. Stored disassembly is what makes it possible to recalculate
MinHashes without re-submitting samples, and that matters more than it sounds: when SMDA's
instruction escaping changes, previously stored MinHashes stop being comparable to newly computed
ones (v1.7.0 adds an escaper fingerprint to `/status` and to exports so you can detect this). With
the disassembly present, repairing an 11.6M-function index is a few core-hours of local computation.
Without it, the only route is re-submitting every sample.

## Checking a corpus for older data

Later releases stopped letting in records that older ones accepted (#265, #266, #270): reports with
a `statistics.num_functions` that is not their function count, MinHashes of another signature
length, imports attaching functions to the wrong sample. Upgrading does not touch what is already
stored. A read-only check reports it:

```bash
$ python -m mcrit.migrations.verify_corpus --out corpus_check.json
```

It writes nothing, prints JSON and exits with 1 when it finds a problem, like the migration's verify
mode. It lists samples whose stored functions do not number their statistics, functions without a
sample or of another family than their sample's, samples holding MinHashes of the wrong length,
stored binaries whose sha256 is not their sample's (`--hash-binaries` hashes the bytes instead of
trusting the recorded hash), family counters that disagree with samples and functions,
replacements an upload left unfinished, deletions that died after claiming their sample, and
documents in `functions_staging`, `xcfg_staging` or `smda_extras_staging` under a staging id no
sample records. It also counts samples by `sha256_verified`. Each check gives a complete count and
the first 20 instances (`--max-examples`).

`sha256_verified: true` says that the sample's stored analysis is MCRIT's own disassembly of bytes
with that sha256, from a `POST /samples/binary` upload; it vouches for nothing the sample says about
itself, such as family, version or filename. A sample indexed from a report or an import that reads
`false` or `null` gets there when its binary is uploaded: the upload replaces its analysis in place.

Family counters are fixed by `POST /recompute_family_stats`, MinHashes by `/repair_minhashes` or a
re-submission of the sample. An unfinished replacement is a job that died between writing the
new functions under the sample and removing the old ones, so the sample holds both function sets;
the next upload of that sample's binary finishes it. An unfinished deletion is finished by
deleting the sample again. Orphaned staging documents can be deleted by their `staging_id`.
Functions without a sample can also be left by a deletion that landed while a replacement copied
its new functions in, with the replacement's job dying right after. The rest needs a look at the
samples named.

The check is one aggregation over `functions`, projected to the three fields it counts, and
projected scans of the other collections. It never reads disassembly, and its memory grows with
the number of samples, not functions.
