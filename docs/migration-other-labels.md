# Plain search terms and function labels (#301)

A plain function search term matches a function's name and every label that differs from it, so a
function renamed by a user is still found by the name it had, and one labelled from a report by
that label. On MongoDB this reads a derived field of each function document, `_other_labels`: the
distinct labels other than the current `function_name`, absent when there are none, with a sparse
index. It is storage-only and never part of an API answer or an export. The memory storage
derives the same labels on the fly.

Every write since this change keeps the field current: a rename, a label added by a rename or a
submitted report, and an import. What is left is the functions stored before it.

## On upgrade

The first start fills the field by itself when the database has at most 100,000 labelled functions
(`MongoDbStorage._OTHER_LABELS_STARTUP_MAX`): one server-side update over those functions, 0.74 s
for 87,700 labelled of 250,000 functions here. Most corpora have that many labelled functions only
because an import or a report labels each named function with its own name; those labels are no
other labels, and only the renamed or user-labelled functions get the field.

A larger database starts without filling it and logs that the migration is due, once an hour at
most: the verdict is kept in `settings` (`other_labels_due_at`), so neither the other processes
nor the one SpawningWorker starts for each job count the labelled functions again meanwhile. Until
the migration has run, a plain term finds a function by a label other than its name only when that
function was renamed or labelled since the upgrade; `function_labels.function_label:?term` finds
all of them as before. MCRIT keeps serving meanwhile, and the migration can run alongside it.

A fill that fails, such as one hitting a document MongoDB cannot let grow, keeps what it wrote
before the failure, is logged as a pointer to `migrate_other_labels --mode apply`, and counts as
the migration being due: no start tries again for an hour. The rest of the startup is done.
While a function the migration cannot update either remains, the database stays due, so a start
tries the fill again about once an hour, and logs it again. The hour is measured against
`other_labels_due_at` on each host's clock, so hosts whose clocks differ by more may wait longer.

## Migrating

```
$ python -m mcrit.migrations.migrate_other_labels --mode dry-run
$ python -m mcrit.migrations.migrate_other_labels --mode apply
$ python -m mcrit.migrations.migrate_other_labels --mode verify
```

`--host`, `--port` and `--db` default to MCRIT's storage configuration.

- `dry-run` writes nothing: it counts the labelled functions and how many would change.
- `apply` writes the field where it is missing or wrong, in batches of `--batch` (2,000) functions
  in function id order. Each batch reads the labelled functions only, without their disassembly,
  and holds one batch in memory. A function is updated from its document as it stands at the
  write, the same update MCRIT's own writes run, so a rename meanwhile is never overwritten. The
  last function id done is kept in the `other_labels_migration_state` collection: a run that is
  stopped, or cut short with `--limit`, resumes there. A finished run records the database as
  complete (`other_labels_complete` in `settings`), after which startup no longer checks, and
  drops its progress. A function the update fails for (one whose document would grow past
  16 MiB) is passed over and reported with the run's result, which then exits 1 and does not
  record the database as complete, as after a write concern error; the next run examines
  everything again and gets past it too.
- `verify` writes nothing: it compares every labelled function's field with its labels and name,
  checks that no function without labels carries one, and exits 1 on a difference.

The server walks the function id index, so it reads every function document once; it returns and
writes only labelled ones. Over 250,000 functions here: 1.1 s for 87,700 labelled functions, 500 of
them changed; 4.0 s for 100,500 labelled functions, 22,000 of them changed.

A re-run of `apply` examines everything again. Run it when a process of an older MCRIT, which does
not keep the field, renamed or labelled functions after the upgrade.

## Cost of a search

A plain term matches its terms against the list of other labels and looks up the functions with a
matching one apart from the rest, whose query is the one a plain term had before. Each process
lists the other labels (a `distinct` over their index) once, and keeps the list and which of its
labels each of the last 256 terms matches until a write changes them: every such write, in any
process, increments `other_labels_version` in `settings`, and a search reads only that counter.

Over 250,000 functions with about 500 other labels that adds 1-2 ms to a search of 20-35 ms when no
other label matches the term, and 4-12 ms when some 200 do, mostly fetching the functions found by
such a label. With 22,000 other labels a term few of them match costs 0-6 ms more, and one that
thousands match 9-26 ms: the query carries those labels, and fetches the functions they find.
