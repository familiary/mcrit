# Development notes

## Archived shinglers

`mcrit/shinglers/archived/` is excluded from normal `ShingleLoader` discovery. Its implementations
remain as historical experiments and compatibility references rather than maintained feature
extractors. `NgramShingler` uses fixed three-instruction windows without a sequence cap; the
tree-trace shingler retains its experimental three-level traversal encoding. Changing either
feature set changes generated MinHashes and requires an explicit decision to revive the shingler,
including a reindexing plan. No algorithm changes are implied by the comments in these files.

## Storage API naming

`FunctionEntry.minhash` remains named as-is. Renaming it would affect serialized field names and
callers while providing little benefit, since its annotation already states that it stores bytes.
Likewise, `createMatchingCache` remains the storage factory method name; renaming it would add API
churn without changing its behavior. `clearMatchingCache` remains a compatibility hook because
matching caches are owned by matcher instances and storage backends have nothing to clear.
