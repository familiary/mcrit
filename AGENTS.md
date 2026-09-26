# AGENTS.md — MCRIT (core package)

MCRIT (MinHash-based Code Relationship & Investigation Toolkit) is a distributed framework for binary code similarity analysis. This repository is the **core `mcrit` package**: the REST server, the async worker, the Python client, and the `mcrit` CLI. It indexes and matches disassembled code using SMDA, storing results in MongoDB (or in-memory for development).

For the full methodology (PicHash/MinHash, LSH banding, case studies) see [`README.md`](README.md) and the [deepwiki overview](https://deepwiki.com/danielplohmann/mcrit/).

## Repository layout

- `mcrit/` — package source.
  - `mcrit/server/` — Falcon REST API (`application_routes.py`, `*Resource.py`).
  - `mcrit/storage/` — data model and storage backends (`MongoDbStorage`, `MemoryStorage`).
  - `mcrit/queue/` — job queue (`LocalQueue`, `MongoQueue`) and RPC plumbing.
  - `mcrit/index/` — `MinHashIndex` facade plus the search query parser/tree and pagination cursor.
  - `mcrit/matchers/` — `MatcherInterface` and its per-scope subclasses (sample, query, cross, vs, vs-group).
  - `mcrit/minhash/` — `MinHash`, `MinHasher`, and the `ShingleLoader`.
  - `mcrit/shinglers/` — feature extractors, discovered by directory scan (`archived/` excluded).
  - `mcrit/config/` — `McritConfig` and the per-area config dataclasses.
  - `mcrit/client/` — `McritClient` (Python API) and `McritConsole` (the `mcrit client` CLI).
  - `mcrit/libs/` — helpers (parallel processing, graph, hashing).
  - `Worker.py`, `__main__.py` — worker process and CLI entry point.
- `tests/` — pytest suite (unit + integration).
- `docs/` — CLI docs, migration guides.
- `examples/` — auxiliary scripts. (`experiments/`, `diagnosis/`, `data/` are gitignored local working directories and are not part of a fresh checkout.)
- `pyproject.toml` — the single source for build metadata, dependencies, and `ruff` / `ty` / `pytest` / `coverage` configuration. There are no `requirements*.txt` files.
- `CHANGELOG.md` — the release history, newest first. It used to live in `README.md`, which now only links here. `MANIFEST.in` exists solely to keep it in the sdist.

`docs/TUNING.md` is mirrored verbatim into the [docker-mcrit](https://github.com/danielplohmann/docker-mcrit)
deployment repository. This repository holds the canonical copy, because the document describes the
defaults in `MinHashConfig` and `StorageConfig`: changing one of those defaults, or adding a knob,
should update `docs/TUNING.md` in the same commit, then be copied downstream. Never edit the
docker-mcrit copy directly - it silently diverged that way once already, and a release shipped tuning
advice that had stopped being true.

## Development setup

Requires **Python 3.12 or newer** (`pyproject.toml` sets `requires-python = ">=3.12"` with no upper bound). CI exercises 3.12 through 3.14.

```bash
pip install -e ".[dev]"
```

MongoDB 5.0+ is the recommended persistent backend (CI runs the integration suite against `mongo:8.0`), but the in-memory storage works for development without a database. The mongo-backed tests read the server from `TEST_MONGODB` and fall back to `127.0.0.1:27017`; the variable takes a bare host or a `host:port`, resolved for every one of them through `getTestMongoServerAndPort()` in `tests/context.py` - use that helper in a new mongo-backed test rather than reading the environment again.

## Common commands

Lint (run before considering work done):

```bash
ruff format --check .
ruff check .
```

Unit tests (no database required):

```bash
python -m pytest -m "not mongo and not sleep"
```

Full/integration tests (requires a running MongoDB at `127.0.0.1:27017`):

```bash
python -m pytest
```

If that MongoDB runs in a container, raise its open-file limit:

```bash
docker run -d --rm --name mcrit-mongo --ulimit nofile=200000:200000 -p 27017:27017 mongo:5.0
```

Pick 5.0 or 7.0 rather than 8.0 for a local container: mongod 8.0 refuses to start on Linux kernels
6.19 and newer (`MongoDB cannot start: ... known incompatibility`, jira SERVER-121912) and the
container exits immediately. CI is unaffected, its runners are on older kernels.

The suite creates and drops the band collections repeatedly, which opens enough WiredTiger files
to exceed docker's default `nofile`. mongod then hits EMFILE, panics and aborts mid-run, and every
test after that fails on a connection error - so the run looks like a dozen broken tests rather
than a dead database. The container log is the only place that says otherwise (`WT_PANIC`,
`Too many open files`). Drop the test databases between runs too: leftover collections from an
aborted run make the next one fail on state that no test created.

Run the backend (separate shells):

```bash
mcrit server     # REST API on http://127.0.0.1:8000/
mcrit worker     # processes queued jobs
```

Interact via the CLI:

```bash
mcrit client status
mcrit client submit <file> -f <family_name>
```

## Architecture primer

- **Server** (Falcon): synchronous endpoints for queries (families/samples/functions); long-running operations (disassembly, indexing, matching) create async **jobs**.
- **Worker + Queue**: jobs are pulled from a queue (`LocalQueue` in-process, `MongoQueue` in production) and executed by one or more workers.
- **Storage**: `MongoDbStorage` (persistent) or `MemoryStorage` (dev). Holds `FamilyEntry`, `SampleEntry`, `FunctionEntry`.
- **Matching**: `PicHash` / `PicBlockHash` give exact (position-independent) matches; `MinHash` signatures + LSH **banding** give fuzzy similarity. Feature extraction is done by `AbstractShingler` implementations.

## Key concepts

- **PicHash / PicBlockHash** — position-independent hashes (function- and basic-block-level). The hash comparison is exact; the code match it implies is quasi-exact, since identical position-independent bytes do not guarantee the same function.
- **MinHash signature** — fuzzy similarity estimate derived from shingled code features.
- **Shingler** — encodes properties of a disassembled function into a feature set for MinHashing.
- **Band** — LSH band used for candidate generation during matching.
- **Family / Sample / Function** — the three-tier storage hierarchy.
- **Job** — async unit of work (disassemble, index, match, cross-compare, unique-blocks).

## Code conventions

- Lint/format: `ruff` (line-length 180, `target-version = "py311"`, selects `E4/E7/E9/F/I/UP`). Run `ruff format .` to auto-format. Note most `UP` rules are explicitly ignored in `[tool.ruff.lint]`, so the existing `Dict`/`Optional` typing style is intentional — do not "modernize" it.
- Type checking: `ty` (`make typecheck` / `ty check`), enforced in CI next to ruff and currently at **zero diagnostics**. There are no suppressions in the tree — fix the code or the annotation instead of adding `ty: ignore`.
- Supported Python: 3.12+ (`requires-python = ">=3.12"`).
- License: GPL-3.0-only. The version is bumped manually in **three** places that must agree — `pyproject.toml`, `McritConfig.VERSION` (served by the `/version` endpoint), and the newest entry in `CHANGELOG.md` — do not change unless asked. `tests/testReleaseGuard.py` asserts the first two agree, and the release workflow refuses a tag that does not match all three. Every PR that changes `mcrit/` adds an entry under `## [Unreleased]` or carries the `no-changelog` label. How a release is cut is in [`RELEASING.md`](RELEASING.md).
- Never introduce or log secrets/API tokens/keys.
- A change that alters what a match report, cross compare or unique-blocks result holds for the same corpus and parameters bumps `RESULTS_VERSION` in `mcrit/Worker.py`, so cached jobs computed before it are not handed out again (#241).

## Agent guardrails

- **Never** run `git commit`, `git push`, or open a PR unless explicitly instructed.
- **Do not** change MinHash/PicHash/shingler configuration or version numbers without explicit instruction — such changes require a full re-index of the database.
- Integration tests require a running MongoDB (`mongo:8.0` in CI); the unit suite runs without one.
- Always run `ruff format`/`ruff check`, `ty check`, and the unit test suite before considering work complete.

## Related repositories (reference only)

- [mcrit-web](https://github.com/fkie-cad/mcrit-web) — Flask browser front-end + user management.
- [docker-mcrit](https://github.com/danielplohmann/docker-mcrit) — containerized full-stack deployment (incl. NGINX).
- [mcrit-plugins](https://github.com/danielplohmann/mcrit-plugin) — IDA Pro integration plugin.
- [mcrit-data](https://github.com/danielplohmann/mcrit-data) — ready-to-use reference data for common compilers/libraries.
