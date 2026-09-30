# Releasing MCRIT

This repository follows the release process shared across the MCRIT ecosystem
([smda](https://github.com/danielplohmann/smda), [purepdb](https://github.com/danielplohmann/purepdb),
[mcrit](https://github.com/familiary/mcrit), [mcritweb](https://github.com/familiary/mcritweb),
[mcrit-plugin](https://github.com/familiary/mcrit-plugin),
[docker-mcrit](https://github.com/familiary/docker-mcrit)). The shape is the same everywhere;
this file states the values that are specific to this repository.

## Versioning

MCRIT follows [Semantic Versioning 2.0.0](https://semver.org/spec/v2.0.0.html) over the REST API, the
configuration surface and the stored data shape; see the note at the top of `CHANGELOG.md` for what
that does and does not cover. A release that changes the database shape ships a migration guide under
`docs/` and says so in its `Upgrading` notes, and `docker-mcrit` is bumped to it afterwards.

The version is declared in `pyproject.toml` (`[project].version`) and `mcrit/config/McritConfig.py` (`VERSION`, served by
`/version`). The release workflow refuses a tag that does not
match every one of them, so a bump that misses one fails before anything is published.

## Changelog

`CHANGELOG.md` follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). It is the one
authoritative record of what a release contains: the GitHub release notes are generated from it, and
nothing is written twice.

- Every pull request that changes something a user can observe adds its own bullet under
  `## [Unreleased]`, in the subsection it belongs to (`Added`, `Changed`, `Deprecated`, `Removed`,
  `Fixed`, `Security`), while the change is fresh. The `Changelog` check fails a PR that touches
  shipped files without touching `CHANGELOG.md`; apply the `no-changelog` label when a change
  genuinely needs no entry (a typo, a CI-only change), and say why in the PR.
- An entry says what changed and what it costs the reader: what to do when upgrading, what may
  behave differently, which issue or PR it closes.
- `.gitattributes` merges `CHANGELOG.md` with `merge=union`. Every open pull request appends to the
  same `[Unreleased]` section, so without it the PRs in a batch conflict with each other rather than
  with `main`. Union keeps both sides' lines, which is safe for this file because entries are only
  ever appended, never edited in place; it would garble two sides rewriting the same line, which a
  changelog does not do. Keep the line; git applies it to local merges, so a batch that GitHub
  reports as conflicting can be merged locally and pushed.
- Dependency bumps need no entry. GitHub lists them under their own heading in the release notes,
  from the `dependencies` / `github_actions` labels (`.github/release.yml`).

## Cutting a release

1. Check that `main` is green and that everything meant for the release has merged.
2. In one commit on a branch, then merged through a PR:
   - set the new version in `pyproject.toml` (`[project].version`) and `mcrit/config/McritConfig.py` (`VERSION`, served by
`/version`);
   - in `CHANGELOG.md`, rename `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD`, drop the empty
     subsections, open a fresh empty `## [Unreleased]` above it, and update the compare links at
     the foot of the file.
3. Wait for CI to pass on the merge commit. Then tag that commit and push the tag:

   ```bash
   git tag -a vX.Y.Z -m "MCRIT X.Y.Z"
   git push origin vX.Y.Z
   ```

Pushing the tag is the release. `.github/workflows/publish-release.yml` then:

1. **Verify** — refuses to continue unless the tag matches both version strings, `CHANGELOG.md` has a
   `## [X.Y.Z] - <date>` section (which becomes the release notes), the tagged commit is on `main`,
   and CI passed on that commit.
2. **Build** — builds the sdist and wheel in an isolated environment, checks their metadata with
   `twine check --strict`, and installs the wheel into a clean virtual environment carrying only the
   declared runtime dependencies to import it, check the version it serves, check the package data
   shipped, and run the `mcrit` entry point.
3. **Publish** — uploads to PyPI through [trusted publishing](https://docs.pypi.org/trusted-publishers/)
   with signed provenance attestations. No API token is stored anywhere.
4. **Release** — creates the GitHub release for the tag with the changelog section as its body,
   GitHub's generated contributor and PR list appended under it, and the sdist and wheel attached.

Each gate fails with a message naming what to fix. Nothing has to be remembered at the console.

## Pre-releases

A release candidate is tagged `vX.Y.Zrc1` (also `a1`, `b1`), with the same version string in
`pyproject.toml` (`[project].version`) and `mcrit/config/McritConfig.py` (`VERSION`, served by
`/version`) and a `## [X.Y.Zrc1] - YYYY-MM-DD` changelog section. The workflow marks the
GitHub release as a pre-release and does not make it "latest". PyPI
does not install a pre-release unless it is asked for explicitly (`pip install --pre`), and
`docker-mcrit` pins exact versions, so a candidate never reaches a deployment by accident.

## Rehearsing

Run *Publish release* manually from the Actions tab, choosing a tag as the ref. A manual run goes
through the same gates and build, publishes to [TestPyPI](https://test.pypi.org/p/mcrit) instead of
PyPI (an already-present version is skipped rather than failed), and stops before creating the
GitHub release. Rehearse the first release after any change to the workflow.

## When a release fails

- **A gate failed before anything was published** (tag/version mismatch, missing changelog section,
  tag not on `main`, CI not green): fix the cause on `main`, delete the
  tag locally and on the remote (`git push --delete origin vX.Y.Z`), and tag again once the fix has
  merged. Nothing needs cleaning up.
- **Publishing to PyPI failed part-way**: a version number on PyPI is permanent even when yanked,
  so do not try to reuse it. Fix the cause, bump the patch version, and release again. Yank the
  incomplete version on PyPI if any of its files were accepted.
- **The GitHub release step failed after publishing**: re-run only the failed job from the Actions
  UI; the built artifacts are kept as workflow artifacts and the step is idempotent.

## Maintainer configuration

Done once, by a repository owner; the workflow cannot create these for itself.

- **PyPI trusted publisher** for the `mcrit` project: owner `familiary`, repository `mcrit`,
  workflow `publish-release.yml`, environment `pypi`. Add the same publisher on TestPyPI with
  environment `testpypi` to enable rehearsals. TestPyPI has no `mcrit` project yet, so there it has
  to be created as a [pending publisher](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/),
  which creates the project on the first rehearsal; an ordinary publisher cannot be added to a
  project that does not exist, and without one a rehearsal fails at the publish job.
- **GitHub environments** `pypi` and `testpypi` (Settings → Environments). Restricting `pypi` to
  the `v*` tag pattern and requiring a reviewer is recommended: it makes the publish step a
  deliberate click even if a tag is pushed by mistake.
- **Label** `no-changelog`, used by the changelog check.
- Optionally, **immutable releases** (Settings → General → Releases), so a published release's
  assets and tag can no longer be changed.

## Release order across the ecosystem

MCRIT depends on `smda` (`>=4.2.13`) and is depended on by `mcritweb` (`>=1.5.3`), by
`mcrit-plugin` (which vendors a minimal client rather than importing the package), and by
`docker-mcrit`, which pins an exact MCRIT and MCRITweb version in its `.env`. When a change here
needs a newer smda, release smda first and raise the floor in `pyproject.toml`; when a change
here is needed by mcritweb, release MCRIT first, then mcritweb with its floor raised, then bump
`docker-mcrit`.

## Releases before v1.10.0

GitHub releases stopped at v1.3.0 while the tags carried on, and `publish-release.yml` only makes
releases from v1.10.0 on (#192). `.github/workflows/scripts/backfill_releases.py` fills the gap once,
with the notes of each version taken from `CHANGELOG.md`. Run it from a full clone (not a shallow
one) with `gh` authenticated as a maintainer:

```bash
python .github/workflows/scripts/backfill_releases.py            # prints what it would create
python .github/workflows/scripts/backfill_releases.py --apply    # creates the releases
```

The dry run prints each `gh release create` command in full, notes included, exactly as `--apply`
runs it. It skips versions that already have a release, never marks one as latest, and creates no
tags: a version in the changelog without a tag (v1.4.4 to v1.4.7) is listed with the commit that
set it, its date, and a warning where that date is not the changelog's, to be tagged there by hand
or left out. None of the four was ever published to PyPI, which goes from 1.4.3 to 1.5.0; v1.4.4's
own commit says it changed nothing and was not tagged on purpose, and the commit that set v1.4.6
is dated three months after the changelog's date for it and also carries the move to ruff.
