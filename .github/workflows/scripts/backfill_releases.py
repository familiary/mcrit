"""Create the GitHub releases that were never made for versions already tagged (#192).

Releases stopped at v1.3.0 while tags carried on, so the releases page named a version years out
of date. publish-release.yml makes every release from v1.10.0 on; this fills the gap before it,
once, with notes lifted from CHANGELOG.md - a `## [X.Y.Z] - date` section where the version has
one, otherwise its ` * date vX.Y.Z: ...` entry under "Older releases".

It prints the `gh` commands and runs nothing unless given --apply. A version whose tag does not
exist is reported with the commit that set it as VERSION, for a maintainer to tag or skip; tags
are never created here. None of the releases is marked latest, which stays with the newest one.
"""

import argparse
import importlib.util
import re
import shlex
import subprocess
import sys
from pathlib import Path

#: ` * 2026-08-25 v1.8.1:  text` - one version per bullet under "Older releases"
OLDER_ENTRY = re.compile(r"^ \* (?P<date>\d{4}-\d{2}-\d{2}) v(?P<version>\d+\.\d+\.\d+):\s+(?P<notes>.+)$")
#: `[#142]: https://...` - a reference-style link definition, kept at the foot of the changelog
LINK_DEFINITION = re.compile(r"^\[(?P<label>[^\]]+)\]:\s+\S+")
SCRIPTS = Path(__file__).resolve().parent


def _releaseGuard():
    spec = importlib.util.spec_from_file_location("release_guard", SCRIPTS / "release_guard.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def versionKey(version: str):
    return tuple(int(part) for part in version.split("."))


def _entries(changelog: str):
    """(version, date, notes) for every release: its section, or its older one-line entry."""
    guard = _releaseGuard()
    found = {}
    for line in changelog.splitlines():
        entry = OLDER_ENTRY.match(line)
        if entry and entry.group("version") not in found:
            found[entry.group("version")] = (entry.group("date"), entry.group("notes").strip())
    for line in changelog.splitlines():
        section = guard.SECTION.match(line)
        if section and guard.VERSION.match(section.group("version")):
            found[section.group("version")] = (section.group("date"), guard.changelogSection(changelog, section.group("version")))
    return found


def changelogDates(changelog: str) -> dict:
    """The release date the changelog gives each version."""
    return {version: date for version, (date, _) in _entries(changelog).items()}


def changelogNotes(changelog: str) -> dict:
    """Release notes by version: the section where there is one, the older one-line entry otherwise.

    Reference-style links (`[#142]`) are defined once at the foot of the changelog; the ones a
    version's notes use go along with them, or the release would show the bare label - which
    GitHub then links to an issue of whatever repository it is in, not the one meant.
    """
    definitions = {match.group("label"): line for line in changelog.splitlines() if (match := LINK_DEFINITION.match(line))}
    notes = {}
    for version, (_, text) in _entries(changelog).items():
        used = [line for label, line in definitions.items() if f"[{label}]" in text and line not in text]
        notes[version] = text + "\n\n" + "\n".join(used) if used else text
    return notes


def plan(notes: dict, tags: set, releases: set, since: str, until: str) -> tuple:
    """Which versions get a release, and which cannot because their tag is missing."""
    wanted = sorted((version for version in notes if versionKey(since) <= versionKey(version) <= versionKey(until)), key=versionKey)
    to_release = [version for version in wanted if f"v{version}" in tags and f"v{version}" not in releases]
    untagged = [version for version in wanted if f"v{version}" not in tags]
    return to_release, untagged


def _lines(command: list) -> list:
    return subprocess.run(command, check=True, capture_output=True, text=True).stdout.split()


def bumpCommit(root: Path, version: str, changelog_date: str = "") -> str:
    """The oldest commit that set McritConfig.VERSION to this version, as a candidate for its tag.

    Said with its date and subject, and flagged when the date is not the one the changelog gives:
    a version bumped as part of a larger change would carry that change in its tag as well.
    """
    commits = _lines(["git", "-C", str(root), "log", "--format=%h", "-S", f'VERSION = "{version}"', "--", "mcrit/config/McritConfig.py"])
    if not commits:
        shallow = _lines(["git", "-C", str(root), "rev-parse", "--is-shallow-repository"]) == ["true"]
        return "an unknown commit (this clone is shallow; `git fetch --unshallow` finds it)" if shallow else "no commit found"
    commit = commits[-1]
    described = subprocess.run(["git", "-C", str(root), "log", "-1", "--format=%cs %s", commit], check=True, capture_output=True, text=True).stdout.strip()
    date = described.split(" ", 1)[0]
    warning = f" - NOT the changelog's {changelog_date}, check what else this commit carries" if changelog_date and date != changelog_date else ""
    return f"{commit} ({described}){warning}"


def main(argv: list) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--since", default="1.4.0", help="oldest version to release (default: 1.4.0)")
    parser.add_argument("--until", default="1.9.0", help="newest version to release (default: 1.9.0)")
    parser.add_argument("--repo", default="familiary/mcrit")
    parser.add_argument("--root", default=".", help="repository root")
    parser.add_argument("--apply", action="store_true", help="create the releases instead of printing the commands")
    args = parser.parse_args(argv)
    root = Path(args.root)

    notes = changelogNotes((root / "CHANGELOG.md").read_text(encoding="utf-8"))
    tags = set(_lines(["git", "-C", str(root), "tag", "--list", "v*"]))
    releases = set(_lines(["gh", "release", "list", "--repo", args.repo, "--limit", "1000", "--json", "tagName", "--jq", ".[].tagName"]))
    to_release, untagged = plan(notes, tags, releases, args.since, args.until)

    dates = changelogDates((root / "CHANGELOG.md").read_text(encoding="utf-8"))
    for version in untagged:
        print(f"# v{version} has no tag; set as VERSION in {bumpCommit(root, version, dates.get(version, ''))} - tag it there to release it, or leave it out")
    for version in to_release:
        command = ["gh", "release", "create", f"v{version}", "--repo", args.repo, "--verify-tag", "--latest=false", "--title", f"v{version}", "--notes", notes[version]]
        if args.apply:
            subprocess.run(command, check=True)
            print(f"released v{version}")
        else:
            # the whole command, quoted for a shell, notes included: this is what --apply runs
            print(shlex.join(command))
    if not to_release:
        print("# nothing to release")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
