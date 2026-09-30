"""The one-off release backfill (#192) reads the changelog in both of the shapes it has had."""

import importlib.util
import shlex
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / ".github" / "workflows" / "scripts" / "backfill_releases.py"

CHANGELOG = """# Changelog

## [Unreleased]

## [1.10.0] - 2026-09-25

### Fixed

- the newest thing.

## [1.9.0] - 2026-09-08

### Fixed

- the 1.9.0 thing, see [#142].

## Older releases

 * 2026-08-25 v1.8.1:  Declares `packaging` as a dependency.
 * 2025-12-22 v1.4.5:  Fixed a bug.
 * 2025-12-22 v1.4.4:  No changes.
 * 2024-01-30 v1.3.0:  Too old for the backfill.

[#142]: https://github.com/familiary/mcrit/issues/142
[#999]: https://github.com/familiary/mcrit/issues/999
"""


@pytest.fixture(scope="module")
def backfill():
    spec = importlib.util.spec_from_file_location("backfill_releases", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_notes_come_from_sections_and_from_the_older_entries(backfill):
    notes = backfill.changelogNotes(CHANGELOG)
    # with the definition of the one link it uses, and only that one
    assert notes["1.9.0"] == "### Fixed\n\n- the 1.9.0 thing, see [#142].\n\n[#142]: https://github.com/familiary/mcrit/issues/142"
    assert notes["1.8.1"] == "Declares `packaging` as a dependency."
    assert notes["1.4.4"] == "No changes."
    assert "Unreleased" not in notes


def test_the_plan_skips_released_and_reports_untagged_versions(backfill):
    notes = backfill.changelogNotes(CHANGELOG)
    tags = {"v1.3.0", "v1.4.5", "v1.8.1", "v1.9.0", "v1.10.0"}
    to_release, untagged = backfill.plan(notes, tags, releases={"v1.3.0", "v1.8.1", "v1.10.0"}, since="1.4.0", until="1.9.0")
    assert to_release == ["1.4.5", "1.9.0"]
    assert untagged == ["1.4.4"]


def test_versions_order_numerically_not_as_text(backfill):
    notes = {"1.4.10": "a", "1.4.9": "b", "1.10.0": "c"}
    to_release, _ = backfill.plan(notes, {"v1.4.10", "v1.4.9", "v1.10.0"}, set(), since="1.4.0", until="1.9.0")
    assert to_release == ["1.4.9", "1.4.10"]


def test_every_version_the_issue_names_has_notes_in_the_real_changelog(backfill):
    notes = backfill.changelogNotes((ROOT / "CHANGELOG.md").read_text(encoding="utf-8"))
    wanted = [
        "1.4.0",
        "1.4.1",
        "1.4.2",
        "1.4.3",
        "1.4.4",
        "1.4.5",
        "1.4.6",
        "1.4.7",
        "1.5.0",
        "1.5.1",
        "1.5.2",
        "1.5.3",
        "1.6.0",
        "1.6.1",
        "1.6.2",
        "1.7.0",
        "1.7.1",
        "1.8.0",
        "1.8.1",
        "1.9.0",
    ]
    assert [version for version in wanted if not notes.get(version, "").strip()] == []


def test_nothing_runs_without_apply(backfill, monkeypatch, capsys):
    calls = []

    def fake_lines(command):
        calls.append(command)
        return {"git": ["v1.9.0"], "gh": []}[command[0]]

    monkeypatch.setattr(backfill, "_lines", fake_lines)
    monkeypatch.setattr(backfill.subprocess, "run", lambda *args, **kwargs: pytest.fail("ran a command without --apply"))
    assert backfill.main(["--root", str(ROOT), "--since", "1.9.0", "--until", "1.9.0"]) == 0
    printed = capsys.readouterr().out
    # the whole command, notes included, quoted so that it can be pasted
    # the notes span lines inside their quotes, so the command is everything from its start on
    command = shlex.split(printed[printed.index("gh release create") :])
    assert command[:10] == ["gh", "release", "create", "v1.9.0", "--repo", "familiary/mcrit", "--verify-tag", "--latest=false", "--title", "v1.9.0"]
    assert command[10] == "--notes" and command[11] == backfill.changelogNotes((ROOT / "CHANGELOG.md").read_text(encoding="utf-8"))["1.9.0"]
    assert [command[0] for command in calls] == ["git", "gh"]


def test_the_candidate_commit_is_the_bump_with_its_date_checked(backfill, monkeypatch):
    answers = {"log": ["fc3d84c", "32c82d3"], "rev-parse": ["false"]}
    monkeypatch.setattr(backfill, "_lines", lambda command: answers[command[3]])
    described = {"32c82d3": "2026-04-08 Modernize CI and adopt Ruff with pre-commit hooks"}
    monkeypatch.setattr(backfill.subprocess, "run", lambda command, **kwargs: type("Done", (), {"stdout": described[command[-1]] + "\n"})())
    # git log lists newest first; the bump is the oldest commit that introduced the version
    same_day = backfill.bumpCommit(ROOT, "1.4.6", "2026-04-08")
    assert same_day == "32c82d3 (2026-04-08 Modernize CI and adopt Ruff with pre-commit hooks)"
    assert "NOT the changelog's 2026-01-13" in backfill.bumpCommit(ROOT, "1.4.6", "2026-01-13")


def test_a_shallow_clone_says_why_it_cannot_name_the_commit(backfill, monkeypatch):
    for shallow, expected in (("true", "git fetch --unshallow"), ("false", "no commit found")):
        answers = {"log": [], "rev-parse": [shallow]}
        monkeypatch.setattr(backfill, "_lines", lambda command, answers=answers: answers[command[3]])
        assert expected in backfill.bumpCommit(ROOT, "1.4.6")


def test_every_release_date_comes_from_the_changelog(backfill):
    dates = backfill.changelogDates(CHANGELOG)
    assert (dates["1.9.0"], dates["1.4.5"]) == ("2026-09-08", "2025-12-22")


def test_a_definition_already_in_the_notes_is_not_added_again(backfill):
    changelog = CHANGELOG.replace("- the 1.9.0 thing, see [#142].", "- the 1.9.0 thing, see [#142].\n\n[#142]: https://github.com/familiary/mcrit/issues/142")
    assert backfill.changelogNotes(changelog)["1.9.0"].count("[#142]: ") == 1


def test_a_version_listed_twice_keeps_its_first_entry(backfill):
    changelog = CHANGELOG + " * 2025-01-01 v1.4.5:  An older duplicate.\n"
    assert backfill.changelogNotes(changelog)["1.4.5"] == "Fixed a bug."


def test_nothing_to_release_says_so(backfill, monkeypatch, capsys):
    monkeypatch.setattr(backfill, "_lines", lambda command: {"git": ["v1.9.0"], "gh": ["v1.9.0"]}[command[0]])
    assert backfill.main(["--root", str(ROOT), "--since", "1.9.0", "--until", "1.9.0"]) == 0
    assert "# nothing to release" in capsys.readouterr().out
