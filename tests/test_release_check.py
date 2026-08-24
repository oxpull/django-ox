"""The release gate, exercised against archives built in tmp_path.

A release is permanent: PyPI will not let the metadata be edited afterwards, so
every one of these failures costs another version to undo. The gate itself has
to be tested for the same reason, and it runs here rather than only at tag time
so that a mismatch fails on the commit that introduces it.

The failure the source tree cannot show is a packaging rule that drops a
migration. The module still imports, every test passes, and the break appears
only when somebody upgrades and their table lacks the column. That one is built
as a fixture below.
"""

import io
import re
import tarfile
import zipfile

import pytest

from tools.check_release import REPO, check_dist, check_source

VERSION = "0.2.0"
MIGRATIONS = {"0001_initial.py", "0002_oxscheduletick.py", "0003_lease_epoch.py"}
LICENCE = "BSD 3-Clause License\n\nCopyright (c) 2026, Oxpull\n"

URLS = {
    "Homepage": "https://oxpull.com/django-ox/",
    "Documentation": "https://oxpull.com/django-ox/",
    "Repository": "https://github.com/oxpull/django-ox",
    "Changelog": "https://github.com/oxpull/django-ox/blob/main/CHANGELOG.md",
    "Issues": "https://github.com/oxpull/django-ox/issues",
}


def _payload(version, migrations):
    payload = {
        "django_ox/__init__.py": f'__version__ = "{version}"\n',
        "django_ox/worker.py": "class Worker:\n    pass\n",
        "django_ox/models.py": "class OxTask:\n    pass\n",
        "django_ox/migrations/__init__.py": "",
    }
    for name in sorted(migrations):
        payload[f"django_ox/migrations/{name}"] = (
            "class Migration:\n    dependencies = []\n"
        )
    return payload


def _dist(tmp_path, *, version=VERSION, migrations=None, licence=LICENCE, omit=()):
    """Build a wheel and an sdist shaped the way hatchling lays them out."""
    dist = tmp_path / "dist"
    dist.mkdir(exist_ok=True)
    payload = _payload(version, MIGRATIONS if migrations is None else migrations)
    payload = {name: body for name, body in payload.items() if name not in omit}

    info = f"django_ox-{version}.dist-info"
    wheel_members = {
        **payload,
        f"{info}/METADATA": f"Name: django-ox\nVersion: {version}\n",
        f"{info}/WHEEL": "Wheel-Version: 1.0\n",
    }
    if licence is not None:
        wheel_members[f"{info}/licenses/LICENSE"] = licence
    with zipfile.ZipFile(
        dist / f"django_ox-{version}-py3-none-any.whl", "w"
    ) as archive:
        for name, body in wheel_members.items():
            archive.writestr(name, body)

    root = f"django_ox-{version}"
    sdist_members = {
        **{f"{root}/src/{name}": body for name, body in payload.items()},
        f"{root}/PKG-INFO": f"Name: django-ox\nVersion: {version}\n",
    }
    if licence is not None:
        sdist_members[f"{root}/LICENSE"] = licence
    with tarfile.open(dist / f"django_ox-{version}.tar.gz", "w:gz") as archive:
        for name, body in sdist_members.items():
            data = body.encode()
            entry = tarfile.TarInfo(name)
            entry.size = len(data)
            archive.addfile(entry, io.BytesIO(data))
    return dist


def _repo(
    tmp_path,
    *,
    version=VERSION,
    dunder=None,
    changelog=VERSION,
    link_ref=True,
    urls=None,
    readme_pin=None,
    docs_pin=None,
):
    repo = tmp_path / "repo"
    (repo / "src" / "django_ox").mkdir(parents=True, exist_ok=True)
    url_block = "\n".join(
        f'{key} = "{value}"' for key, value in (URLS if urls is None else urls).items()
    )
    (repo / "pyproject.toml").write_text(
        f'[project]\nname = "django-ox"\nversion = "{version}"\n'
        f"\n[project.urls]\n{url_block}\n",
        encoding="utf-8",
    )
    (repo / "src" / "django_ox" / "__init__.py").write_text(
        f'__version__ = "{dunder or version}"\n', encoding="utf-8"
    )
    if changelog:
        refs = (
            f"\n[{changelog}]: https://github.com/oxpull/django-ox/releases/tag/"
            f"v{changelog}\n"
            if link_ref
            else "\n"
        )
        (repo / "CHANGELOG.md").write_text(
            f"# Changelog\n\n## [{changelog}] - 2026-08-20\n\n### Fixed\n\n- A thing.\n"
            + refs,
            encoding="utf-8",
        )
    pin = f"`django-ox=={readme_pin}`" if readme_pin else "pip install django-ox"
    (repo / "README.md").write_text(f"Python 3.12+, Django 6.0+. {pin}\n", "utf-8")
    (repo / "docs").mkdir(exist_ok=True)
    docs_text = f"Pin `django-ox~={docs_pin}`.\n" if docs_pin else "Stable.\n"
    (repo / "docs" / "stability.md").write_text(docs_text, "utf-8")
    return repo


def _messages(problems):
    return "\n".join(problems)


# --- the source tree ------------------------------------------------------


def test_consistent_source_passes(tmp_path):
    problems, notes = check_source(_repo(tmp_path))
    assert problems == []
    assert notes


def test_mismatched_dunder_version_fails(tmp_path):
    problems, _ = check_source(_repo(tmp_path, dunder="0.1.2"))
    assert "__version__ is 0.1.2 but pyproject version is 0.2.0" in _messages(problems)


def test_stale_changelog_entry_fails(tmp_path):
    problems, _ = check_source(_repo(tmp_path, changelog="0.1.2"))
    assert "newest CHANGELOG entry is [0.1.2]" in _messages(problems)


def test_missing_changelog_link_reference_fails(tmp_path):
    problems, _ = check_source(_repo(tmp_path, link_ref=False))
    assert "no link reference for [0.2.0]" in _messages(problems)


def test_tag_must_match_the_declared_version(tmp_path):
    repo = _repo(tmp_path)
    assert check_source(repo, tag="v0.2.0")[0] == []
    problems, _ = check_source(repo, tag="v0.1.2")
    assert "tag v0.1.2 does not match the declared version (v0.2.0)" in _messages(
        problems
    )


def test_missing_project_url_fails(tmp_path):
    urls = {k: v for k, v in URLS.items() if k != "Documentation"}
    problems, _ = check_source(_repo(tmp_path, urls=urls))
    assert "is missing ['Documentation']" in _messages(problems)


def test_non_https_url_fails(tmp_path):
    urls = {**URLS, "Homepage": "http://oxpull.com/django-ox/"}
    problems, _ = check_source(_repo(tmp_path, urls=urls))
    assert "is not https" in _messages(problems)


def test_unexpected_host_fails(tmp_path):
    urls = {**URLS, "Documentation": "https://example.com/django-ox/"}
    problems, _ = check_source(_repo(tmp_path, urls=urls))
    assert "unexpected host" in _messages(problems)


def test_stale_readme_pin_fails(tmp_path):
    problems, _ = check_source(_repo(tmp_path, readme_pin="0.1.2"))
    assert "README.md pins django-ox 0.1.2 but this release is 0.2.0" in _messages(
        problems
    )


def test_stale_docs_pin_fails(tmp_path):
    problems, _ = check_source(_repo(tmp_path, docs_pin="0.1.2"))
    assert "stability.md pins django-ox 0.1.2 but this release is 0.2.0" in _messages(
        problems
    )


def test_this_repository_is_consistent():
    """The gate applied to the tree it ships with, so a bump cannot skip it.

    This is the assertion that makes the whole file worth having: it runs on
    every test run, so a version bumped in one place and not the other fails on
    the commit that does it rather than at tag time.
    """
    problems, _ = check_source(REPO)
    assert problems == []


# --- the built archives ---------------------------------------------------


def test_clean_build_passes(tmp_path):
    problems, notes = check_dist(_dist(tmp_path), VERSION, MIGRATIONS)
    assert problems == []
    assert any("django_ox-0.2.0.tar.gz" in note for note in notes)
    assert any("django_ox-0.2.0-py3-none-any.whl" in note for note in notes)


def test_missing_dist_directory_fails(tmp_path):
    problems, _ = check_dist(tmp_path / "dist", VERSION, MIGRATIONS)
    assert "does not exist" in _messages(problems)


@pytest.mark.parametrize("dropped", sorted(MIGRATIONS))
def test_a_dropped_migration_fails(tmp_path, dropped):
    """The failure no import and no test in this suite can see.

    A packaging rule that stops shipping a migration leaves a package that
    imports, passes, and then breaks on somebody else's upgrade with a column
    that is not there.
    """
    dist = _dist(tmp_path, migrations=MIGRATIONS - {dropped})
    problems, _ = check_dist(dist, VERSION, MIGRATIONS)
    text = _messages(problems)
    assert f"is missing migration {dropped}" in text
    # Caught in both archives; either one can be the thing that gets installed.
    assert "django_ox-0.2.0-py3-none-any.whl" in text
    assert "django_ox-0.2.0.tar.gz" in text


def test_two_versions_staged_together_fails(tmp_path):
    _dist(tmp_path, version="0.1.2")
    problems, _ = check_dist(_dist(tmp_path), VERSION, MIGRATIONS)
    text = _messages(problems)
    assert "holds a django_ox wheel at 0.1.2 as well as 0.2.0" in text
    assert "holds a django_ox sdist at 0.1.2 as well as 0.2.0" in text


def test_no_artifact_at_the_declared_version_fails(tmp_path):
    problems, _ = check_dist(_dist(tmp_path, version="0.1.2"), VERSION, MIGRATIONS)
    text = _messages(problems)
    assert "has no django_ox wheel at 0.2.0" in text
    assert "has no django_ox sdist at 0.2.0" in text


def test_missing_licence_fails(tmp_path):
    problems, _ = check_dist(_dist(tmp_path, licence=None), VERSION, MIGRATIONS)
    assert "does not carry LICENSE" in _messages(problems)


@pytest.mark.parametrize("module", ["worker.py", "models.py", "__init__.py"])
def test_missing_module_fails(tmp_path, module):
    dist = _dist(tmp_path, omit=(f"django_ox/{module}",))
    problems, _ = check_dist(dist, VERSION, MIGRATIONS)
    assert f"does not contain django_ox/{module}" in _messages(problems)


def test_the_migration_set_is_read_from_the_tree():
    """The expected set is the tree's, so a new migration is covered on arrival."""
    from tools.check_release import read_migrations

    shipped = read_migrations(REPO)
    assert shipped, "no migrations found in the source tree"
    assert all(re.match(r"^[0-9]{4}_", name) for name in shipped)
