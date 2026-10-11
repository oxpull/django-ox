"""
Installed-wheel tests. Opt-in: they build wheels, create virtualenvs and run
pip, so they are not in the default run (testpaths is tests) and every test
skips unless the run asks for the marker:

    python -m pytest tests_wheel -m wheel

Django's wheels come from the index once per run (or from OX_WHEELHOUSE, a
directory of wheels, for an offline run). Nothing else leaves the machine.
"""

import os
import sys

import pytest

from . import wheelkit


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "wheel: builds wheels and virtualenvs and runs pip; run with -m wheel",
    )


def pytest_collection_modifyitems(config, items):
    selected = "wheel" in (config.option.markexpr or "")
    skip = pytest.mark.skip(reason="installed-wheel test: run with -m wheel")
    for item in items:
        item.add_marker(pytest.mark.wheel)
        if not selected:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def artifacts(tmp_path_factory):
    return wheelkit.build_artifacts(tmp_path_factory.mktemp("artifacts"))


def _download(dest, requirements):
    dest.mkdir(parents=True, exist_ok=True)
    done = wheelkit.run(
        [
            sys.executable,
            "-m",
            "pip",
            "download",
            "--disable-pip-version-check",
            "--dest",
            dest,
            *requirements,
        ]
    )
    if done.returncode != 0:
        pytest.skip(f"cannot download {requirements}: {done.stderr[-300:]}")
    return dest


@pytest.fixture(scope="session")
def wheelhouse(tmp_path_factory):
    """Django 6.0 and its dependencies."""
    if os.environ.get("OX_WHEELHOUSE"):
        from pathlib import Path

        return Path(os.environ["OX_WHEELHOUSE"])
    return _download(tmp_path_factory.mktemp("wheelhouse"), ["Django>=6.0,<6.1"])


@pytest.fixture(scope="session")
def wheelhouse_52(tmp_path_factory):
    """Django 5.2 and the Tasks backport."""
    return _download(
        tmp_path_factory.mktemp("wheelhouse52"),
        ["Django>=5.2,<5.3", "django-tasks>=0.12"],
    )


@pytest.fixture
def make_env(tmp_path, artifacts, wheelhouse):
    count = 0

    def make(wheelhouse_dir=None):
        nonlocal count
        count += 1
        return wheelkit.Env(
            tmp_path, f"env{count}", wheelhouse_dir or wheelhouse, artifacts
        )

    return make
