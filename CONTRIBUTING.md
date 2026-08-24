# Contributing

## Setup

Requires Python 3.12+.

```
python -m venv .venv
.venv/bin/pip install --group dev -e .
```

## Running the tests

SQLite (default):

```
.venv/bin/python -m pytest
```

PostgreSQL 16, which exercises the single-statement `SKIP LOCKED` claim
path:

```
docker run -d --name ox-pg -e POSTGRES_PASSWORD=ox -p 54329:5432 postgres:16
DJANGO_SETTINGS_MODULE=tests.settings_postgres .venv/bin/python -m pytest
```

Run both before opening a PR; CI tests every supported Python and Django
version on SQLite and PostgreSQL, and the oldest and newest corners on
MySQL 8.

## Style

- `ruff check .` and `ruff format .` must pass. Configuration lives in
  `pyproject.toml`; don't fight the formatter.
- `mypy --strict src/` must pass.
- Comments explain why, not what. No commented-out code.

The lint selection includes rules that exist to keep the codebase readable
rather than to catch bugs: no commented-out code, no leftover `TODO` or `FIXME`
markers, no shadowed builtins, no boolean positional arguments. A block of
comments in `pyproject.toml` records which rule families are deliberately not
enabled and why, so that decision is visible instead of being rediscovered.

## Documentation

The documentation site builds with `mkdocs build --strict`, which is also a CI
job, so a broken link or a missing page fails the build rather than the site. It
publishes with `mkdocs gh-deploy` and does not publish on merge.

Prose is held to the same bar as code: state what the software does, keep the
measurement caveats that make a number quotable, and describe scope rather than
listing things as missing.

## Releases

`tools/check_release.py` checks that the version in `pyproject.toml`, the
`__version__` attribute and the newest changelog entry agree, that the changelog
has a link reference for that version, and that the project URLs are complete.
It runs on every push and again on the release tag.

The project URL check is not cosmetic. A package page cannot be edited after
upload, so anything wrong in the metadata stays wrong until the next version.

## Pull requests

- One change per PR, with tests. Behavior changes need a test that fails
  without the fix.
- Anything touching the claim, retry, or dispatch paths must keep the
  invariants documented in the module docstrings (attempts consumed at
  claim time, at-least-once execution, transactional enqueue).
- Add a line to CHANGELOG.md under an Unreleased heading for anything
  user-visible.
