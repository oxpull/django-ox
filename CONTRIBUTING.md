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

PostgreSQL 16, which exercises the `SELECT ... FOR UPDATE SKIP LOCKED`
claim path:

```
docker run -d --name ox-pg -e POSTGRES_PASSWORD=ox -p 54329:5432 postgres:16
DJANGO_SETTINGS_MODULE=tests.settings_postgres .venv/bin/python -m pytest
```

Run both before opening a PR; CI tests every supported Python and Django
version against both databases.

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

## Documentation and prose

Everything a reader outside the project sees is checked:

```
python tools/copy_lint.py
```

It rejects copy that describes the project as weak or broken, copy that narrates
defects fixed before release, internal review markers, and a test count that
disagrees between files. Warnings are advisory; errors block. If a rule is wrong
about a specific line, suppress that line and give the reason:

```
<!-- copy-lint: allow SELF_DOWNGRADE the slow service here is the upstream one -->
```

A suppression without a reason is rejected. `tests/test_copy_lint.py` covers the
rules, including the false-positive side.

The documentation site builds with `mkdocs build --strict` and publishes with
`mkdocs gh-deploy`. It does not publish on merge.

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
