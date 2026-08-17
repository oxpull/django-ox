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

## Pull requests

- One change per PR, with tests. Behavior changes need a test that fails
  without the fix.
- Anything touching the claim, retry, or dispatch paths must keep the
  invariants documented in the module docstrings (attempts consumed at
  claim time, at-least-once execution, transactional enqueue).
- Add a line to CHANGELOG.md under an Unreleased heading for anything
  user-visible.
