# Working in this repository

Facts for anyone, human or tool, making changes to django-ox. User-facing
guidance is on the docs site: https://oxpull.com/django-ox/agents/

## Layout

- `src/django_ox/`: the package. `backend.py` (enqueue, result API),
  `worker.py` (claim, execute, lease, reaper, schedule dispatch),
  `management/commands/` (`ox_worker`, `ox_health`, `ox_prune`),
  `schedules.py` and `cron.py` (recurring tasks), `stats.py` (queue metrics).
- `tests/`: pytest suite. `tests/settings.py` is SQLite; `settings_postgres.py`
  and `settings_mysql.py` switch the database through `DJANGO_SETTINGS_MODULE`.
- `docs/`: MkDocs site. `docs/llms-full.txt` is generated; do not edit it by
  hand.
- `tools/`: `check_release.py` (version and metadata gate),
  `build_llms_full.py` (regenerates `docs/llms-full.txt`).
- `benchmarks/`: benchmark harness and published results. Not shipped.

## Setup

```
python -m venv .venv
.venv/bin/pip install --group dev -e .
.venv/bin/pip install -r docs/requirements.txt
```

## Gates

Every one of these runs in CI on push and pull request, and all must pass:

```
.venv/bin/python -m pytest -q --cov
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/mypy --strict src/
.venv/bin/mkdocs build --strict
.venv/bin/python tools/check_release.py
```

PostgreSQL and MySQL legs, run locally when a change touches the claim path:

```
docker run -d --name ox-pg -e POSTGRES_PASSWORD=ox -p 54329:5432 postgres:16
DJANGO_SETTINGS_MODULE=tests.settings_postgres .venv/bin/python -m pytest -q
```

## Rules that are not obvious from the code

- Attempts are consumed at claim time, execution is at-least-once, and enqueue
  is transactional. Changes to the claim, retry or dispatch paths keep those
  invariants; the module docstrings in `worker.py` and `backend.py` state them.
- Behaviour changes need a test that fails without the change.
- User-visible changes get a line in `CHANGELOG.md` under an Unreleased heading.
  `tools/check_release.py` requires the version in `pyproject.toml`,
  `django_ox.__version__` and the newest changelog entry to agree.
- After editing anything under `docs/` or the `nav` in `mkdocs.yml`, run
  `python tools/build_llms_full.py`; `tests/test_llms_full.py` fails otherwise.
- `context7.json` `rules` are facts about the shipped code. Add one only when
  it is true in `src/` and stated in the docs.
- Comments explain why, not what. No commented-out code and no leftover work markers; ruff
  enforces both.
- The docs site publishes by hand with `mkdocs gh-deploy`, never on merge, and
  a PyPI release cannot be edited afterwards.
