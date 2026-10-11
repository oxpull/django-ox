# Working in this repository

**If you are adding django-ox to an application, read the application guide.**
Use https://oxpull.com/django-ox/agents/ instead. The instructions below are
for changing django-ox itself, and the editable install is for contributors.

## Layout

django-ox is one distribution. It contains `django_ox` and the optional
`oxpull` app, which requires Django 6.0 or later.

- `src/django_ox/`: the base app. `backend.py` implements enqueue and result
  APIs; `worker.py` handles claims, execution, leases, reaping and schedule
  dispatch. `supervisor.py` implements `--processes`, and `heartbeat.py`
  handles local heartbeat-file updates and checks.
- Management commands include `ox_worker`, `ox_health`, `ox_prune`, `ox_import_beat_schedules` and the optional app's `oxpull_prune`.
- Other `django_ox` modules include `tasks.py` (`PolicyTask`), `testing.py`
  (policy-aware test backends), `schedules.py` and `cron.py` (recurring
  tasks), `stats.py` (queue metrics), and `metrics.py`, `views.py` and
  `urls.py` (the Prometheus endpoint). `actions.py` implements retry and
  discard; `bulk.py` implements `enqueue_many`. `admin.py` is imported only
  when `django.contrib.admin` is installed.
- `src/oxpull/`: the optional app. `backend.py` implements `OxpullBackend`
  and unique enqueue; `worker.py` implements `OxpullWorker`, rate-limit
  admission and the workflow claim gate. Feature modules include
  `unique.py`, `batches.py`, `workflows.py` and `rate_limits.py`.
  Models, exceptions and migrations live in `models.py`, `exceptions.py`
  and `migrations/`. The app adds the `oxpull_prune` command.
  `_django_gate.py` and `_requires_django_6.py` enforce the Django version
  requirement. Workflows are provisional.
- `src/oxpull/routing.py` enforces the shared database requirement. `OxpullBackend.check()` implements 14 of the app's check ids; `src/oxpull/checks.py` implements the database checks `oxpull.E009` and `oxpull.W007`.
- `tests/`: the base app's pytest suite and the default pytest target.
  `tests/settings.py` uses SQLite; `settings_postgres.py` and
  `settings_mysql.py` select other databases.
- `tests_oxpull/`: the optional app's suite, requiring Django 6.0 or later.
  Use its own settings through `--ds`. Settings cover SQLite, PostgreSQL,
  pooled PostgreSQL and MySQL. Hazard tests carry the `hazard` marker.
  `fixtures/PROVENANCE.md` documents the immutable historical source
  archive loaded by `_historical.py`.
- `tests_wheel/`: opt-in installed-wheel transition tests, selected with
  `-m wheel`.
- `docs/`: the MkDocs site. `docs/llms-full.txt` is generated; do not edit
  it by hand.
- `tools/check_release.py` checks release metadata. `pyproject.toml`, `django_ox.__version__` and the newest release entry must agree. `oxpull.__version__` is imported from `django_ox`; the gate refuses a separate literal.
- `tools/build_llms_full.py`: regenerates `docs/llms-full.txt`.
- `tools/ci/changes.py`: selects feature jobs from a diff, defaulting to
  more coverage when uncertain. `tools/ci/shard.py` and
  `tools/ci/shards.json` divide `tests_oxpull/` into six measured shards.
- `benchmarks/`: the benchmark harness and published results. Not shipped.

## Contributor setup

Use Django 6.0 or later when working on the optional app.

```sh
python -m venv .venv
.venv/bin/pip install --group dev -e .
.venv/bin/pip install -r docs/requirements.txt
```

## Gates

Run these local checks:

```sh
.venv/bin/python -m pytest -q --cov
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/mypy --strict src/
.venv/bin/mkdocs build --strict
.venv/bin/python tools/check_release.py
```

The pytest command above runs `tests/` only. Strict type checking covers
both apps. Run the optional app's suite separately on Django 6.0 or later:

```sh
.venv/bin/python -m pytest tests_oxpull --ds=tests_oxpull.settings
```

Do not use `tests.*` settings for `tests_oxpull/`. Add `-m "not hazard"`
to exclude hazard tests during a focused run. Run hazard tests when
changing worker, claim, transaction, model, migration or workflow behaviour.

Check for missing migrations for both apps:

```sh
DJANGO_SETTINGS_MODULE=tests.settings_typecheck .venv/bin/python -m django makemigrations --check --dry-run django_ox oxpull
```

Run installed-wheel transition tests when changing packaging or upgrade
behaviour:

```sh
.venv/bin/python -m pytest tests_wheel -m wheel
```

CI configures six shards for the `oxpull` feature suite. The base suite is in `tests/`; the optional app's suite is in `tests_oxpull/`. Run the suites affected by a change, including both for shared runtime behavior.

### Database checks

Run the relevant suites on PostgreSQL and MySQL when changing database
behaviour, especially claims.

```sh
docker run -d --name ox-pg -e POSTGRES_PASSWORD=ox -p 54329:5432 postgres:16
DJANGO_SETTINGS_MODULE=tests.settings_postgres .venv/bin/python -m pytest -q
.venv/bin/python -m pytest tests_oxpull --ds=tests_oxpull.settings_postgres
```

Use `--ds=tests_oxpull.settings_postgres_pooled` to exercise the pooled
PostgreSQL configuration.

```sh
.venv/bin/pip install PyMySQL
docker run -d --name ox-mysql -e MYSQL_ROOT_PASSWORD=ox \
    -e MYSQL_DATABASE=oxtest -p 33069:3306 mysql:8
DJANGO_SETTINGS_MODULE=tests.settings_mysql .venv/bin/python -m pytest -q
.venv/bin/python -m pytest tests_oxpull --ds=tests_oxpull.settings_mysql
```

## Rules that are not obvious from the code

- Attempts are consumed at claim time, execution is at-least-once, and
  enqueue is transactional. Preserve these invariants when changing
  claim, retry or dispatch paths. Read the module docstrings in
  `src/django_ox/worker.py` and `src/django_ox/backend.py`.
- Uniqueness deduplicates pending tasks at enqueue time. It does not
  prevent two executions of one key from overlapping.
- `oxpull` creates only its own tables and does not alter `django_ox`
  tables, columns, indexes, constraints or migrations. Workflows do write
  `django_ox` task rows. Preserve the workflow claim gate and the task-row
  lease-epoch checks for cancellation and release. The workflow
  `generation` counter is reserved and remains 0; it is not a lease epoch.
  Read the module docstrings in `src/oxpull/backend.py`,
  `src/oxpull/worker.py` and `src/oxpull/workflows.py`.
- `OPTIONS["WORKFLOWS"]` is a boolean. Set it to `True` to enable
  workflows, which are created at runtime rather than from definitions.
- Keep `oxpull` and `django_ox` tables on the same database.
  `OxpullBackend.__init__` raises `ImproperlyConfigured` at startup if
  routing splits them. `oxpull.E004` reports a backend constructed before
  a router change; it is not the initial startup guard.
- Behaviour changes need a test that fails without the change.
- User-visible changes get a line in `CHANGELOG.md` under an Unreleased
  heading. `pyproject.toml`, `django_ox.__version__` and the newest
  release entry must agree.
  `tools/check_release.py` checks release consistency.
- After editing anything under `docs/` or the `nav` in `mkdocs.yml`, run
  `python tools/build_llms_full.py`; `tests/test_llms_full.py` checks the
  generated output.
- Document each `oxpull` event in `docs/monitoring.md`. Keep the
  monitoring-docs scanner's coverage aligned with the documented events.
- Keep the README and `oxpull.backend` module docstring explicit about
  the schema boundary and workflow task-row writes. The workflow hazard
  suite checks both.
- `context7.json` rules describe shipped code. Add a rule only when it is
  true in `src/` and stated in the docs.
- Comments explain why, not what. No commented-out code or leftover work
  markers.
- The docs site publishes by hand with `mkdocs gh-deploy`, never on merge.
  A PyPI release cannot be edited afterwards.
