# Release checklist — 0.1.0

Ordered. Do not skip ahead. `[PHILIP]` = needs the owner's decision or keystroke
(accounts, tokens, anything that publishes). `[agent-ok]` = an agent may execute
and verify. Nothing below the "Publish" line runs until everything above it is
green.

All commands run from the repo root unless noted. `$VENV` = the project venv
(currently `../.venv`).

## 0. Decisions (blocker for everything else)

- [x] `[PHILIP]` Final package name: **django-ox**, locked 2026-08-13 (was
      working-named django-plow; rename executed locally the same night).
- [x] `[PHILIP]` DONE 2026-08-14: brand Oxpull; author + org URLs filled
      placeholder, and the final repository URL.

## 1. Rename (only if the name changes — otherwise skip)

Executed 2026-08-13: django-plow → django-ox, all items below done and
verified (151 passed on SQLite and PG16, fresh-venv smoke test rerun).

The name appears in exactly three spellings. The rename is a mechanical
find-replace of each, plus two directory moves:

- [ ] `[agent-ok]` Distribution/dashed name: replace `django-ox` → `django-NEW`
      in `pyproject.toml`, `README.md`, `CHANGELOG.md`, `RELEASE-CHECKLIST.md`.
- [ ] `[agent-ok]` Import/underscored name: replace `django_ox` → `django_NEW`
      everywhere (source, tests, pyproject build targets, README settings
      snippets), then `mv src/django_ox src/django_NEW`.
- [ ] `[agent-ok]` Brand prefix: replace `Ox` → `NEW` (classes `OxBackend`,
      `OxTask`, `OxScheduleTick`) and `ox_` → `NEW_` (management commands
      `ox_worker`, `ox_prune`; rename the two files under
      `src/*/management/commands/`). Note the logger name and worker log lines
      use `django_ox` and are covered by the underscore replace.
- [ ] `[agent-ok]` Regenerate nothing: migrations reference models by app label,
      which follows the package name; run the test suite to confirm
      (`$VENV/bin/python -m pytest -q` → must be 141 passed), and run
      `manage.py makemigrations --check --dry-run` in a scratch project to
      confirm no migration drift.
- [ ] `[agent-ok]` Check name availability: `https://pypi.org/project/<name>/`
      must 404, on both PyPI and TestPyPI. A deleted/blocked name cannot be
      reused — check before printing the name anywhere.

## 2. Placeholders

- [x] `[agent-ok]` DONE 2026-08-14, rebuilt + twine PASSED + fresh-venv verified: `pyproject.toml` (`authors`, all four
      `[project.urls]` entries), `LICENSE` (copyright line), `CHANGELOG.md`
      (release link at the bottom). `grep -rn "\[COMPANY\]" . --include="*.toml" --include="*.md" --include=LICENSE`
      must return nothing afterwards.
- [ ] `[agent-ok]` Confirm the CHANGELOG release date is the actual publish date.
- [ ] `[agent-ok]` Version consistency: `0.1.0` in `pyproject.toml` and
      `src/*/__init__.py.__version__` must match.

## 3. Git (repo is not yet under version control)

- [ ] `[agent-ok]` `git init`, commit the tree (respecting `.gitignore`;
      `benchmarks/logs/`, `tests/db.sqlite3` and `.contract-ref/` should NOT be
      committed to the public repo — decide and prune first).
- [ ] `[PHILIP]` Create the GitHub repo under the chosen org and push.

## 4. Verify (all local, no publishing)

- [ ] `[agent-ok]` Full suite on the primary combo:
      `$VENV/bin/python -m pytest -q` (SQLite) and the PG16 settings run —
      141 passed each.
- [ ] `[agent-ok]` Clean build:
      `rm -rf dist && $VENV/bin/python -m build`
- [ ] `[agent-ok]` `$VENV/bin/twine check dist/*` — both PASSED.
- [ ] `[agent-ok]` Inspect contents: `tar tzf dist/*.tar.gz` and
      `unzip -l dist/*.whl`. Must contain: migrations, `py.typed`, `LICENSE`
      (wheel: under `.dist-info/licenses/`). Must NOT contain: `tests/`,
      `benchmarks/`, `.contract-ref/`, `__pycache__`, `*.sqlite3`.
- [ ] `[agent-ok]` Fresh-venv smoke test: new venv → `pip install dist/*.whl` →
      scratch `startproject` with the `TASKS` backend configured →
      `manage.py check` → `manage.py migrate` → enqueue one task →
      `manage.py <prefix>_worker` picks it up → status SUCCESSFUL.

## 5. Accounts and tokens

- [ ] `[PHILIP]` PyPI account (and TestPyPI account — they are separate), 2FA
      enabled, org/owner set as intended for the company.
- [ ] `[PHILIP]` Create a **project-scoped** API token on each (first upload
      needs an account-scoped token; scope it down to the project immediately
      after). Tokens go in `~/.pypirc` or env — never in the repo.

## 6. Dry run against TestPyPI

- [ ] `[PHILIP]` `twine upload --repository testpypi dist/*`
      (uploading is publishing, even on TestPyPI — owner's keystroke).
- [ ] `[agent-ok]` Install from TestPyPI in a fresh venv (Django must come from
      real PyPI):
      `pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ <name>`
      then repeat the scratch-project smoke test.
- [ ] `[agent-ok]` Eyeball the TestPyPI project page: README renders, license
      shows BSD-3-Clause, links point where intended.

## 7. Publish

- [ ] `[PHILIP]` `twine upload dist/*`
- [ ] `[agent-ok]` `git tag -a v0.1.0 -m "0.1.0"` on the exact released commit.
- [ ] `[PHILIP]` `git push origin main --tags`
- [ ] `[PHILIP]` GitHub release for `v0.1.0` with the CHANGELOG entry as body.

## 8. Post-publish verification

- [ ] `[agent-ok]` Fresh venv, `pip install <name>` from real PyPI (no index
      overrides), repeat the scratch-project smoke test end to end.
- [ ] `[agent-ok]` `pip download <name> --no-deps -d /tmp/x && twine check /tmp/x/*`
      — verify what PyPI actually serves.
- [ ] `[agent-ok]` Check the PyPI project page rendering one more time.

## Notes

- The sdist contains `.gitignore` at its root. That is hatchling's default (it
  uses the file for file selection when building from the sdist) and is normal
  for hatchling-built packages on PyPI. Not a defect; do not fight it.
- Verified support matrix as of 2026-08-13: Python 3.12/3.14 × Django
  6.0.8/6.1, 141/141 on SQLite; PG16 verified on py3.12 × Django 6.0.8.
  Python 3.13 is claimed (classifier) per Django 6.0's documented support but
  no 3.13 interpreter was available locally — if one is installed before
  release, run the suite under it.
