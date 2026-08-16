# Build notes

Internal notes for django-ox v0.1.0. Not shipped documentation.

## Contract sources (fetched 2026-08-13)

Backend contract derived from primary sources, not third-party writeups:

- https://raw.githubusercontent.com/django/django/stable/6.0.x/django/tasks/base.py (method: fetched)
- https://raw.githubusercontent.com/django/django/stable/6.0.x/django/tasks/backends/base.py (method: fetched)
- https://raw.githubusercontent.com/django/django/stable/6.0.x/django/tasks/backends/immediate.py (method: fetched)
- https://raw.githubusercontent.com/django/django/stable/6.0.x/django/tasks/exceptions.py (method: fetched)
- https://raw.githubusercontent.com/django/django/stable/6.0.x/django/tasks/signals.py (method: fetched)
- https://raw.githubusercontent.com/django/django/stable/6.0.x/django/tasks/checks.py (method: fetched)
- https://docs.djangoproject.com/en/6.0/topics/tasks/ (method: fetched, summarized by WebFetch model)
- https://docs.djangoproject.com/en/6.0/ref/tasks/ (method: fetched, summarized by WebFetch model)

Local copies of the fetched source files are in `.contract-ref/`.

Contract facts that drove the design:

- Statuses are `TaskResultStatus.READY / RUNNING / FAILED / SUCCESSFUL`.
  The original brief said "SUCCEEDED"; the real enum value is SUCCESSFUL, so
  the model mirrors the enum values exactly.
- `BaseTaskBackend.__init__(alias, params)` reads `QUEUES` (empty list means
  any queue is valid) and `OPTIONS` from the `TASKS` setting entry.
- Capability flags: `supports_defer`, `supports_async_task`,
  `supports_get_result`, `supports_priority`. All honestly True here.
- `enqueue(task, args, kwargs)` must call `validate_task` and return a frozen
  `TaskResult`; `_return_value` is `init=False` and set via
  `object.__setattr__` (ImmediateBackend does the same).
- `TaskResult.attempts` is `len(worker_ids)`, so the worker appends its id
  once per execution attempt.
- `errors` is `list[TaskError]` with `exception_class_path` + `traceback`;
  stored as a JSON list of dicts with those exact keys.
- Signals `task_enqueued`, `task_started`, `task_finished` exist in
  `django.tasks.signals` (undocumented but used by core logging); the backend
  and worker send them at the same points ImmediateBackend does.
- Core has no `ENQUEUE_ON_COMMIT`; the docs recommend manual
  `transaction.on_commit`. A DB backend makes that moot: the INSERT is
  transactional by construction.
- `django.utils.json.normalize_json` is the canonical argument/return-value
  normalizer; used at enqueue and on return values.

## Design decisions

- License: BSD-3-Clause for v0, matching Django ecosystem norms. Licensing
  strategy (including any dual-license Pro tier) is decided later.
- Single table (`OxTask`) is both queue and result store. Status values
  mirror `TaskResultStatus` so conversion is a value cast. Two indexes: the
  dequeue path (status, queue_name, -priority, run_after) and the reaper path
  (status, locked_at).
- Claiming: `SELECT ... FOR UPDATE SKIP LOCKED` when
  `connection.features.has_select_for_update_skip_locked`, else an optimistic
  compare-and-set `UPDATE ... WHERE status=READY` checked by rowcount, which
  is atomic on every backend including SQLite. Both paths increment
  `attempts` inside the claim UPDATE itself, so a worker that dies mid-task
  has already consumed the attempt and retries stay bounded.
- Backoff state is `run_after` plus `attempts`; no separate columns. A retry
  is just READY with a future run_after, so the dequeue query needs no extra
  logic. Delay is `BACKOFF_INITIAL * 2**(attempts-1)` capped at
  `BACKOFF_MAX`. No jitter in v0.
- Threading model: one polling main loop, `ThreadPoolExecutor` for execution,
  `close_old_connections()` at both ends of each task. Threads fit the
  target workload (I/O-bound Django tasks) and keep one process and one
  settings module. CPU-bound work should scale with processes instead
  (documented in README). Process pools would break DB connection handling
  and app registry sharing for marginal gain here.
- Async task functions are supported by running them through `Task.call`,
  which wraps coroutines with `async_to_sync` inside the worker thread. The
  worker itself stays synchronous; an async event-loop worker is not v0.
- Reaper does a CAS on `locked_at` when reclaiming so it cannot stomp a task
  that finished or was reclaimed between read and update. Exhausted tasks get
  a `django_ox.exceptions.TaskAbandoned` error record, a real exception
  class so `TaskError.exception_class` resolves.
- Signals during shutdown: first SIGTERM/SIGINT stops claiming and drains via
  `executor.shutdown(wait=True)`; second signal `os._exit(130)`.
- `task_finished` is sent only on terminal states (SUCCESSFUL / FAILED), not
  on retries; retries log a warning on the `django_ox` logger.
- Tests use a file-based SQLite test database (`TEST["NAME"]` set). Django's
  default shared-cache in-memory test DB raises "database table is locked"
  across threads regardless of busy timeout; this was observed as a roughly
  1-in-3 flake before the fix.
- Tested against Django 6.0.8 and 6.1 (both green, 6 consecutive runs each).

## Known gaps

- The SKIP LOCKED claim branch is untested: the suite runs on SQLite, which
  exercises only the CAS path. Needs a PostgreSQL CI job before any release.
- If a worker dies in the window between the claim UPDATE and the
  `worker_ids` save, the row shows an attempt with no corresponding worker
  id; `TaskResult.attempts` (len of worker_ids) then undercounts the model's
  `attempts` field. Cosmetic, but a divergence.
- `task_enqueued` fires at INSERT time; if the surrounding transaction rolls
  back the signal has still fired. Core gives no guidance here.
- Signal handlers in the management command are exercised by one subprocess
  smoke test during development, not by the pytest suite; the suite covers
  `request_stop()` drain behavior and CLI flag wiring.
- No admin registration, no metrics. (Pruning added in increment 2.)
- `worker_ids` truncation: locked_by is capped at 64 chars via hostname
  truncation; collisions are theoretically possible but ids include pid and
  8 random chars.

## Possible Pro tier

- Task batches and chaining.
- Per-queue and per-task rate limiting.
- Web dashboard (queue depth, failures, retry curves).
- Metrics exporters (Prometheus/OpenTelemetry).
- Row pruning/archival policies.

## Postgres verification (root session, 2026-08-13 00:30-00:34, timestamped from date)

The SKIP LOCKED gap flagged above is closed: full suite run against PostgreSQL 16 (docker,
psycopg[binary]) via tests/settings_postgres.py — `29 passed` three consecutive runs. Postgres
reports has_select_for_update_skip_locked=True, so claim_one() exercised the SELECT FOR UPDATE
SKIP LOCKED branch on all three runs. Known cosmetic issue: pytest teardown warns 'database
"test_postgres" is being accessed by other users' — a worker-pool thread's connection outlives
the drain at test-database drop time. Harmless in tests; before release, consider closing
thread-local connections explicitly at executor shutdown rather than relying on
close_old_connections() per task. Container and Docker Desktop shut down after the run.

## Increment 2 (2026-08-13)

- `ox_prune` management command: deletes SUCCESSFUL (and with
  `--include-failed`, FAILED) rows with `finished_at__lt` cutoff, in
  pk-batches (`--batch-size`, default 1000) so no table-length lock or giant
  IN clause; `--dry-run` counts only; `--older-than` accepts 7d/24h/90m/45s
  or plain seconds (default 7d). README gained a Pruning section; the
  "not yet supported" bullet moved out.
- Executor-shutdown connection fix (closes the Postgres-verification note
  above): after the in-flight drain, run() submits one
  barrier-synchronized close task per pool slot, then shuts the executor
  down and closes its own thread's connections. Chosen over `close_all()`
  per task (would defeat CONN_MAX_AGE reuse) and over an executor
  initializer (runs at thread spawn, not exit); the Barrier guarantees each
  pool thread takes exactly one close task. SQLite suite green; the
  Postgres teardown-warning re-check needs Docker, which was not running
  this session — left to the operator.
- Tests 29 -> 51 (`51 passed`, 4 consecutive runs): duration parsing
  accept/reject, default prune scope, `--include-failed`, cutoff boundary,
  READY/RUNNING immunity even with ancient finished_at, dry-run, query-count
  proof of batching, flag validation, and one-close-per-pool-thread on
  worker shutdown.

## Increment 2 root verification (2026-08-13 01:02, from date)

Suite re-run independently by root: SQLite 51 passed; PostgreSQL 16 (docker) 51 passed with the
teardown warning GONE (was: 'database "test_postgres" is being accessed by other users' on every
PG run of increment 1). ox_prune.py audited: style-consistent, batching bounded, terminal-status
invariant documented at the delete site. Accepted. Docker container removed and Docker Desktop
quit after the run.

## Benchmark-driven fix (root session, 2026-08-13 02:57)

benchmarks/ (increment 3, by agent) measured ox vs django-tasks-db on PG16 and caught a real
bug: worker run() slept the full poll_interval whenever all executor slots were busy, capping
c1 throughput at 0.86 tasks/sec. Fix: futures-aware wait (FIRST_COMPLETED, timeout=poll_interval)
when tasks are in flight; plain stop-event wait only when idle. Suite 51/51 green after. Post-fix
re-run: c1 0.86 -> 54 tasks/sec, diagnostic interval-0.1 cell now identical to default (bound
removed). Remaining: ~18.5 ms/task residual vs tasksdb 14.1 (query-count suspected), c4 242 vs
372 (threads-vs-processes caveat applies). Honest loss recorded in results doc; optimization is
a pre-release item, not a launch blocker: the product claim is transactional durability, not
no-op throughput.

## Increment 4 (2026-08-13 03:07, from date): claim-path round trips

Bounded performance pass on the ~18.5 ms/task c1 residual. Profiled first
(harness on benchsite.settings_ox against the ox-bench PG16 container;
force_debug_cursor for the query census, perf_counter + cProfile for time):

- Per-task query census, inline run_once, before: **7 statements** — BEGIN,
  SELECT FOR UPDATE SKIP LOCKED, claim UPDATE, re-fetch SELECT, COMMIT,
  started-fields UPDATE, success UPDATE. The threaded run() path added an
  8th (re-fetch GET in `_execute_in_thread`).
- Wall time inline (400 tasks): claim_one mean 10.6 ms, execute mean 5.2 ms,
  total 15.8 ms/task. DB-reported statement time only ~5.2 ms/task, so most
  of the cost was per-statement client/round-trip overhead (~2.2 ms per
  statement; cProfile showed psycopg `wait` dominating with 4.27 s tottime
  over 3200 calls in a 10.1 s run).
- `task_from_db` / `import_string` did not appear in the top 28 cumulative
  entries, so Task-object caching was NOT justified by the profile and was
  not done. Batch-claiming was skipped too: at c1 the loop never has more
  than one free slot, and the profile shows round trips, not claim passes,
  as the cost.

Three changes, all round-trip elimination in the claim/execute path:

1. **Single-statement claim on PostgreSQL**: `UPDATE ... SET <claim +
   bookkeeping> WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1)
   RETURNING *`, executed via `OxTask.objects.raw()` in autocommit —
   atomic, one round trip, gated on `vendor == "postgresql" and
   has_select_for_update_skip_locked`. Other SKIP LOCKED databases keep the
   ORM transaction path.
2. **Bookkeeping folded into the claim UPDATE on every path** (started_at
   via COALESCE / Python fallback, last_attempted_at, worker_ids append,
   attempts increment), and the returned instance is built from values in
   hand instead of a post-claim re-fetch. The CAS path now keys its UPDATE
   on `(pk, status=READY, attempts=<fetched>)` — attempts doubles as a
   version counter so literal worker_ids can never stomp a concurrent
   claim's writes. Side effect: the increment-1 known gap (worker death
   between claim and started-save leaving attempts > len(worker_ids)) is
   gone; the two are written atomically and always agree.
3. **run() hands the claimed instance to the pool thread** instead of the pk,
   removing the per-task re-fetch GET in `_execute_in_thread`.

Invariants preserved: attempts consumed on claim (now strictly stronger),
at-least-once execution, transactional enqueue untouched, signal points
unchanged (task_started still fires in execute before the call).

After: **2 statements/task** (claim UPDATE + success UPDATE), inline
run_once 15.8 -> 4.0 ms/task mean (claim 2.1, execute 2.0).

Tests: SQLite **54 passed** (51 + 3 new: bookkeeping-written-at-claim,
retry preserves started_at/appends worker_id, CAS stale-candidate guard);
PostgreSQL 16 **54 passed** three consecutive runs (raw claim path
exercised, including the queue `ANY(%s)` clause via the test settings'
QUEUES; the plow-pg container on 54329 was left running for re-verification).

E2E re-measure (same harness `run_e2e`, same container, 1 run per cell —
indicative; raw JSON in benchmarks/results-raw-2026-08-13-inc4.json):

| Cell | ox before (inc-3) | ox after | tasksdb (same session) |
| --- | --- | --- | --- |
| c1 | 37.1 s (54 t/s, 18.5 ms/task) | 22.6 s (89 t/s, 11.3 ms/task) | 28.0 s (71 t/s, 14.0 ms/task) |
| c4 | 8.3 s (242 t/s) | 5.0 s (398 t/s) | 5.4 s (373 t/s) |

ox now leads both e2e cells on this single run. The c4 caveat (4 threads
vs 4 processes) still applies and now cuts against ox, which wins despite
sharing one GIL. A full 3-run interleaved matrix should be re-run before
quoting these numbers anywhere public.

## Increment 5 (2026-08-13): recurring tasks

Closes the main competitive gap against Solid Queue's recurring tasks.
Moved out of the Pro-tier list: table stakes, not an upsell.

Design decisions:

- **Schedules are configuration, not data.** Declared under
  `OPTIONS["SCHEDULES"]` of the backend entry, mirroring Solid Queue's
  static recurring config and Oban's cron plugin: versioned and deployed
  with the code that defines the tasks. The database holds only the
  dispatch log (`OxScheduleTick`), never the schedule definitions.
- **Coordination is the unique constraint** on
  `(schedule_name, scheduled_for)`. Dispatch wraps the task enqueue and
  the tick INSERT in one transaction; when two workers race the same
  tick, the loser's IntegrityError rolls back its task row with it. Same
  durability construction as transactional enqueue, and no reliance on
  wall-clock uniqueness: every worker derives identical tick datetimes
  from the cron expression.
- **Every worker dispatches** alongside its polling (no separate
  scheduler process to deploy, matching the single-process philosophy of
  the reaper). A pass is one aggregate query plus INSERTs when due;
  cadence is `schedule_interval`, default `max(1, min(poll_interval, 30))`.
- **Missed-tick policy: fire the latest missed tick once on recovery,
  skip older ones.** Rationale: recurring jobs are overwhelmingly
  state-refreshing (digests, syncs, pruning) where the latest run
  restores state and a replayed backlog is N times the cost for zero
  benefit, plus a thundering herd at recovery. Skipping entirely (Solid
  Queue's behavior: it only schedules future ticks from memory) silently
  loses a nightly job to an unlucky deploy window, which is the complaint
  users actually file. Firing exactly one is the middle with bounded work
  and no silent loss.
- **Anchor rows** (`task=None`): the first time a worker sees a schedule
  with no tick history it records the current tick without firing, so a
  schedule deployed at noon does not immediately fire its 03:00 job for a
  time predating its existence; it first fires at the next tick. From
  then on the latest row per schedule is the recovery baseline, which is
  why pruning must never delete it.
- **Cron parser is stdlib-only** (`cron.py`); croniter was not needed for
  five-field vixie syntax: lists/ranges/steps, month and weekday names,
  0 and 7 both Sunday, the dom/dow OR rule, and the @macros.
  Never-matching expressions ("0 0 30 2 *") are rejected at parse time.
  `following()`/`previous()` correct month, then day, then hour, then
  minute, so they jump instead of stepping minute-wise; the search bound
  is 366*9 days because the longest legal gap is the eight-year Feb 29
  hole around 2100 (2096 -> 2104).
- **Timezone**: cron fields are wall-clock in the project's current
  timezone (`localtime` -> naive math -> `make_aware`); with `USE_TZ =
  False` everything stays naive. DST edges resolve via zoneinfo fold
  handling without raising; a 02:30 tick on a spring-forward day shifts
  rather than erroring. Not exhaustively tested; documented behavior is
  best-effort around transitions.
- **Fail fast on bad config**: `schedules_from_options` raises
  ImproperlyConfigured at Worker startup, and `OxBackend.check()`
  surfaces the same problems as system check `django_ox.E002` (task
  path imports to a Task, cron parses and can fire, args/kwargs
  JSON-serializable, queue/priority validated via `Task.using`, which
  re-runs `validate_task`).
- **Signal caveat**: `task_enqueued` fires inside the dispatch
  transaction; when the unique race rolls it back, the signal has already
  fired for a task that never committed. Same class of caveat as the
  existing enqueue-inside-rollback note; core gives no guidance.
- **Pruning**: `ox_prune` now also deletes tick rows older than the
  cutoff, always keeping each schedule's latest (the anchor). The
  `task` FK is SET_NULL, which keeps the tick log as an audit trail when
  task rows are pruned, at the cost of a deletion-collector SELECT and a
  SET NULL UPDATE per task batch delete (batching test updated 7 -> 15
  queries with the breakdown in its comment).

Tests 54 -> 141: cron parsing (field forms, names, macros, rejects,
never-match), following/previous walks (month/year rollover, leap day,
OR rule, inclusivity, aware-datetime rejection, cross-consistency),
schedule loading (all rejection paths, overrides, backend rebinding,
system check), dispatch (anchoring, idempotence within a tick, firing,
missed-tick collapse, simulated two-scheduler race through the unique
constraint, sequential agreement, USE_TZ off), end-to-end through the
real run loop, and tick pruning. Dispatch tests pin `timezone.now` mid-
minute so a minute boundary cannot roll over inside a test.

Verification (2026-08-13, from date): SQLite **141 passed** four
consecutive runs (plus five runs of the three new/changed test files
alone). PostgreSQL 16 (docker, fresh `plow-pg` container per the
settings_postgres recipe) **141 passed** three consecutive runs; the
unique-constraint race and IntegrityError rollback are exercised on both
databases. The plow-pg container on 54329 was left running for
re-verification, matching increment 4's handling.

## Adversarial review fixes (2026-08-13, night session)

Fix cycle from the confirmed adversarial-review findings (repro scripts in
/tmp/ox_attack/). Three code fixes, each with tests; one documented
limitation left unfixed by decision.

- **Cross-backend schedule-name collision** (attack_dispatch.py section
  G): tick rows are keyed on (schedule_name, scheduled_for) with no
  backend column, so two backends defining the same schedule name
  silently starved one of them. Now rejected at config time:
  `schedule_name_collisions()` in schedules.py, surfaced as system check
  `django_ox.E003` in `OxBackend.check()` and as
  ImproperlyConfigured at Worker init.
- **Future-dated tick suppression** (attack_addendum.py SKEW): a tick row
  with scheduled_for in the future (a clock-skewed worker's write)
  suppressed dispatch fleet-wide until wall clock passed it. The
  latest-tick guard in `dispatch_schedules()` now also requires
  `last <= now`, so a future row cannot suppress ticks that are due; the
  unique constraint still protects the future instant itself.
- **Oversized cron steps**: `*/61` in the minute field silently parsed to
  `(0,)`. Steps larger than the field's range now raise ValueError at
  parse time, consistent with the other rejections. `*/60` (equal to the
  range) still parses.
- **Known limitation, documented not fixed: DST fall-back double fire**
  (attack_addendum.py C2). A daily schedule inside the repeated hour
  (e.g. `30 1 * * *` on 2026-11-01 America/New_York) fires twice on the
  fall-back day, once per pass of the repeated wall-clock hour; the fold
  survives into make_aware, so the two passes store distinct UTC
  instants. At most one duplicate run per year, consistent with
  at-least-once delivery; hourly jobs in the repeated hour intentionally
  fire each pass. Also documented in docs/recurring-tasks.md.
- Docs additions from the same review: */n in dom/dow follows the
  Python-ecosystem (croniter-style) OR interpretation, not vixie's
  star-flag AND (cron.py docstring corrected); backward TIME_ZONE
  changes can re-fire a wall-clock label once; removed schedules' latest
  tick rows survive prune and are harmless; workers should run under a
  supervisor with restart because transient DB errors terminate the run
  loop.

Tests 141 -> 151.

## Increment 6 (2026-08-16): observability

- `stats.py`: read-only metrics as plain ORM queries, no new state or
  signals. `queue_stats` (raw per-queue status counts), `ready_count` and
  `oldest_ready_age` (both mirror the worker's dequeue predicate, so
  deferred tasks are not backlog; age measured from eligibility, i.e.
  run_after when set), `throughput` and `failure_rate` (terminal outcomes
  over a trailing window; rate is None when nothing finished), and
  `last_claim_age`.
- `ox_health`: DB reachability always; `--max-backlog`, `--max-age`,
  `--worker-timeout` thresholds off by default; `--queue` scoping;
  CommandError with all failing checks on one line (exit 1). Worker
  liveness decision: there is no worker/heartbeat table and none was
  added; the only worker trace is claim activity (`last_attempted_at`,
  written by every claim UPDATE), so `--worker-timeout` measures that and
  the docs steer bursty queues to `--max-age` instead. `locked_at` was
  rejected as the signal because it is cleared at task finish.
- Structured logging: lifecycle events on the `django_ox` logger carry
  stable extra keys (event, task_id, task_path, queue, attempt,
  worker_id, plus duration_ms/exception/status/schedule where relevant).
  New records: task_claimed and task_started at DEBUG (once per attempt),
  task_succeeded at INFO, terminal task_failed at ERROR. Existing
  retry/reclaim/dispatch/start/stop messages kept their text and gained
  extras. claim_one() became a logging wrapper over _claim_one() so the
  event is emitted once for all three claim paths.
- Docs: new Monitoring page (stats API, ox_health with k8s liveness and
  cron examples, event/key tables, recipes); production.md Monitoring
  section now defers to it; configuration.md gained the ox_health flag
  table; README gained Health and monitoring; changelog updated.

Tests 151 -> 189 (test_stats, test_health, test_logging). SQLite 189
passed (including `--cov`, 94.5% against the 91% gate); PostgreSQL 16
189 passed three consecutive runs (plow-pg container on 54329, left
running). ruff check, ruff format --check, mypy --strict, and
`mkdocs build --strict` all clean.

## Soak and chaos verification (2026-08-16)

New harness `benchmarks/soak.py` + `benchmarks/soaksite/` (stdlib +
existing dev deps only; ruff excludes benchmarks/, so gates are
unaffected). 21.5 min under load against PG16 (plow-pg, `soak_ox`
database): 12 min steady at 30 tasks/s (3 producers, 3 workers x c4),
9 min identical load with 15 worker SIGKILLs + restarts, plus a forced
crash-restart of a worker holding 2 claimed sleeping tasks. Every
execution writes nonce'd phase rows to a soak_ledger side table so
at-least-once vs exactly-once is measured, not assumed. Result: 37/37
assertions passed across the three scenarios, no bug found, no code
change. 37,802 tasks all terminal; 39 kill-interrupted claims (4 of them
in the claim-to-body-entry window) all reclaimed and re-executed in
15.5-20.1 s against a 37.5 s bound (LOCK_TIMEOUT 15 + reap 7.5 +
margin); attempts == len(worker_ids) held on every row under SIGKILL
(first real-kill exercise of the increment-4 atomic claim bookkeeping);
zero double completions (observation, not a guarantee; the design stays
at-least-once); worker RSS flat at ~58.5-58.9 MB after boot over 12 min.
Evidence: benchmarks/SOAK-2026-08-16.md, raw JSON
soak-results-raw-2026-08-16.json (invocation `soak.py --seed 51`).
Post-run gates: SQLite 189 passed, PostgreSQL 189 passed, ruff check +
format --check, mypy --strict, mkdocs build --strict all clean.

## Security posture + API stability (2026-08-16)

Threat pass on the task-args path. Findings:

- **task_path was unconstrained (medium, fixed).** `task_from_db()` ran
  `import_string(task_path)` on the untrusted row value and, via an
  `else obj` fallback, executed whatever it resolved to even when that was
  not a Task. A row written by a compromised DB user or a malicious enqueue
  could name any importable callable (e.g. `os.system`) and have the worker
  call it with attacker-chosen JSON args: database-write escalated to
  worker code-execution. Fix: `task_from_db()` now requires the resolved
  object to be a `django.tasks` Task and raises `ImportError` otherwise, so
  the worker only runs functions registered with `@task`. worker.execute
  records that as a failed attempt; get_result propagates it (unchanged
  handling of un-resolvable rows). Regression tests: a stored `os.system`
  row fails with `builtins.ImportError` and the side-effect file is never
  created; get_result on the same row raises ImportError
  (tests/test_backend.py, +2 tests, 189 -> 191).
- **Serialization (no finding).** args/kwargs/return_value are JSONField
  via normalize_json; no pickle or eval anywhere. `import json` in worker.py
  is only `json.dumps([worker_id])` for the jsonb claim param.
- **Secrets in the table (posture, documented).** args, kwargs,
  return_value, and error tracebacks are plaintext. Documented in
  SECURITY.md: pass references not secrets, keep secrets out of exception
  messages. Tracebacks are format_exception output (frames + message, no
  locals). Referenced the Pro encrypted-payloads item (already on pro.md).
- **Raw SQL (no finding).** POSTGRES_CLAIM_SQL.format only injects
  `OxTask._meta.db_table` and a branch-selected constant clause; all runtime
  values are bound params. No string-interpolated row/user data.

pip-audit: installed pip-audit 2.10.1 into /tmp/ox-venv; audited the built
wheel's runtime tree in an isolated venv (asgiref 3.12.1, Django 6.1,
sqlparse 0.6.0). No known vulnerabilities found. No runtime dep changes.

Docs: SECURITY.md extended with threat model (trusted: DB, settings, code;
not trusted: args/return values, stored task_path), JSON serialization
note, secrets guidance, SQL note; supported-versions table unchanged. New
docs/stability.md (public API surface, pre-1.0 SemVer with CHANGELOG
notice, one-minor deprecation window, Python 3.12-3.14 x Django 6.0-6.1
matrix from CI); added to mkdocs nav after Benchmarks; README gained a
Stability section linking it. CHANGELOG 0.1.0 gained a Security subsection.

Tests 189 -> 191. Gates: SQLite 191 passed, PostgreSQL 16 191 passed
(DJANGO_SETTINGS_MODULE=tests.settings_postgres, plow-pg :54329), ruff
check + format --check clean, mypy --strict src/ clean (18 files), mkdocs
build --strict exit 0. No changes to dist/, benchmarks/, no runtime deps.
