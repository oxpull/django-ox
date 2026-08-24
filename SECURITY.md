# Security policy

## Threat model

django-ox turns a database table into a work queue: a worker process reads
rows and executes code named by them. What it trusts and what it does not
follows from that.

**Trusted.** The application database, the Django settings, and the
application's own code are the trust boundary. django-ox trusts that:

- Rows in its tables were written by the application through `enqueue()` (or
  by a schedule declared in settings), not by an untrusted party.
- The `TASKS` setting and every backend's `OPTIONS` (including `SCHEDULES`)
  are controlled by whoever deploys the project.
- The code importable on the worker's Python path is the code the project
  intends to run.

**Not trusted.** Task arguments and return values are treated as opaque
data, never as code. A stored `task_path` is not trusted to name any
callable: `task_from_db()` resolves it and rejects anything that is not a
`django.tasks` Task, so a worker never invokes an arbitrary dotted path
pulled from a row. A row naming, say, `os.system` fails as an
un-resolvable task instead of executing; only functions the application
registered with `@task` can run. This bounds what a malicious or corrupted
row can do: it can run one of your registered tasks with attacker-chosen
JSON arguments, but it cannot call an unregistered callable, and it cannot
smuggle code through the arguments (they are JSON, never pickle).

The residual escalation from database-write to worker code-execution is the
usual one for any database-backed queue: a `task_path` that imports a module
present on the worker's path runs that module's import-time side effects.
Putting a module on the worker's path already requires code-deploy access,
which is above the queue in the trust order.

## Serialization

Arguments, keyword arguments, and return values are stored as JSON
(`JSONField`, normalized through `django.utils.json.normalize_json`).
django-ox never pickles or `eval`s task data. There is no code path that
deserializes a payload into executable objects.

## Secrets in task data

The task table stores `args`, `kwargs`, `return_value`, and per-attempt
error tracebacks in plaintext. Anyone who can read the table, a database
backup, or those log records can read them.

- **Do not pass secrets as task arguments.** Pass a reference (a primary
  key, a settings name, a secrets-manager handle) and resolve it inside the
  task, so the secret never lands in a row.
- Exception messages become part of the stored traceback. A task that
  raises `ValueError(f"bad token {token}")` writes that token to the
  `errors` column. Keep secrets out of exception text.
- Tracebacks are standard `traceback.format_exception()` output: stack
  frames and the exception message, without local-variable values.

Encrypting arguments at rest is not offered in either tier today; the plaintext
posture above is what both packages do.

## The admin page

When `django.contrib.admin` is installed, the task detail page renders
`args`, `kwargs`, `return_value` and every stored traceback to any user
holding the `view_oxtask` permission; grant it as you would read access to
the table itself. The `change_oxtask` permission enables the retry and
discard actions, and a retry re-runs a stored task with its stored
arguments, so treat that permission as the ability to execute the
application's registered tasks.

## The metrics endpoint

Mounting `django_ox.urls` exposes `/ox/metrics` with no authentication of
its own. It reveals queue names and traffic shape, never task data. Put it
behind the project's own policy before it is reachable from outside; the
[Monitoring](docs/monitoring.md#prometheus) page shows two one-line guards.

## SQL

Every query is built through the Django ORM. The one raw statement (the
PostgreSQL `UPDATE ... FOR UPDATE SKIP LOCKED` claim) interpolates only the
model's own table name and a fixed clause chosen by branch; all runtime
values are passed as bound parameters. No query is assembled from row data
or user input by string formatting.

## Supported versions

| Version | Supported |
| ------- | --------- |
| 0.3.x   | yes       |
| older   | no        |

## Reporting a vulnerability

Report vulnerabilities privately via GitHub security advisories:
[Report a vulnerability](https://github.com/oxpull/django-ox/security/advisories/new).
Do not open a public issue for anything security-sensitive.

You will get an acknowledgement within three business days. Please include a
reproduction or a clear description of the affected code path.
