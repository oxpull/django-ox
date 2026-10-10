import json
import logging
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from django.core.management.base import CommandError, CommandParser
from django.db import DatabaseError, connections, transaction
from django.db.models import Exists, F, OuterRef, QuerySet
from django.utils import timezone

from django_ox import _contention, stored
from django_ox._stored_read import TickRow, can_read_on, read_ticks
from django_ox.durations import parse_duration
from django_ox.management._database import DatabaseCommand
from django_ox.models import OxScheduleTick, OxTask

logger = logging.getLogger("django_ox")

# Reserve a full day so conversion to any connection timezone cannot
# underflow datetime.min. Apply the same floor on every engine so the
# answer does not depend on the engine or connection timezone.
_CUTOFF_FLOOR = datetime.min + timedelta(days=1)

#: --older-than when it is not given.
_DEFAULT_OLDER_THAN = "7d"

#: The most unreadable ticks a run names one by one, in its warning, its log
#: and its JSON. The counts cover every one.
_NAMED_LIMIT = 100

# What the command says about tick rows it cannot read, all of it in this
# block. `key` is a schedule key as repr() quotes it, `pk` a tick row's
# primary key, `reason` why the row cannot be read, already safe to print.

#: The flag without anything to remove.
_PURGE_NEEDS_A_TARGET = (
    "--purge-unreadable-ticks needs at least one --schedule-key or --tick-pk. On "
    "MySQL, --schedule-key uses the tick table's collation, which may match keys "
    "differing in case, accents or trailing spaces. Inspect --dry-run before removing "
    "rows, or use --tick-pk to target individual rows."
)
#: A target without the flag that acts on it.
_TARGET_NEEDS_PURGE = "Use --schedule-key and --tick-pk with --purge-unreadable-ticks."
#: The flag beside options that prune. `flags` lists them, comma-separated.
_PURGE_COMBINED = (
    "--purge-unreadable-ticks cannot be combined with {flags}. Run ordinary pruning "
    "separately."
)
#: One unreadable tick row, in a list.
_TICK_LINE = "  tick {pk}, schedule {key}: {reason}"
#: The end of a list cut at _NAMED_LIMIT.
_MORE_LINE = "  and {count} more."
#: Ordinary pruning met tick rows it cannot read and kept them. Then one
#: _TICK_LINE each, _MORE_LINE if the list was cut, and _UNREADABLE_REMEDY.
_UNREADABLE_KEPT = (
    "Warning: this run encountered {count} schedule tick row(s) it cannot read. All "
    "were kept. Unreadable tick history can block automatic dispatch."
)
_UNREADABLE_REMEDY = (
    "To remove unreadable ticks, stop the workers that dispatch these schedules. Run "
    "ox_prune --purge-unreadable-ticks with --schedule-key KEY or --tick-pk PK and "
    "--dry-run. Use --tick-pk PK if the schedule key cannot be read. Check the listed "
    "rows and the tasks those ticks recorded. Run the same command without --dry-run, "
    "then start the workers again."
)
#: The stdout line beside the deletion counts.
_UNREADABLE_COUNT = (
    "{verb} {count} unreadable schedule tick row(s) encountered in this run."
)
#: Old ticks kept because each is the newest one its schedule has that reads.
_ANCHORS_COUNT = (
    "{verb} {count} schedule tick row(s) scheduled before {cutoff}, each the "
    "newest readable tick of its schedule."
)
#: What --purge-unreadable-ticks removed, or would. Then one _TICK_LINE each.
_PURGE_COUNT = "{verb} {count} unreadable schedule tick row(s){colon}"
#: A tick named by --tick-pk whose scheduled time reads.
_NAMED_TICK_READS = (
    "Kept tick {pk}, schedule {key}: the row is readable, with scheduled_for {at}."
)
#: A tick that read when it came to be removed, repaired since it was found.
_TICK_REPAIRED = (
    "Kept tick {pk}, schedule {key}: the row is now readable, with scheduled_for {at}."
)
#: A tick named by --tick-pk that is not there.
_NO_SUCH_TICK = "No tick row {pk}."
#: Written whenever --purge-unreadable-ticks has something to remove.
_HISTORY_LOSS = (
    "Warning: removing dispatch history can make a schedule run a tick again, or "
    "anchor again and skip its next tick. Stop the workers that dispatch these "
    "schedules before removing history. Run with --dry-run first and check the listed "
    "rows and the tasks those ticks recorded. Then run without --dry-run and start the "
    "workers again."
)
#: The log line for each tick row ordinary pruning kept because it cannot be
#: read, and for those past _NAMED_LIMIT together.
_LOG_TICK_KEPT = "Tick %s, schedule key %s, cannot be read and was kept: %s"
_LOG_TICKS_KEPT = "%d more schedule tick rows cannot be read and were kept"
#: The log line for each tick row --purge-unreadable-ticks removed.
_LOG_TICK_REMOVED = "Removed tick %s, schedule key %s, because it could not be read: %s"


@dataclass
class _TickTally:
    """What a pass over the tick log found and did, as far as it got."""

    #: Tick rows deleted, or that a dry run would delete.
    deleted: int = 0
    #: Tick rows past the cutoff kept because each was its schedule's newest
    #: readable tick.
    anchors: int = 0
    #: Tick rows kept because they cannot be read, by primary key.
    unreadable: dict[int, TickRow] = field(default_factory=dict)

    def keep(self, row: TickRow) -> None:
        """Count a tick row that cannot be read, once however often it is met."""
        # Without the exception that says why: it holds on to the whole
        # batch the row was read in, for as long as the tally lives.
        self.unreadable.setdefault(row.pk, replace(row, cause=None))


def _named(row: TickRow) -> dict[str, Any]:
    """An unreadable tick row as the JSON output names it."""
    return {"pk": row.pk, "schedule_name": row.key, "reason": row.reason}


def _chunks(values: Sequence[int], size: int) -> Iterator[list[int]]:
    for start in range(0, len(values), size):
        yield list(values[start : start + size])


def _possible_keys(numbers: Sequence[int], alias: str) -> list[int]:
    """
    Those of these numbers a tick row's primary key can be on this database.

    A number outside the column's range names no row. It is left out of the
    read rather than sent: SQLite's driver refuses to bind an integer wider
    than the column, and that refusal is not a row that does not read.
    """
    low, high = connections[alias].ops.integer_field_range(
        OxScheduleTick._meta.pk.get_internal_type()
    )
    return [number for number in numbers if low <= number <= high]


def _delete_ticks(pks: list[int], alias: str) -> int:
    """
    Delete tick rows by primary key alone, and say how many went.

    Django's delete() loads every row it deletes as soon as anything listens
    for its delete signals, and a tick that cannot be read cannot be loaded.
    So the rows go in one DELETE of this command's own, which reads nothing:
    on the connection the command reads and writes through, the table and
    its key quoted as that database quotes them, and the keys passed as
    parameters, never more of them than the batch the caller is on.

    Every tick row this command removes goes this way, whether or not one
    that cannot be read is among them, so no pre_delete or post_delete is
    sent for a tick row it removes. Task rows are deleted by Django's
    delete(), which sends theirs.
    """
    if not pks:
        return 0
    connection = connections[alias]
    table = connection.ops.quote_name(OxScheduleTick._meta.db_table)
    key = connection.ops.quote_name(cast(str, OxScheduleTick._meta.pk.column))
    placeholders = ", ".join(["%s"] * len(pks))
    with connection.cursor() as cursor:
        cursor.execute(
            f"DELETE FROM {table} WHERE {key} IN ({placeholders})",  # noqa: S608
            pks,
        )
        return int(cursor.rowcount)


def _older_than_their_anchor(
    rows: list[TickRow], anchors: dict[str, int], alias: str
) -> set[int]:
    """
    Which of these tick rows sort before the tick kept for their schedule,
    that tick still being there.

    `anchors` is each schedule key's kept tick, by primary key. A row with
    a tick of its schedule above it is not the schedule's newest, which is
    all it takes for the row to go. The rows and their anchors are asked
    for by primary key, in the database's own order, so no tick is read
    and the cost is the batch's, however long a schedule's history is.
    Rows of one schedule sort by their tick among themselves, whatever the
    column's collation makes of two keys.

    A plain read, never a locking one: see `_newest_of_their_schedules`.
    """
    wanted = {anchors[row.key] for row in rows if row.key in anchors}
    if not wanted:
        return set()
    place = {
        pk: index
        for index, pk in enumerate(
            OxScheduleTick.objects.using(alias)
            .filter(pk__in=[*(row.pk for row in rows), *wanted])
            .order_by("schedule_name", "scheduled_for")
            .values_list("pk", flat=True)
        )
    }
    older: set[int] = set()
    for row in rows:
        anchor = anchors.get(row.key)
        if anchor is None:
            continue
        here, above = place.get(row.pk), place.get(anchor)
        if here is not None and above is not None and here < above:
            older.add(row.pk)
    return older


def _newest_of_their_schedules(pks: list[int], alias: str) -> set[int]:
    """
    Which of these tick rows is its schedule's newest tick right now.

    Asked of the database by key, in the database's own order, so no tick
    is read: a row is its schedule's newest when no row of the same
    schedule sorts after it. A plain read, never a locking one. A worker's
    first-sighting read skips locked tick rows, and a schedule whose every
    tick it skipped would look like one it had never seen.

    For the few rows `_older_than_their_anchor` cannot answer for. The
    subquery runs once a row, and on MySQL each run walks the schedule's
    history up to the row.
    """
    if not pks:
        return set()
    ticks = OxScheduleTick.objects.using(alias)
    newer = ticks.filter(
        schedule_name=OuterRef("schedule_name"),
        scheduled_for__gt=OuterRef("scheduled_for"),
    )
    return set(
        ticks.filter(pk__in=pks).filter(~Exists(newer)).values_list("pk", flat=True)
    )


class Command(DatabaseCommand):
    help = "Delete finished task rows older than a cutoff."

    _task_rows = 0
    #: What the tick phase has found and done, set afresh by each run.
    _ticks: _TickTally

    def add_arguments(self, parser: CommandParser) -> None:
        super().add_arguments(parser)
        parser.add_argument(
            "--queue",
            default=None,
            help="Restrict pruning to one queue's task rows (default: all queues).",
        )
        parser.add_argument(
            "--older-than",
            default=None,
            help=(
                "How long a task must have been finished before it is "
                "deleted. Forms: 7d, 24h, 90m, 45s, or a plain number of "
                f"seconds (default: {_DEFAULT_OLDER_THAN})."
            ),
        )
        parser.add_argument(
            "--include-failed",
            action="store_true",
            help=(
                "Also delete FAILED and LOST rows. Off by default: they "
                "hold the per-attempt tracebacks and only go when asked."
            ),
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=1000,
            help=(
                "Rows deleted per DELETE statement, keeping locks and IN "
                "clauses bounded on large tables (default: %(default)s)."
            ),
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report how many rows would be deleted without deleting any.",
        )
        parser.add_argument(
            "--format",
            choices=["text", "json"],
            default="text",
            help=(
                "Output format. json prints one object with the same figures "
                "on stdout; the exit status is the same either way "
                "(default: %(default)s)."
            ),
        )
        parser.add_argument(
            "--purge-unreadable-ticks",
            action="store_true",
            help=(
                "Remove unreadable tick rows targeted by --schedule-key or --tick-pk. "
                "No schedule or task rows are removed. On MySQL, --schedule-key uses "
                "the tick table's collation, which may match keys differing in case, "
                "accents or trailing spaces. Stop the workers that dispatch these "
                "schedules. Run with --dry-run first and check the listed rows and the "
                "tasks those ticks recorded. Then run without --dry-run and start the "
                "workers again. Removing history can repeat a tick or cause "
                "re-anchoring that skips the next tick."
            ),
        )
        parser.add_argument(
            "--schedule-key",
            action="append",
            default=None,
            metavar="KEY",
            help=(
                "With --purge-unreadable-ticks, select unreadable ticks by schedule "
                "key: a settings schedule's name, or db:<pk> for a stored schedule. "
                "Repeatable. On MySQL, matching uses the tick table's collation, which "
                "may include keys differing in case, accents or trailing spaces. "
                "Inspect --dry-run, or use --tick-pk to target individual rows."
            ),
        )
        parser.add_argument(
            "--tick-pk",
            action="append",
            type=int,
            default=None,
            metavar="PK",
            help=(
                "With --purge-unreadable-ticks, one tick row to remove by "
                "primary key, if it cannot be read. Repeatable."
            ),
        )

    def handle(self, *args: Any, **options: Any) -> None:
        if options["batch_size"] < 1:
            raise CommandError("--batch-size must be a positive integer.")
        keys = list(dict.fromkeys(options["schedule_key"] or []))
        tick_pks = list(dict.fromkeys(options["tick_pk"] or []))
        if options["purge_unreadable_ticks"]:
            # Asked for one thing by name, so nothing it was not asked for:
            # a prune option beside it would otherwise do nothing, silently.
            combined = [
                flag
                for flag, given in (
                    ("--queue", options["queue"] is not None),
                    ("--older-than", options["older_than"] is not None),
                    ("--include-failed", options["include_failed"]),
                )
                if given
            ]
            if combined:
                raise CommandError(_PURGE_COMBINED.format(flags=", ".join(combined)))
            if not keys and not tick_pks:
                raise CommandError(_PURGE_NEEDS_A_TARGET)
            self._purge(self.database(options), keys, tick_pks, options)
            return
        if keys or tick_pks:
            raise CommandError(_TARGET_NEEDS_PURGE)
        # Once, before the first statement. Every queryset below is built on
        # this alias, so the rows the command reads are the rows it deletes.
        alias = self.database(options)
        older_than = options["older_than"]
        if older_than is None:
            older_than = _DEFAULT_OLDER_THAN
        try:
            cutoff = timezone.now() - parse_duration(older_than)
        except OverflowError:
            # A duration timedelta can hold may still reach back past year 1.
            raise CommandError(
                f"Invalid duration {older_than!r}; it is out of range."
            ) from None
        floor = _CUTOFF_FLOOR
        if timezone.is_aware(cutoff):
            floor = timezone.make_aware(_CUTOFF_FLOOR, UTC)
        if cutoff < floor:
            raise CommandError(f"Invalid duration {older_than!r}; it is out of range.")
        # DISCARDED prunes with SUCCESSFUL: the row is already closed, so
        # there is nothing left on it to wait for. WAITING is in neither
        # list, because that task has not run.
        statuses = [OxTask.Status.SUCCESSFUL, OxTask.Status.DISCARDED]
        if options["include_failed"]:
            # LOST goes with FAILED rather than with SUCCESSFUL: it is the
            # same kind of row to keep, holding tracebacks and the note that
            # the lease was lost, and the same kind to discard. Leaving it
            # out of both would make it the one status that never prunes.
            statuses += [OxTask.Status.FAILED, OxTask.Status.LOST]
        prunable = OxTask.objects.using(alias).filter(
            status__in=statuses, finished_at__lt=cutoff
        )
        label = "/".join(statuses)
        queue: str | None = options["queue"]
        if queue is not None:
            # Task rows only. The tick log below is pruned for every schedule
            # whatever --queue names: deleting a task clears its ticks' link
            # to it, and an anchor never had one, so a tick's queue cannot be
            # read reliably.
            prunable = prunable.filter(queue_name=queue)
            label = f"{label} (queue {queue})"

        as_json = options["format"] == "json"
        batch_size = options["batch_size"]
        self._ticks = _TickTally()

        if options["dry_run"]:
            task_count = prunable.count()
            self._prune_ticks(alias, cutoff, batch_size, write=False)
            if as_json:
                self._write_json(
                    queue=queue,
                    cutoff=cutoff,
                    statuses=statuses,
                    task_rows=task_count,
                    dry_run=True,
                )
            else:
                self.stdout.write(
                    f"Would delete {task_count} {label} task row(s) "
                    f"finished before {cutoff.isoformat()}."
                )
                self.stdout.write(
                    f"Would delete {self._ticks.deleted} schedule tick row(s) "
                    f"scheduled before {cutoff.isoformat()}."
                )
                self._write_kept("Would keep", cutoff)
            self._warn_unreadable(alias)
            return

        self._task_rows = 0
        try:
            deleted = self._delete_tasks_in_batches(prunable, batch_size, label, alias)
            # Written before the ticks are touched, the way it was before
            # --format text gained a second deletion behind the same try.
            # A tick failure must not cost the operator the task count.
            if not as_json:
                self.stdout.write(
                    f"Deleted {deleted} {label} task row(s) "
                    f"finished before {cutoff.isoformat()}."
                )
            self._prune_ticks(alias, cutoff, batch_size, write=True)
        except (CommandError, DatabaseError):
            # DatabaseError too: the tick phase has no retry wrapper, and
            # is_contention() only knows PostgreSQL and MySQL codes, so a
            # lock on SQLite or a failure in the tick phase arrives raw.
            # Either way the committed rows are gone and their count is the
            # one thing the caller cannot recover afterwards.
            if as_json:
                self._write_json(
                    queue=queue,
                    cutoff=cutoff,
                    statuses=statuses,
                    task_rows=self._task_rows,
                    dry_run=False,
                )
            raise

        if as_json:
            self._write_json(
                queue=queue,
                cutoff=cutoff,
                statuses=statuses,
                task_rows=deleted,
                dry_run=False,
            )
        else:
            self.stdout.write(
                f"Deleted {self._ticks.deleted} schedule tick row(s) "
                f"scheduled before {cutoff.isoformat()}."
            )
            self._write_kept("Kept", cutoff)
        self._warn_unreadable(alias)

    def _write_json(
        self,
        *,
        queue: str | None,
        cutoff: datetime,
        statuses: Sequence[str],
        task_rows: int,
        dry_run: bool,
    ) -> None:
        ticks = self._ticks
        self.stdout.write(
            json.dumps(
                {
                    "queue": queue,
                    "cutoff": cutoff.isoformat(),
                    "statuses": list(statuses),
                    "task_rows": task_rows,
                    "tick_rows": ticks.deleted,
                    "unreadable_tick_rows": len(ticks.unreadable),
                    "anchor_tick_rows": ticks.anchors,
                    "unreadable_ticks": [
                        _named(row)
                        for row in list(ticks.unreadable.values())[:_NAMED_LIMIT]
                    ],
                    "dry_run": dry_run,
                }
            )
        )

    def _write_kept(self, verb: str, cutoff: datetime) -> None:
        """The report's lines for tick rows kept that the cutoff would not keep."""
        ticks = self._ticks
        if ticks.unreadable:
            self.stdout.write(
                _UNREADABLE_COUNT.format(verb=verb, count=len(ticks.unreadable))
            )
        if ticks.anchors:
            self.stdout.write(
                _ANCHORS_COUNT.format(
                    verb=verb, count=ticks.anchors, cutoff=cutoff.isoformat()
                )
            )

    def _warn_unreadable(self, alias: str) -> None:
        """
        Say on stderr, and in the log, which tick rows were kept because they
        cannot be read. On stderr whatever the format, so a cron line sees
        it and JSON on stdout stays one object.
        """
        rows = list(self._ticks.unreadable.values())
        if not rows:
            return
        listed = rows[:_NAMED_LIMIT]
        lines = [_UNREADABLE_KEPT.format(count=len(rows))]
        lines += [
            _TICK_LINE.format(pk=row.pk, key=repr(row.key), reason=row.reason)
            for row in listed
        ]
        if len(rows) > len(listed):
            lines.append(_MORE_LINE.format(count=len(rows) - len(listed)))
        lines.append(_UNREADABLE_REMEDY)
        self.stderr.write("\n".join(lines), style_func=self.style.WARNING)
        # Only to handlers someone configured. Without one, Python's last
        # resort would print each record on stderr again, below the warning
        # that already says it.
        if not logger.hasHandlers():
            return
        for row in listed:
            logger.warning(
                _LOG_TICK_KEPT,
                row.pk,
                repr(row.key),
                row.reason,
                extra={
                    "event": "schedule_tick_unreadable",
                    "surface": "prune",
                    "database": alias,
                    # Escaped as the message beside it is: the key is
                    # whatever the tick row holds.
                    "schedule_key": stored._printable(row.key),
                    "tick_pk": row.pk,
                    "reason": row.reason,
                },
            )
        if len(rows) > len(listed):
            logger.warning(
                _LOG_TICKS_KEPT,
                len(rows) - len(listed),
                extra={
                    "event": "schedule_tick_unreadable",
                    "surface": "prune",
                    "database": alias,
                    "unlisted": len(rows) - len(listed),
                },
            )

    # -- the tick log ------------------------------------------------------

    def _prune_ticks(
        self, alias: str, cutoff: datetime, batch_size: int, *, write: bool
    ) -> None:
        """
        Delete tick rows scheduled before `cutoff`, keeping two kinds.

        Each schedule's newest tick that reads is the baseline the dispatcher
        measures missed ticks against, and deleting it would make the
        schedule anchor again and skip a pending tick, so it stays whatever
        its age. A tick that cannot be read stays too, and is reported: what
        it meant is unknown, and removing dispatch history on a guess can
        run a tick twice or anchor over one. --purge-unreadable-ticks removes
        those by name.

        No tick is loaded through Django's converters, which raise on a
        value they cannot read and take the whole result with them. Ticks
        are fetched as text and decoded one by one. The rows a cutoff
        selects are visited a batch at a time in key order, since those kept
        stay selected, and a batch's rows are checked once more, just before
        they go, for any that has become its schedule's newest tick: because
        the tick kept for its schedule has gone since, or because the row
        itself was dispatched since.

        A row can hold a key the first read of the keys did not return. On
        MySQL the column's collation takes two spellings of a key for one
        key, by case, by an accent or by a trailing space, and returns one
        of them for both; elsewhere it is a schedule first dispatched since
        that read. The tick kept for such a key is looked up when its first
        row is met, by the comparison the database itself makes, so the
        rows of every spelling are checked a batch at a time like the rest.

        With `write` false, the same rows are chosen and counted, and
        nothing is deleted.
        """
        ticks = OxScheduleTick.objects.using(alias)
        anchors: dict[str, int] = {}
        for key in self._schedule_keys(alias, batch_size):
            newest = self._newest_readable(key, alias, batch_size)
            if newest is not None:
                anchors[key] = newest
        protected = set(anchors.values())
        # Keys met in a batch that turned out to have no tick that reads.
        unanchored: set[str] = set()
        tally = self._ticks
        selected = ticks.filter(scheduled_for__lt=cutoff)
        after = None
        while True:
            page = selected if after is None else selected.filter(pk__gt=after)
            rows = read_ticks(page.order_by("pk"), using=alias, limit=batch_size)
            if not rows:
                break
            after = rows[-1].pk
            old: list[TickRow] = []
            for row in rows:
                if row.unreadable:
                    tally.keep(row)
                    continue
                if row.key not in anchors and row.key not in unanchored:
                    newest = self._newest_readable(row.key, alias, batch_size)
                    if newest is None:
                        unanchored.add(row.key)
                    else:
                        anchors[row.key] = newest
                        protected.add(newest)
                if row.pk in protected:
                    tally.anchors += 1
                else:
                    old.append(row)
            # Since the newest ticks were read, a schedule's newest may have
            # gone, and an older one in this batch become its newest. Or a
            # tick dispatched since is in this batch, above the one kept.
            older = _older_than_their_anchor(old, anchors, alias)
            newest_now = _newest_of_their_schedules(
                [row.pk for row in old if row.pk not in older], alias
            )
            tally.anchors += len(newest_now)
            doomed = [row.pk for row in old if row.pk not in newest_now]
            if doomed:
                # Counted once deleted, so a failure leaves the count of what
                # is gone.
                tally.deleted += _delete_ticks(doomed, alias) if write else len(doomed)
            if len(rows) < batch_size:
                break

    def _schedule_keys(self, alias: str, batch_size: int) -> list[str]:
        """
        Every schedule key the tick log holds.

        One statement, unless a key is text the driver cannot hand over,
        which SQLite keeps. Then every tick row is read, a batch at a time
        and each answered on its own. The keys that read are the answer,
        and a row whose key does not read is kept and reported like a row
        whose tick does not.

        SQLite also keeps bytes where a key should be, and hands them over
        as they are, with no error. They are no schedule's key and are left
        out; a row that holds them is kept and reported when the cutoff
        selects it.

        The rows are read again that way only after that failure, which
        is the driver's and leaves a caller's transaction as it was. A read
        the database itself refuses is raised as it is, inside a caller's
        transaction and with none open (`_stored_read.can_read_on`): its
        error names no tick, and no savepoint is taken around a read of
        the tick log to come back to.
        """
        ticks = OxScheduleTick.objects.using(alias)
        try:
            return [
                key
                for key in ticks.values_list("schedule_name", flat=True).distinct()
                if isinstance(key, str)
            ]
        except Exception as exc:
            if not can_read_on(exc, using=alias):
                raise
        keys: dict[str, None] = {}
        after = None
        while True:
            page = ticks if after is None else ticks.filter(pk__gt=after)
            rows = read_ticks(page.order_by("pk"), using=alias, limit=batch_size)
            for row in rows:
                if row.unreadable and not row.key:
                    self._ticks.keep(row)
                else:
                    keys[row.key] = None
            if len(rows) < batch_size:
                return list(keys)
            after = rows[-1].pk

    def _newest_readable(self, key: str, alias: str, batch_size: int) -> int | None:
        """
        The primary key of `key`'s newest tick that reads, or None when none
        does. The ticks above it, newer by the database's order and not
        readable, are added to the tally as they are passed.

        Newest first, one row and then a batch at a time, so a schedule whose
        newest tick reads costs one row.
        """
        newest_first = (
            OxScheduleTick.objects.using(alias)
            .filter(schedule_name=key)
            .order_by("-scheduled_for")
        )
        offset, size = 0, 1
        while True:
            rows = read_ticks(newest_first[offset:], using=alias, limit=size)
            for row in rows:
                if not row.unreadable:
                    return row.pk
                self._ticks.keep(row)
            if len(rows) < size:
                return None
            offset += len(rows)
            size = batch_size

    # -- --purge-unreadable-ticks --------------------------------------------

    def _purge(
        self, alias: str, keys: list[str], tick_pks: list[int], options: dict[str, Any]
    ) -> None:
        """
        Remove the named schedules' unreadable tick rows and the named
        unreadable tick rows, and nothing else.

        What is removed is found first, and a dry run stops there. Each
        batch is then locked, read again, and only the rows that still do
        not read are removed: one repaired since it was found stays. Rows go
        by primary key, in a transaction a batch.
        """
        batch_size = options["batch_size"]
        as_json = options["format"] == "json"
        dry_run = options["dry_run"]
        ticks = OxScheduleTick.objects.using(alias)
        found: dict[int, TickRow] = {}
        for key in keys:
            after = None
            while True:
                page = ticks.filter(schedule_name=key)
                if after is not None:
                    page = page.filter(pk__gt=after)
                rows = read_ticks(page.order_by("pk"), using=alias, limit=batch_size)
                for row in rows:
                    if row.unreadable:
                        found.setdefault(row.pk, row)
                if len(rows) < batch_size:
                    break
                after = rows[-1].pk
        reads: list[TickRow] = []
        missing: list[int] = []
        for chunk in _chunks(tick_pks, batch_size):
            named = {
                row.pk: row
                for row in read_ticks(
                    ticks.filter(pk__in=_possible_keys(chunk, alias)).order_by("pk"),
                    using=alias,
                )
            }
            for pk in chunk:
                tick = named.get(pk)
                if tick is None:
                    missing.append(pk)
                elif tick.unreadable:
                    found.setdefault(pk, tick)
                else:
                    reads.append(tick)

        removed: list[TickRow] = []
        repaired: list[TickRow] = []
        if dry_run:
            removed = list(found.values())
        else:
            try:
                for chunk in _chunks(sorted(found), batch_size):
                    gone, kept = self._remove_unreadable(chunk, alias)
                    # Logged as each batch commits, so a failure later on
                    # leaves a record of every row already gone.
                    self._log_removed(gone, alias)
                    removed += gone
                    repaired += kept
            except DatabaseError:
                if as_json:
                    self._write_purge_json(
                        keys=keys,
                        tick_pks=tick_pks,
                        removed=removed,
                        kept=reads + repaired,
                        missing=missing,
                        dry_run=dry_run,
                    )
                raise

        if as_json:
            self._write_purge_json(
                keys=keys,
                tick_pks=tick_pks,
                removed=removed,
                kept=reads + repaired,
                missing=missing,
                dry_run=dry_run,
            )
        else:
            verb = "Would remove" if dry_run else "Removed"
            colon = ":" if removed else "."
            lines = [_PURGE_COUNT.format(verb=verb, count=len(removed), colon=colon)]
            lines += [
                _TICK_LINE.format(pk=row.pk, key=repr(row.key), reason=row.reason)
                for row in removed
            ]
            lines += [
                _NAMED_TICK_READS.format(pk=row.pk, key=repr(row.key), at=_iso(row.at))
                for row in reads
            ]
            lines += [
                _TICK_REPAIRED.format(pk=row.pk, key=repr(row.key), at=_iso(row.at))
                for row in repaired
            ]
            lines += [_NO_SUCH_TICK.format(pk=pk) for pk in missing]
            self.stdout.write("\n".join(lines))
        if found:
            self.stderr.write(_HISTORY_LOSS, style_func=self.style.WARNING)

    def _remove_unreadable(
        self, pks: list[int], alias: str
    ) -> tuple[list[TickRow], list[TickRow]]:
        """
        Remove those of these tick rows that still cannot be read, in one
        transaction. Returns the rows removed and the rows that read now.

        The read again is the one that takes the rows' locks, so a repair
        either committed before it, and is what it reads, or waits for this
        transaction. Being a locking read, it reads the rows as they are
        now, not as a snapshot older than this transaction. SQLite has no
        row locks, and a transaction that reads first starts as a reader,
        so there a no-op UPDATE makes this one the writer first.
        """
        with transaction.atomic(using=alias):
            held = OxScheduleTick.objects.using(alias).filter(pk__in=pks)
            if connections[alias].features.has_select_for_update:
                held = held.select_for_update()
            else:
                held.update(schedule_name=F("schedule_name"))
            again = read_ticks(held.order_by("pk"), using=alias)
            gone = [row for row in again if row.unreadable]
            if gone:
                _delete_ticks([row.pk for row in gone], alias)
        # Returned once the block has committed.
        return gone, [row for row in again if not row.unreadable]

    def _log_removed(self, removed: list[TickRow], alias: str) -> None:
        """
        One record a removed tick row, every one of them, since each is
        dispatch history gone for good. To the handlers someone configured:
        without one, Python's last resort would print on stderr again what
        stdout already lists.
        """
        if not logger.hasHandlers():
            return
        for row in removed:
            logger.warning(
                _LOG_TICK_REMOVED,
                row.pk,
                repr(row.key),
                row.reason,
                extra={
                    "event": "schedule_tick_history_removed",
                    "database": alias,
                    "schedule_key": stored._printable(row.key),
                    "tick_pk": row.pk,
                    "reason": row.reason,
                },
            )

    def _write_purge_json(
        self,
        *,
        keys: list[str],
        tick_pks: list[int],
        removed: list[TickRow],
        kept: list[TickRow],
        missing: list[int],
        dry_run: bool,
    ) -> None:
        self.stdout.write(
            json.dumps(
                {
                    "purge_unreadable_ticks": True,
                    "schedule_keys": keys,
                    "tick_pks": tick_pks,
                    "tick_rows": len(removed),
                    "unreadable_ticks": [_named(row) for row in removed],
                    "kept_ticks": [
                        {
                            "pk": row.pk,
                            "schedule_name": row.key,
                            "scheduled_for": _iso(row.at),
                        }
                        for row in kept
                    ],
                    "missing_tick_pks": missing,
                    "dry_run": dry_run,
                }
            )
        )

    # -- task rows -----------------------------------------------------------

    def _delete_tasks_in_batches(
        self, prunable: QuerySet[OxTask], batch_size: int, label: str, alias: str
    ) -> int:
        # Each batch commits by itself. One that loses a deadlock or a
        # serialization failure runs again in a new transaction, which checks
        # the batch again under the lock and deletes again. Not inside a
        # caller's transaction, where the error has rolled back more than this
        # batch, all of it on MySQL. There the error goes to the caller.
        tries = _contention.attempts(alias)
        deleted = 0
        while True:
            candidates = list(prunable.values_list("pk", flat=True)[:batch_size])
            if not candidates:
                break
            attempt = 1
            while True:
                try:
                    count = self._delete_batch(prunable, candidates, alias)
                except DatabaseError as exc:
                    if tries == 1 or not _contention.is_contention(exc):
                        raise
                    if attempt >= tries:
                        # The batches before this one have committed. The
                        # candidates are the first rows still prunable, so a
                        # rerun starts again from this batch.
                        raise CommandError(
                            f"Stopped after deleting {deleted} {label} task "
                            "row(s). The next batch hit a database deadlock or "
                            f"serialization failure {tries} times. The rows "
                            "already deleted stay deleted. Run ox_prune again "
                            "to prune the rest."
                        ) from exc
                    _contention.pause(attempt)
                    attempt += 1
                    continue
                # Counted once the batch has committed, so a batch that ran
                # twice counts once.
                deleted += count
                self._task_rows = deleted
                break
        return deleted

    def _delete_batch(
        self, prunable: QuerySet[OxTask], candidates: list[Any], alias: str
    ) -> int:
        """Delete what is still prunable of one batch, in one transaction."""
        batch = candidates
        # A row can leave the selection after that read: an operator
        # retries it, or discards it and its finished_at becomes now, or
        # django_ox._waiting revives a DISCARDED row to WAITING.
        # Django's delete reads the rows again but then deletes by
        # primary key alone, so a row that changes between that read and
        # the DELETE is deleted anyway. The batch is therefore checked
        # again under a lock, in the transaction that deletes it, and
        # only the rows still prunable under that lock go.
        #
        # SQLite has no row locks, and Django drops FOR UPDATE there
        # without raising. What serialises SQLite is being the writer,
        # and a transaction that reads first starts as a reader. The
        # no-op UPDATE makes this one the writer before it reads.
        with transaction.atomic(using=alias):
            selected = prunable.filter(pk__in=batch)
            if connections[alias].features.has_select_for_update:
                # In primary-key order. Without an ORDER BY the read locks
                # rows in the order its plan reads them, which on
                # PostgreSQL can be table order. A writer that locks the
                # same rows in key order could then hold a row this read
                # waits for while this read holds one that writer needs,
                # and the database would abort one of the two.
                batch = list(
                    selected.select_for_update()
                    .order_by("pk")
                    .values_list("pk", flat=True)
                )
            else:
                selected.update(finished_at=F("finished_at"))
            # Returned from inside the block, and so reaches the caller only
            # once the block has committed.
            return prunable.filter(pk__in=batch).delete()[0]


def _iso(at: datetime | None) -> str | None:
    return None if at is None else at.isoformat()
