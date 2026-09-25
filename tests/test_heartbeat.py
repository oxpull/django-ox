"""
django_ox.heartbeat on its own: what the writer does to the file and what
the probe accepts.

File times are set explicitly and the clock is passed in, so no test waits
for a file to age. The commands are in test_heartbeat_commands.py, and the
worker, supervisor and probe as real processes in
test_heartbeat_processes.py.
"""

import logging
import os
import stat
import threading
import time
from pathlib import Path

import pytest

from django_ox import heartbeat
from django_ox.heartbeat import (
    HeartbeatFile,
    check,
    check_file,
    child_file,
    expected_files,
    supervisor_file,
)

# A whole number of seconds, so that a file time set from it reads back
# exactly and the boundary arithmetic below has no rounding in it.
T0 = 1_700_000_000

posix_only = pytest.mark.skipif(os.name != "posix", reason="POSIX file semantics")
not_root = pytest.mark.skipif(
    hasattr(os, "geteuid") and os.geteuid() == 0,
    reason="root ignores the permission bits this test relies on",
)


def set_mtime(path: Path, seconds: float) -> None:
    ns = int(seconds * 1_000_000_000)
    os.utime(path, ns=(ns, ns), follow_symlinks=False)


def mtime(path: Path) -> float:
    return path.lstat().st_mtime


def warnings_about(caplog, path) -> list[logging.LogRecord]:
    return [
        r
        for r in caplog.records
        if getattr(r, "event", None) == "heartbeat_write_failed"
        and getattr(r, "heartbeat_file", None) == str(path)
    ]


# -- the writer ---------------------------------------------------------------


class TestTheWriter:
    @posix_only
    @pytest.mark.parametrize("umask", [0o022, 0o077, 0o002])
    def test_a_new_file_is_owner_read_write_only(self, tmp_path, umask):
        path = tmp_path / "hb"
        previous = os.umask(umask)
        try:
            assert HeartbeatFile(str(path), owner="test").touch() is True
        finally:
            os.umask(previous)
        assert stat.S_ISREG(path.lstat().st_mode)
        assert stat.S_IMODE(path.lstat().st_mode) == 0o600 & ~umask
        assert path.read_bytes() == b""

    @posix_only
    def test_an_existing_file_keeps_its_contents_and_permissions(self, tmp_path):
        path = tmp_path / "hb"
        path.write_bytes(b"left by someone else\n")
        path.chmod(0o640)
        set_mtime(path, T0)

        assert HeartbeatFile(str(path), owner="test").touch() is True

        assert path.read_bytes() == b"left by someone else\n"
        assert stat.S_IMODE(path.lstat().st_mode) == 0o640
        assert mtime(path) > T0

    @posix_only
    @not_root
    @pytest.mark.parametrize("umask", [0o200, 0o277])
    def test_a_umask_without_owner_write_does_not_freeze_the_file(
        self, tmp_path, umask, caplog
    ):
        """
        The first pass creates the file read-only, which the open that
        creates it is allowed to do, and every later open for writing is
        refused. The owner may still set a file's time to now, as touch
        does, so the file keeps moving.
        """
        path = tmp_path / "hb"
        caplog.set_level(logging.WARNING, logger="django_ox")
        previous = os.umask(umask)
        try:
            writer = HeartbeatFile(str(path), owner="test")
            assert writer.touch() is True
            assert stat.S_IMODE(path.lstat().st_mode) == 0o400
            set_mtime(path, T0)

            assert writer.touch() is True
        finally:
            os.umask(previous)
        assert mtime(path) > T0
        assert warnings_about(caplog, path) == []

    @posix_only
    @not_root
    def test_an_existing_read_only_file_of_its_own_is_still_updated(self, tmp_path):
        path = tmp_path / "hb"
        path.write_bytes(b"left by someone else\n")
        path.chmod(0o400)
        set_mtime(path, T0)

        assert HeartbeatFile(str(path), owner="test").touch() is True

        assert mtime(path) > T0
        assert stat.S_IMODE(path.lstat().st_mode) == 0o400
        assert path.read_bytes() == b"left by someone else\n"

    @posix_only
    @not_root
    def test_the_fallback_is_for_a_regular_file_only(self, tmp_path, caplog):
        # A FIFO with no write permission is refused by the open just as a
        # read-only regular file is; setting its time instead would make a
        # special file look like a heartbeat to the writer.
        path = tmp_path / "hb"
        os.mkfifo(path, 0o400)
        set_mtime(path, T0)
        caplog.set_level(logging.WARNING, logger="django_ox")

        assert HeartbeatFile(str(path), owner="test").touch() is False

        assert mtime(path) == T0
        assert len(warnings_about(caplog, path)) == 1

    def test_every_touch_moves_the_time_to_now(self, tmp_path):
        path = tmp_path / "hb"
        writer = HeartbeatFile(str(path), owner="test")
        writer.touch()
        set_mtime(path, T0)
        before = time.time()

        assert writer.touch() is True

        assert mtime(path) >= before - 1
        assert check_file(str(path), 60).ok

    def test_a_missing_directory_warns_once_and_is_picked_up_later(
        self, tmp_path, caplog
    ):
        directory = tmp_path / "not-yet"
        path = directory / "hb"
        writer = HeartbeatFile(str(path), owner="Worker w-1")
        caplog.set_level(logging.WARNING, logger="django_ox")

        assert [writer.touch() for _ in range(3)] == [False, False, False]
        (record,) = warnings_about(caplog, path)
        assert "Worker w-1 could not update its heartbeat file" in record.getMessage()

        directory.mkdir()
        assert writer.touch() is True
        assert path.is_file()
        assert len(warnings_about(caplog, path)) == 1

    @posix_only
    @not_root
    def test_a_read_only_directory_warns_once_and_never_raises(self, tmp_path, caplog):
        directory = tmp_path / "ro"
        directory.mkdir()
        directory.chmod(0o500)
        path = directory / "hb"
        caplog.set_level(logging.WARNING, logger="django_ox")
        try:
            writer = HeartbeatFile(str(path), owner="test")
            assert [writer.touch() for _ in range(3)] == [False, False, False]
        finally:
            directory.chmod(0o700)
        assert len(warnings_about(caplog, path)) == 1
        assert not path.exists()

    @posix_only
    def test_a_symlink_is_not_followed(self, tmp_path, caplog):
        target = tmp_path / "elsewhere"
        target.write_bytes(b"")
        set_mtime(target, T0)
        path = tmp_path / "hb"
        path.symlink_to(target)
        caplog.set_level(logging.WARNING, logger="django_ox")

        assert HeartbeatFile(str(path), owner="test").touch() is False

        assert mtime(target) == T0
        assert len(warnings_about(caplog, path)) == 1

    @posix_only
    def test_a_fifo_is_refused_without_blocking(self, tmp_path, caplog):
        path = tmp_path / "hb"
        os.mkfifo(path)
        caplog.set_level(logging.WARNING, logger="django_ox")
        result = []
        thread = threading.Thread(
            target=lambda: result.append(HeartbeatFile(str(path), owner="t").touch()),
            daemon=True,
        )
        thread.start()
        thread.join(timeout=10)

        assert not thread.is_alive(), "the touch blocked on a FIFO with no reader"
        assert result == [False]
        assert len(warnings_about(caplog, path)) == 1

    @posix_only
    def test_a_fifo_with_a_reader_is_still_refused(self, tmp_path):
        path = tmp_path / "hb"
        os.mkfifo(path)
        reader = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
        try:
            assert HeartbeatFile(str(path), owner="test").touch() is False
        finally:
            os.close(reader)

    def test_a_directory_at_the_path_is_refused(self, tmp_path):
        path = tmp_path / "hb"
        path.mkdir()
        assert HeartbeatFile(str(path), owner="test").touch() is False
        assert path.is_dir()

    def test_nothing_the_open_raises_escapes(self, tmp_path, monkeypatch, caplog):
        def broken(*args, **kwargs):
            raise RuntimeError("not an OSError")

        monkeypatch.setattr(heartbeat.os, "open", broken)
        caplog.set_level(logging.WARNING, logger="django_ox")
        writer = HeartbeatFile(str(tmp_path / "hb"), owner="test")

        assert writer.touch() is False
        assert writer.touch() is False
        assert len(warnings_about(caplog, tmp_path / "hb")) == 1


# -- the probe ----------------------------------------------------------------


class TestFreshness:
    @pytest.fixture
    def path(self, tmp_path):
        path = tmp_path / "hb"
        path.write_bytes(b"")
        set_mtime(path, T0)
        return path

    @pytest.mark.parametrize("age", [0, 0.5, 59.999, 60])
    def test_an_age_from_zero_to_the_limit_passes(self, path, age):
        report = check_file(str(path), 60, now=T0 + age)
        assert report.ok, report.problem
        assert report.age == pytest.approx(age)

    def test_the_limit_is_inclusive(self, path):
        assert check_file(str(path), 60, now=T0 + 60).ok
        report = check_file(str(path), 60, now=T0 + 60.001)
        assert not report.ok
        assert report.problem == (
            f"heartbeat file {path} is 60.1s old, over --max-heartbeat-age 60s"
        )

    @pytest.mark.parametrize(
        ("limit", "age", "shown", "limit_shown"),
        [
            # Rounded to the nearest tenth, each of these would read as the
            # limit itself, or under it, in a sentence that says it is over.
            (60, 0.000001, "60.1", "60"),
            (60, 0.04, "60.1", "60"),
            (60, 0.049, "60.1", "60"),
            (0.5, 0.02, "0.6", "0.5"),
            (60.04, 0.001, "60.1", "60.04"),
            # Away from the limit the age is rounded as before.
            (60, 1.24, "61.2", "60"),
            (60, 1.26, "61.3", "60"),
            (60, 15, "75.0", "60"),
        ],
    )
    def test_an_age_over_the_limit_never_reads_as_the_limit(
        self, path, limit, age, shown, limit_shown
    ):
        report = check_file(str(path), limit, now=T0 + limit + age)
        assert not report.ok
        assert report.problem == (
            f"heartbeat file {path} is {shown}s old, "
            f"over --max-heartbeat-age {limit_shown}s"
        )
        assert float(shown) > limit

    def test_a_fractional_limit(self, path):
        assert check_file(str(path), 0.5, now=T0 + 0.5).ok
        assert not check_file(str(path), 0.5, now=T0 + 0.75).ok

    def test_a_time_in_the_future_is_clock_skew_not_freshness(self, path):
        report = check_file(str(path), 60, now=T0 - 12)
        assert not report.ok
        assert report.age == pytest.approx(-12)
        assert report.problem == (
            f"heartbeat file {path} was updated 12.0s in the future; "
            "the clocks of the worker and the check disagree"
        )

    def test_the_smallest_step_into_the_future_fails(self, path):
        assert not check_file(str(path), 60, now=T0 - 0.001).ok

    @pytest.mark.parametrize(
        ("ahead", "shown"), [(0.000001, "<0.1"), (0.04, "<0.1"), (0.3, "0.3")]
    )
    def test_a_step_into_the_future_is_never_shown_as_zero(self, path, ahead, shown):
        report = check_file(str(path), 60, now=T0 - ahead)
        assert report.problem == (
            f"heartbeat file {path} was updated {shown}s in the future; "
            "the clocks of the worker and the check disagree"
        )

    def test_missing(self, tmp_path):
        report = check_file(str(tmp_path / "hb"), 60, now=T0)
        assert report.age is None
        assert report.problem == f"heartbeat file {tmp_path / 'hb'} is missing"

    def test_a_deleted_directory_is_missing(self, tmp_path):
        directory = tmp_path / "gone"
        directory.mkdir()
        (directory / "hb").write_bytes(b"")
        (directory / "hb").unlink()
        directory.rmdir()
        report = check_file(str(directory / "hb"), 60, now=T0)
        assert report.problem == f"heartbeat file {directory / 'hb'} is missing"

    def test_a_directory(self, tmp_path):
        path = tmp_path / "hb"
        path.mkdir()
        set_mtime(path, T0)
        report = check_file(str(path), 60, now=T0)
        assert report.problem == (
            f"heartbeat file {path} is a directory, not a regular file"
        )

    @posix_only
    def test_a_symlink_to_a_fresh_file_fails(self, tmp_path):
        target = tmp_path / "real"
        target.write_bytes(b"")
        set_mtime(target, T0)
        path = tmp_path / "hb"
        path.symlink_to(target)
        set_mtime(path, T0)
        report = check_file(str(path), 60, now=T0)
        assert report.problem == (
            f"heartbeat file {path} is a symlink, not a regular file"
        )

    @posix_only
    def test_a_fifo(self, tmp_path):
        path = tmp_path / "hb"
        os.mkfifo(path)
        set_mtime(path, T0)
        report = check_file(str(path), 60, now=T0)
        assert report.problem == (
            f"heartbeat file {path} is a special file, not a regular file"
        )

    @posix_only
    @not_root
    def test_a_path_that_cannot_be_read(self, tmp_path):
        directory = tmp_path / "locked"
        directory.mkdir()
        (directory / "hb").write_bytes(b"")
        directory.chmod(0o000)
        try:
            report = check_file(str(directory / "hb"), 60, now=T0)
        finally:
            directory.chmod(0o700)
        assert report.age is None
        assert report.problem == (
            f"heartbeat file {directory / 'hb'} cannot be read: Permission denied"
        )

    @posix_only
    def test_file_permissions_do_not_matter_to_the_probe(self, path):
        # The probe reads metadata, not contents, so it can run as any user
        # who can reach the directory.
        path.chmod(0o000)
        assert check_file(str(path), 60, now=T0 + 1).ok

    def test_the_default_clock_is_the_wall_clock(self, path):
        now = time.time()
        set_mtime(path, now - 5)
        assert check_file(str(path), 60).ok
        set_mtime(path, now - 120)
        assert not check_file(str(path), 60).ok


class TestTheExpectedSet:
    def test_one_process_is_the_path_itself(self):
        assert expected_files("/run/ox/hb") == ["/run/ox/hb"]
        assert expected_files("/run/ox/hb", 1) == ["/run/ox/hb"]

    def test_several_are_the_supervisor_then_every_slot(self):
        assert expected_files("/run/ox/hb", 3) == [
            "/run/ox/hb.supervisor",
            "/run/ox/hb.0",
            "/run/ox/hb.1",
            "/run/ox/hb.2",
        ]
        assert supervisor_file("/run/ox/hb") == "/run/ox/hb.supervisor"
        assert child_file("/run/ox/hb", 7) == "/run/ox/hb.7"

    def test_below_one_is_refused(self):
        with pytest.raises(ValueError):
            expected_files("/run/ox/hb", 0)


class TestTheSetIsJudgedWhole:
    """
    The attacks on the one check that decides a multi-process container:
    every expected file must pass, however many others do.
    """

    @pytest.fixture
    def base(self, tmp_path):
        return tmp_path / "hb"

    def write(self, path: Path | str, when: float) -> None:
        path = Path(path)
        path.write_bytes(b"")
        set_mtime(path, when)

    def test_all_fresh_passes(self, base):
        for name in expected_files(str(base), 2):
            self.write(name, T0)
        assert all(r.ok for r in check(str(base), 60, 2, now=T0 + 1))

    def test_a_fresh_sibling_does_not_mask_a_stale_slot(self, base):
        self.write(supervisor_file(str(base)), T0)
        self.write(child_file(str(base), 0), T0 - 3600)
        self.write(child_file(str(base), 1), T0)
        reports = check(str(base), 60, 2, now=T0 + 1)
        assert [r.ok for r in reports] == [True, False, True]
        assert reports[1].problem.startswith(f"heartbeat file {base}.0 is ")

    def test_a_missing_slot_stays_in_the_count(self, base):
        self.write(supervisor_file(str(base)), T0)
        self.write(child_file(str(base), 0), T0)
        reports = check(str(base), 60, 2, now=T0 + 1)
        assert [r.problem for r in reports] == [
            None,
            None,
            f"heartbeat file {base}.1 is missing",
        ]

    def test_fresh_children_do_not_cover_for_the_supervisor(self, base):
        self.write(supervisor_file(str(base)), T0 - 3600)
        self.write(child_file(str(base), 0), T0)
        self.write(child_file(str(base), 1), T0)
        reports = check(str(base), 60, 2, now=T0 + 1)
        assert [r.ok for r in reports] == [False, True, True]

    def test_leftover_files_outside_the_set_count_for_nothing(self, base):
        # A slot above N, from a run with more processes, and the
        # single-process file: neither is expected, so neither can pass the
        # check for the slot that is stale.
        self.write(base, T0)
        self.write(child_file(str(base), 2), T0)
        self.write(supervisor_file(str(base)), T0)
        self.write(child_file(str(base), 0), T0 - 3600)
        self.write(child_file(str(base), 1), T0)
        reports = check(str(base), 60, 2, now=T0 + 1)
        assert [r.path for r in reports] == expected_files(str(base), 2)
        assert [r.ok for r in reports] == [True, False, True]

    def test_a_write_between_the_clock_read_and_the_stat_is_not_the_future(
        self, base, monkeypatch
    ):
        # A worker's update can land just before its file's metadata is read.
        # Each file is checked against the clock sampled after its metadata,
        # so the update is not treated as a future timestamp.
        for name in expected_files(str(base), 2):
            self.write(name, T0)
        clock = [T0 + 1.0]
        real_lstat = os.lstat
        racing = child_file(str(base), 1)

        def lstat(path, *args, **kwargs):
            if str(path) == racing:
                clock[0] = T0 + 1.005
                set_mtime(racing, clock[0])
            return real_lstat(path, *args, **kwargs)

        with monkeypatch.context() as patch:
            patch.setattr(heartbeat.time, "time", lambda: clock[0])
            patch.setattr(heartbeat.os, "lstat", lstat)
            reports = check(str(base), 60, 2)
        assert [r.problem for r in reports] == [None, None, None]

    def test_a_file_ahead_of_the_clock_still_fails(self, base, monkeypatch):
        for name in expected_files(str(base), 2):
            self.write(name, T0)
        self.write(child_file(str(base), 1), T0 + 10)
        with monkeypatch.context() as patch:
            patch.setattr(heartbeat.time, "time", lambda: T0 + 1)
            reports = check(str(base), 60, 2)
        assert [r.ok for r in reports] == [True, True, False]
        assert reports[2].problem.startswith(
            f"heartbeat file {base}.1 was updated 9.0s in the future"
        )

    def test_a_supplied_now_is_used_for_every_file(self, base):
        for name in expected_files(str(base), 2):
            self.write(name, T0)
        reports = check(str(base), 60, 2, now=T0 - 1)
        assert [r.ok for r in reports] == [False, False, False]
        assert all("in the future" in r.problem for r in reports)

    def test_one_process_ignores_slot_files(self, base):
        self.write(child_file(str(base), 0), T0)
        self.write(supervisor_file(str(base)), T0)
        (report,) = check(str(base), 60, 1, now=T0 + 1)
        assert report.problem == f"heartbeat file {base} is missing"

    def test_a_shared_path_lets_a_live_writer_mask_a_dead_one(self, base):
        """
        Why a directory shared between containers is unsupported, shown
        rather than argued: two workers given the same path write one file,
        and the one still running keeps it fresh for the one that stopped.
        """
        stopped = HeartbeatFile(str(base), owner="container A")
        running = HeartbeatFile(str(base), owner="container B")
        stopped.touch()
        set_mtime(base, T0)
        # Container A stops writing here. Container B carries on.
        running.touch()
        (report,) = check(str(base), 60, 1)
        assert report.ok
