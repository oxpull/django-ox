"""
`ox_worker` has to import on a platform without every POSIX signal.

The single-process worker needs no signal forwarding and is documented as
the way to run on Windows. A signal named at module scope would make the
import itself raise there, before argparse and before the command could
explain anything, so the constants are built from the signals the platform
has.
"""

import importlib
import signal
import sys

import pytest


@pytest.fixture
def without_sighup(monkeypatch):
    """The `signal` module as a platform with no SIGHUP and no SIGKILL."""
    monkeypatch.delattr(signal, "SIGHUP", raising=False)
    monkeypatch.delattr(signal, "SIGKILL", raising=False)
    for name in [n for n in sys.modules if n.startswith("django_ox.supervisor")]:
        monkeypatch.delitem(sys.modules, name, raising=False)
    # No reload on the way out: monkeypatch restores the signal module after
    # this fixture finishes, so reloading here would rebuild the module while
    # the signals are still missing and leave it that way for everyone else.
    # Dropping it from sys.modules is enough; the next import rebuilds it.
    return


class TestTheSupervisorImportsWithoutPosixSignals:
    def test_the_module_imports(self, without_sighup):
        module = importlib.import_module("django_ox.supervisor")
        assert module.STOP_SIGNALS, "no stop signal survived"

    def test_it_forwards_only_signals_the_platform_has(self, without_sighup):
        module = importlib.import_module("django_ox.supervisor")
        assert signal.SIGTERM in module.STOP_SIGNALS
        assert signal.SIGINT in module.STOP_SIGNALS
        assert all(isinstance(s, signal.Signals) for s in module.STOP_SIGNALS)

    def test_the_force_signal_falls_back_rather_than_failing(self, without_sighup):
        module = importlib.import_module("django_ox.supervisor")
        assert module.FORCE_SIGNAL == signal.SIGTERM

    def test_the_management_command_still_imports(self, without_sighup, monkeypatch):
        for name in [
            n for n in sys.modules if n.startswith("django_ox.management.commands")
        ]:
            monkeypatch.delitem(sys.modules, name, raising=False)
        module = importlib.import_module("django_ox.management.commands.ox_worker")
        assert hasattr(module, "Command"), "ox_worker did not import"


class TestOnThisPlatformNothingChanged:
    def test_every_stop_signal_is_still_forwarded(self):
        module = importlib.reload(importlib.import_module("django_ox.supervisor"))
        assert module.STOP_SIGNALS == (
            signal.SIGTERM,
            signal.SIGINT,
            signal.SIGHUP,
        )
        assert module.FORCE_SIGNAL == signal.SIGKILL
