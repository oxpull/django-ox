"""
What happens to an installed project when the combined django-ox arrives.

These tests cover package contents, upgrade ordering and repair of
overlapping installations.
The tests build a stand-in earlier `oxpull` wheel from the current source,
versioned as an earlier release, rather than using the published 1.8.0 wheel.
Two kinds of test live here:

- the supported path (back up, uninstall oxpull, install django-ox, migrate)
  must leave every oxpull row, the schema, the migration state and the queued
  work exactly as it was;
- the unsafe paths do not fail loudly in pip. They are pinned as they behave,
  so that a change in pip, in the packaging or in what the wheel ships shows
  up as a failed test and a decision, not as a user report. Each says
  what the repair is.

Run with: python -m pytest tests_wheel -m wheel
"""

import tarfile
import zipfile

import pytest

from . import wheelkit

pytestmark = pytest.mark.wheel


def ok(done):
    assert done.returncode == 0, f"rc={done.returncode}\n{done.stdout}\n{done.stderr}"
    return done


# ---------------------------------------------------------------- the artifacts


def test_the_wheel_and_sdist_carry_oxpull_and_pass_the_release_check(artifacts):
    wheel = wheelkit.read_wheel(artifacts.combined)
    names = set(wheel)
    for needed in (
        "django_ox/__init__.py",
        "oxpull/__init__.py",
        "oxpull/backend.py",
        "oxpull/models.py",
        "oxpull/py.typed",
        "oxpull/_django_gate.py",
        "oxpull/_requires_django_6.py",
    ):
        assert needed in names, needed
    migrations = {n for n in names if n.startswith("oxpull/migrations/0")}
    assert len(migrations) == 5
    # Nothing but the two packages and their metadata.
    tops = {n.split("/")[0] for n in names}
    assert tops == {"django_ox", "oxpull", f"django_ox-{artifacts.version}.dist-info"}
    licences = [n for n in names if n.endswith("LICENSE")]
    assert licences and b"BSD 3-Clause" in wheel[licences[0]]
    with tarfile.open(artifacts.sdist) as tar:
        members = tar.getnames()
    assert any("/src/oxpull/backend.py" in m for m in members)
    assert any("/src/oxpull/migrations/0005_" in m for m in members)
    assert not any(m.endswith(".gitignore") for m in members)
    assert not any("/tests_oxpull/" in m or "/tests_wheel/" in m for m in members)
    # The release check opens the same archives, as it does for a tag.
    from tools.check_release import check_dist

    problems, _ = check_dist(artifacts.dist_dir, artifacts.version)
    assert problems == []


def test_twine_accepts_the_combined_distribution_and_the_metapackage(artifacts):
    done = wheelkit.run(
        [
            wheelkit.sys.executable,
            "-m",
            "twine",
            "check",
            "--strict",
            artifacts.combined,
            artifacts.sdist,
            artifacts.meta,
        ]
    )
    if "No module named twine" in done.stderr:
        pytest.skip("twine is not installed")
    ok(done)


def test_the_metapackage_ships_no_files(artifacts):
    with zipfile.ZipFile(artifacts.meta) as z:
        names = z.namelist()
        metadata = z.read(next(n for n in names if n.endswith("METADATA"))).decode()
    assert all(".dist-info/" in n for n in names), names
    assert f"Requires-Dist: django-ox>={artifacts.version}" in metadata


# ----------------------------------------------------------------- a fresh install


def test_a_fresh_install_runs_a_task_through_each_backend(make_env, artifacts):
    # Installing django-ox without an earlier `oxpull` distribution.
    env = make_env()
    ok(env.install(artifacts.combined))
    ok(env.check())
    done = env.import_oxpull()
    assert done.stdout.strip() == artifacts.version
    ok(env.manage("migrate"))
    ok(env.manage("makemigrations", "--check", "--dry-run"))
    assert env.enqueue("through-oxpull", "oxpull").stdout.strip().endswith("READY")
    assert env.enqueue("through-django-ox", "free").stdout.strip().endswith("READY")
    ok(env.run_worker())
    assert sorted(env.marks()) == ["through-django-ox", "through-oxpull"]


# --------------------------------------------------- the stub that holds the name


def test_the_stub_then_the_combined_wheel_leaves_the_combined_init(make_env, artifacts):
    # Upgrade from the earlier module-bearing `oxpull` stub.
    env = make_env()
    ok(env.install(artifacts.stub))
    ok(env.install(artifacts.combined))
    ok(env.check())
    assert env.import_oxpull().stdout.strip() == artifacts.version


def test_removing_the_stub_after_the_combined_wheel_takes_oxpull_init_with_it(
    make_env, artifacts
):
    # The earlier stub's RECORD lists oxpull/__init__.py, so uninstalling it
    # deletes the file django-ox wrote over it. `oxpull` survives as a namespace
    # package. The apps keep working, but __version__ is gone.
    env = make_env()
    ok(env.install(artifacts.stub))
    ok(env.install(artifacts.combined))
    ok(env.uninstall("oxpull"))
    assert not (env.oxpull_dir() / "__init__.py").exists()
    assert (env.oxpull_dir() / "backend.py").exists()
    assert env.import_oxpull().stdout.strip() == "NO __version__"
    ok(env.manage("check"))
    ok(env.manage("migrate"))
    # Repair.
    ok(env.install("--force-reinstall", "--no-deps", artifacts.combined))
    assert env.import_oxpull().stdout.strip() == artifacts.version


def test_removing_the_stub_first_is_clean(make_env, artifacts):
    env = make_env()
    ok(env.install(artifacts.stub))
    ok(env.uninstall("oxpull"))
    ok(env.install(artifacts.combined))
    ok(env.check())
    assert (env.oxpull_dir() / "__init__.py").exists()
    assert env.import_oxpull().stdout.strip() == artifacts.version
    ok(env.uninstall("oxpull")) if "oxpull" in env.freeze() else None
    assert env.import_oxpull().stdout.strip() == artifacts.version


# --------------------------- an earlier privately distributed oxpull installation


def legacy_oxpull_site(make_env, artifacts):
    """A project on legacy django-ox plus legacy oxpull, with rows and queued work."""
    env = make_env()
    ok(
        env.install(
            artifacts.legacy_free,
            artifacts.legacy_oxpull,
            extra_links=[artifacts.legacy_oxpull.parent],
        )
    )
    ok(env.check())
    ok(env.manage("migrate"))
    ok(env.py("seed.py"))
    ok(env.enqueue("before-upgrade-1", "oxpull"))
    ok(env.run_worker())
    # still READY when the packages change
    ok(env.enqueue("before-upgrade-2", "oxpull"))
    assert env.marks() == ["before-upgrade-1"]
    return env


def test_the_supported_procedure_changes_nothing_in_the_database(make_env, artifacts):
    # Back up, uninstall the earlier `oxpull` distribution, install django-ox, migrate.
    env = legacy_oxpull_site(make_env, artifacts)
    before = env.snapshot()
    assert before["rows"]["oxpull_oxpullbatch"] and before["tasks"]
    env.backup()
    ok(env.uninstall("oxpull"))
    ok(env.install(artifacts.combined))
    ok(env.check())
    done = ok(env.manage("migrate"))
    assert "No migrations to apply" in done.stdout
    assert env.snapshot() == before
    assert env.import_oxpull().stdout.strip() == artifacts.version
    ok(env.run_worker())  # the task that was READY before the change runs
    ok(env.enqueue("after-upgrade", "oxpull"))
    ok(env.run_worker())
    assert env.marks() == ["before-upgrade-1", "before-upgrade-2", "after-upgrade"]


def test_installing_over_old_oxpull_succeeds_in_pip_and_pip_check_fails(
    make_env, artifacts
):
    # Installing django-ox over the earlier distribution exits 0 with a resolver
    # complaint on stderr. Both claim oxpull/*, and `pip check` fails until the
    # earlier `oxpull` distribution is removed.
    env = legacy_oxpull_site(make_env, artifacts)
    done = ok(env.install(artifacts.combined))
    assert "requires django-ox==" in done.stderr
    assert env.check().returncode == 1
    assert (
        env.import_oxpull().stdout.strip() == artifacts.version
    )  # last writer: the combined file
    ok(env.manage("check"))
    done = ok(env.manage("migrate"))
    assert "No migrations to apply" in done.stdout


def test_uninstalling_old_oxpull_after_installing_over_it_deletes_oxpull(
    make_env, artifacts
):
    # Uninstalling the earlier distribution after the overlap reports success
    # and makes `pip check` pass, but removes the package files. Data is untouched.
    env = legacy_oxpull_site(make_env, artifacts)
    before = env.snapshot()
    ok(env.install(artifacts.combined))
    ok(env.uninstall("oxpull"))
    ok(env.check())
    assert not env.oxpull_dir().exists()
    failed = env.import_oxpull()
    assert failed.returncode != 0 and "No module named 'oxpull'" in failed.stderr
    assert env.snapshot() == before
    # Repair: one command, then the same checks as the supported path.
    ok(env.install("--force-reinstall", "--no-deps", artifacts.combined))
    assert env.import_oxpull().stdout.strip() == artifacts.version
    assert "No migrations to apply" in ok(env.manage("migrate")).stdout
    assert env.snapshot() == before
    ok(env.run_worker())
    assert env.marks()[-1] == "before-upgrade-2"


def test_pip_install_upgrade_oxpull_over_old_oxpull_deletes_oxpull_and_reports_success(
    make_env, artifacts
):
    # The metapackage upgrade removes the earlier `oxpull` distribution after
    # unpacking django-ox, so that uninstall can remove shared package files.
    env = legacy_oxpull_site(make_env, artifacts)
    before = env.snapshot()
    done = ok(
        env.install(
            "-U", "oxpull", extra_links=[artifacts.meta.parent, artifacts.dist_dir]
        )
    )
    assert "Successfully installed" in done.stdout
    ok(env.check())
    assert env.freeze()["oxpull"] == artifacts.version
    assert not env.oxpull_dir().exists()
    assert "No module named 'oxpull'" in env.import_oxpull().stderr
    assert env.snapshot() == before
    ok(env.install("--force-reinstall", "--no-deps", artifacts.combined))
    assert env.import_oxpull().stdout.strip() == artifacts.version
    assert env.snapshot() == before
    ok(env.run_worker())


def test_uninstalling_old_oxpull_before_the_metapackage_upgrade_is_safe(
    make_env, artifacts
):
    # Remove stale distribution metadata, then restore django-ox's files.
    env = legacy_oxpull_site(make_env, artifacts)
    before = env.snapshot()
    ok(env.uninstall("oxpull"))
    ok(
        env.install(
            "-U", "oxpull", extra_links=[artifacts.meta.parent, artifacts.dist_dir]
        )
    )
    ok(env.check())
    assert env.import_oxpull().stdout.strip() == artifacts.version
    assert "No migrations to apply" in ok(env.manage("migrate")).stdout
    assert env.snapshot() == before


# ----------------------------------------------------------------- the metapackage


def test_the_metapackage_after_the_combined_wheel_collides_with_nothing(
    make_env, artifacts
):
    # The `oxpull` metapackage contains no importable module.
    env = make_env()
    ok(env.install(artifacts.combined))
    ok(env.install(artifacts.meta, extra_links=[artifacts.dist_dir]))
    ok(env.check())
    assert env.freeze()["oxpull"] == artifacts.version
    ok(env.uninstall("oxpull"))
    ok(env.check())
    assert env.import_oxpull().stdout.strip() == artifacts.version


def test_the_metapackage_before_the_combined_wheel_collides_with_nothing(
    make_env, artifacts
):
    # The metapackage depends on django-ox for the importable `oxpull` package.
    env = make_env()
    ok(env.install("--no-deps", artifacts.meta))
    assert env.check().returncode == 1  # django-ox missing, as pip says
    ok(env.install(artifacts.combined))
    ok(env.check())
    assert env.import_oxpull().stdout.strip() == artifacts.version


def test_the_metapackage_alone_installs_the_combined_wheel(make_env, artifacts):
    # Uninstalling the metapackage leaves django-ox's module files in place.
    env = make_env()
    ok(env.install("oxpull", extra_links=[artifacts.meta.parent, artifacts.dist_dir]))
    ok(env.check())
    assert env.freeze()["django-ox"] == artifacts.version
    assert env.import_oxpull().stdout.strip() == artifacts.version


# ----------------------------------------------------------------------- Django 5.2


def test_the_installed_wheel_refuses_oxpull_on_django_5_2_with_the_fix(
    make_env, artifacts, wheelhouse_52
):
    # The check must tell the project what to do rather than raise
    # ModuleNotFoundError from inside the check.
    env = make_env(wheelhouse_52)
    ok(env.install(artifacts.combined, "django-tasks>=0.12"))
    env.oxpull_app = False
    ok(env.manage("check"))  # django-ox alone is fine on 5.2
    ok(env.manage("migrate"))
    env.oxpull_app = True
    refused = env.manage("check")
    assert refused.returncode != 0
    assert "oxpull needs Django 6.0 or later" in refused.stderr
    assert "remove 'oxpull' from INSTALLED_APPS" in refused.stderr
