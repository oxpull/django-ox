"""
Build the artifacts the transition tests install, and run pip against them.

The combined wheel and sdist are built from this tree. Everything older is
rebuilt from that wheel, so the tests need no package index for these wheels
and no network beyond Django itself:

- the legacy django-ox wheel is the combined wheel without oxpull/;
- the earlier separately installed oxpull wheel contains only oxpull/, with the literal
  __version__ the earlier `oxpull` distribution had and an exact pin on the
  legacy django-ox. Its other files are the combined wheel's, so it runs
  against the legacy django-ox; it is the file layout, not the old code, that
  the tests need;
- the PyPI stub is the placeholder that holds the name today;
- the metapackage is a real hatchling build of a wheel with no files.

Their versions sit below the combined one, which is the order the release
puts them in. File ownership is what the pip behaviour depends on, and these
have the same file names as the real ones.
"""

from __future__ import annotations

import base64
import hashlib
import os
import subprocess
import sys
import textwrap
import zipfile
from dataclasses import dataclass
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
LEGACY = "0.9.0"

STUB_INIT = '''\
"""Placeholder for the Oxpull commercial package."""

__version__ = "0.0.1"


def __getattr__(name: str) -> object:
    raise AttributeError("The 'oxpull' package on PyPI is a name placeholder.")
'''

LEGACY_INIT = f'__version__ = "{LEGACY}"\n\n__all__ = ["__version__"]\n'


def run(argv, *, cwd=None, env=None, check=False):
    return subprocess.run(
        [str(a) for a in argv],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        check=check,
    )


def _record_line(name: str, data: bytes) -> str:
    digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=")
    return f"{name},sha256={digest.decode()},{len(data)}"


def write_wheel(
    dest: Path,
    name: str,
    version: str,
    files: dict[str, bytes],
    requires: tuple[str, ...] = (),
    extra_metadata: str = "",
) -> Path:
    """A pure-Python wheel with the given files, named the way pip expects."""
    norm = name.replace("-", "_")
    info = f"{norm}-{version}.dist-info"
    metadata = f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n"
    metadata += "".join(f"Requires-Dist: {r}\n" for r in requires)
    metadata += extra_metadata
    members = dict(files)
    members[f"{info}/METADATA"] = metadata.encode()
    members[f"{info}/WHEEL"] = (
        b"Wheel-Version: 1.0\nGenerator: tests_wheel\n"
        b"Root-Is-Purelib: true\nTag: py3-none-any\n"
    )
    record = [_record_line(n, d) for n, d in members.items()]
    record.append(f"{info}/RECORD,,")
    members[f"{info}/RECORD"] = ("\n".join(record) + "\n").encode()
    dest.mkdir(parents=True, exist_ok=True)
    path = dest / f"{norm}-{version}-py3-none-any.whl"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        for member, data in members.items():
            z.writestr(member, data)
    return path


def read_wheel(path: Path) -> dict[str, bytes]:
    with zipfile.ZipFile(path) as z:
        return {n: z.read(n) for n in z.namelist() if not n.endswith("/")}


@dataclass
class Artifacts:
    """Where each kind of artifact lives, and the version it carries."""

    root: Path
    version: str
    combined: Path
    sdist: Path
    legacy_free: Path
    legacy_oxpull: Path
    stub: Path
    meta: Path

    @property
    def dist_dir(self) -> Path:
        return self.root / "combined"


def build_combined(root: Path) -> tuple[Path, Path, str]:
    out = root / "combined"
    out.mkdir(parents=True)
    done = run([sys.executable, "-m", "build", "--no-isolation", "--outdir", out, REPO])
    assert done.returncode == 0, done.stdout + done.stderr
    (wheel,) = out.glob("*.whl")
    (sdist,) = out.glob("*.tar.gz")
    version = wheel.name.split("-")[1]
    return wheel, sdist, version


def build_meta(root: Path, version: str) -> Path:
    src = root / "meta-src"
    src.mkdir(parents=True)
    (src / "README.md").write_text("Installs django-ox, which contains oxpull.\n")
    (src / "pyproject.toml").write_text(
        textwrap.dedent(
            f"""\
            [build-system]
            requires = ["hatchling>=1.27"]
            build-backend = "hatchling.build"

            [project]
            name = "oxpull"
            version = "{version}"
            description = "Installs django-ox, which now contains oxpull."
            readme = "README.md"
            requires-python = ">=3.12"
            dependencies = ["django-ox>={version}"]

            [tool.hatch.build.targets.wheel]
            bypass-selection = true
            """
        )
    )
    out = root / "meta"
    done = run(
        [
            sys.executable,
            "-m",
            "build",
            "--no-isolation",
            "--wheel",
            "--outdir",
            out,
            src,
        ]
    )
    assert done.returncode == 0, done.stdout + done.stderr
    (wheel,) = out.glob("*.whl")
    return wheel


def build_artifacts(root: Path) -> Artifacts:
    combined, sdist, version = build_combined(root)
    members = read_wheel(combined)

    def dist_info(name: str) -> bool:
        return name.split("/")[0].endswith(".dist-info")

    free_files = {
        n: d
        for n, d in members.items()
        if not n.startswith("oxpull/") and not dist_info(n)
    }
    legacy_free = write_wheel(
        root / "legacy", "django-ox", LEGACY, free_files, ("Django>=5.2",)
    )
    oxpull_files = {n: d for n, d in members.items() if n.startswith("oxpull/")}
    oxpull_files["oxpull/__init__.py"] = LEGACY_INIT.encode()
    legacy_oxpull = write_wheel(
        root / "legacy", "oxpull", LEGACY, oxpull_files, (f"django-ox=={LEGACY}",)
    )
    stub = write_wheel(
        root / "legacy", "oxpull", "0.0.1", {"oxpull/__init__.py": STUB_INIT.encode()}
    )
    return Artifacts(
        root=root,
        version=version,
        combined=combined,
        sdist=sdist,
        legacy_free=legacy_free,
        legacy_oxpull=legacy_oxpull,
        stub=stub,
        meta=build_meta(root, version),
    )


SETTINGS = """\
import os

SECRET_KEY = "x"
USE_TZ = True
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
INSTALLED_APPS = ["django_ox"]
if os.environ.get("SITE_OXPULL") == "1":
    INSTALLED_APPS.append("oxpull")
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": os.environ["SITE_DB"],
        "OPTIONS": {"timeout": 20},
    }
}
_free = {"BACKEND": "django_ox.backend.OxBackend", "QUEUES": ["default"]}
_oxpull = {"BACKEND": "oxpull.backend.OxpullBackend", "QUEUES": ["default"]}
TASKS = {"default": _free, "free": _free}
if os.environ.get("SITE_OXPULL") == "1":
    TASKS["oxpull"] = _oxpull
"""

TASKS = """\
import os

from django.tasks import task


@task()
def hello(tag):
    with open(os.environ["SITE_MARK"], "a") as f:
        f.write(tag + "\\n")
    return tag
"""

MANAGE = """\
import os, sys
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "wheelsite.settings")
from django.core.management import execute_from_command_line
execute_from_command_line(sys.argv)
"""

SEED = """\
import os, uuid
import django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "wheelsite.settings")
django.setup()
from django.utils import timezone
from oxpull.models import OxpullBatch, OxpullRateWindow, OxpullUniqueLock
now = timezone.now()
OxpullBatch.objects.create(name="seed-batch", created_at=now)
OxpullRateWindow.objects.create(
    limit_name="seed-limit", window_id=7, window_started_at=now, used=3
)
OxpullUniqueLock.objects.create(
    key="k" * 64,
    task_id=uuid.uuid4(),
    task_path="wheelsite.tasks.hello",
    created_at=now,
)
"""

ENQUEUE = """\
import os, sys
import django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "wheelsite.settings")
django.setup()
from wheelsite.tasks import hello
print(hello.using(backend=sys.argv[2]).enqueue(sys.argv[1]).status)
"""


class Env:
    """A virtualenv with a one-file Django site next to it."""

    def __init__(self, root: Path, name: str, wheelhouse: Path, artifacts: Artifacts):
        self.dir = root / name
        self.dir.mkdir(parents=True)
        self.wheelhouse = wheelhouse
        self.artifacts = artifacts
        done = run([sys.executable, "-m", "venv", self.dir / "venv"])
        assert done.returncode == 0, done.stderr
        self.python = self.dir / "venv" / "bin" / "python"
        self.site = self.dir / "site"
        (self.site / "wheelsite").mkdir(parents=True)
        (self.site / "wheelsite" / "__init__.py").write_text("")
        (self.site / "wheelsite" / "settings.py").write_text(SETTINGS)
        (self.site / "wheelsite" / "tasks.py").write_text(TASKS)
        (self.site / "manage.py").write_text(MANAGE)
        (self.site / "seed.py").write_text(SEED)
        (self.site / "enqueue.py").write_text(ENQUEUE)
        self.db = self.dir / "db.sqlite3"
        self.mark = self.dir / "mark.txt"
        self.oxpull_app = True

    # pip ------------------------------------------------------------------
    def pip(self, *args, extra_links=()):
        links = [self.wheelhouse, *extra_links]
        argv = [self.python, "-m", "pip", "--disable-pip-version-check"]
        argv += [a for a in args if a != "--offline"]
        if "--offline" in args:
            argv += ["--no-index"]
            for link in links:
                argv += ["--find-links", link]
        return run(argv)

    def install(self, *specs, extra_links=(), flags=()):
        return self.pip(
            "install",
            "--offline",
            *flags,
            *[str(s) for s in specs],
            extra_links=extra_links,
        )

    def uninstall(self, name):
        return self.pip("uninstall", "-y", name)

    def check(self):
        return self.pip("check")

    def freeze(self) -> dict[str, str]:
        done = self.pip("list", "--format=freeze")
        return dict(line.split("==") for line in done.stdout.split())

    @property
    def packages(self) -> Path:
        done = run(
            [
                self.python,
                "-c",
                "import sysconfig;print(sysconfig.get_paths()['purelib'])",
            ]
        )
        return Path(done.stdout.strip())

    # the site -------------------------------------------------------------
    def env(self, **extra):
        env = {
            **os.environ,
            "SITE_DB": str(self.db),
            "SITE_MARK": str(self.mark),
            "SITE_OXPULL": "1" if self.oxpull_app else "0",
            "PYTHONPATH": str(self.site),
            "PYTHONDONTWRITEBYTECODE": "1",
        }
        env.pop("DJANGO_SETTINGS_MODULE", None)
        env.update(extra)
        return env

    def py(self, *args):
        return run([self.python, *args], cwd=self.site, env=self.env())

    def code(self, source: str):
        return self.py("-c", source)

    def manage(self, *args):
        return run([self.python, "manage.py", *args], cwd=self.site, env=self.env())

    def enqueue(self, tag: str, backend: str = "default"):
        return self.py("enqueue.py", tag, backend)

    def run_worker(self):
        return self.manage("ox_worker", "--batch")

    def marks(self) -> list[str]:
        return self.mark.read_text().split() if self.mark.exists() else []

    def oxpull_dir(self) -> Path:
        return self.packages / "oxpull"

    def import_oxpull(self):
        return self.code(
            "import oxpull; print(getattr(oxpull, '__version__', 'NO __version__'))"
        )

    def snapshot(self) -> dict:
        """The oxpull rows and schema, applied migrations and task rows."""
        import sqlite3

        c = sqlite3.connect(self.db)
        try:
            tables = [
                r[0]
                for r in c.execute(
                    "select name from sqlite_master"
                    " where type='table' and name like 'oxpull_%' order by 1"
                )
            ]
            rows = {t: list(c.execute(f"select * from {t} order by 1")) for t in tables}
            schema = list(
                c.execute(
                    "select sql from sqlite_master"
                    " where name like 'oxpull_%' order by name"
                )
            )
            applied = {
                app: [
                    r[0]
                    for r in c.execute(
                        "select name from django_migrations where app=? order by id",
                        (app,),
                    )
                ]
                for app in ("oxpull", "django_ox")
            }
            tasks = list(
                c.execute("select status,count(*) from django_ox_oxtask group by 1")
            )
        finally:
            c.close()
        return {"rows": rows, "schema": schema, "applied": applied, "tasks": tasks}

    def backup(self, suffix="backup") -> Path:
        import shutil

        target = self.dir / f"db.{suffix}.sqlite3"
        shutil.copy(self.db, target)
        return target
