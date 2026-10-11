"""
Rebuild oxpull-1.2.0-src.tar.gz from a tar stream of the historical
`oxpull` 1.2.0 `src/oxpull` tree on standard input.

    python build_oxpull_1_2_0_src.py OUT.tar.gz < INPUT.tar

With the same zlib, output is byte-for-byte reproducible: entries sorted
by name, zero mtimes, uids and gids, empty owner names, fixed modes, and
a gzip header with no name and no timestamp. Across zlib versions, the
tree digest is the stable check.
"""

import gzip
import io
import sys
import tarfile
from pathlib import Path


def build(source: bytes) -> bytes:
    with tarfile.open(fileobj=io.BytesIO(source)) as tar:
        members = sorted(
            (m for m in tar.getmembers() if m.isreg() or m.isdir()),
            key=lambda m: m.name,
        )
        raw = io.BytesIO()
        with tarfile.open(fileobj=raw, mode="w", format=tarfile.USTAR_FORMAT) as out:
            for member in members:
                info = tarfile.TarInfo(member.name.rstrip("/"))
                info.mtime = 0
                info.uid = info.gid = 0
                info.uname = info.gname = ""
                if member.isdir():
                    info.type = tarfile.DIRTYPE
                    info.mode = 0o755
                    out.addfile(info)
                else:
                    info.size = member.size
                    info.mode = 0o644
                    out.addfile(info, tar.extractfile(member))
    packed = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=packed, mtime=0) as gz:
        gz.write(raw.getvalue())
    return packed.getvalue()


if __name__ == "__main__":
    with Path(sys.argv[1]).open("wb") as handle:
        handle.write(build(sys.stdin.buffer.read()))
