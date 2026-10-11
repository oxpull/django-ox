"""
The released oxpull 1.2.0 source, read from a vendored archive.

Two hazard test files start a real worker process on the 1.2.0 package to
prove that a fleet still running it stays correct beside the current code.
The archive holds src/oxpull exactly as released, so those tests need no git
history, no tag and no network. See fixtures/PROVENANCE.md.
"""

import hashlib
import tarfile
from pathlib import Path

ARCHIVE = Path(__file__).resolve().parent / "fixtures" / "oxpull-1.2.0-src.tar.gz"
ARCHIVE_SHA256 = "395c0deecf4310e1fce2dab537d88637084a98ed35bb8fa7b2c2742941c365a2"
TREE_SHA256 = "57a24e1f3cf301ecab07fb3478d075c3668a3099da5c02536641f7d884dba518"
FILE_COUNT = 18


def _tree_digest(root: Path) -> tuple[str, int]:
    """SHA-256 of 'sha256  path' lines for every file under root/src, sorted by path."""
    paths = sorted(
        (p for p in (root / "src").rglob("*") if p.is_file()),
        key=lambda p: p.relative_to(root).as_posix(),
    )
    lines = [
        f"{hashlib.sha256(p.read_bytes()).hexdigest()}  "
        f"{p.relative_to(root).as_posix()}\n"
        for p in paths
    ]
    return hashlib.sha256("".join(lines).encode()).hexdigest(), len(lines)


def extract_released_oxpull(destination: Path) -> Path:
    """
    Unpack oxpull 1.2.0 into destination and return its `src` directory.

    Raises AssertionError, never skips, when the archive or the unpacked
    tree does not match the recorded digests: a damaged fixture must fail
    the tests that depend on it rather than quietly drop them.
    """
    data = ARCHIVE.read_bytes()
    actual = hashlib.sha256(data).hexdigest()
    assert actual == ARCHIVE_SHA256, (
        f"{ARCHIVE.name} has SHA-256 {actual}, expected {ARCHIVE_SHA256}"
    )
    with tarfile.open(ARCHIVE, mode="r:gz") as tar:
        tar.extractall(destination, filter="data")
    tree, count = _tree_digest(destination)
    assert (tree, count) == (TREE_SHA256, FILE_COUNT), (
        f"the unpacked tree has digest {tree} over {count} files, "
        f"expected {TREE_SHA256} over {FILE_COUNT}"
    )
    return destination / "src"
