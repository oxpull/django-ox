# oxpull-1.2.0-src.tar.gz

This archive contains `src/oxpull` from the historical `oxpull` 1.2.0 release.
Two hazard-test files start worker processes using this source to exercise
mixed-version scenarios. `_historical.py` verifies and unpacks the archive;
the test loader uses the unpacked source without installing it.

- Source: earlier `oxpull` 1.2.0 source snapshot, dated 2026-09-12.
- Contents: 18 files, `src/oxpull/**`, `__version__ = "1.2.0"`. No LICENSE, README or tests.
- Archive SHA-256: `395c0deecf4310e1fce2dab537d88637084a98ed35bb8fa7b2c2742941c365a2`
- Tree digest: `57a24e1f3cf301ecab07fb3478d075c3668a3099da5c02536641f7d884dba518`
  (SHA-256 of the lines sorted by path `<sha256 of file>  <path>\n` for the 18 files)

Build from a tar stream of the historical `oxpull` 1.2.0 `src/oxpull` tree:

    python build_oxpull_1_2_0_src.py oxpull-1.2.0-src.tar.gz < INPUT.tar

The builder sorts entries, zeroes mtimes, uids and gids, fixes modes and writes a
gzip header with no name and no timestamp. Rebuilding gives the same bytes on one
zlib; if a different zlib compresses differently, the tree digest is the stable
check and the archive digest in `_historical.py` is updated with it.

Do not edit the archive. A change to what 1.2.0 shipped is a different fixture.
