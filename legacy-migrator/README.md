# Memo legacy migrator

This separately installed, one-time utility migrates recordings written by the
pre-daemon Memo prototype. It is pinned to `memo-agent==1.0.0`, the recording
format version against which it was built and tested.

Install the migrator from the Memo repository root using the instructions that
match how you installed Memo.

If you installed Memo with `pipx install .`, inject the migrator into Memo's
existing environment and expose its command:

```console
pipx inject --include-apps memo-agent ./legacy-migrator
```

If you installed Memo with `uv tool install .`, install the migrator as a
separate tool and explicitly provide the local Memo package as its dependency:

```console
uv tool install ./legacy-migrator --with .
```

The explicit local dependency is required because `memo-agent` is not
published in the package registry. A development virtual environment or an
editable install is not required to run the migration.

Preview all eligible recordings before writing anything:

```console
memo-migrate-legacy --dry-run
```

Then perform the migration:

```console
memo-migrate-legacy
```

To migrate an old Memo home stored somewhere else, pass that directory:

```console
memo-migrate-legacy --legacy-dir /path/to/old-memo-home
```

The utility discovers old recordings under `$MEMO_HOME/scratch` and the old
tarball archive layout by default. When `--legacy-dir` is supplied, it accepts
either an old Memo home containing `scratch` and `archive`, or a directory whose
immediate children are unpacked recording directories. It reconstructs the
final filesystem snapshot, copies Claude and Codex JSONL traces into agent-run
sidecars, skips sessions already present in the current store, and leaves all
legacy source directories and tarballs untouched.

Migration preserves the final state, not the complete timeline: old per-leg Git
history is not expanded into separate current-format steps. Keep the original
legacy data if that history may still be useful.

## Upgrade existing S3 recordings

The upgrader recognizes every historical directory-session and S3 transport
format in Memo's Git history and converts it to the current compact format.
This includes checkpoint sessions, single-step and complete-history archives,
copied snapshots, unpacked Git snapshot repositories, checksum-sidecar object
layouts, and the current content-addressed transport. First run a read-only
preview:

```console
memo-migrate-legacy --upgrade-s3 --dry-run
```

The command creates a unique disposable work directory under
`$XDG_CACHE_HOME/memo/legacy-migrator` (normally
`~/.cache/memo/legacy-migrator`) instead of relying on a potentially
memory-backed system `/tmp`. Each session's work directory is removed before
the next one proceeds, including on validation failure, Ctrl-C, or SIGTERM.
Use `--scratch-dir DIRECTORY` to select another filesystem. The parent
directory may remain, but completed runs leave no session archives or extracted
copies inside it. As with any process, SIGKILL or a machine power loss can
prevent cleanup.

S3 upgrades process four independent sessions concurrently by default. Use
`--workers N` to select between one and eight workers:

```console
memo-migrate-legacy --upgrade-s3 --dry-run --workers 4
```

Use a repeatable `--session` filter to audit or retry particular S3 sessions
without downloading every indexed archive again:

```console
memo-migrate-legacy --upgrade-s3 --dry-run \
  --session 030a33a509794930ba13868586c1b627 \
  --session 1aa41616320f452c819a37307cdf1e34
```

Every worker uses a separate scratch directory and performs the complete
download, checksum, format, conversion, equivalence, and replacement-validation
sequence. More workers increase CPU, memory, network, and scratch-disk demand;
use `--workers 1` on a constrained machine. Ctrl-C and SIGTERM ask all workers
to unwind and remove their temporary data before the command exits.

The preview downloads every indexed session's selected generation into
temporary storage and verifies its SHA-256 digest and size. Large numeric step
histories are validated and parsed directly from the safely checked archive
stream instead of first materializing every manifest as a scratch file; all
other session data still passes through the normal safe extraction path. The
upgrader converts every filesystem step to Git storage, creates a replacement
generation, then safely scans that archive again without materializing its
manifests. Existing Git histories are retained after validating that every step
is present, connected, and reachable from the published head. Because Git tree
IDs are content-addressed, repeated filesystem states are restored and
fingerprinted once while every step manifest is still checked against its
state. Each unique replacement state must match an independent pre-conversion
fingerprint of every source file's bytes and executable mode; non-snapshot
session files must also match by path, mode, and digest. It does not write to
S3. Sessions already in the latest format are skipped. Active sessions and
sessions with an unselected newer generation are also skipped so the migrator
cannot race an in-progress upload.
If a selected historical Git generation references commit objects omitted from
its bundle, the preview searches older immutable generations of that same
indexed session. It accepts objects only from an archive whose size and SHA-256
digest match S3 metadata and whose own manifests name the exact missing commits,
then rebuilds and independently fingerprints the complete linear history. It
still fails rather than dropping a step when no verified generation contains
the referenced data.
An interactive terminal shows separate overall and current-session progress
bars, each with its own estimated time remaining. The overall estimate becomes
more stable as sessions complete; archive sizes and conversion costs can differ
substantially.

After reviewing the preview, apply the migration explicitly:

```console
memo-migrate-legacy --upgrade-s3
```

For each eligible session, the applying run repeats all preview checks, uploads
the replacement to a staging key, downloads it to confirm byte identity, then
uploads and verifies the final generation. Only after the replacement is
selected and valid does it delete the original generation. A completed
session's old completion record remains unchanged until the final archive has
been remotely verified. The former `--recompress-s3` spelling remains as a
compatibility alias.
