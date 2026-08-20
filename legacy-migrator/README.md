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

The preview downloads every indexed session's selected generation into
temporary storage, verifies its SHA-256 digest and size, converts every
filesystem step to Git storage, creates a replacement generation, extracts it
again, and verifies every Git tree and all non-snapshot session files. It does
not write to S3. Sessions already in the latest format are skipped. Active
sessions and sessions with an unselected newer generation are also skipped so
the migrator cannot race an in-progress upload.

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
