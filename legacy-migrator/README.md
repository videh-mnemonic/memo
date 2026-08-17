# Memo legacy migrator

This separately installed, one-time utility migrates recordings written by the
pre-daemon Memo prototype. It is pinned to `memo-agent==1.0.0`, the recording
format version against which it was built and tested.

Install it from the Memo repository:

```console
uv tool install ./legacy-migrator
```

Preview all eligible recordings before writing anything:

```console
memo-migrate-legacy --dry-run
```

Then perform the migration:

```console
memo-migrate-legacy
```

The utility discovers old recordings under `$MEMO_HOME/scratch` and the old
tarball archive layout. It reconstructs the final filesystem snapshot, copies
Claude and Codex JSONL traces into agent-run sidecars, skips sessions already
present in the current store, and leaves all legacy source directories and
tarballs untouched.

Migration preserves the final state, not the complete timeline: old per-leg Git
history is not expanded into separate current-format steps. Keep the original
legacy data if that history may still be useful.
