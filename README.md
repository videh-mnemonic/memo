# memo

`memo` continuously records a directory and its attached terminals. A per-user daemon publishes complete directory checkpoints every 15 seconds into a writable local archive.

Requires Python 3.11+ and Git. Install for development with `pip install -e .`, or personally with `pipx install .`. Set `MEMO_HOME` to override the default storage directory, `~/memo`.

## Commands

Start or join the recording for the current directory and open your configured shell:

```console
memo .
```

Several terminals can join the same canonical directory. Each terminal has an independently ordered input/output stream. To record without opening a shell, use `memo --background .`.

End the recording explicitly, then inspect or restore its final checkpoint:

```console
memo --end .
memo --status
memo --load <id> --inspect
memo --load <id> --at final --path <dir>
memo --load <id> --at generation:2 --path <dir>
memo --load <id> --terminals
memo --load <id> --terminals --terminal <terminal-id> --path <file.json>
```

Directory sessions live at `$MEMO_HOME/archive/<namespace>/<id>/`. Immutable checkpoint manifests and snapshots are published through an atomic `HEAD` pointer. `.gitignore` and `.memoignore` control capture; ignored, oversized, unstable, and special entries remain represented in checkpoint metadata. Set `MEMO_MAX_FILE_SIZE` to change the default 100 MiB file limit.

`--at final` resolves `HEAD` once. Historical directory checkpoints can be selected with `generation:N` or `checkpoint:ID`. Restores refuse to replace non-empty destinations unless `--force` is supplied.

## Legacy Sessions

Run an agent normally through memo; all arguments and terminal I/O pass through:

```console
memo claude [args...]
memo codex [args...]
```

Native resume flags append a leg when the referenced session is still in scratch. Resuming a shipped session starts a child session that records the old session ID.

Legacy invocation-scoped scratch and tar sessions remain discoverable and loadable. The compatibility save route is:

```console
memo --status
memo --load <id> --inspect
memo --save                         # sessions idle for at least 48 hours
memo --save --older-than 12h
memo --save --all
memo --save --session <id>
```

Archives are written to `$MEMO_HOME/archive/<namespace>/<id>.tar.gz` with an adjacent `.sha256` file. Existing archives are never overwritten.

Find and unpack a session by ID (the namespace is discovered automatically):

```console
memo --load <id> --unpack
```

Reconstruct its repository state:

```console
memo --load <id> --at initial --path <dir>
memo --load <id> --at leg:1  --path <dir>
memo --load <id> --at final  --path <dir>
```

Memo refuses to overwrite a non-empty directory. Add `--force` when replacement is intentional.

Export traces in the common schema, or retain vendor records with a leg tag:

```console
memo --load <id> --traces
memo --load <id> --traces --path <file.json>
memo --load <id> --traces --path -          # same as omitting --path
memo --load <id> --traces --raw --path <file.json>
```

TODO: Add optional AWS trace recording for durable off-machine trace storage.

Reconstruct a state and add `MEMO_TASK.md` containing the original ordered user prompts:

```console
memo --load <id> --replay --at initial --path <dir>
memo --load <id> --replay --at leg:2  --path <dir>
memo --load <id> --replay --at final  --path <dir>
```

Scratch sessions live under `$MEMO_HOME/scratch`. Remote-backed repositories share a canonical remote namespace across clones; repositories without a usable remote use a namespace derived from their canonical local path. Synthetic capture never writes `.git` into the user's directory.
