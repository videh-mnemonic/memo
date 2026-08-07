# memo

`memo` wraps Claude Code and Codex CLI sessions, preserving their Git starting point, commits, final working tree, and traces as portable archives.

Requires Python 3.11+ and Git. Install for development with `pip install -e .`, or personally with `pipx install .`. Set `MEMO_HOME` to override the default storage directory, `~/memo`.

## Commands

Run an agent normally through memo; all arguments and terminal I/O pass through:

```console
memo claude [args...]
memo codex [args...]
```

Native resume flags append a leg when the referenced session is still in scratch. Resuming a shipped session starts a child session that records the old session ID.

Inspect and ship captured sessions:

```console
memo --status
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
memo --load <id> --traces --path <file.json>
memo --load <id> --traces --raw --path <file.json>
```

Reconstruct a state and add `MEMO_TASK.md` containing the original ordered user prompts:

```console
memo --load <id> --replay --at initial --path <dir>
memo --load <id> --replay --at leg:2  --path <dir>
memo --load <id> --replay --at final  --path <dir>
```

Scratch sessions live under `$MEMO_HOME/scratch`. Remote-backed repositories share a canonical remote namespace across clones; repositories without a usable remote use a namespace derived from their canonical local path. Synthetic capture never writes `.git` into the user's directory.
