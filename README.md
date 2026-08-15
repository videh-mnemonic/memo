# memo

`memo` continuously records a directory and its attached terminals. A per-user daemon publishes complete, immutable steps to a local archive so recorded work can be inspected, exported, replayed, and transported.

Memo requires Python 3.11+ and Git. Install for development with `pip install -e .`, or as a user application with `pipx install .`.

## Record a directory

Start or join the recording for the current directory and open your configured shell:

```console
memo
```

Pass a path to record another directory:

```console
memo /path/to/project
```

Several terminals can join the same canonical directory. Each terminal has an independently ordered input/output stream. To record without opening a shell, use `background`:

```console
memo background [PATH]
```

The lifecycle commands default to the current directory when their path is omitted:

```console
memo status
memo end [PATH]
```

Sessions live at `$MEMO_HOME/archive/<namespace>/<id>/`. Each recording begins at step `0`; later publications increment the step by one. Immutable step manifests and snapshots become visible through an atomically updated numeric `HEAD`.

## Inspect, export, and replay

Inspect the latest published state of a recording:

```console
memo inspect <id>
```

Export terminal events from the latest published step. Output goes to standard output unless `--path` names a file. Use `--terminals` with a comma-separated list to select terminal streams.

```console
memo traces <id>
memo traces <id> --path <file.json>
memo traces <id> --terminals <terminal-id>,<terminal-id>
```

Trace records are deterministic and bounded by the latest step's terminal high-water marks.

Replay a recorded filesystem state into a directory:

```console
memo replay <id> 0 <dir>
memo replay <id> -1 <dir>
memo replay <id> 3 <dir>
```

Step `0` is the initial state, `-1` selects the latest published state, and any other nonnegative integer selects that step. Memo refuses to replace a non-empty destination unless `--force` is supplied.

Add `--include-prompts` to write `<dir>/.prompts.md`. The document contains decoded terminal input events from the selected step boundary, grouped by terminal with timestamps and metadata. It records terminal input rather than inferred application-specific prompts.

```console
memo replay <id> -1 <dir> --include-prompts
```

## Capture policy

Memo applies `.gitignore` rules within each Git repository. Nested repositories, including worktrees and submodules represented by a `.git` file, begin a new ignore scope instead of inheriting rules from an outer repository. Memo's own archive and runtime directories are always excluded when they are inside a recorded tree.

Operational defaults such as the step interval, maximum file size, watcher debounce, and push interval are editable constants in `memo/config.py`.

## S3 transport

Set `MEMO_S3_BUCKET` to enable S3-compatible transport. Credentials come from the standard AWS SDK credential chain and are never written to session metadata.

```console
memo push
memo push <id>
memo pull <id>
memo pull <id> --force
```

Push packages the complete published history: all step manifests, snapshots, and bounded terminal stream data needed for historical replay. Data and checksum objects publish before `latest.json`. Pull verifies package integrity, rejects unsafe archive entries, and installs atomically. Existing local state is not replaced without `--force`.

Automatic push runs whenever S3 transport is configured.

## Configuration

Memo reads only location and deployment settings from the environment:

- `MEMO_HOME`: local storage root; defaults to `~/memo`.
- `MEMO_S3_BUCKET`: bucket name; setting it enables transport and automatic push.
- `MEMO_S3_PREFIX`: object-key prefix; defaults to `memo`.
- `MEMO_S3_ENDPOINT`: optional endpoint for an S3-compatible service.
- `MEMO_S3_REGION`: optional AWS region.
- `MEMO_S3_PROFILE`: optional AWS SDK profile.

Run `memo --help` or `memo <command> --help` for the complete command-specific argument list.
