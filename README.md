# Memo

Memo records how work is done. Memo easily records 

 Memo records the filesystem, terminal commands, and agent promts / traces in a single package for further analysis.

## Installation

Memo requires Python 3.11 or newer and Git. From a checkout of this repository:

```console
pipx install .
```

For development:

```console
python -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'
pytest
```

## CLI guide

Run `memo --help` or `memo <command> --help` for built-in help.

### Start or join a recording

```console
memo [DIRECTORY]
```

Opens an interactive shell attached to a recording for `DIRECTORY`, or the
current directory when omitted. If an inactive recording already exists, Memo
offers to resume it or complete it and start a new one.

### End a recording

```console
memo end [PATH] [--scope {partial,full}]
```

Publishes a final step and completes the recording. Inside a Memo shell, `PATH`
can be omitted even after changing directories.

- `--scope partial`: mark the recording as missing some intended work.
- `--scope full`: mark the recording as containing all intended work.

Without `--scope`, an interactive invocation asks. Memo also asks before ending
a recording that still has other attached shells.

### List recordings

```console
memo status [SESSION_ID] [--include-archive] [--limit N]
```

- `SESSION_ID`: show one recording; pulls it from S3 if needed.
- `--include-archive`: include remote-only recordings in the list.
- `--limit N`: limit the total number of listed recordings.

`--include-archive` and `--limit` cannot be used with `SESSION_ID`.

### Export traces

```console
memo traces SESSION_ID [--path PATH] [--terminals ID,...] [--raw]
memo traces SESSION_ID --list-terminals
```

Exports the latest agent trace when the recording contains agent runs;
otherwise it exports all terminal streams.

- `--path PATH`: write to a file; omit it or use `-` for standard output.
- `--terminals ID,...`: export selected terminal streams instead of agent traces.
- `--list-terminals`: list terminal stream IDs without exporting.
- `--raw`: when exporting agent traces, emit provider-native records instead of
  normalized events.

`--list-terminals` cannot be combined with the other export options.

### Replay a filesystem step

```console
memo replay SESSION_ID STEP DIRECTORY [--include-prompts] [--force]
```

Restores the recorded filesystem at `STEP`. Use `0` for the first step or `-1`
for the latest.

- `--include-prompts`: add `.prompts.md` containing terminal input through the
  selected step.
- `--force`: replace a non-empty destination.

Agent-only recordings contain no filesystem and cannot be replayed.

### Import historical agent sessions

```console
memo import
```

Imports or refreshes uncaptured Claude and Codex native sessions as standalone,
agent-only Memo recordings. Unchanged sessions are skipped.

### Push recordings

```console
memo push [SESSION_ID]
```

Pushes one recording, or all local recordings when `SESSION_ID` is omitted, to
the configured S3-compatible archive.

### Pull a recording

```console
memo pull SESSION_ID [--force]
```

Downloads and verifies a recording from S3.

- `--force`: replace an existing local copy.

### Import, archive, and clean up

```console
memo tidy
```

Imports historical agent traces, pushes all recordings, then removes only local
recordings that are complete and confirmed recoverable from S3. Active,
unpublished, or uncertain recordings are retained.

## Optional setup

### S3-compatible storage

Set a bucket to enable `push`, `pull`, `tidy`, automatic uploads every 15
minutes, and a final upload after `memo end`:

```console
export MEMO_S3_BUCKET=my-memo-bucket
```

Optional settings:

- `MEMO_S3_PREFIX`: object-key prefix; defaults to `memo`.
- `MEMO_S3_ENDPOINT`: endpoint for a non-AWS S3-compatible service.
- `MEMO_S3_REGION`: AWS region.
- `MEMO_S3_PROFILE`: AWS credentials profile.
- `MEMO_S3_UPLOAD_CONCURRENCY`: multipart upload concurrency; defaults to `3`.

Memo uses AWS environment, profile, and IAM credential providers. Credentials
need `GetObject`, prefix-limited `ListBucket`, `PutObject`, and
`AbortMultipartUpload`; object deletion is not required. Archives are
checksummed, validated before installation, and installed atomically.

### Show Memo in your shell prompt

Memo sets `MEMO_SESSION_ID` inside every attached shell. Add an indicator after
your existing prompt setup.

Bash (`~/.bashrc`):

```bash
if [[ -n ${MEMO_SESSION_ID:-} ]]; then
    PS1="[memo] $PS1"
fi
```

Zsh (`~/.zshrc`):

```zsh
if [[ -n ${MEMO_SESSION_ID:-} ]]; then
    PROMPT="[memo] $PROMPT"
fi
```

### Import existing traces and archive old work

After configuring S3, run:

```console
memo tidy
```

This discovers existing Claude and Codex traces, turns uncaptured sessions into
agent-only recordings, uploads recordings, and removes only safely archived
completed local copies. Use `memo import` instead if you want to import traces
without uploading or removing anything.

## How recording works

- A recording belongs to one canonical directory and can have several attached
  Memo shells. Exiting a shell only detaches it; the recording remains active
  until `memo end`.
- Memo runs a per-user background daemon. Filesystem activity requests durable
  numbered steps, with a regular publication interval of about 15 seconds.
- Each step contains a directory snapshot, terminal-stream high-water marks,
  and references to captured agent runs. `HEAD` points only to a completely
  published step.
- Filesystem capture stays rooted at the original directory even if a shell
  changes directories. Git-compatible ignore rules are honored. Files over 100
  MiB and files changing during capture are skipped or retain their previous
  captured version rather than producing an inconsistent snapshot.
- Every attached shell has its own ordered terminal stream. Memo records its
  input and output until the shell detaches or the recording ends.
- Memo captures Claude and Codex launched normally inside a Memo shell by using
  private `PATH` shims. Absolute paths and aliases that bypass `PATH` bypass
  automatic agent capture. Native JSONL traces are copied only through the last
  complete record observed.
- Resuming the same native agent session updates its existing run. Agent traces
  are recording-level sidecars, so an older step that references a resumed run
  may expose the run's later trace content.
- New recordings start with `partial` scope. Ending a recording can mark it
  `full` when Memo captured all intended work; Memo does not infer this.

Local data lives under `$MEMO_HOME`, which defaults to `~/memo`. Recordings may
contain source files, terminal input and output, prompts, agent responses, tool
calls, tool results, username, and hostname. Treat both local and S3 archives as
sensitive data. Memo does not capture files outside the recording root or undo
actions performed against external services, databases, or processes.
