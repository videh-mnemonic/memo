# Memo

Memo records how work is done. Memo records the terminal commands, prompts / instructions, agentic traces, and filesystem diffs in one place. These combined sessions make evaluating AI tooling on real work simple.

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
memo end [PATH] [--scope {partial,full}] [--wait-for-push]
```

Publishes a final step and completes the recording. Inside a Memo shell, `PATH`
can be omitted even after changing directories.

- `--scope partial`: mark the recording as missing some intended work.
- `--scope full`: mark the recording as containing all intended work.
- `--wait-for-push`: when S3 is configured, wait for the final cloud upload and
  fail if it cannot complete.

Without `--scope`, an interactive invocation asks. Memo also asks before ending
a recording that still has other attached shells.

### Agent sandboxing

Inside a Memo shell, ordinary `claude` and `codex` commands are automatically
linked to the recording and launched through a Bubblewrap sandbox. Memo adds
the provider's externally-sandboxed dangerous-mode flag so the provider does
not ask for redundant filesystem approvals. Provider arguments otherwise stay
in their normal order.

Bubblewrap is a Linux system dependency. Memo never installs it implicitly or
invokes `sudo`. Install it with the system package manager, then check the host:

```console
sudo apt-get install bubblewrap   # Debian and Ubuntu
memo sandbox setup
```

Native distribution packages should depend on Bubblewrap directly. Python and
editable installations instead report an appropriate installation command.
If Bubblewrap is missing or the kernel blocks the required namespaces, Memo
fails closed and does not launch the provider unsandboxed.

The default sandbox exposes the recording root read-write, linked-worktree Git
metadata, read-only system tools, the active provider's native state, existing
shared `~/.cache`, `~/.triton`, and `~/.nv` directories, and compatible GPUs.
Other home-directory content—including personal `~/.aws` credentials and
sibling projects—is hidden. Shared provider state, Git metadata, and caches are
deliberate cross-recording blast radii; caches are not recorded and can be
deleted, corrupted, or poisoned by an agent. Missing default cache directories
are not created on the host and remain ephemeral if created in the sandbox.

The sandbox shares the host network by default. It can reach localhost
services, internal and VPN networks, cloud metadata endpoints, and services
authenticated by network location. Filtering credential environment variables
does not prevent these network-side effects. Disable networking for one launch
with:

```console
codex --sandbox-args --unshare-net
```

Root-persistent permissions live in `.memo-sandbox`, which should be ignored by
Git and is excluded from Memo filesystem snapshots and replay. Manage it with:

```console
memo sandbox show
memo sandbox allow --read ~/Documents/datasets
memo sandbox allow --read-write ~/Documents/shared-output
memo sandbox allow --read ~/.aws/project.credentials --at ~/.aws/credentials
memo sandbox disallow ~/.aws/credentials
memo sandbox reset
```

Permission changes affect the next agent launch or resume. A sandboxed provider
or debugging shell must start inside the recording root; Memo never mounts a
different current directory automatically. To inspect the same base sandbox
without starting a provider, run `memo sandbox shell`. Its terminal activity,
filesystem changes, policy digest, and exit status remain part of the Memo
recording.

Use `claude --no-sandbox ...` or `codex --no-sandbox ...` for an explicit
one-invocation bypass. The invocation remains terminal- and trace-recorded, but
Memo does not add the dangerous-mode flag and the provider receives normal host
filesystem and environment access.

### List recordings

```console
memo status [SESSION_ID] [--include-archive] [--limit N]
```

- `SESSION_ID`: show one recording; pulls it from S3 if needed.
- `--include-archive`: include remote-only recordings in the list.
- `--limit N`: limit the total number of listed recordings.
- `--active`: list only recordings that are still active.

`--include-archive`, `--limit`, and `--active` cannot be used with `SESSION_ID`.
Single-recording status shows lifecycle, step, terminal, archive, and agent-run
details.

To inspect a row from `memo status`, copy its session ID and run:

```console
memo status SESSION_ID
```

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

To print what Memo has recorded to the console instead of writing a temporary
file:

```console
memo traces SESSION_ID --path -
memo traces SESSION_ID --raw --path -
```

For terminal streams, first list stream IDs, then export one or more:

```console
memo traces SESSION_ID --list-terminals
memo traces SESSION_ID --terminals TERMINAL_ID --path -
```

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
memo import --dry-run
```

Imports or refreshes uncaptured Claude and Codex native sessions as standalone,
agent-only Memo recordings. Unchanged sessions are skipped.

Use `--dry-run` to preview imported, refreshed, skipped, and unimportable
sessions without writing recordings.

### Migrate old Memo recordings

Recordings written by the pre-daemon Memo prototype require the separately
installed [Memo legacy migrator](legacy-migrator/README.md). The one-time utility
is not included in the main Memo package or CLI.

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

Use normal AWS credential sources. Environment variables are the most direct:

```console
export MEMO_S3_BUCKET=my-memo-bucket
export MEMO_S3_REGION=us-east-1
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...   # only for temporary credentials
```

Or use an AWS profile:

```console
aws configure --profile memo
export MEMO_S3_BUCKET=my-memo-bucket
export MEMO_S3_PROFILE=memo
export MEMO_S3_REGION=us-east-1
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

Memo sends current `MEMO_S3_*`, `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, and temporary session-token settings with push/end
requests. If a daemon was started before S3 variables were set, explicit
`memo push` and final `memo end` uploads use the caller's current settings; the
daemon's periodic automatic upload loop uses the daemon process environment.

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

To import existing native agent sessions cautiously, preview before writing:

```console
memo import --dry-run
memo import
memo tidy
memo status --include-archive
```

## How recording works

- A recording belongs to one canonical directory and can have several attached
  Memo shells. Exiting a shell only detaches it; the recording remains active
  until `memo end`.
- Memo runs a per-user background daemon. Filesystem activity requests durable
  numbered steps, with a regular publication interval of about 15 seconds.
- VS Code, terminal, or shell restarts do not complete a recording by
  themselves. Re-enter the directory with `memo` to resume, or run `memo end`
  when the work should be finalized.
- If a terminal exits cleanly, Memo detaches it. If a terminal or VS Code dies
  without detaching while the daemon stays alive, Memo treats that terminal as
  stale after about five minutes. Starting `memo` in that directory will then
  offer to resume the existing recording or complete it and start a new one.
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
