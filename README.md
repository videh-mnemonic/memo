# Memo

Memo records work as it happens: changes to a project directory, input and output from attached terminal shells, and native Claude or Codex traces created from those shells. A recording can be inspected, replayed, exported, and moved through S3-compatible storage.

The most useful mental model is:

> A Memo recording belongs to one directory, can have several recorded shells, and remains alive until you explicitly end it.

Memo runs a per-user daemon in the background, but there is no separate background-recording mode. Running `memo` always opens a normal interactive shell connected to a recording.

## Installation

Memo requires Python 3.11 or newer and Git.

For development:

```console
pip install -e '.[dev]'
pytest
ruff check memo tests
ruff format --check memo tests
```

For a user installation from this repository:

```console
pipx install .
```

The source tree is organized by responsibility:

```text
memo/
  agents/             Coding-agent capture, import, and run metadata
    harnesses/        Provider-specific Claude and Codex integrations
  cli/commands/       User-facing command implementations
  daemon/             Background coordination and the active-session registry
  export/             Trace export and filesystem replay
  recording/          Local paths, metadata, snapshots, streams, and storage
  transport/          Archive construction and S3-compatible transport
```

The `tests/` tree mirrors these areas, with cross-component behavior under
`tests/integration/`.

## Start recording

From the directory you want to record:

```console
memo
```

Or name another directory:

```console
memo /path/to/project
```

Memo creates a recording and opens your configured shell in that directory. Use the shell normally: run commands, edit files, start development servers, invoke Claude or Codex, and change directories as needed.

The directory passed to `memo` is the recording's identity and filesystem root. Memo recursively snapshots that directory for the lifetime of the recording.

Changing directory inside the shell does not stop terminal recording. All input and output in the Memo shell continues to be recorded. Filesystem snapshots, however, remain rooted at the original directory; files outside that tree are not added merely because the shell changed into another directory.

### Show Memo in your shell prompt

Memo sets `MEMO_SESSION_ID` inside every attached shell. You can use it to make recorded shells visibly different from ordinary shells, even after changing directories.

For Bash, add this near the end of `~/.bashrc`, after any other prompt setup:

```bash
if [[ -n ${MEMO_SESSION_ID:-} ]]; then
    PS1="[memo] $PS1"
fi
```

For Zsh, add this near the end of `~/.zshrc`, after any theme or prompt setup:

```zsh
if [[ -n ${MEMO_SESSION_ID:-} ]]; then
    PROMPT="[memo] $PROMPT"
fi
```

New Memo shells will then have a prompt such as:

```text
[memo] user@host:~/project$
```

The indicator disappears automatically in shells that were not opened through Memo.

## Recordings and shells are different things

A recording is not the same as a shell process.

- One recording represents the history of one canonical directory.
- Each invocation of `memo` opens a new shell with its own terminal ID and ordered event stream.
- Several Memo shells can be attached to the same recording at once.
- Closing a shell detaches only that shell. It does not end the recording or affect other shells.
- A detached terminal stream is permanent and cannot later receive more events.
- Opening another shell always creates a new terminal stream, including when resuming an existing recording.

If a recording already has attached shells, another `memo` invocation joins it automatically. Memo does not offer to replace a recording while other shells are attached.

If the last shell has closed, the recording remains active. Running `memo` again presents:

```text
A Memo recording already exists for this directory.

1. Resume existing recording
2. Start a new recording
```

Resume keeps the existing history and creates a new terminal stream. Start new completes the old recording first—including its final filesystem, terminal, and agent state—then creates a separate recording.

Memo rejects simultaneously active recording roots that are the same directory or that contain one another. Sibling project directories can be recorded independently.

## End a recording

Recordings end explicitly:

```console
memo end
```

Inside a Memo shell, pathless `memo end` uses that shell's recording identity. It still targets the right recording if you have changed directories.

Outside Memo, pathless `memo end` targets the current directory. You can always provide a directory explicitly:

```console
memo end /path/to/project
```

If no other shells are attached, Memo ends the recording immediately. If ending it would invalidate other attached shells, Memo asks first:

```text
This recording has 2 other attached terminals.
End the recording for all terminals? [y/N]
```

Declining leaves the recording untouched. If attachment membership changes while the question is open, Memo reports the change and asks again rather than acting on a stale answer.

When another terminal ends a recording, affected Memo shells terminate cleanly with a `memo: recording ended` message.

A recording ends only through `memo end` or by choosing Start new when it has no attached terminals. Exiting every shell is not enough.

New recordings have a `partial` capture scope. Interactive `memo end` asks whether Memo captured all intended work; confirming marks the completed recording `full`, while declining keeps it `partial`. Scripts can choose explicitly with `memo end --scope full` or `memo end --scope partial`. Memo never infers full coverage from terminal count or other activity.

## What a step means

Memo periodically publishes numbered steps, beginning with step `0`. A step is a consistent, durable boundary containing:

- A recursive snapshot of the recorded directory.
- A high-water mark for every terminal stream.
- References to safely installed agent runs and traces.

`HEAD` identifies the latest completely published step. A failed or interrupted publication does not make a partial step visible.

Terminal streams remain independently ordered. A step's high-water marks define exactly which events belong to that step, so replay and export do not accidentally include later terminal activity.

Native agent runs are recording-level sidecars. When the same native Claude or Codex session is resumed, Memo updates the existing archived run and trace. Consequently, an older step that references that run can expose the run's later trace content; agent traces are not byte-bounded independently for every historical step.

## Filesystem capture policy

Memo applies `.gitignore` rules within each Git repository. A nested repository, worktree, or submodule represented by its own `.git` file starts a new ignore scope instead of inheriting every rule from an outer repository.

Memo's local archive and runtime directories are excluded if they happen to be inside the recorded tree.

Files larger than the configured limit—100 MiB by default—are not copied into a new snapshot. When possible, Memo retains the previously captured version and marks the entry accordingly. Files that change while being copied are treated similarly rather than publishing an inconsistent copy.

The default publication interval is 15 seconds, with filesystem activity able to request earlier publication after a short debounce. An active recording continues publishing filesystem steps even when it has no attached terminals.

## Claude and Codex capture

Run supported agents normally inside a Memo shell:

```console
claude
codex
codex resume <native-session-id>
```

There are no `memo claude` or `memo codex` commands.

Memo prepends a private shim directory to the `PATH` of each Memo shell. The generated shim:

1. Notifies Memo that a supported agent is starting.
2. Runs the real executable with the original arguments and terminal behavior.
3. Reports its completion and exit status.
4. Lets Memo collect every matching native JSONL trace through a complete-record boundary.

The shim is local to Memo shells and does not modify global shell configuration or `PATH`. Invoking an agent through an absolute executable path, or through an alias that bypasses `PATH`, bypasses automatic capture.

Capture is scoped by provider and the directory in which the agent was launched. An agent started after `cd` still belongs to the original Memo recording, while its launch directory is used to match its native trace.

Memo can capture several agents running concurrently, several distinct native sessions, and later resumes of the same native session. It identifies a run by provider plus the provider's native session ID.

Memo does not continuously scan all Claude and Codex history. A capture window begins only when a supported shim runs. During that window, another process from the same provider and launch directory may be indistinguishable and may also be archived. Conversely, a direct executable invocation that bypasses the shim is not expected to be captured.

Native trace files can still be growing when Memo publishes. Memo copies only through the last complete newline observed at a fixed byte boundary. An incomplete final JSON record remains pending for a later collection pass.

## Review recordings

List local recordings:

```console
memo status
memo status --limit 10
memo status --include-archive --limit 25
```

Status shows each recording's root, lifecycle state, capture scope, age, latest local activity, attached terminal count, local step count, local archive size, and cloud archive progress. Times are compact and relative; `STEPS` is a human-oriented count even though replay selectors remain zero-based. `ARCHIVED` is also count-based: `13/15` means the cloud contains history through 13 of 15 local steps, while `—` means the recording has never been uploaded.

By default, status lists local recordings. `--include-archive` appends remote-only recordings after the local recordings and avoids duplicate session IDs. `--limit` caps the combined number of rows. Metadata unavailable without downloading an archive is shown as `—`.

Show one recording using the same status columns:

```console
memo status <session-id>
```

Local recordings use the globally unique session ID as their archive key:

```text
$MEMO_HOME/archive/<session-id>/
  session.json
  HEAD
  steps/
  snapshots/
  streams/
  agents/
    runs/
    traces/
```

`MEMO_HOME` defaults to `~/memo`.

Each `session.json` records the Memo version, username, and hostname from the computer that originally created the recording. That origin does not change when another computer pulls or re-uploads it.

## Recover native agent sessions

Recover Claude and Codex logs that Memo did not capture:

```console
memo import
```

The command scans both providers, checks local recordings and same-origin cloud recordings, and imports every uncovered native session. Imported sessions use the native ID as their Memo ID and have `agent-only` scope. They contain the native trace but no terminal or filesystem history, so `memo traces` works while `memo replay` refuses them.

Claude and Codex sessions may be resumed later, so agent-only sessions remain active and refreshable. Running `memo import` again skips unchanged traces and adds a step when a native log has grown. Divergent or ambiguous logs are reported without overwriting archived data.

## Export traces

Export traces from the latest published step to standard output:

```console
memo traces <session-id>
```

Single-session `status`, `traces`, and `replay` automatically pull a session from the configured cloud archive when it is not already available locally.

Write them to a file:

```console
memo traces <session-id> --path traces.json
```

If the step contains captured agent runs, the default export is the normalized agent trace. Use `--raw` to export the provider-native agent records instead:

```console
memo traces <session-id> --raw
```

List the terminal stream IDs available at the latest step:

```console
memo traces <session-id> --list-terminals
```

To export terminal events explicitly, select one or more of those IDs:

```console
memo traces <session-id> --terminals <terminal-id>
memo traces <session-id> --terminals <terminal-id>,<terminal-id>
```

If a recording has no agent runs, the default trace export contains all terminal streams.

## Replay filesystem state

Restore a recorded filesystem step into a destination directory:

```console
memo replay <session-id> 0 <destination>
memo replay <session-id> 3 <destination>
memo replay <session-id> -1 <destination>
```

Step `0` selects the initial published state. A nonnegative integer selects that exact step, and `-1` selects the latest published step.

Memo refuses to replace a non-empty destination unless explicitly requested:

```console
memo replay <session-id> -1 <destination> --force
```

To add a `.prompts.md` file containing terminal input events through the selected step boundary:

```console
memo replay <session-id> -1 <destination> --include-prompts
```

This file contains recorded terminal input, grouped by terminal with sequence and timing information. Memo does not infer which terminal input was an application-level prompt.

## S3-compatible transport

Set a bucket to enable manual and automatic transport:

```console
export MEMO_S3_BUCKET=my-memo-bucket
```

Push all local recordings or one recording:

```console
memo push
memo push <session-id>
```

To import native sessions, push all recordings, and then remove local recordings
that are complete and fully archived:

```console
memo tidy
```

`memo tidy` retains active recordings, recordings whose latest step was not
successfully pushed, and anything else whose remote recoverability is uncertain.
Removed recordings remain available from the cloud with `memo pull`.

Pull a recording by its globally unique ID:

```console
memo pull <session-id>
memo pull <session-id> --force
```

Memo uses the MinIO S3-compatible client with AWS environment, profile, and IAM credential providers, and does not write credentials into recordings. Packages are deterministic, checksummed, validated before installation, and installed atomically. Pull rejects unsafe archive paths and does not replace existing local data without `--force`.

Remote objects are organized by the recording's original identity:

```text
s3://<bucket>/<prefix>/<username>/<hostname>/sessions/<session-id>/
  generations/
    00000042-<archive-sha256>.tar.zst
  completions/
    00000042-<archive-sha256>.json

s3://<bucket>/<prefix>/index/sessions/<session-id>/<index-sha256>.json
```

The direct index lets `memo pull <session-id>` locate a recording without listing the whole bucket. Generation and index keys contain their content digests, making repeated publication idempotent without conditional multipart operations. A completion record is published only after its generation exists. If concurrent writers publish different digests for the same step, or different index or completion records for one session, Memo reports the conflict instead of choosing one silently.

Pull uses the sole completion record for a completed recording. While a recording is still active, it lists that recording's `generations/` prefix and selects the highest generation. The archive digest is verified while streaming the download. Username and hostname are encoded safely for object keys, but remain intentionally visible to anyone who can inspect the bucket.

Memo's S3 credentials need `s3:GetObject`, prefix-limited `s3:ListBucket`, `s3:PutObject`, and `s3:AbortMultipartUpload` so failed multipart uploads can be cleaned up. They do not need object deletion permissions.

When S3 is configured, the daemon attempts an automatic push every 15 minutes and an immediate final push after `memo end`. Local completion remains successful if S3 is unavailable; the completed recording remains eligible for a later automatic or manual retry.

## Configuration

Memo reads these location and deployment settings from the environment:

- `MEMO_HOME`: local storage root; defaults to `~/memo`.
- `MEMO_S3_BUCKET`: S3 bucket; setting it enables transport and automatic push.
- `MEMO_S3_PREFIX`: object-key prefix; defaults to `memo`.
- `MEMO_S3_ENDPOINT`: optional endpoint for an S3-compatible service.
- `MEMO_S3_REGION`: optional AWS region.
- `MEMO_S3_PROFILE`: optional AWS credentials profile.
- `MEMO_S3_UPLOAD_CONCURRENCY`: parallel multipart upload count; defaults to `3`.

Memo sets `MEMO_SESSION_ID` and `MEMO_TERMINAL_ID` inside recorded shells. They identify the recording and attached terminal and are used internally by commands such as pathless `memo end`.

Run `memo --help` or `memo <command> --help` for command-specific syntax.

## Privacy and operational expectations

Memo is designed to preserve detailed working context. That means a recording may contain:

- Source files and other non-ignored files under the recorded root.
- Terminal input, including commands and pasted text.
- Terminal output, including logs and command results.
- Native Claude and Codex traces, including prompts, responses, tool calls, and tool results.
- The originating username and hostname.

Treat Memo archives and S3 buckets as sensitive data. Review ignore rules and storage permissions, avoid recording secrets when possible, and restrict access to remote objects appropriately.

Memo records observed state; it is not a transactional backup system for external services, databases, processes, or files outside the recorded root. Replay restores captured filesystem content and optionally renders terminal inputs. It does not recreate running processes or undo actions performed against external systems.
