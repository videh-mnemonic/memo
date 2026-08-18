# Git-backed filesystem snapshots

## Goal

Store the filesystem portion of each Memo step as a Git commit so unchanged
files are represented by shared content-addressed blobs instead of being
copied into every snapshot directory. Keep terminal streams, agent traces,
step metadata, completion state, and transport behavior in their existing
Memo-owned formats.

## Local layout

Each session will contain `snapshots.git`, a bare Git repository owned by
Memo. The repository is outside the captured project root and is never part
of a filesystem snapshot. `steps/*.json`, `HEAD`, `streams/`, `agents/`, and
`session.json` remain in the session archive as they are today.

## Capture and restore

1. Scan the working tree with the existing ignore and stability rules into a
   temporary directory under the session archive.
2. Add that temporary tree to the session's Git index and create a commit,
   using the previous filesystem commit as its parent.
3. Record the resulting commit ID in the step manifest. The existing entry
   list remains the authoritative record of ignored, oversized, special,
   missing, and unstable paths.
4. Restore a Git-backed step with `git archive`; retain the existing directory
   snapshot restore path for legacy sessions.

The temporary scan directory is removed after commit publication. Git's
content-addressed object store deduplicates unchanged files; changed files
still create new blobs, and large binary files are still subject to the
existing size policy.

## Compatibility and transport

Manifests gain an optional filesystem commit field. Existing manifests without
that field continue to use `snapshots/<step>` directories. New archives include
the Git repository once, while legacy directory snapshots continue to be
included normally. Pull, integrity checks, replay, and tidy must understand
both forms.

## Failure and recovery rules

- A Git commit may be unreachable after a crash, but it must not become visible
  until the step manifest and `HEAD` are published atomically in the existing
  order.
- Existing step recovery and stale-head behavior must remain unchanged.
- Git command failures must surface as Memo errors and must not delete the
  previous visible step.
- No Git metadata is written into the user's project directory.

## Verification

Add tests for Git-backed capture, unchanged-file deduplication, changed-file
history, restore, integrity validation, legacy directory compatibility, and
archive path selection. Run the full lint and test suites before committing.
