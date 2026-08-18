# Security Policy

Memo records terminal I/O, prompts, agent traces, filesystem snapshots, and
archive metadata. Treat Memo archives and their S3-compatible storage as
sensitive.

## Reporting a Vulnerability

Please do not report security issues in public issues or discussions. Use
GitHub private vulnerability reporting for this repository when available.

Include:

- Affected Memo version or commit.
- Steps to reproduce.
- Expected and observed behavior.
- Any relevant platform details, especially operating system, sandbox backend,
  and storage backend.

We will acknowledge valid reports, investigate them privately, and coordinate
fixes before public disclosure.

## Scope

Security-sensitive areas include:

- Sandbox escapes or unexpected host filesystem, credential, network, or device
  exposure.
- Archive tampering, replay path traversal, unsafe extraction, or corruption of
  local recordings.
- S3 transport integrity, overwrite, or authorization boundary failures.
- Leakage of environment variables, native agent traces, terminal input/output,
  or recorded filesystem content beyond documented behavior.

Memo intentionally records sensitive work artifacts. Reports about expected
recording behavior should identify the specific undocumented exposure or
unexpected boundary failure.
