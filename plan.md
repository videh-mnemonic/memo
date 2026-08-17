# Memo Agent Sandboxing

## Default behavior

Inside a Memo shell:

```bash
claude [args...]
codex [args...]
```

Memo automatically:

1. Links the agent invocation to the active Memo recording.
2. Launches it through the Memo sandbox.
3. Records terminal I/O and native agent traces.
4. Records whether the invocation was sandboxed and its effective sandbox policy.
5. Passes ordinary arguments unchanged to Claude or Codex.

The Memo shell and daemon remain outside the sandbox.

For sandboxed invocations, Memo automatically enables the provider's non-interactive dangerous
mode:

```text
claude → --dangerously-skip-permissions
codex  → --dangerously-bypass-approvals-and-sandbox
```

Memo does not add this flag when the user supplies provider-native permission or sandbox options,
or when the invocation uses `--no-sandbox`. Explicit user arguments always take precedence.

## Default sandbox

Each agent receives:

- Memo recording root: read-write.
- Linked-worktree shared Git metadata: read-write.
- Other user folders: hidden.
- System executables, libraries, compilers, and headers: read-only.
- Selected system configuration and certificates: read-only.
- Existing host cache trees `~/.cache`, `~/.triton`, and `~/.nv`: read-write by default.
- The active provider's existing native state: read-write.
- Ephemeral private home, `/tmp`, `/dev/shm`, and process namespace.
- Network access.
- GPU access through validated NVIDIA devices, driver libraries, and read-only driver and topology
  information when compatible hardware is available; missing GPU hardware does not fail the
  launch.
- No inherited AWS configuration or environment credentials, SSH agent, Docker socket, Memo daemon
  socket, or Memo archives.

If the sandbox cannot start, Memo fails closed rather than silently launching without it.

## Sandbox backend

Memo uses Bubblewrap (`bwrap`) as its sandbox backend on Linux. Installing the executable is not
enough: on the first sandboxed launch, Memo lazily performs a self-test that creates a representative
sandbox and verifies the required namespace, mount, process, and device behavior. If Bubblewrap is
missing or the host kernel, security policy, or installation does not permit the required
isolation, sandboxed agent launches fail closed with a diagnostic.

Memo never installs Bubblewrap implicitly during an agent launch or invokes `sudo`. Native system
packages should declare Bubblewrap as a dependency. Python and editable installations report the
platform-appropriate package-manager command, and `memo sandbox setup` explicitly checks the
installation and runs the compatibility self-test.

Every default launch uses a fresh mount, user, PID, IPC, and UTS namespace. It retains the host
network namespace because network access is enabled by default. Memo drops capabilities before
starting the provider, mounts a private `/proc`, and creates a minimal private `/dev`. When a
compatible NVIDIA GPU is present, Memo discovers and exposes all applicable `/dev/nvidia*` and
`/dev/nvidia-caps/*` nodes, any required render devices, driver libraries outside the ordinary
read-only system mounts, and the read-only `/proc/driver/nvidia` and `/sys` information required by
NVML, CUDA, NCCL, and device-topology discovery. Render devices and host information are included
only when present and required; GPU support is capability-based rather than tied to one fixed host
layout. The provider starts in a new process namespace while preserving Memo's existing PTY and
process-group behavior. Bubblewrap terminates it when the wrapper or its parent dies. Writes through
the recording and cache mounts resolve to the invoking user's ownership on the host.

The Bubblewrap policy provides filesystem, process, IPC, hostname, and capability isolation. It
does not initially provide syscall filtering, resource limits, or network isolation. Those are not
required by the current threat model and can be added separately without changing the filesystem
policy.

## Writable storage and environments

The sandbox has no persistent, general-purpose storage outside the recording root. Its writable
areas are:

```text
Recording root          persistent and monitored by Memo
Native provider state   persistent shared Codex/Claude sessions and authentication
Host cache trees        persistent shared caches, not monitored by Memo
/tmp                    private and destroyed when the agent exits
/dev/shm                private and destroyed when the agent exits
```

Memo mounts the active provider's complete existing state read-write: `~/.codex` for Codex or
`~/.claude` for Claude. A harness can declare an additional required state path, such as
`~/.claude.json`. State for the inactive provider remains hidden. The mounted state includes
authentication, configuration, plugins, skills, caches, indexes, and native sessions from other
projects. This is an accepted provider-level shared blast radius, like linked-worktree Git metadata.
Memo does not create a second provider session store.

Python environments and other project dependencies normally live in ignored directories inside
the recording root, such as `.venv`, `node_modules`, or Rust's `target`. Dependency manifests and
lockfiles remain in the recording root. System-wide and `--user` package installation are not
supported by default.

Memo bind-mounts the host's existing `~/.cache`, `~/.triton`, and `~/.nv` trees read-write at their
normal locations inside the synthetic home. These caches are shared by all sandboxed projects and
worktrees so package downloads, model data, compiled kernels, and similar artifacts can be reused.
They are disposable shared state: any sandboxed agent can delete, corrupt, or poison them, and Memo
does not snapshot or restore them. Memo does not inspect or filter their contents. Tokens, private
model data, or other sensitive material stored in these trees are therefore visible to agents;
high-value credentials should not be stored there.

The cache mounts are shipped defaults, so users do not grant them for every recording. A user can
remove one from a particular recording with, for example:

```bash
memo sandbox disallow ~/.cache
```

`read_write_if_present` never creates a directory on the host. Only cache roots that already exist
when the sandbox launches are mounted persistently. If a configured cache root is absent, software
may create the corresponding path inside the synthetic home, but that directory is ephemeral for
that launch. `memo sandbox show` distinguishes configured defaults from effective mounts and labels
each absent `read_write_if_present` path as `absent; ephemeral if created`.

Memo preserves the user's normal `HOME` path but mounts a private ephemeral filesystem there. For
example, `/home/user` inside the sandbox is not the host's `/home/user`; Memo mounts only the
recording root, shared cache trees, and active native provider state back into their expected
locations beneath it.

Memo does not override cache or temporary-directory variables:

```text
HOME              preserved path backed by the synthetic home
XDG_CACHE_HOME    unset unless explicitly supplied by the user
PIP_CACHE_DIR     unset unless explicitly supplied by the user
UV_CACHE_DIR      unset unless explicitly supplied by the user
npm_config_cache  unset unless explicitly supplied by the user
TMPDIR            unset unless explicitly supplied by the user
```

Tools therefore use their normal default cache paths without Memo-specific environment overrides.
Paths under `~/.cache`, `~/.triton`, and `~/.nv` resolve to the persistent shared host caches, while
other paths in the synthetic home and private `/tmp` remain ephemeral.
Existing system Python installations, compilers, headers, and libraries remain available read-only.
Memo does not set environment variables that change language or package-manager behavior. It
preserves the host `PATH` in its original order, removes the Memo shim directory to prevent recursive
interception, and ensures the resolved provider executable is mounted. Entries under the recording
root, such as an activated `.venv/bin`, therefore continue to work; entries for hidden host paths
remain strings in `PATH` but cannot expose or execute files that are not mounted.

Memo never creates or modifies `AGENTS.md`, `CLAUDE.md`, or another instruction file in the
recording root. Instead, it injects its packaged `agent-guidance.md` at every sandboxed provider
launch, including resume invocations, using the provider's runtime instruction mechanism:

```text
Claude  → --append-system-prompt-file /run/memo/agent-guidance.md
Codex   → developer_instructions composed with the effective existing value
```

For Claude, Memo mounts the packaged guidance read-only at the private path shown above and appends
it without replacing Claude's built-in prompt or the project's `CLAUDE.md`. For Codex, Memo passes
the guidance as additional `developer_instructions` and preserves any existing user or invocation
value by composing the two rather than replacing either one. Project `AGENTS.md` files continue to
load normally. Provider-specific quoting and composition happen in the harness adapter; the user's
ordinary arguments remain unchanged.

Memo records the injection method and the packaged guidance version or digest in launch metadata.
The guidance is advisory ergonomics, not a security boundary: Bubblewrap enforces the filesystem
policy even when the provider ignores or contradicts the instruction. The injected instruction
says:

> Create dependency environments inside the recording root and ensure generated dependency
> directories are ignored by Git. Keep source code, configuration, dependency manifests, and
> lockfiles in the recording root. Use the conventional shared cache locations and ephemeral `/tmp`
> for temporary files. Do not install system-wide or with `pip --user`. Ask the user when a missing
> system dependency or large external dataset requires host setup or an additional sandbox grant.
> Do not modify `.memo-sandbox`; ask the user to change sandbox permissions.

Large datasets, model caches outside the shared cache trees, or toolchains outside the project
require an explicit `memo sandbox allow` grant. A persistent scratch capability can be added later
if real workflows demonstrate a need for it; it is not part of the default design.

## Managing permissions

Permissions belong to the Memo root and persist across recordings in `<recording-root>/.memo-sandbox`.
On the first sandboxed provider or debugging-shell launch, Memo atomically copies its packaged
`defaults.toml` to this path if the file does not already exist. Existing root policies are never
silently changed when Memo's packaged defaults change.

The policy file contains the materialized defaults plus user grants:

```toml
network = true
gpu = true

[[grants]]
source = "/home/me/Documents/datasets"
destination = "/home/me/Documents/datasets"
mode = "read"

[[grants]]
source = "/home/me/.aws/project1.credentials"
destination = "/home/me/.aws/credentials"
mode = "read"
```

Users can edit `.memo-sandbox` directly or use:

```bash
memo sandbox show
memo sandbox allow --read ~/Documents/datasets
memo sandbox allow --read-write ~/Documents/shared-output
memo sandbox disallow ~/Documents/datasets
memo sandbox reset
```

An exact read-only host path can be mapped to a different conventional location inside the
sandbox:

```bash
memo sandbox allow --read SOURCE --at SANDBOX_DESTINATION
```

This is the general mechanism for supplying project-specific credentials or configuration without
adding a Memo credential manager. For example:

```bash
memo sandbox allow \
  --read ~/.aws/project1.credentials \
  --at ~/.aws/credentials
```

The sandbox sees the project credential at AWS's standard path, while the user's personal
`~/.aws/credentials` remains hidden. The source stays outside the recording and is mounted
read-only. Memo records the source and destination paths but never the file contents. Grants are
keyed by sandbox destination, so `memo sandbox disallow ~/.aws/credentials` removes this mapping.
User grants are trusted: Memo translates them into sandbox mounts and lets the sandbox backend
reject combinations it cannot apply. Defaults are applied first, followed by grants in file order;
later mounts take precedence when the backend permits them.

Changes apply to the next agent invocation. To expand access during a conversation:

```bash
# Exit the agent
memo sandbox allow --read ~/Documents/datasets
codex resume <id>
```

Extra paths are read-only unless explicitly granted write access. Writable extra paths are labeled
"not filesystem-captured," because Memo snapshots only the recording root.

The recording root is mounted read-write, then `.memo-sandbox` is mounted over itself read-only.
The user and host tools can edit the file, but a sandboxed agent cannot change the policy used by a
future launch. An invocation using `--no-sandbox` retains normal host write access.

`.memo-sandbox` is Memo control state rather than project content:

- Recommend adding it to `.gitignore`.
- Exclude it from Memo filesystem snapshots and replay.
- Do not upload it as project content.
- Record the effective policy summary and digest with every agent launch.

The policy file remains when a recording ends and is reused by future recordings in the same root.
Each linked worktree has its own file because each worktree is a distinct Memo root. A malformed
policy fails the sandboxed launch with a file and line diagnostic; Memo never overwrites it
automatically. `memo sandbox reset` explicitly replaces it with the current packaged defaults.

## Working-directory behavior

A sandboxed Claude or Codex invocation can start only when its current working directory is the
Memo recording root or one of its descendants. Memo resolves both paths canonically before checking
containment so `..` components and symlinks cannot bypass the boundary.

```text
Recording root: ~/Documents/project1

~/Documents/project1          allowed
~/Documents/project1/src      allowed
~/Documents/project2          rejected
~/Documents/datasets          rejected, even if mounted
```

Additional read or read-write mounts provide access after launch; they never become valid launch
roots. Memo does not silently mount the current directory or relocate the provider to the recording
root. A rejected launch reports both paths and tells the user to return to the recording root or
run the provider with `--no-sandbox`.

An invocation using `--no-sandbox` can start outside the recording root and remains terminal- and
trace-monitored, but Memo's filesystem capture remains rooted at the original recording directory.
Changing directories after a sandboxed launch does not expand its fixed mount policy.

## Debugging shell

Users can inspect and debug the environment Memo gives an agent without starting a provider:

```bash
memo sandbox shell
```

This command resolves the root's current `.memo-sandbox` through the same policy and command-building
code as an agent launch. It starts the user's shell in the foreground with the same recording-root
mount, explicit grants, shared caches, synthetic home, filtered environment and `PATH`, working
directory, network access, GPU access, namespaces, and lifecycle behavior. It does not add a
provider dangerous-mode flag, inject provider guidance, or mount Claude or Codex state. If debugging
provider state later proves necessary, a separate explicit `--provider claude|codex` option can be
added rather than exposing both providers by default.

The debugging shell requires an active Memo recording and applies the same canonical cwd containment
rule as a sandboxed provider. It fails closed when policy resolution or sandbox setup fails.

The shell is a first-class recorded Memo launch. It runs as a foreground child of the existing
recorded terminal, so Memo captures its input and output and the normal filesystem observer captures
changes under the recording root. The CLI reports shell launch and completion events to the daemon,
which stores the command, canonical cwd, start and completion times, effective policy summary and
digest, completion status, and exit code. An interrupted or abruptly terminated shell is finalized
using the same lifecycle rules as other monitored children. It is labeled `sandbox-shell`, not as a
Claude or Codex agent run, and has no provider trace or native provider session ID.

## Invocation without the sandbox

A user can bypass the sandbox for one invocation:

```bash
claude --no-sandbox [claude args...]
codex --no-sandbox [codex args...]
```

Memo strips `--no-sandbox` before launching the provider and does not automatically add its
dangerous-mode flag.

The invocation remains:

- Linked to the Memo recording.
- Terminal-monitored.
- Trace-captured.
- Marked as `no-sandbox` in its launch metadata.

It receives the normal host environment and filesystem permissions.

## Advanced sandbox arguments

Provider arguments come first. An explicit marker introduces arguments for the sandbox backend:

```bash
codex <codex args...> --sandbox-args <sandbox args...>
claude <claude args...> --sandbox-args <sandbox args...>
```

Example:

```bash
codex --model gpt-5 --sandbox-args --unshare-net
```

Because raw sandbox-backend arguments can weaken isolation, Memo should:

- Display the resulting command before launch.
- Record the arguments in launch metadata.
- Mark the launch as `custom`.
- Reject arguments that interfere with Memo's required mounts or lifecycle controls.

Normal filesystem additions should use `memo sandbox allow`, not raw bind arguments.

The shim removes Memo-specific options before invoking the provider and rejects conflicting options,
including combining `--no-sandbox` with `--sandbox-args`. The provider harness constructs the
effective command so injected dangerous-mode and guidance arguments appear in provider-valid
positions rather than being blindly appended. Memo records both the command entered by the user and
the effective provider command it launched; sandbox-backend arguments are recorded separately.

## Git worktrees

Memo automatically detects linked worktrees and mounts their shared Git metadata. Consequently:

- Sibling worktree files remain hidden.
- Git objects, refs, and administration remain shared.
- Corruption of shared Git state remains inside the accepted repository-level blast radius.

## Implementation boundary

Sandbox-specific code and the shipped default configuration live in a dedicated package:

```text
memo/agents/sandbox/
├── __init__.py
├── config.py       load and validate configuration
├── policy.py       resolve the root policy and effective launch permissions
├── guidance.py     construct provider-specific runtime instruction injection
├── command.py      construct the sandbox command and environment
├── defaults.toml   version-controlled default sandbox configuration
└── agent-guidance.md
```

`defaults.toml` contains only concise, user-reviewable policy for the Bubblewrap backend: network
and GPU availability, shared host cache trees, read-only system visibility, and inherited terminal
environment. On first use in a Memo root, these defaults initialize `.memo-sandbox`; subsequent
launches use the root file without silently merging newer packaged defaults.

The sandbox uses an allowlist and an empty synthetic filesystem. User paths such as `~/.aws` are
hidden because Memo never mounts them, not because a deny rule attempts to conceal them. A default
configuration has this shape:

```toml
network = true
gpu = true

[home]
read_write_if_present = [".cache", ".triton", ".nv"]

[system]
read_only = ["/usr"]
read_only_if_present = [
    "/bin",
    "/lib",
    "/lib64",
    "/etc/passwd",
    "/etc/group",
    "/etc/ld.so.cache",
    "/etc/localtime",
    "/etc/gitconfig",
    "/etc/ssl/certs",
    "/etc/resolv.conf",
    "/etc/hosts",
    "/etc/nsswitch.conf",
]

[environment]
inherit = "all"
exclude = [
    "*_API_KEY",
    "*_ACCESS_KEY",
    "*_SECRET",
    "*_TOKEN",
    "*_PASSWORD",
    "*_CREDENTIALS",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "DATABASE_URL",
    "KUBECONFIG",
    "SSH_AUTH_SOCK",
    "GPG_AGENT_INFO",
    "DOCKER_HOST",
    "MEMO_*",
]
```

Exclusion matching is case-insensitive. Memo reconstructs sandbox-specific values such as `HOME`,
`PWD`, and shell-internal variables instead of copying their host values. It preserves host `PATH`
ordering after removing the Memo shim directory. It also removes host socket variables whose targets
are not deliberately mounted. Root-policy environment overrides can explicitly allow or disallow
named variables; Memo records names but never values.

The `/etc` entries are an explicit, read-only compatibility allowlist for identity lookup, dynamic
linking, time zones, networking, TLS, and system Git behavior. Integration tests determine whether
this set is sufficient across supported hosts; Memo does not respond to a compatibility failure by
dynamically exposing additional `/etc` content.

`policy.py` resolves symbolic and machine-specific values at launch: the recording root, linked Git
metadata, shared host cache trees, the active provider's complete existing state, provider
installation closure, system paths, and validated GPU devices, driver libraries, and read-only host
information. Device patterns are expanded only to existing device files; they are not passed
directly to the shell.

The provider installation closure is more than the executable path. Each harness adapter resolves
symlink and launcher chains, identifies the minimal runtime and package subtree needed by that
installation, and mounts those paths read-only. This supports providers installed under locations
such as `~/.local`, npm, or NVM without mounting the entire parent directory. The adapter verifies
the effective provider command inside the sandbox and fails with a bounded-mount diagnostic if it
cannot construct a runnable closure; it never broadens home visibility automatically.

Configuration precedence is:

1. Create `.memo-sandbox` from the shipped `defaults.toml` when the root file is absent.
2. Load the existing `.memo-sandbox` without silently merging newer defaults.
3. Apply safe host discovery, such as available system paths and GPU devices.
4. Apply invocation-specific `--sandbox-args`.

Implementation and security behavior remain hard invariants rather than configurable defaults:

- Use Bubblewrap and verify its required behavior with a lazy first-launch self-test.
- Fail closed if the sandbox backend cannot start.
- Start from an empty synthetic filesystem.
- Mount the recording root read-write.
- Mount `.memo-sandbox` read-only over the writable recording-root mount.
- Detect and mount linked-worktree Git metadata read-write.
- Mount the host's existing `~/.cache`, `~/.triton`, and `~/.nv` trees read-write at their normal
  locations in the synthetic home.
- Never create a configured `read_write_if_present` source on the host; absent paths remain
  ephemeral if software creates them inside the sandbox.
- Create an ephemeral home, `/tmp`, and `/dev/shm`.
- Preserve the user's normal `HOME` path while replacing its filesystem contents.
- Preserve host `PATH` ordering while removing the Memo shim directory, and mount the provider's
  resolved, read-only installation closure without exposing an entire hidden parent directory.
- Mount the active provider's complete existing state read-write and leave the inactive provider's
  state hidden.
- Inject the packaged agent guidance at runtime without creating or modifying project instruction
  files, and preserve existing provider and project instructions.
- Create private mount, user, PID, IPC, and UTS namespaces while retaining the host network
  namespace by default.
- Drop capabilities, mount a private `/proc`, create a minimal private `/dev`, preserve Memo's
  existing PTY and process-group behavior, and terminate the sandbox with its parent. Do not use
  `bwrap --new-session`.
- Never mount the real home directory wholesale.
- Never expose Memo archives, the registry, or the daemon socket to the agent.
- Apply configured environment exclusions unless the root policy explicitly grants a variable.
- Validate discovered GPU devices and expose required driver libraries and read-only driver and
  topology information. User grants are trusted and passed to the sandbox backend.
- Never expose a parent directory because a requested child path is missing.
- Never fall back to an unsandboxed launch.

The agent's Memo access consists only of its recording root, active native provider state, shared
host cache trees, and ephemeral runtime filesystems. The outer shim retains access to the daemon and
handles monitoring.

`defaults.toml` and `agent-guidance.md` are package data included by `pyproject.toml`. `config.py`
loads them with `importlib.resources`, validates the schema, and rejects unknown fields so mistakes
fail closed.

Memo treats `.memo-sandbox` as internal control state: the recording ignore policy excludes it from
filesystem snapshots, replay, and project-content uploads. Each launch still stores the effective
policy summary and digest in daemon metadata and carries them into archived launch metadata. The
daemon registry therefore needs backward-compatible nullable agent-launch fields for sandbox mode,
the user and effective commands, policy summary and digest, and guidance digest. It also needs a
recorded `sandbox-shell` launch kind, or an equivalent dedicated record, with command, cwd, timing,
policy, status, and exit-code fields but no provider or native-session association. It does not need
a grants table because `.memo-sandbox` is the persistent source of truth.

The existing agent shim calls this package:

```text
Memo PATH shim
    ├── report agent_launch
    ├── resolve recording policy
    ├── prepare sandbox mounts and environment
    ├── inject packaged provider guidance
    ├── launch sandbox → provider
    └── report agent_complete
```

> The shim owns policy resolution and process construction. The daemon validates the active
> recording, manages trace capture, and stores the policy summary supplied by the shim; it does not
> construct or modify the launch specification.

The filesystem observer, PTY recorder, and native trace-ingestion model need no architectural
change. The sandbox bind-mounts the existing provider trace paths, so the outer daemon continues to
checkpoint and ingest the global `~/.codex` and `~/.claude` locations as it does today.

## Self-test and fallback

Run the sandbox self-test lazily on the first sandboxed provider or debugging-shell launch, not when
Memo itself starts, so a sandbox problem cannot prevent unrelated recording functionality. Cache a
successful result for the installed Memo version, sandbox-backend version, and kernel combination.

A failure identifies the missing or broken capability, suggests `--no-sandbox` as an explicit user
choice, and never falls back to an unsandboxed launch automatically. An invocation that already
uses `--no-sandbox` does not require the sandbox self-test.

## Documentation

Add a README section for agent sandboxing. It must state that network access shares the host network
namespace and therefore includes localhost services, internal and VPN routes, cloud metadata
endpoints, and services authenticated by network location. Environment credential filtering does
not contain these network-side effects. Document that this is an accepted default boundary and that
users can disable network access for an invocation with:

```bash
codex ... --sandbox-args --unshare-net
```

## Testing

### Trace capture

Verify through integration tests that the existing native trace-ingestion model continues to work
through the sandbox's bind-mounted provider state. Cover:

- New Claude and Codex sessions.
- Exact-ID resume updating the existing Memo agent run rather than creating a duplicate.
- Multiple Claude and Codex agents within one Memo recording.
- Concurrent agents using the same provider and working directory.
- A trace observed while its final JSONL record is incomplete, followed by collection after the
  record is completed.
- Interactive keyboard input and terminal resizing through the sandbox.
- Ctrl-C delivery, process-group signal forwarding, and clean provider exit codes.
- Claude and Codex full-screen and inline terminal modes.
- Sandbox background processes terminating when the wrapper or its parent exits.
- `memo sandbox shell` using the same base mounts, environment, cwd, network, GPU, namespace, and
  lifecycle policy as an agent launch while omitting provider-specific state and behavior.
- Debugging-shell terminal input and output, filesystem changes, launch metadata, policy digest,
  clean exit, signals, and abrupt termination being recorded under the active Memo recording without
  creating a Claude or Codex agent session.
- On GPU-equipped hosts, exposure of applicable NVIDIA device and capability nodes, required driver
  libraries, NVML access, and successful `nvidia-smi` execution through the sandbox.
- Basic PyTorch CUDA discovery and allocation in a GPU-equipped integration environment; PyTorch is
  not a runtime self-test dependency.
- PyTorch multiprocessing and `DataLoader` behavior with the sandbox's private `/dev/shm` and
  process namespace.
- A small NCCL collective when at least two compatible GPUs are available. This test is skipped,
  rather than failed, on single-GPU and CPU-only hosts.

These tests verify the existing design; they do not require an ingestion change unless they expose
an existing association or concurrency defect.

### Sandbox and packaging

Before enabling sandboxing by default, cover:

- An ordinary repository and a pre-created linked worktree.
- `.venv` creation and Python package installation.
- Git status, diff, commit, and branch operations in ordinary and linked worktrees.
- The recording root and each existing configured shared cache being writable with normal host-user
  ownership; absent `read_write_if_present` caches remaining uncreated on the host and reported as
  ephemeral by `memo sandbox show`.
- Read-only grants rejecting writes.
- The user's personal `~/.aws`, sibling projects, and sibling-worktree files remaining inaccessible.
- Host processes being neither visible through the private process namespace nor signalable from
  inside the sandbox.
- Concurrent recordings remaining isolated except for the explicitly documented provider state,
  Git metadata, and shared cache blast radii.
- DNS, TLS, and provider API connectivity through the shared host network.
- Native Claude and Codex authentication, new sessions, and resume.
- A project AWS credential remapped read-only to the conventional sandbox location.
- Environment exclusions and explicit root-policy overrides.
- An activated project `.venv` retaining its `VIRTUAL_ENV` and `.venv/bin` PATH entry.
- CUDA, NCCL, compiler, and thread-tuning variables surviving unless matched by a configured
  exclusion.
- GPU topology discovery through the selected read-only `/sys` and `/proc/driver/nvidia` visibility.
- Host PATH ordering remaining intact while the Memo shim directory is absent inside the sandbox.
- Provider startup from system paths and representative `~/.local`, npm, and NVM installations
  using only the discovered read-only installation closure.
- Identity lookup, dynamic linking, local time, TLS, networking, and Git behavior using the explicit
  read-only `/etc` compatibility allowlist.
- Read-only and read-write extra mounts.
- Symlinks and nested mounts not exposing host paths that were not mounted.
- `--no-sandbox` launches remaining terminal- and trace-monitored.
- Sandbox setup or self-test failure remaining fail-closed.
- Installation and resource loading from both an editable checkout and a built wheel, including
  packaged `defaults.toml` and `agent-guidance.md`.
