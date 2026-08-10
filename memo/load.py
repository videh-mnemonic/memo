from __future__ import annotations

import json
import shutil
import subprocess
import tarfile
from pathlib import Path

from .config import Paths
from .models import SessionMeta
from .normalize import all_traces
from .store import find_session


def _safe_members(archive: tarfile.TarFile, target: Path) -> list[tarfile.TarInfo]:
    members = []
    root = target.resolve()
    for member in archive.getmembers():
        name = Path(member.name)
        if name.is_absolute() or ".." in name.parts:
            raise RuntimeError(f"unsafe archive path: {member.name}")
        if member.issym() or member.islnk() or member.isdev():
            raise RuntimeError(f"unsafe archive entry type: {member.name}")
        destination = (root / name).resolve()
        try:
            destination.relative_to(root)
        except ValueError as error:
            raise RuntimeError(f"archive path escapes destination: {member.name}") from error
        members.append(member)
    return members


def safe_extract_tar(path: Path, target: Path) -> None:
    with tarfile.open(path, "r:*") as archive:
        members = _safe_members(archive, target)
        archive.extractall(target, members=members, filter="data")


def unpack(session_id: str, paths: Paths | None = None) -> Path:
    paths = paths or Paths.discover()
    location = find_session(session_id, paths)
    target = paths.unpack / session_id
    marker = target / ".unpacked-ok"
    source = f"{location.kind}:{location.path}"
    if location.kind == "archive" and marker.is_file() and marker.read_text().strip() == source:
        return target
    if target.exists():
        shutil.rmtree(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.tmp")
    if temporary.exists():
        shutil.rmtree(temporary)
    temporary.mkdir()
    try:
        if location.kind == "scratch":
            shutil.copytree(location.path, temporary, dirs_exist_ok=True)
        else:
            safe_extract_tar(location.path, temporary)
        (temporary / ".unpacked-ok").write_text(source + "\n")
        temporary.replace(target)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return target


def _prepare_destination(destination: Path, force: bool) -> None:
    if destination.exists() and any(destination.iterdir()):
        if not force:
            raise FileExistsError(f"destination is not empty: {destination}")
        shutil.rmtree(destination)


def _run(args: list[str], cwd: Path | None = None) -> None:
    result = subprocess.run(args, cwd=cwd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode:
        raise RuntimeError(f"command failed ({' '.join(args)}): {result.stderr.strip()}")


def _apply_mailbox(path: Path, destination: Path) -> None:
    if path.is_file() and path.stat().st_size:
        _run(["git", "am", "--committer-date-is-author-date", str(path)], destination)


def _apply_session_mailboxes(source: Path, destination: Path) -> None:
    cumulative = source / "git" / "session-commits.patch"
    if cumulative.is_file():
        _apply_mailbox(cumulative, destination)
        return
    for patch in sorted((source / "legs").glob("*/commits.patch")):
        _apply_mailbox(patch, destination)


def _apply_diff(path: Path, destination: Path) -> None:
    if path.is_file() and path.stat().st_size:
        _run(["git", "apply", "--binary", str(path)], destination)


def _extract_untracked(path: Path, destination: Path) -> None:
    if path.is_file():
        safe_extract_tar(path, destination)


def reconstruct(session_id: str, at: str, destination: Path, force: bool = False,
                paths: Paths | None = None) -> Path:
    source = unpack(session_id, paths)
    _prepare_destination(destination, force)
    _run(["git", "clone", str(source / "git" / "initial.bundle"), str(destination)])
    if at == "initial":
        _apply_diff(source / "git" / "initial-uncommitted.patch", destination)
        _extract_untracked(source / "git" / "initial-untracked.tar.gz", destination)
    elif at == "final":
        _apply_session_mailboxes(source, destination)
        _apply_diff(source / "git" / "final-uncommitted.patch", destination)
        _extract_untracked(source / "git" / "final-untracked.tar.gz", destination)
    elif at.startswith("leg:"):
        try:
            number = int(at.partition(":")[2])
        except ValueError as error:
            raise ValueError(f"invalid leg selector: {at}") from error
        legs = sorted((source / "legs").glob("[0-9][0-9][0-9]"))
        if number < 1 or number > len(legs):
            raise ValueError(f"leg out of range: {number}")
        for directory in legs[:number]:
            _apply_mailbox(directory / "commits.patch", destination)
    else:
        raise ValueError(f"invalid reconstruction point: {at}")
    return destination


def write_traces(session_id: str, destination: Path, raw: bool = False,
                 paths: Paths | None = None) -> Path:
    source = unpack(session_id, paths)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(all_traces(source, raw), indent=2, ensure_ascii=False) + "\n")
    return destination


def _prompt_text(content: object) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict) and isinstance(item.get("text"), str):
                parts.append(item["text"])
        return "\n".join(parts)
    if isinstance(content, dict):
        for key in ("text", "content", "prompt"):
            if isinstance(content.get(key), str):
                return content[key]
    return json.dumps(content, ensure_ascii=False)


def replay(session_id: str, at: str, destination: Path, force: bool = False,
           paths: Paths | None = None) -> Path:
    source = unpack(session_id, paths)
    reconstruct(session_id, at, destination, force, paths)
    meta = SessionMeta.load(source / "meta.json")
    through = int(at.partition(":")[2]) if at.startswith("leg:") else None
    records = [] if at == "initial" else all_traces(source, through_leg=through)
    prompts = [_prompt_text(r["content"]) for r in records if r["type"] == "user_input"]
    lines = [
        "# Memo task", "", f"- Session ID: {meta.session_id}", f"- Repository: {meta.repo_name}",
        f"- Original root: {meta.repo_root}",
        f"- Canonical remote: {meta.canonical_remote or '(none)'}", f"- Branch: {meta.branch or '(unknown)'}",
        f"- Tool: {meta.tool}", f"- Time: {meta.first_seen_utc} — {meta.last_activity_utc}", "", "## User prompts", "",
    ]
    if prompts:
        for index, prompt in enumerate(prompts, 1):
            lines.extend([f"### Prompt {index}", "", prompt, ""])
    else:
        lines.append("(none)\n")
    (destination / "MEMO_TASK.md").write_text("\n".join(lines))
    return destination
