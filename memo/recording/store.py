"""Persist session metadata, manifests, snapshots, and archived agent traces."""

from __future__ import annotations

import json
import os
import shutil
import uuid
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING

from ..agents.run_metadata import AgentRunMetadata
from .filesystem import atomic_write
from .metadata import DirectorySession, StepManifest
from .paths import StoragePaths

if TYPE_CHECKING:
    from .streams import StreamEvent


class SessionNotFoundError(FileNotFoundError):
    pass


def validate_session_id(session_id: str) -> str:
    if (
        not session_id
        or session_id in {".", ".."}
        or Path(session_id).name != session_id
        or "\\" in session_id
    ):
        raise ValueError(f"unsafe session ID: {session_id}")
    return session_id


def _json_bytes(value: dict[str, object]) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


class SessionStore:
    def __init__(self, paths: StoragePaths):
        self.paths = paths
        paths.ensure_storage()

    def session_path(self, session_id: str) -> Path:
        return self.paths.archive / validate_session_id(session_id)

    def list_sessions(self) -> list[tuple[Path, DirectorySession]]:
        result = []
        for session_file in sorted(self.paths.archive.glob("*/session.json")):
            try:
                result.append((session_file.parent, DirectorySession.load(session_file)))
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                continue
        return result

    def find(self, session_id: str) -> tuple[Path, DirectorySession]:
        metadata = self.session_path(session_id) / "session.json"
        if not metadata.is_file():
            raise SessionNotFoundError(f"session not found: {session_id}")
        return metadata.parent, DirectorySession.load(metadata)

    def create(self, session: DirectorySession) -> Path:
        session.validate()
        path = self.session_path(session.session_id)
        path.mkdir(parents=True, exist_ok=False)
        (path / "steps").mkdir()
        (path / "snapshots").mkdir()
        (path / "streams" / "terminals").mkdir(parents=True)
        (path / "agents" / "runs").mkdir(parents=True)
        (path / "agents" / "traces").mkdir(parents=True)
        atomic_write(path / "session.json", _json_bytes(session.to_dict()))
        return path

    def load_session(self, session_id: str) -> DirectorySession:
        return DirectorySession.load(self.session_path(session_id) / "session.json")

    def update_session(self, session: DirectorySession) -> None:
        session.validate()
        atomic_write(
            self.session_path(session.session_id) / "session.json", _json_bytes(session.to_dict())
        )

    def head(self, session_id: str) -> StepManifest | None:
        path = self.session_path(session_id)
        head = path / "HEAD"
        if not head.is_file():
            return None
        value = head.read_text().strip()
        if not value.isdigit():
            raise ValueError("invalid numeric HEAD step")
        manifest = StepManifest.load(path / "steps" / f"{value}.json")
        self._validate_manifest(path, session_id, manifest, streams=True)
        if manifest.step != int(value):
            raise ValueError("HEAD step does not match manifest")
        return manifest

    def step(self, session_id: str, selector: str | int = -1) -> StepManifest:
        if selector == -1 or selector == "-1":
            manifest = self.head(session_id)
            if manifest is None:
                raise ValueError(f"session has no published step: {session_id}")
            return manifest
        try:
            number = int(selector)
        except (TypeError, ValueError) as error:
            raise ValueError(f"invalid step selector: {selector}") from error
        if number < 0:
            raise ValueError(f"invalid step selector: {selector}")
        path = self.session_path(session_id)
        manifest_path = path / "steps" / f"{number}.json"
        if not manifest_path.is_file():
            raise ValueError(f"step not found: {number}")
        manifest = StepManifest.load(manifest_path)
        self._validate_manifest(path, session_id, manifest)
        return manifest

    def steps(self, session_id: str) -> list[StepManifest]:
        path = self.session_path(session_id)
        return self._validate_history(path, session_id)

    @staticmethod
    def _validate_manifest(
        path: Path, session_id: str, manifest: StepManifest, streams: bool = False
    ) -> None:
        if manifest.session_id != session_id:
            raise ValueError("step belongs to another session")
        snapshot = path / manifest.snapshot
        if not snapshot.is_dir():
            raise ValueError(f"step references missing snapshot: {manifest.snapshot}")
        for entry in manifest.entries:
            if (entry.kind == "file" or entry.retained) and not (snapshot / entry.path).is_file():
                raise ValueError(f"step references missing snapshot file: {entry.path}")
        if streams:
            for terminal_id, sequence in manifest.stream_high_water.items():
                metadata_path = path / "streams" / "terminals" / terminal_id / "stream.json"
                if sequence and not metadata_path.is_file():
                    raise ValueError(f"HEAD references missing terminal stream: {terminal_id}")
                if sequence:
                    metadata = json.loads(metadata_path.read_text())
                    if metadata.get("highest_sequence", -1) < sequence:
                        raise ValueError(f"terminal stream does not reach step: {terminal_id}")
                    for chunk in metadata.get("chunks", []):
                        if not (metadata_path.parent / chunk).is_file():
                            raise ValueError(
                                f"terminal stream references missing chunk: {terminal_id}/{chunk}"
                            )
        for run_id in manifest.agent_runs:
            metadata_path = path / "agents" / "runs" / f"{run_id}.json"
            if not metadata_path.is_file():
                raise ValueError(f"step references missing agent run: {run_id}")
            metadata = AgentRunMetadata.load(metadata_path)
            if metadata.run_id != run_id:
                raise ValueError(f"agent run metadata ID does not match: {run_id}")
            if not (path / "agents" / "traces" / metadata.trace_file).is_file():
                raise ValueError(f"agent run references missing trace: {run_id}")

    def restore_manifest(
        self, session_id: str, manifest: StepManifest, destination: Path, force: bool = False
    ) -> Path:
        path = self.session_path(session_id)
        self._validate_manifest(path, session_id, manifest)
        source = self.session_path(session_id) / manifest.snapshot
        if destination.exists():
            occupied = not destination.is_dir() or any(destination.iterdir())
            if occupied and not force:
                raise FileExistsError(f"destination is not empty: {destination}")
            if occupied:
                shutil.rmtree(destination) if destination.is_dir() else destination.unlink()
        destination.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source, destination, dirs_exist_ok=True, copy_function=shutil.copy2)
        return destination

    def stream_events_for_manifest(
        self, session_id: str, manifest: StepManifest, terminal_ids: Iterable[str] | None = None
    ) -> list[StreamEvent]:
        from .streams import merged_timeline

        path = self.session_path(session_id)
        self._validate_manifest(path, session_id, manifest, streams=True)
        terminals = manifest.stream_high_water
        selected = sorted(terminals) if terminal_ids is None else list(terminal_ids)
        unknown = [terminal_id for terminal_id in selected if terminal_id not in terminals]
        if unknown:
            raise KeyError(f"terminal stream not found: {', '.join(unknown)}")
        selected = sorted(set(selected))
        chunks = []
        root = path / "streams" / "terminals"
        for stream_id in selected:
            if terminals[stream_id] == 0:
                continue
            metadata_path = root / stream_id / "stream.json"
            metadata = json.loads(metadata_path.read_text())
            chunks.extend(
                metadata_path.parent / relative for relative in metadata.get("chunks", [])
            )
        return [
            event
            for event in merged_timeline(chunks)
            if event.sequence <= terminals.get(event.terminal_id, -1)
        ]

    def check_integrity(self, session_id: str) -> StepManifest | None:
        path = self.session_path(session_id)
        manifests = self._validate_history(path, session_id)
        return manifests[-1] if manifests else None

    def remove_archived(self, session_id: str) -> None:
        """Remove a complete session whose latest local step is recorded as pushed."""
        path = self.session_path(session_id)
        session = self.load_session(session_id)
        head = self.check_integrity(session_id)
        if session.state != "complete":
            raise ValueError("recording is not complete")
        if head is None:
            raise ValueError("recording has no published step")
        if (
            not isinstance(session.last_pushed_step, int)
            or isinstance(session.last_pushed_step, bool)
            or session.last_pushed_step != head.step
        ):
            raise ValueError("latest local step is not archived")
        digest = session.last_pushed_digest
        remote_object = session.remote_object
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(value not in "0123456789abcdef" for value in digest.lower())
            or not isinstance(remote_object, str)
            or not remote_object
        ):
            raise ValueError("remote archive metadata is incomplete")

        removing = self.paths.archive / ".removing"
        removing.mkdir(mode=0o700, exist_ok=True)
        destination = removing / f"{session_id}-{uuid.uuid4().hex}"
        path.replace(destination)
        shutil.rmtree(destination)

    @classmethod
    def _validate_history(cls, path: Path, session_id: str) -> list[StepManifest]:
        session = DirectorySession.load(path / "session.json")
        if session.session_id != session_id:
            raise ValueError("session metadata does not match archive location")
        head_path = path / "HEAD"
        if not head_path.is_file():
            if any((path / "steps").glob("*.json")):
                raise ValueError("session has steps but no HEAD")
            return []
        head_value = head_path.read_text().strip()
        if not head_value.isdigit():
            raise ValueError("invalid numeric HEAD step")
        head_step = int(head_value)
        step_files = sorted(
            (path / "steps").glob("*.json"),
            key=lambda item: int(item.stem) if item.stem.isdigit() else -1,
        )
        expected_names = {f"{step}.json" for step in range(head_step + 1)}
        actual_names = {item.name for item in step_files}
        if not expected_names.issubset(actual_names):
            raise ValueError("published step history is not contiguous through HEAD")
        manifests = []
        for step in range(head_step + 1):
            manifest = StepManifest.load(path / "steps" / f"{step}.json")
            if manifest.step != step:
                raise ValueError("step filename does not match manifest")
            cls._validate_manifest(path, session_id, manifest, streams=True)
            manifests.append(manifest)
        return manifests

    def next_step(self, session_id: str) -> int:
        current = self.head(session_id)
        return 0 if current is None else current.step + 1

    def publish(
        self, session: DirectorySession, manifest: StepManifest, prepared_snapshot: Path
    ) -> StepManifest:
        manifest.validate()
        if manifest.session_id != session.session_id:
            raise ValueError("step belongs to a different session")
        path = self.session_path(session.session_id)
        expected = self.next_step(session.session_id)
        if manifest.step != expected:
            raise ValueError(f"step {manifest.step} is not next step {expected}")
        snapshot = path / manifest.snapshot
        manifest_path = path / "steps" / f"{manifest.step}.json"
        if snapshot.exists() and not manifest_path.exists():
            shutil.rmtree(snapshot) if snapshot.is_dir() else snapshot.unlink()
        elif snapshot.exists() and manifest_path.exists():
            existing = StepManifest.load(manifest_path)
            self._validate_manifest(path, session.session_id, existing, streams=True)
            atomic_write(path / "HEAD", f"{existing.step}\n".encode())
            return existing
        elif manifest_path.exists():
            raise FileExistsError(f"step already exists: {manifest.step}")
        prepared_snapshot.replace(snapshot)
        self._fsync_tree(snapshot)
        atomic_write(manifest_path, _json_bytes(manifest.to_dict()))
        atomic_write(path / "HEAD", f"{manifest.step}\n".encode())
        return manifest

    @staticmethod
    def _fsync_tree(root: Path) -> None:
        for path in sorted(root.rglob("*"), reverse=True):
            if path.is_file():
                with path.open("rb") as handle:
                    os.fsync(handle.fileno())
            elif path.is_dir():
                descriptor = os.open(path, os.O_RDONLY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
        descriptor = os.open(root, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
