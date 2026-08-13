from __future__ import annotations

import json
import socket
import struct
from dataclasses import asdict, dataclass
from typing import Any


PROTOCOL_VERSION = 1
SCHEMA_VERSION = 1
MAX_FRAME_SIZE = 8 * 1024 * 1024


class ProtocolError(RuntimeError):
    pass


class DisconnectedError(ProtocolError):
    pass


@dataclass(frozen=True)
class Request:
    operation: str
    payload: dict[str, Any]
    protocol_version: int = PROTOCOL_VERSION
    schema_version: int = SCHEMA_VERSION

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "Request":
        try:
            result = cls(
                operation=value["operation"],
                payload=value.get("payload", {}),
                protocol_version=value["protocol_version"],
                schema_version=value["schema_version"],
            )
        except (KeyError, TypeError) as error:
            raise ProtocolError("malformed request") from error
        if result.protocol_version != PROTOCOL_VERSION:
            raise ProtocolError(f"unsupported protocol version: {result.protocol_version}")
        if result.schema_version != SCHEMA_VERSION:
            raise ProtocolError(f"unsupported schema version: {result.schema_version}")
        if not isinstance(result.operation, str) or not isinstance(result.payload, dict):
            raise ProtocolError("malformed request")
        return result


@dataclass(frozen=True)
class Response:
    ok: bool
    payload: dict[str, Any]
    error: str | None = None
    protocol_version: int = PROTOCOL_VERSION
    schema_version: int = SCHEMA_VERSION

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "Response":
        try:
            result = cls(**value)
        except TypeError as error:
            raise ProtocolError("malformed response") from error
        if result.protocol_version != PROTOCOL_VERSION or result.schema_version != SCHEMA_VERSION:
            raise ProtocolError("response version mismatch")
        if not isinstance(result.ok, bool) or not isinstance(result.payload, dict):
            raise ProtocolError("malformed response")
        if not result.ok and not result.error:
            raise ProtocolError("error response has no message")
        return result


def encode_frame(value: Request | Response | dict[str, Any]) -> bytes:
    body = json.dumps(asdict(value) if not isinstance(value, dict) else value,
                      separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(body) > MAX_FRAME_SIZE:
        raise ProtocolError("frame is too large")
    return struct.pack("!I", len(body)) + body


def _read_exact(connection: socket.socket, size: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        chunk = connection.recv(size - len(chunks))
        if not chunk:
            raise DisconnectedError("client disconnected during frame")
        chunks.extend(chunk)
    return bytes(chunks)


def receive_dict(connection: socket.socket) -> dict[str, Any]:
    size = struct.unpack("!I", _read_exact(connection, 4))[0]
    if size == 0 or size > MAX_FRAME_SIZE:
        raise ProtocolError(f"invalid frame size: {size}")
    try:
        value = json.loads(_read_exact(connection, size))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProtocolError("invalid JSON frame") from error
    if not isinstance(value, dict):
        raise ProtocolError("frame must contain a JSON object")
    return value


def receive_request(connection: socket.socket) -> Request:
    return Request.from_dict(receive_dict(connection))


def receive_response(connection: socket.socket) -> Response:
    return Response.from_dict(receive_dict(connection))


def send_message(connection: socket.socket, value: Request | Response | dict[str, Any]) -> None:
    connection.sendall(encode_frame(value))


def request(socket_path: str, operation: str, payload: dict[str, Any] | None = None,
            timeout: float = 10.0) -> dict[str, Any]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(timeout)
        connection.connect(socket_path)
        send_message(connection, Request(operation, payload or {}))
        response = receive_response(connection)
    if not response.ok:
        raise ProtocolError(response.error or "daemon request failed")
    return response.payload
