from __future__ import annotations

import socket
import threading

import pytest

from memo.protocol import (
    DisconnectedError,
    ProtocolError,
    Request,
    Response,
    encode_frame,
    receive_request,
    receive_response,
)


def test_partial_frame_is_reassembled() -> None:
    reader, writer = socket.socketpair()
    frame = encode_frame(Request("health", {}))

    def send() -> None:
        for byte in frame:
            writer.sendall(bytes([byte]))
        writer.close()

    thread = threading.Thread(target=send)
    thread.start()
    try:
        assert receive_request(reader).operation == "health"
    finally:
        reader.close()
        thread.join()


@pytest.mark.parametrize(
    "value, message",
    [
        ({"protocol_version": 2, "schema_version": 1, "operation": "health", "payload": {}},
         "protocol version"),
        ({"protocol_version": 1, "schema_version": 2, "operation": "health", "payload": {}},
         "schema version"),
        ({"protocol_version": 1, "schema_version": 1, "payload": {}}, "malformed"),
    ],
)
def test_invalid_requests_are_rejected(value: dict[str, object], message: str) -> None:
    reader, writer = socket.socketpair()
    writer.sendall(encode_frame(value))
    with pytest.raises(ProtocolError, match=message):
        receive_request(reader)
    reader.close()
    writer.close()


def test_error_response_requires_message() -> None:
    reader, writer = socket.socketpair()
    writer.sendall(encode_frame(Response(False, {})))
    with pytest.raises(ProtocolError, match="no message"):
        receive_response(reader)
    reader.close()
    writer.close()


def test_disconnect_mid_frame_is_reported() -> None:
    reader, writer = socket.socketpair()
    writer.sendall(b"\x00\x00\x00\x10{}")
    writer.close()
    with pytest.raises(DisconnectedError):
        receive_request(reader)
    reader.close()
