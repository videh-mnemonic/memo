from __future__ import annotations

from multiprocessing import Pipe

import pytest

from memo.protocol import (
    DisconnectedError,
    ProtocolError,
    Request,
    Response,
    MAX_FRAME_SIZE,
    receive_request,
    receive_response,
    send_message,
)


def test_request_round_trip() -> None:
    reader, writer = Pipe(duplex=True)
    try:
        send_message(writer, Request("health", {}))
        assert receive_request(reader).operation == "health"
    finally:
        reader.close()
        writer.close()


@pytest.mark.parametrize(
    "value, message",
    [
        ({"protocol_version": 3, "schema_version": 2, "operation": "health", "payload": {}},
         "protocol version"),
        ({"protocol_version": 2, "schema_version": 3, "operation": "health", "payload": {}},
         "schema version"),
        ({"protocol_version": 2, "schema_version": 2, "payload": {}}, "malformed"),
    ],
)
def test_invalid_requests_are_rejected(value: dict[str, object], message: str) -> None:
    reader, writer = Pipe(duplex=True)
    send_message(writer, value)
    with pytest.raises(ProtocolError, match=message):
        receive_request(reader)
    reader.close()
    writer.close()


def test_error_response_requires_message() -> None:
    reader, writer = Pipe(duplex=True)
    send_message(writer, Response(False, {}))
    with pytest.raises(ProtocolError, match="no message"):
        receive_response(reader)
    reader.close()
    writer.close()


def test_disconnect_before_message_is_reported() -> None:
    reader, writer = Pipe(duplex=True)
    writer.close()
    with pytest.raises(DisconnectedError):
        receive_request(reader)
    reader.close()


def test_oversized_message_is_rejected_before_send() -> None:
    reader, writer = Pipe(duplex=True)
    try:
        with pytest.raises(ProtocolError, match="too large"):
            send_message(writer, {"value": "x" * MAX_FRAME_SIZE})
    finally:
        reader.close()
        writer.close()
