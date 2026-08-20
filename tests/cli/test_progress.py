from __future__ import annotations

import io

from memo.cli.progress import ProgressBar


class TtyBuffer(io.StringIO):
    def isatty(self) -> bool:
        return True


def test_progress_bar_renders_percentage_and_message() -> None:
    stream = TtyBuffer()

    with ProgressBar(stream=stream, width=10) as progress:
        progress.update(1, 4, "downloading session")

    output = stream.getvalue()
    assert " 25% downloading session" in output
    assert output.endswith("\n")


def test_progress_bar_is_silent_for_non_tty_stream() -> None:
    stream = io.StringIO()

    with ProgressBar(stream=stream) as progress:
        progress.update(1, 1, "complete")

    assert stream.getvalue() == ""


def test_progress_bar_can_render_eta() -> None:
    stream = TtyBuffer()
    values = iter([10.0, 20.0, 30.0])

    with ProgressBar(
        stream=stream,
        width=10,
        show_eta=True,
        clock=lambda: next(values),
    ) as progress:
        progress.update(0, 100, "starting")
        progress.update(25, 100, "working")
        progress.update(100, 100, "done")

    output = stream.getvalue()
    assert "ETA -- starting" in output
    assert "ETA 30s working" in output
    assert "ETA 0s done" in output
