"""Watershed key roles and progress markers on a terminal."""

import sys
from typing import TYPE_CHECKING

from rhydrator._terminal import Watershed

if TYPE_CHECKING:
    import pytest


def test_watershed_terminal(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
    monkeypatch.setenv("TERM", "xterm-256color")
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.delenv("COLORFGBG", raising=False)
    term = Watershed()
    assert term.step == "\033[1;38;5;75m≈ "
    assert term.substep == "\033[38;5;110m~ "
    assert term.primary == "\033[1;38;5;221m◆ "
    assert term.foreign == "\033[38;5;81m◇ \033[4m"
    assert term.both == "\033[1;38;5;212m◈ \033[4m"
    assert term.level(1, 15) == "▁"
    assert term.level(2, 15) == "▂"
    assert term.level(15, 15) == "█"
    assert term.level(0, 0) == "·"

    monkeypatch.setenv("NO_COLOR", "1")
    plain = Watershed()
    assert plain.value(plain.both, "field_id=66") == "◈ field_id=66"
    assert plain.arrow == "──▶"
