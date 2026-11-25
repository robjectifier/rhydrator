"""Watershed colors and markers shared by ingest and the SQLite printer."""

import os
import sys


class Watershed:
    """Choose the palette once for the current stdout, including redirected output."""

    def __init__(self) -> None:
        term = os.environ.get("TERM", "")
        color = (
            sys.stdout.isatty() and term != "dumb" and not os.environ.get("NO_COLOR")
        )
        extended = (
            "256" in term
            or "direct" in term
            or os.environ.get("COLORTERM", "").lower() in {"truecolor", "24bit"}
        )
        light = os.environ.get("COLORFGBG", "").split(";")[-1] in {"7", "15", "231"}

        def sgr(dark: str, on_light: str, basic: str) -> str:
            if not color:
                return ""
            code = basic
            if extended:
                code = on_light if light else dark
            return f"\033[{code}m"

        try:
            "≈✓▁█·◆◇◈──▶…".encode(sys.stdout.encoding or "utf-8")
            unicode = True
        except UnicodeEncodeError:
            unicode = False

        self.reset = "\033[0m" if color else ""
        self.title = sgr("1;38;5;195", "1", "1" if light else "1;97")
        self.mist = sgr("38;5;245", "38;5;240", "90")
        self.step = sgr("1;38;5;75", "1;38;5;26", "1;94") + ("≈ " if unicode else "~ ")
        self.substep = sgr("38;5;110", "38;5;67", "94") + "~ "
        self.success = sgr("1;38;5;78", "1;38;5;28", "1;92") + (
            "✓ " if unicode else "+ "
        )
        self.warning = sgr("1;38;5;209", "1;38;5;166", "1;91") + "! "
        self.levels = "▁▂▃▄▅▆▇█" if unicode else "12345678"
        self.dot = "·" if unicode else "."
        self.ellipsis = "…" if unicode else "..."

        gold = sgr("1;38;5;221", "1;38;5;136", "1;93")
        aqua = sgr("38;5;81", "38;5;31", "96")
        pink = sgr("1;38;5;212", "1;38;5;127", "1;95")
        underline = "\033[4m" if color else ""
        self.arrow = aqua + ("──▶" if unicode else "-->") + self.reset
        # The glyph stays clear. Only the name=value of a foreign key is underlined.
        self.primary = gold + ("◆ " if unicode else "* ")
        self.foreign = aqua + ("◇ " if unicode else "> ") + underline
        self.both = pink + ("◈ " if unicode else "*> ") + underline

    def heading(self, text: str, rule: str = "---") -> str:
        """Frame a report title with quieter rules."""
        return f"{self.mist}{rule}{self.reset} {self.title}{text}{self.reset} {self.mist}{rule}{self.reset}"

    def level(self, current: int, total: int) -> str:
        """Return the filled level for this cluster, or a dot if the total is unknown."""
        if total <= 0:
            return self.dot
        index = max(0, min(7, (8 * current - 1) // total))
        return self.levels[index]

    def key_prefix(self, primary: bool, foreign: bool) -> str:
        """Identify this saved column's role, including members of composite keys."""
        if primary:
            return self.both if foreign else self.primary
        return self.foreign if foreign else ""

    def value(self, prefix: str, text: str) -> str:
        """Mark a key's complete name/value pair and leave ordinary data plain."""
        return f"{prefix}{text}{self.reset}" if prefix else text
