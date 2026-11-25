"""Draw the SQLAlchemy model as SVG and PNG for the README."""

import json
import subprocess
from html import escape
from pathlib import Path
from typing import TYPE_CHECKING

from print_db import _table_groups
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects import sqlite

from rhydrator.model import Base

if TYPE_CHECKING:
    from sqlalchemy import Table


# Group backgrounds and table headers follow the original README diagram.
GROUP_COLORS = {
    "Dataset": ("#FBECE8", "#AD4539"),
    "ObjectStore": ("#F8EAF2", "#9A326F"),
    "RNTuple": ("#EAF5EF", "#19734D"),
}
HEADER_COLORS = {
    "rntuple": "#A34F0C",
    "rntuple_instance": "#856011",
    "cluster_group": "#856011",
    "cluster": "#856011",
    "field": "#216692",
    "column": "#216692",
    "alias_column": "#216692",
    "column_representation": "#364E64",
}


def table_label(table: Table, color: str) -> str:
    """Draw one row per SQLite column, with ports for its foreign-key lines."""
    rows = [
        f'<TR><TD COLSPAN="3" ALIGN="LEFT" BGCOLOR="{color}" CELLPADDING="7">'
        f'<FONT COLOR="white"><B>{escape(table.name)}</B></FONT></TD></TR>'
    ]
    for column in table.columns:
        name = escape(column.name)
        sql_type = escape(column.type.compile(dialect=sqlite.dialect()))
        flags = []
        if column.primary_key:
            flags.append("PK")
        if column.foreign_keys:
            flags.append("FK")
        if not column.nullable and not column.primary_key:
            flags.append("NN")
        rows.append(
            f'<TR><TD PORT="left_{name}" ALIGN="LEFT">{name}</TD>'
            f'<TD ALIGN="LEFT"><FONT COLOR="#455663" POINT-SIZE="10">{sql_type}</FONT></TD>'
            f'<TD PORT="right_{name}" ALIGN="RIGHT"><FONT COLOR="{color}" '
            f'POINT-SIZE="10">{" ".join(flags) or "&#160;"}</FONT></TD></TR>'
        )
    unique_keys = sorted(
        tuple(col.name for col in constraint.columns)
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    )
    for key in unique_keys:
        columns = escape(", ".join(key))
        rows.append(
            '<TR><TD COLSPAN="3" ALIGN="LEFT" BGCOLOR="#F0F4F5">'
            f'<FONT POINT-SIZE="10">UNIQUE ({columns})</FONT></TD></TR>'
        )
    return (
        '<TABLE BORDER="1" COLOR="#B6C3C5" CELLBORDER="0" CELLSPACING="0" '
        'CELLPADDING="4" BGCOLOR="white">' + "".join(rows) + "</TABLE>"
    )


def model_dot() -> str:
    """Return a DOT graph of the model with arrows pointing to referenced columns."""
    lines = [
        "digraph rhydrator {",
        'graph [rankdir=LR, newrank=true, bgcolor="white", pad=0.18, '
        'nodesep=0.30, ranksep=0.53, fontname="Helvetica", outputorder=edgesfirst];',
        'node [shape=plain, fontname="Helvetica", fontsize=12, fontcolor="#1D3038"];',
        "edge [dir=back, arrowtail=vee, arrowsize=0.65, penwidth=1.2];",
    ]
    tables = Base.metadata.tables
    header_colors = {}
    for heading, names in _table_groups(set(tables)).items():
        group = heading.removesuffix(" group")
        background, accent = GROUP_COLORS.get(group, ("#F2F4F4", "#475B66"))
        lines.extend(
            [
                f"subgraph {json.dumps('cluster_' + group)} {{",
                f"graph [label=<{escape(group)}>, labelloc=t, labeljust=l, "
                f'fontname="Helvetica-Bold", fontsize=16, fontcolor="{accent}", '
                f'style="rounded,filled", color="{background}", margin=15];',
            ]
        )
        if group == "ObjectStore":
            lines.append("rank=same;")
        for name in names:
            color = HEADER_COLORS.get(name, accent)
            header_colors[name] = color
            label = table_label(tables[name], color)
            lines.append(f"{json.dumps(name)} [label=<{label}>];")
        lines.append("}")
    for table in tables.values():
        for fk in sorted(
            table.foreign_keys, key=lambda key: (key.parent.name, key.target_fullname)
        ):
            parent = fk.column.table.name
            # Route a field's link to its parent along the outside of the field table.
            if parent == table.name:
                pk_port = f"{json.dumps('left_' + fk.column.name)}:w"
            else:
                pk_port = f"{json.dumps('right_' + fk.column.name)}:e"
            fk_port = f"{json.dumps('left_' + fk.parent.name)}:w"
            tooltip = json.dumps(
                f"{table.name}.{fk.parent.name} references {fk.target_fullname}"
            )
            lines.append(
                f"{json.dumps(parent)}:{pk_port} -> {json.dumps(table.name)}:{fk_port} "
                f'[color="{header_colors[parent]}", tooltip={tooltip}];'
            )
    lines.append("}")
    return "\n".join(lines)


if __name__ == "__main__":
    source = model_dot().encode()
    images = {
        extension: subprocess.check_output(["dot", f"-T{extension}"], input=source)
        for extension in ("svg", "png")
    }
    docs = Path(__file__).resolve().parents[1] / "docs"
    for extension, image in images.items():
        (docs / f"robjectifier.{extension}").write_bytes(image)
