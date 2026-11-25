"""Summarize a saved SQLite database, or print every row and link with --full."""

# ruff: noqa: T201

import argparse
import re
import sqlite3
from collections import defaultdict
from functools import partial
from pathlib import Path
from reprlib import Repr
from typing import TYPE_CHECKING

from sqlalchemy import create_engine, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import aliased
from sqlalchemy.pool import NullPool

from rhydrator._terminal import Watershed
from rhydrator.model import Base

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection
    from sqlalchemy.orm import Mapper


_PREVIEW = {
    mark: Repr(maxstring=64, maxother=64, fillvalue=mark) for mark in ("…", "...")
}


def _preview_value(value: object, ellipsis: str) -> str:
    """Keep short values readable and show the size of long binary payloads."""
    if value is None:
        return "NULL"
    if isinstance(value, bytes) and len(value) > 64:
        return f"<{len(value):,} bytes>"
    return _PREVIEW[ellipsis].repr(value)


def _table_groups(remaining: set[str]) -> dict[str, list[str]]:
    """Return the saved tables in model.py's group and declaration order.

    Args:
        remaining: Set of saved table names to distribute among the groups.

    Returns:
        Group headings and their tables. Extra saved tables appear last.
    """
    groups: dict[str, list[str]] = {}
    group = "Other tables"
    model_path = Path(__file__).resolve().parents[1] / "src" / "rhydrator" / "model.py"
    for line in model_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("# ---- ") and line.endswith(" ----"):
            group = line.removeprefix("# ---- ").removesuffix(" ----")
        elif match := re.match(r"""    __tablename__\s*=\s*["']([^"']+)["']""", line):
            name = match[1]
            if name in remaining:
                groups.setdefault(group, []).append(name)
                remaining.remove(name)
    if remaining:
        groups.setdefault("Other tables", []).extend(sorted(remaining))
    return groups


def _linked_rows(
    db: Connection,
    mapper: Mapper,
    saved_columns: dict[str, dict[str, str]],
    term: Watershed,
    *,
    example_key: tuple | None = None,
):
    """Look up target primary keys for each relationship in a single join.

    Args:
        db: Read-only connection used for the rest of the dump.
        mapper: SQLAlchemy mapping for the table being printed.
        saved_columns: Key prefixes for the columns actually present in each saved table.
        term: Palette used for the linked rows' keys.
        example_key: For a summary, fetch at most two targets for this row.
            The second target tells us whether to display an ellipsis.

    Yields:
        Each relationship and its target labels keyed by source primary key,
        or None if the saved schema lacks a column needed for the join.
    """
    format_key = (
        repr if example_key is None else partial(_preview_value, ellipsis=term.ellipsis)
    )
    for rel in mapper.relationships:
        target = rel.mapper
        required = (
            *mapper.primary_key,
            *target.primary_key,
            *rel.local_columns,
            *rel.remote_side,
        )
        if any(
            col.name not in saved_columns.get(col.table.name, ()) for col in required
        ):
            yield rel, None
            continue

        # An alias keeps both sides distinct for field.parent and field.children.
        target_alias = aliased(target.class_)
        target_keys = [getattr(target_alias, col.key) for col in target.primary_key]
        query = (
            select(*mapper.primary_key, *target_keys)
            .select_from(mapper.class_)
            .join(target_alias, getattr(mapper.class_, rel.key))
            .order_by(*target_keys)
        )
        if example_key is not None:
            query = query.where(
                *(
                    col == value
                    for col, value in zip(mapper.primary_key, example_key, strict=True)
                )
            ).limit(2)
        links = defaultdict(list)
        key_size = len(mapper.primary_key)
        for row in db.execute(query):
            keys = ", ".join(
                term.value(
                    saved_columns[target.local_table.name][col.name],
                    f"{col.name}={format_key(value)}",
                )
                for col, value in zip(target.primary_key, row[key_size:], strict=True)
            )
            links[row[:key_size]].append(f"{target.local_table.name}({keys})")
        yield rel, links


def _relationship_labels(relationships, key: tuple, term: Watershed, *, full: bool):
    """Format the links for one row, including empty or unavailable relationships."""
    for rel, links in relationships:
        if links is None:
            text = "(unavailable in saved schema)"
        else:
            targets = links.get(key, [])
            if not full and len(targets) > 1:
                text = f"[{targets[0]}, {term.ellipsis}]"
            elif rel.uselist or len(targets) > 1:
                text = f"[{', '.join(targets)}]"
            else:
                text = targets[0] if targets else "NULL"
        yield f"{rel.key} {term.arrow} {text}"


def _print_table_summary(
    db: Connection,
    table_name: str,
    mapper: Mapper | None,
    saved_columns: dict[str, dict[str, str]],
    term: Watershed,
) -> None:
    """Print a row count and one compact example with its relationship previews."""
    quoted_name = db.dialect.identifier_preparer.quote_identifier(table_name)
    count = db.exec_driver_sql(f"SELECT COUNT(*) FROM {quoted_name}").scalar_one()
    print(
        f"\t{term.title}{table_name}{term.reset}  {count:,} {'row' if count == 1 else 'rows'}"
    )
    if not count:
        return

    row = db.exec_driver_sql(f"SELECT * FROM {quoted_name} LIMIT 1").mappings().one()
    values = "  ".join(
        term.value(
            saved_columns[table_name][col],
            f"{col}={_preview_value(value, term.ellipsis)}",
        )
        for col, value in row.items()
    )
    print(f"\t\t{values}")
    if mapper is not None:
        key = tuple(row.get(col.name) for col in mapper.primary_key)
        links = _linked_rows(db, mapper, saved_columns, term, example_key=key)
        labels = "  ".join(_relationship_labels(links, key, term, full=False))
        if labels:
            print(f"\t\t{labels}")


def print_db(db_path: Path | str, *, full: bool = False) -> None:
    """Print table counts and example rows, or the full contents of the database.

    Args:
        db_path: Path to an existing SQLite database, opened read-only.
        full: Print all rows, values and relationship targets.
    """
    uri = Path(db_path).resolve().as_uri() + "?mode=ro"
    engine = create_engine(
        "sqlite://", creator=lambda: sqlite3.connect(uri, uri=True), poolclass=NullPool
    )
    mappers = {mapper.local_table.name: mapper for mapper in Base.registry.mappers}
    with engine.connect() as db:
        db.exec_driver_sql("BEGIN")  # Read every table from the same snapshot.
        term = Watershed()
        saved_columns: dict[str, dict[str, str]] = defaultdict(dict)
        for table, column, primary, foreign in db.exec_driver_sql(
            'SELECT t.name, c.name, c.pk, fk."from" IS NOT NULL FROM sqlite_schema AS t '
            "JOIN pragma_table_xinfo(t.name) AS c "
            'LEFT JOIN pragma_foreign_key_list(t.name) AS fk ON fk."from" = c.name COLLATE NOCASE '
            "WHERE t.type = 'table'"
        ):
            saved_columns[table][column] = term.key_prefix(primary, foreign)
        title = "Contents" if full else "Summary"
        print(term.heading(f"{title} of {db_path}"))
        print(
            f"{term.primary}primary key{term.reset}  {term.foreign}foreign key{term.reset}  {term.both}both{term.reset}"
        )
        if not full:
            print(
                f"One example row per table ({term.ellipsis} means more). Use print_db.py --full for all rows and links."
            )
        for group, tables in _table_groups(set(saved_columns)).items():
            print(term.heading(group, "----"))
            for table_name in tables:
                mapper = mappers.get(table_name)
                if not full:
                    _print_table_summary(db, table_name, mapper, saved_columns, term)
                    continue

                print(f"\t{term.title}{table_name}{term.reset}")
                quoted_name = db.dialect.identifier_preparer.quote_identifier(
                    table_name
                )
                rows = db.exec_driver_sql(f"SELECT * FROM {quoted_name}").mappings()
                columns = list(rows.keys())
                row_number = 0
                for row_number, row in enumerate(rows, start=1):
                    print(f"\t\t{term.title}Row {row_number}{term.reset}")
                    for column, value in row.items():
                        text = "NULL" if value is None else repr(value)
                        cell = term.value(
                            saved_columns[table_name][column], f"{column} = {text}"
                        )
                        print(f"\t\t\t{cell}")
                    if mapper is None:
                        continue
                    if row_number == 1:
                        relationships = list(
                            _linked_rows(db, mapper, saved_columns, term)
                        )
                    key = tuple(row.get(col.name) for col in mapper.primary_key)
                    print(f"\t\t\t{term.title}Relationships{term.reset}")
                    for label in _relationship_labels(
                        relationships, key, term, full=True
                    ):
                        print(f"\t\t\t\t{label}")
                if row_number == 0:
                    print(f"\t\t(empty)  {', '.join(columns)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("db_path", type=Path, help="Path to the saved SQLite database.")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Print every row, value and relationship target.",
    )
    args = parser.parse_args()
    try:
        print_db(args.db_path, full=args.full)
    except SQLAlchemyError as error:
        parser.exit(1, f"Could not read {args.db_path}: {error}\n")
