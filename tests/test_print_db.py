"""The database printer summarizes tables by default and can dump every value."""

import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest

PRINT_DB = Path(__file__).parents[1] / "scripts" / "print_db.py"


def test_print_db_full(tmp_path: Path):
    path = tmp_path / "example.db"
    with closing(sqlite3.connect(path)) as db:
        db.execute("CREATE TABLE empty_table (id INTEGER)")
        db.execute('CREATE TABLE "a""b" (label TEXT, payload BLOB)')
        db.executemany(
            'INSERT INTO "a""b" VALUES (?, ?)', [("first", b"\x00"), ("second", None)]
        )
        db.commit()

    result = subprocess.run(
        [sys.executable, PRINT_DB, path, "--full"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "\033" not in result.stdout
    lines = result.stdout.splitlines()
    assert lines == [
        f"--- Contents of {path} ---",
        "◆ primary key  ◇ foreign key  ◈ both",
        "---- Other tables ----",
        '\ta"b',
        "\t\tRow 1",
        "\t\t\tlabel = 'first'",
        "\t\t\tpayload = b'\\x00'",
        "\t\tRow 2",
        "\t\t\tlabel = 'second'",
        "\t\t\tpayload = NULL",
        "\tempty_table",
        "\t\t(empty)  id",
    ]


def test_model_groups_and_table_order(tmp_path: Path):
    path = tmp_path / "partial.db"
    with closing(sqlite3.connect(path)) as db:
        for name in (
            "page",
            "input_file",
            "legacy",
            "page_bundle",
            "rntuple",
            "dataset",
        ):
            db.execute(f'CREATE TABLE "{name}" (id INTEGER)')

    result = subprocess.run(
        [sys.executable, PRINT_DB, path], capture_output=True, text=True, check=True
    )
    headings = [
        line for line in result.stdout.splitlines() if line.startswith(("---- ", "\t"))
    ]
    assert headings == [
        "---- Dataset group ----",
        "\tdataset  0 rows",
        "\tinput_file  0 rows",
        "---- ObjectStore group ----",
        "\tpage_bundle  0 rows",
        "---- RNTuple group ----",
        "\trntuple  0 rows",
        "\tpage  0 rows",
        "---- Other tables ----",
        "\tlegacy  0 rows",
    ]


def test_relationships_point_to_saved_rows(tmp_path: Path):
    path = tmp_path / "relationships.db"
    with closing(sqlite3.connect(path)) as db:
        db.executescript("""
            CREATE TABLE field (id INTEGER PRIMARY KEY, parent_id INTEGER);
            CREATE TABLE alias_column (field_id INTEGER REFERENCES field(id), column_id INTEGER,
                                       PRIMARY KEY (field_id, column_id));
            INSERT INTO field VALUES (7, NULL), (12, 7), (15, 7);
            INSERT INTO alias_column VALUES (12, 4), (12, 2);
        """)

    summary = subprocess.check_output([sys.executable, PRINT_DB, path], text=True)
    assert "field  3 rows" in summary
    assert "children ──▶ [field(◆ id=12), …]" in summary
    assert "parent ──▶ NULL" in summary
    assert "field(◆ id=15)" not in summary
    assert "column ──▶ (unavailable in saved schema)" in summary

    result = subprocess.run(
        [sys.executable, PRINT_DB, path, "--full"],
        capture_output=True,
        text=True,
        check=True,
    )
    output = result.stdout
    parent, child, sibling = (
        output.split("\tfield\n")[1].split("\talias_column\n")[0].split("\t\tRow ")[1:]
    )
    assert "parent ──▶ NULL" in parent
    assert "children ──▶ [field(◆ id=12), field(◆ id=15)]" in parent
    assert "parent ──▶ field(◆ id=7)" in child
    assert "children ──▶ []" in child
    assert (
        "alias_columns ──▶ [alias_column(◈ field_id=12, ◆ column_id=2), alias_column(◈ field_id=12, ◆ column_id=4)]"
        in child
    )
    assert "alias_columns ──▶ []" in sibling
    assert "field ──▶ field(◆ id=12)" in output
    assert "column ──▶ (unavailable in saved schema)" in output


def test_summary_with_long_values(tmp_path: Path):
    path = tmp_path / "large.db"
    long_name = "prefix" + "x" * 999 + "suffix"
    with closing(sqlite3.connect(path)) as db:
        db.execute('CREATE TABLE "a""b" (name TEXT, payload BLOB)')
        db.execute('INSERT INTO "a""b" VALUES (?, zeroblob(4096))', (long_name,))
        db.executemany('INSERT INTO "a""b" VALUES (?, NULL)', [("later row",)] * 999)
        db.execute("CREATE TABLE empty_table (id INTEGER)")
        db.commit()

    summary = subprocess.check_output([sys.executable, PRINT_DB, path], text=True)
    output = summary
    assert 'a"b  1,000 rows' in output
    assert "empty_table  0 rows" in output
    assert "prefix" in output
    assert "suffix" in output
    assert long_name not in output
    assert "<4,096 bytes>" in output
    assert "later row" not in output
    assert len(output.splitlines()) < 10

    full = subprocess.check_output(
        [sys.executable, PRINT_DB, path, "--full"], text=True
    )
    assert long_name in full
    assert repr(bytes(4096)) in full
    assert "Row 1000" in full


@pytest.mark.parametrize("flags", [[], ["--full"]])
def test_saved_key_roles(tmp_path: Path, flags: list[str]):
    path = tmp_path / "roles.db"
    with closing(sqlite3.connect(path)) as db:
        db.executescript("""
            CREATE TABLE extra_parent (code TEXT PRIMARY KEY, label TEXT);
            CREATE TABLE extra_roles (
                owner TEXT REFERENCES extra_parent (code),
                slot TEXT,
                ref TEXT REFERENCES extra_parent (code),
                item_id INTEGER,
                note TEXT,
                PRIMARY KEY (owner, slot));
            INSERT INTO extra_parent VALUES ('a', 'first');
            INSERT INTO extra_roles VALUES ('a', 's1', 'a', 5, 'n');
        """)

    output = subprocess.check_output(
        [sys.executable, PRINT_DB, path, *flags], text=True
    )
    assert "\033" not in output
    separator = " = " if flags else "="
    for name, value in (
        ("◆ code", "'a'"),
        ("◈ owner", "'a'"),
        ("◆ slot", "'s1'"),
        ("◇ ref", "'a'"),
        ("item_id", "5"),
    ):
        assert f"{name}{separator}{value}" in output
    assert all(f"{marker} item_id" not in output for marker in "◆◇◈")


def test_missing_database(tmp_path: Path):
    path = tmp_path / "missing.db"
    result = subprocess.run(
        [sys.executable, PRINT_DB, path],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert str(path) in result.stderr
    assert not path.exists()
