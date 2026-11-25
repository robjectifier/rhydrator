"""Tests for the local ingest command."""

import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest
from conftest import root_io_spec_path

SCRIPT = Path(__file__).parents[1] / "scripts" / "dehydrate_local.py"


def test_quiet_ingest(tmp_path: Path):
    source = root_io_spec_path("anchor.root")
    result = subprocess.run(
        [sys.executable, SCRIPT, source, "/store/anchor.root", "test-dataset"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout == ""
    assert result.stderr == ""

    output = tmp_path / "output"
    assert any((output / "objects").iterdir())
    with closing(sqlite3.connect(output / "rhydrator.db")) as db:
        datasets = db.execute("SELECT name FROM dataset").fetchall()
        files = db.execute("SELECT COUNT(*) FROM input_file").fetchone()
    assert datasets == [("test-dataset",)]
    assert files == (1,)


@pytest.mark.parametrize("recreate", [False, True])
def test_existing_output_directory(tmp_path: Path, recreate: bool):
    source = root_io_spec_path("anchor.root")
    output = tmp_path / "results"
    command: list[str | Path] = [
        sys.executable,
        SCRIPT,
        "--output_dir",
        output,
        source,
        "/store/anchor.root",
    ]
    subprocess.run([*command, "old"], cwd=tmp_path, check=True)
    blob = output / "blobs" / "old.bin"
    blob.parent.mkdir()
    blob.write_bytes(b"old page")

    flags = ["--recreate"] if recreate else []
    subprocess.run([*command, "new", *flags], cwd=tmp_path, check=True)

    with closing(sqlite3.connect(output / "rhydrator.db")) as db:
        datasets = db.execute("SELECT name FROM dataset ORDER BY name").fetchall()
    if recreate:
        assert datasets == [("new",)]
        assert not blob.parent.exists()
    else:
        assert datasets == [("new",), ("old",)]
        assert blob.read_bytes() == b"old page"


@pytest.mark.parametrize(
    ("directory", "reason"),
    [
        (".", "working directory"),
        ("..", "working directory"),
        ("cwd_link", "working directory"),
        ("parent_link", "working directory"),
        ("../inputs", "input file"),
        ("inputs_link", "input file"),
    ],
)
def test_recreate_rejects_overlapping_paths(
    tmp_path: Path, directory: str, reason: str
):
    cwd = tmp_path / "work"
    cwd.mkdir()
    source = tmp_path / "inputs" / "anchor.root"
    source.parent.mkdir()
    source.write_bytes(root_io_spec_path("anchor.root").read_bytes())
    (cwd / "cwd_link").symlink_to(cwd, target_is_directory=True)
    (cwd / "parent_link").symlink_to(tmp_path, target_is_directory=True)
    (cwd / "inputs_link").symlink_to(source.parent, target_is_directory=True)

    result = subprocess.run(
        [
            sys.executable,
            SCRIPT,
            source,
            "/store/anchor.root",
            "new",
            "-r",
            "--output_dir",
            directory,
        ],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert f"would delete the {reason}" in result.stderr
    assert source.is_file()
    assert cwd.is_dir()
