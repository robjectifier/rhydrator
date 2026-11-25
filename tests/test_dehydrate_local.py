"""Tests for the local ingest command."""

import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest
from conftest import root_io_spec_path


@pytest.mark.parametrize("flags", [[], ["-d"], ["--debug"]])
def test_debug_output(tmp_path: Path, flags: list[str]):
    script = Path(__file__).parents[1] / "scripts" / "dehydrate_local.py"
    source = root_io_spec_path("anchor.root")
    result = subprocess.run(
        [sys.executable, script, source, "/store/anchor.root", "test-dataset", *flags],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    )

    if flags:
        assert "--- Contents of" not in result.stdout
        assert "--- Using SQLite database" in result.stdout
        assert "Opening ROOT file" in result.stdout
        lines = result.stdout.rstrip().splitlines()
        assert all(not line.startswith(" ") for line in lines)
        assert [line for line in lines if "Step" in line] == [
            "\t≈ Step 1: Register the input file '/store/anchor.root'",
            "\t\t~ Step 1.1: Find or create the Dataset",
            "\t\t~ Step 1.2: Create the InputFile and initialize its page placements",
            "\t≈ Step 2: Read the RNTuple schema and register its instance",
            "\t\t~ Step 2.1: Read the anchor and schema, then create the RNTuple",
            "\t\t~ Step 2.2: Create the fields, then link children to their parents",
            "\t\t~ Step 2.3: Create columns and representations, then link aliases",
            "\t\t~ Step 2.4: Create the RNTupleInstance for this input file",
            "\t≈ Step 3: Store this RNTuple's clusters and pages",
            "\t\t~ Step 3.1: Create each ClusterGroup and its Clusters",
            "\t\t~ Step 3.2: Create one PageGroup per column range with stored pages",
            "\t\t~ Step 3.3: Bundle this cluster's pages",
            "\t\t~ Step 3.4: Set the instance's entry count after all its clusters",
            "\t≈ Step 4: Finish the input file",
            "\t\t~ Step 4.1: Build extents from placements in source-file order",
            "\t\t~ Step 4.2: Collect the non-page bytes before, between, and after the extents",
            "\t\t~ Step 4.3: Pack the rehydration record, reserve its object ID, and write it",
            "\t≈ Final Step: Commit the file's metadata",
        ]
        assert "\t\t\t✓ Created dataset 'test-dataset' (id=1)" in lines
        assert "\t\t\tRNTuple: b'ntpl'" in lines
        assert "\t\t\t█ Cluster: 1/1, 3 entries, 2 columns with pages" in lines
        assert "\t\t\tExtents: 2 after writing 2 page bundles" in lines
        assert "\t\t\t✓ Schema ready: 2 fields, 2 logical columns, 2 physical representations" in result.stdout
        assert f"\t\t\tChecksumming {source.stat().st_size:,} input bytes" in result.stdout
        assert "\t\t\t✓ Wrote output/objects/rehydration-1" in result.stdout
        assert lines[-1] == "\t\t✓ Committed session for '/store/anchor.root'"
        assert "\033[" not in result.stdout
        assert "rhydrator.db" in result.stdout
        assert str(source) in result.stdout
        assert "test-dataset" in result.stdout
        assert "/store/anchor.root" in result.stdout
    else:
        assert result.stdout == ""


@pytest.mark.parametrize(
    ("print_flag", "standalone_flags", "title"),
    [
        ("-p", [], "Summary"),
        ("--print-db", [], "Summary"),
        ("--print-db-full", ["--full"], "Contents"),
    ],
)
def test_print_saved_database_after_ingest(
    tmp_path: Path, print_flag: str, standalone_flags: list[str], title: str
):
    script = Path(__file__).parents[1] / "scripts" / "dehydrate_local.py"
    source = root_io_spec_path("anchor.root")
    output = tmp_path / "saved #1?"
    after_ingest = subprocess.run(
        [
            sys.executable,
            script,
            source,
            "/store/anchor.root",
            "test-dataset",
            "--output_dir",
            output,
            print_flag,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    standalone = subprocess.run(
        [sys.executable, script.with_name("print_db.py"), output / "rhydrator.db", *standalone_flags],
        capture_output=True,
        text=True,
        check=True,
    )
    assert after_ingest.stdout == standalone.stdout
    assert f"--- {title} of" in standalone.stdout
    assert "'/store/anchor.root'" in standalone.stdout
    assert "'test-dataset'" in standalone.stdout
    assert "input_files ──▶ [input_file(◆ id=1)]" in standalone.stdout
    assert "dataset ──▶ dataset(◆ id=1)" in standalone.stdout


@pytest.mark.parametrize("recreate", [False, True])
def test_existing_output_directory(tmp_path: Path, recreate: bool):
    script = Path(__file__).parents[1] / "scripts" / "dehydrate_local.py"
    source = root_io_spec_path("anchor.root")
    output = tmp_path / "results"
    command = [
        sys.executable, script, "--output_dir", output, source, "/store/anchor.root",
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
    ("directory", "reason"), [(".", "working directory"), ("inputs", "input file")]
)
def test_recreate_rejects_overlapping_paths(tmp_path: Path, directory: str, reason: str):
    source = tmp_path / "inputs" / "anchor.root"
    source.parent.mkdir()
    source.write_bytes(root_io_spec_path("anchor.root").read_bytes())
    script = Path(__file__).parents[1] / "scripts" / "dehydrate_local.py"
    result = subprocess.run(
        [sys.executable, script, source, "/store/anchor.root", "new", "-r", "--output_dir", directory],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert f"would delete the {reason}" in result.stderr
    assert source.is_file()
