"""Dehydrate a local file and ingest it into a SQLite database."""

import argparse
import shutil
from pathlib import Path

from sqlalchemy import URL, create_engine
from sqlalchemy.orm import Session

from rhydrator import ingest
from rhydrator.bundle import BUNDLERS
from rhydrator.model import Base


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file_path", help="Path to the local file to ingest.")
    parser.add_argument("file_lfn", help="Logical file name (LFN) of the file.")
    parser.add_argument("dataset_name", help="Name of the dataset for the file.")
    parser.add_argument(
        "--bundler",
        choices=BUNDLERS,
        default="one-page-per-object",
        help="Policy for grouping pages into objects (default: %(default)s).",
    )
    parser.add_argument(
        "--output_dir",
        default="./output",
        help="Directory for the output blobs and SQLite database.",
    )
    parser.add_argument(
        "-r",
        "--recreate",
        action="store_true",
        help="Delete the output directory before ingesting.",
    )
    return parser


def main() -> None:
    parser = make_parser()
    args = parser.parse_args()

    output_path = Path(args.output_dir)
    output_dir = output_path.resolve()
    if args.recreate and output_dir.exists():
        protected = {
            "working directory": Path.cwd().resolve(),
            "input file": Path(args.file_path).resolve(),
        }
        for reason, path in protected.items():
            if path == output_dir or output_dir in path.parents:
                parser.error(f"--recreate would delete the {reason}: {output_dir}")
        input_path = Path(args.file_path).absolute()
        if (
            output_dir in input_path.parents
            or output_path.absolute() in input_path.parents
        ):
            parser.error(f"--recreate would delete the input file: {output_dir}")
        if output_path.is_symlink():
            parser.error(f"--recreate cannot replace a symbolic link: {output_path}")
        shutil.rmtree(output_dir)

    objects_dir = output_dir / "objects"
    objects_dir.mkdir(parents=True, exist_ok=True)

    new_file = ingest.NewFile(
        dataset_name=args.dataset_name,
        file_lfn=args.file_lfn,
        file_path=args.file_path,
    )
    engine = create_engine(
        URL.create("sqlite", database=str(output_dir / "rhydrator.db"))
    )
    try:
        Base.metadata.create_all(engine)
        with Session(engine) as session:
            ingest.ingest_file(
                session,
                new_file,
                objects_dir=objects_dir,
                bundler=BUNDLERS[args.bundler],
            )
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
