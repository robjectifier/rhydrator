"""Wrapper for dehydrating a local file and ingesting it into the database."""

# abbott: this will need to be an executable that can parse args
#  args: filepath, output paths (for blobs & SQLite), dataset & file metadata?
# should spawn the SQLite session and call the ingest function with the file info

import argparse
import os
import shutil
from rhydrator import ingest
from rhydrator._terminal import Watershed
from rhydrator.bundle import BUNDLERS
from rhydrator.model import Base
from sqlalchemy import URL, create_engine
from sqlalchemy.orm import Session
from pathlib import Path

if __package__:
    from .print_db import print_db
else:
    from print_db import print_db

# handle args
parser = argparse.ArgumentParser(description="Dehydrate a local file and ingest it into the database.")
parser.add_argument("file_path", type=str, help="Path to the local file to be dehydrated and ingested.")
parser.add_argument("file_lfn", type=str, help="Logical file name (LFN) of the file to be ingested.")
parser.add_argument("dataset_name", type=str, help="Name of the dataset to associate with the file.")
parser.add_argument(
    "--bundler",
    choices=BUNDLERS,
    default="one-page-per-object",
    help="Policy for grouping pages into objects (default: %(default)s).",
)
parser.add_argument("--output_dir", type=str, default="./output", help="Directory to store the output blobs and SQLite database.")
parser.add_argument("-d", "--debug", action="store_true", help="Enable debug mode for verbose output.")
parser.add_argument(
    "-p", "--print-db", action="store_true", help="Summarize the saved database after ingest."
)
parser.add_argument(
    "--print-db-full", action="store_true", help="Print every row, value and relationship target after ingest."
)
parser.add_argument("-r", "--recreate", action="store_true", help="Delete the output directory and all its contents before ingesting.")
args = parser.parse_args()
if args.debug:
    term = Watershed()

# create a NewFile instance from the provided arguments
new_file = ingest.NewFile(
    dataset_name=args.dataset_name,
    file_lfn=args.file_lfn,
    file_path=args.file_path,
)

# Create the output directory if it doesn't exist, or recreate it if specified
output_dir = Path(args.output_dir)
if args.recreate and output_dir.exists():
    if args.debug:
        print(f"{term.warning}Recreating output directory{term.reset} {args.output_dir!r}")  # noqa: T201
    shutil.rmtree(output_dir)
output_dir.mkdir(parents=True, exist_ok=True)
objects_dir = os.path.join(output_dir, "objects")
os.makedirs(objects_dir, exist_ok=True)

# Create a SQLAlchemy engine and session
db_path = os.path.join(output_dir, "rhydrator.db")
engine = create_engine(URL.create("sqlite", database=db_path))
if args.debug:
    print(term.heading(f"Using SQLite database {engine.url.database}"))  # noqa: T201
Base.metadata.create_all(engine) # Create the database tables if they don't exist
with Session(engine) as session:
    # Ingest the new file into the database
    ingest.ingest_file(
        session, new_file, objects_dir=objects_dir, bundler=BUNDLERS[args.bundler], debug=args.debug
    )

    session.commit()

if args.print_db or args.print_db_full:
    print_db(db_path, full=args.print_db_full)
