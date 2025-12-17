"""
Copyright (c) 2025 Samantha Abbott. All rights reserved.

rhydrator: A bidirectional data transformation tool for ROOT RNTuple files. Dehydrates ROOT files into page bundles stored in an object store, and rehydrates them back into functional ROOT files on demand.
"""

from __future__ import annotations

from ._version import version as __version__

__all__ = ["__version__"]
