from __future__ import annotations

import importlib.metadata

import rhydrator as m


def test_version():
    assert importlib.metadata.version("rhydrator") == m.__version__
