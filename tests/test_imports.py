from __future__ import annotations

import importlib
import pkgutil

import pytest

import rhydrator


def _modules() -> list[str]:
    return [
        info.name
        for info in pkgutil.walk_packages(rhydrator.__path__, f"{rhydrator.__name__}.")
    ]


def test_package_has_modules():
    assert "rhydrator.model" in _modules()


@pytest.mark.parametrize("name", _modules())
def test_module_imports(name):
    importlib.import_module(name)
