# -*- coding: utf-8 -*-
"""Garante que a raiz do repositório possa ser importada como pacote `sidra_connector`."""

import os
import sys
import types


def ensure_plugin_package():
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    parent_dir = os.path.dirname(repo_root)
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

    if "sidra_connector" not in sys.modules:
        pkg = types.ModuleType("sidra_connector")
        pkg.__path__ = [repo_root]
        pkg.__package__ = "sidra_connector"
        sys.modules["sidra_connector"] = pkg


ensure_plugin_package()
