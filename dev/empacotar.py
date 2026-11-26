# -*- coding: utf-8 -*-
"""
Gera o pacote sidra_connector.zip pronto para upload em plugins.qgis.org.

Uso:
    python dev/empacotar.py
"""

import configparser
import os
import zipfile

ARCHIVE_ROOT = "sidra_connector"

RUNTIME_FILES = (
    "__init__.py",
    "plugin.py",
    "metadata.txt",
    "icon.png",
    "LICENSE",
    "README.md",
    "agregados_ibge.db",
    "constants.py",
    "exceptions.py",
)

RUNTIME_DIRS = (
    "core",
    "gis",
    "infrastructure",
    "services",
    "ui",
    "utils",
)

IGNORED_EXTENSIONS = (".pyc", ".pyo", ".psd", ".db-wal", ".db-shm")
REQUIRED_METADATA_KEYS = ("name", "version", "qgisMinimumVersion", "author", "email", "category", "icon")


def _validate_metadata(repo_root: str) -> None:
    meta_path = os.path.join(repo_root, "metadata.txt")
    if not os.path.isfile(meta_path):
        raise FileNotFoundError(f"metadata.txt não encontrado em {repo_root}")

    parser = configparser.ConfigParser(delimiters=("=",))
    parser.read(meta_path, encoding="utf-8")
    if "general" not in parser:
        raise ValueError("Seção [general] ausente em metadata.txt")

    general = parser["general"]
    missing = [key for key in REQUIRED_METADATA_KEYS if not general.get(key, "").strip()]
    if missing:
        raise ValueError(f"Campos obrigatórios ausentes em metadata.txt: {', '.join(missing)}")


def build_plugin_zip(repo_root: str, output_zip: str) -> list:
    """Empacota apenas os arquivos de runtime do plugin sob sidra_connector/."""
    repo_root = os.path.abspath(repo_root)
    _validate_metadata(repo_root)

    files_to_pack = []

    for filename in RUNTIME_FILES:
        src_path = os.path.join(repo_root, filename)
        if not os.path.isfile(src_path):
            raise FileNotFoundError(f"Arquivo obrigatório ausente: {src_path}")
        files_to_pack.append((src_path, f"{ARCHIVE_ROOT}/{filename}"))

    for dirname in RUNTIME_DIRS:
        dir_path = os.path.join(repo_root, dirname)
        if not os.path.isdir(dir_path):
            raise FileNotFoundError(f"Diretório obrigatório ausente: {dir_path}")
        for current_root, subdirs, filenames in os.walk(dir_path):
            subdirs[:] = sorted(d for d in subdirs if d != "__pycache__")
            for fname in sorted(filenames):
                if fname.lower().endswith(IGNORED_EXTENSIONS):
                    continue
                src_path = os.path.join(current_root, fname)
                rel_path = os.path.relpath(src_path, repo_root).replace(os.sep, "/")
                files_to_pack.append((src_path, f"{ARCHIVE_ROOT}/{rel_path}"))

    os.makedirs(os.path.dirname(os.path.abspath(output_zip)), exist_ok=True)
    with zipfile.ZipFile(output_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for src_path, arcname in files_to_pack:
            zf.write(src_path, arcname)

    with zipfile.ZipFile(output_zip, "r") as zf:
        archived = zf.namelist()
    for required in (f"{ARCHIVE_ROOT}/metadata.txt", f"{ARCHIVE_ROOT}/__init__.py"):
        if required not in archived:
            raise RuntimeError(f"Pacote inválido: {required} ausente no zip.")

    return archived


def main():
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    output_zip = os.path.join(repo_root, "sidra_connector.zip")
    archived = build_plugin_zip(repo_root, output_zip)
    size_mb = os.path.getsize(output_zip) / (1024 * 1024)
    print(f"Pacote gerado: {output_zip}")
    print(f"Arquivos: {len(archived)} | Tamanho: {size_mb:.2f} MB")


if __name__ == "__main__":
    main()
