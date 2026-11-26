# -*- coding: utf-8 -*-
"""
Baixa e extrai malhas territoriais do IBGE (shapefiles zipados).

O arquivo .zip vem do GeoFTP do IBGE, eh extraido pra uma pasta
temporaria, e depois o task_manager converte pra camada em memoria.
"""

import os
import re
import shutil
import tempfile
import zipfile

import requests

from ..exceptions import SidraCancelledError
from ..utils import constants

try:
    from qgis.core import QgsMessageLog, Qgis
    QGIS_AVAILABLE = True
except ImportError:
    QGIS_AVAILABLE = False

# Mapeamento dos sufixos antigos do GeoFTP (<= 2019)
_LEGACY_MESH_SLUGS = {
    "Municipios": "municipios",
    "UF": "unidades_da_federacao",
    "RG_Imediatas": "regioes_geograficas_imediatas",
    "RG_Intermediarias": "regioes_geograficas_intermediarias",
    "Pais": "",
}

# Anos em que o GeoFTP usa subpastas por escala/projeção em vez de malha por UF
_UNSUPPORTED_GEOFTP_YEARS = {2005, 2007}


def build_mesh_url(ano, localidade_sigla, malha_prefixo):
    """Monta a URL do .zip da malha no GeoFTP respeitando o layout de cada ano."""
    year = int(str(ano).strip())
    sigla = str(localidade_sigla).strip().upper()

    if sigla != "BR" and malha_prefixo in {"Pais", "Regioes"}:
        raise ValueError(
            f"A malha '{malha_prefixo}' só está disponível para a localidade 'Brasil'."
        )

    base_url = constants.IBGE_MESH_BASE_URL.format(ano=year)

    if year >= 2023:
        if sigla == "BR":
            return f"{base_url}Brasil/BR_{malha_prefixo}_{year}.zip"
        return f"{base_url}UFs/{sigla}/{sigla}_{malha_prefixo}_{year}.zip"

    if 2020 <= year <= 2022:
        if sigla == "BR":
            return f"{base_url}Brasil/BR/BR_{malha_prefixo}_{year}.zip"
        return f"{base_url}UFs/{sigla}/{sigla}_{malha_prefixo}_{year}.zip"

    legacy_slug = _LEGACY_MESH_SLUGS.get(malha_prefixo)
    if legacy_slug is None:
        raise ValueError(
            f"A malha '{malha_prefixo}' não está disponível para o ano {year}."
        )

    if 2015 <= year <= 2019:
        folder = "Brasil/BR/" if sigla == "BR" else f"UFs/{sigla}/"
        if malha_prefixo == "Pais":
            return f"{base_url}{folder}BR.zip"
        return f"{base_url}{folder}{sigla.lower()}_{legacy_slug}.zip"

    if sigla == "BR":
        raise ValueError(f"Malha nacional consolidada indisponível no GeoFTP para {year}.")

    uf_folder = sigla if year in {2013, 2014} else sigla.lower()
    return f"{base_url}{uf_folder}/{sigla.lower()}_{legacy_slug}.zip"


def fetch_available_years():
    """Descobre quais anos de malha municipal estao disponiveis no IBGE."""
    try:
        url = constants.IBGE_MESH_BASE_URL_PARENT
        response = requests.get(url, timeout=constants.API_TIMEOUT)
        response.raise_for_status()

        year_folders = re.findall(r'href="municipio_(\d{4})/"', response.text)

        if not year_folders:
            raise ValueError("Nenhuma pasta de ano encontrada no site do IBGE.")

        years = sorted(
            {int(y) for y in year_folders if int(y) not in _UNSUPPORTED_GEOFTP_YEARS},
            reverse=True,
        )
        return [str(y) for y in years]

    except requests.exceptions.RequestException as e:
        raise ConnectionError(f"Nao conseguiu conectar ao IBGE: {e}")


class MeshDownloader:
    """Baixa um shapefile zipado do GeoFTP e extrai pra pasta temporaria."""

    def __init__(self, url):
        self.url = url
        self.temp_dir_path = tempfile.mkdtemp()

    def _resolve_url_if_404(self):
        """Se o arquivo exato der 404 (ex: br_municipios_20200807.zip em 2019),
        consulta o índice da pasta pai no GeoFTP procurando um .zip equivalente.
        """
        parent_url, expected_file = self.url.rsplit("/", 1)
        stem = os.path.splitext(expected_file)[0].lower()
        resp = requests.get(parent_url + "/", timeout=constants.API_TIMEOUT)
        resp.raise_for_status()
        candidates = re.findall(r'href="([^"]+\.zip)"', resp.text, flags=re.IGNORECASE)
        for cand in candidates:
            cand_lower = cand.lower()
            if cand_lower == expected_file.lower() or cand_lower.startswith(stem):
                return f"{parent_url}/{cand}"
        return None

    def download_and_extract(self, progress_callback=None, is_canceled=None):
        """Baixa o .zip em pedacos e extrai o .shp."""
        try:
            zip_path = os.path.join(self.temp_dir_path, "download.zip")
            target_url = self.url

            response = requests.get(target_url, stream=True, timeout=constants.DOWNLOAD_TIMEOUT)
            if getattr(response, "status_code", 200) == 404 and "/" in target_url:
                response.close()
                alt_url = self._resolve_url_if_404()
                if alt_url:
                    target_url = alt_url
                    response = requests.get(
                        target_url, stream=True, timeout=constants.DOWNLOAD_TIMEOUT
                    )

            with response:
                response.raise_for_status()

                total_size = int(response.headers.get("content-length", 0))
                bytes_downloaded = 0

                with open(zip_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=constants.CHUNK_SIZE):
                        if is_canceled and is_canceled():
                            raise SidraCancelledError("Download cancelado.")
                        f.write(chunk)
                        bytes_downloaded += len(chunk)
                        if progress_callback and total_size > 0:
                            progress = (bytes_downloaded / total_size) * 100
                            progress_callback(progress)

            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                shapefile_name = None
                safe_dir = os.path.realpath(self.temp_dir_path)
                for member in zip_ref.infolist():
                    if is_canceled and is_canceled():
                        raise SidraCancelledError("Download cancelado.")
                    if os.path.isabs(member.filename) or ".." in member.filename:
                        raise ValueError(f"Caminho suspeito no zip: {member.filename}")
                    target_path = os.path.realpath(
                        os.path.join(self.temp_dir_path, member.filename)
                    )
                    if not target_path.startswith(safe_dir + os.sep) and target_path != safe_dir:
                        raise ValueError(f"Caminho suspeito no zip: {member.filename}")
                    zip_ref.extract(member, self.temp_dir_path)
                    if member.filename.lower().endswith(".shp"):
                        shapefile_name = member.filename

                if not shapefile_name:
                    raise FileNotFoundError("O zip nao tem nenhum .shp dentro.")
                return os.path.join(self.temp_dir_path, shapefile_name)

        except SidraCancelledError:
            self.cleanup()
            raise
        except requests.exceptions.RequestException as e:
            self.cleanup()
            raise ConnectionError(f"Falha no download: {e}")
        except Exception:
            self.cleanup()
            raise

    def cleanup(self):
        """Apaga a pasta temporaria com tudo dentro."""
        if not self.temp_dir_path or not os.path.exists(self.temp_dir_path):
            return
        path = self.temp_dir_path
        try:
            shutil.rmtree(path)
        except PermissionError:
            # No Windows, o pool OGR do QGIS solta o .dbf após o retorno ao event loop
            if QGIS_AVAILABLE:
                from qgis.PyQt.QtCore import QTimer
                QTimer.singleShot(2000, lambda p=path: shutil.rmtree(p, ignore_errors=True))
        except Exception as e:
            if QGIS_AVAILABLE:
                QgsMessageLog.logMessage(
                    f"Erro ao limpar temporarios: {e}",
                    "SIDRA Connector", Qgis.MessageLevel.Warning,
                )
