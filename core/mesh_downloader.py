# -*- coding: utf-8 -*-
"""
Baixa e extrai malhas territoriais do IBGE (shapefiles zipados).

O arquivo .zip vem do GeoFTP do IBGE, eh extraido pra uma pasta
temporaria, e depois o task_manager converte pra camada em memoria.
"""

import os
import requests
import zipfile
import tempfile
import shutil
import re

from qgis.core import QgsMessageLog, Qgis
from ..utils import constants


def fetch_available_years():
    """Descobre quais anos de malha municipal estao disponiveis no IBGE.

    Entra na pagina do GeoFTP e procura pastas tipo "municipio_2024/".
    Retorna em ordem decrescente (mais recente primeiro).
    """
    try:
        url = constants.IBGE_MESH_BASE_URL_PARENT
        response = requests.get(url, timeout=constants.API_TIMEOUT)
        response.raise_for_status()

        year_folders = re.findall(r'href="municipio_(\d{4})/"', response.text)

        if not year_folders:
            raise ValueError("Nenhuma pasta de ano encontrada no site do IBGE.")

        years = sorted([int(y) for y in year_folders], reverse=True)
        return [str(y) for y in years]

    except requests.exceptions.RequestException as e:
        raise ConnectionError(f"Nao conseguiu conectar ao IBGE: {e}")


class MeshDownloader:
    """Baixa um shapefile zipado do GeoFTP e extrai pra pasta temporaria."""

    def __init__(self, url):
        self.url = url
        self.temp_dir_path = tempfile.mkdtemp()

    def download_and_extract(self, progress_callback=None):
        """Baixa o .zip em pedacos e extrai o .shp.

        Retorna o caminho completo do .shp extraido.
        Levanta excecao se nao encontrar .shp dentro do zip.
        """
        try:
            zip_path = os.path.join(self.temp_dir_path, 'download.zip')

            with requests.get(self.url, stream=True, timeout=constants.DOWNLOAD_TIMEOUT) as response:
                response.raise_for_status()

                total_size = int(response.headers.get('content-length', 0))
                bytes_downloaded = 0

                with open(zip_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=constants.CHUNK_SIZE):
                        f.write(chunk)
                        bytes_downloaded += len(chunk)
                        if progress_callback and total_size > 0:
                            progress = (bytes_downloaded / total_size) * 100
                            progress_callback(progress)

            # Extrai tudo, mas com protecao contra Zip Slip
            # (caminhos maliciosos tipo "../../etc/passwd")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                shapefile_name = None
                for member in zip_ref.infolist():
                    if os.path.isabs(member.filename) or '..' in member.filename:
                        raise ValueError(
                            f"Caminho suspeito no zip: {member.filename}"
                        )
                    target_path = os.path.realpath(
                        os.path.join(self.temp_dir_path, member.filename)
                    )
                    safe_dir = os.path.realpath(self.temp_dir_path)
                    if not target_path.startswith(safe_dir + os.sep) and target_path != safe_dir:
                        raise ValueError(
                            f"Caminho suspeito no zip: {member.filename}"
                        )
                    zip_ref.extract(member, self.temp_dir_path)
                    if member.filename.lower().endswith('.shp'):
                        shapefile_name = member.filename

                if not shapefile_name:
                    raise FileNotFoundError("O zip nao tem nenhum .shp dentro.")
                return os.path.join(self.temp_dir_path, shapefile_name)

        except requests.exceptions.RequestException as e:
            self.cleanup()
            raise ConnectionError(f"Falha no download: {e}")
        except Exception as e:
            self.cleanup()
            raise e

    def cleanup(self):
        """Apaga a pasta temporaria com tudo dentro."""
        try:
            if self.temp_dir_path and os.path.exists(self.temp_dir_path):
                shutil.rmtree(self.temp_dir_path)
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Erro ao limpar temporarios: {e}",
                "SIDRA Connector", Qgis.Warning,
            )
