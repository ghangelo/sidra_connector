# -*- coding: utf-8 -*-
"""
Módulo de download e extração de malhas territoriais do IBGE.

Baixa arquivos .zip do GeoFTP do IBGE (shapefiles de malhas municipais,
estaduais, etc.), extrai o .shp para um diretório temporário e disponibiliza
o caminho para que o ``task_manager`` converta em camada de memória.

Fluxo:
    1. ``MeshDownloader(url)`` → cria diretório temporário.
    2. ``download_and_extract()`` → baixa .zip em streaming, extrai .shp.
    3. Após conversão para memória, ``cleanup()`` remove os temporários.
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
    """Busca os anos de malhas municipais disponíveis no GeoFTP do IBGE.

    Faz um GET na página-índice do FTP e extrai os anos a partir dos nomes
    das pastas ``municipio_YYYY/`` encontradas nos links HTML.

    :returns: Lista de strings (ex: ``['2024','2023',...]``), ordem decrescente.
    :raises ConnectionError: Se o servidor estiver inacessível.
    :raises ValueError: Se nenhuma pasta de ano for encontrada.
    """
    try:
        url = constants.IBGE_MESH_BASE_URL_PARENT
        response = requests.get(url, timeout=constants.API_TIMEOUT)
        response.raise_for_status()

        # Extrai anos das pastas "municipio_YYYY/"
        year_folders = re.findall(r'href="municipio_(\d{4})/"', response.text)

        if not year_folders:
            raise ValueError("Nenhuma pasta de ano encontrada na página do IBGE.")

        years = sorted([int(y) for y in year_folders], reverse=True)
        return [str(y) for y in years]

    except requests.exceptions.RequestException as e:
        raise ConnectionError(
            f"Não foi possível conectar ao servidor do IBGE para buscar os anos: {e}"
        )


class MeshDownloader:
    """Baixa e extrai um shapefile de malha territorial a partir do GeoFTP IBGE.

    Ciclo de vida típico (executado dentro de ``DownloadAndLoadLayerTask``):
        >>> dl = MeshDownloader(url)
        >>> shp_path = dl.download_and_extract(progress_callback)
        >>> # ... converter shp_path em camada de memória ...
        >>> dl.cleanup()
    """

    def __init__(self, url):
        """
        :param url: URL completa do arquivo .zip no GeoFTP do IBGE.
        """
        self.url = url
        # Diretório temporário exclusivo para esta operação de download
        self.temp_dir_path = tempfile.mkdtemp()

    def download_and_extract(self, progress_callback=None):
        """Baixa o .zip em streaming, extrai e retorna o caminho do .shp.

        :param progress_callback: Função ``f(percent: float)`` chamada
            durante o download para relatar progresso (0–100).
        :returns: Caminho completo para o arquivo .shp extraído.
        :raises ConnectionError: Se o download falhar.
        :raises FileNotFoundError: Se o .zip não contiver um .shp.
        """
        try:
            zip_path = os.path.join(self.temp_dir_path, 'download.zip')

            # Download em streaming dentro de um context manager para
            # garantir que o socket é fechado mesmo em caso de falha.
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

            # Extrai e localiza o .shp dentro do .zip (com proteção Zip Slip)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                shapefile_name = None
                for member in zip_ref.infolist():
                    # Rejeitar caminhos absolutos (cross-platform) e travessias
                    # de diretório (Zip Slip)
                    if os.path.isabs(member.filename) or '..' in member.filename:
                        raise ValueError(
                            f"Caminho inseguro detectado no .zip: {member.filename}"
                        )
                    # Verificação extra: o caminho resolvido deve ficar dentro
                    # do diretório temporário
                    target_path = os.path.realpath(
                        os.path.join(self.temp_dir_path, member.filename)
                    )
                    safe_dir = os.path.realpath(self.temp_dir_path)
                    if not target_path.startswith(safe_dir + os.sep) and target_path != safe_dir:
                        raise ValueError(
                            f"Caminho inseguro detectado no .zip: {member.filename}"
                        )
                    zip_ref.extract(member, self.temp_dir_path)
                    if member.filename.lower().endswith('.shp'):
                        shapefile_name = member.filename

                if not shapefile_name:
                    raise FileNotFoundError("Nenhum ficheiro .shp encontrado no arquivo .zip.")
                return os.path.join(self.temp_dir_path, shapefile_name)

        except requests.exceptions.RequestException as e:
            self.cleanup()
            raise ConnectionError(f"Falha no download da malha: {e}")
        except Exception as e:
            self.cleanup()
            raise e

    def cleanup(self):
        """Remove o diretório temporário e todo o seu conteúdo.

        Chamado pelo ``task_manager`` após a conversão para camada de
        memória. Falhas silenciosas são toleradas (diretório pode já
        ter sido removido).
        """
        try:
            if self.temp_dir_path and os.path.exists(self.temp_dir_path):
                shutil.rmtree(self.temp_dir_path)
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Erro ao limpar temporários: {e}",
                "SIDRA Connector", Qgis.Warning,
            )
