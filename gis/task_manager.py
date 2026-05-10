# -*- coding: utf-8 -*-
"""
Gerencia as tarefas que rodam em background (downloads, chamadas de API).

Usa o QgsTaskManager do QGIS pra nao travar a interface enquanto
faz requisicoes HTTP ou baixa arquivos pesados.
"""

from qgis.core import QgsTask, QgsMessageLog, Qgis, QgsApplication, QgsVectorLayer
from qgis.PyQt.QtCore import pyqtSignal

from ..core.sidra_api_client import SidraApiClient
from ..core.mesh_downloader import MeshDownloader
from .layer_manager import load_vector_layer, add_layer_to_project, file_layer_to_memory

# Guarda referencia das tarefas rodando pra poder cancelar tudo
# quando o plugin for descarregado
active_tasks = []


def cancel_all_tasks():
    """Cancela tudo que ta rodando e limpa a lista."""
    for task in active_tasks:
        if task.isRunning():
            task.cancel()
    active_tasks.clear()


class FetchSidraDataTask(QgsTask):
    """Busca dados da API SIDRA em background."""

    dataReady = pyqtSignal(dict, dict)
    fetchError = pyqtSignal(str)

    def __init__(self, url):
        super().__init__('A procurar dados da API SIDRA', QgsTask.CanCancel)
        self.url = url
        self.exception = None
        self.sidra_data = None
        self.header_info = None

    def run(self):
        """Roda na thread de background -- faz a requisicao HTTP."""
        QgsMessageLog.logMessage(
            f'Buscando dados de: {self.url}', 'SIDRA Connector', Qgis.Info
        )
        try:
            client = SidraApiClient(self.url)
            self.sidra_data, self.header_info = client.fetch_and_parse()

            if isinstance(self.sidra_data, dict):
                QgsMessageLog.logMessage(
                    f'Recebidos {len(self.sidra_data)} registros',
                    'SIDRA Connector', Qgis.Info,
                )
                if self.sidra_data:
                    sample_keys = list(self.sidra_data.keys())[:3]
                    QgsMessageLog.logMessage(
                        f'Exemplo de codigos: {sample_keys}',
                        'SIDRA Connector', Qgis.Info,
                    )
            else:
                QgsMessageLog.logMessage(
                    f'Tipo inesperado: {type(self.sidra_data)}',
                    'SIDRA Connector', Qgis.Warning,
                )

            return True
        except Exception as e:
            self.exception = str(e)
            QgsMessageLog.logMessage(
                f'Erro: {e}', 'SIDRA Connector', Qgis.Critical
            )
            return False

    def finished(self, result):
        """Volta pra thread principal e avisa quem ta ouvindo."""
        if self in active_tasks:
            active_tasks.remove(self)
        if result and self.sidra_data is not None:
            self.dataReady.emit(self.sidra_data, self.header_info)
        else:
            error_message = self.exception if self.exception else 'Tarefa cancelada.'
            self.fetchError.emit(error_message)


class DownloadAndLoadLayerTask(QgsTask):
    """Baixa malha do IBGE, converte pra camada em memoria e adiciona ao projeto.

    O download roda em background, mas a criacao da camada QGIS
    acontece na thread principal (exigencia do Qt).
    """

    layerReady = pyqtSignal(QgsVectorLayer)
    downloadError = pyqtSignal(str)

    def __init__(self, url, layer_name):
        super().__init__(f'A baixar malha: {layer_name}', QgsTask.CanCancel)
        self.url = url
        self.layer_name = layer_name
        self.exception = None
        self.downloader = None
        self.shapefile_path = None

    def run(self):
        """Background: baixa o zip e extrai o shapefile."""
        try:
            self.downloader = MeshDownloader(self.url)

            def progress_update(progress):
                self.setProgress(progress)

            self.shapefile_path = self.downloader.download_and_extract(progress_update)

            if self.isCanceled():
                return False

            return self.shapefile_path is not None

        except Exception as e:
            self.exception = str(e)
            return False

    def finished(self, result):
        """Thread principal: cria a camada, converte pra memoria e limpa disco."""
        if self in active_tasks:
            active_tasks.remove(self)

        if result and self.shapefile_path:
            file_layer = load_vector_layer(self.shapefile_path, self.layer_name)
            mem_layer = (
                file_layer_to_memory(file_layer, self.layer_name)
                if file_layer.isValid()
                else None
            )

            # Solta a referencia OGR antes de apagar os arquivos,
            # senao no Windows o .shp fica travado
            del file_layer

            if self.downloader:
                self.downloader.cleanup()

            if mem_layer:
                add_layer_to_project(mem_layer)
                self.layerReady.emit(mem_layer)
            else:
                self.downloadError.emit(
                    "Nao conseguiu converter a malha pra camada temporaria."
                )
        else:
            if self.downloader:
                self.downloader.cleanup()
            error_message = f"Falha ao baixar a malha: {self.exception}"
            if self.isCanceled():
                error_message = "Download cancelado."
            self.downloadError.emit(error_message)


def run_fetch_task(url, on_success, on_error):
    """Atalho pra criar e rodar uma tarefa de busca de dados."""
    task = FetchSidraDataTask(url)
    task.dataReady.connect(on_success)
    task.fetchError.connect(on_error)
    active_tasks.append(task)
    QgsApplication.taskManager().addTask(task)


def run_download_task(url, layer_name, on_success, on_error):
    """Atalho pra criar e rodar uma tarefa de download de malha."""
    task = DownloadAndLoadLayerTask(url, layer_name)
    task.layerReady.connect(on_success)
    task.downloadError.connect(on_error)
    active_tasks.append(task)
    QgsApplication.taskManager().addTask(task)
