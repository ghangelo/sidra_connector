# -*- coding: utf-8 -*-
"""
Gerencia as tarefas que rodam em background (downloads, chamadas de API).

Usa o QgsTaskManager do QGIS pra nao travar a interface enquanto
faz requisicoes HTTP ou baixa arquivos pesados.
"""

import gc

from qgis.core import (
    QgsApplication,
    QgsFeedback,
    QgsMessageLog,
    Qgis,
    QgsTask,
    QgsVectorLayer,
)
from qgis.PyQt.QtCore import pyqtSignal

from ..core.api_helpers import get_metadata_from_api
from ..core.mesh_downloader import MeshDownloader, fetch_available_years
from ..core.sidra_api_client import SidraApiClient
from ..exceptions import SidraCancelledError
from .layer_manager import add_layer_to_project, file_layer_to_memory, load_vector_layer

active_tasks = []


def cancel_all_tasks():
    """Cancela tudo que ta rodando e limpa a lista."""
    for task in list(active_tasks):
        if task.isRunning():
            task.cancel()
    active_tasks.clear()


class FetchSidraDataTask(QgsTask):
    """Busca dados da API SIDRA em background."""

    dataReady = pyqtSignal(dict, dict)
    fetchError = pyqtSignal(str)

    def __init__(self, url):
        super().__init__("A procurar dados da API SIDRA", QgsTask.Flag.CanCancel)
        self.url = url
        self.feedback = QgsFeedback()
        self.exception = None
        self.sidra_data = None
        self.header_info = None

    def cancel(self):
        self.feedback.cancel()
        super().cancel()

    def run(self):
        QgsMessageLog.logMessage(
            f"Buscando dados de: {self.url}", "SIDRA Connector", Qgis.MessageLevel.Info
        )
        try:
            client = SidraApiClient(self.url)
            self.sidra_data, self.header_info = client.fetch_and_parse(
                feedback=self.feedback,
                progress_callback=lambda done, total, _b: self.setProgress((done / total) * 100) if total else None,
            )
            return not self.isCanceled()
        except SidraCancelledError:
            return False
        except Exception as e:
            self.exception = str(e)
            QgsMessageLog.logMessage(
                f"Erro: {e}", "SIDRA Connector", Qgis.MessageLevel.Critical
            )
            return False

    def finished(self, result):
        if self in active_tasks:
            active_tasks.remove(self)
        if result and self.sidra_data is not None:
            self.dataReady.emit(self.sidra_data, self.header_info)
        else:
            error_message = self.exception if self.exception else "Tarefa cancelada."
            self.fetchError.emit(error_message)


class DownloadAndLoadLayerTask(QgsTask):
    """Baixa malha do IBGE, converte pra camada em memoria e adiciona ao projeto."""

    layerReady = pyqtSignal(QgsVectorLayer)
    downloadError = pyqtSignal(str)

    def __init__(self, url, layer_name):
        super().__init__(f"A baixar malha: {layer_name}", QgsTask.Flag.CanCancel)
        self.url = url
        self.layer_name = layer_name
        self.exception = None
        self.downloader = None
        self.shapefile_path = None

    def run(self):
        try:
            self.downloader = MeshDownloader(self.url)

            def progress_update(progress):
                self.setProgress(progress)

            self.shapefile_path = self.downloader.download_and_extract(
                progress_callback=progress_update,
                is_canceled=self.isCanceled,
            )

            if self.isCanceled():
                return False

            return self.shapefile_path is not None

        except SidraCancelledError:
            return False
        except Exception as e:
            self.exception = str(e)
            return False

    def finished(self, result):
        if self in active_tasks:
            active_tasks.remove(self)

        if result and self.shapefile_path:
            file_layer = load_vector_layer(self.shapefile_path, self.layer_name)
            mem_layer = (
                file_layer_to_memory(file_layer, self.layer_name)
                if file_layer.isValid()
                else None
            )

            # Solta todas as referências OGR/SIP antes de apagar os temporários no Windows
            file_layer = None
            gc.collect()

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


class FetchAvailableYearsTask(QgsTask):
    """Consulta em background os anos de malha disponíveis no GeoFTP."""

    yearsReady = pyqtSignal(list)

    def __init__(self):
        super().__init__("Consultando anos de malha no IBGE", QgsTask.Flag.CanCancel)
        self.years = None

    def run(self):
        try:
            self.years = fetch_available_years()
            return bool(self.years) and not self.isCanceled()
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Nao conseguiu atualizar anos do IBGE em background: {e}",
                "SIDRA Connector",
                Qgis.MessageLevel.Warning,
            )
            return False

    def finished(self, result):
        if self in active_tasks:
            active_tasks.remove(self)
        if result and self.years:
            self.yearsReady.emit(self.years)


class FetchTableMetadataTask(QgsTask):
    """Busca metadados de uma tabela SIDRA em background com cancelamento cooperativo."""

    metadataReady = pyqtSignal(object)
    metadataError = pyqtSignal(str)

    def __init__(self, table_id):
        super().__init__(f"Buscando detalhes da tabela {table_id}", QgsTask.Flag.CanCancel)
        self.table_id = str(table_id)
        self.feedback = QgsFeedback()
        self.metadata = None
        self.exception = None

    def cancel(self):
        self.feedback.cancel()
        super().cancel()

    def run(self):
        try:
            self.metadata = get_metadata_from_api(self.table_id, feedback=self.feedback)
            if self.feedback.isCanceled() or self.isCanceled():
                return False
            return self.metadata is not None
        except SidraCancelledError:
            return False
        except Exception as e:
            self.exception = str(e)
            return False

    def finished(self, result):
        if self in active_tasks:
            active_tasks.remove(self)
        if self.feedback.isCanceled() or self.isCanceled():
            return
        if result and self.metadata:
            self.metadataReady.emit(self.metadata)
        else:
            self.metadataError.emit(
                self.exception or "Não consegui buscar os detalhes dessa tabela."
            )


def run_fetch_task(url, on_success, on_error):
    task = FetchSidraDataTask(url)
    task.dataReady.connect(on_success)
    task.fetchError.connect(on_error)
    active_tasks.append(task)
    QgsApplication.taskManager().addTask(task)
    return task.feedback


def run_download_task(url, layer_name, on_success, on_error):
    task = DownloadAndLoadLayerTask(url, layer_name)
    task.layerReady.connect(on_success)
    task.downloadError.connect(on_error)
    active_tasks.append(task)
    QgsApplication.taskManager().addTask(task)
    return task


def run_years_task(on_success):
    task = FetchAvailableYearsTask()
    task.yearsReady.connect(on_success)
    active_tasks.append(task)
    QgsApplication.taskManager().addTask(task)
    return task


def run_metadata_task(table_id, on_success, on_error):
    task = FetchTableMetadataTask(table_id)
    task.metadataReady.connect(on_success)
    task.metadataError.connect(on_error)
    active_tasks.append(task)
    QgsApplication.taskManager().addTask(task)
    return task.feedback
