# -*- coding: utf-8 -*-
"""
Gerenciador de tarefas assíncronas (QgsTask) do plugin.

Utiliza o ``QgsTaskManager`` do QGIS para executar operações longas
(requisições HTTP, downloads) em threads de segundo plano, sem
bloquear a interface gráfica.

Tarefas implementadas:
- ``FetchSidraDataTask``: busca dados tabulares da API SIDRA.
- ``DownloadAndLoadLayerTask``: baixa malha territorial, converte
  para camada temporária em memória e adiciona ao projeto.

**Thread-safety**: objetos ``QgsVectorLayer`` são criados apenas no
thread principal (dentro de ``finished()``), nunca no ``run()``.
"""

from qgis.core import QgsTask, QgsMessageLog, Qgis, QgsApplication, QgsVectorLayer
from qgis.PyQt.QtCore import pyqtSignal

from ..core.sidra_api_client import SidraApiClient
from ..core.mesh_downloader import MeshDownloader
from .layer_manager import load_vector_layer, add_layer_to_project, file_layer_to_memory

# Lista global de tarefas ativas — permite cancelamento em lote ao descarregar o plugin.
active_tasks = []


def cancel_all_tasks():
    """Cancela todas as tarefas pendentes e limpa a lista.

    Chamado por ``SidraConnector.unload()`` para liberar recursos
    quando o plugin é descarregado.
    """
    for task in active_tasks:
        if task.isRunning():
            task.cancel()
    active_tasks.clear()


# ---------------------------------------------------------------------------
#  Tarefa: busca de dados da API SIDRA
# ---------------------------------------------------------------------------

class FetchSidraDataTask(QgsTask):
    """Busca dados tabulares da API SIDRA em background.

    Sinais emitidos (thread principal, via ``finished()``):
    - ``dataReady(dict, dict)`` — dados + header_info em caso de sucesso.
    - ``fetchError(str)`` — mensagem de erro.
    """

    dataReady = pyqtSignal(dict, dict)
    fetchError = pyqtSignal(str)

    def __init__(self, url):
        super().__init__('A procurar dados da API SIDRA', QgsTask.CanCancel)
        self.url = url
        self.exception = None
        self.sidra_data = None
        self.header_info = None

    def run(self):
        """Executado em thread de background — faz a requisição HTTP."""
        QgsMessageLog.logMessage(
            f'A iniciar busca de dados de: {self.url}', 'SIDRA Connector', Qgis.Info
        )
        try:
            client = SidraApiClient(self.url)
            self.sidra_data, self.header_info = client.fetch_and_parse()

            # Log de diagnóstico
            if isinstance(self.sidra_data, dict):
                QgsMessageLog.logMessage(
                    f'Dados recebidos: {len(self.sidra_data)} registros',
                    'SIDRA Connector', Qgis.Info,
                )
                if self.sidra_data:
                    sample_keys = list(self.sidra_data.keys())[:3]
                    QgsMessageLog.logMessage(
                        f'Códigos geográficos de exemplo: {sample_keys}',
                        'SIDRA Connector', Qgis.Info,
                    )
            else:
                QgsMessageLog.logMessage(
                    f'Dados recebidos têm tipo incorreto: {type(self.sidra_data)}',
                    'SIDRA Connector', Qgis.Warning,
                )

            return True
        except Exception as e:
            self.exception = str(e)
            QgsMessageLog.logMessage(
                f'Erro na busca de dados: {e}', 'SIDRA Connector', Qgis.Critical
            )
            return False

    def finished(self, result):
        """Executado no thread principal após ``run()`` terminar."""
        if self in active_tasks:
            active_tasks.remove(self)
        if result and self.sidra_data is not None:
            self.dataReady.emit(self.sidra_data, self.header_info)
        else:
            error_message = self.exception if self.exception else 'A tarefa foi cancelada.'
            self.fetchError.emit(error_message)


# ---------------------------------------------------------------------------
#  Tarefa: download de malha territorial
# ---------------------------------------------------------------------------

class DownloadAndLoadLayerTask(QgsTask):
    """Baixa malha do GeoFTP, converte para camada de memória e adiciona ao projeto.

    Fluxo:
    1. ``run()`` (background) — baixa .zip e extrai .shp, armazena o caminho.
    2. ``finished()`` (main thread) — cria QgsVectorLayer, converte para
       memória, limpa temporários e emite sinal de sucesso/erro.

    Sinais emitidos:
    - ``layerReady(QgsVectorLayer)`` — camada temporária pronta.
    - ``downloadError(str)`` — mensagem de erro.
    """

    layerReady = pyqtSignal(QgsVectorLayer)
    downloadError = pyqtSignal(str)

    def __init__(self, url, layer_name):
        super().__init__(f'A baixar malha: {layer_name}', QgsTask.CanCancel)
        self.url = url
        self.layer_name = layer_name
        self.exception = None
        self.downloader = None
        # Caminho do .shp extraído — preenchido no run(), lido no finished()
        self.shapefile_path = None

    def run(self):
        """Background: baixa e extrai o shapefile (sem criar QgsVectorLayer)."""
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
        """Main thread: cria a camada, converte para memória e limpa disco."""
        if self in active_tasks:
            active_tasks.remove(self)

        if result and self.shapefile_path:
            # Cria camada OGR a partir do .shp no thread principal (seguro para Qt)
            file_layer = load_vector_layer(self.shapefile_path, self.layer_name)
            # Converte para memória para desvincular dos arquivos temporários
            mem_layer = (
                file_layer_to_memory(file_layer, self.layer_name)
                if file_layer.isValid()
                else None
            )

            # Libera explicitamente a referência OGR antes de limpar os
            # ficheiros temporários — no Windows o provider mantém um lock
            # no .shp que impede shutil.rmtree de apagar a pasta.
            del file_layer

            # Agora é seguro apagar os ficheiros temporários
            if self.downloader:
                self.downloader.cleanup()

            if mem_layer:
                add_layer_to_project(mem_layer)
                self.layerReady.emit(mem_layer)
            else:
                self.downloadError.emit(
                    "Falha ao converter a malha para camada temporária."
                )
        else:
            if self.downloader:
                self.downloader.cleanup()
            error_message = f"Falha ao baixar/carregar a malha: {self.exception}"
            if self.isCanceled():
                error_message = "Download da malha cancelado."
            self.downloadError.emit(error_message)


# ---------------------------------------------------------------------------
#  Funções de conveniência para lançar tarefas
# ---------------------------------------------------------------------------

def run_fetch_task(url, on_success, on_error):
    """Cria e agenda uma tarefa de busca de dados SIDRA.

    :param url: URL completa da API SIDRA.
    :param on_success: Callback ``f(dict, dict)`` chamado com (dados, header).
    :param on_error: Callback ``f(str)`` chamado com mensagem de erro.
    """
    task = FetchSidraDataTask(url)
    task.dataReady.connect(on_success)
    task.fetchError.connect(on_error)
    active_tasks.append(task)
    QgsApplication.taskManager().addTask(task)


def run_download_task(url, layer_name, on_success, on_error):
    """Cria e agenda uma tarefa de download de malha territorial.

    :param url: URL do .zip no GeoFTP do IBGE.
    :param layer_name: Nome de exibição para a camada resultante.
    :param on_success: Callback ``f(QgsVectorLayer)``.
    :param on_error: Callback ``f(str)``.
    """
    task = DownloadAndLoadLayerTask(url, layer_name)
    task.layerReady.connect(on_success)
    task.downloadError.connect(on_error)
    active_tasks.append(task)
    QgsApplication.taskManager().addTask(task)
