# -*- coding: utf-8 -*-
"""
Diálogo principal do SIDRA Connector.

Esta janela condensa os três fluxos do plugin:

1. **Download de malha** — seleciona UF, tipo e ano; baixa do GeoFTP
   do IBGE via ``task_manager.run_download_task``.
2. **Busca de tabela** — abre o ``QueryBuilderDialog`` para montar a
   URL da API SIDRA interativamente.
3. **Busca e união** — consulta a API com a URL gerada e une os dados
   tabulares à camada vetorial selecionada (``DataJoiner``).

A UI é carregada dinamicamente a partir do ``.ui`` (compatível Qt5/Qt6).
"""

import os
import datetime

from qgis.core import Qgis, QgsMessageLog
from qgis.PyQt import QtWidgets, uic

from .query_builder_dialog import QueryBuilderDialog
from ..core.mesh_downloader import fetch_available_years

# Carrega a classe do formulário diretamente do .ui (sem pyuic5/pyuic6)
FORM_CLASS, _ = uic.loadUiType(os.path.join(
    os.path.dirname(__file__), 'main_dialog_base.ui'))
from ..gis import layer_manager, task_manager
from ..core.data_joiner import DataJoiner
from ..utils import constants

# Compatibilidade Qt5 (QGIS ≤ 3.38) / Qt6 (QGIS 3.40+).
try:
    DIALOG_ACCEPTED = QtWidgets.QDialog.DialogCode.Accepted
except AttributeError:
    DIALOG_ACCEPTED = QtWidgets.QDialog.Accepted


class SidraConnectorDialog(QtWidgets.QDialog, FORM_CLASS):
    """Janela principal do plugin — orquestra downloads, consultas e uniões.

    Herda de ``FORM_CLASS`` (gerado em runtime pelo ``uic.loadUiType``)
    para ter acesso direto aos widgets definidos no ``.ui``.
    """

    def __init__(self, iface, plugin_dir, parent=None):
        """Inicializa a janela, conecta sinais e popula comboboxes.

        :param iface: Referência à interface do QGIS (``QgisInterface``).
        :param plugin_dir: Caminho absoluto da pasta do plugin.
        :param parent: Widget pai (opcional).
        """
        super(SidraConnectorDialog, self).__init__(parent)
        self.setupUi(self)
        
        self.iface = iface
        self.plugin_dir = plugin_dir

        # Botão criado programaticamente (não existe no .ui) para abrir
        # o assistente de busca de tabelas SIDRA.
        self.btn_query_builder = QtWidgets.QPushButton("Buscar Tabela...")
        self.verticalLayout_2.addWidget(self.btn_query_builder)
        self.btn_query_builder.clicked.connect(self.open_query_builder)

        # Indicador visual de que a consulta está pronta
        self.lbl_query_status = QtWidgets.QLabel("")
        self.lbl_query_status.setWordWrap(True)
        self.verticalLayout_2.addWidget(self.lbl_query_status)

        # --- Sinais ---
        # Atualiza a lista de camadas sempre que o dropdown é aberto
        self.cb_target_layer.aboutToShowPopup.connect(self.populate_layers_combobox)
        # Habilita/desabilita controles dependentes da seleção de camada
        self.cb_target_layer.currentIndexChanged.connect(self.on_layer_selection_changed)
        self.btn_download_malha.clicked.connect(self.handle_download_mesh)
        self.btn_fetch_join.clicked.connect(self.handle_fetch_and_join)

        # Estado inicial
        self.populate_malha_comboboxes()
        self.populate_layers_combobox()
        self.on_layer_selection_changed()

    def populate_malha_comboboxes(self):
        """Popula as comboboxes de ano, UF e tipo de malha.

        Tenta obter os anos disponíveis dinamicamente do GeoFTP do IBGE.
        Em caso de falha de rede, usa uma lista estática como fallback.
        """
        try:
            anos = fetch_available_years()
        except (ConnectionError, ValueError) as e:
            QgsMessageLog.logMessage(
                f"Não foi possível obter anos do IBGE, usando lista estática: {e}",
                "SIDRA Connector", Qgis.Warning,
            )
            current_year = datetime.datetime.now().year
            anos = [str(y) for y in range(current_year, 1999, -1)]

        self.cb_ano_malha.addItems(anos)
        self.cb_localidade_malha.addItems(constants.UFS.keys())
        self.cb_tipo_malha.addItems(constants.MALHAS.keys())

    def populate_layers_combobox(self):
        """Recarrega a combobox de camadas vetoriais do projeto.

        Preserva a seleção atual (se ainda existir) e bloqueia sinais
        durante a atualização para evitar callbacks recursivos.
        """
        self.cb_target_layer.blockSignals(True)
        current_layer = self.cb_target_layer.currentData()
        self.cb_target_layer.clear()
        
        self.cb_target_layer.addItem("Selecione uma camada...", None)
        
        vector_layers = layer_manager.get_project_vector_layers()
        for layer in vector_layers:
            self.cb_target_layer.addItem(layer.name(), layer)
        
        index = self.cb_target_layer.findData(current_layer)
        if index != -1:
            self.cb_target_layer.setCurrentIndex(index)
        else:
            self.cb_target_layer.setCurrentIndex(0)
            
        self.cb_target_layer.blockSignals(False)
        self.on_layer_selection_changed()

    def populate_fields_combobox(self):
        """Lista os campos da camada alvo na combobox de campo de união."""
        self.cb_target_field.clear()
        layer = self.cb_target_layer.currentData()
        if layer:
            self.cb_target_field.addItems(layer_manager.get_layer_fields(layer))

    def on_layer_selection_changed(self):
        """Habilita/desabilita controles conforme a seleção de camada."""
        layer_is_selected = self.cb_target_layer.currentData() is not None
        self.btn_fetch_join.setEnabled(layer_is_selected)
        self.populate_fields_combobox()

    def handle_download_mesh(self):
        """Monta a URL do GeoFTP e agenda o download assíncrono da malha."""
        ano = self.cb_ano_malha.currentText()
        localidade_nome = self.cb_localidade_malha.currentText()
        malha_nome = self.cb_tipo_malha.currentText()
        
        localidade_sigla = constants.UFS[localidade_nome]
        malha_prefixo = constants.MALHAS[malha_nome]

        if localidade_sigla == "BR":
            url_path = f"Brasil/{localidade_sigla}_{malha_prefixo}_{ano}.zip"
        else:
            url_path = f"UFs/{localidade_sigla}/{localidade_sigla}_{malha_prefixo}_{ano}.zip"

        base_url = constants.IBGE_MESH_BASE_URL.format(ano=ano)
        url = base_url + url_path
        layer_name = f"{malha_prefixo}_{localidade_nome}_{ano}".replace(" ", "_")

        self.iface.messageBar().pushMessage("Download", f"Iniciando download da malha: {layer_name}", level=Qgis.Info, duration=5)
        task_manager.run_download_task(url, layer_name, self.on_download_success, self.on_download_error)

    def on_download_success(self, new_layer):
        """Callback: malha baixada e carregada com sucesso.

        Alem de notificar o usuario, seleciona automaticamente a
        camada recem baixada na etapa 3, deixando-a pronta para o join.
        """
        self.iface.messageBar().pushMessage(
            "Sucesso",
            f"Camada '{new_layer.name()}' carregada com sucesso!",
            level=Qgis.Success,
        )
        # Recarrega o combobox e seleciona a camada recem adicionada
        self.populate_layers_combobox()
        index = self.cb_target_layer.findData(new_layer)
        if index != -1:
            self.cb_target_layer.setCurrentIndex(index)

    def on_download_error(self, error_message):
        """Callback: falha no download da malha."""
        self.iface.messageBar().pushMessage(
            "Erro no Download", error_message, level=Qgis.Critical, duration=10
        )

    def handle_fetch_and_join(self):
        """Valida os inputs e agenda a busca assíncrona de dados SIDRA."""
        api_url = self.le_api_url.text().strip()
        target_layer = self.cb_target_layer.currentData()
        join_field = self.cb_target_field.currentText()

        # Validações de entrada
        if not api_url:
            self.iface.messageBar().pushMessage("Erro", "URL da API deve ser preenchida.", level=Qgis.Critical)
            return
            
        if not api_url.startswith(('http://', 'https://')):
            self.iface.messageBar().pushMessage("Erro", "URL deve começar com http:// ou https://", level=Qgis.Critical)
            return
            
        if not target_layer:
            self.iface.messageBar().pushMessage("Erro", "Camada alvo deve ser selecionada.", level=Qgis.Critical)
            return
            
        if not join_field:
            self.iface.messageBar().pushMessage("Erro", "Campo de união deve ser selecionado.", level=Qgis.Critical)
            return

        self.iface.messageBar().pushMessage("SIDRA Connector", "Buscando dados na API...", level=Qgis.Info, duration=5)

        # Desabilita o botão para evitar race condition caso o usuário
        # clique novamente antes de a callback retornar.
        self.btn_fetch_join.setEnabled(False)

        # Captura referências antes do dispatch assíncrono para que a
        # callback use a camada/campo que estavam selecionados no momento
        # do clique — mesmo que o usuário altere a seleção enquanto espera.
        self._pending_target_layer = target_layer
        self._pending_join_field = join_field
        task_manager.run_fetch_task(api_url, self.on_fetch_success, self.on_fetch_error)

    def on_fetch_success(self, sidra_data, header_info):
        """Callback: dados SIDRA recebidos — valida e executa a união."""
        self.btn_fetch_join.setEnabled(True)
        self.iface.messageBar().pushMessage("SIDRA Connector", "Dados recebidos. Processando e unindo...", level=Qgis.Info)
        
        # Validação adicional dos dados recebidos
        if not sidra_data or not isinstance(sidra_data, dict):
            self.iface.messageBar().pushMessage(
                "Erro", 
                "A API não retornou dados válidos. Verifique se a URL está correta e se contém dados para o período/localização especificados.", 
                level=Qgis.Critical, 
                duration=10
            )
            return
        
        if len(sidra_data) == 0:
            self.iface.messageBar().pushMessage(
                "Aviso", 
                "A API retornou dados vazios. Verifique se a URL da API está correta e se há dados disponíveis para os parâmetros especificados.", 
                level=Qgis.Warning, 
                duration=10
            )
            return
        
        # Usa as referências capturadas no momento do clique (não da UI atual)
        target_layer = self._pending_target_layer
        join_field = self._pending_join_field

        try:
            joiner = DataJoiner(target_layer, join_field, sidra_data, header_info)
            new_layer, join_count, unmatched, layer_keys = joiner.join_data()
            
            layer_manager.add_layer_to_project(new_layer)

            if join_count > 0:
                self.iface.messageBar().pushMessage("Sucesso", f"Cópia da camada criada com {join_count} feições unidas!", Qgis.Success)
            else:
                sidra_keys_sample = list(sidra_data.keys())[:5] if sidra_data else []
                self.iface.messageBar().pushMessage(
                    "Aviso", 
                    f"Nenhuma correspondência encontrada. Verifique o formato dos códigos. "
                    f"Exemplos da sua camada: {layer_keys}. "
                    f"Exemplos dos dados SIDRA: {sidra_keys_sample}.",
                    level=Qgis.Warning, 
                    duration=20
                )
        except ValueError as e:
            self.iface.messageBar().pushMessage("Erro de Validação", f"Dados inválidos: {e}", Qgis.Critical)
        except TypeError as e:
            self.iface.messageBar().pushMessage("Erro de Tipo", f"Erro no processamento dos dados: {e}", Qgis.Critical)
        except Exception as e:
            self.iface.messageBar().pushMessage("Erro", f"Falha no processamento ou união: {e}", Qgis.Critical)

    def on_fetch_error(self, error_message):
        """Callback: falha na requisição à API SIDRA."""
        self.btn_fetch_join.setEnabled(True)
        self.iface.messageBar().pushMessage(
            "Erro na API", f"Ocorreu um erro: {error_message}",
            level=Qgis.Critical, duration=10,
        )

    def open_query_builder(self):
        """Abre o ``QueryBuilderDialog`` e preenche a URL ao aceitar."""
        dialog = QueryBuilderDialog(self.plugin_dir, self)

        if dialog.exec() == DIALOG_ACCEPTED:
            generated_url = dialog.get_generated_url()
            if generated_url:
                self.le_api_url.setText(generated_url)
                # Indicador verde de consulta pronta
                self.lbl_query_status.setText(
                    "Consulta pronta! Clique em 'Unir a Camada Alvo'."
                )
                self.lbl_query_status.setStyleSheet(
                    "color: #2e7d32; font-weight: bold; padding: 4px;"
                )
                self.iface.messageBar().pushMessage(
                    "SIDRA Connector",
                    "Consulta montada com sucesso!",
                    level=Qgis.Info,
                    duration=5,
                )

