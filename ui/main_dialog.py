# -*- coding: utf-8 -*-
"""
Tela principal do SIDRA Connector.

Junta tudo num lugar so: baixar malha, montar consulta e fazer o join.
A interface vem do .ui (Qt Designer) e o resto eh montado por codigo.
"""

import os
import datetime

from qgis.core import Qgis, QgsMessageLog
from qgis.PyQt import QtWidgets, uic

from .query_builder_dialog import QueryBuilderDialog
from ..core.mesh_downloader import fetch_available_years

FORM_CLASS, _ = uic.loadUiType(os.path.join(
    os.path.dirname(__file__), 'main_dialog_base.ui'))
from ..gis import layer_manager, task_manager
from ..core.data_joiner import DataJoiner
from ..utils import constants

# Qt5 vs Qt6
try:
    DIALOG_ACCEPTED = QtWidgets.QDialog.DialogCode.Accepted
except AttributeError:
    DIALOG_ACCEPTED = QtWidgets.QDialog.Accepted


class SidraConnectorDialog(QtWidgets.QDialog, FORM_CLASS):
    """Janela principal -- de onde o usuario controla tudo."""

    def __init__(self, iface, plugin_dir, parent=None):
        super(SidraConnectorDialog, self).__init__(parent)
        self.setupUi(self)

        self.iface = iface
        self.plugin_dir = plugin_dir

        # Botao "Buscar Tabela" e o indicador de status ficam
        # dentro do grupo "2. Montar Consulta" (verticalLayout_2)
        self.btn_query_builder = QtWidgets.QPushButton("Buscar Tabela...")
        self.verticalLayout_2.addWidget(self.btn_query_builder)
        self.btn_query_builder.clicked.connect(self.open_query_builder)

        # Label que mostra se a consulta ta pronta ou nao
        self.lbl_query_status = QtWidgets.QLabel("")
        self.lbl_query_status.setWordWrap(True)
        self.verticalLayout_2.addWidget(self.lbl_query_status)

        # Quando abre o dropdown de camadas, recarrega a lista
        self.cb_target_layer.aboutToShowPopup.connect(self.populate_layers_combobox)
        self.cb_target_layer.currentIndexChanged.connect(self.on_layer_selection_changed)
        self.btn_download_malha.clicked.connect(self.handle_download_mesh)
        self.btn_fetch_join.clicked.connect(self.handle_fetch_and_join)

        self.populate_malha_comboboxes()
        self.populate_layers_combobox()
        self.on_layer_selection_changed()

    def populate_malha_comboboxes(self):
        """Preenche os dropdowns de ano, UF e tipo de malha.

        Tenta pegar os anos disponiveis do site do IBGE.
        Se nao conseguir (sem internet), usa uma lista fixa.
        """
        try:
            anos = fetch_available_years()
        except (ConnectionError, ValueError) as e:
            QgsMessageLog.logMessage(
                f"Nao conseguiu pegar anos do IBGE, usando lista fixa: {e}",
                "SIDRA Connector", Qgis.Warning,
            )
            current_year = datetime.datetime.now().year
            anos = [str(y) for y in range(current_year, 1999, -1)]

        self.cb_ano_malha.addItems(anos)
        self.cb_localidade_malha.addItems(constants.UFS.keys())
        self.cb_tipo_malha.addItems(constants.MALHAS.keys())

    def populate_layers_combobox(self):
        """Atualiza a lista de camadas do projeto no dropdown.

        Tenta manter a selecao anterior se ela ainda existir.
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
        """Lista os campos da camada selecionada."""
        self.cb_target_field.clear()
        layer = self.cb_target_layer.currentData()
        if layer:
            self.cb_target_field.addItems(layer_manager.get_layer_fields(layer))

    def on_layer_selection_changed(self):
        """Habilita/desabilita o botao de join conforme tem camada ou nao."""
        layer_is_selected = self.cb_target_layer.currentData() is not None
        self.btn_fetch_join.setEnabled(layer_is_selected)
        self.populate_fields_combobox()

    def handle_download_mesh(self):
        """Monta a URL da malha no GeoFTP e dispara o download."""
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

        self.iface.messageBar().pushMessage("SIDRA", f"Baixando a malha {layer_name}...", level=Qgis.Info, duration=5)
        task_manager.run_download_task(url, layer_name, self.on_download_success, self.on_download_error)

    def on_download_success(self, new_layer):
        """Malha baixada! Ja seleciona ela no dropdown da etapa 3."""
        self.iface.messageBar().pushMessage(
            "Pronto!",
            f"A camada '{new_layer.name()}' já está no projeto.",
            level=Qgis.Success,
        )
        self.populate_layers_combobox()
        index = self.cb_target_layer.findData(new_layer)
        if index != -1:
            self.cb_target_layer.setCurrentIndex(index)

    def on_download_error(self, error_message):
        """Deu ruim no download."""
        self.iface.messageBar().pushMessage(
            "Ops!", error_message, level=Qgis.Critical, duration=10
        )

    def handle_fetch_and_join(self):
        """Valida tudo e dispara a busca de dados + join."""
        api_url = self.le_api_url.text().strip()
        target_layer = self.cb_target_layer.currentData()
        join_field = self.cb_target_field.currentText()

        if not api_url:
            self.iface.messageBar().pushMessage("Atenção", "Primeiro monte a consulta clicando em 'Buscar Tabela'.", level=Qgis.Critical)
            return

        if not api_url.startswith(('http://', 'https://')):
            self.iface.messageBar().pushMessage("Atenção", "A URL da consulta parece inválida.", level=Qgis.Critical)
            return

        if not target_layer:
            self.iface.messageBar().pushMessage("Atenção", "Escolha uma camada na etapa 3.", level=Qgis.Critical)
            return

        if not join_field:
            self.iface.messageBar().pushMessage("Atenção", "Escolha o campo que será usado pra unir os dados.", level=Qgis.Critical)
            return

        self.iface.messageBar().pushMessage("SIDRA", "Buscando os dados...", level=Qgis.Info, duration=5)

        # Desabilita pra evitar clique duplo
        self.btn_fetch_join.setEnabled(False)

        # Guarda a camada e o campo que tavam selecionados AGORA,
        # porque o usuario pode mudar a selecao enquanto espera
        self._pending_target_layer = target_layer
        self._pending_join_field = join_field
        task_manager.run_fetch_task(api_url, self.on_fetch_success, self.on_fetch_error)

    def on_fetch_success(self, sidra_data, header_info):
        """Dados chegaram da API -- hora de fazer o join."""
        self.btn_fetch_join.setEnabled(True)
        self.iface.messageBar().pushMessage("SIDRA", "Dados recebidos! Fazendo a união...", level=Qgis.Info)

        if not sidra_data or not isinstance(sidra_data, dict):
            self.iface.messageBar().pushMessage(
                "Ops!",
                "A API não devolveu dados válidos. Tenta montar a consulta de novo.",
                level=Qgis.Critical,
                duration=10
            )
            return

        if len(sidra_data) == 0:
            self.iface.messageBar().pushMessage(
                "Hmm",
                "A API retornou vazio. Confere se os parâmetros da consulta estão certos.",
                level=Qgis.Warning,
                duration=10
            )
            return

        # Usa a camada/campo que tavam selecionados quando clicou
        target_layer = self._pending_target_layer
        join_field = self._pending_join_field

        try:
            joiner = DataJoiner(target_layer, join_field, sidra_data, header_info)
            new_layer, join_count, unmatched, layer_keys = joiner.join_data()

            layer_manager.add_layer_to_project(new_layer)

            if join_count > 0:
                self.iface.messageBar().pushMessage("Pronto!", f"Nova camada criada com {join_count} feições unidas!", Qgis.Success)
            else:
                sidra_keys_sample = list(sidra_data.keys())[:5] if sidra_data else []
                self.iface.messageBar().pushMessage(
                    "Nenhuma correspondência",
                    f"Os códigos não bateram. "
                    f"Na camada: {layer_keys}. "
                    f"No SIDRA: {sidra_keys_sample}.",
                    level=Qgis.Warning,
                    duration=20
                )
        except ValueError as e:
            self.iface.messageBar().pushMessage("Ops!", f"Problema nos dados: {e}", Qgis.Critical)
        except TypeError as e:
            self.iface.messageBar().pushMessage("Ops!", f"Erro no processamento: {e}", Qgis.Critical)
        except Exception as e:
            self.iface.messageBar().pushMessage("Ops!", f"Algo deu errado: {e}", Qgis.Critical)

    def on_fetch_error(self, error_message):
        """Deu erro na chamada da API."""
        self.btn_fetch_join.setEnabled(True)
        self.iface.messageBar().pushMessage(
            "Ops!", f"Não consegui buscar os dados: {error_message}",
            level=Qgis.Critical, duration=10,
        )

    def open_query_builder(self):
        """Abre o assistente de busca e guarda a URL gerada."""
        dialog = QueryBuilderDialog(self.plugin_dir, self)

        if dialog.exec() == DIALOG_ACCEPTED:
            generated_url = dialog.get_generated_url()
            if generated_url:
                self.le_api_url.setText(generated_url)
                self.lbl_query_status.setText(
                    "Consulta pronta! Agora é só clicar em 'Unir à Camada Alvo'."
                )
                self.lbl_query_status.setStyleSheet(
                    "color: #2e7d32; font-weight: bold; padding: 4px;"
                )
                self.iface.messageBar().pushMessage(
                    "SIDRA",
                    "Tudo certo! Consulta montada.",
                    level=Qgis.Info,
                    duration=5,
                )
