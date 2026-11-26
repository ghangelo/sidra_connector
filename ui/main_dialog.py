# -*- coding: utf-8 -*-
"""
Tela principal do SIDRA Connector.

Junta tudo num lugar so: baixar malha, montar consulta e fazer o join.
A interface vem do .ui (Qt Designer) e o resto eh montado por codigo.
"""

import datetime
import os
import re

from qgis.core import Qgis
from qgis.PyQt import QtWidgets, uic

from ..core.data_joiner import DataJoiner
from ..core.mesh_downloader import build_mesh_url, fetch_available_years  # noqa: F401
from ..core.sidra_api_client import VALID_IBGE_UF_CODES, apply_uf_filter_to_url
from ..gis import layer_manager, task_manager
from ..utils import constants
from .query_builder_dialog import QueryBuilderDialog

FORM_CLASS, _ = uic.loadUiType(os.path.join(
    os.path.dirname(__file__), "main_dialog_base.ui"))

# Qt5 vs Qt6
try:
    DIALOG_ACCEPTED = QtWidgets.QDialog.DialogCode.Accepted
except AttributeError:
    DIALOG_ACCEPTED = QtWidgets.QDialog.Accepted

_RG_MESH_LABELS = {
    "Regiões Geográficas Imediatas",
    "Regiões Geográficas Intermediárias",
}

_PREFERRED_JOIN_FIELDS = (
    "cd_mun",
    "cd_geocmu",
    "cd_uf",
    "cd_rgi",
    "cd_rgint",
    "cd_regia",
    "cd_meso",
    "cd_micro",
    "cd_geocod",
    "cod_ibge",
    "codmun",
    "geocodigo",
)


def _extract_target_layer_uf_codes(layer, join_field):
    """Infere códigos de UF (2 dígitos) a partir de CD_UF ou do campo de união."""
    if not layer:
        return []

    field_names = [f.name() for f in layer.fields()]
    uf_field = next((name for name in field_names if name.lower() == "cd_uf"), None)

    uf_codes = set()
    for feat in layer.getFeatures():
        if uf_field:
            raw_uf = feat[uf_field]
            if raw_uf is not None:
                uf_str = re.sub(r"\.0+$", "", str(raw_uf).strip())
                if uf_str in VALID_IBGE_UF_CODES:
                    uf_codes.add(uf_str)
                    continue

        if join_field:
            raw_val = feat[join_field]
            if raw_val is not None:
                val_str = re.sub(r"\.0+$", "", str(raw_val).strip())
                if val_str.isdigit() and len(val_str) >= 4:
                    prefix = val_str[:2]
                    if prefix in VALID_IBGE_UF_CODES:
                        uf_codes.add(prefix)

    return sorted(uf_codes)


class SidraConnectorDialog(QtWidgets.QDialog, FORM_CLASS):
    """Janela principal -- de onde o usuario controla tudo."""

    def __init__(self, iface, plugin_dir, parent=None):
        super(SidraConnectorDialog, self).__init__(parent)
        self.setupUi(self)

        self.iface = iface
        self.plugin_dir = plugin_dir
        self._pending_target_layer = None
        self._pending_join_field = None

        self.btn_query_builder = QtWidgets.QPushButton("Buscar Tabela...")
        self.verticalLayout_2.insertWidget(0, self.btn_query_builder)
        self.btn_query_builder.clicked.connect(self.open_query_builder)

        self.lbl_query_status = QtWidgets.QLabel("")
        self.lbl_query_status.setWordWrap(True)
        self.verticalLayout_2.insertWidget(1, self.lbl_query_status)

        self.chk_manual_url.toggled.connect(self._toggle_manual_url)
        self._toggle_manual_url(False)

        self.cb_ano_malha.currentIndexChanged.connect(self.update_tipo_malha_options)
        self.cb_localidade_malha.currentIndexChanged.connect(self.update_tipo_malha_options)
        self.cb_target_layer.aboutToShowPopup.connect(self.populate_layers_combobox)
        self.cb_target_layer.currentIndexChanged.connect(self.on_layer_selection_changed)
        self.btn_download_malha.clicked.connect(self.handle_download_mesh)
        self.btn_fetch_join.clicked.connect(self.handle_fetch_and_join)

        self.populate_malha_comboboxes()
        self.populate_layers_combobox()
        self.on_layer_selection_changed()

    def _toggle_manual_url(self, checked):
        self.label.setVisible(bool(checked))
        self.le_api_url.setVisible(bool(checked))

    def populate_malha_comboboxes(self):
        """Preenche os dropdowns imediatamente sem bloquear a UI e atualiza anos em background."""
        current_year = min(datetime.datetime.now().year, 2025)
        fallback_years = [
            str(y) for y in range(current_year, 1999, -1)
            if y not in {2005, 2007}
        ]

        self.cb_ano_malha.blockSignals(True)
        self.cb_localidade_malha.blockSignals(True)

        self.cb_ano_malha.addItems(fallback_years)
        self.cb_localidade_malha.addItems(constants.UFS.keys())

        self.cb_ano_malha.blockSignals(False)
        self.cb_localidade_malha.blockSignals(False)

        self.update_tipo_malha_options()
        task_manager.run_years_task(self._on_years_fetched)

    def _on_years_fetched(self, years):
        """Atualiza o dropdown de anos quando a consulta em background retorna."""
        try:
            current_text = self.cb_ano_malha.currentText()
            self.cb_ano_malha.blockSignals(True)
            self.cb_ano_malha.clear()
            self.cb_ano_malha.addItems(years)
            idx = self.cb_ano_malha.findText(current_text)
            if idx != -1:
                self.cb_ano_malha.setCurrentIndex(idx)
            self.cb_ano_malha.blockSignals(False)
            self.update_tipo_malha_options()
        except RuntimeError:
            pass

    def update_tipo_malha_options(self):
        """Filtra os tipos de malha válidos conforme a localidade e o ano escolhidos."""
        localidade = self.cb_localidade_malha.currentText()
        ano_str = self.cb_ano_malha.currentText()
        ano = int(ano_str) if ano_str.isdigit() else 2024
        previous = self.cb_tipo_malha.currentText()

        valid_types = []
        for label in constants.MALHAS.keys():
            if localidade != "Brasil" and label in constants.MALHAS_BRASIL_ONLY:
                continue
            if ano < 2019 and label in _RG_MESH_LABELS:
                continue
            if ano <= 2014 and localidade == "Brasil":
                continue
            valid_types.append(label)

        self.cb_tipo_malha.blockSignals(True)
        self.cb_tipo_malha.clear()
        self.cb_tipo_malha.addItems(valid_types)
        idx = self.cb_tipo_malha.findText(previous)
        if idx != -1:
            self.cb_tipo_malha.setCurrentIndex(idx)
        self.cb_tipo_malha.blockSignals(False)

    def populate_layers_combobox(self):
        """Atualiza a lista de camadas do projeto no dropdown."""
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
        """Lista os campos da camada selecionada e pré-seleciona o código IBGE padrão."""
        self.cb_target_field.clear()
        layer = self.cb_target_layer.currentData()
        if not layer:
            return

        fields = layer_manager.get_layer_fields(layer)
        self.cb_target_field.addItems(fields)

        lower_map = {f.lower(): idx for idx, f in enumerate(fields)}
        for preferred in _PREFERRED_JOIN_FIELDS:
            if preferred in lower_map:
                self.cb_target_field.setCurrentIndex(lower_map[preferred])
                break

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

        if not ano or not localidade_nome or not malha_nome:
            self.iface.messageBar().pushMessage(
                "Atenção",
                "Selecione ano, localidade e tipo de malha válidos.",
                level=Qgis.MessageLevel.Warning,
            )
            return

        localidade_sigla = constants.UFS[localidade_nome]
        malha_prefixo = constants.MALHAS[malha_nome]

        try:
            url = build_mesh_url(ano, localidade_sigla, malha_prefixo)
        except ValueError as err:
            self.iface.messageBar().pushMessage(
                "Combinação indisponível",
                str(err),
                level=Qgis.MessageLevel.Warning,
                duration=8,
            )
            return

        layer_name = f"{malha_prefixo}_{localidade_nome}_{ano}".replace(" ", "_")

        self.iface.messageBar().pushMessage(
            "SIDRA",
            f"Baixando a malha {layer_name}...",
            level=Qgis.MessageLevel.Info,
            duration=5,
        )
        task_manager.run_download_task(url, layer_name, self.on_download_success, self.on_download_error)

    def on_download_success(self, new_layer):
        """Malha baixada! Ja seleciona ela no dropdown da etapa 3."""
        self.iface.messageBar().pushMessage(
            "Pronto!",
            f"A camada '{new_layer.name()}' já está no projeto.",
            level=Qgis.MessageLevel.Success,
        )
        try:
            self.populate_layers_combobox()
            index = self.cb_target_layer.findData(new_layer)
            if index != -1:
                self.cb_target_layer.setCurrentIndex(index)
        except RuntimeError:
            pass

    def on_download_error(self, error_message):
        self.iface.messageBar().pushMessage(
            "Ops!", error_message, level=Qgis.MessageLevel.Critical, duration=10
        )

    def handle_fetch_and_join(self):
        """Valida tudo, aplica recorte automático por UF se aplicável e dispara o join."""
        api_url = self.le_api_url.text().strip()
        target_layer = self.cb_target_layer.currentData()
        join_field = self.cb_target_field.currentText()

        if not api_url:
            self.iface.messageBar().pushMessage(
                "Atenção",
                "Primeiro monte a consulta clicando em 'Buscar Tabela...'.",
                level=Qgis.MessageLevel.Critical,
            )
            return

        if not api_url.startswith(("http://", "https://")):
            self.iface.messageBar().pushMessage(
                "Atenção",
                "A URL da consulta parece inválida.",
                level=Qgis.MessageLevel.Critical,
            )
            return

        if not target_layer:
            self.iface.messageBar().pushMessage(
                "Atenção",
                "Escolha uma camada na etapa 3.",
                level=Qgis.MessageLevel.Critical,
            )
            return

        if not join_field:
            self.iface.messageBar().pushMessage(
                "Atenção",
                "Escolha o campo que será usado pra unir os dados.",
                level=Qgis.MessageLevel.Critical,
            )
            return

        uf_codes = _extract_target_layer_uf_codes(target_layer, join_field)
        effective_url = apply_uf_filter_to_url(api_url, uf_codes)

        self.iface.messageBar().pushMessage(
            "SIDRA", "Buscando os dados...", level=Qgis.MessageLevel.Info, duration=5
        )

        self.btn_fetch_join.setEnabled(False)
        self._pending_target_layer = target_layer
        self._pending_join_field = join_field
        task_manager.run_fetch_task(effective_url, self.on_fetch_success, self.on_fetch_error)

    def on_fetch_success(self, sidra_data, header_info):
        """Dados chegaram da API -- hora de fazer o join."""
        try:
            self.btn_fetch_join.setEnabled(True)
        except RuntimeError:
            pass

        self.iface.messageBar().pushMessage(
            "SIDRA", "Dados recebidos! Fazendo a união...", level=Qgis.MessageLevel.Info
        )

        if not isinstance(sidra_data, dict):
            self.iface.messageBar().pushMessage(
                "Ops!",
                "A API não devolveu dados válidos. Tenta montar a consulta de novo.",
                level=Qgis.MessageLevel.Critical,
                duration=10,
            )
            return

        if len(sidra_data) == 0:
            self.iface.messageBar().pushMessage(
                "Hmm",
                "A API retornou vazio. Confere se os parâmetros da consulta estão certos.",
                level=Qgis.MessageLevel.Warning,
                duration=10,
            )
            return

        target_layer = self._pending_target_layer
        join_field = self._pending_join_field

        try:
            joiner = DataJoiner(target_layer, join_field, sidra_data, header_info)
            new_layer, join_count, unmatched, layer_keys = joiner.join_data()

            layer_manager.add_layer_to_project(new_layer)

            if join_count > 0:
                self.iface.messageBar().pushMessage(
                    "Pronto!",
                    f"Nova camada criada com {join_count} feições unidas!",
                    Qgis.MessageLevel.Success,
                )
            else:
                sidra_keys_sample = list(sidra_data.keys())[:5] if sidra_data else []
                self.iface.messageBar().pushMessage(
                    "Nenhuma correspondência",
                    f"Os códigos não bateram. "
                    f"Na camada: {layer_keys}. "
                    f"No SIDRA: {sidra_keys_sample}.",
                    level=Qgis.MessageLevel.Warning,
                    duration=20,
                )
        except ValueError as e:
            self.iface.messageBar().pushMessage("Ops!", f"Problema nos dados: {e}", Qgis.MessageLevel.Critical)
        except TypeError as e:
            self.iface.messageBar().pushMessage("Ops!", f"Erro no processamento: {e}", Qgis.MessageLevel.Critical)
        except Exception as e:
            self.iface.messageBar().pushMessage("Ops!", f"Algo deu errado: {e}", Qgis.MessageLevel.Critical)

    def on_fetch_error(self, error_message):
        try:
            self.btn_fetch_join.setEnabled(True)
        except RuntimeError:
            pass
        self.iface.messageBar().pushMessage(
            "Ops!", f"Não consegui buscar os dados: {error_message}",
            level=Qgis.MessageLevel.Critical, duration=10,
        )

    def open_query_builder(self):
        """Abre o assistente de busca e guarda a URL gerada."""
        dialog = QueryBuilderDialog(self.plugin_dir, self)

        if dialog.exec() == DIALOG_ACCEPTED:
            generated_url = dialog.get_generated_url()
            if generated_url:
                self.le_api_url.setText(generated_url)
                self.lbl_query_status.setText(
                    "Consulta pronta! Agora é só clicar em 'Unir à Camada'."
                )
                self.lbl_query_status.setStyleSheet(
                    "color: #2e7d32; font-weight: bold; padding: 4px;"
                )
                self.iface.messageBar().pushMessage(
                    "SIDRA",
                    "Tudo certo! Consulta montada.",
                    level=Qgis.MessageLevel.Info,
                    duration=5,
                )
