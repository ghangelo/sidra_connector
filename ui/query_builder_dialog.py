# -*- coding: utf-8 -*-
"""
Janela do assistente de busca do SIDRA Connector.

Aqui o usuario pesquisa tabelas do SIDRA pelo nome ou ID,
escolhe periodos, nivel geografico, variaveis e categorias,
e no final sai com a URL da API pronta pra consultar.

A pesquisa roda em cima do banco local agregados_ibge.db,
entao nao precisa de internet pra achar a tabela -- so
pra puxar os metadados depois.
"""

import os
import sqlite3
import unicodedata

from qgis.PyQt import QtCore, QtWidgets

from ..core.api_helpers import montar_url_interativa
from ..gis import task_manager

IBGE_SEARCH_ACRONYMS = {
    "pib": "produto interno bruto",
    "vab": "valor adicionado bruto",
    "pam": "producao agricola municipal",
    "ppm": "pesquisa da pecuaria municipal",
    "pevs": "extracao vegetal e da silvicultura",
    "ipca": "indice nacional de precos ao consumidor amplo",
    "inpc": "indice nacional de precos ao consumidor",
    "pnad": "pesquisa nacional por amostra de domicilios",
}


def normalize_search_text(texto):
    """Remove acentos e converte para minúsculas para busca no SQLite."""
    if texto is None:
        return ""
    nfkd = unicodedata.normalize("NFKD", str(texto))
    sem_acento = "".join(c for c in nfkd if not unicodedata.combining(c))
    return sem_acento.lower()


def build_search_query(search_term):
    """Monta a query SQL e parâmetros para busca por ID ou palavras/siglas."""
    term = str(search_term or "").strip()
    if term.isdigit():
        query = """
            SELECT a.id, a.nome, g.nome as grupo_nome
            FROM agregados a
            JOIN grupos g ON a.grupo_id = g.id
            WHERE CAST(a.id AS TEXT) LIKE ?
            ORDER BY a.id
        """
        return query, [f"{term}%"]

    tokens = normalize_search_text(term).split()
    where_parts = []
    params = []
    for tok in tokens:
        expanded = IBGE_SEARCH_ACRONYMS.get(tok)
        if expanded:
            where_parts.append(
                "(norm(a.nome) LIKE ? OR norm(g.nome) LIKE ? OR norm(a.nome) LIKE ? OR norm(g.nome) LIKE ?)"
            )
            params.extend([f"%{tok}%", f"%{tok}%", f"%{expanded}%", f"%{expanded}%"])
        else:
            where_parts.append("(norm(a.nome) LIKE ? OR norm(g.nome) LIKE ?)")
            params.extend([f"%{tok}%", f"%{tok}%"])

    where_clause = " AND ".join(where_parts) if where_parts else "1=1"
    query = (
        "SELECT a.id, a.nome, g.nome as grupo_nome"
        " FROM agregados a"
        " JOIN grupos g ON a.grupo_id = g.id"
        f" WHERE {where_clause} ORDER BY a.nome"
    )
    return query, params


def _format_option_label(item_name, item_id):
    """Mostra o nome e o ID uma única vez."""
    label = str(item_name or "").strip()
    id_text = str(item_id).strip()

    if not id_text:
        return label

    id_markers = (
        f"({id_text})",
        f"(ID: {id_text})",
        f"ID: {id_text}",
        f"id: {id_text}",
    )

    if any(marker.lower() in label.lower() for marker in id_markers):
        return label.strip()

    return f"{label} (ID: {id_text})".strip()


# Qt5 vs Qt6
try:
    USER_ROLE = QtCore.Qt.ItemDataRole.UserRole
except AttributeError:
    USER_ROLE = QtCore.Qt.UserRole

try:
    MULTI_SELECTION = QtWidgets.QAbstractItemView.SelectionMode.MultiSelection
except AttributeError:
    MULTI_SELECTION = QtWidgets.QAbstractItemView.MultiSelection

try:
    DIALOG_ACCEPTED = QtWidgets.QDialog.DialogCode.Accepted
except AttributeError:
    DIALOG_ACCEPTED = QtWidgets.QDialog.Accepted


class QueryBuilderDialog(QtWidgets.QDialog):
    """Assistente pra montar a URL da API SIDRA passo a passo."""

    def __init__(self, plugin_dir, parent=None):
        super(QueryBuilderDialog, self).__init__(parent)
        self.plugin_dir = plugin_dir
        self.generated_url = None
        self.selected_table_id = None
        self._metadata_feedback = None

        self.setup_ui()
        self.connect_signals()

    def setup_ui(self):
        self.setWindowTitle("Buscar Tabela do SIDRA")
        self.setFixedSize(600, 400)

        layout = QtWidgets.QVBoxLayout(self)

        search_group = QtWidgets.QGroupBox("Buscar Tabelas")
        search_layout = QtWidgets.QVBoxLayout(search_group)

        search_input_layout = QtWidgets.QHBoxLayout()
        self.le_search = QtWidgets.QLineEdit()
        self.le_search.setPlaceholderText("Digite o nome ou número da tabela...")
        self.btn_clear = QtWidgets.QPushButton("Limpar")
        self.btn_clear.setMaximumWidth(80)

        search_input_layout.addWidget(self.le_search)
        search_input_layout.addWidget(self.btn_clear)

        search_layout.addLayout(search_input_layout)

        self.lbl_status = QtWidgets.QLabel("")
        self.lbl_status.setStyleSheet("color: gray; font-style: italic;")

        self.list_results = QtWidgets.QListWidget()
        self.list_results.setMaximumHeight(200)

        search_layout.addWidget(self.lbl_status)
        search_layout.addWidget(self.list_results)

        selected_group = QtWidgets.QGroupBox("Tabela Selecionada")
        selected_layout = QtWidgets.QVBoxLayout(selected_group)

        self.lbl_selected_table = QtWidgets.QLabel("Escolha uma tabela na lista acima")
        self.lbl_selected_table.setWordWrap(True)
        self.btn_build_query = QtWidgets.QPushButton("Construir Consulta")
        self.btn_build_query.setEnabled(False)

        selected_layout.addWidget(self.lbl_selected_table)
        selected_layout.addWidget(self.btn_build_query)

        button_layout = QtWidgets.QHBoxLayout()
        self.btn_cancel = QtWidgets.QPushButton("Cancelar")
        self.btn_ok = QtWidgets.QPushButton("OK")
        self.btn_ok.setEnabled(False)

        button_layout.addStretch()
        button_layout.addWidget(self.btn_cancel)
        button_layout.addWidget(self.btn_ok)

        layout.addWidget(search_group)
        layout.addWidget(selected_group)
        layout.addLayout(button_layout)

    def connect_signals(self):
        self.search_timer = QtCore.QTimer()
        self.search_timer.setSingleShot(True)
        self.search_timer.timeout.connect(self.perform_search)

        self.le_search.textChanged.connect(self.on_search_text_changed)
        self.btn_clear.clicked.connect(self.clear_search)
        self.list_results.itemClicked.connect(self.on_table_selected)
        self.btn_build_query.clicked.connect(self.build_query)
        self.btn_cancel.clicked.connect(self.reject)
        self.btn_ok.clicked.connect(self.accept)

    def closeEvent(self, event):
        """Cancela cooperativamente a requisição de metadados sem matar threads à força."""
        if self._metadata_feedback is not None:
            self._metadata_feedback.cancel()
        super().closeEvent(event)

    def reject(self):
        if self._metadata_feedback is not None:
            self._metadata_feedback.cancel()
        super().reject()

    def get_db_connection(self):
        """Abre o banco SQLite com as tabelas do SIDRA."""
        db_path = os.path.join(self.plugin_dir, "agregados_ibge.db")

        if not os.path.exists(db_path):
            QtWidgets.QMessageBox.critical(
                self,
                "Ops!",
                f"Não encontrei o banco de dados em:\n{db_path}\n\n"
                "Confere se o arquivo agregados_ibge.db tá na pasta do plugin."
            )
            return None

        try:
            return sqlite3.connect(db_path)
        except sqlite3.Error as e:
            QtWidgets.QMessageBox.critical(
                self,
                "Ops!",
                f"Não consegui abrir o banco de dados: {e}"
            )
            return None

    def on_search_text_changed(self):
        search_text = self.le_search.text().strip()
        self.search_timer.stop()

        if not search_text:
            self.list_results.clear()
            self.lbl_status.setText("")
            self.lbl_status.setStyleSheet("color: gray; font-style: italic;")
            return

        self.search_timer.start(500)

    def clear_search(self):
        self.le_search.clear()
        self.list_results.clear()
        self.lbl_status.setText("")
        self.lbl_status.setStyleSheet("color: gray; font-style: italic;")

    def perform_search(self):
        search_term = self.le_search.text().strip()
        if not search_term:
            return
        self.lbl_status.setText("Procurando...")
        self.lbl_status.setStyleSheet("color: blue; font-style: italic;")
        self.search_tables(search_term)

    def search_tables(self, search_term=None):
        """Pesquisa no banco local, sem precisar de acento e em qualquer ordem."""
        if search_term is None:
            search_term = self.le_search.text().strip()

        if not search_term:
            return

        conn = self.get_db_connection()
        if not conn:
            return

        try:
            conn.create_function("norm", 1, normalize_search_text)
            cursor = conn.cursor()

            query, params = build_search_query(search_term)
            cursor.execute(query, params)

            results = cursor.fetchall()
            self.list_results.clear()

            if results:
                for table_id, table_name, group_name in results:
                    item = QtWidgets.QListWidgetItem(
                        f"[{table_id}] {table_name} ({group_name})"
                    )
                    item.setData(USER_ROLE, table_id)
                    self.list_results.addItem(item)

                count = len(results)
                self.lbl_status.setText(f"{count} tabela(s) encontrada(s)")
                self.lbl_status.setStyleSheet("color: green;")
            else:
                self.lbl_status.setText("Nenhuma tabela encontrada. Tenta com outras palavras.")
                self.lbl_status.setStyleSheet("color: orange;")

        except sqlite3.Error as e:
            self.lbl_status.setText(f"Erro na busca: {e}")
            self.lbl_status.setStyleSheet("color: red;")
            QtWidgets.QMessageBox.critical(self, "Ops!", f"Deu problema na busca: {e}")
        finally:
            conn.close()

    def on_table_selected(self, item):
        table_id = item.data(USER_ROLE)
        if table_id is None:
            return

        self.selected_table_id = table_id
        self.lbl_selected_table.setText(f"Selecionada: {item.text()}")
        self.btn_build_query.setEnabled(True)

    def build_query(self):
        """Puxa os metadados da tabela escolhida via QgsTask cancelável."""
        if self.selected_table_id is None:
            QtWidgets.QMessageBox.warning(self, "Atenção", "Escolhe uma tabela da lista antes.")
            return

        if self._metadata_feedback is not None:
            self._metadata_feedback.cancel()

        self.btn_build_query.setEnabled(False)
        self.btn_build_query.setText("Carregando detalhes...")

        self._metadata_feedback = task_manager.run_metadata_task(
            str(self.selected_table_id),
            self._on_metadata_ready,
            self._on_metadata_error,
        )

    def _on_metadata_error(self, error_message):
        self._metadata_feedback = None
        try:
            self.btn_build_query.setEnabled(True)
            self.btn_build_query.setText("Construir Consulta")
            QtWidgets.QMessageBox.critical(
                self,
                "Sem conexão",
                f"Não consegui buscar os detalhes dessa tabela.\n{error_message}",
            )
        except RuntimeError:
            pass

    def _on_metadata_ready(self, metadata):
        """Metadados chegaram -- agora percorre as 4 etapas de selecao."""
        self._metadata_feedback = None
        try:
            self.btn_build_query.setEnabled(True)
            self.btn_build_query.setText("Construir Consulta")
        except RuntimeError:
            return

        if not metadata:
            QtWidgets.QMessageBox.critical(
                self,
                "Sem conexão",
                "Não consegui buscar os detalhes dessa tabela. Verifica se tá conectado à internet."
            )
            return

        try:
            # 1) Periodos
            periodos_disponiveis = [
                (p.get("Id"), p.get("Nome"), p.get("Codigo"))
                for p in metadata.get("Periodos", {}).get("Periodos", [])
            ]
            periodos_selecionados = self.show_selection_dialog(
                "Selecione o(s) Periodo(s)",
                periodos_disponiveis,
            )
            if not periodos_selecionados:
                return

            # 2) Nivel geografico
            territorios = metadata.get("Territorios", {})
            dic_niveis = territorios.get("DicionarioNiveis", {})
            nivel_nome_map = dict(zip(
                dic_niveis.get("Ids", []),
                dic_niveis.get("Nomes", []),
            ))
            niveis_disponiveis = [
                (n.get("Id"), nivel_nome_map.get(n.get("Id"), n.get("Sigla", "?")), n.get("Sigla", ""))
                for n in territorios.get("NiveisTabela", [])
            ]
            nivel_selecionado = self.show_selection_dialog(
                "Selecione o Nivel Geografico",
                niveis_disponiveis,
                single_selection=True,
            )
            if not nivel_selecionado:
                return

            # 3) Variaveis
            variaveis_disponiveis = []
            for var in metadata.get("Variaveis", []):
                unidade = ""
                if isinstance(var.get("UnidadeDeMedida"), list):
                    if var.get("UnidadeDeMedida"):
                        unidade = var.get("UnidadeDeMedida")[0].get("Unidade", "")
                else:
                    unidade = var.get("UnidadeDeMedida", "")

                variaveis_disponiveis.append((var.get("Id"), var.get("Nome"), unidade))

                for derivada in var.get("VariaveisDerivadas", []):
                    unidade_derivada = derivada.get("UnidadeDeMedida", "")
                    variaveis_disponiveis.append(
                        (derivada.get("Id"), f"  \u2514\u2500 {derivada.get('Nome')}", unidade_derivada)
                    )

            variaveis_selecionadas = self.show_selection_dialog(
                "Selecione a(s) Variavel(is)",
                variaveis_disponiveis,
            )
            if not variaveis_selecionadas:
                return

            # 4) Categorias por classificacao (se houver)
            classificacoes_selecionadas = {}
            for classif in metadata.get("Classificacoes", []):
                class_id = classif.get("Id")
                class_nome = classif.get("Nome")

                categorias_disponiveis = []
                for cat in classif.get("Categorias", []):
                    indentacao = "  " * cat.get("IdentacaoApresentacao", 0)
                    categorias_disponiveis.append(
                        (cat.get("Id"), f"{indentacao}{cat.get('Nome')}")
                    )

                if categorias_disponiveis:
                    categorias_selecionadas = self.show_selection_dialog(
                        f"Selecione categorias para: {class_nome}",
                        categorias_disponiveis,
                    )
                    if categorias_selecionadas:
                        classificacoes_selecionadas[class_id] = [item[0] for item in categorias_selecionadas]

            self.generated_url = montar_url_interativa(
                self.selected_table_id,
                nivel_selecionado[0],
                variaveis_selecionadas,
                periodos_selecionados,
                classificacoes_selecionadas,
            )

            self.accept()

        except Exception as e:
            QtWidgets.QMessageBox.critical(
                self,
                "Ops!",
                f"Algo deu errado ao montar a consulta: {e}"
            )

    def show_selection_dialog(self, title, options, single_selection=False):
        dialog = QtWidgets.QDialog(self)
        dialog.setWindowTitle(title)
        dialog.setFixedSize(500, 400)

        layout = QtWidgets.QVBoxLayout(dialog)
        list_widget = QtWidgets.QListWidget()

        for option in options:
            item_id, item_name = option[0], option[1]
            info_extra = f" ({option[2]})" if len(option) > 2 and option[2] else ""

            label = _format_option_label(item_name, item_id)
            item = QtWidgets.QListWidgetItem(f"{label}{info_extra}")
            item.setData(USER_ROLE, option)
            list_widget.addItem(item)

        if not single_selection:
            list_widget.setSelectionMode(MULTI_SELECTION)

        layout.addWidget(list_widget)

        button_layout = QtWidgets.QHBoxLayout()
        btn_ok = QtWidgets.QPushButton("OK")
        btn_ok.setEnabled(False)
        btn_cancel = QtWidgets.QPushButton("Cancelar")

        button_layout.addStretch()
        button_layout.addWidget(btn_cancel)
        button_layout.addWidget(btn_ok)

        layout.addLayout(button_layout)

        def _on_selection_changed():
            btn_ok.setEnabled(len(list_widget.selectedItems()) > 0)

        list_widget.itemSelectionChanged.connect(_on_selection_changed)

        btn_ok.clicked.connect(dialog.accept)
        btn_cancel.clicked.connect(dialog.reject)

        if dialog.exec() == DIALOG_ACCEPTED:
            selected_items = list_widget.selectedItems()
            if selected_items:
                return [item.data(USER_ROLE) for item in selected_items]

        return None

    def get_generated_url(self):
        return self.generated_url
