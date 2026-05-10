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
from qgis.PyQt import QtWidgets, QtCore
from qgis.PyQt.QtCore import QThread, pyqtSignal
from qgis.core import QgsMessageLog, Qgis

from ..core.api_helpers import get_metadata_from_api, montar_url_interativa

# Qt5 vs Qt6 -- o QGIS 3.40+ usa Qt6, antes era Qt5.
# Esses try/except garantem que funciona nos dois.
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


class _MetadataWorker(QThread):
    """Thread que busca os metadados de uma tabela sem travar a tela."""

    resultReady = pyqtSignal(object)

    def __init__(self, table_id, parent=None):
        super().__init__(parent)
        self.table_id = table_id

    def run(self):
        result = get_metadata_from_api(self.table_id)
        self.resultReady.emit(result)


class QueryBuilderDialog(QtWidgets.QDialog):
    """Assistente pra montar a URL da API SIDRA passo a passo."""

    def __init__(self, plugin_dir, parent=None):
        super(QueryBuilderDialog, self).__init__(parent)
        self.plugin_dir = plugin_dir
        self.generated_url = None
        self.selected_table_id = None

        self.setup_ui()
        self.connect_signals()

    def setup_ui(self):
        """Monta a interface toda por codigo (sem .ui)."""
        self.setWindowTitle("Buscar Tabela do SIDRA")
        self.setFixedSize(600, 400)

        layout = QtWidgets.QVBoxLayout(self)

        # Campo de pesquisa
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

        # Mostra quantos resultados encontrou (ou se deu erro)
        self.lbl_status = QtWidgets.QLabel("")
        self.lbl_status.setStyleSheet("color: gray; font-style: italic;")

        self.list_results = QtWidgets.QListWidget()
        self.list_results.setMaximumHeight(200)

        search_layout.addWidget(self.lbl_status)
        search_layout.addWidget(self.list_results)

        # Painel da tabela que o usuario escolheu
        selected_group = QtWidgets.QGroupBox("Tabela Selecionada")
        selected_layout = QtWidgets.QVBoxLayout(selected_group)

        self.lbl_selected_table = QtWidgets.QLabel("Escolha uma tabela na lista acima")
        self.lbl_selected_table.setWordWrap(True)
        self.btn_build_query = QtWidgets.QPushButton("Construir Consulta")
        self.btn_build_query.setEnabled(False)

        selected_layout.addWidget(self.lbl_selected_table)
        selected_layout.addWidget(self.btn_build_query)

        # OK e Cancelar
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
        """Liga os eventos dos botoes e do campo de texto."""
        # Espera 500ms depois da ultima tecla antes de buscar,
        # pra nao ficar disparando consulta a cada letra
        self.search_timer = QtCore.QTimer()
        self.search_timer.setSingleShot(True)
        self.search_timer.timeout.connect(self.perform_search)

        self._metadata_worker = None

        self.le_search.textChanged.connect(self.on_search_text_changed)
        self.btn_clear.clicked.connect(self.clear_search)
        self.list_results.itemClicked.connect(self.on_table_selected)
        self.btn_build_query.clicked.connect(self.build_query)
        self.btn_cancel.clicked.connect(self.reject)
        self.btn_ok.clicked.connect(self.accept)

    def closeEvent(self, event):
        """Se a thread de metadados ainda ta rodando, mata antes de fechar."""
        if self._metadata_worker is not None and self._metadata_worker.isRunning():
            self._metadata_worker.resultReady.disconnect(self._on_metadata_ready)
            self._metadata_worker.terminate()
            self._metadata_worker.wait(3000)
        super().closeEvent(event)

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
        """A cada tecla, reseta o timer de 500ms pra nao buscar frenetico."""
        search_text = self.le_search.text().strip()

        self.search_timer.stop()

        if not search_text:
            self.list_results.clear()
            self.lbl_status.setText("")
            self.lbl_status.setStyleSheet("color: gray; font-style: italic;")
            return

        self.search_timer.start(500)

    def clear_search(self):
        """Limpa o campo e a lista, volta ao estado inicial."""
        self.le_search.clear()
        self.list_results.clear()
        self.lbl_status.setText("")
        self.lbl_status.setStyleSheet("color: gray; font-style: italic;")

    def perform_search(self):
        """Dispara a busca de fato (chamado pelo timer)."""
        search_term = self.le_search.text().strip()
        if not search_term:
            return
        self.lbl_status.setText("Procurando...")
        self.lbl_status.setStyleSheet("color: blue; font-style: italic;")
        self.search_tables(search_term)

    def search_tables(self, search_term=None):
        """Pesquisa no banco local, sem precisar de acento e em qualquer ordem.

        Funciona assim: o termo eh quebrado em palavras, e TODAS precisam
        aparecer no nome da tabela ou do grupo. Entao "populacao municipio"
        acha a mesma coisa que "municipio populacao".

        Se digitar so numeros, busca por ID (prefixo).
        """
        import unicodedata

        if search_term is None:
            search_term = self.le_search.text().strip()

        if not search_term:
            return

        conn = self.get_db_connection()
        if not conn:
            return

        def _normalizar(texto):
            """Tira acentos e passa pra minusculo -- usada dentro do SQLite."""
            if texto is None:
                return ''
            nfkd = unicodedata.normalize('NFKD', str(texto))
            sem_acento = ''.join(c for c in nfkd if not unicodedata.combining(c))
            return sem_acento.lower()

        try:
            # Registra a funcao Python direto no SQLite pra poder usar nas queries
            conn.create_function('norm', 1, _normalizar)
            cursor = conn.cursor()

            is_numeric = search_term.isdigit()

            if is_numeric:
                query = """
                    SELECT a.id, a.nome, g.nome as grupo_nome
                    FROM agregados a
                    JOIN grupos g ON a.grupo_id = g.id
                    WHERE CAST(a.id AS TEXT) LIKE ?
                    ORDER BY a.id
                """
                cursor.execute(query, (f"{search_term}%",))
            else:
                # Cada palavra vira um filtro AND -- todas tem que bater
                tokens = _normalizar(search_term).split()

                where_parts = []
                params = []
                for tok in tokens:
                    where_parts.append(
                        "(norm(a.nome) LIKE ? OR norm(g.nome) LIKE ?)"
                    )
                    params.extend([f"%{tok}%", f"%{tok}%"])

                where_clause = ' AND '.join(where_parts)
                query = f"""
                    SELECT a.id, a.nome, g.nome as grupo_nome
                    FROM agregados a
                    JOIN grupos g ON a.grupo_id = g.id
                    WHERE {where_clause}
                    ORDER BY a.nome
                """
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
            QtWidgets.QMessageBox.critical(
                self,
                "Ops!",
                f"Deu problema na busca: {e}"
            )
        finally:
            conn.close()

    def on_table_selected(self, item):
        """Quando o usuario clica numa tabela da lista."""
        table_id = item.data(USER_ROLE)

        if table_id is None:
            return

        self.selected_table_id = table_id
        self.lbl_selected_table.setText(f"Selecionada: {item.text()}")
        self.btn_build_query.setEnabled(True)

    def build_query(self):
        """Puxa os metadados da tabela escolhida (em background)."""
        if self.selected_table_id is None:
            QtWidgets.QMessageBox.warning(self, "Atenção", "Escolhe uma tabela da lista antes.")
            return

        self.btn_build_query.setEnabled(False)
        self.btn_build_query.setText("Carregando detalhes...")

        self._metadata_worker = _MetadataWorker(
            str(self.selected_table_id), parent=self
        )
        self._metadata_worker.resultReady.connect(self._on_metadata_ready)
        self._metadata_worker.start()

    def _on_metadata_ready(self, metadata):
        """Metadados chegaram -- agora percorre as 4 etapas de selecao.

        1. Periodos (ex: 2020, 2021, 2022...)
        2. Nivel geografico (municipio, UF, etc.)
        3. Variaveis (o que voce quer medir)
        4. Categorias de cada classificacao (sexo, idade, etc.)

        No final monta a URL e fecha o dialogo.
        """
        self.btn_build_query.setEnabled(True)
        self.btn_build_query.setText("Construir Consulta")

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
                (p.get('Id'), p.get('Nome'), p.get('Codigo'))
                for p in metadata.get('Periodos', {}).get('Periodos', [])
            ]
            periodos_selecionados = self.show_selection_dialog(
                "Selecione o(s) Periodo(s)",
                periodos_disponiveis
            )
            if not periodos_selecionados:
                return

            # 2) Nivel geografico
            # O dicionario de niveis traduz os IDs pra nomes legiveis
            territorios = metadata.get('Territorios', {})
            dic_niveis = territorios.get('DicionarioNiveis', {})
            nivel_nome_map = dict(zip(
                dic_niveis.get('Ids', []),
                dic_niveis.get('Nomes', [])
            ))
            niveis_disponiveis = [
                (n.get('Id'), nivel_nome_map.get(n.get('Id'), n.get('Sigla', '?')), n.get('Sigla', ''))
                for n in territorios.get('NiveisTabela', [])
            ]
            nivel_selecionado = self.show_selection_dialog(
                "Selecione o Nivel Geografico",
                niveis_disponiveis,
                single_selection=True
            )
            if not nivel_selecionado:
                return

            # 3) Variaveis (inclui as derivadas, tipo percentual, variacao etc.)
            variaveis_disponiveis = []
            for var in metadata.get('Variaveis', []):
                unidade = ""
                if isinstance(var.get('UnidadeDeMedida'), list):
                    if var.get('UnidadeDeMedida'):
                        unidade = var.get('UnidadeDeMedida')[0].get('Unidade', '')
                else:
                    unidade = var.get('UnidadeDeMedida', '')

                variaveis_disponiveis.append((var.get('Id'), var.get('Nome'), unidade))

                for derivada in var.get('VariaveisDerivadas', []):
                    unidade_derivada = derivada.get('UnidadeDeMedida', '')
                    variaveis_disponiveis.append(
                        (derivada.get('Id'), f"  \u2514\u2500 {derivada.get('Nome')}", unidade_derivada)
                    )

            variaveis_selecionadas = self.show_selection_dialog(
                "Selecione a(s) Variavel(is)",
                variaveis_disponiveis
            )
            if not variaveis_selecionadas:
                return

            # 4) Categorias por classificacao (se houver)
            classificacoes_selecionadas = {}
            for classif in metadata.get('Classificacoes', []):
                class_id = classif.get('Id')
                class_nome = classif.get('Nome')

                categorias_disponiveis = []
                for cat in classif.get('Categorias', []):
                    indentacao = "  " * cat.get('IdentacaoApresentacao', 0)
                    categorias_disponiveis.append(
                        (cat.get('Id'), f"{indentacao}{cat.get('Nome')}")
                    )

                if categorias_disponiveis:
                    categorias_selecionadas = self.show_selection_dialog(
                        f"Selecione categorias para: {class_nome}",
                        categorias_disponiveis
                    )
                    if categorias_selecionadas:
                        classificacoes_selecionadas[class_id] = [item[0] for item in categorias_selecionadas]

            # Tudo selecionado, monta a URL e fecha
            self.generated_url = montar_url_interativa(
                self.selected_table_id,
                nivel_selecionado[0],
                variaveis_selecionadas,
                periodos_selecionados,
                classificacoes_selecionadas
            )

            self.accept()

        except Exception as e:
            QtWidgets.QMessageBox.critical(
                self,
                "Ops!",
                f"Algo deu errado ao montar a consulta: {e}"
            )

    def show_selection_dialog(self, title, options, single_selection=False):
        """Abre uma janelinha pra escolher itens de uma lista.

        O botao OK so fica clicavel depois de selecionar pelo menos 1 item.
        Se for single_selection, so deixa escolher 1.
        Retorna lista de tuplas ou None se cancelar.
        """
        dialog = QtWidgets.QDialog(self)
        dialog.setWindowTitle(title)
        dialog.setFixedSize(500, 400)

        layout = QtWidgets.QVBoxLayout(dialog)

        list_widget = QtWidgets.QListWidget()

        for option in options:
            item_id, item_name = option[0], option[1]
            info_extra = f" ({option[2]})" if len(option) > 2 and option[2] else ""

            item = QtWidgets.QListWidgetItem(f"{item_name} (ID: {item_id}){info_extra}")
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

        # So libera o OK quando tem algo selecionado
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
        """Retorna a URL montada, ou None se o usuario cancelou no meio."""
        return self.generated_url
