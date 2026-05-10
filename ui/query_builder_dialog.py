# -*- coding: utf-8 -*-
"""
Assistente de busca de tabelas e construção de URLs da API SIDRA.

Fluxo do usuário:
1. Digita texto ou ID numérico no campo de busca.
2. Seleciona uma tabela da lista de resultados.
3. Clica em "Construir Consulta" — percorre 4 etapas de seleção:
   a. Períodos disponíveis.
   b. Nível geográfico (usa ``DicionarioNiveis`` para nomes legíveis).
   c. Variáveis (incluindo derivadas).
   d. Categorias por classificação.
4. A URL é montada por ``montar_url_interativa()`` e devolvida ao
   diálogo principal.

A busca é feita localmente no SQLite ``agregados_ibge.db`` (gerado
pelo script ``dev/criar_db.py``).
"""

import os
import sqlite3
from qgis.PyQt import QtWidgets, QtCore
from qgis.PyQt.QtCore import QThread, pyqtSignal
from qgis.core import QgsMessageLog, Qgis

from ..core.api_helpers import get_metadata_from_api, montar_url_interativa

# Constante compatível com Qt5 (QGIS ≤ 3.38) e Qt6 (QGIS 3.40+).
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
    """Thread auxiliar para buscar metadados sem bloquear a GUI.

    Sinais:
    - ``resultReady(object)`` — metadados recebidos (``dict`` ou ``None``).
    """

    resultReady = pyqtSignal(object)  # dict ou None

    def __init__(self, table_id, parent=None):
        super().__init__(parent)
        self.table_id = table_id

    def run(self):
        """Executado em thread separada — chama a API de metadados."""
        result = get_metadata_from_api(self.table_id)
        self.resultReady.emit(result)


class QueryBuilderDialog(QtWidgets.QDialog):
    """Assistente interativo para buscar tabelas SIDRA e gerar URLs da API."""

    def __init__(self, plugin_dir, parent=None):
        """Inicializa o diálogo de busca.

        :param plugin_dir: Caminho absoluto da pasta do plugin (contém o SQLite).
        :param parent: Widget pai (normalmente o ``SidraConnectorDialog``).
        """
        super(QueryBuilderDialog, self).__init__(parent)
        self.plugin_dir = plugin_dir
        self.generated_url = None
        self.selected_table_id = None
        
        self.setup_ui()
        self.connect_signals()

    def setup_ui(self):
        """Cria e organiza todos os widgets programaticamente."""
        self.setWindowTitle("Assistente de Busca - SIDRA Connector")
        self.setFixedSize(600, 400)
        
        # Layout principal
        layout = QtWidgets.QVBoxLayout(self)
        
        # Seção de busca
        search_group = QtWidgets.QGroupBox("Buscar Tabelas")
        search_layout = QtWidgets.QVBoxLayout(search_group)
        
        # Campo de busca
        search_input_layout = QtWidgets.QHBoxLayout()
        self.le_search = QtWidgets.QLineEdit()
        self.le_search.setPlaceholderText("Busque por nome ou ID da tabela...")
        self.btn_clear = QtWidgets.QPushButton("Limpar")
        self.btn_clear.setMaximumWidth(80)
        
        search_input_layout.addWidget(self.le_search)
        search_input_layout.addWidget(self.btn_clear)
        
        search_layout.addLayout(search_input_layout)
        
        # Label de status
        self.lbl_status = QtWidgets.QLabel("")
        self.lbl_status.setStyleSheet("color: gray; font-style: italic;")
        
        # Lista de resultados
        self.list_results = QtWidgets.QListWidget()
        self.list_results.setMaximumHeight(200)
        
        search_layout.addWidget(self.lbl_status)
        search_layout.addWidget(self.list_results)
        
        # Seção de tabela selecionada
        selected_group = QtWidgets.QGroupBox("Tabela Selecionada")
        selected_layout = QtWidgets.QVBoxLayout(selected_group)
        
        self.lbl_selected_table = QtWidgets.QLabel("Nenhuma tabela selecionada")
        self.lbl_selected_table.setWordWrap(True)
        self.btn_build_query = QtWidgets.QPushButton("Construir Consulta")
        self.btn_build_query.setEnabled(False)
        
        selected_layout.addWidget(self.lbl_selected_table)
        selected_layout.addWidget(self.btn_build_query)
        
        # Botões de ação
        button_layout = QtWidgets.QHBoxLayout()
        self.btn_cancel = QtWidgets.QPushButton("Cancelar")
        self.btn_ok = QtWidgets.QPushButton("OK")
        self.btn_ok.setEnabled(False)
        
        button_layout.addStretch()
        button_layout.addWidget(self.btn_cancel)
        button_layout.addWidget(self.btn_ok)
        
        # Adicionar ao layout principal
        layout.addWidget(search_group)
        layout.addWidget(selected_group)
        layout.addLayout(button_layout)

    def connect_signals(self):
        """Liga sinais de widgets a slots — inclui timer de debounce."""
        # Timer para busca dinâmica
        self.search_timer = QtCore.QTimer()
        self.search_timer.setSingleShot(True)
        self.search_timer.timeout.connect(self.perform_search)
        
        # Referência para worker de metadados (para poder esperar na saída)
        self._metadata_worker = None

        # Conectar eventos
        self.le_search.textChanged.connect(self.on_search_text_changed)
        self.btn_clear.clicked.connect(self.clear_search)
        self.list_results.itemClicked.connect(self.on_table_selected)
        self.btn_build_query.clicked.connect(self.build_query)
        self.btn_cancel.clicked.connect(self.reject)
        self.btn_ok.clicked.connect(self.accept)

    def closeEvent(self, event):
        """Garante que a thread de metadados é finalizada antes de fechar."""
        if self._metadata_worker is not None and self._metadata_worker.isRunning():
            self._metadata_worker.resultReady.disconnect(self._on_metadata_ready)
            # terminate() em vez de quit(): run() sobrescrito não possui
            # event loop, então quit() não tem efeito.
            self._metadata_worker.terminate()
            self._metadata_worker.wait(3000)
        super().closeEvent(event)

    def get_db_connection(self):
        """Abre conexão com ``agregados_ibge.db``.

        :returns: ``sqlite3.Connection`` ou ``None`` se o ficheiro não existir.
        """
        db_path = os.path.join(self.plugin_dir, "agregados_ibge.db")
        
        if not os.path.exists(db_path):
            QtWidgets.QMessageBox.critical(
                self, 
                "Erro", 
                f"Banco de dados não encontrado em: {db_path}\n\n"
                "Certifique-se de que o arquivo agregados_ibge.db está na pasta do plugin."
            )
            return None
            
        try:
            return sqlite3.connect(db_path)
        except sqlite3.Error as e:
            QtWidgets.QMessageBox.critical(
                self, 
                "Erro de Banco de Dados", 
                f"Não foi possível conectar ao banco de dados: {e}"
            )
            return None

    def on_search_text_changed(self):
        """Debounce: reinicia timer de 500 ms a cada tecla digitada."""
        search_text = self.le_search.text().strip()

        # Parar o timer anterior
        self.search_timer.stop()

        if not search_text:
            # Limpar resultados se o campo estiver vazio
            self.list_results.clear()
            self.lbl_status.setText("")
            self.lbl_status.setStyleSheet("color: gray; font-style: italic;")
            return

        # Iniciar novo timer de 500ms
        self.search_timer.start(500)

    def clear_search(self):
        """Restaura o campo de busca e a lista ao estado inicial."""
        self.le_search.clear()
        self.list_results.clear()
        self.lbl_status.setText("")
        self.lbl_status.setStyleSheet("color: gray; font-style: italic;")

    def perform_search(self):
        """Chamado pelo timer de debounce — dispara ``search_tables``."""
        search_term = self.le_search.text().strip()
        if not search_term:
            return
        self.lbl_status.setText("Buscando...")
        self.lbl_status.setStyleSheet("color: blue; font-style: italic;")
        self.search_tables(search_term)

    def search_tables(self, search_term=None):
        """Consulta ``agregados_ibge.db`` e preenche a lista de resultados.

        A busca e insensivel a acentos e a ordem das palavras:
        cada palavra do termo digitado deve aparecer em algum lugar
        no nome da tabela ou do grupo (qualquer ordem, sem acento).

        Para IDs numericos, faz busca por prefixo.

        :param search_term: Texto digitado pelo usuario (ou ``None``).
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
            """Remove acentos e converte para minusculas."""
            if texto is None:
                return ''
            nfkd = unicodedata.normalize('NFKD', str(texto))
            sem_acento = ''.join(c for c in nfkd if not unicodedata.combining(c))
            return sem_acento.lower()

        try:
            # Registrar funcao de normalizacao no SQLite
            conn.create_function('norm', 1, _normalizar)
            cursor = conn.cursor()

            is_numeric = search_term.isdigit()

            if is_numeric:
                # Busca por ID exato ou prefixo do ID
                query = """
                    SELECT a.id, a.nome, g.nome as grupo_nome
                    FROM agregados a
                    JOIN grupos g ON a.grupo_id = g.id
                    WHERE CAST(a.id AS TEXT) LIKE ?
                    ORDER BY a.id
                """
                cursor.execute(query, (f"{search_term}%",))
            else:
                # Quebrar o termo em palavras e exigir que todas apareçam
                # (em qualquer ordem) no nome normalizado da tabela ou grupo
                tokens = _normalizar(search_term).split()

                # Cada token gera uma condicao AND
                # norm(a.nome) LIKE '%token%' OR norm(g.nome) LIKE '%token%'
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

            # Limpar resultados anteriores
            self.list_results.clear()

            if results:
                for table_id, table_name, group_name in results:
                    item = QtWidgets.QListWidgetItem(
                        f"[{table_id}] {table_name} ({group_name})"
                    )
                    item.setData(USER_ROLE, table_id)
                    self.list_results.addItem(item)

                count = len(results)
                self.lbl_status.setText(f"{count} resultado(s) encontrado(s)")
                self.lbl_status.setStyleSheet("color: green;")
            else:
                self.lbl_status.setText("Nenhum resultado encontrado")
                self.lbl_status.setStyleSheet("color: orange;")

        except sqlite3.Error as e:
            self.lbl_status.setText(f"Erro na busca: {e}")
            self.lbl_status.setStyleSheet("color: red;")
            QtWidgets.QMessageBox.critical(
                self,
                "Erro de Busca",
                f"Erro ao buscar no banco de dados: {e}"
            )
        finally:
            conn.close()

    def on_table_selected(self, item):
        """Slot ``itemClicked`` — registra a tabela selecionada.

        :param item: ``QListWidgetItem`` clicado na lista de resultados.
        """
        table_id = item.data(USER_ROLE)
        
        if table_id is None:
            # Item inválido (como mensagem de "nenhum resultado")
            return
            
        self.selected_table_id = table_id
        self.lbl_selected_table.setText(f"Tabela selecionada: {item.text()}")
        self.btn_build_query.setEnabled(True)

    def build_query(self):
        """Inicia a busca assíncrona de metadados da tabela selecionada.

        A requisição HTTP é delegada a ``_MetadataWorker`` (QThread) para
        não bloquear a interface gráfica. Quando a resposta chega,
        ``_on_metadata_ready`` é chamado para prosseguir com o fluxo de
        4 etapas de seleção.
        """
        if self.selected_table_id is None:
            QtWidgets.QMessageBox.warning(self, "Aviso", "Selecione uma tabela primeiro.")
            return

        # Desabilitar o botão enquanto a requisição está em andamento
        self.btn_build_query.setEnabled(False)
        self.btn_build_query.setText("A carregar metadados...")

        self._metadata_worker = _MetadataWorker(
            str(self.selected_table_id), parent=self
        )
        self._metadata_worker.resultReady.connect(self._on_metadata_ready)
        self._metadata_worker.start()

    def _on_metadata_ready(self, metadata):
        """Callback: metadados recebidos — prossegue com as 4 etapas de seleção.

        Etapas (cada uma abre um ``show_selection_dialog``):
        1. Períodos — lista vinda de ``metadata['Periodos']``.
        2. Nível geográfico — nomes resolvidos via ``DicionarioNiveis``.
        3. Variáveis (incluindo derivadas).
        4. Categorias por classificação.

        Ao final, a URL é montada por ``montar_url_interativa()``.
        """
        # Restaurar o botão independentemente do resultado
        self.btn_build_query.setEnabled(True)
        self.btn_build_query.setText("Construir Consulta")

        if not metadata:
            QtWidgets.QMessageBox.critical(
                self,
                "Erro",
                "Não foi possível obter os metadados da tabela. Verifique sua conexão com a internet."
            )
            return

        try:
            # Selecionar períodos
            periodos_disponiveis = [
                (p.get('Id'), p.get('Nome'), p.get('Codigo')) 
                for p in metadata.get('Periodos', {}).get('Periodos', [])
            ]
            periodos_selecionados = self.show_selection_dialog(
                "Selecione o(s) Período(s)", 
                periodos_disponiveis
            )
            if not periodos_selecionados:
                return
                
            # --- Etapa 2: Nível geográfico ---
            # 'NiveisTabela' só contém Id e Sigla; os nomes legíveis estão
            # em 'DicionarioNiveis' (listas paralelas Ids/Nomes).
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
                "Selecione o Nível Geográfico", 
                niveis_disponiveis, 
                single_selection=True
            )
            if not nivel_selecionado:
                return
                
            # Selecionar variáveis
            variaveis_disponiveis = []
            for var in metadata.get('Variaveis', []):
                unidade = ""
                if isinstance(var.get('UnidadeDeMedida'), list):
                    if var.get('UnidadeDeMedida'):
                        unidade = var.get('UnidadeDeMedida')[0].get('Unidade', '')
                else:
                    unidade = var.get('UnidadeDeMedida', '')
                    
                variaveis_disponiveis.append((var.get('Id'), var.get('Nome'), unidade))
                
                # Adicionar variáveis derivadas
                for derivada in var.get('VariaveisDerivadas', []):
                    unidade_derivada = derivada.get('UnidadeDeMedida', '')
                    variaveis_disponiveis.append(
                        (derivada.get('Id'), f"  └─ {derivada.get('Nome')}", unidade_derivada)
                    )
                    
            variaveis_selecionadas = self.show_selection_dialog(
                "Selecione a(s) Variável(is)", 
                variaveis_disponiveis
            )
            if not variaveis_selecionadas:
                return
                
            # Selecionar categorias para cada classificação
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
            
            # Montar URL
            self.generated_url = montar_url_interativa(
                self.selected_table_id,
                nivel_selecionado[0],
                variaveis_selecionadas,
                periodos_selecionados,
                classificacoes_selecionadas
            )
            
            # URL montada — fecha o dialogo automaticamente
            self.accept()
            
        except Exception as e:
            QtWidgets.QMessageBox.critical(
                self, 
                "Erro", 
                f"Erro ao processar metadados da tabela: {e}"
            )

    def show_selection_dialog(self, title, options, single_selection=False):
        """Abre diálogo genérico de seleção (simples ou múltipla).

        :param title: Título da janela.
        :param options: Lista de tuplas ``(id, nome[, info_extra])``.
        :param single_selection: Se ``True``, permite selecionar apenas 1 item.
        :returns: Lista de tuplas selecionadas ou ``None`` se cancelado.
        """
        dialog = QtWidgets.QDialog(self)
        dialog.setWindowTitle(title)
        dialog.setFixedSize(500, 400)
        
        layout = QtWidgets.QVBoxLayout(dialog)
        
        # Lista de opções
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
        
        # Botões
        button_layout = QtWidgets.QHBoxLayout()
        btn_ok = QtWidgets.QPushButton("OK")
        btn_ok.setEnabled(False)  # Desabilitado ate selecionar algo
        btn_cancel = QtWidgets.QPushButton("Cancelar")

        button_layout.addStretch()
        button_layout.addWidget(btn_cancel)
        button_layout.addWidget(btn_ok)

        layout.addLayout(button_layout)

        # Habilitar OK somente quando ha selecao
        def _on_selection_changed():
            btn_ok.setEnabled(len(list_widget.selectedItems()) > 0)

        list_widget.itemSelectionChanged.connect(_on_selection_changed)

        # Conectar sinais
        btn_ok.clicked.connect(dialog.accept)
        btn_cancel.clicked.connect(dialog.reject)

        # Executar diálogo
        if dialog.exec() == DIALOG_ACCEPTED:
            selected_items = list_widget.selectedItems()
            if selected_items:
                return [item.data(USER_ROLE) for item in selected_items]

        return None

    def get_generated_url(self):
        """Retorna a URL gerada ou ``None`` se o fluxo não foi concluído."""
        return self.generated_url
