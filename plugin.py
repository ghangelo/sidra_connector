# -*- coding: utf-8 -*-
"""
Módulo principal do plugin SIDRA Connector.

Registra o plugin na interface do QGIS (menu + toolbar) e controla
o ciclo de vida: inicialização → exibição do diálogo → descarga.

Compatível com Qt5 (QGIS ≤ 3.38) e Qt6 (QGIS 3.40+/4.x).
"""

import os
from qgis.PyQt.QtWidgets import QAction
from qgis.PyQt.QtGui import QIcon
from .ui.main_dialog import SidraConnectorDialog
from .gis.task_manager import active_tasks, cancel_all_tasks


class SidraConnector:
    """
    Classe principal do plugin — ponto de entrada registrado pelo QGIS.

    Responsabilidades:
    - Criar o item de menu e o ícone na toolbar.
    - Abrir o diálogo principal quando acionado.
    - Cancelar tarefas assíncronas pendentes ao descarregar.
    """

    def __init__(self, iface):
        """
        Construtor.

        :param iface: Instância de ``QgsInterface`` fornecida pelo QGIS,
                      usada para interagir com a barra de menus, toolbar
                      e barra de mensagens.
        """
        self.iface = iface
        self.plugin_dir = os.path.dirname(__file__)
        self.action = None
        self.menu = u'&SIDRA Connector'
        self.dialog = None

    def initGui(self):
        """
        Chamado pelo QGIS ao carregar o plugin.

        Cria a QAction com ícone, conecta ao slot ``run()`` e registra
        tanto na toolbar quanto no menu de plugins.
        """
        icon_path = os.path.join(self.plugin_dir, 'icon.png')
        self.action = QAction(QIcon(icon_path), 'SIDRA Connector', self.iface.mainWindow())
        self.action.triggered.connect(self.run)
        self.iface.addToolBarIcon(self.action)
        self.iface.addPluginToMenu(self.menu, self.action)

    def unload(self):
        """
        Chamado pelo QGIS ao descarregar o plugin.

        Cancela todas as QgsTasks em andamento (downloads, fetches)
        e remove o ícone/menu da interface.
        """
        cancel_all_tasks()
        self.iface.removePluginMenu(u'&SIDRA Connector', self.action)
        self.iface.removeToolBarIcon(self.action)

    def run(self):
        """
        Abre o diálogo principal do plugin em modo modal.

        Uma nova instância de ``SidraConnectorDialog`` é criada a cada
        chamada para garantir que o estado da UI esteja limpo.
        """
        self.dialog = SidraConnectorDialog(self.iface, self.plugin_dir)
        self.dialog.show()
        self.dialog.exec()
