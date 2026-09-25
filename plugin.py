# -*- coding: utf-8 -*-
"""
Modulo principal do plugin SIDRA Connector.

Registra o botao no menu e na toolbar do QGIS, abre a janela
principal e limpa tudo quando o plugin eh descarregado.
"""

import os
from qgis.PyQt.QtWidgets import QAction
from qgis.PyQt.QtGui import QIcon
from .ui.main_dialog import SidraConnectorDialog
from .gis.task_manager import cancel_all_tasks


class SidraConnector:
    """Classe principal do plugin -- o QGIS chama ela pra tudo."""

    def __init__(self, iface):
        self.iface = iface
        self.plugin_dir = os.path.dirname(__file__)
        self.action = None
        self.menu = u'&SIDRA Connector'
        self.dialog = None

    def initGui(self):
        """Coloca o icone na toolbar e o item no menu de plugins."""
        icon_path = os.path.join(self.plugin_dir, 'icon.png')
        self.action = QAction(QIcon(icon_path), 'SIDRA Connector', self.iface.mainWindow())
        self.action.triggered.connect(self.run)
        self.iface.addToolBarIcon(self.action)
        self.iface.addPluginToMenu(self.menu, self.action)

    def unload(self):
        """Remove o plugin do QGIS e cancela tarefas pendentes."""
        cancel_all_tasks()
        self.iface.removePluginMenu(u'&SIDRA Connector', self.action)
        self.iface.removeToolBarIcon(self.action)

    def run(self):
        """Abre a janela principal. Cria uma nova a cada vez pra comecar limpo."""
        self.dialog = SidraConnectorDialog(self.iface, self.plugin_dir)
        self.dialog.exec()
