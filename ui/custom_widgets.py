# -*- coding: utf-8 -*-
"""
Widgets personalizados utilizados pela interface do plugin.

Este módulo é referenciado pelo arquivo .ui (Qt Designer) através da
tag ``<header>`` de promoted widgets, permitindo que o QGIS instancie
automaticamente o ``RefreshableComboBox`` ao carregar a UI.
"""

from qgis.PyQt import QtWidgets
from qgis.PyQt.QtCore import pyqtSignal


class RefreshableComboBox(QtWidgets.QComboBox):
    """
    QComboBox que emite o sinal ``aboutToShowPopup`` imediatamente antes
    de abrir o menu suspenso.

    Uso típico: conectar o sinal para recarregar a lista de camadas do
    projeto toda vez que o usuário clica na combobox, garantindo dados
    sempre atualizados sem polling.

    Registrado como *promoted widget* no arquivo ``main_dialog_base.ui``.
    """

    # Emitido antes de o popup ser exibido; permite atualizar os itens.
    aboutToShowPopup = pyqtSignal()

    def showPopup(self):
        """Sobrescreve ``QComboBox.showPopup`` para emitir o sinal primeiro."""
        self.aboutToShowPopup.emit()
        super(RefreshableComboBox, self).showPopup()
