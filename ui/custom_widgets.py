# -*- coding: utf-8 -*-
"""
Widgets customizados usados na interface do plugin.

O RefreshableComboBox eh referenciado pelo arquivo .ui como
promoted widget -- o QGIS instancia ele automaticamente.
"""

from qgis.PyQt import QtWidgets
from qgis.PyQt.QtCore import pyqtSignal


class RefreshableComboBox(QtWidgets.QComboBox):
    """ComboBox que avisa quando o usuario vai abrir o dropdown.

    Util pra recarregar a lista de camadas toda vez que clica,
    sem precisar ficar fazendo polling.
    """

    aboutToShowPopup = pyqtSignal()

    def showPopup(self):
        """Emite o sinal antes de abrir o dropdown."""
        self.aboutToShowPopup.emit()
        super(RefreshableComboBox, self).showPopup()
