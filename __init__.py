# -*- coding: utf-8 -*-
"""
/***************************************************************************
 SIDRA Connector
                                 A QGIS plugin
 Busca dados do SIDRA/IBGE, baixa malhas territoriais e faz join
 com camadas vetoriais direto no QGIS.

 Funciona com Qt5 (QGIS 3.x) e Qt6 (QGIS 4.x).
                             -------------------
        begin                : 2025-07-29
        copyright            : (C) 2025 by Gabriel Henrique Angelo
        email                : angelo.henrique.gabriel@gmail.com
 ***************************************************************************/

 Ponto de entrada do plugin. O QGIS chama classFactory() automaticamente
 quando carrega o plugin.
"""


# noinspection PyPep8Naming
def classFactory(iface):  # pylint: disable=invalid-name
    """O QGIS chama essa funcao pra instanciar o plugin."""
    from .plugin import SidraConnector
    return SidraConnector(iface)
