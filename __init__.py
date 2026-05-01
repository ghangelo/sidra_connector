# -*- coding: utf-8 -*-
"""
/***************************************************************************
 SIDRA Connector
                                 A QGIS plugin
 Plugin para buscar dados tabulares da API SIDRA/IBGE, baixar malhas
 territoriais e uni-los a camadas vetoriais dentro do QGIS.

 Compatível com Qt5 (QGIS ≤ 3.38) e Qt6 (QGIS 3.40+ / 4.x).
                             -------------------
        begin                : 2025-07-29
        copyright            : (C) 2025 by Gabriel Henrique Angelo
        email                : angelo.henrique.gabriel@gmail.com
 ***************************************************************************/

 Este módulo é o ponto de entrada exigido pelo QGIS para reconhecer o plugin.
 A função ``classFactory()`` é chamada automaticamente pelo carregador de
 plugins e deve retornar uma instância da classe principal.
"""


# noinspection PyPep8Naming
def classFactory(iface):  # pylint: disable=invalid-name
    """Ponto de entrada do plugin — chamado pelo QGIS ao carregar.

    :param iface: Instância da interface do QGIS (``QgsInterface``).
    :type iface: QgsInterface
    :returns: Instância de ``SidraConnector``.
    """
    from .plugin import SidraConnector
    return SidraConnector(iface)
