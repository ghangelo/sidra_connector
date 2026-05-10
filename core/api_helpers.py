# -*- coding: utf-8 -*-
"""
Funcoes auxiliares pra se comunicar com as APIs do SIDRA / IBGE.

get_metadata_from_api -- pega a estrutura de uma tabela (periodos,
variaveis, classificacoes, niveis geograficos).

montar_url_interativa -- monta a URL de consulta a partir das
escolhas que o usuario fez no assistente.
"""

import requests
import json

from ..utils import constants

try:
    from qgis.core import QgsMessageLog, Qgis
    QGIS_AVAILABLE = True
except ImportError:
    QGIS_AVAILABLE = False


def get_metadata_from_api(tabela_id):
    """Puxa os metadados de uma tabela do SIDRA.

    Retorna o JSON com periodos, variaveis, classificacoes e niveis
    geograficos. O QueryBuilderDialog usa isso pra montar as opcoes.
    """
    url = f"https://sidra.ibge.gov.br/Ajax/JSon/Tabela/1/{tabela_id}?versao=-1"

    if QGIS_AVAILABLE:
        QgsMessageLog.logMessage(
            f"Buscando metadados da tabela {tabela_id}...",
            "SIDRA Connector", Qgis.Info,
        )

    try:
        response = requests.get(url, timeout=constants.API_TIMEOUT)
        response.raise_for_status()
        if QGIS_AVAILABLE:
            QgsMessageLog.logMessage("Metadados recebidos.", "SIDRA Connector", Qgis.Info)
        return response.json()

    except requests.exceptions.HTTPError as errh:
        error_msg = f"Erro HTTP: {errh}. Confere se a tabela '{tabela_id}' existe."
        if QGIS_AVAILABLE:
            QgsMessageLog.logMessage(error_msg, "SIDRA Connector", Qgis.Critical)

    except requests.exceptions.RequestException as err:
        error_msg = f"Erro de conexao: {err}"
        if QGIS_AVAILABLE:
            QgsMessageLog.logMessage(error_msg, "SIDRA Connector", Qgis.Critical)

    except json.JSONDecodeError:
        error_msg = "A resposta da API nao eh JSON valido."
        if QGIS_AVAILABLE:
            QgsMessageLog.logMessage(error_msg, "SIDRA Connector", Qgis.Critical)

    return None


def montar_url_interativa(
    tabela_id, nivel_geo, variaveis, periodos, classificacoes_selecionadas
):
    """Monta a URL da API SIDRA com base nas escolhas do usuario.

    Formato final:
        /values/t/{tabela}/n{nivel}/all/v/{vars}/p/{periodos}/c{class}/{cats}/f/u
    """
    base_url = "https://apisidra.ibge.gov.br/values"

    url_parts = [f"/t/{tabela_id}"]

    # Nivel geografico (municipio, UF, etc.) -- pega todos
    url_parts.append(f"/n{nivel_geo[0]}/all")

    # Variaveis selecionadas
    ids_variaveis = ",".join([str(v[0]) for v in variaveis])
    url_parts.append(f"/v/{ids_variaveis}")

    # Periodos selecionados
    codigos_periodos = ",".join([str(p[2]) for p in periodos])
    url_parts.append(f"/p/{codigos_periodos}")

    # Classificacoes e categorias
    for class_id, cat_ids in classificacoes_selecionadas.items():
        ids_categorias = ",".join(map(str, cat_ids))
        url_parts.append(f"/c{class_id}/{ids_categorias}")

    # /f/u -- pede pra API incluir os nomes das unidades
    url_parts.append("/f/u")

    return base_url + "".join(url_parts)
