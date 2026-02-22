# -*- coding: utf-8 -*-
"""
Funções auxiliares para comunicação com as APIs do SIDRA / IBGE.

- ``get_metadata_from_api``: busca a estrutura (metadados) de uma tabela
  na API interna do SIDRA (períodos, variáveis, classificações, níveis
  geográficos).
- ``montar_url_interativa``: compõe a URL de consulta da *apisidra* a
  partir das seleções feitas pelo usuário no assistente de busca.

Endpoints utilizados:
    Metadados: https://sidra.ibge.gov.br/Ajax/JSon/Tabela/1/{id}?versao=-1
    Dados:     https://apisidra.ibge.gov.br/values/t/{id}/...
"""

import requests
import json
from qgis.core import QgsMessageLog, Qgis


def get_metadata_from_api(tabela_id):
    """Busca os metadados completos de uma tabela do SIDRA.

    Retorna o JSON com períodos disponíveis, variáveis, classificações
    e níveis geográficos — usado pelo ``QueryBuilderDialog`` para
    montar a consulta interativamente.

    :param tabela_id: ID numérico (str) da tabela no SIDRA.
    :returns: Dicionário com os metadados, ou ``None`` em caso de erro.
    """
    url = f"https://sidra.ibge.gov.br/Ajax/JSon/Tabela/1/{tabela_id}?versao=-1"

    QgsMessageLog.logMessage(
        f"Buscando metadados para a tabela {tabela_id}...",
        "SIDRA Connector", Qgis.Info,
    )

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        QgsMessageLog.logMessage(
            "Metadados recebidos com sucesso.", "SIDRA Connector", Qgis.Info
        )
        return response.json()

    except requests.exceptions.HTTPError as errh:
        error_msg = (
            f"Erro HTTP: {errh}. Verifique se o código da tabela "
            f"'{tabela_id}' está correto e disponível."
        )
        QgsMessageLog.logMessage(error_msg, "SIDRA Connector", Qgis.Critical)

    except requests.exceptions.RequestException as err:
        error_msg = f"Erro de conexão: {err}"
        QgsMessageLog.logMessage(error_msg, "SIDRA Connector", Qgis.Critical)

    except json.JSONDecodeError:
        error_msg = "Erro: A resposta da API não é um JSON válido."
        QgsMessageLog.logMessage(error_msg, "SIDRA Connector", Qgis.Critical)

    return None


def montar_url_interativa(
    tabela_id, nivel_geo, variaveis, periodos, classificacoes_selecionadas
):
    """Compõe a URL da API SIDRA a partir das seleções do usuário.

    Segue a especificação REST da *apisidra*::

        /values/t/{tabela}/n{nivel}/all/v/{vars}/p/{periodos}/c{class}/{cats}/f/u

    :param tabela_id: ID da tabela.
    :param nivel_geo: Tupla ``(id, nome, sigla)`` do nível geográfico.
    :param variaveis: Lista de tuplas ``(id, nome, ...)`` selecionadas.
    :param periodos: Lista de tuplas ``(id, nome, codigo)`` selecionados.
    :param classificacoes_selecionadas: ``{class_id: [cat_id, ...]}``.
    :returns: URL completa pronta para requisição GET.
    """
    base_url = "https://apisidra.ibge.gov.br/values"

    # /t/{tabela}
    url_parts = [f"/t/{tabela_id}"]

    # /n{nivel}/all  → todos os territórios daquele nível
    url_parts.append(f"/n{nivel_geo[0]}/all")

    # /v/{id1,id2,...}  → variáveis
    ids_variaveis = ",".join([str(v[0]) for v in variaveis])
    url_parts.append(f"/v/{ids_variaveis}")

    # /p/{cod1,cod2,...}  → períodos
    codigos_periodos = ",".join([str(p[2]) for p in periodos])
    url_parts.append(f"/p/{codigos_periodos}")

    # /c{classif}/{cat1,cat2,...}  → classificações e categorias
    for class_id, cat_ids in classificacoes_selecionadas.items():
        ids_categorias = ",".join(map(str, cat_ids))
        url_parts.append(f"/c{class_id}/{ids_categorias}")

    # /f/u  → retorna nomes de unidade no resultado
    url_parts.append("/f/u")

    return base_url + "".join(url_parts)
