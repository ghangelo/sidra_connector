# -*- coding: utf-8 -*-
"""
Funcoes auxiliares pra se comunicar com as APIs do SIDRA / IBGE.

get_metadata_from_api -- pega a estrutura de uma tabela (periodos,
variaveis, classificacoes, niveis geograficos).

montar_url_interativa -- monta a URL de consulta a partir das
escolhas que o usuario fez no assistente.
"""

from ..exceptions import SidraCancelledError, SidraError
from ..infrastructure.http_client import SidraHttpClient
from ..services.sidra_metadata_service import (
    SidraMetadataService,
    extract_period_name,
    extract_territorial_levels,
)

try:
    from qgis.core import QgsMessageLog, Qgis
    QGIS_AVAILABLE = True
except ImportError:
    QGIS_AVAILABLE = False


def _normalize_periods(periods_payload):
    periods = []
    for item in periods_payload or []:
        if not isinstance(item, dict):
            continue
        code = item.get("id") or item.get("codigo") or item.get("periodo")
        if code is None:
            continue
        name = extract_period_name(item, code)
        periods.append({
            "Id": code,
            "Nome": name,
            "Codigo": code,
        })
    return periods


def _normalize_aggregate_metadata(table_id, metadata_payload, periods_payload, locations_payload=None):
    """Normaliza o payload oficial do IBGE para a estrutura esperada pela GUI."""
    metadata = metadata_payload or {}
    periods = _normalize_periods(periods_payload)

    variables = []
    for variable in metadata.get("variaveis", []) or []:
        if not isinstance(variable, dict):
            continue
        variable_id = variable.get("id")
        variable_name = variable.get("nome") or variable.get("variavel") or str(variable_id or "")
        variables.append({
            "Id": variable_id,
            "Nome": variable_name,
            "UnidadeDeMedida": [{"Unidade": variable.get("unidade") or ""}],
            "VariaveisDerivadas": [],
        })

    classifications = []
    for classification in metadata.get("classificacoes", []) or []:
        if not isinstance(classification, dict):
            continue
        categories = []
        for category in classification.get("categorias", []) or []:
            if isinstance(category, dict):
                category_id = category.get("id")
                category_name = category.get("nome") or category.get("categoria") or str(category_id or "")
                indent = int(category.get("nivel") or 0)
            else:
                category_id = category
                category_name = str(category)
                indent = 0
            categories.append({
                "Id": category_id,
                "Nome": category_name,
                "IdentacaoApresentacao": indent,
            })

        classifications.append({
            "Id": classification.get("id"),
            "Nome": classification.get("nome") or classification.get("classificacao") or str(classification.get("id") or ""),
            "Categorias": categories,
        })

    levels = extract_territorial_levels(metadata)
    territorios = {
        "DicionarioNiveis": {
            "Ids": [lvl["id"] for lvl in levels],
            "Nomes": [lvl["name"] for lvl in levels],
        },
        "NiveisTabela": [
            {"Id": lvl["id"], "Sigla": lvl["sigla"]}
            for lvl in levels
        ],
    }

    return {
        "Periodos": {"Periodos": periods},
        "Territorios": territorios,
        "Variaveis": variables,
        "Classificacoes": classifications,
        "Tabela": {"Id": table_id},
        "Localidades": locations_payload or [],
    }


def get_metadata_from_api(table_id, feedback=None, http_client=None):
    """Puxa os metadados de uma tabela usando os endpoints oficiais do IBGE."""
    table_id = str(table_id).strip()

    if QGIS_AVAILABLE:
        QgsMessageLog.logMessage(
            f"Buscando metadados da tabela {table_id}...",
            "SIDRA Connector", Qgis.MessageLevel.Info,
        )

    try:
        service = SidraMetadataService(http_client or SidraHttpClient())
        normalized = service.get_table_metadata(table_id, feedback=feedback)
        raw_metadata = normalized.get("raw_metadata") or {}
        periods_payload = [
            {"id": p["id"], "literal": p["name"]}
            for p in normalized.get("periods", [])
        ]
        return _normalize_aggregate_metadata(
            table_id,
            raw_metadata,
            periods_payload,
        )
    except SidraCancelledError:
        return None
    except (SidraError, ValueError, TypeError) as err:
        error_msg = f"Não consegui buscar metadados: {err}"
        if QGIS_AVAILABLE:
            QgsMessageLog.logMessage(error_msg, "SIDRA Connector", Qgis.MessageLevel.Critical)
        return None


def montar_url_interativa(
    tabela_id, nivel_geo, variaveis, periodos, classificacoes_selecionadas
):
    """Monta a URL da API SIDRA com base nas escolhas do usuario."""
    base_url = "https://apisidra.ibge.gov.br/values"

    url_parts = [f"/t/{tabela_id}"]
    url_parts.append(f"/n{nivel_geo[0]}/all")

    ids_variaveis = ",".join([str(v[0]) for v in variaveis])
    url_parts.append(f"/v/{ids_variaveis}")

    codigos_periodos = ",".join([str(p[2]) for p in periodos])
    url_parts.append(f"/p/{codigos_periodos}")

    for class_id, cat_ids in classificacoes_selecionadas.items():
        ids_categorias = ",".join(map(str, cat_ids))
        url_parts.append(f"/c{class_id}/{ids_categorias}")

    return base_url + "".join(url_parts)
