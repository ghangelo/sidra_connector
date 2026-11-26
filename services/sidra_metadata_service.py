# -*- coding: utf-8 -*-

from ..constants import (
    IBGE_AGGREGATES_BASE_URL,
    SIDRA_PREFERRED_LEVEL_ORDER,
    SIDRA_TERRITORIAL_LEVELS,
)
from ..exceptions import SidraInvalidResponseError


def extract_period_name(period, code):
    literals = period.get("literals")
    if isinstance(literals, list) and literals:
        first_literal = str(literals[0] or "").strip()
        if first_literal:
            return first_literal
    return str(period.get("literal") or period.get("nome") or code or "")


def extract_territorial_levels(metadata):
    """Extrai e ordena os níveis territoriais suportados pela tabela."""
    supported_ids = []
    seen = set()

    nivel_territorial = (metadata or {}).get("nivelTerritorial")
    if isinstance(nivel_territorial, dict):
        for group_codes in nivel_territorial.values():
            if not isinstance(group_codes, list):
                continue
            for raw_code in group_codes:
                code_str = str(raw_code or "").strip().upper()
                if code_str.startswith("N") and code_str[1:].isdigit():
                    lvl_id = int(code_str[1:])
                    if lvl_id not in seen:
                        seen.add(lvl_id)
                        supported_ids.append(lvl_id)

    if not supported_ids:
        supported_ids = [1, 2, 3, 6, 7, 8, 9]

    order_index = {lvl: idx for idx, lvl in enumerate(SIDRA_PREFERRED_LEVEL_ORDER)}
    supported_ids.sort(key=lambda lvl: (order_index.get(lvl, 999), lvl))

    return [
        {
            "id": lvl_id,
            "sigla": f"N{lvl_id}",
            "name": SIDRA_TERRITORIAL_LEVELS.get(lvl_id, f"Nível N{lvl_id}"),
        }
        for lvl_id in supported_ids
    ]


class SidraMetadataService:
    def __init__(self, http_client):
        self.http_client = http_client

    def get_table_metadata(self, table_id, feedback=None):
        table_id = self._validate_table_id(table_id)
        metadata_url = f"{IBGE_AGGREGATES_BASE_URL}/{table_id}/metadados"
        periods_url = f"{IBGE_AGGREGATES_BASE_URL}/{table_id}/periodos"

        metadata = self.http_client.get_json(metadata_url, feedback=feedback)
        periods = self.http_client.get_json(periods_url, feedback=feedback)

        return self._normalize_aggregate_metadata(
            table_id=table_id,
            metadata=metadata,
            periods=periods,
        )

    @staticmethod
    def _normalize_aggregate_metadata(table_id, metadata, periods, locations=None):
        if not isinstance(metadata, dict):
            raise SidraInvalidResponseError("Os metadados do IBGE têm estrutura inesperada.")

        normalized_periods = []
        for period in periods or []:
            if isinstance(period, dict):
                code = period.get("id") or period.get("codigo") or period.get("periodo")
                name = extract_period_name(period, code)
            else:
                code = str(period)
                name = str(period)
            if code is not None:
                normalized_periods.append({"id": str(code), "name": str(name)})

        normalized_variables = []
        for variable in metadata.get("variaveis", []) or []:
            if not isinstance(variable, dict):
                continue
            variable_id = variable.get("id")
            variable_name = variable.get("nome") or variable.get("variavel") or str(variable_id or "")
            normalized_variables.append({
                "id": str(variable_id),
                "name": str(variable_name),
                "unit": variable.get("unidade") or "",
                "raw": variable,
            })

        normalized_classifications = []
        for classification in metadata.get("classificacoes", []) or []:
            if not isinstance(classification, dict):
                continue

            categories = []
            for category in classification.get("categorias", []) or []:
                if isinstance(category, dict):
                    category_id = category.get("id")
                    category_name = category.get("nome") or category.get("categoria") or str(category_id or "")
                    category_level = int(category.get("nivel") or 0)
                else:
                    category_id = category
                    category_name = str(category)
                    category_level = 0

                categories.append({
                    "id": str(category_id),
                    "name": str(category_name),
                    "level": category_level,
                })

            normalized_classifications.append({
                "id": str(classification.get("id")),
                "name": str(classification.get("nome") or classification.get("classificacao") or classification.get("id")),
                "categories": categories,
                "raw": classification,
            })

        return {
            "table_id": str(table_id),
            "name": metadata.get("nome") or metadata.get("agregado") or f"Tabela {table_id}",
            "survey": metadata.get("pesquisa"),
            "subject": metadata.get("assunto"),
            "variables": normalized_variables,
            "classifications": normalized_classifications,
            "periods": normalized_periods,
            "territorial_levels": extract_territorial_levels(metadata),
            "locations": locations or [],
            "source": "ibge-aggregates-v3",
            "raw_metadata": metadata,
        }

    @staticmethod
    def _validate_table_id(table_id):
        value = str(table_id).strip()
        if not value.isdigit():
            raise ValueError("O código da tabela deve conter apenas números.")
        return value
