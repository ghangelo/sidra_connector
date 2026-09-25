# -*- coding: utf-8 -*-

from ..constants import IBGE_AGGREGATES_BASE_URL
from ..exceptions import SidraInvalidResponseError


class SidraMetadataService:
    def __init__(self, http_client):
        self.http_client = http_client

    def get_table_metadata(self, table_id, feedback=None):
        table_id = self._validate_table_id(table_id)
        metadata_url = f"{IBGE_AGGREGATES_BASE_URL}/{table_id}/metadados"
        periods_url = f"{IBGE_AGGREGATES_BASE_URL}/{table_id}/periodos"
        locations_url = f"{IBGE_AGGREGATES_BASE_URL}/{table_id}/localidades"

        metadata = self.http_client.get_json(metadata_url, feedback=feedback)
        periods = self.http_client.get_json(periods_url, feedback=feedback)

        try:
            locations = self.http_client.get_json(locations_url, feedback=feedback)
        except Exception:
            locations = []

        return self._normalize_aggregate_metadata(
            table_id=table_id,
            metadata=metadata,
            periods=periods,
            locations=locations,
        )

    @staticmethod
    def _normalize_aggregate_metadata(table_id, metadata, periods, locations):
        if not isinstance(metadata, dict):
            raise SidraInvalidResponseError("Os metadados do IBGE têm estrutura inesperada.")

        normalized_periods = []
        for period in periods or []:
            if isinstance(period, dict):
                code = period.get("id") or period.get("codigo") or period.get("periodo")
                name = period.get("literal") or period.get("nome") or str(code or "")
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
                "unit": variable.get("unidade"),
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
                else:
                    category_id = category
                    category_name = str(category)

                categories.append({"id": str(category_id), "name": str(category_name)})

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
