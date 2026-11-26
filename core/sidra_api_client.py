# -*- coding: utf-8 -*-
"""
Cliente da API de dados do SIDRA/IBGE.

Pega a resposta da API (JSON ou XML), transforma num dicionario
{codigo_geografico: {variavel: valor}} e entrega pro DataJoiner
fazer o join com a camada vetorial.

Nao usa pandas -- tudo na mao com dicts e listas.
"""

import os
import re
import tempfile

# Usa defusedxml para proteger contra ataques XML (XXE, billion laughs, etc.).
try:
    import defusedxml.ElementTree as ET
except ImportError:  # pragma: no cover
    import xml.etree.ElementTree as ET  # noqa: S405  # nosec B405 -- intentional fallback

from ..constants import DEFAULT_PERIOD_BATCH_SIZE
from ..exceptions import SidraQueryTooLargeError
from ..infrastructure.checkpoint_store import JsonCheckpointStore
from ..infrastructure.http_client import SidraHttpClient
from ..services.sidra_values_service import SidraValuesService

IBGE_MISSING_SYMBOLS = {"...", "..", "X", "x"}
IBGE_ZERO_SYMBOLS = {"-"}

VALID_IBGE_UF_CODES = {
    "11", "12", "13", "14", "15", "16", "17",
    "21", "22", "23", "24", "25", "26", "27", "28", "29",
    "31", "32", "33", "35",
    "41", "42", "43",
    "50", "51", "52", "53",
}
SUB_UF_LEVELS = {"6", "8", "9", "10", "11", "102"}


def apply_uf_filter_to_url(url: str, uf_codes) -> str:
    """Substitui /n{nivel}/all por /n{nivel}/in%20n3%20{ufs} em níveis subestaduais."""
    if not url or not uf_codes:
        return url

    valid_ufs = sorted({
        str(code).strip()
        for code in uf_codes
        if str(code).strip() in VALID_IBGE_UF_CODES
    })
    if not valid_ufs or len(valid_ufs) >= len(VALID_IBGE_UF_CODES):
        return url

    uf_list = ",".join(valid_ufs)

    def _replace(match):
        level = match.group(1)
        if level in SUB_UF_LEVELS:
            return f"/n{level}/in%20n3%20{uf_list}"
        return match.group(0)

    return re.sub(r"/n(\d+)/all(?=/|$|\?)", _replace, url, count=1, flags=re.IGNORECASE)


def _notna(value):
    """Verifica se o valor nao eh None nem string vazia."""
    return value is not None and value != ""


def _parse_numeric(value_str):
    """Tenta converter pra float, lidando com formato brasileiro (1.234,56).

    '-' segue a convencao do IBGE (zero absoluto -> 0.0).
    '...', '..', 'X' retornam None.
    """
    if value_str is None:
        return None
    normalized = str(value_str).strip()
    if not normalized or normalized in IBGE_MISSING_SYMBOLS:
        return None
    if normalized in IBGE_ZERO_SYMBOLS:
        return 0.0
    try:
        if "," in normalized:
            normalized = normalized.replace(".", "").replace(",", ".")
        return float(normalized)
    except (ValueError, TypeError):
        return str(value_str) if _notna(value_str) else None


class SidraApiClient:
    """Busca dados tabulares da API SIDRA e transforma pra join."""

    def __init__(self, table_query, http_client=None, checkpoint_store=None):
        self.full_query_url = None
        if isinstance(table_query, str) and table_query.startswith("http"):
            self.full_query_url = table_query
            match = re.search(r"/t/(\d+)", table_query)
            if match:
                self.table_code = int(match.group(1))
            else:
                raise ValueError(f"Nao achei o codigo da tabela na URL: {table_query}")
        else:
            self.table_code = int(table_query)

        self.base_url = f"https://apisidra.ibge.gov.br/values/t/{self.table_code}"
        self.http_client = http_client
        self.checkpoint_store = checkpoint_store

    def _fetch_raw_rows(self, client, url: str, feedback=None, progress_callback=None):
        period_match = re.search(r"/p/([^/?#]+)", url, re.IGNORECASE)
        periods = (
            [p.strip() for p in period_match.group(1).split(",") if p.strip()]
            if period_match
            else []
        )

        if len(periods) > 1:
            store = self.checkpoint_store
            if store is None and self.http_client is None:
                checkpoint_dir = os.path.join(
                    tempfile.gettempdir(), "sidra_connector_checkpoints"
                )
                store = JsonCheckpointStore(checkpoint_dir)
            values_service = SidraValuesService(client, checkpoint_store=store)
            return values_service.collect(
                query_url=url,
                periods=periods,
                batch_size=DEFAULT_PERIOD_BATCH_SIZE,
                feedback=feedback,
                progress_callback=progress_callback,
            )

        try:
            return client.get_json(url, feedback=feedback)
        except Exception as exc:
            if SidraValuesService._is_query_too_large(exc):
                raise SidraQueryTooLargeError(
                    "A consulta excedeu o limite de 100.000 valores do SIDRA. "
                    "Reduza as variáveis/categorias ou use uma camada de um estado específico."
                ) from exc
            raise

    def fetch_and_parse(self, params: dict = None, feedback=None, progress_callback=None) -> tuple:
        """Faz a requisicao (em lotes se necessario) e retorna (dados, header_info)."""
        if self.full_query_url:
            final_url = self.full_query_url
        else:
            if params is None:
                params = {}
            sanitized_params = {k: str(v).replace(" ", "") for k, v in params.items()}
            final_url = self.base_url
            if sanitized_params:
                path_params = "/".join([f"{k}/{v}" for k, v in sanitized_params.items()])
                final_url = f"{self.base_url}/{path_params}"

        client = self.http_client or SidraHttpClient()
        data = self._fetch_raw_rows(
            client, final_url, feedback=feedback, progress_callback=progress_callback
        )

        if (not data or len(data) <= 1) and re.search(r"/in%20n3%20[^/?#]+", final_url, re.IGNORECASE):
            fallback_url = re.sub(
                r"/in%20n3%20[^/?#]+", "/all", final_url, count=1, flags=re.IGNORECASE
            )
            data = self._fetch_raw_rows(
                client, fallback_url, feedback=feedback, progress_callback=progress_callback
            )

        if not data or len(data) <= 1:
            return {}, {}

        header = data[0]
        rows = data[1:]

        columns = list(header.keys())
        table_data = [{col: row.get(col) for col in columns} for row in rows]
        column_mapping = dict(header.items())
        return self._convert_rows_to_dict(table_data, columns, column_mapping)

    def _parse_xml(self, xml_string: str) -> tuple:
        """Parseia resposta XML (formato antigo da API)."""
        root = ET.fromstring(xml_string)  # nosec B314
        namespace = {"ns": "http://schemas.datacontract.org/2004/07/IBGE.BTE.Tabela"}

        header_element = root.find("ns:ValorDescritoPorSuasDimensoes", namespace)
        if header_element is None:
            return {}, {}

        header_map = {child.tag.split("}")[-1]: child.text for child in header_element}
        data_elements = root.findall("ns:ValorDescritoPorSuasDimensoes", namespace)[1:]

        all_rows = []
        for elem in data_elements:
            row_data = {
                header_map.get(child.tag.split("}")[-1], child.tag.split("}")[-1]): child.text
                for child in elem
            }
            all_rows.append(row_data)

        if not all_rows:
            return {}, {}

        columns = list(all_rows[0].keys())

        geo_code_col = None
        niveis_geograficos = [
            "brasil", "grande regiao", "unidade da federacao",
            "regiao metropolitana", "regiao integrada de desenvolvimento",
            "microrregiao geografica", "mesorregiao geografica",
            "regiao geografica imediata", "regiao geografica intermediaria",
            "municipio", "distrito", "subdistrito", "bairro", "setor censitario",
        ]

        for i in range(1, 10):
            dim_name = f"D{i}N"
            dim_code = f"D{i}C"
            if header_map.get(dim_name) and any(
                k in header_map[dim_name].lower() for k in niveis_geograficos
            ):
                geo_code_col = header_map.get(dim_code)
                break

        if geo_code_col is None:
            raise ValueError("Nao achei a coluna de codigo geografico no XML.")

        if geo_code_col in columns:
            columns = ["geo_code" if c == geo_code_col else c for c in columns]
            for row in all_rows:
                if geo_code_col in row:
                    row["geo_code"] = row.pop(geo_code_col)

        return self._convert_rows_to_dict(all_rows, columns, {})

    def _convert_rows_to_dict(self, rows: list, columns: list, column_labels: dict = None) -> tuple:
        """Transforma as linhas da API no dicionario {geo_code: {coluna: valor}}."""
        if not rows:
            return {}, {}

        sidra_data_dict = {}
        header_info = {}
        column_labels = column_labels or {}

        dim_cols = {c for c in columns if re.match(r"^D\d+[CN]$", c)}
        excluded_cols = {"geo_code", "NC", "NN", "MC", "MN"} | dim_cols
        col_set = set(columns)

        value_cols = []
        if "V" in col_set:
            value_cols.append("V")
        for col in columns:
            if col not in excluded_cols and col not in value_cols:
                value_cols.append(col)

        first_row = rows[0]
        for col in columns:
            if col.endswith("N"):
                val = first_row.get(col)
                header_info[col] = val if _notna(val) else col

        # Identifica coluna de codigo geografico (e seu par de nome D{i}N)
        geo_name_col = None
        if "geo_code" not in col_set:
            geo_code_col = None
            nn_label = str(first_row.get("NN") or "").strip().lower()
            if nn_label and column_labels:
                for col in columns:
                    if re.match(r"^D\d+N$", col):
                        lbl = str(column_labels.get(col) or "").strip().lower()
                        if lbl == nn_label:
                            candidate_c = col[:-1] + "C"
                            if candidate_c in col_set:
                                geo_code_col = candidate_c
                                geo_name_col = col
                                break

            if geo_code_col is None:
                if "D1C" in col_set:
                    geo_code_col = "D1C"
                    geo_name_col = "D1N"
                else:
                    geo_candidates = [col for col in columns if re.match(r"^D\d+C$", col)]
                    if geo_candidates:
                        geo_code_col = geo_candidates[0]
                        geo_name_col = geo_code_col[:-1] + "N"
                    else:
                        return {}, header_info

            for row in rows:
                if geo_code_col in row:
                    row["geo_code"] = row.pop(geo_code_col)

        # Todas as dimensoes nao-geograficas que possuem mais de 1 valor unico
        non_geo_dim_names = [
            col for col in columns
            if re.match(r"^D\d+N$", col) and col != geo_name_col
        ]
        varying_dims = [
            col for col in non_geo_dim_names
            if len({row.get(col) for row in rows if _notna(row.get(col))}) > 1
        ]

        has_value_column = "V" in col_set

        if varying_dims and has_value_column:
            for row in rows:
                gc = row.get("geo_code")
                if not _notna(gc):
                    continue
                geo_code = str(gc).strip()
                parts = [
                    str(row.get(dim_col)).strip()
                    for dim_col in varying_dims
                    if _notna(row.get(dim_col))
                ]
                variable_name = "_".join(parts) if parts else "Valor"
                processed_value = _parse_numeric(row.get("V"))

                if geo_code not in sidra_data_dict:
                    sidra_data_dict[geo_code] = {}

                var_key = variable_name
                counter = 1
                while var_key in sidra_data_dict[geo_code]:
                    var_key = f"{variable_name}_{counter}"
                    counter += 1

                sidra_data_dict[geo_code][var_key] = processed_value
        else:
            single_var_name = None
            if has_value_column and column_labels:
                var_col = None
                for candidate in non_geo_dim_names:
                    label = str(column_labels.get(candidate, "")).lower()
                    if "ariavel" in label or "ariável" in label:
                        var_col = candidate
                        break

                if var_col and var_col in col_set:
                    unique_vals = {
                        row.get(var_col) for row in rows if _notna(row.get(var_col))
                    }
                    if len(unique_vals) == 1:
                        single_var_name = str(next(iter(unique_vals))).strip()

            for row in rows:
                gc = row.get("geo_code")
                if not _notna(gc):
                    continue
                geo_code = str(gc).strip()

                row_data = {}
                for col in value_cols:
                    val = row.get(col)
                    if _notna(val):
                        key = single_var_name if (col == "V" and single_var_name) else col
                        row_data[key] = _parse_numeric(val)

                if row_data:
                    if geo_code in sidra_data_dict:
                        sidra_data_dict[geo_code].update(row_data)
                    else:
                        sidra_data_dict[geo_code] = row_data

        return sidra_data_dict, header_info
