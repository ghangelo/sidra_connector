# -*- coding: utf-8 -*-
"""
Cliente da API de dados do SIDRA/IBGE.

Pega a resposta da API (JSON ou XML), transforma num dicionario
{codigo_geografico: {variavel: valor}} e entrega pro DataJoiner
fazer o join com a camada vetorial.

Nao usa pandas -- tudo na mao com dicts e listas.
"""

import requests
import defusedxml.ElementTree as ET
import re

from ..utils import constants

try:
    from qgis.core import QgsMessageLog, Qgis
    QGIS_AVAILABLE = True
except ImportError:
    QGIS_AVAILABLE = False


def _notna(value):
    """Verifica se o valor nao eh None nem string vazia."""
    return value is not None and value != ''


def _parse_numeric(value_str):
    """Tenta converter pra float, lidando com formato brasileiro (1.234,56).

    Se nao conseguir, retorna a string original. Se for vazio, retorna None.
    """
    if value_str is None or value_str == '':
        return None
    try:
        normalized = str(value_str).strip()
        if ',' in normalized:
            normalized = normalized.replace('.', '').replace(',', '.')
        return float(normalized)
    except (ValueError, TypeError):
        return str(value_str) if _notna(value_str) else None


class SidraApiClient:
    """Busca dados tabulares da API SIDRA e transforma pra join.

    Aceita URL completa ou so o codigo da tabela.
    """

    def __init__(self, table_query):
        self.full_query_url = None
        if isinstance(table_query, str) and table_query.startswith('http'):
            self.full_query_url = table_query
            match = re.search(r'/t/(\d+)', table_query)
            if match:
                self.table_code = int(match.group(1))
            else:
                raise ValueError(f"Nao achei o codigo da tabela na URL: {table_query}")
        else:
            self.table_code = int(table_query)

        self.base_url = f"https://apisidra.ibge.gov.br/values/t/{self.table_code}"

    def fetch_and_parse(self, params: dict = None) -> tuple:
        """Faz a requisicao e retorna (dados, header_info)."""

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

        try:
            response = requests.get(final_url, timeout=constants.API_TIMEOUT)
            response.raise_for_status()
        except requests.exceptions.Timeout:
            raise TimeoutError(f"Timeout ao chamar a API: {final_url}")
        except requests.exceptions.ConnectionError:
            raise ConnectionError(f"Sem conexao com a API: {final_url}")
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else '?'
            raise requests.exceptions.HTTPError(
                f"Erro HTTP {status}: {final_url}",
                response=e.response,
            ) from e
        except requests.exceptions.RequestException as e:
            raise requests.exceptions.RequestException(f"Erro na requisicao: {e}")

        # A API pode devolver XML (raro) ou JSON (padrao)
        if response.headers.get('Content-Type', '').startswith('application/xml'):
            return self._parse_xml(response.text)

        data = response.json()
        # Primeira posicao eh sempre o cabecalho, dados comecam na segunda
        if not data or len(data) <= 1:
            return {}, {}

        header = data[0]
        rows = data[1:]

        columns = list(header.keys())
        table_data = [{col: row.get(col) for col in columns} for row in rows]

        column_mapping = {k: v for k, v in header.items()}
        if QGIS_AVAILABLE:
            QgsMessageLog.logMessage(f"Colunas: {column_mapping}", "SIDRA Connector", Qgis.Info)

        sidra_data_dict, header_info = self._convert_rows_to_dict(table_data, columns, column_mapping)
        return sidra_data_dict, header_info

    def _parse_xml(self, xml_string: str) -> tuple:
        """Parseia resposta XML (formato antigo da API)."""
        root = ET.fromstring(xml_string)
        namespace = {'ns': 'http://schemas.datacontract.org/2004/07/IBGE.BTE.Tabela'}

        header_element = root.find('ns:ValorDescritoPorSuasDimensoes', namespace)
        if header_element is None:
            return {}, {}

        header_map = {child.tag.split('}')[-1]: child.text for child in header_element}
        data_elements = root.findall('ns:ValorDescritoPorSuasDimensoes', namespace)[1:]

        all_rows = []
        for elem in data_elements:
            row_data = {header_map.get(child.tag.split('}')[-1], child.tag.split('}')[-1]): child.text for child in elem}
            all_rows.append(row_data)

        if not all_rows:
            return {}, {}

        columns = list(all_rows[0].keys())

        # Descobre qual coluna tem o codigo geografico
        geo_code_col = None
        niveis_geograficos = [
            'brasil', 'grande regiao', 'unidade da federacao',
            'regiao metropolitana', 'regiao integrada de desenvolvimento',
            'microrregiao geografica', 'mesorregiao geografica',
            'regiao geografica imediata', 'regiao geografica intermediaria',
            'municipio', 'distrito', 'subdistrito', 'bairro', 'setor censitario'
        ]

        for i in range(1, 10):
            dim_name = f'D{i}N'
            dim_code = f'D{i}C'
            if header_map.get(dim_name) and any(k in header_map[dim_name].lower() for k in niveis_geograficos):
                geo_code_col = header_map.get(dim_code)
                break

        if geo_code_col is None:
            raise ValueError("Nao achei a coluna de codigo geografico no XML.")

        if geo_code_col in columns:
            columns = ['geo_code' if c == geo_code_col else c for c in columns]
            for row in all_rows:
                if geo_code_col in row:
                    row['geo_code'] = row.pop(geo_code_col)

        return self._convert_rows_to_dict(all_rows, columns, {})

    def _convert_rows_to_dict(self, rows: list, columns: list, column_labels: dict = None) -> tuple:
        """Transforma as linhas da API no dicionario de lookup.

        Se tem varias variaveis, agrupa por nome da variavel.
        Se tem so uma, usa o nome dela (nao deixa ficar "V").
        """
        n_rows = len(rows)
        n_cols = len(columns)

        if QGIS_AVAILABLE:
            QgsMessageLog.logMessage(f"Convertendo {n_rows} linhas x {n_cols} colunas", "SIDRA Connector", Qgis.Info)

        if not rows:
            if QGIS_AVAILABLE:
                QgsMessageLog.logMessage("Nenhum dado recebido", "SIDRA Connector", Qgis.Warning)
            return {}, {}

        sidra_data_dict = {}
        header_info = {}

        # Colunas que sao metadados, nao valores numericos
        excluded_cols = {
            'geo_code',
            'D1C', 'D1N', 'D2C', 'D2N', 'D3C', 'D3N', 'D4C', 'D4N',
            'D5C', 'D5N', 'D6C', 'D6N', 'D7C', 'D7N',
            'NC', 'NN', 'MC', 'MN',
        }

        col_set = set(columns)

        # Descobre quais colunas tem valores numericos
        value_cols = []
        if 'V' in col_set:
            value_cols.append('V')
        for col in columns:
            if col not in excluded_cols and col not in value_cols:
                value_cols.append(col)

        if QGIS_AVAILABLE:
            QgsMessageLog.logMessage(f"Colunas de valor: {value_cols}", "SIDRA Connector", Qgis.Info)

        # Pega info descritiva da primeira linha (nomes das dimensoes)
        first_row = rows[0]
        for col in columns:
            if col.endswith('N'):
                val = first_row.get(col)
                header_info[col] = val if _notna(val) else col

        # Acha a coluna de codigo geografico
        geo_code_col = None
        if 'geo_code' not in col_set:
            if QGIS_AVAILABLE:
                QgsMessageLog.logMessage("'geo_code' nao encontrado, procurando alternativa...", "SIDRA Connector", Qgis.Warning)

            if 'D1C' in col_set:
                geo_code_col = 'D1C'
            else:
                geo_candidates = [col for col in columns if col.endswith('C') and any(dim in col for dim in ['D1', 'D2', 'D3', 'D4'])]
                if geo_candidates:
                    geo_code_col = geo_candidates[0]
                else:
                    if QGIS_AVAILABLE:
                        QgsMessageLog.logMessage("Nenhuma coluna geografica encontrada!", "SIDRA Connector", Qgis.Critical)
                    return {}, header_info

            # Renomeia pra 'geo_code' em todas as linhas
            for row in rows:
                if geo_code_col in row:
                    row['geo_code'] = row.pop(geo_code_col)

        rows_processed = 0

        # Procura coluna de variavel (a que tem mais de 1 valor unico)
        # Ordem de prioridade: D4N > D3N > D2N > D5N > D6N > D7N
        variable_column = None
        variable_candidates = ['D4N', 'D3N', 'D2N', 'D5N', 'D6N', 'D7N']

        for candidate in variable_candidates:
            if candidate in col_set:
                unique_values = len(set(row.get(candidate) for row in rows))
                if unique_values > 1:
                    variable_column = candidate
                    break

        has_value_column = 'V' in col_set

        if QGIS_AVAILABLE:
            QgsMessageLog.logMessage(f"Coluna de variavel: {variable_column}", "SIDRA Connector", Qgis.Info)

        # Caminho 1: varias variaveis empilhadas na coluna V
        if variable_column and has_value_column:
            for row in rows:
                gc = row.get('geo_code')
                if _notna(gc):
                    geo_code = str(gc).strip()
                    var_val = row.get(variable_column)
                    variable_name = str(var_val).strip() if _notna(var_val) else 'Valor'
                    value = row.get('V')

                    if geo_code not in sidra_data_dict:
                        sidra_data_dict[geo_code] = {}

                    processed_value = _parse_numeric(value)

                    # Se ja existe essa variavel pro mesmo geo_code, adiciona sufixo
                    var_key = variable_name
                    counter = 1
                    while var_key in sidra_data_dict[geo_code]:
                        var_key = f"{variable_name}_{counter}"
                        counter += 1

                    sidra_data_dict[geo_code][var_key] = processed_value
                    rows_processed += 1
        else:
            # Caminho 2: so 1 variavel (ou nenhuma coluna de agrupamento)
            # Nesse caso, tenta descobrir o nome da variavel pelo header
            # pra nao ficar uma coluna generica chamada "V"
            single_var_name = None
            if has_value_column and not variable_column and column_labels:
                # Procura no header qual coluna eh rotulada "Variavel"
                var_col = None
                for candidate in variable_candidates:
                    label = str(column_labels.get(candidate, '')).lower()
                    if 'ariavel' in label or 'ariável' in label:
                        var_col = candidate
                        break

                if var_col and var_col in col_set:
                    unique_vals = set(row.get(var_col) for row in rows)
                    unique_vals.discard(None)
                    unique_vals.discard('')
                    if len(unique_vals) == 1:
                        single_var_name = str(next(iter(unique_vals))).strip()

            if single_var_name and QGIS_AVAILABLE:
                QgsMessageLog.logMessage(
                    f"Variavel unica: '{single_var_name}'",
                    "SIDRA Connector", Qgis.Info,
                )

            for row in rows:
                gc = row.get('geo_code')
                if _notna(gc):
                    geo_code = str(gc).strip()

                    row_data = {}
                    for col in value_cols:
                        val = row.get(col)
                        if _notna(val):
                            # Usa o nome real da variavel em vez de "V"
                            key = single_var_name if (col == 'V' and single_var_name) else col
                            row_data[key] = _parse_numeric(val)

                    if row_data:
                        if geo_code in sidra_data_dict:
                            sidra_data_dict[geo_code].update(row_data)
                        else:
                            sidra_data_dict[geo_code] = row_data
                        rows_processed += 1

        if QGIS_AVAILABLE:
            QgsMessageLog.logMessage(f"Pronto: {len(sidra_data_dict)} localidades, {rows_processed} linhas", "SIDRA Connector", Qgis.Info)
            if len(sidra_data_dict) > 0:
                sample_key = next(iter(sidra_data_dict.keys()))
                sample_data = sidra_data_dict[sample_key]
                variables = list(sample_data.keys())
                QgsMessageLog.logMessage(f"Exemplo: {sample_key} -> {variables}", "SIDRA Connector", Qgis.Info)

        return sidra_data_dict, header_info
