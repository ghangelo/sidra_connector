# -*- coding: utf-8 -*-
"""
Cliente HTTP para a API de dados do SIDRA/IBGE.

Este módulo converte a resposta da API (JSON ou XML) num dicionário
de lookup ``{geo_code: {variavel: valor}}`` pronto para ser unido a
camadas vetoriais pelo ``DataJoiner``.

A implementação é **pure-Python** (sem pandas).  Duas funções auxiliares
de nível de módulo (``_notna``, ``_parse_numeric``) substituem as
utilidades equivalentes do pandas.

Endpoint de dados:
    ``https://apisidra.ibge.gov.br/values/t/{table}/...``
"""

import requests
import xml.etree.ElementTree as ET
import re

try:
    from qgis.core import QgsMessageLog, Qgis
    QGIS_AVAILABLE = True
except ImportError:
    # Permite importar fora do QGIS (testes, scripts standalone)
    QGIS_AVAILABLE = False


# ---------------------------------------------------------------------------
#  Funções auxiliares (substituem pandas)
# ---------------------------------------------------------------------------

def _notna(value):
    """Retorna ``True`` se *value* não for ``None`` nem string vazia.

    Equivalente simplificado de ``pandas.notna()``.
    """
    return value is not None and value != ''


def _parse_numeric(value_str):
    """Converte string numérica (possivelmente com vírgula) em ``float``.

    :param value_str: Valor bruto vindo da API.
    :returns: ``float`` se conversível, ``str`` original caso contrário,
        ou ``None`` se vazio.
    """
    if value_str is None or value_str == '':
        return None
    try:
        # A API SIDRA usa vírgula como separador decimal
        normalized = str(value_str).replace(',', '.')
        if normalized.replace('.', '').replace('-', '').isdigit():
            return float(normalized)
        return str(value_str)
    except (ValueError, TypeError):
        return str(value_str) if _notna(value_str) else None


# ---------------------------------------------------------------------------
#  Cliente SIDRA
# ---------------------------------------------------------------------------

class SidraApiClient:
    """Busca e transforma dados tabulares da API SIDRA.

    Aceita tanto uma URL completa da API quanto um código numérico de
    tabela.  O resultado é sempre um dicionário de lookup indexado por
    código geográfico, adequado para junção espacial.
    """

    def __init__(self, table_query):
        """Inicializa o cliente.

        :param table_query: Código da tabela (``int``) ou URL completa
            da API SIDRA (``str`` começando por ``http``).
        :raises ValueError: Se a URL não contiver ``/t/<código>``.
        """
        self.full_query_url = None
        if isinstance(table_query, str) and table_query.startswith('http'):
            self.full_query_url = table_query
            match = re.search(r'/t/(\d+)', table_query)
            if match:
                self.table_code = int(match.group(1))
            else:
                raise ValueError(f"Não foi possível extrair o código da tabela da URL: {table_query}")
        else:
            self.table_code = int(table_query)

        self.base_url = f"https://apisidra.ibge.gov.br/values/t/{self.table_code}"

    def fetch_and_parse(self, params: dict = None) -> tuple:
        """Faz a requisição HTTP, detecta o formato (JSON/XML) e transforma.

        :param params: Parâmetros extras da consulta (ignorados quando o
            cliente foi inicializado com uma URL completa).
        :returns: Tupla ``(sidra_data_dict, header_info)``.
        :raises TimeoutError: Timeout na requisição.
        :raises ConnectionError: Erro de rede.
        :raises requests.exceptions.HTTPError: Código HTTP ≥ 400.
        """

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
            response = requests.get(final_url, timeout=30)
            response.raise_for_status()
        except requests.exceptions.Timeout:
            raise TimeoutError(f"Timeout na requisição à API SIDRA: {final_url}")
        except requests.exceptions.ConnectionError:
            raise ConnectionError(f"Erro de conexão com a API SIDRA: {final_url}")
        except requests.exceptions.HTTPError as e:
            raise requests.exceptions.HTTPError(f"Erro HTTP na API SIDRA: {e.response.status_code} - {final_url}")
        except requests.exceptions.RequestException as e:
            raise requests.exceptions.RequestException(f"Erro na requisição à API SIDRA: {e}")

        # A API pode retornar XML (legado) ou JSON (padrão)
        if response.headers.get('Content-Type', '').startswith('application/xml'):
            return self._parse_xml(response.text)

        data = response.json()
        # A primeira posição do array é sempre o cabeçalho descritivo;
        # se não houver dados reais, retorna vazio.
        if not data or len(data) <= 1:
            return {}, {}

        header = data[0]   # Dict com nomes legíveis das colunas
        rows = data[1:]     # Linhas de dados propriamente ditas

        # Normaliza para lista de dicts uniformes
        columns = list(header.keys())
        table_data = [{col: row.get(col) for col in columns} for row in rows]

        # Log do mapeamento coluna-código → coluna-nome para debug
        column_mapping = {k: v for k, v in header.items()}
        if QGIS_AVAILABLE:
            QgsMessageLog.logMessage(f"Mapeamento de colunas: {column_mapping}", "SIDRA Connector", Qgis.Info)

        sidra_data_dict, header_info = self._convert_rows_to_dict(table_data, columns)
        return sidra_data_dict, header_info

    def _parse_xml(self, xml_string: str) -> tuple:
        """Analisa resposta XML (formato legado) da API SIDRA.

        :param xml_string: Corpo XML completo da resposta.
        :returns: Tupla ``(sidra_data_dict, header_info)``.
        """
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

        # Identifica coluna de código geográfico
        geo_code_col = None
        niveis_geograficos = [
            'brasil', 'grande região', 'unidade da federação',
            'região metropolitana', 'região integrada de desenvolvimento',
            'microrregião geográfica', 'mesorregião geográfica',
            'região geográfica imediata', 'região geográfica intermediária',
            'município', 'distrito', 'subdistrito', 'bairro', 'setor censitário'
        ]

        for i in range(1, 10):
            dim_name = f'D{i}N'
            dim_code = f'D{i}C'
            if header_map.get(dim_name) and any(k in header_map[dim_name].lower() for k in niveis_geograficos):
                geo_code_col = header_map.get(dim_code)
                break

        if geo_code_col is None:
            raise ValueError("Erro: Não foi possível identificar a coluna de código geográfico no cabeçalho do XML.")

        # Renomeia coluna geográfica para 'geo_code'
        if geo_code_col in columns:
            columns = ['geo_code' if c == geo_code_col else c for c in columns]
            for row in all_rows:
                if geo_code_col in row:
                    row['geo_code'] = row.pop(geo_code_col)

        return self._convert_rows_to_dict(all_rows, columns)

    def _convert_rows_to_dict(self, rows: list, columns: list) -> tuple:
        """Transforma linhas tabulares num dict de lookup por código geográfico.

        Estratégia de processamento:
        - Se existir uma coluna de variável com múltiplos valores únicos
          (ex.: ``D4N``) **e** uma coluna ``V``, agrupa por variável.
        - Caso contrário (fallback), trata cada coluna de valor como
          campo independente.

        :param rows: Lista de dicts (uma entrada por linha da API).
        :param columns: Nomes das colunas na ordem original.
        :returns: Tupla ``(sidra_data_dict, header_info)``.
        """
        n_rows = len(rows)
        n_cols = len(columns)

        if QGIS_AVAILABLE:
            QgsMessageLog.logMessage(f"Iniciando conversão dos dados. Shape: ({n_rows}, {n_cols})", "SIDRA Connector", Qgis.Info)
            QgsMessageLog.logMessage(f"Colunas disponíveis: {columns}", "SIDRA Connector", Qgis.Info)

        if not rows:
            if QGIS_AVAILABLE:
                QgsMessageLog.logMessage("Dados vazios recebidos da API SIDRA", "SIDRA Connector", Qgis.Warning)
            return {}, {}

        sidra_data_dict = {}
        header_info = {}

        # Colunas de dimensão/metadados — não representam valores numéricos
        excluded_cols = {
            'geo_code', 'D1C', 'D1N', 'D2C', 'D2N', 'D3C', 'D3N', 'D4C', 'D4N',
            'NC', 'NN', 'MC', 'MN'
        }

        col_set = set(columns)

        # Identificar colunas que contêm valores numéricos de interesse
        value_cols = []
        if 'V' in col_set:
            value_cols.append('V')  # Coluna padrão de valor da API

        for col in columns:
            if col not in excluded_cols and col not in value_cols:
                value_cols.append(col)

        if QGIS_AVAILABLE:
            QgsMessageLog.logMessage(f"Colunas de valores identificadas: {value_cols}", "SIDRA Connector", Qgis.Info)

        # Extrair metadados descritivos (colunas terminadas em 'N') da 1ª linha
        first_row = rows[0]
        for col in columns:
            if col.endswith('N'):
                val = first_row.get(col)
                header_info[col] = val if _notna(val) else col

        # --- Identificar coluna de código geográfico ---
        # A API retorna o geo code em D1C (mais comum), D2C etc.
        geo_code_col = None
        if 'geo_code' not in col_set:
            if QGIS_AVAILABLE:
                QgsMessageLog.logMessage("Coluna 'geo_code' não encontrada. Tentando identificar coluna geográfica...", "SIDRA Connector", Qgis.Warning)

            if 'D1C' in col_set:
                geo_code_col = 'D1C'
                if QGIS_AVAILABLE:
                    QgsMessageLog.logMessage("Usando coluna 'D1C' como código geográfico", "SIDRA Connector", Qgis.Info)
            else:
                geo_candidates = [col for col in columns if col.endswith('C') and any(dim in col for dim in ['D1', 'D2', 'D3', 'D4'])]
                if geo_candidates:
                    geo_code_col = geo_candidates[0]
                    if QGIS_AVAILABLE:
                        QgsMessageLog.logMessage(f"Usando coluna '{geo_code_col}' como código geográfico", "SIDRA Connector", Qgis.Info)
                else:
                    if QGIS_AVAILABLE:
                        QgsMessageLog.logMessage("Nenhuma coluna geográfica identificada", "SIDRA Connector", Qgis.Critical)
                    return {}, header_info

            # Renomeia a coluna geográfica em todas as linhas
            for row in rows:
                if geo_code_col in row:
                    row['geo_code'] = row.pop(geo_code_col)
        else:
            geo_code_col = 'geo_code'

        rows_processed = 0

        # --- Detectar coluna de variável para agrupamento ---
        # A coluna de variável é aquela (D2N..D7N) que apresenta mais de
        # um valor único, indicando que os dados estão "empilhados" por
        # variável. Prioridade: D4N > D3N > D2N > D5N > D6N > D7N.
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
            QgsMessageLog.logMessage(f"Coluna de variável identificada: {variable_column}", "SIDRA Connector", Qgis.Info)
            QgsMessageLog.logMessage(f"Tem coluna de valor (V): {has_value_column}", "SIDRA Connector", Qgis.Info)

        # ===== Caminho 1: dados empilhados por variável =====
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

                    var_key = variable_name
                    counter = 1
                    while var_key in sidra_data_dict[geo_code]:
                        var_key = f"{variable_name}_{counter}"
                        counter += 1

                    sidra_data_dict[geo_code][var_key] = processed_value
                    rows_processed += 1
        else:
            if QGIS_AVAILABLE:
                QgsMessageLog.logMessage("Usando lógica de fallback - sem agrupamento por variável", "SIDRA Connector", Qgis.Info)

            for row in rows:
                gc = row.get('geo_code')
                if _notna(gc):
                    geo_code = str(gc).strip()

                    row_data = {}
                    for col in value_cols:
                        val = row.get(col)
                        if _notna(val):
                            row_data[col] = _parse_numeric(val)

                    if row_data:
                        sidra_data_dict[geo_code] = row_data
                        rows_processed += 1

        if QGIS_AVAILABLE:
            QgsMessageLog.logMessage(f"Conversão concluída: {len(sidra_data_dict)} registros geográficos, {rows_processed} linhas processadas", "SIDRA Connector", Qgis.Info)
            if len(sidra_data_dict) > 0:
                sample_key = next(iter(sidra_data_dict.keys()))
                sample_data = sidra_data_dict[sample_key]
                variables = list(sample_data.keys())
                QgsMessageLog.logMessage(f"Exemplo de dados: geo_code={sample_key}, variáveis={variables}", "SIDRA Connector", Qgis.Info)

        return sidra_data_dict, header_info
