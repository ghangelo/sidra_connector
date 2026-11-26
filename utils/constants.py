# -*- coding: utf-8 -*-
"""
Constantes usadas pelo plugin todo.

Tem os mapeamentos de UF, tipos de malha, níveis territoriais,
URLs do IBGE e configurações de timeout.
"""

# UFs do Brasil (nome -> sigla)
# "Brasil" ta aqui pra quando quiser baixar malha nacional
UFS = {
    "Brasil": "BR", "Acre": "AC", "Alagoas": "AL", "Amapá": "AP",
    "Amazonas": "AM", "Bahia": "BA", "Ceará": "CE", "Distrito Federal": "DF",
    "Espírito Santo": "ES", "Goiás": "GO", "Maranhão": "MA", "Mato Grosso": "MT",
    "Mato Grosso do Sul": "MS", "Minas Gerais": "MG", "Pará": "PA",
    "Paraíba": "PB", "Paraná": "PR", "Pernambuco": "PE", "Piauí": "PI",
    "Rio de Janeiro": "RJ", "Rio Grande do Norte": "RN", "Rio Grande do Sul": "RS",
    "Rondônia": "RO", "Roraima": "RR", "Santa Catarina": "SC", "São Paulo": "SP",
    "Sergipe": "SE", "Tocantins": "TO",
}

# Tipos de malha (o que aparece na tela -> nome do arquivo no servidor)
MALHAS = {
    "Municípios": "Municipios",
    "Unidades da Federação": "UF",
    "Grandes Regiões": "Regioes",
    "Regiões Geográficas Imediatas": "RG_Imediatas",
    "Regiões Geográficas Intermediárias": "RG_Intermediarias",
    "País": "Pais",
}

# Malhas que só existem no escopo nacional ("Brasil")
MALHAS_BRASIL_ONLY = {"País", "Grandes Regiões"}

# Códigos oficiais de níveis territoriais do SIDRA/IBGE (N{id} -> Nome)
SIDRA_TERRITORIAL_LEVELS = {
    1: "Brasil",
    2: "Grande Região",
    3: "Unidade da Federação",
    6: "Município",
    7: "Região Metropolitana",
    8: "Mesorregião Geográfica",
    9: "Microrregião Geográfica",
    10: "Distrito",
    11: "Subdistrito",
    13: "Região Metropolitana e Subdivisão",
    14: "Região Integrada de Desenvolvimento (RIDE)",
    15: "Aglomeração Urbana",
    17: "Aglomerado Subnormal",
    18: "Área de Ponderação",
    23: "Arranjo Populacional",
    24: "Região Geográfica Intermediária",
    25: "Região Geográfica Imediata",
    102: "Bairro",
    131: "Amazônia Legal",
    134: "Semiárido",
}

SIDRA_PREFERRED_LEVEL_ORDER = (
    1, 2, 3, 6, 7, 8, 9, 24, 25, 10, 11, 102, 13, 14, 15, 23, 17, 18, 131, 134,
)

# URLs do GeoFTP do IBGE
IBGE_MESH_BASE_URL_PARENT = (
    "https://geoftp.ibge.gov.br/organizacao_do_territorio/"
    "malhas_territoriais/malhas_municipais/"
)

# {ano} eh substituido na hora de montar a URL
IBGE_MESH_BASE_URL = IBGE_MESH_BASE_URL_PARENT + "municipio_{ano}/"

# Timeouts
API_TIMEOUT = 30        # Segundos pra chamadas de API
DOWNLOAD_TIMEOUT = 300  # Segundos pra downloads de malha
CHUNK_SIZE = 65536      # Bytes por pedaco no download

SIDRA_VALUES_BASE_URL = "https://apisidra.ibge.gov.br/values"
IBGE_AGGREGATES_BASE_URL = "https://servicodados.ibge.gov.br/api/v3/agregados"

HTTP_CONNECT_TIMEOUT_SECONDS = 15
HTTP_READ_TIMEOUT_SECONDS = 60
HTTP_MAX_RETRIES = 3
HTTP_RETRY_BASE_DELAY_SECONDS = 1.0
HTTP_RETRY_STATUS_CODES = {408, 429, 500, 502, 503, 504}
DEFAULT_PERIOD_BATCH_SIZE = 12
MAX_RESPONSE_BYTES = 100 * 1024 * 1024
PLUGIN_USER_AGENT = (
    "SIDRA-Connector-QGIS/2.2.3 "
    "(https://github.com/ghangelo/sidra_connector)"
)
CHECKPOINT_SCHEMA_VERSION = 1
