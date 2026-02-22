# -*- coding: utf-8 -*-
"""
Constantes globais do plugin SIDRA Connector.

Centraliza mapeamentos geográficos, URLs do IBGE e parâmetros de rede
utilizados pelos módulos ``mesh_downloader``, ``task_manager`` e
``main_dialog``.
"""

# ---------------------------------------------------------------------------
#  Mapeamento de UFs (nome completo → sigla IBGE)
#  Inclui "Brasil" para download de malhas nacionais.
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
#  Tipos de malha disponíveis (rótulo da UI → prefixo do arquivo no GeoFTP)
# ---------------------------------------------------------------------------
MALHAS = {
    "Municípios": "Municipios",
    "Unidades da Federação": "UF",
    "Grandes Regiões": "Regioes",
    "Regiões Geográficas Imediatas": "RG_Imediatas",
    "Regiões Geográficas Intermediárias": "RG_Intermediarias",
    "País": "Pais",
}

# ---------------------------------------------------------------------------
#  URLs do GeoFTP / IBGE para malhas territoriais
# ---------------------------------------------------------------------------
IBGE_MESH_BASE_URL_PARENT = (
    "https://geoftp.ibge.gov.br/organizacao_do_territorio/"
    "malhas_territoriais/malhas_municipais/"
)

# {ano} é substituído em tempo de execução por main_dialog.py
IBGE_MESH_BASE_URL = IBGE_MESH_BASE_URL_PARENT + "municipio_{ano}/"

# ---------------------------------------------------------------------------
#  Parâmetros de rede e performance
# ---------------------------------------------------------------------------
API_TIMEOUT = 30        # Timeout (s) para requisições de metadados / API SIDRA
DOWNLOAD_TIMEOUT = 300  # Timeout (s) para download de malhas (.zip)
CHUNK_SIZE = 65536      # Tamanho do bloco (bytes) para streaming de download
