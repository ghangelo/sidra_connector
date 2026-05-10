# -*- coding: utf-8 -*-
"""
Constantes usadas pelo plugin todo.

Tem os mapeamentos de UF, tipos de malha, URLs do IBGE
e configuracoes de timeout.
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
