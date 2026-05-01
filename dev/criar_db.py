# -*- coding: utf-8 -*-
"""
Script de desenvolvimento para gerar o banco de dados SQLite de agregados do IBGE.

Baixa automaticamente o JSON de agregados diretamente da API do IBGE:
    https://servicodados.ibge.gov.br/api/v3/agregados

E gera o arquivo 'agregados_ibge.db' utilizado pelo plugin SIDRA Connector
para busca local de tabelas (por nome ou ID).

Estrutura do JSON da API:
    [
        {
            "id": "CL",
            "nome": "Cadastro Central de Empresas",
            "agregados": [
                {"id": "1685", "nome": "Unidades locais, ..."},
                ...
            ]
        },
        ...
    ]

Estrutura do banco gerado:
    - grupos(id TEXT PK, nome TEXT)         -- pesquisas/temas do IBGE
    - agregados(id INTEGER PK, nome TEXT, grupo_id TEXT FK) -- tabelas SIDRA

Uso:
    python criar_db.py
"""

import json
import sqlite3
import os
import urllib.request
import urllib.error

# URL oficial da API v3 de agregados do IBGE
IBGE_AGREGADOS_URL = "https://servicodados.ibge.gov.br/api/v3/agregados"

# Nome do arquivo de banco de dados gerado
DB_FILENAME = "agregados_ibge.db"


def baixar_agregados(url: str) -> list:
    """
    Baixa a lista completa de agregados (tabelas) do IBGE via API REST.

    Faz uma requisição GET ao endpoint de agregados e retorna o JSON
    desserializado como lista de dicionários (cada um representando um
    grupo/pesquisa com seus agregados aninhados).

    Args:
        url: URL do endpoint da API de agregados do IBGE.

    Returns:
        Lista de dicionários no formato da API.

    Raises:
        ConnectionError: Se houver falha de rede ou resposta HTTP de erro.
        ValueError: Se a resposta não for um JSON válido ou a estrutura
                    não for a esperada (lista de grupos).
    """
    print(f"Baixando agregados de: {url}")

    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = resp.read()
            if resp.info().get('Content-Encoding') == 'gzip':
                import gzip
                data = gzip.decompress(data)
            corpo = data.decode("utf-8")
    except urllib.error.HTTPError as e:
        raise ConnectionError(f"Erro HTTP {e.code}: {e.reason}") from e
    except urllib.error.URLError as e:
        raise ConnectionError(f"Erro de conexão: {e.reason}") from e

    try:
        dados = json.loads(corpo)
    except json.JSONDecodeError as e:
        raise ValueError(f"Resposta da API não é um JSON válido: {e}") from e

    if not isinstance(dados, list):
        raise ValueError(
            "Estrutura de JSON inesperada. Esperava-se uma lista de grupos."
        )

    print(f"  -> {len(dados)} grupos de pesquisa encontrados.")
    return dados


def criar_conexao(db_file: str) -> sqlite3.Connection:
    """
    Abre (ou cria) o arquivo SQLite e retorna a conexão.

    Args:
        db_file: Caminho do arquivo .db a ser criado/aberto.

    Returns:
        Conexão sqlite3 aberta.

    Raises:
        sqlite3.Error: Se não for possível conectar ao banco.
    """
    conn = sqlite3.connect(db_file)
    # Ativa WAL para melhor desempenho de escrita em lote
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def criar_tabelas(conn: sqlite3.Connection) -> None:
    """
    Cria as tabelas 'grupos' e 'agregados' caso não existam.

    - grupos: representa as pesquisas/temas do IBGE (ex: "Censo Demográfico").
    - agregados: representa as tabelas SIDRA vinculadas a cada grupo.

    Args:
        conn: Conexão SQLite aberta.
    """
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS grupos (
            id   TEXT PRIMARY KEY,
            nome TEXT NOT NULL
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS agregados (
            id       INTEGER PRIMARY KEY,
            nome     TEXT NOT NULL,
            grupo_id TEXT NOT NULL,
            FOREIGN KEY (grupo_id) REFERENCES grupos (id)
        );
    """)

    conn.commit()


def popular_banco(conn: sqlite3.Connection, dados: list) -> None:
    """
    Insere os grupos e agregados no banco de dados.

    Percorre a lista retornada pela API e faz INSERT OR REPLACE para
    cada grupo e cada agregado, garantindo que execuções repetidas
    atualizem os dados sem duplicar registros.

    Args:
        conn: Conexão SQLite aberta.
        dados: Lista de dicionários no formato da API do IBGE.
    """
    cursor = conn.cursor()
    total_grupos = 0
    total_agregados = 0

    for grupo in dados:
        grupo_id = grupo.get("id")
        grupo_nome = grupo.get("nome")

        if not grupo_id or not grupo_nome:
            continue

        cursor.execute(
            "INSERT OR REPLACE INTO grupos (id, nome) VALUES (?, ?)",
            (grupo_id, grupo_nome),
        )
        total_grupos += 1

        for agregado in grupo.get("agregados", []):
            agregado_id = agregado.get("id")
            agregado_nome = agregado.get("nome")

            if not agregado_id or not agregado_nome:
                continue

            cursor.execute(
                "INSERT OR REPLACE INTO agregados (id, nome, grupo_id) VALUES (?, ?, ?)",
                (int(agregado_id), agregado_nome, grupo_id),
            )
            total_agregados += 1

    conn.commit()
    print(f"  -> {total_grupos} grupos e {total_agregados} agregados inseridos.")


def main():
    """
    Fluxo principal: baixa os dados da API, cria o banco e popula as tabelas.

    O banco é gerado no mesmo diretório deste script para depois ser
    copiado manualmente para a raiz do plugin.
    """
    # Gera o banco no mesmo diretório do script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, DB_FILENAME)

    # Se já existir, remove para recriar do zero
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Banco anterior removido: {db_path}")

    print("=" * 60)
    print("  Gerador do banco de agregados IBGE para o SIDRA Connector")
    print("=" * 60)

    # 1. Baixar dados da API
    dados = baixar_agregados(IBGE_AGREGADOS_URL)

    # 2. Criar banco e tabelas
    conn = criar_conexao(db_path)
    criar_tabelas(conn)

    # 3. Inserir dados
    popular_banco(conn, dados)

    # 4. Otimizar e fechar
    conn.execute("VACUUM;")
    conn.close()

    print(f"\nBanco de dados gerado com sucesso: {db_path}")
    print(f"Copie o arquivo '{DB_FILENAME}' para a raiz do plugin.")


if __name__ == "__main__":
    main()
