# -*- coding: utf-8 -*-
"""
Script de desenvolvimento para gerar o banco de dados SQLite **completo**
de agregados do IBGE, incluindo todas as variáveis de consulta.

Diferente do ``criar_db.py`` (que grava apenas grupos e agregados),
este script também busca, para **cada agregado**, os metadados detalhados:
    - Variáveis (id, nome, unidade)
    - Períodos disponíveis (id, literal, modificação)
    - Níveis geográficos (sigla, administrativos/especiais/IBGE)
    - Classificações e suas categorias (id, nome, nível hierárquico)

APIs utilizadas:
    - Lista de agregados:
        GET https://servicodados.ibge.gov.br/api/v3/agregados
    - Metadados por agregado:
        GET https://servicodados.ibge.gov.br/api/v3/agregados/{id}/metadados
    - Períodos por agregado:
        GET https://servicodados.ibge.gov.br/api/v3/agregados/{id}/periodos

Estrutura do banco gerado (``agregados_ibge_completo.db``):

    grupos           (id TEXT PK, nome TEXT)
    agregados        (id INTEGER PK, nome TEXT, pesquisa TEXT, assunto TEXT,
                      periodicidade TEXT, grupo_id TEXT FK)
    variaveis        (id INTEGER, agregado_id INTEGER, nome TEXT, unidade TEXT,
                      PK(id, agregado_id))
    periodos         (id TEXT, agregado_id INTEGER, literal TEXT,
                      modificacao TEXT, PK(id, agregado_id))
    niveis_geo       (sigla TEXT, agregado_id INTEGER, tipo TEXT,
                      PK(sigla, agregado_id))
    classificacoes   (id INTEGER, agregado_id INTEGER, nome TEXT,
                      PK(id, agregado_id))
    categorias       (id INTEGER, classificacao_id INTEGER, agregado_id INTEGER,
                      nome TEXT, unidade TEXT, nivel INTEGER,
                      PK(id, classificacao_id, agregado_id))

Funcionalidades:
    - **Resumo automático**: pula agregados cujos metadados já estão no banco.
      Pode ser interrompido e retomado sem perda.
    - **Rate limiting**: pausa configurável entre requisições.
    - **Workers concorrentes**: usa ThreadPoolExecutor para paralelizar
      as requisições HTTP (padrão: 4 workers).
    - **Progresso detalhado**: exibe barra e ETA.

Uso:
    python criar_db_completo.py                    # execução normal
    python criar_db_completo.py --fresh            # recria do zero
    python criar_db_completo.py --workers 8        # 8 threads
    python criar_db_completo.py --delay 0.5        # 500ms entre requests
"""

import argparse
import gzip
import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

# ──────────────────────────────────────────────────────────────────────
# Constantes
# ──────────────────────────────────────────────────────────────────────

IBGE_AGREGADOS_URL = "https://servicodados.ibge.gov.br/api/v3/agregados"
IBGE_METADADOS_URL = "https://servicodados.ibge.gov.br/api/v3/agregados/{}/metadados"
IBGE_PERIODOS_URL = "https://servicodados.ibge.gov.br/api/v3/agregados/{}/periodos"

DB_FILENAME = "agregados_ibge_completo.db"

DEFAULT_WORKERS = 4
DEFAULT_DELAY = 0.3  # segundos entre requisições (por worker)
HTTP_TIMEOUT = 60     # timeout de cada requisição


# ──────────────────────────────────────────────────────────────────────
# Utilitários de rede
# ──────────────────────────────────────────────────────────────────────

def _http_get_json(url: str, timeout: int = HTTP_TIMEOUT) -> dict | list | None:
    """Faz GET e retorna o JSON desserializado, ou None em caso de erro."""
    try:
        req = urllib.request.Request(url, headers={
            "Accept": "application/json",
            "User-Agent": "SIDRA-Connector-DBBuilder/2.0",
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
            if resp.info().get("Content-Encoding") == "gzip":
                data = gzip.decompress(data)
            return json.loads(data.decode("utf-8"))
    except (urllib.error.HTTPError, urllib.error.URLError, json.JSONDecodeError,
            TimeoutError, OSError) as e:
        return None


def baixar_agregados() -> list:
    """Baixa a lista completa de agregados (grupos + tabelas) da API."""
    print(f"Baixando lista de agregados de: {IBGE_AGREGADOS_URL}")
    dados = _http_get_json(IBGE_AGREGADOS_URL, timeout=120)
    if not isinstance(dados, list):
        raise ValueError("Resposta inesperada da API de agregados.")
    print(f"  -> {len(dados)} grupos de pesquisa encontrados.")
    return dados


# ──────────────────────────────────────────────────────────────────────
# Banco de dados — criação
# ──────────────────────────────────────────────────────────────────────

def criar_conexao(db_file: str) -> sqlite3.Connection:
    """Abre/cria o SQLite com WAL ativado."""
    conn = sqlite3.connect(db_file)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def criar_tabelas(conn: sqlite3.Connection) -> None:
    """Cria todas as tabelas necessárias (idempotente)."""
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS grupos (
            id   TEXT PRIMARY KEY,
            nome TEXT NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS agregados (
            id             INTEGER PRIMARY KEY,
            nome           TEXT NOT NULL,
            pesquisa       TEXT,
            assunto        TEXT,
            periodicidade  TEXT,
            grupo_id       TEXT NOT NULL,
            metadados_ok   INTEGER DEFAULT 0,
            FOREIGN KEY (grupo_id) REFERENCES grupos (id)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS variaveis (
            id          INTEGER NOT NULL,
            agregado_id INTEGER NOT NULL,
            nome        TEXT NOT NULL,
            unidade     TEXT,
            PRIMARY KEY (id, agregado_id),
            FOREIGN KEY (agregado_id) REFERENCES agregados (id)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS periodos (
            id          TEXT NOT NULL,
            agregado_id INTEGER NOT NULL,
            literal     TEXT,
            modificacao TEXT,
            PRIMARY KEY (id, agregado_id),
            FOREIGN KEY (agregado_id) REFERENCES agregados (id)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS niveis_geo (
            sigla       TEXT NOT NULL,
            agregado_id INTEGER NOT NULL,
            tipo        TEXT NOT NULL DEFAULT 'Administrativo',
            PRIMARY KEY (sigla, agregado_id),
            FOREIGN KEY (agregado_id) REFERENCES agregados (id)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS classificacoes (
            id          INTEGER NOT NULL,
            agregado_id INTEGER NOT NULL,
            nome        TEXT NOT NULL,
            PRIMARY KEY (id, agregado_id),
            FOREIGN KEY (agregado_id) REFERENCES agregados (id)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS categorias (
            id               INTEGER NOT NULL,
            classificacao_id INTEGER NOT NULL,
            agregado_id      INTEGER NOT NULL,
            nome             TEXT NOT NULL,
            unidade          TEXT,
            nivel            INTEGER DEFAULT 0,
            PRIMARY KEY (id, classificacao_id, agregado_id),
            FOREIGN KEY (classificacao_id, agregado_id)
                REFERENCES classificacoes (id, agregado_id)
        );
    """)

    conn.commit()


# ──────────────────────────────────────────────────────────────────────
# Banco de dados — inserção em lote (grupos + agregados básicos)
# ──────────────────────────────────────────────────────────────────────

def popular_grupos_e_agregados(conn: sqlite3.Connection, dados: list) -> list[int]:
    """
    Insere grupos e agregados básicos.  Retorna a lista de IDs de todos
    os agregados encontrados.
    """
    cur = conn.cursor()
    total_g = 0
    total_a = 0
    todos_ids: list[int] = []

    for grupo in dados:
        gid = grupo.get("id")
        gnome = grupo.get("nome")
        if not gid or not gnome:
            continue

        cur.execute(
            "INSERT OR IGNORE INTO grupos (id, nome) VALUES (?, ?)",
            (gid, gnome),
        )
        total_g += 1

        for ag in grupo.get("agregados", []):
            aid = ag.get("id")
            anome = ag.get("nome")
            if not aid or not anome:
                continue
            aid_int = int(aid)
            cur.execute(
                """INSERT OR IGNORE INTO agregados
                   (id, nome, grupo_id) VALUES (?, ?, ?)""",
                (aid_int, anome, gid),
            )
            total_a += 1
            todos_ids.append(aid_int)

    conn.commit()
    print(f"  -> {total_g} grupos e {total_a} agregados inseridos/verificados.")
    return todos_ids


def ids_pendentes(conn: sqlite3.Connection, todos_ids: list[int]) -> list[int]:
    """Retorna apenas os IDs cujo metadados ainda não foram baixados."""
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM agregados WHERE metadados_ok = 1"
    )
    prontos = {row[0] for row in cur.fetchall()}
    return [aid for aid in todos_ids if aid not in prontos]


# ──────────────────────────────────────────────────────────────────────
# Busca e gravação de metadados por agregado
# ──────────────────────────────────────────────────────────────────────

def _buscar_metadados_agregado(agregado_id: int, delay: float) -> dict | None:
    """
    Busca metadados + períodos de um agregado.
    Retorna um dict combinado ou None se falhar.
    """
    meta = _http_get_json(IBGE_METADADOS_URL.format(agregado_id))
    if meta is None:
        return None

    time.sleep(delay)

    periodos = _http_get_json(IBGE_PERIODOS_URL.format(agregado_id))
    if periodos is None:
        periodos = []

    time.sleep(delay)

    meta["_periodos_detalhados"] = periodos
    return meta


def _gravar_metadados(conn: sqlite3.Connection, agregado_id: int, meta: dict) -> None:
    """Grava metadados de um agregado no banco (transação única)."""
    cur = conn.cursor()

    # Atualiza informações extras do agregado
    pesquisa = meta.get("pesquisa", "")
    assunto = meta.get("assunto", "")
    period_info = meta.get("periodicidade", {})
    periodicidade = period_info.get("frequencia", "") if isinstance(period_info, dict) else ""

    cur.execute(
        """UPDATE agregados
           SET pesquisa = ?, assunto = ?, periodicidade = ?, metadados_ok = 1
           WHERE id = ?""",
        (pesquisa, assunto, periodicidade, agregado_id),
    )

    # ── Variáveis ──
    for var in meta.get("variaveis", []):
        vid = var.get("id")
        vnome = var.get("nome", "")
        vunidade = var.get("unidade", "")
        if vid is not None:
            cur.execute(
                """INSERT OR REPLACE INTO variaveis
                   (id, agregado_id, nome, unidade) VALUES (?, ?, ?, ?)""",
                (int(vid), agregado_id, vnome, vunidade),
            )

    # ── Períodos ──
    for per in meta.get("_periodos_detalhados", []):
        pid = per.get("id", "")
        literals = per.get("literals", [])
        literal = literals[0] if literals else pid
        modificacao = per.get("modificacao", "")
        cur.execute(
            """INSERT OR REPLACE INTO periodos
               (id, agregado_id, literal, modificacao) VALUES (?, ?, ?, ?)""",
            (str(pid), agregado_id, literal, modificacao),
        )

    # ── Níveis geográficos ──
    nivel_terr = meta.get("nivelTerritorial", {})
    if isinstance(nivel_terr, dict):
        for tipo, siglas in nivel_terr.items():
            if isinstance(siglas, list):
                for sigla in siglas:
                    cur.execute(
                        """INSERT OR REPLACE INTO niveis_geo
                           (sigla, agregado_id, tipo) VALUES (?, ?, ?)""",
                        (sigla, agregado_id, tipo),
                    )

    # ── Classificações e categorias ──
    for clas in meta.get("classificacoes", []):
        cid = clas.get("id")
        cnome = clas.get("nome", "")
        if cid is None:
            continue
        cid_int = int(cid)

        cur.execute(
            """INSERT OR REPLACE INTO classificacoes
               (id, agregado_id, nome) VALUES (?, ?, ?)""",
            (cid_int, agregado_id, cnome),
        )

        for cat in clas.get("categorias", []):
            cat_id = cat.get("id")
            cat_nome = cat.get("nome", "")
            cat_unidade = cat.get("unidade")
            cat_nivel = cat.get("nivel", 0)
            if cat_id is None:
                continue
            cur.execute(
                """INSERT OR REPLACE INTO categorias
                   (id, classificacao_id, agregado_id, nome, unidade, nivel)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (int(cat_id), cid_int, agregado_id,
                 cat_nome, cat_unidade, cat_nivel if cat_nivel else 0),
            )

    conn.commit()


# ──────────────────────────────────────────────────────────────────────
# Progresso
# ──────────────────────────────────────────────────────────────────────

def _formatar_tempo(segundos: float) -> str:
    """Formata segundos em h:mm:ss."""
    h = int(segundos // 3600)
    m = int((segundos % 3600) // 60)
    s = int(segundos % 60)
    if h:
        return f"{h}h{m:02d}m{s:02d}s"
    return f"{m}m{s:02d}s"


# ──────────────────────────────────────────────────────────────────────
# Pipeline principal
# ──────────────────────────────────────────────────────────────────────

def processar_metadados(
    conn: sqlite3.Connection,
    pendentes: list[int],
    workers: int,
    delay: float,
) -> None:
    """
    Baixa e grava metadados para cada agregado pendente.
    Usa ThreadPoolExecutor para paralelizar as requisições HTTP.
    """
    total = len(pendentes)
    if total == 0:
        print("  OK: Todos os metadados já estão no banco.")
        return

    print(f"\nBaixando metadados de {total} agregados ({workers} workers)...")

    concluidos = 0
    erros = 0
    t0 = time.time()

    # Para evitar sobrecarga no SQLite, as gravações são feitas na
    # thread principal após o download.  O ThreadPoolExecutor cuida
    # apenas das requisições HTTP.
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futuro_para_id = {
            pool.submit(_buscar_metadados_agregado, aid, delay): aid
            for aid in pendentes
        }

        for futuro in as_completed(futuro_para_id):
            aid = futuro_para_id[futuro]
            meta = futuro.result()

            if meta is not None:
                try:
                    _gravar_metadados(conn, aid, meta)
                except sqlite3.Error as e:
                    print(f"\n  [ERRO DB] Agregado {aid}: {e}")
                    erros += 1
                else:
                    concluidos += 1
            else:
                erros += 1

            # Progresso
            feitos = concluidos + erros
            pct = feitos / total * 100
            elapsed = time.time() - t0
            if concluidos > 0:
                eta = elapsed / feitos * (total - feitos)
                eta_str = _formatar_tempo(eta)
            else:
                eta_str = "---"

            sys.stdout.write(
                f"\r  [{feitos}/{total}] {pct:5.1f}%  "
                f"OK: {concluidos}  ERR: {erros}  "
                f"ETA: {eta_str}   "
            )
            sys.stdout.flush()

    elapsed = time.time() - t0
    print(f"\n  Concluído em {_formatar_tempo(elapsed)} "
          f"— {concluidos} OK, {erros} erros.")


def main():
    parser = argparse.ArgumentParser(
        description="Gera o banco completo de agregados IBGE com metadados."
    )
    parser.add_argument(
        "--fresh", action="store_true",
        help="Remove o banco existente e recria do zero.",
    )
    parser.add_argument(
        "--workers", type=int, default=DEFAULT_WORKERS,
        help=f"Número de workers HTTP (padrão: {DEFAULT_WORKERS}).",
    )
    parser.add_argument(
        "--delay", type=float, default=DEFAULT_DELAY,
        help=f"Segundos entre requests por worker (padrão: {DEFAULT_DELAY}).",
    )
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, DB_FILENAME)

    if args.fresh and os.path.exists(db_path):
        os.remove(db_path)
        print(f"Banco anterior removido: {db_path}")

    print("=" * 64)
    print("  Gerador COMPLETO do banco de agregados IBGE")
    print("  (grupos + agregados + variáveis + períodos +")
    print("   níveis geográficos + classificações + categorias)")
    print("=" * 64)

    # 1. Baixar lista de agregados
    dados = baixar_agregados()

    # 2. Criar/abrir banco
    conn = criar_conexao(db_path)
    criar_tabelas(conn)

    # 3. Inserir grupos e agregados básicos
    todos_ids = popular_grupos_e_agregados(conn, dados)

    # 4. Determinar quais ainda precisam de metadados
    pendentes = ids_pendentes(conn, todos_ids)
    print(f"  -> {len(todos_ids)} agregados totais, "
          f"{len(todos_ids) - len(pendentes)} já processados, "
          f"{len(pendentes)} pendentes.")

    # 5. Baixar e gravar metadados
    processar_metadados(conn, pendentes, args.workers, args.delay)

    # 6. Criar índices para consultas rápidas
    print("\nCriando índices...")
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_variaveis_agregado
        ON variaveis (agregado_id);
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_periodos_agregado
        ON periodos (agregado_id);
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_niveis_agregado
        ON niveis_geo (agregado_id);
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_classificacoes_agregado
        ON classificacoes (agregado_id);
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_categorias_classificacao
        ON categorias (classificacao_id, agregado_id);
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_agregados_nome
        ON agregados (nome);
    """)
    conn.commit()

    # 7. Otimizar e fechar
    print("Compactando banco (VACUUM)...")
    conn.execute("VACUUM;")
    conn.close()

    size_mb = os.path.getsize(db_path) / (1024 * 1024)
    print(f"\n{'=' * 64}")
    print(f"  Banco gerado com sucesso: {db_path}")
    print(f"  Tamanho: {size_mb:.1f} MB")
    print(f"  Copie '{DB_FILENAME}' para a raiz do plugin.")
    print(f"{'=' * 64}")


if __name__ == "__main__":
    main()
