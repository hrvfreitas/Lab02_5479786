# =============================================================================
# gold_setup.py — Camada Gold: cria schema Star Schema no PostgreSQL
# Lab01_PART1_5479786 — PNCP Contratos Públicos
# Hercules Ramos Veloso de Freitas
# hercules.veloso@gmail.com
# Execute UMA VEZ antes do gold_load.py:
#   1. Crie o banco no psql/pgAdmin: CREATE DATABASE pncp_db;
#   2. Ajuste DB_CONFIG abaixo
#   3. python gold_setup.py
# =============================================================================
import logging
import sys
from pathlib import Path

import psycopg2
import psycopg2.extras

LOG_DIR = Path('logs')
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_DIR / 'gold_setup.log', encoding='utf-8', errors='replace'),
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuração — ajuste conforme seu ambiente
# ---------------------------------------------------------------------------
DB_CONFIG = {
    'host':     'localhost',
    'port':     5432,
    'dbname':   'pncp_db',
    'user':     'postgres',
    'password': 'postgres',
}

# ---------------------------------------------------------------------------
# DDL — ordem importa por causa das FKs
# ---------------------------------------------------------------------------
DDL = [
    # 1. Dimensão de Tempo (gerada via Python, 2021-2030)
    """
    CREATE TABLE IF NOT EXISTS dim_tempo (
        id_data       DATE        PRIMARY KEY,
        dia           SMALLINT    NOT NULL,
        mes           SMALLINT    NOT NULL,
        nome_mes      VARCHAR(20) NOT NULL,
        trimestre     SMALLINT    NOT NULL,
        semestre      SMALLINT    NOT NULL,
        ano           SMALLINT    NOT NULL,
        is_fim_semana BOOLEAN     NOT NULL
    )
    """,
    # 2. Dimensão de Modalidades (tipo de licitação — Lei 14.133/2021)
    """
    CREATE TABLE IF NOT EXISTS dim_modalidades (
        id_modalidade   INTEGER      PRIMARY KEY,
        nome_modalidade VARCHAR(100)
    )
    """,
    # 3. Dimensão de Situações de Contrato
    """
    CREATE TABLE IF NOT EXISTS dim_situacoes (
        id_situacao   INTEGER      PRIMARY KEY,
        nome_situacao VARCHAR(100)
    )
    """,
    # 4. Dimensão de Órgãos/Entidades Contratantes
    #    Usa orgao_entidade_id (código interno PNCP) como PK
    #    pois o CNPJ do órgão não vem no endpoint /contratos
    """
    CREATE TABLE IF NOT EXISTS dim_orgaos (
        orgao_entidade_id VARCHAR(50)  PRIMARY KEY,
        nome_orgao        VARCHAR(255),
        codigo_unidade    VARCHAR(50),
        nome_unidade      VARCHAR(255)
    )
    """,
    # 5. Dimensão de Fornecedores (contratados)
    """
    CREATE TABLE IF NOT EXISTS dim_fornecedores (
        cnpj_contratada   CHAR(14)     PRIMARY KEY,
        nome_contratada   VARCHAR(255),
        nome_razao_social VARCHAR(255),
        ni_fornecedor     VARCHAR(20)
    )
    """,
    # 6. Tabela Fato — um registro por contrato publicado no PNCP
    """
    CREATE TABLE IF NOT EXISTS fato_contratos (
        id_contrato          SERIAL        PRIMARY KEY,
        id_contrato_pncp     VARCHAR(100),
        numero_contrato      VARCHAR(100),
        processo             VARCHAR(100),
        categoria_processo_id INTEGER,
        categoria_processo    VARCHAR(100),

        -- Chaves para dimensões
        orgao_entidade_id    VARCHAR(50)   REFERENCES dim_orgaos(orgao_entidade_id),
        cnpj_contratada      CHAR(14)      REFERENCES dim_fornecedores(cnpj_contratada),
        id_modalidade        INTEGER       REFERENCES dim_modalidades(id_modalidade),
        id_situacao          INTEGER       REFERENCES dim_situacoes(id_situacao),

        -- Datas (NULLable — dados históricos nem sempre têm todas as datas)
        data_assinatura      DATE          REFERENCES dim_tempo(id_data),
        data_vigencia_inicio DATE          REFERENCES dim_tempo(id_data),
        data_vigencia_fim    DATE          REFERENCES dim_tempo(id_data),
        data_publicacao      DATE          REFERENCES dim_tempo(id_data),

        -- Métricas financeiras
        valor_inicial        NUMERIC(18,2),
        valor_global         NUMERIC(18,2),
        valor_parcelas       NUMERIC(18,2),

        -- Auditoria
        ano_mes_coleta       CHAR(6),
        data_carga           TIMESTAMP     DEFAULT NOW(),
        usuario_ingestao     VARCHAR(50)   DEFAULT 'hercules_bot'
    )
    """,
    # 7. Índices para performance das queries de negócio
    "CREATE INDEX IF NOT EXISTS idx_fato_data_assin  ON fato_contratos(data_assinatura)",
    "CREATE INDEX IF NOT EXISTS idx_fato_data_pub    ON fato_contratos(data_publicacao)",
    "CREATE INDEX IF NOT EXISTS idx_fato_orgao       ON fato_contratos(orgao_entidade_id)",
    "CREATE INDEX IF NOT EXISTS idx_fato_fornecedor  ON fato_contratos(cnpj_contratada)",
    "CREATE INDEX IF NOT EXISTS idx_fato_modalidade  ON fato_contratos(id_modalidade)",
    "CREATE INDEX IF NOT EXISTS idx_fato_situacao    ON fato_contratos(id_situacao)",
    "CREATE INDEX IF NOT EXISTS idx_fato_valor       ON fato_contratos(valor_global)",
    "CREATE INDEX IF NOT EXISTS idx_fato_ano_mes     ON fato_contratos(ano_mes_coleta)",
]


def criar_schema(conn) -> None:
    with conn.cursor() as cur:
        for stmt in DDL:
            preview = stmt.strip().splitlines()[0][:70]
            try:
                cur.execute(stmt)
                logger.info(f'OK: {preview}')
            except Exception as e:
                logger.error(f'ERRO: {preview}\n  → {e}')
                raise
    conn.commit()


def popular_dim_tempo(conn, ano_inicio: int = 2021, ano_fim: int = 2030) -> None:
    """Preenche dim_tempo com todas as datas do intervalo."""
    import datetime
    MESES = ['Janeiro','Fevereiro','Março','Abril','Maio','Junho',
             'Julho','Agosto','Setembro','Outubro','Novembro','Dezembro']

    logger.info(f'Populando dim_tempo ({ano_inicio}–{ano_fim})...')
    rows = []
    d    = datetime.date(ano_inicio, 1, 1)
    fim  = datetime.date(ano_fim, 12, 31)
    while d <= fim:
        rows.append((
            d, d.day, d.month, MESES[d.month - 1],
            (d.month - 1) // 3 + 1,
            1 if d.month <= 6 else 2,
            d.year, d.weekday() >= 5,
        ))
        d += datetime.timedelta(days=1)

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO dim_tempo
                (id_data, dia, mes, nome_mes, trimestre, semestre, ano, is_fim_semana)
            VALUES %s
            ON CONFLICT (id_data) DO NOTHING
            """,
            rows, page_size=1000,
        )
    conn.commit()
    logger.info(f'  {len(rows):,} datas inseridas em dim_tempo.')


def main() -> None:
    logger.info('=' * 60)
    logger.info('GOLD SETUP — Criando schema no PostgreSQL')
    logger.info('=' * 60)

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        logger.info(f'Conectado: {DB_CONFIG["dbname"]}@{DB_CONFIG["host"]}')
    except Exception as e:
        logger.error(f'Falha na conexão: {e}')
        logger.error('Verifique DB_CONFIG e se o PostgreSQL está rodando.')
        sys.exit(1)

    try:
        criar_schema(conn)
        popular_dim_tempo(conn)
        logger.info('=' * 60)
        logger.info('Schema criado! Execute gold_load.py agora.')
        logger.info('=' * 60)
    except Exception as e:
        conn.rollback()
        logger.error(f'Erro fatal: {e}')
        sys.exit(1)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
