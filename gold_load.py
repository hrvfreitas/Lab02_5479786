# =============================================================================
# gold_load.py — Camada Gold: carrega Silver → PostgreSQL
# Lab01_PART1_5479786 — PNCP Contratos Públicos
# Hercules Ramos Veloso de Freitas
# 5479786
# Execute APÓS gold_setup.py.
# Estratégia:
#   - UPSERT nas dimensões (idempotente, atualiza se nome mudar)
#   - INSERT na fato por lotes, com checkpoint por ano_mes_coleta
#   - Filtro de sanidade: descarta contratos com valor_global > R$ 10 bi
# =============================================================================
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
import pyarrow.parquet as pq
from tqdm import tqdm

LOG_DIR = Path('logs')
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_DIR / 'gold_load.log', encoding='utf-8', errors='replace'),
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
DB_CONFIG = {
    'host':     'localhost',
    'port':     5432,
    'dbname':   'pncp_db',
    'user':     'postgres',
    'password': 'postgres',
}

SILVER_DIR = Path('data/silver')
BATCH_SIZE = 10_000
VALOR_MAX  = 10_000_000_000  # R$ 10 bi — contratos acima disso são descartados


# ---------------------------------------------------------------------------
# Helpers de limpeza de tipos
# ---------------------------------------------------------------------------
def _str(v, max_len: int = None):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    s = str(v).strip()
    if s in ('', 'nan', 'None', 'NaN'):
        return None
    return s[:max_len] if max_len else s


def _int(v):
    try:
        f = float(v)
        return int(f) if not np.isnan(f) else None
    except (TypeError, ValueError):
        return None


def _int_fk(v):
    """Como _int, mas retorna None se o valor for 0 (sentinela de ausência).
    Evita FK violation quando a dimensão não tem o registro de id=0."""
    result = _int(v)
    return None if result == 0 else result


def _float(v):
    try:
        f = float(v)
        return f if not np.isnan(f) else None
    except (TypeError, ValueError):
        return None


def _date(v):
    if pd.isna(v):
        return None
    try:
        ts = pd.Timestamp(v)
        if pd.isna(ts) or not (2000 <= ts.year <= 2100):
            return None
        return ts.date()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Upsert nas dimensões
# ---------------------------------------------------------------------------
def upsert_dim_modalidades(cur, df: pd.DataFrame) -> None:
    if 'modalidade_id' not in df.columns:
        return
    rows = (df[['modalidade_id', 'modalidade_nome']]
            .dropna(subset=['modalidade_id'])
            .drop_duplicates('modalidade_id'))
    dados = [
        (_int(r.modalidade_id), _str(r.modalidade_nome, 100))
        for r in rows.itertuples(index=False)
        if _int(r.modalidade_id) is not None and _str(r.modalidade_nome)
    ]
    if dados:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO dim_modalidades (id_modalidade, nome_modalidade)
            VALUES %s
            ON CONFLICT (id_modalidade) DO UPDATE SET
                nome_modalidade = EXCLUDED.nome_modalidade
            WHERE dim_modalidades.nome_modalidade IS DISTINCT FROM EXCLUDED.nome_modalidade
        """, dados)


def upsert_dim_situacoes(cur, df: pd.DataFrame) -> None:
    if 'situacao_contrato_id' not in df.columns:
        return
    rows = (df[['situacao_contrato_id', 'situacao_contrato_nome']]
            .dropna(subset=['situacao_contrato_id'])
            .drop_duplicates('situacao_contrato_id'))
    dados = [
        (_int(r.situacao_contrato_id), _str(r.situacao_contrato_nome, 100))
        for r in rows.itertuples(index=False)
        if _int(r.situacao_contrato_id) is not None and _str(r.situacao_contrato_nome)
    ]
    if dados:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO dim_situacoes (id_situacao, nome_situacao)
            VALUES %s
            ON CONFLICT (id_situacao) DO UPDATE SET
                nome_situacao = EXCLUDED.nome_situacao
            WHERE dim_situacoes.nome_situacao IS DISTINCT FROM EXCLUDED.nome_situacao
        """, dados)


def upsert_dim_orgaos(cur, df: pd.DataFrame) -> None:
    if 'orgao_entidade_id' not in df.columns:
        return
    rows = (df[['orgao_entidade_id', 'orgao_entidade_nome',
                'codigo_unidade', 'nome_unidade']]
            .dropna(subset=['orgao_entidade_id'])
            .drop_duplicates('orgao_entidade_id'))
    dados = [
        (_str(r.orgao_entidade_id, 50), _str(r.orgao_entidade_nome, 255),
         _str(r.codigo_unidade, 50),    _str(r.nome_unidade, 255))
        for r in rows.itertuples(index=False)
        if _str(r.orgao_entidade_id)
    ]
    if dados:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO dim_orgaos
                (orgao_entidade_id, nome_orgao, codigo_unidade, nome_unidade)
            VALUES %s
            ON CONFLICT (orgao_entidade_id) DO UPDATE SET
                nome_orgao     = EXCLUDED.nome_orgao,
                codigo_unidade = EXCLUDED.codigo_unidade,
                nome_unidade   = EXCLUDED.nome_unidade
        """, dados)


def upsert_dim_fornecedores(cur, df: pd.DataFrame) -> None:
    if 'cnpj_contratada' not in df.columns:
        return
    rows = (df[['cnpj_contratada', 'nome_contratada',
                'nome_razao_social_fornecedor', 'ni_fornecedor']]
            .dropna(subset=['cnpj_contratada'])
            .drop_duplicates('cnpj_contratada'))
    dados = [
        (_str(r.cnpj_contratada, 14), _str(r.nome_contratada, 255),
         _str(r.nome_razao_social_fornecedor, 255), _str(r.ni_fornecedor, 20))
        for r in rows.itertuples(index=False)
        if _str(r.cnpj_contratada)
    ]
    if dados:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO dim_fornecedores
                (cnpj_contratada, nome_contratada, nome_razao_social, ni_fornecedor)
            VALUES %s
            ON CONFLICT (cnpj_contratada) DO UPDATE SET
                nome_contratada   = EXCLUDED.nome_contratada,
                nome_razao_social = EXCLUDED.nome_razao_social
        """, dados)


# ---------------------------------------------------------------------------
# dim_tempo: garante que datas dos dados estejam presentes
# ---------------------------------------------------------------------------
def garantir_datas_dim_tempo(cur, datas: set) -> None:
    import datetime
    MESES = ['Janeiro','Fevereiro','Março','Abril','Maio','Junho',
             'Julho','Agosto','Setembro','Outubro','Novembro','Dezembro']
    rows = [
        (d, d.day, d.month, MESES[d.month-1],
         (d.month-1)//3+1, 1 if d.month <= 6 else 2,
         d.year, d.weekday() >= 5)
        for d in datas if d is not None
    ]
    if rows:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO dim_tempo
                (id_data, dia, mes, nome_mes, trimestre, semestre, ano, is_fim_semana)
            VALUES %s
            ON CONFLICT (id_data) DO NOTHING
        """, rows, page_size=500)


# ---------------------------------------------------------------------------
# Checkpoint e inserção na fato
# ---------------------------------------------------------------------------
def mes_ja_carregado(cur, ano_mes: str) -> bool:
    cur.execute(
        "SELECT 1 FROM fato_contratos WHERE ano_mes_coleta = %s LIMIT 1",
        (ano_mes,)
    )
    return cur.fetchone() is not None


def inserir_fato(cur, df: pd.DataFrame) -> int:
    DATE_COLS = ['data_assinatura', 'data_vigencia_inicio',
                 'data_vigencia_fim', 'data_publicacao']

    todas_datas = set()
    for col in DATE_COLS:
        if col in df.columns:
            todas_datas.update(_date(v) for v in df[col])
    todas_datas.discard(None)
    garantir_datas_dim_tempo(cur, todas_datas)

    total     = 0
    registros = df.to_dict('records')

    for inicio in range(0, len(registros), BATCH_SIZE):
        lote  = registros[inicio: inicio + BATCH_SIZE]
        dados = [
            (
                _str(r.get('id'), 100),
                _str(r.get('numero_contrato'), 100),
                _str(r.get('processo'), 100),
                _int_fk(r.get('categoria_processo_id')),
                _str(r.get('categoria_processo_nome'), 100),
                _str(r.get('orgao_entidade_id'), 50),
                _str(r.get('cnpj_contratada'), 14),
                _int_fk(r.get('modalidade_id')),
                _int_fk(r.get('situacao_contrato_id')),
                _date(r.get('data_assinatura')),
                _date(r.get('data_vigencia_inicio')),
                _date(r.get('data_vigencia_fim')),
                _date(r.get('data_publicacao')),
                _float(r.get('valor_inicial')),
                _float(r.get('valor_global')),
                _float(r.get('valor_parcelas')),
                _str(r.get('ano_mes_coleta'), 6),
            )
            for r in lote
        ]
        psycopg2.extras.execute_values(cur, """
            INSERT INTO fato_contratos (
                id_contrato_pncp, numero_contrato, processo,
                categoria_processo_id, categoria_processo,
                orgao_entidade_id, cnpj_contratada, id_modalidade, id_situacao,
                data_assinatura, data_vigencia_inicio, data_vigencia_fim, data_publicacao,
                valor_inicial, valor_global, valor_parcelas,
                ano_mes_coleta
            ) VALUES %s
        """, dados, page_size=BATCH_SIZE)
        total += len(dados)

    return total


# ---------------------------------------------------------------------------
# Processamento de um arquivo
# ---------------------------------------------------------------------------
def carregar_arquivo(conn, arq: Path) -> int:
    partes  = arq.stem.split('_')
    if len(partes) < 3:
        logger.warning(f'Nome inesperado: {arq.name} — pulando')
        return 0
    ano_mes = partes[1] + partes[2]

    with conn.cursor() as cur:
        if mes_ja_carregado(cur, ano_mes):
            logger.info(f'  {arq.name}: já carregado, pulando.')
            return 0

    logger.info(f'  Carregando {arq.name} ({ano_mes})...')
    df = pd.read_parquet(arq)
    if df.empty:
        return 0

    total_bruto = len(df)

    # Filtro de sanidade — remove valores absurdos
    if 'valor_global' in df.columns:
        descartados = (df['valor_global'] > VALOR_MAX).sum()
        if descartados:
            logger.warning(
                f'    {descartados:,} registros descartados '
                f'(valor_global > R$ {VALOR_MAX:,.0f})'
            )
            df = df[df['valor_global'].isna() | (df['valor_global'] <= VALOR_MAX)]

    logger.info(f'    {total_bruto:,} lidos → {len(df):,} após filtro de sanidade')

    with conn.cursor() as cur:
        upsert_dim_modalidades(cur, df)
        upsert_dim_situacoes(cur, df)
        upsert_dim_orgaos(cur, df)
        upsert_dim_fornecedores(cur, df)
        total = inserir_fato(cur, df)

    conn.commit()
    logger.info(f'    ✓ {total:,} registros inseridos')
    return total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    logger.info('=' * 60)
    logger.info('GOLD LOAD — Silver → PostgreSQL')
    logger.info('=' * 60)

    arquivos = sorted(SILVER_DIR.glob('contratos_*.parquet'))
    if not arquivos:
        logger.error(f'Nenhum arquivo em {SILVER_DIR}. Execute silver.py primeiro.')
        sys.exit(1)

    logger.info(f'Arquivos encontrados: {len(arquivos)}')

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        logger.info(f'Conectado: {DB_CONFIG["dbname"]}@{DB_CONFIG["host"]}')
    except Exception as e:
        logger.error(f'Falha na conexão: {e}')
        sys.exit(1)

    total_geral = 0
    erros       = []

    try:
        for arq in tqdm(arquivos, desc='Gold Load', unit='arq'):
            try:
                total_geral += carregar_arquivo(conn, arq)
            except Exception as e:
                conn.rollback()
                logger.error(f'Erro em {arq.name}: {e}')
                erros.append(arq.name)
    finally:
        conn.close()

    logger.info('=' * 60)
    logger.info(f'Total inserido na fato : {total_geral:,} registros')
    if erros:
        logger.warning(f'Arquivos com erro ({len(erros)}): {", ".join(erros)}')
    else:
        logger.info('Carga concluída sem erros.')
    logger.info('=' * 60)


if __name__ == '__main__':
    main()
