# =============================================================================
# silver_to_postgres.py — Carga do Silver Parquet → PostgreSQL (schema silver)
# Lab02_5479786 — PNCP Contratos Públicos
#
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

SILVER_DIR   = Path('data/silver')   
##DATABASE_URL = 'postgresql://postgres:postgres@localhost:5432/pncp_db' <= use para localhost
DATABASE_URL = 'postgresql://postgres:postgres@pncp_postgres:5432/pncp_db'
SCHEMA       = 'silver'
TABELA       = 'contratos'
LOTE         = 50_000

# Colunas de data identificadas no silver.py
DATE_COLS = ['data_assinatura', 'data_vigencia_inicio', 
             'data_vigencia_fim', 'data_publicacao', 'data_coleta']

# ---------------------------------------------------------------------------
# Conecta ao banco
# ---------------------------------------------------------------------------
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

print(f'\n{"="*58}')
print('  SILVER → PostgreSQL (Schema Atualizado)')
print(f'  Diretório Origem: {SILVER_DIR}')
print(f'  Tabela Destino: {SCHEMA}.{TABELA}')
print(f'{"="*58}\n')

# Cria schema silver se não existir
try:
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {SCHEMA}'))
    print(f'✓ Schema "{SCHEMA}" garantido\n')
except Exception as e:
    print(f'❌ Erro ao conectar/criar schema: {e}')
    sys.exit(1)

# ---------------------------------------------------------------------------
# Carrega Parquets
# ---------------------------------------------------------------------------
arquivos = sorted(SILVER_DIR.glob('contratos_*.parquet'))
if not arquivos:
    print(f'❌ Nenhum Parquet encontrado em {SILVER_DIR.resolve()}')
    sys.exit(1)

print(f'Carregando {len(arquivos)} arquivos Parquet...\n')

total_inserido = 0
primeiro = True

for arq in tqdm(arquivos, desc='Silver → PG', unit='arq'):
    df = pd.read_parquet(arq)

    # Converte timestamps para datetime sem timezone (essencial para PostgreSQL)
    # Garante que as colunas de data do novo schema sejam tratadas
    for col in df.columns:
        if col in DATE_COLS or pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.tz_localize(None)

    # Primeira iteração: recria a tabela (replace); demais: append
    modo = 'replace' if primeiro else 'append'
    
    df.to_sql(
        name      = TABELA,
        con       = engine,
        schema    = SCHEMA,
        if_exists = modo,
        index     = False,
        chunksize = LOTE,
        method    = 'multi',
    )
    
    total_inserido += len(df)
    primeiro = False

# ---------------------------------------------------------------------------
# Cria índices (Otimização para DBT e Analytics)
# ---------------------------------------------------------------------------
print('\nCriando índices de performance...')
with engine.begin() as conn:
    conn.execute(text(f'''
        -- Índice na chave primária de controle do PNCP
        CREATE INDEX IF NOT EXISTS idx_silver_pncp_id 
            ON {SCHEMA}.{TABELA} (id);
            
        -- Índices para filtros de entidade e fornecedor
        CREATE INDEX IF NOT EXISTS idx_silver_orgao
            ON {SCHEMA}.{TABELA} (orgao_entidade_id);
        CREATE INDEX IF NOT EXISTS idx_silver_cnpj
            ON {SCHEMA}.{TABELA} (cnpj_contratada);
            
        -- Índices para análise temporal (frequentes no silver.py)
        CREATE INDEX IF NOT EXISTS idx_silver_assinatura
            ON {SCHEMA}.{TABELA} (data_assinatura);
        CREATE INDEX IF NOT EXISTS idx_silver_ano_mes_coleta
            ON {SCHEMA}.{TABELA} (ano_mes_coleta);
            
        -- Índice para agrupamento por modalidade
        CREATE INDEX IF NOT EXISTS idx_silver_modalidade
            ON {SCHEMA}.{TABELA} (modalidade_id);
    '''))

print(f'\n{"="*58}')
print(f'✅ Carga concluída com sucesso!')
print(f'   Total: {total_inserido:,} registros em {SCHEMA}.{TABELA}')
print(f'{"="*58}\n')
