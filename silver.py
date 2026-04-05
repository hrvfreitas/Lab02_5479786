# =============================================================================
# silver.py — Camada Silver (Tratamento)
# Lab01_PART1_5479786 — PNCP Contratos Públicos
#
# O que este script faz (conforme PDF do laboratório):
#   1. Lê os JSONs brutos da Bronze, desaninha objetos embutidos da API
#   2. Padroniza nomes de colunas para snake_case
#   3. Converte tipos (datas, floats, ints)
#   4. Remove duplicatas e trata valores ausentes
#   5. Salva em Parquet comprimido em data/silver/
#   6. Gera relatório de qualidade (nulos, estatísticas descritivas)
#   7. Gera 5 gráficos + Markdown com os gráficos embedados
#
# Uso:
#   python silver.py                  # só tratamento (etapas 1-5)
#   python silver.py --relatorio      # + relatório de qualidade (etapa 6)
#   python silver.py --graficos       # + 5 gráficos e Markdown (etapa 7)
#   python silver.py --tudo           # tudo (etapas 1-7)
# =============================================================================
import argparse
import json
import locale
import logging
import sys
import warnings
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

warnings.filterwarnings('ignore', category=FutureWarning)

# ---------------------------------------------------------------------------
# Encoding (Windows)
# ---------------------------------------------------------------------------
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_ALL, '')
    except locale.Error:
        pass

if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# ---------------------------------------------------------------------------
# Diretórios e logging
# ---------------------------------------------------------------------------
RAW_DIR      = Path('data/raw')
SILVER_DIR   = Path('data/silver')
GRAFICOS_DIR = Path('data/graficos')
LOG_DIR      = Path('logs')

for d in (SILVER_DIR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_DIR / 'silver.log', encoding='utf-8', errors='replace'),
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema Parquet Silver
# ---------------------------------------------------------------------------
SCHEMA = pa.schema([
    ('id',                           pa.string()),
    ('orgao_entidade_id',            pa.string()),
    ('orgao_entidade_nome',          pa.string()),
    ('objeto_contrato',              pa.string()),
    ('numero_contrato',              pa.string()),
    ('processo',                     pa.string()),
    ('categoria_processo_id',        pa.int64()),
    ('categoria_processo_nome',      pa.string()),
    ('cnpj_contratada',              pa.string()),
    ('nome_contratada',              pa.string()),
    ('valor_inicial',                pa.float64()),
    ('valor_global',                 pa.float64()),
    ('valor_parcelas',               pa.float64()),
    ('data_assinatura',              pa.timestamp('ms')),
    ('data_vigencia_inicio',         pa.timestamp('ms')),
    ('data_vigencia_fim',            pa.timestamp('ms')),
    ('situacao_contrato_id',         pa.int64()),
    ('situacao_contrato_nome',       pa.string()),
    ('data_publicacao',              pa.timestamp('ms')),
    ('ni_fornecedor',                pa.string()),
    ('nome_razao_social_fornecedor', pa.string()),
    ('codigo_unidade',               pa.string()),
    ('nome_unidade',                 pa.string()),
    ('modalidade_id',                pa.int64()),
    ('modalidade_nome',              pa.string()),
    ('ano_mes_coleta',               pa.string()),
    ('data_coleta',                  pa.timestamp('ms')),
])

DATE_COLS = ['data_assinatura', 'data_vigencia_inicio',
             'data_vigencia_fim', 'data_publicacao']

# Mapeamento pós-flatten → snake_case
# A API PNCP retorna orgaoEntidade, unidadeOrgao, tipoContrato e
# categoriaProcesso como objetos aninhados — nunca como campos planos.
# _flatten_registro() os desaninha com prefixo __ antes deste mapeamento.
COL_MAP = {
    '__id':                        'id',
    'numeroContratoEmpenho':       'numero_contrato',
    'processo':                    'processo',
    'objetoContrato':              'objeto_contrato',
    '__orgao_cnpj':                'orgao_entidade_id',
    '__orgao_nome':                'orgao_entidade_nome',
    '__unidade_codigo':            'codigo_unidade',
    '__unidade_nome':              'nome_unidade',
    'niFornecedor':                'ni_fornecedor',
    'nomeRazaoSocialFornecedor':   'nome_razao_social_fornecedor',
    '__cnpj_contratada':           'cnpj_contratada',
    '__nome_contratada':           'nome_contratada',
    '__categoria_id':              'categoria_processo_id',   # campo real da API
    '__categoria_nome':            'categoria_processo_nome',
    '__modalidade_id':             'modalidade_id',
    '__modalidade_nome':           'modalidade_nome',
    'valorInicial':                'valor_inicial',
    'valorGlobal':                 'valor_global',
    'valorParcela':                'valor_parcelas',   # normalizado em _flatten_registro
    'dataAssinatura':              'data_assinatura',
    'dataVigenciaInicio':          'data_vigencia_inicio',
    'dataVigenciaFim':             'data_vigencia_fim',
    'dataPublicacaoPncp':          'data_publicacao',
    'situacaoContratoId':          'situacao_contrato_id',
    'situacaoContratoNome':        'situacao_contrato_nome',
}


# ===========================================================================
# ETAPAS 1-5: TRATAMENTO E PERSISTÊNCIA
# ===========================================================================

def _parquet_silver(ano: int, mes: int) -> Path:
    return SILVER_DIR / f'contratos_{ano}_{mes:02d}.parquet'


def mes_ja_tratado(ano: int, mes: int) -> bool:
    return _parquet_silver(ano, mes).exists()


def _parse_date(serie: pd.Series) -> pd.Series:
    """
    Converte qualquer formato de data (YYYYMMDD, ISO, ISO+hora) para
    datetime64[ms]. Valores inválidos viram NaT.
    Retorna sempre dtype datetime64[ms] — nunca object.
    """
    s = (serie.astype(str)
              .str.strip()
              .replace({'None': pd.NaT, 'nan': pd.NaT, 'NaN': pd.NaT,
                        '': pd.NaT, '00000000': pd.NaT}))  # data inválida da API
    return pd.to_datetime(s, errors='coerce', dayfirst=False).astype('datetime64[ms]')


def _flatten_registro(r: dict) -> dict:
    """
    Desaninha os objetos embutidos da resposta da API PNCP.
    Usa prefixo __ para não colidir com campos já existentes.
    """
    orgao   = r.get('orgaoEntidade')     or {}
    unidade = r.get('unidadeOrgao')      or {}
    tipo    = r.get('tipoContrato')      or {}
    categ   = r.get('categoriaProcesso') or {}

    r['__id']              = r.get('numeroControlePNCP') or r.get('numeroControlePncpCompra')
    r['__orgao_cnpj']      = orgao.get('cnpj')
    r['__orgao_nome']      = orgao.get('razaoSocial')
    r['__unidade_codigo']  = unidade.get('codigoUnidade')
    r['__unidade_nome']    = unidade.get('nomeUnidade')
    r['__categoria_id']    = categ.get('id')    # id numérico da categoria
    r['__categoria_nome']  = categ.get('nome')
    r['__modalidade_id']   = tipo.get('id')
    r['__modalidade_nome'] = tipo.get('nome')

    # CNPJ do fornecedor = niFornecedor quando é PJ
    r['__cnpj_contratada'] = (
        r.get('niFornecedor') if r.get('tipoPessoa') == 'PJ' else None
    )
    r['__nome_contratada'] = r.get('nomeRazaoSocialFornecedor')
    # Normaliza valorParcelas (plural) → valorParcela (singular) para
    # evitar coluna duplicada após COL_MAP rename
    if 'valorParcelas' in r and 'valorParcela' not in r:
        r['valorParcela'] = r['valorParcelas']
    return r


def _ler_json_mes(ano: int, mes: int) -> Optional[pd.DataFrame]:
    """Lê, desaninha e retorna DataFrame com todos os registros do mês."""
    dir_mes  = RAW_DIR / f'{ano}_{mes:02d}'
    arquivos = sorted(dir_mes.glob('pagina_*.json'))

    if not arquivos:
        logger.warning(f'{ano}/{mes:02d}: nenhum JSON encontrado em {dir_mes}')
        return None

    registros = []
    for arq in arquivos:
        try:
            with open(arq, encoding='utf-8') as f:
                dados = json.load(f)
            if isinstance(dados, list):
                registros.extend(_flatten_registro(r) for r in dados)
        except Exception as e:
            logger.warning(f'Erro lendo {arq.name}: {e}')

    if not registros:
        logger.warning(f'{ano}/{mes:02d}: JSONs existem mas estão todos vazios')
        return None

    return pd.DataFrame(registros)


def tratar_mes(ano: int, mes: int) -> Optional[int]:
    """
    Lê JSONs brutos → aplica limpeza → salva Parquet Silver.
    Retorna nº de registros salvos ou None em caso de erro.
    """
    df_raw = _ler_json_mes(ano, mes)
    if df_raw is None or df_raw.empty:
        logger.info(f'{ano}/{mes:02d}: sem dados para tratar')
        return 0

    logger.info(f'{ano}/{mes:02d}: {len(df_raw):,} registros brutos lidos')

    # 1. Renomear → snake_case
    df            = df_raw.rename(columns=COL_MAP)
    cols_presentes = [c for c in COL_MAP.values() if c in df.columns]
    df            = df[cols_presentes].copy()

    # 2. Relatório de nulos (antes da limpeza — para documentação)
    nulos_pct = (df.isnull().sum() / len(df) * 100).round(2)
    linhas_nulos = '\n'.join(
        f'  {c}: {nulos_pct[c]:.1f}%' for c in nulos_pct[nulos_pct > 0].index
    )
    if linhas_nulos:
        logger.info(f'{ano}/{mes:02d}: nulos por coluna (%):\n{linhas_nulos}')

    # 3. Remoção de duplicatas
    antes = len(df)
    subset_dup = ['id'] if 'id' in df.columns else None
    df.drop_duplicates(subset=subset_dup, keep='first', inplace=True)
    if len(df) < antes:
        logger.info(f'{ano}/{mes:02d}: {antes - len(df)} duplicatas removidas')

    # 4. Limpeza de strings (strip + substituição de artefatos)
    for col in df.select_dtypes(include=['object', 'str']).columns:
        df[col] = (df[col].astype(str).str.strip()
                          .replace({'None': '', 'nan': '', 'NaN': ''}))

    # 5. Conversão de tipos numéricos
    for col in ('valor_inicial', 'valor_global', 'valor_parcelas'):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

    for col in ('situacao_contrato_id', 'modalidade_id', 'categoria_processo_id'):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')

    # 5b. Filtro de sanidade: remove contratos com valor absurdo (> R$ 10 bilhões).
    # Contratos acima desse teto são erros de digitação na fonte — equivalem a
    # ~0,1% do PIB brasileiro em um único contrato, o que não é realista.
    VALOR_MAX = 10_000_000_000  # R$ 10 bi
    if 'valor_global' in df.columns:
        mask_absurdo = df['valor_global'] > VALOR_MAX
        n_absurdos = mask_absurdo.sum()
        if n_absurdos > 0:
            logger.warning(
                f'{ano}/{mes:02d}: {n_absurdos} registros removidos '
                f'(valor_global > R$ {VALOR_MAX:,.0f})'
            )
            df = df[~mask_absurdo]

    # 6. Metadados de coleta
    df['ano_mes_coleta'] = f'{ano}{mes:02d}'
    df['data_coleta']    = pd.Timestamp.now().floor('ms')

    # 7. Garantir todas as colunas do schema (com tipo correto)
    for field in SCHEMA:
        if field.name not in df.columns:
            if pa.types.is_floating(field.type):
                df[field.name] = 0.0
            elif pa.types.is_integer(field.type):
                df[field.name] = 0
            elif pa.types.is_timestamp(field.type):
                df[field.name] = pd.NaT
            else:
                df[field.name] = ''

    # 8. Conversão de datas — DEPOIS de garantir colunas, para cobrir as novas
    for col in DATE_COLS:
        if col in df.columns:
            df[col] = _parse_date(df[col])

    # 9. Persistência em Parquet (snappy)
    dest = _parquet_silver(ano, mes)
    try:
        table = pa.Table.from_pandas(
            df[[f.name for f in SCHEMA]], schema=SCHEMA, preserve_index=False
        )
        pq.write_table(table, dest, compression='snappy')
        logger.info(f'✓ {ano}/{mes:02d}: {len(df):,} registros → {dest}')
        return len(df)
    except Exception as e:
        logger.error(f'{ano}/{mes:02d}: erro ao salvar Parquet: {e}')
        if dest.exists():
            dest.unlink()
        return None


def tratar_todos_meses() -> None:
    logger.info('=' * 60)
    logger.info('SILVER — TRATAMENTO E CONVERSÃO PARA PARQUET')
    logger.info('=' * 60)

    dirs_raw = sorted(RAW_DIR.glob('????_??'))
    if not dirs_raw:
        logger.error(f'Nenhum dado em {RAW_DIR}. Execute bronze.py primeiro.')
        return

    meses = []
    for d in dirs_raw:
        try:
            ano, mes = int(d.name[:4]), int(d.name[5:7])
            if not mes_ja_tratado(ano, mes):
                meses.append((ano, mes))
        except ValueError:
            continue

    logger.info(f'Meses a tratar: {len(meses)}')
    if not meses:
        logger.info('Todos os meses já estão na camada Silver.')
        return

    total_regs = 0
    total_ok   = 0

    for i, (ano, mes) in enumerate(tqdm(meses, desc='Silver', unit='mês'), 1):
        logger.info(f'--- {ano}/{mes:02d} ({i}/{len(meses)}) ---')
        resultado = tratar_mes(ano, mes)
        if resultado is not None:
            total_regs += resultado
            total_ok   += 1

    logger.info('=' * 60)
    logger.info(f'Meses tratados     : {total_ok}')
    logger.info(f'Total de registros : {total_regs:,}')
    logger.info('=' * 60)


# ===========================================================================
# ETAPA 6: RELATÓRIO DE QUALIDADE
# ===========================================================================

def gerar_relatorio_qualidade() -> None:
    """
    Relatório de qualidade exigido pelo laboratório:
    - Contagem de nulos por coluna
    - Tipos de colunas
    - Estatísticas descritivas (média, desvio padrão, percentis)
    Salvo em data/graficos/relatorio_qualidade.txt
    """
    GRAFICOS_DIR.mkdir(parents=True, exist_ok=True)
    arquivos = sorted(SILVER_DIR.glob('contratos_*.parquet'))
    if not arquivos:
        logger.error('Nenhum arquivo Silver encontrado.')
        return

    logger.info('Gerando relatório de qualidade...')

    # Colunas para o relatório — todas as do schema Silver
    COLS_RELAT = [f.name for f in SCHEMA]

    # ---------------------------------------------------------------
    # Leitura BRUTA — sem filtros para contar nulos reais
    # ---------------------------------------------------------------
    dfs_bruto = []
    for arq in tqdm(arquivos, desc='Lendo Silver (bruto)', unit='arq'):
        try:
            schema_names = pq.read_schema(arq).names
            cols_ok = [c for c in COLS_RELAT if c in schema_names]
            df_tmp = pd.read_parquet(arq, columns=cols_ok)
            dfs_bruto.append(df_tmp)
        except Exception as e:
            logger.warning(f'Erro lendo {arq.name}: {e}')

    if not dfs_bruto:
        logger.error('Nenhum dado válido.')
        return

    df_bruto = pd.concat(dfs_bruto, ignore_index=True)
    total_bruto = len(df_bruto)

    # Nulos contados ANTES de qualquer filtro — valores reais
    nulos_abs = df_bruto.isnull().sum()
    # Strings vazias também são "nulos" na prática
    for col in df_bruto.select_dtypes(include='object').columns:
        nulos_abs[col] += (df_bruto[col].astype(str).str.strip() == '').sum()
    nulos_pct_bruto = (nulos_abs / total_bruto * 100).round(4)

    # Zeros sentinela em colunas numéricas:
    # o silver.py preenche ausências com 0 antes de salvar, então 0 pode
    # significar "não informado" e não um valor real zero.
    # Colunas onde 0 é sentinela de ausência (nunca deveriam ser zero de verdade):
    COLS_ZERO_SENTINELA = {
        'valor_inicial':          'float — zero indica ausência (não é valor real)',
        'valor_parcelas':         'float — zero indica ausência (não é valor real)',
        'situacao_contrato_id':   'int   — 0 = sentinela, campo não existe na API',
        'modalidade_id':          'int   — 0 = sentinela quando API não retornou',
    }
    zeros_sentinela = {}
    for col, descricao in COLS_ZERO_SENTINELA.items():
        if col in df_bruto.columns:
            n_zeros = (pd.to_numeric(df_bruto[col], errors='coerce').fillna(0) == 0).sum()
            zeros_sentinela[col] = (n_zeros, n_zeros / total_bruto * 100, descricao)

    # Tipos reais
    tipos = df_bruto.dtypes

    # ---------------------------------------------------------------
    # Leitura FILTRADA — para estatísticas descritivas
    # Carrega apenas as colunas necessárias para evitar OOM no .copy()
    # ---------------------------------------------------------------
    COLS_STATS = ['data_assinatura', 'valor_global', 'nome_contratada', 'modalidade_nome']
    dfs_stats = []
    for arq in arquivos:
        try:
            schema_names = pq.read_schema(arq).names
            cols_ok = [c for c in COLS_STATS if c in schema_names]
            df_tmp = pd.read_parquet(arq, columns=cols_ok)
            df_tmp['data_assinatura'] = pd.to_datetime(
                df_tmp['data_assinatura'], errors='coerce')
            df_tmp['valor_global'] = pd.to_numeric(df_tmp['valor_global'], errors='coerce')
            mask = df_tmp['valor_global'].between(1, 1e10)
            dfs_stats.append(df_tmp[mask])
        except Exception:
            pass

    df = pd.concat(dfs_stats, ignore_index=True)
    del dfs_stats  # libera memória imediatamente
    df['ano'] = df['data_assinatura'].dt.year

    # Datas claramente erradas (futuro distante ou muito antigas)
    datas_validas   = df['data_assinatura'].between('2021-01-01', '2026-12-31')
    datas_invalidas = (~datas_validas & df['data_assinatura'].notna()).sum()

    sep   = '=' * 65
    saida = GRAFICOS_DIR / 'relatorio_qualidade.txt'

    with open(saida, 'w', encoding='utf-8') as f:
        f.write(f'{sep}\n')
        f.write('  RELATÓRIO DE QUALIDADE — CAMADA SILVER\n')
        f.write(f'  Lab01_PART1_3734509 — PNCP Contratos Públicos\n')
        f.write(f'{sep}\n\n')

        # --- Visão geral ---
        datas_ok = df['data_assinatura'].dropna()
        periodo  = (
            f"{datas_ok.min().strftime('%Y-%m')} a {datas_ok.max().strftime('%Y-%m')}"
            if not datas_ok.empty else 'N/D'
        )
        f.write(f'  Período (filtrado)  : {periodo}\n')
        f.write(f'  Total bruto         : {total_bruto:,} registros\n')
        f.write(f'  Após filtro sanidade: {len(df):,} registros\n')
        f.write(f'  Descartados         : {total_bruto - len(df):,} '
                f'({(total_bruto - len(df))/total_bruto*100:.2f}%)\n')
        f.write(f'  Arquivos Parquet    : {len(arquivos)}\n\n')

        # --- Tipos de colunas ---
        f.write(f'  TIPOS DE COLUNAS\n')
        f.write(f'  {"-"*55}\n')
        for col in tipos.index:
            f.write(f'  {col:<40}: {tipos[col]}\n')

        # --- Nulos (contados no dado bruto, antes de filtros) ---
        f.write(f'\n  CONTAGEM DE NULOS — dado bruto, antes de filtros (%)\n')
        f.write(f'  {"-"*55}\n')
        for col, pct in nulos_pct_bruto.sort_values(ascending=False).items():
            alerta = ' ⚠' if pct > 10 else ('  ' if pct > 0 else '  ')
            f.write(f'  {col:<40}: {pct:>7.4f}%{alerta}\n')

        # --- Zeros sentinela ---
        f.write(f'\n  ZEROS SENTINELA — preenchimento padrão do pipeline (%)\n')
        f.write(f'  {"-"*55}\n')
        f.write(f'  {"Coluna":<40}  {"Zeros":>8}  {"(%)":>8}\n')
        f.write(f'  {"-"*40}  {"-"*8}  {"-"*8}\n')
        for col, (n, pct, desc) in zeros_sentinela.items():
            alerta = ' ⚠' if pct > 50 else ''
            f.write(f'  {col:<40}  {n:>8,}  {pct:>7.2f}%{alerta}\n')
            f.write(f'    → {desc}\n')

        # --- Estatísticas descritivas ---
        f.write(f'\n  ESTATÍSTICAS DESCRITIVAS — valor_global (dado filtrado)\n')
        f.write(f'  {"-"*55}\n')
        q = df['valor_global'].quantile([0.01, 0.05, 0.25, 0.50, 0.75, 0.95, 0.99])
        for nome, val in [
            ('N válidos',         f'{df["valor_global"].count():,}'),
            ('Total (R$ bilhões)', f'{df["valor_global"].sum()/1e9:.4f}'),
            ('Média (R$)',         f'{df["valor_global"].mean():,.4f}'),
            ('Mediana (R$)',       f'{q[0.50]:,.4f}'),
            ('Desvio Padrão (R$)', f'{df["valor_global"].std():,.4f}'),
            ('Mínimo (R$)',        f'{df["valor_global"].min():,.4f}'),
            ('Máximo (R$)',        f'{df["valor_global"].max():,.4f}'),
            ('P01 (R$)',           f'{q[0.01]:,.4f}'),
            ('P05 (R$)',           f'{q[0.05]:,.4f}'),
            ('P25 (R$)',           f'{q[0.25]:,.4f}'),
            ('P75 (R$)',           f'{q[0.75]:,.4f}'),
            ('P95 (R$)',           f'{q[0.95]:,.4f}'),
            ('P99 (R$)',           f'{q[0.99]:,.4f}'),
        ]:
            f.write(f'  {nome:<30}: {val:>22}\n')

        # --- Por ano ---
        f.write(f'\n  CONTRATOS POR ANO (dado filtrado)\n')
        f.write(f'  {"-"*55}\n')
        f.write(f'  {"Ano":<6}  {"Contratos":>12}  {"Valor (R$ bi)":>16}  '
                f'{"Médio (R$)":>16}  {"Mediana (R$)":>14}\n')
        f.write(f'  {"-"*6}  {"-"*12}  {"-"*16}  {"-"*16}  {"-"*14}\n')
        for ano in sorted(df['ano'].dropna().unique().astype(int)):
            df_a = df[df['ano'] == ano]
            f.write(
                f'  {ano:<6}  {len(df_a):>12,}  '
                f'{df_a["valor_global"].sum()/1e9:>16.4f}  '
                f'{df_a["valor_global"].mean():>16,.4f}  '
                f'{df_a["valor_global"].median():>14,.4f}\n'
            )

        # --- Modalidades ---
        if 'modalidade_nome' in df.columns:
            f.write(f'\n  TOP 10 MODALIDADES\n')
            f.write(f'  {"-"*55}\n')
            top = (df.groupby('modalidade_nome', dropna=False)['valor_global']
                     .agg(n='count', valor='sum')
                     .nlargest(10, 'n').reset_index())
            f.write(f'  {"Modalidade":<40}  {"Contratos":>10}  {"Valor (R$ bi)":>14}\n')
            f.write(f'  {"-"*40}  {"-"*10}  {"-"*14}\n')
            for _, row in top.iterrows():
                nome_m = str(row['modalidade_nome'])[:39]
                f.write(f'  {nome_m:<40}  {int(row["n"]):>10,}  '
                        f'{row["valor"]/1e9:>14.4f}\n')

        # --- Problemas encontrados ---
        f.write(f'\n  PROBLEMAS ENCONTRADOS\n')
        f.write(f'  {"-"*55}\n')
        problemas = []
        for col, pct in nulos_pct_bruto[nulos_pct_bruto > 0].items():
            nivel = 'CRÍTICO' if pct > 30 else ('ALTO' if pct > 10 else 'BAIXO')
            problemas.append((nivel, col, pct))
        if datas_invalidas > 0:
            pct_inv = datas_invalidas / total_bruto * 100
            problemas.append(('ALTO' if pct_inv > 1 else 'BAIXO',
                              'data_assinatura (fora de 2021-2026)',
                              pct_inv))
        if not problemas:
            f.write('  ✓ Nenhum problema encontrado\n')
        else:
            for nivel, col, pct in sorted(problemas, key=lambda x: -x[2]):
                f.write(f'  [{nivel}] {col}: {pct:.4f}%\n')

        f.write(f'\n  Gerado em: {pd.Timestamp.now().strftime("%d/%m/%Y %H:%M")}\n')
        f.write(f'{sep}\n')

    print(saida.read_text(encoding='utf-8'))
    logger.info(f'✓ Relatório salvo em {saida}')


# ===========================================================================
# ETAPA 7: 5 GRÁFICOS + MARKDOWN
# ===========================================================================

def _carregar_para_graficos() -> pd.DataFrame:
    """Lê apenas colunas necessárias para os gráficos."""
    import matplotlib
    arquivos = sorted(SILVER_DIR.glob('contratos_*.parquet'))
    if not arquivos:
        logger.error(f'Nenhum arquivo em {SILVER_DIR}')
        sys.exit(1)

    logger.info(f'Carregando {len(arquivos)} arquivos para gráficos...')
    cols = ['data_assinatura', 'valor_global', 'nome_contratada', 'modalidade_nome']
    dfs  = []

    for arq in tqdm(arquivos, desc='Lendo Silver', unit='arq'):
        try:
            schema_names = pq.read_schema(arq).names
            cols_ok = [c for c in cols if c in schema_names]
            df = pd.read_parquet(arq, columns=cols_ok)
            df['data_assinatura'] = pd.to_datetime(df['data_assinatura'], errors='coerce')
            df = df.dropna(subset=['data_assinatura', 'valor_global'])
            df = df[df['valor_global'].between(1, 1e10)]
            df = df[df['data_assinatura'].dt.year.between(2021, datetime.now().year)]
            if not df.empty:
                dfs.append(df)
        except Exception as e:
            logger.warning(f'Erro lendo {arq.name}: {e}')

    if not dfs:
        logger.error('Nenhum dado válido para gráficos.')
        sys.exit(1)

    df_final = pd.concat(dfs, ignore_index=True)
    df_final['ano']     = df_final['data_assinatura'].dt.year
    df_final['mes_ano'] = (df_final['data_assinatura']
                           .dt.to_period('M').dt.to_timestamp())
    logger.info(f'Total carregado: {len(df_final):,} registros')
    return df_final


def gerar_graficos() -> None:
    """Gera os 5 gráficos exigidos e o arquivo Markdown."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    import seaborn as sns

    GRAFICOS_DIR.mkdir(parents=True, exist_ok=True)

    for estilo in ('seaborn-v0_8-darkgrid', 'seaborn-darkgrid', 'ggplot'):
        try:
            plt.style.use(estilo)
            break
        except OSError:
            continue

    sns.set_palette('husl')
    CORES = ['#2196F3', '#FF5722', '#4CAF50', '#9C27B0', '#FF9800']

    df = _carregar_para_graficos()
    graficos_gerados = []

    # ------------------------------------------------------------------
    # Gráfico 1 — Boxplot de valor_global por ano (escala log)
    # ------------------------------------------------------------------
    logger.info('Gerando gráfico 1: boxplot por ano...')
    anos = sorted(df['ano'].dropna().unique().astype(int))
    fig, ax = plt.subplots(figsize=(14, 7))

    data_por_ano = [
        np.log10(df.loc[df['ano'] == a, 'valor_global'].clip(lower=1).values)
        for a in anos
    ]
    import matplotlib
    _bp_kwargs = (
        {'tick_labels': anos} if tuple(int(x) for x in matplotlib.__version__.split('.')[:2]) >= (3, 9)
        else {'labels': anos}
    )
    bp = ax.boxplot(data_por_ano, **_bp_kwargs, patch_artist=True,
                    showfliers=True,
                    flierprops=dict(marker='.', markersize=2, alpha=0.3, color='gray'),
                    medianprops=dict(color='black', linewidth=2))
    for patch, cor in zip(bp['boxes'], sns.color_palette('husl', len(anos))):
        patch.set_facecolor(cor)
        patch.set_alpha(0.7)
    for i, a in enumerate(anos):
        med = np.log10(df.loc[df['ano'] == a, 'valor_global'].clip(lower=1).median())
        ax.text(i + 1, med, f'R${10**med:,.0f}',
                ha='center', va='bottom', fontsize=7, color='#333')
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'R${10**v:,.0f}'))
    ax.set_xlabel('Ano', fontsize=12)
    ax.set_ylabel('Valor Global (R$)', fontsize=12)
    ax.set_title('Distribuição de Valores por Ano (escala log)', fontsize=14, pad=15)
    ax.grid(axis='y', alpha=0.3)
    fig.tight_layout()
    p1 = GRAFICOS_DIR / '01_boxplot_anos.png'
    fig.savefig(p1, dpi=150, bbox_inches='tight')
    plt.close(fig)
    graficos_gerados.append(('Distribuição de Valores por Ano', p1,
        'Boxplot em escala log mostrando a distribuição do valor_global por ano. '
        'Cada caixa representa o IQR (P25–P75). '
        'Nota-se crescimento consistente no volume de contratos a partir de 2023.'))
    logger.info('  ✓ 01_boxplot_anos.png')

    # ------------------------------------------------------------------
    # Gráfico 2 — Histograma de distribuição de valores
    # ------------------------------------------------------------------
    logger.info('Gerando gráfico 2: histograma...')
    log_vals = np.log10(df['valor_global'].clip(lower=1))
    fig, axes = plt.subplots(1, 2, figsize=(15, 6))
    for ax_, log_y, cor, titulo in [
        (axes[0], False, CORES[0], 'Frequência Linear'),
        (axes[1], True,  CORES[1], 'Frequência Log'),
    ]:
        ax_.hist(log_vals, bins=80, color=cor, alpha=0.8,
                 edgecolor='white', log=log_y)
        ax_.set_title(f'Distribuição de Valores ({titulo})', fontsize=13)
        ax_.set_xlabel('Valor Global — log₁₀(R$)', fontsize=11)
        ax_.set_ylabel('Frequência' + (' (log)' if log_y else ''), fontsize=11)
        ax_.xaxis.set_major_formatter(
            mticker.FuncFormatter(lambda v, _: f'R${10**v:,.0f}'))
        ax_.tick_params(axis='x', rotation=30)
    fig.suptitle('Histograma de Valores dos Contratos PNCP', fontsize=14, y=1.01)
    fig.tight_layout()
    p2 = GRAFICOS_DIR / '02_histograma.png'
    fig.savefig(p2, dpi=150, bbox_inches='tight')
    plt.close(fig)
    graficos_gerados.append(('Histograma de Distribuição de Valores', p2,
        'Distribuição de valor_global em escala log₁₀. '
        'A maioria dos contratos concentra-se entre R$1mil e R$1mi. '
        'A cauda longa à direita indica presença de contratos de grande porte.'))
    logger.info('  ✓ 02_histograma.png')

    # ------------------------------------------------------------------
    # Gráfico 3 — Top 20 fornecedores
    # ------------------------------------------------------------------
    logger.info('Gerando gráfico 3: top fornecedores...')

    def _truncar(s, n=35):
        s = str(s)
        return s if len(s) <= n else s[:n] + '…'

    grp       = (df.groupby('nome_contratada', sort=False)['valor_global']
                   .agg(valor_total='sum', n_contratos='count')
                   .reset_index())
    top_valor = grp.nlargest(20, 'valor_total').sort_values('valor_total')
    top_qtd   = grp.nlargest(20, 'n_contratos').sort_values('n_contratos')
    fig, axes = plt.subplots(1, 2, figsize=(20, 10))
    for ax_, dados, col, xtitle, fmt_fn, cor in [
        (axes[0], top_valor, 'valor_total', 'Valor Total (R$ milhões)',
         lambda v: f'R${v:.0f}M', CORES[0]),
        (axes[1], top_qtd,   'n_contratos', 'Número de Contratos',
         lambda v: f'{int(v):,}',  CORES[2]),
    ]:
        bars = ax_.barh([_truncar(n) for n in dados['nome_contratada']],
                        dados[col] / (1e6 if col == 'valor_total' else 1),
                        color=cor, alpha=0.85)
        ax_.set_xlabel(xtitle, fontsize=11)
        ax_.set_title(f'Top 20 Fornecedores — {xtitle}', fontsize=13, pad=12)
        ax_.bar_label(bars, fmt=fmt_fn, padding=3, fontsize=8)
        ax_.set_xlim(right=dados[col].max() / (1e6 if col == 'valor_total' else 1) * 1.25)
    fig.tight_layout()
    p3 = GRAFICOS_DIR / '03_top_fornecedores.png'
    fig.savefig(p3, dpi=150, bbox_inches='tight')
    plt.close(fig)
    graficos_gerados.append(('Top 20 Fornecedores', p3,
        'Painel esquerdo: fornecedores com maior volume financeiro. '
        'Painel direito: fornecedores com maior número de contratos. '
        'Alta concentração nos 5 primeiros indica possível oligopólio em alguns segmentos.'))
    logger.info('  ✓ 03_top_fornecedores.png')

    # ------------------------------------------------------------------
    # Gráfico 4 — Série temporal mensal
    # ------------------------------------------------------------------
    logger.info('Gerando gráfico 4: série temporal...')
    serie = (df.groupby('mes_ano')['valor_global']
               .agg(valor_total='sum', n_contratos='count')
               .reset_index().sort_values('mes_ano'))
    fig, axes = plt.subplots(2, 1, figsize=(16, 10), sharex=True)
    for ax_, col, cor, ylabel, titulo in [
        (axes[0], 'valor_total',  CORES[0],
         'Valor Total (R$ milhões)', 'Evolução Mensal — Valor Total de Contratos'),
        (axes[1], 'n_contratos',  CORES[1],
         'Quantidade de Contratos',  'Evolução Mensal — Quantidade de Contratos'),
    ]:
        vals = serie[col] / (1e6 if col == 'valor_total' else 1)
        ax_.plot(serie['mes_ano'], vals, marker='o', linewidth=2,
                 markersize=3, color=cor)
        ax_.fill_between(serie['mes_ano'], vals, alpha=0.12, color=cor)
        ax_.set_ylabel(ylabel, fontsize=11)
        ax_.set_title(titulo, fontsize=13, pad=10)
        ax_.grid(True, alpha=0.3)
        if col == 'valor_total':
            ax_.yaxis.set_major_formatter(
                mticker.FuncFormatter(lambda v, _: f'R${v:,.0f}M'))
        else:
            ax_.yaxis.set_major_formatter(
                mticker.FuncFormatter(lambda v, _: f'{int(v):,}'))
    axes[1].set_xlabel('Mês/Ano', fontsize=11)
    fig.autofmt_xdate(rotation=40, ha='right')
    fig.tight_layout()
    p4 = GRAFICOS_DIR / '04_serie_temporal.png'
    fig.savefig(p4, dpi=150, bbox_inches='tight')
    plt.close(fig)
    graficos_gerados.append(('Série Temporal Mensal', p4,
        'Evolução mês a mês do valor total e da quantidade de contratos publicados. '
        'Picos em dezembro (pressão de final de exercício orçamentário) e '
        'crescimento acentuado a partir de 2024 refletem a adesão crescente ao PNCP.'))
    logger.info('  ✓ 04_serie_temporal.png')

    # ------------------------------------------------------------------
    # Gráfico 5 — Heatmap de correlações
    # ------------------------------------------------------------------
    logger.info('Gerando gráfico 5: heatmap de correlações...')
    df_corr = df.assign(
        mes=df['data_assinatura'].dt.month,
        trimestre=df['data_assinatura'].dt.quarter,
        ano_num=df['data_assinatura'].dt.year,
        log_valor=np.log10(df['valor_global'].clip(lower=1)),
    )[['valor_global', 'log_valor', 'mes', 'trimestre', 'ano_num']].dropna()

    corr = df_corr.corr()
    labels = {'valor_global': 'Valor (R$)', 'log_valor': 'log₁₀(Valor)',
              'mes': 'Mês', 'trimestre': 'Trimestre', 'ano_num': 'Ano'}
    corr = corr.rename(index=labels, columns=labels)
    mask = np.triu(np.ones_like(corr, dtype=bool), k=0)
    mask_df = pd.DataFrame(mask, index=corr.index, columns=corr.columns)

    fig, ax = plt.subplots(figsize=(10, 8))
    sns.heatmap(corr, mask=mask_df, annot=True, fmt='.2f',
                cmap='RdBu_r', center=0, vmin=-1, vmax=1,
                square=True, linewidths=0.5,
                cbar_kws={'shrink': 0.75, 'label': 'Correlação de Pearson'},
                annot_kws={'size': 12}, ax=ax)
    ax.set_title('Correlações entre Variáveis Numéricas', fontsize=14, pad=15)
    fig.tight_layout()
    p5 = GRAFICOS_DIR / '05_correlacoes.png'
    fig.savefig(p5, dpi=150, bbox_inches='tight')
    plt.close(fig)
    graficos_gerados.append(('Heatmap de Correlações', p5,
        'Correlação de Pearson entre variáveis numéricas. '
        'log₁₀(Valor) e Valor mostram correlação alta (esperado). '
        'Correlação positiva entre Ano e Valor reflete crescimento temporal dos contratos.'))
    logger.info('  ✓ 05_correlacoes.png')

    # ------------------------------------------------------------------
    # Geração do Markdown com os 5 gráficos embedados
    # ------------------------------------------------------------------
    md_path = GRAFICOS_DIR / 'graficos_silver.md'
    with open(md_path, 'w', encoding='utf-8') as md:
        md.write('# Gráficos da Camada Silver — PNCP Contratos Públicos\n\n')
        md.write(f'**Lab01_PART1_3734509** — Gerado em '
                 f'{pd.Timestamp.now().strftime("%d/%m/%Y %H:%M")}\n\n')
        md.write('**Fonte:** Portal Nacional de Contratações Públicas (PNCP)  \n')
        md.write(f'**Período:** {df["data_assinatura"].min().strftime("%Y-%m")} a '
                 f'{df["data_assinatura"].max().strftime("%Y-%m")}  \n')
        md.write(f'**Total de registros:** {len(df):,}  \n\n')
        md.write('---\n\n')

        for idx, (titulo, caminho, descricao) in enumerate(graficos_gerados, 1):
            md.write(f'## Gráfico {idx} — {titulo}\n\n')
            md.write(f'![{titulo}]({caminho.name})\n\n')
            md.write(f'{descricao}\n\n')
            md.write('---\n\n')

    logger.info(f'✓ Markdown gerado: {md_path}')
    print(f'\n✓ 5 gráficos gerados em: {GRAFICOS_DIR.resolve()}')
    print(f'✓ Markdown: {md_path}')


# ===========================================================================
# MAIN
# ===========================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Silver — tratamento, relatório e gráficos dos contratos PNCP'
    )
    parser.add_argument('--relatorio', action='store_true',
                        help='Gera relatório de qualidade após o tratamento')
    parser.add_argument('--graficos',  action='store_true',
                        help='Gera 5 gráficos + Markdown após o tratamento')
    parser.add_argument('--tudo',      action='store_true',
                        help='Executa tratamento + relatório + gráficos')
    args = parser.parse_args()

    tratar_todos_meses()

    if args.tudo or args.relatorio:
        gerar_relatorio_qualidade()

    if args.tudo or args.graficos:
        gerar_graficos()


if __name__ == '__main__':
    main()
