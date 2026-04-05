# =============================================================================
# bronze.py — Camada Bronze (Raw)
# Lab01_PART1_5479786 — PNCP Contratos Públicos
#
# Objetivo: ingestão "as-is" da API PNCP, salvando cada página como JSON
# bruto em data/raw/AAAA_MM/pagina_NNNN.json SEM nenhuma alteração.
#
# Checkpoint: manifesto por mês (_manifesto.json) garante retomada sem
# reprocessar tudo.
# Janela deslizante: re-verifica os últimos JANELA_REVERIFICACAO_MESES para
# capturar publicações retroativas com custo mínimo (1 request extra/mês).
# =============================================================================
import calendar
import json
import locale
import logging
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

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
# Diretórios e Logging
# ---------------------------------------------------------------------------
RAW_DIR  = Path('data/raw')
LOG_DIR  = Path('logs')
TEMP_DIR = Path('temp')

for d in (RAW_DIR, LOG_DIR, TEMP_DIR):
    d.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(threadName)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_DIR / 'bronze.log', encoding='utf-8', errors='replace'),
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuração da API
# ---------------------------------------------------------------------------
API_CONFIG: Dict = {
    'base_url':            'https://pncp.gov.br/api/consulta/v1/contratos',
    'timeout':             180,
    'max_retries':         5,
    'backoff_factor':      2,
    'page_size':           500,   # máximo aceito pela API
    'delay_between_pages': 0.15,
    'max_workers':         5,
}

# Meses dentro desta janela são re-verificados a cada execução para capturar
# publicações retroativas. Custo: 1 request HTTP por mês na janela.
JANELA_REVERIFICACAO_MESES = 6

ANO_INICIO = 2021

# ---------------------------------------------------------------------------
# Sessões HTTP reutilizáveis por thread
# ---------------------------------------------------------------------------
_thread_local = threading.local()


def _nova_sessao(worker_id: int) -> requests.Session:
    sessao = requests.Session()
    retry = Retry(
        total=API_CONFIG['max_retries'],
        backoff_factor=API_CONFIG['backoff_factor'],
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=['GET'],
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=20)
    sessao.mount('http://', adapter)
    sessao.mount('https://', adapter)
    sessao.headers.update({
        'User-Agent': f'PNCPBronze/1.0 (Worker {worker_id})',
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
    })
    return sessao


def get_sessao(worker_id: int) -> requests.Session:
    """Retorna sessão HTTP reutilizável da thread atual."""
    if not hasattr(_thread_local, 'sessao'):
        _thread_local.sessao = _nova_sessao(worker_id)
    return _thread_local.sessao


# ---------------------------------------------------------------------------
# Shutdown por Control+C
# ---------------------------------------------------------------------------
shutdown_flag = threading.Event()


def _signal_handler(sig, frame):
    logger.info('Interrupção recebida — finalizando graciosamente...')
    shutdown_flag.set()


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# ---------------------------------------------------------------------------
# Checkpoint + janela deslizante
# ---------------------------------------------------------------------------
def _dir_mes(ano: int, mes: int) -> Path:
    return RAW_DIR / f'{ano}_{mes:02d}'


def _manifesto(ano: int, mes: int) -> Path:
    return _dir_mes(ano, mes) / '_manifesto.json'


def _ler_manifesto(ano: int, mes: int) -> dict:
    m = _manifesto(ano, mes)
    if not m.exists():
        return {}
    try:
        with open(m, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def _dentro_da_janela(ano: int, mes: int) -> bool:
    now = datetime.now()
    return (now.year - ano) * 12 + (now.month - mes) <= JANELA_REVERIFICACAO_MESES


def mes_ja_baixado(ano: int, mes: int) -> bool:
    """
    Fora da janela + manifesto ok → imutável, pula.
    Dentro da janela → passa por checagem leve em coletar_mes().
    """
    manifesto = _ler_manifesto(ano, mes)
    if manifesto.get('status') != 'ok':
        return False
    return not _dentro_da_janela(ano, mes)


def mes_precisa_reprocessar(ano: int, mes: int, total_paginas_api: int) -> bool:
    """
    Chamado APÓS obter totalPaginas da API (0 custo extra — request já feito).
    True se o mês precisa ser (re)baixado.
    """
    manifesto = _ler_manifesto(ano, mes)
    if manifesto.get('status') != 'ok':
        return True
    if _dentro_da_janela(ano, mes):
        if total_paginas_api > manifesto.get('total_paginas', 0):
            logger.info(
                f'{ano}/{mes:02d}: re-verificação — API={total_paginas_api} págs, '
                f'manifesto={manifesto.get("total_paginas", 0)}. Baixando diferença.'
            )
            return True
        return False
    return False


def _salvar_manifesto(ano: int, mes: int, total_paginas: int,
                      total_registros: int, paginas_vazias: int) -> None:
    dados = {
        'status':          'ok',
        'ano':             ano,
        'mes':             mes,
        'total_paginas':   total_paginas,
        'total_registros': total_registros,
        'paginas_vazias':  paginas_vazias,
        'data_coleta':     datetime.now().isoformat(),
    }
    with open(_manifesto(ano, mes), 'w', encoding='utf-8') as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Download de uma página (worker paralelo)
# ---------------------------------------------------------------------------
def baixar_pagina(
    worker_id: int,
    ano: int,
    mes: int,
    params_base: dict,
    pagina: int,
    pbar: Optional[tqdm] = None,
) -> Tuple[int, Optional[List[dict]]]:
    """
    Retorna (pagina, registros).
    registros=None  → falha definitiva.
    registros=[]    → página vazia / sem dados.
    """
    if shutdown_flag.is_set():
        return pagina, None

    sessao = get_sessao(worker_id)
    params = {**params_base, 'pagina': pagina}
    tag    = f'W{worker_id}-P{pagina}'

    for tentativa in range(API_CONFIG['max_retries']):
        if shutdown_flag.is_set():
            return pagina, None
        try:
            resp = sessao.get(API_CONFIG['base_url'], params=params,
                              timeout=API_CONFIG['timeout'])
            resp.raise_for_status()

            if not resp.content or not resp.text.strip():
                logger.warning(f'{tag}: resposta vazia')
                return pagina, []

            dados     = resp.json()
            registros = dados.get('data', [])

            if pbar:
                pbar.update(1)
                pbar.set_postfix({'w': worker_id, 'p': pagina, 'n': len(registros)})

            logger.debug(f'{tag} OK: {len(registros)} registros')
            time.sleep(API_CONFIG['delay_between_pages'])
            return pagina, registros

        except requests.exceptions.RequestException as e:
            if tentativa < API_CONFIG['max_retries'] - 1:
                delay = API_CONFIG['backoff_factor'] * (2 ** tentativa)
                logger.warning(f'{tag} erro (tent. {tentativa+1}): {e} — aguarda {delay}s')
                time.sleep(delay)
            else:
                logger.error(f'{tag} falha após {API_CONFIG["max_retries"]} tentativas: {e}')
                return pagina, None

        except json.JSONDecodeError as e:
            logger.error(f'{tag} JSON inválido: {e} — conteúdo: {resp.text[:200]!r}')
            return pagina, []

        except Exception as e:
            logger.error(f'{tag} erro inesperado: {e}')
            return pagina, None

    return pagina, None


# ---------------------------------------------------------------------------
# Coleta de um mês inteiro
# ---------------------------------------------------------------------------
def coletar_mes(ano: int, mes: int) -> Optional[int]:
    """
    Baixa todas as páginas do mês e salva JSON bruto em data/raw/.
    Retorna total de registros salvos, ou None em caso de erro fatal.
    """
    data_ini   = f'{ano}{mes:02d}01'
    ultimo_dia = calendar.monthrange(ano, mes)[1]
    data_fim   = f'{ano}{mes:02d}{ultimo_dia:02d}'

    params_base = {
        'dataInicial':   data_ini,
        'dataFinal':     data_fim,
        'tamanhoPagina': API_CONFIG['page_size'],
    }

    # Request de página 1: descobre metadata E já traz dados
    sessao = _nova_sessao(0)
    try:
        resp = sessao.get(API_CONFIG['base_url'],
                          params={**params_base, 'pagina': 1},
                          timeout=API_CONFIG['timeout'])
        resp.raise_for_status()

        if not resp.content or not resp.text.strip():
            logger.warning(f'{ano}/{mes:02d}: corpo vazio — sem dados')
            _dir_mes(ano, mes).mkdir(parents=True, exist_ok=True)
            _salvar_manifesto(ano, mes, 0, 0, 0)
            return 0

        meta           = resp.json()
        total_paginas  = meta.get('totalPaginas', 0)
        total_estimado = meta.get('totalRegistros', 0)
        pagina1_data   = meta.get('data', [])

    except json.JSONDecodeError as e:
        logger.warning(f'{ano}/{mes:02d}: JSON inválido ({e}) — pulando mês')
        return 0
    except Exception as e:
        logger.error(f'{ano}/{mes:02d}: erro obtendo metadata — {e}')
        return None

    if total_paginas == 0:
        logger.info(f'{ano}/{mes:02d}: sem dados (totalPaginas=0)')
        _dir_mes(ano, mes).mkdir(parents=True, exist_ok=True)
        _salvar_manifesto(ano, mes, 0, 0, 0)
        return 0

    # Checagem leve — usa totalPaginas já obtido, zero custo extra
    if not mes_precisa_reprocessar(ano, mes, total_paginas):
        logger.info(
            f'{ano}/{mes:02d}: sem novidades '
            f'(API={total_paginas} págs = manifesto). Pulando.'
        )
        return 0

    logger.info(f'{ano}/{mes:02d}: {total_paginas} páginas, ~{total_estimado} registros')

    dir_mes         = _dir_mes(ano, mes)
    dir_mes.mkdir(parents=True, exist_ok=True)
    total_registros = 0
    paginas_vazias  = 0
    lock            = threading.Lock()

    def _salvar_pagina(pagina: int, registros: List[dict]) -> None:
        nonlocal total_registros
        with open(dir_mes / f'pagina_{pagina:04d}.json', 'w', encoding='utf-8') as f:
            json.dump(registros, f, ensure_ascii=False)
        with lock:
            total_registros += len(registros)

    _salvar_pagina(1, pagina1_data)

    if total_paginas == 1:
        _salvar_manifesto(ano, mes, total_paginas, total_registros, paginas_vazias)
        logger.info(f'✓ {ano}/{mes:02d}: {total_registros} registros (1 página)')
        return total_registros

    paginas_restantes = list(range(2, total_paginas + 1))

    with tqdm(total=len(paginas_restantes), desc=f'{ano}/{mes:02d}',
              unit='pág', leave=True) as pbar:
        with ThreadPoolExecutor(max_workers=API_CONFIG['max_workers']) as executor:
            futures = {
                executor.submit(
                    baixar_pagina,
                    (p % API_CONFIG['max_workers']) + 1,
                    ano, mes, params_base, p, pbar
                ): p
                for p in paginas_restantes
                if not shutdown_flag.is_set()
            }

            for future in as_completed(futures):
                if shutdown_flag.is_set():
                    executor.shutdown(wait=False)
                    break
                try:
                    pagina, registros = future.result(timeout=API_CONFIG['timeout'])
                    if registros is None:
                        logger.error(
                            f'{ano}/{mes:02d}: falha na página {pagina}, '
                            'mês será reprocessado na próxima execução'
                        )
                        return None
                    if not registros:
                        with lock:
                            paginas_vazias += 1
                    else:
                        _salvar_pagina(pagina, registros)
                except Exception as e:
                    logger.error(f'{ano}/{mes:02d}: erro no future — {e}')

    _salvar_manifesto(ano, mes, total_paginas, total_registros, paginas_vazias)
    logger.info(f'✓ {ano}/{mes:02d}: {total_registros:,} registros salvos em {dir_mes}')
    return total_registros


# ---------------------------------------------------------------------------
# Teste de conectividade
# ---------------------------------------------------------------------------
def testar_api() -> bool:
    try:
        sessao = _nova_sessao(0)
        resp   = sessao.get(
            API_CONFIG['base_url'],
            params={
                'pagina': 1, 'tamanhoPagina': API_CONFIG['page_size'],
                'dataInicial': '20240101', 'dataFinal': '20240131',
            },
            timeout=30,
        )
        resp.raise_for_status()
        if not resp.content:
            logger.error('API retornou corpo vazio no teste')
            return False
        data = resp.json()
        if 'totalPaginas' in data:
            logger.info('API testada com sucesso!')
            return True
        logger.error(f'Formato inesperado: {list(data.keys())}')
        return False
    except Exception as e:
        logger.error(f'Erro ao testar API: {e}')
        return False


# ---------------------------------------------------------------------------
# Orquestração principal
# ---------------------------------------------------------------------------
def coletar_todos_meses() -> None:
    logger.info('=' * 60)
    logger.info(f'BRONZE — COLETA RAW PNCP ({API_CONFIG["max_workers"]} workers)')
    logger.info(f'Janela de re-verificação: {JANELA_REVERIFICACAO_MESES} meses')
    logger.info('=' * 60)

    if not testar_api():
        logger.error('API indisponível. Encerrando.')
        return

    now               = datetime.now()
    ano_atual, mes_atual = now.year, now.month

    meses = [
        (ano, mes)
        for ano in range(ANO_INICIO, ano_atual + 1)
        for mes in (range(1, mes_atual + 1) if ano == ano_atual else range(1, 13))
        if not mes_ja_baixado(ano, mes)
    ]

    logger.info(f'Meses a processar: {len(meses)}')
    if not meses:
        logger.info('Todos os meses já estão na camada Bronze.')
        return

    total_regs = 0
    total_ok   = 0

    try:
        for i, (ano, mes) in enumerate(meses, 1):
            if shutdown_flag.is_set():
                break
            logger.info(f'--- {ano}/{mes:02d} ({i}/{len(meses)}) ---')
            resultado = coletar_mes(ano, mes)
            if resultado is not None:
                total_regs += resultado
                total_ok   += 1
            time.sleep(3)
    except KeyboardInterrupt:
        logger.info('Interrompido pelo usuário')
    finally:
        logger.info('=' * 60)
        logger.info(f'Meses concluídos : {total_ok}')
        logger.info(f'Total de registros: {total_regs:,}')
        logger.info('=' * 60)


if __name__ == '__main__':
    coletar_todos_meses()
