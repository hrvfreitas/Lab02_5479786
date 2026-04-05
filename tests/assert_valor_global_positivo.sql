-- =============================================================================
-- tests/assert_valor_global_positivo.sql
-- Teste Singular — Regra de Negócio
--
-- Verifica se existem contratos com valor_global <= 0 na fato.
-- O filtro de sanidade do silver.py e o staging devem ter eliminado
-- esses registros. Se esta query retornar qualquer linha, o teste falha.
-- =============================================================================

select
    id_contrato_pncp,
    orgao_entidade_id,
    valor_global,
    ano_mes_coleta
from {{ ref('fct_contratos') }}
where valor_global <= 0
   or valor_global is null
