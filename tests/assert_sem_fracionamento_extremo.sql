-- =============================================================================
-- tests/assert_sem_fracionamento_extremo.sql
-- Teste Singular — Regra de Negócio
--
-- Identifica possível fracionamento de licitação:
-- mesmo fornecedor + mesmo órgão + mesmo mês com 20+ contratos.
-- Contratos fracionados fogem da modalidade mais rigorosa (Lei 14.133/2021).
-- Esta query retorna os casos extremos para auditoria manual.
-- O teste falha se encontrar algum caso com >= 20 contratos/mês.
-- =============================================================================

select
    cnpj_contratada,
    orgao_entidade_id,
    ano_mes_coleta,
    count(*)                                as qtd_contratos_mes,
    round(sum(valor_global)::numeric, 2)    as valor_total_mes
from {{ ref('fct_contratos') }}
where cnpj_contratada is not null
group by cnpj_contratada, orgao_entidade_id, ano_mes_coleta
having count(*) >= 20
order by qtd_contratos_mes desc
