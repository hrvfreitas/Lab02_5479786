-- =============================================================================
-- tests/assert_publicacao_apos_assinatura.sql
-- Teste Singular — Regra de Negócio
--
-- Verifica contratos onde data_publicacao < data_assinatura.
-- Não é possível publicar um contrato antes de assiná-lo.
-- Se esta query retornar linhas, indica erro na fonte de dados.
-- Tolerância: apenas casos onde ambas as datas estão preenchidas.
-- =============================================================================

select
    id_contrato_pncp,
    orgao_entidade_id,
    data_assinatura,
    data_publicacao,
    data_publicacao - data_assinatura as dias_antecipacao
from {{ ref('fct_contratos') }}
where data_assinatura  is not null
  and data_publicacao  is not null
  and data_publicacao  < data_assinatura
