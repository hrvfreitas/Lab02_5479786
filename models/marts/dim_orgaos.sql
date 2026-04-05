-- =============================================================================
-- dim_orgaos.sql — Dimensão Órgãos Contratantes
-- Materialização: table (schema gold)
-- =============================================================================
with stg as (
    select * from {{ ref('stg_contratos') }}
),

dim as (
    select
        orgao_cnpj                      as orgao_entidade_id,
        max(orgao_nome)                 as nome_orgao,
        max(codigo_unidade)             as codigo_unidade,
        max(nome_unidade)               as nome_unidade,
        count(*)                        as total_contratos
    from stg
    where orgao_cnpj is not null
    group by 1 -- Garante um registro único por CNPJ do órgão
)

select * from dim
