-- =============================================================================
-- dim_orgaos.sql — Dimensão Órgãos Contratantes
-- Materialização: table (schema gold)
-- =============================================================================

with stg as (

    select * from {{ ref('stg_contratos') }}

),

dim as (

    select distinct
        orgao_cnpj                                          as orgao_entidade_id,
        orgao_nome                                          as nome_orgao,
        codigo_unidade,
        nome_unidade,
        count(*) over (
            partition by orgao_cnpj
        )                                                   as total_contratos

    from stg

    where orgao_cnpj is not null

)

select * from dim
