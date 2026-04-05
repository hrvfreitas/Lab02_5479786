-- =============================================================================
-- dim_fornecedores.sql — Dimensão Fornecedores
-- Materialização: table (schema gold)
-- =============================================================================

with stg as (

    select * from {{ ref('stg_contratos') }}

),

dim as (

    select distinct
        fornecedor_cnpj                                     as cnpj_contratada,
        fornecedor_nome                                     as nome_contratada,
        nome_razao_social_fornecedor,
        ni_fornecedor,
        count(*) over (
            partition by fornecedor_cnpj
        )                                                   as total_contratos

    from stg

    where fornecedor_cnpj is not null

)

select * from dim
