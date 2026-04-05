-- =============================================================================
-- dim_fornecedores.sql — Dimensão Fornecedores
-- Materialização: table (schema gold)
-- =====================================================================
with stg as (
    select * from {{ ref('stg_contratos') }}
),

dim as (
    select
        fornecedor_cnpj                 as cnpj_contratada,
        max(fornecedor_nome)            as nome_contratada,
        max(nome_razao_social_fornecedor) as nome_razao_social_fornecedor,
        max(ni_fornecedor)              as ni_fornecedor,
        count(*)                        as total_contratos
    from stg
    where fornecedor_cnpj is not null
    group by 1 -- Garante um registro único por CNPJ
)

select * from dim
