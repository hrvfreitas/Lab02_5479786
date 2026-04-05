-- =============================================================================
-- dim_modalidades.sql — Dimensão Modalidades de Contrato
-- Materialização: table (schema gold)
-- ======================================================================
with stg as (
    select * from {{ ref('stg_contratos') }}
),

dim as (
    select
        modalidade_id                   as id_modalidade,
        max(modalidade_nome)            as nome_modalidade,
        count(*)                        as total_contratos
    from stg
    where modalidade_id is not null
    group by 1
)

select * from dim
