-- =============================================================================
-- dim_modalidades.sql — Dimensão Modalidades de Contrato
-- Materialização: table (schema gold)
-- =============================================================================

with stg as (

    select * from {{ ref('stg_contratos') }}

),

dim as (

    select distinct
        modalidade_id                                       as id_modalidade,
        modalidade_nome                                     as nome_modalidade,
        count(*) over (
            partition by modalidade_id
        )                                                   as total_contratos

    from stg

    where modalidade_id is not null

)

select * from dim
