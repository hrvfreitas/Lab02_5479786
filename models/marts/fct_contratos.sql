-- =============================================================================
-- fct_contratos.sql — Mart: Tabela Fato de Contratos
-- Materialização: table (schema gold)
--
-- Tabela central do Star Schema Gold gerado pelo DBT.
-- Referencia todas as dimensões via FK e inclui métricas calculadas.
-- =============================================================================

with stg as (

    select * from {{ ref('stg_contratos') }}

),

dim_orgao as (

    select * from {{ ref('dim_orgaos') }}

),

dim_fornecedor as (

    select * from {{ ref('dim_fornecedores') }}

),

dim_modalidade as (

    select * from {{ ref('dim_modalidades') }}

),

fato as (

    select
        -- Chave surrogate
        row_number() over (order by s.contrato_id)          as id_contrato,

        -- Identificação do contrato
        s.contrato_id                                       as id_contrato_pncp,
        s.numero_contrato,
        s.processo,

        -- Categoria (campo degenerado — não vira dimensão própria)
        s.categoria_processo_id,
        s.categoria_processo_nome                           as categoria_processo,

        -- FKs para dimensões
        s.orgao_cnpj                                        as orgao_entidade_id,
        s.fornecedor_cnpj                                   as cnpj_contratada,
        s.modalidade_id                                     as id_modalidade,

        -- Datas
        s.data_assinatura,
        s.data_vigencia_inicio,
        s.data_vigencia_fim,
        s.data_publicacao,

        -- Métricas
        s.valor_inicial,
        s.valor_global,
        s.valor_parcelas,

        -- Métricas calculadas pelo DBT
        {{ calcular_variacao_valor('s.valor_global', 's.valor_inicial') }}
            as variacao_valor_pct,

        case
            when s.data_publicacao is not null
             and s.data_assinatura is not null
            then s.data_publicacao - s.data_assinatura
        end                                                 as delay_publicacao_dias,

        case
            when s.data_vigencia_fim > current_date then true
            else false
        end                                                 as contrato_ativo,

        -- Metadados
        s.ano_mes_coleta,
        s.data_coleta,
        current_timestamp                                   as data_carga_dbt

    from stg s

)

select * from fato
