-- =============================================================================
-- stg_contratos.sql — Staging: limpeza final e padronização
-- Materialização: view (schema staging)
--
-- Seleciona e renomeia campos da Silver, aplica cast de tipos
-- e filtra registros com valor_global inválido.
-- =============================================================================

with source as (

    select * from {{ source('silver', 'contratos') }}

),

staged as (

    select
        -- Identificação
        id                                                  as contrato_id,
        numero_contrato,
        processo,
        ano_mes_coleta,

        -- Órgão
        orgao_entidade_id                                   as orgao_cnpj,
        orgao_entidade_nome                                 as orgao_nome,
        codigo_unidade,
        nome_unidade,

        -- Fornecedor
        cnpj_contratada                                     as fornecedor_cnpj,
        nome_contratada                                     as fornecedor_nome,
        ni_fornecedor,
        nome_razao_social_fornecedor,

        -- Categoria / Modalidade
        categoria_processo_id,
        categoria_processo_nome,
        modalidade_id,
        modalidade_nome,

        -- Valores — usa macro format_brl para documentação
        valor_inicial::numeric(18,2)                        as valor_inicial,
        valor_global::numeric(18,2)                         as valor_global,
        valor_parcelas::numeric(18,2)                       as valor_parcelas,

        -- Datas
        data_assinatura::date                               as data_assinatura,
        data_vigencia_inicio::date                          as data_vigencia_inicio,
        data_vigencia_fim::date                             as data_vigencia_fim,
        data_publicacao::date                               as data_publicacao,

        -- Metadados de carga
        data_coleta

    from source

    where
        valor_global is not null
        and valor_global > 0
        and id is not null

)

select * from staged
