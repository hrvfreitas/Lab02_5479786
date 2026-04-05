-- =============================================================================
-- stg_contratos.sql — Staging: limpeza final e padronização
-- Materialização: view (schema staging)
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

        -- Valores
        valor_inicial::numeric(18,2)                         as valor_inicial,
        valor_global::numeric(18,2)                          as valor_global,
        valor_parcelas::numeric(18,2)                        as valor_parcelas,

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
        
        -- Resolve ERROR: id_contrato not null
        and id is not null

        -- Resolve ERROR: assert_publicacao_apos_assinatura
        -- Filtra registros onde a publicação ocorreu antes da assinatura (erro de input do PNCP)
        and data_publicacao::date >= data_assinatura::date

        -- Filtra CNPJs/IDs de fornecedor nulos para evitar falhas nas dimensões Gold
        and cnpj_contratada is not null
        and orgao_entidade_id is not null

)

select * from staged
