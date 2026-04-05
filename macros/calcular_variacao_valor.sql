-- =============================================================================
-- macros/calcular_variacao_valor.sql
--
-- Macro: calcular_variacao_valor
-- Calcula a variação percentual entre valor_global e valor_inicial.
-- Retorna NULL quando valor_inicial = 0 (evita divisão por zero).
-- Retorna NULL quando valor_inicial é nulo.
-- Valores positivos indicam aditivos; negativos indicam reduções.
--
-- Uso nos models:
--   {{ calcular_variacao_valor('valor_global', 'valor_inicial') }}
-- =============================================================================

{% macro calcular_variacao_valor(valor_global, valor_inicial) %}

    case
        when {{ valor_inicial }} is null then null
        when {{ valor_inicial }} = 0     then null
        else
            round(
                (
                    ({{ valor_global }} - {{ valor_inicial }})
                    / {{ valor_inicial }}
                    * 100
                )::numeric,
                2
            )
    end

{% endmacro %}


-- =============================================================================
-- Macro auxiliar: format_brl
-- Formata um valor numérico como string em reais (R$ X.XXX,XX).
-- Útil para colunas descritivas em models de apresentação.
--
-- Uso:
--   {{ format_brl('valor_global') }}
-- =============================================================================

{% macro format_brl(coluna) %}

    'R$ ' || to_char({{ coluna }}, 'FM999G999G999G990D00')

{% endmacro %}


-- =============================================================================
-- Macro: categoria_risco_valor
-- Classifica o contrato por faixa de valor.
--
-- Uso:
--   {{ categoria_risco_valor('valor_global') }}
-- =============================================================================

{% macro categoria_risco_valor(coluna) %}

    case
        when {{ coluna }} < 10000          then 'Micro (< R$10k)'
        when {{ coluna }} < 100000         then 'Pequeno (R$10k–R$100k)'
        when {{ coluna }} < 1000000        then 'Médio (R$100k–R$1M)'
        when {{ coluna }} < 10000000       then 'Grande (R$1M–R$10M)'
        when {{ coluna }} < 100000000      then 'Alto Valor (R$10M–R$100M)'
        else                                    'Estratégico (> R$100M)'
    end

{% endmacro %}
