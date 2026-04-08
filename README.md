# Lab02_5479786
## Transformação de Dados com DBT — PNCP Contratos Públicos
### Hercules Ramos Veloso de Freitas

**Disciplina:** Fundamentos de Engenharia de Dados  
**Aluno:** Hercules Ramos Veloso de Freitas — NUSP 5479786  
**Repositório Lab02:** https://github.com/hrvfreitas/Lab02_5479786  
**Repositório Lab01:** https://github.com/hrvfreitas/Lab01_PART1_5479786  
**Escola:** Escola Politécnica USP — PECE Big Data  
**Professores:** Prof. Dr. Pedro L. P. Corrêa & Profa. Dra. Jeaneth Machicao

---

## Sumário

1. [Visão Geral](#1-visão-geral)
2. [Arquitetura](#2-arquitetura)
3. [Estrutura de Diretórios](#3-estrutura-de-diretórios)
4. [Infraestrutura Docker](#4-infraestrutura-docker)
5. [Passo a Passo para Reproduzir](#5-passo-a-passo-para-reproduzir)
6. [Modelos DBT](#6-modelos-dbt)
7. [Macros](#7-macros)
8. [Testes](#8-testes)
9. [Documentação DBT](#9-documentação-dbt)
10. [Visualização — Metabase](#10-visualização--metabase)

---

## 1. Visão Geral

O Lab02 substitui os scripts `gold_setup.py` e `gold_load.py` do Lab01 pelo **DBT (Data Build Tool)**, introduzindo transformação declarativa SQL com versionamento, testes automatizados, lineage e documentação.

**Ponto de partida:** os Parquets da camada Silver gerados pelo Lab01 são carregados no PostgreSQL via `silver_to_postgres.py` e o DBT transforma de `silver` → `staging` → `gold`.

| Componente | Tecnologia | Papel |
|---|---|---|
| **Carga Silver** | `silver_to_postgres.py` | Parquets → `pncp_db.silver.contratos` |
| **Transformação** | DBT 1.8+ (dbt-postgres) | `silver` → `staging` → `gold` |
| **Banco de dados** | PostgreSQL 16 | Armazena Silver, Staging e Gold |
| **Visualização** | Metabase | Dashboards sobre o schema `gold` |
| **Infraestrutura** | Docker Compose | 4 containers orquestrados |

---

## 2. Arquitetura

### Fluxo completo

```
data/silver/*.parquet  (Lab01_PART1_5479786)
          │
          ▼
silver_to_postgres.py ──────────► pncp_db.silver.contratos
  (roda localmente com localhost)        │
                                         ▼
                            ┌────────────────────────────────┐
                            │  DBT (container pncp_dbt)       │
                            │                                  │
                            │  source: silver.contratos        │
                            │          │                       │
                            │          ▼                       │
                            │  stg_contratos  (view·staging)   │
                            │          │                       │
                            │    ┌─────┼──────┬────────────┐  │
                            │    ▼     ▼      ▼            ▼  │
                            │  dim_  dim_   dim_      fct_    │
                            │  orgaos forn. modal.  contratos  │
                            │         (tables · gold)          │
                            └────────────────────────────────┘
                                         │
                                         ▼
                              Metabase (porta 3000)
                         conecta direto em pncp_db.gold
```

### Lineage DBT

```
[source] silver.contratos
        │
        └──► stg_contratos              view  · pncp_db.staging
                  │
                  ├──► dim_orgaos       table · pncp_db.gold
                  ├──► dim_fornecedores table · pncp_db.gold
                  ├──► dim_modalidades  table · pncp_db.gold
                  └──► fct_contratos    table · pncp_db.gold
                            ├── ref(dim_orgaos)       FK
                            ├── ref(dim_fornecedores) FK
                            └── ref(dim_modalidades)  FK
```

### Schemas no banco após execução

| Schema | Criado por | Conteúdo |
|---|---|---|
| `silver` | `init-db` + `silver_to_postgres.py` | Tabela bruta dos Parquets |
| `staging` | DBT | View `stg_contratos` |
| `gold` | DBT | `fct_contratos`, `dim_orgaos`, `dim_fornecedores`, `dim_modalidades` |
| `metabase_meta` | `init-db` (automático) | Metadados internos do Metabase |

---

## 3. Estrutura de Diretórios

```
Lab02_5479786/
├── docker-compose.yml          # 4 containers: postgres, dbt, dbt-docs, metabase
├── Dockerfile.dbt              # Imagem do container DBT (python:3.12-slim)
├── dbt_project.yml             # Configuração principal: paths, schemas, materialização
├── profiles.yml                # Conexão PostgreSQL via variáveis de ambiente
├── silver_to_postgres.py       # Carrega data/silver/*.parquet → pncp_db.silver
├── requirements.txt            # dbt-postgres, pandas, pyarrow, sqlalchemy, psycopg2
│
├── init-db/
│   └── 01_create_databases.sql # Cria metabase_meta + schemas silver/staging/gold
│
├── models/
│   ├── staging/
│   │   ├── sources.yml         # Declara silver.contratos com testes e descrições
│   │   ├── stg_contratos.sql   # View: cast de tipos, filtros, renomeação
│   │   └── stg_contratos.yml   # Testes genéricos: not_null, unique
│   └── marts/
│       ├── fct_contratos.sql   # Fato: métricas calculadas + FKs para dimensões
│       ├── dim_orgaos.sql      # Dimensão: órgãos contratantes distintos
│       ├── dim_fornecedores.sql# Dimensão: fornecedores com CNPJ
│       ├── dim_modalidades.sql # Dimensão: tipos de contrato
│       └── schema.yml          # Testes: not_null, unique, relationships, accepted_values
│
├── macros/
│   └── calcular_variacao_valor.sql  # 3 macros Jinja2
│
└── tests/
    ├── assert_valor_global_positivo.sql
    ├── assert_publicacao_apos_assinatura.sql
    └── assert_sem_fracionamento_extremo.sql
```

---

## 4. Infraestrutura Docker

### Containers

| Container | Imagem | Porta | Tipo | Descrição |
|---|---|---|---|---|
| `pncp_postgres` | postgres:16-alpine | 5432 | Permanente | Banco de dados (Model) |
| `pncp_dbt` | Dockerfile.dbt | — | Tarefa (encerra) | Executa DBT e encerra |
| `pncp_dbt_docs` | Dockerfile.dbt | 8080 | Sob demanda | Serve docs do DBT |
| `pncp_metabase` | metabase/metabase | 3000 | Permanente | Dashboards (View) |

### Volumes

| Volume | Conteúdo |
|---|---|
| `pncp_data` | Dados persistentes do PostgreSQL |
| `metabase_data` | Configurações e dashboards do Metabase |
| `dbt_target` | Artefatos compilados do DBT (`target/`) |

### Inicialização automática do banco (`init-db/`)

O arquivo `init-db/01_create_databases.sql` é executado pelo PostgreSQL na **primeira** inicialização do volume. Ele cria automaticamente:

- Banco `metabase_meta` — metadados internos do Metabase
- Schemas `silver`, `staging` e `gold` no `pncp_db`

> **Atenção:** se o volume `pncp_data` já existir de uma execução anterior, o script é ignorado. Para recriar tudo do zero: `docker compose down -v && docker compose up -d`

### Por que o DBT não fica "up" permanentemente?

O DBT é uma ferramenta de transformação batch — executa, gera os artefatos e encerra. O padrão correto é `docker compose run --rm dbt`, exatamente como se faz em pipelines CI/CD. Manter o container rodando em loop não agrega valor.

---

## 5. Passo a Passo para Reproduzir

### Pré-requisitos

```
Docker + Docker Compose
Python 3.10+ (para rodar silver_to_postgres.py localmente)
Parquets Silver do Lab01 em data/silver/
```

### Instalação das dependências locais

```bash
git clone https://github.com/hrvfreitas/Lab02_5479786
cd Lab02_5479786

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### `requirements.txt`

```
dbt-postgres>=1.8.0
pandas>=2.0.0
pyarrow>=14.0.0
sqlalchemy>=2.0.0
psycopg2-binary>=2.9.0
tqdm>=4.66.0
```

### Passo 1 — Preparar os dados Silver

Os Parquets devem estar em `data/silver/` dentro do Lab02. Use symlink ou cópia:

```bash
# Opção 1 — symlink (recomendado, sem duplicar dados)
ln -s ~/Lab01_PART1_5479786/data/silver ~/Lab02_5479786/data/silver

# Opção 2 — cópia direta
cp -r ~/Lab01_PART1_5479786/data/silver ~/Lab02_5479786/data/
```

### Passo 2 — Build e inicialização do banco

```bash
# Build das imagens Docker
docker compose build

# Sobe o PostgreSQL (init-db cria metabase_meta + schemas automaticamente)
docker compose up -d postgres

# Confirma que está healthy
docker compose ps
```

### Passo 3 — Carregar Silver no PostgreSQL (roda localmente)

```bash
# O script usa localhost — deve rodar FORA do Docker
python silver_to_postgres.py
```

Resultado esperado:

```
========================================================
  SILVER → PostgreSQL (Schema Atualizado)
  Diretório Origem: data/silver
  Tabela Destino: silver.contratos
========================================================

✓ Schema "silver" garantido

Carregando 55 arquivos Parquet...

Silver → PG: 100%|████████████████| 55/55 [02:30<00:00]

Criando índices de performance...

========================================================
✅ Carga concluída com sucesso!
   Total: 3,953,151 registros em silver.contratos
========================================================
```

### Passo 4 — Executar o pipeline DBT

```bash
# Pipeline completo: dbt debug → dbt run → dbt test → dbt docs generate
docker compose run --rm dbt
```

Resultado esperado de `dbt run`:

```
1 of 5 START sql view model staging.stg_contratos ......... [OK]
2 of 5 START sql table model gold.dim_orgaos .............. [OK]
3 of 5 START sql table model gold.dim_fornecedores ........ [OK]
4 of 5 START sql table model gold.dim_modalidades ......... [OK]
5 of 5 START sql table model gold.fct_contratos ........... [OK]

Finished running 5 models. Completed successfully.
```

Resultado esperado de `dbt test`:

```
PASS assert_valor_global_positivo .......................... [PASS]
PASS assert_publicacao_apos_assinatura ..................... [PASS]
PASS assert_sem_fracionamento_extremo ...................... [PASS]
PASS not_null_fct_contratos_id_contrato ................... [PASS]
PASS unique_fct_contratos_id_contrato_pncp ................ [PASS]
PASS relationships_fct_orgao_entidade_id__dim_orgaos ...... [PASS]
PASS relationships_fct_id_modalidade__dim_modalidades ..... [PASS]
... (demais testes genéricos)

Finished running 14 tests. Completed successfully.
```

### Passo 5 — Subir Metabase e documentação

```bash
# Sobe o Metabase
docker compose up -d metabase

# Sobe o servidor de documentação DBT
docker compose up dbt-docs
```

### Comandos úteis adicionais

```bash
# Rodar só os modelos (sem teste e docs)
docker compose run --rm dbt bash -c "dbt run"

# Rodar só os testes
docker compose run --rm dbt bash -c "dbt test"

# Rodar model específico
docker compose run --rm dbt bash -c "dbt run --select fct_contratos"

# Ver SQL compilado de um model
docker compose run --rm dbt bash -c "dbt compile --select fct_contratos"

# Recriar tudo do zero (apaga volumes)
docker compose down -v
docker compose up -d postgres
python silver_to_postgres.py
docker compose run --rm dbt
```

### Acessos

| Serviço | URL | Credenciais |
|---|---|---|
| Metabase | http://localhost:3000 | Configurado no primeiro acesso |
| DBT Docs | http://localhost:8080 | — |
| PostgreSQL | localhost:5432 | postgres / postgres / pncp_db |

---

## 6. Modelos DBT

### Staging — `stg_contratos`

**Materialização:** `view` · **Schema:** `staging`  
**Fonte:** `{{ source('silver', 'contratos') }}`

Responsabilidades:
- Cast explícito de tipos: `::numeric(18,2)` para valores, `::date` para datas
- Renomeação padronizada: `orgao_entidade_id → orgao_cnpj`, `cnpj_contratada → fornecedor_cnpj`
- Filtro de registros inválidos: `valor_global > 0 AND id IS NOT NULL`

### Marts — schema `gold`

| Model | Materialização | Registros aprox. | Descrição |
|---|---|---|---|
| `fct_contratos` | table | ~3,9M | Tabela fato central |
| `dim_orgaos` | table | ~13K | Órgãos contratantes distintos |
| `dim_fornecedores` | table | ~370K | Fornecedores PJ com CNPJ |
| `dim_modalidades` | table | ~12 | Tipos de contrato |

### Campos calculados em `fct_contratos`

| Campo | Origem | Descrição |
|---|---|---|
| `id_contrato` | `row_number()` | Chave surrogate gerada pelo DBT |
| `variacao_valor_pct` | macro `calcular_variacao_valor` | % de variação global vs inicial |
| `delay_publicacao_dias` | `data_publicacao - data_assinatura` | Dias entre assinatura e publicação |
| `contrato_ativo` | `data_vigencia_fim > current_date` | Booleano de vigência |
| `data_carga_dbt` | `current_timestamp` | Timestamp da última execução |

---

## 7. Macros

Definidas em `macros/calcular_variacao_valor.sql`:

### `calcular_variacao_valor(valor_global, valor_inicial)`

Calcula a variação percentual entre os dois valores. Retorna `NULL` quando `valor_inicial` é nulo ou zero (evita divisão por zero). Valores positivos indicam aditivos.

```sql
-- Uso em fct_contratos.sql:
{{ calcular_variacao_valor('s.valor_global', 's.valor_inicial') }} as variacao_valor_pct
```

### `format_brl(coluna)`

Formata número como string em reais: `R$ 1.234.567,89`.

```sql
{{ format_brl('valor_global') }}
-- → 'R$ 533.300,37'
```

### `categoria_risco_valor(coluna)`

Classifica o contrato em 6 faixas de valor:

| Categoria | Intervalo |
|---|---|
| Micro | < R$ 10.000 |
| Pequeno | R$ 10k – R$ 100k |
| Médio | R$ 100k – R$ 1M |
| Grande | R$ 1M – R$ 10M |
| Alto Valor | R$ 10M – R$ 100M |
| Estratégico | > R$ 100M |

---

## 8. Testes

### Testes Genéricos (YAML)

Definidos em `models/staging/sources.yml`, `stg_contratos.yml` e `marts/schema.yml`:

| Tipo | Colunas / contexto |
|---|---|
| `not_null` | `id`, `orgao_entidade_id`, `orgao_entidade_nome`, `valor_global`, `modalidade_id`, `ano_mes_coleta` |
| `unique` | `id` (source), `contrato_id` (staging), `id_contrato` e `id_contrato_pncp` (fato), PKs das dimensões |
| `relationships` | `fct.orgao_entidade_id → dim_orgaos.orgao_entidade_id` |
| `relationships` | `fct.id_modalidade → dim_modalidades.id_modalidade` |
| `accepted_values` | `fct.contrato_ativo: [true, false]` |

### Testes Singulares (SQL customizados)

| Arquivo | Regra de negócio | O teste falha quando |
|---|---|---|
| `assert_valor_global_positivo.sql` | `valor_global > 0` em toda a fato | Existem valores ≤ 0 ou nulos |
| `assert_publicacao_apos_assinatura.sql` | `data_publicacao ≥ data_assinatura` | Contrato publicado antes de assinado |
| `assert_sem_fracionamento_extremo.sql` | Mesmo fornecedor/órgão < 20 contratos/mês | Possível fracionamento de licitação |

```bash
# Rodar só testes singulares
docker compose run --rm dbt bash -c "dbt test --select test_type:singular"

# Rodar só testes genéricos
docker compose run --rm dbt bash -c "dbt test --select test_type:generic"
```

---

## 9. Documentação DBT

### Gerar e servir

```bash
# Gerar (necessário antes de servir)
docker compose run --rm dbt bash -c "dbt docs generate"

# Servir em http://localhost:8080
docker compose up dbt-docs
```

### Conteúdo da documentação

- **Catalog:** descrição de todos os models, sources e colunas com tipos de dados
- **Lineage graph:** grafo interativo de dependências entre models
- **Testes:** status de execução por model e coluna
- **SQL compilado:** query final gerada pelo DBT para cada model

> Adicionar prints da tela de documentação e do lineage após execução local.

---

## 10. Visualização — Metabase

### Configurar a fonte de dados

Na primeira execução em `http://localhost:3000`, o Metabase exibe um wizard de configuração. Adicione o banco de dados:

```
Tipo:    PostgreSQL
Host:    pncp_postgres     ← nome do container na rede Docker
Porta:   5432
Banco:   pncp_db
Schema:  gold              ← schema gerado pelo DBT
Usuário: postgres
Senha:   postgres
```

> Se acessar o Metabase de outra máquina na rede local, use o IP do servidor Ubuntu no lugar de `pncp_postgres`.

### Dashboards implementados (≥ 3 visualizações)

**Visualização 1 — Evolução Anual por Modalidade **

```sql
SELECT
    EXTRACT(YEAR FROM f.data_assinatura)           AS ano,
    COALESCE(m.nome_modalidade, 'Não informado')   AS modalidade,
    ROUND(SUM(f.valor_global) / 1e9, 2)            AS valor_bilhoes,
    COUNT(*)                                       AS qtd_contratos
FROM "gold_gold".fct_contratos f
LEFT JOIN "gold_gold".dim_modalidades m ON f.id_modalidade = m.id_modalidade
WHERE f.data_assinatura IS NOT NULL
  AND EXTRACT(YEAR FROM f.data_assinatura) BETWEEN 2021 AND 2026
GROUP BY 1, 2
ORDER BY 1 ASC, 3 DESC;
```

**Visualização 2 — Proporçao de Dados  por Categoria/Processo**

```sql
SELECT
  "gold_gold"."fct_contratos"."categoria_processo" AS "categoria_processo",
  COUNT(*) AS "count"
FROM
  "gold_gold"."fct_contratos"
GROUP BY
  "gold_gold"."fct_contratos"."categoria_processo"
ORDER BY
  "gold_gold"."fct_contratos"."categoria_processo" ASC
```

**Visualização 3 — Delay de Publicação por Ano **

```sql
SELECT
    EXTRACT(YEAR FROM data_assinatura)                              AS ano,
    ROUND(AVG(delay_publicacao_dias), 1)                            AS delay_medio_dias,
    MAX(delay_publicacao_dias)                                      AS delay_maximo,
    COUNT(*) FILTER (WHERE delay_publicacao_dias > 20)              AS publicacao_tardia,
    COUNT(*) FILTER (WHERE delay_publicacao_dias BETWEEN 0 AND 5)   AS publicacao_rapida
FROM "gold_gold".fct_contratos
WHERE delay_publicacao_dias IS NOT NULL
  AND delay_publicacao_dias >= 0
  AND EXTRACT(YEAR FROM data_assinatura) BETWEEN 2021 AND 2026
GROUP BY 1
ORDER BY 1;
```

---

## Observações Técnicas

**Por que `silver_to_postgres.py` roda localmente e não no container?**  
O script usa `localhost` como host do banco. Dentro de um container Docker, `localhost` resolve para o próprio container, não para o host ou o `pncp_postgres`. Como o script já funciona conforme está, a solução mais simples é mantê-lo rodando na máquina host, que enxerga o PostgreSQL via `localhost:5432`.

**Por que os schemas são criados no `init-db` e não pelo DBT?**  
O DBT cria schemas automaticamente ao materializar models, mas o `metabase_meta` não é gerenciado pelo DBT. O `init-db` centraliza a criação de todos os schemas na inicialização do banco, garantindo que o Metabase encontre seu banco de metadados desde o primeiro `docker compose up`.

**Atenção com o volume `pncp_data`:**  
O `init-db` só roda na **primeira** inicialização. Se o volume já existe, o script é ignorado. Para recriar tudo do zero:

```bash
docker compose down -v     # apaga todos os volumes
docker compose up -d postgres
python silver_to_postgres.py
docker compose run --rm dbt
docker compose up -d metabase
```
