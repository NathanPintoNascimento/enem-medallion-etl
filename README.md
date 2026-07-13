# Pipeline ETL Medallion — Desigualdade Educacional no ENEM

Pipeline de Engenharia de Dados utilizando arquitetura **Medallion (Bronze → Silver → Gold)** para processar e analisar aproximadamente **10,8 milhões de registros** dos microdados do ENEM (2021–2023), investigando desigualdades de desempenho entre candidatos por **renda familiar**, **tipo de escola** e **região do Brasil**.

## Resumo

**Objetivo:** construir um pipeline ETL completo seguindo a arquitetura Medallion, transformando os microdados brutos do ENEM em tabelas analíticas prontas para consultas SQL e visualizações.

**Stack**

* Python
* Pandas
* Parquet
* DuckDB
* Plotly
* Google Colab

**Resultado**

* Pipeline organizado em camadas Bronze, Silver e Gold
* Tratamento e padronização dos microdados do INEP
* Três tabelas analíticas em Parquet
* Dashboard interativo
* Consultas SQL utilizando DuckDB

---

## Principais resultados

A análise dos microdados reais do ENEM (2021–2023) revelou diferenças significativas de desempenho entre grupos socioeconômicos.

* **Gap de renda:** candidatos de maior renda obtiveram, em média, **123,3 pontos** a mais na nota geral do que candidatos de menor renda (**635,6 vs. 512,3**).
* **Gap entre escolas públicas e privadas:** em alguns estados, como Piauí e Ceará, a diferença média chegou a **162 pontos**.
* **Qualidade dos dados:** após os filtros aplicados na camada Silver, aproximadamente **65%** dos registros permaneceram válidos para análise, percentual compatível com a taxa histórica de candidatos presentes no ENEM após exclusão de ausentes e registros sem nota.

---

## Arquitetura

```
                    ┌──────────────┐
  CSVs do INEP      │    Bronze    │
──────────────────► │              │
                    │ Dados brutos │
                    │ Auditoria    │
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │    Silver    │
                    │              │
                    │ Tipagem      │
                    │ Limpeza      │
                    │ Enriquecimento
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │     Gold     │
                    │              │
                    │ Agregações   │
                    │ Métricas     │
                    └──────┬───────┘
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
       DuckDB         Dashboard         GitHub
```

---

## Fluxo do pipeline

### Bronze

Responsável pela ingestão dos arquivos CSV do INEP preservando os dados exatamente como foram recebidos.

Cada registro recebe metadados de auditoria:

* `_source`
* `_ingested_at`
* `_file_hash`

Todos os campos são carregados inicialmente como **string**, evitando perda de informação durante a ingestão.

---

### Silver

Camada responsável pelo tratamento dos dados.

Principais transformações:

* conversão de tipos
* remoção de registros inválidos
* padronização de colunas
* criação da nota média
* classificação por faixa de renda
* classificação do tipo de escola
* identificação da região do país

Também é nesta etapa que são aplicadas as regras de qualidade utilizadas pelo restante do pipeline.

---

### Gold

Produz tabelas analíticas prontas para consumo.

Entre elas:

* nota média por faixa de renda
* comparação entre escolas públicas e privadas por UF
* indicadores regionais de desempenho

Essas tabelas são armazenadas em Parquet e consultadas diretamente pelo DuckDB.

---

## Desenvolvimento do projeto

O pipeline foi desenvolvido em duas etapas.

### 1. Validação com dados sintéticos

Antes da utilização dos microdados reais, foi criado um gerador de dados sintéticos (`gerar_dados_sinteticos_enem()`) reproduzindo a estrutura dos arquivos do INEP.

Essa etapa permitiu validar toda a arquitetura Bronze → Silver → Gold, testar regras de transformação e verificar as agregações sem depender de arquivos com vários gigabytes.

### 2. Migração para dados reais

Após a validação, o pipeline foi executado utilizando os microdados oficiais do ENEM (2021–2023).

Como esperado em projetos de dados reais, surgiram diferenças de schema e características não previstas durante a fase sintética, como novas categorias em `TP_ESCOLA` e uma taxa de retenção diferente da inicialmente estimada.

Essas diferenças foram tratadas sem necessidade de alterar a lógica principal do pipeline.

---

## Tecnologias utilizadas

| Tecnologia   | Finalidade                             |
| ------------ | -------------------------------------- |
| Python       | Implementação do pipeline              |
| Pandas       | Transformações e manipulação dos dados |
| Parquet      | Persistência das camadas               |
| DuckDB       | Consultas analíticas SQL               |
| Plotly       | Dashboard interativo                   |
| Google Colab | Ambiente de desenvolvimento            |

O projeto foi desenvolvido integralmente no Google Colab.

A arquitetura foi mantida desacoplada das ferramentas específicas, permitindo substituir componentes como Pandas por PySpark ou incorporar Airflow em uma futura evolução sem alterar a estrutura Medallion.

---

## Estrutura do repositório

```text
enem-medallion-etl/
├── src/
├── data/
│   └── gold/
├── dashboard_enem.html
├── README.md
└── LICENSE
```

Apenas a camada **Gold** é versionada no Git.

As camadas Raw, Bronze e Silver podem ser regeneradas a qualquer momento a partir dos arquivos oficiais do INEP, evitando versionar aproximadamente **4,3 GB** de dados brutos.

---

## Dependências

```bash
pip install duckdb plotly
```

O ambiente padrão do Google Colab já inclui bibliotecas como `pandas`, `os`, `hashlib` e `datetime`.

---

## Como executar

1. Baixe os microdados do ENEM diretamente no portal de Dados Abertos do INEP.
2. Abra o notebook no Google Colab.
3. Monte o Google Drive.
4. Extraia os arquivos `.zip`.
5. Configure o dicionário `CAMINHO_DADOS_REAIS`.
6. Execute as etapas Bronze → Silver → Gold.
7. Rode as consultas SQL utilizando DuckDB.
8. Gere o dashboard em Plotly.

---

## Consultas SQL

### Ranking dos estados por diferença entre escolas públicas e privadas

```sql
WITH base AS (
    SELECT *
    FROM read_parquet('data/gold/gold_publica_vs_privada_uf.parquet')
)

SELECT
    uf,
    regiao,
    nota_media_publica,
    nota_media_privada,
    gap_privada_publica,

    RANK() OVER (
        ORDER BY gap_privada_publica DESC
    ) ranking_nacional,

    RANK() OVER (
        PARTITION BY regiao
        ORDER BY gap_privada_publica DESC
    ) ranking_regional,

    ROUND(
        AVG(gap_privada_publica)
        OVER (PARTITION BY regiao),
        2
    ) gap_medio_regiao

FROM base

ORDER BY ranking_nacional;
```

### Desempenho por faixa de renda

```sql
SELECT
    faixa_renda,
    nota_media_geral,

    RANK() OVER (
        ORDER BY nota_media_geral DESC
    ) ranking_desempenho,

    ROUND(
        nota_media_geral -
        LAG(nota_media_geral)
        OVER (ORDER BY ordem_renda),
        2
    ) variacao_vs_faixa_anterior,

    ROUND(
        nota_media_geral -
        FIRST_VALUE(nota_media_geral)
        OVER (ORDER BY ordem_renda),
        2
    ) gap_vs_baixa_renda

FROM read_parquet(
'data/gold/gold_nota_media_por_renda.parquet'
);
```

---

## Limitações

* O projeto mede **correlações**, não relações de causalidade.
* Não utiliza dados do Censo Escolar para enriquecer as análises.
* A categoria **"Não informado"** em `TP_ESCOLA` foi mantida separadamente e excluída dos cálculos de comparação entre escolas públicas e privadas.
* Os critérios de qualidade dos dados ainda não são executados como um gate automatizado entre as camadas.

---

## Próximas melhorias

* Implementar validações automáticas de qualidade entre Bronze, Silver e Gold.
* Adicionar testes automatizados.
* Automatizar a execução do pipeline.
* Integrar dados do Censo Escolar.
* Formalizar indicadores de qualidade e monitoramento do pipeline.

---

## Licença

Este projeto está licenciado sob a licença **MIT**.
