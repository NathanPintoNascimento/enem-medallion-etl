# Pipeline ETL Medallion — Desigualdade Educacional no ENEM

Pipeline de dados usando arquitetura **Medallion (Bronze → Silver → Gold)** para analisar desigualdade de desempenho no ENEM (Exame Nacional do Ensino Médio) a partir dos **microdados reais do INEP** (2021-2023), cruzando renda familiar, tipo de escola (pública/privada) e região do Brasil.

## Principais achados

Com dados reais de ~10,8 milhões de inscritos (2021-2023):

- **Gap de renda**: candidatos de alta renda tiram, em média, **~123 pontos a mais** na nota geral do que candidatos de baixa renda (635,6 vs. 512,3)
- **Gap escola pública/privada**: chega a **até 162 pontos** de diferença nos estados com maior disparidade (ex: Piauí, Ceará)
- **Retenção real vs. esperada**: ~65% dos registros passaram no filtro de qualidade (nota preenchida), consistente com a taxa histórica de abstenção do ENEM (25-35%) — não é perda de qualidade do pipeline, é abstenção real de candidatos

## A jornada: sintético → real

O pipeline foi desenvolvido em duas etapas deliberadas:

1. **Prova de conceito com dados sintéticos**: um gerador (`gerar_dados_sinteticos_enem()`) criou dados simulando a estrutura dos microdados do INEP, com notas aleatórias e independentes de renda/escola. Isso permitiu validar toda a lógica do pipeline (Bronze → Silver → Gold, filtros, agregações) sem depender de arquivos de vários GB, e sem risco de erro de schema atrasar o desenvolvimento.
2. **Migração para dados reais do INEP**: com o pipeline validado, o mesmo código (sem alterar a lógica de transformação) foi apontado para os microdados reais de 2021-2023. Essa etapa expôs diferenças reais de schema (ex: categoria adicional em `TP_ESCOLA`, taxa de retenção diferente da esperada) — tratadas e documentadas na seção de Limitações abaixo.

Essa abordagem (validar com dado sintético antes de gastar tempo/recursos com dado real e pesado) é uma prática comum em engenharia de dados para reduzir o custo de iteração.

## Arquitetura

```
                    ┌──────────────┐
  INEP (CSV bruto)  │              │
  ────────────────► │   BRONZE     │  Tudo como string + auditoria
                    │              │  (_source, _ingested_at, _file_hash)
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │   SILVER     │  Tipagem correta, filtros de
                    │              │  qualidade, colunas derivadas
                    └──────┬───────┘  (faixa_renda, tipo_escola, regiao)
                           │
                    ┌──────▼───────┐
                    │    GOLD      │  3 tabelas analíticas prontas
                    │              │  para consumo
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼             ▼
           DuckDB      Dashboard      GitHub
           (SQL)      (HTML/Plotly)  (versionado)
```

## Stack técnico

| Camada original (plano) | Substituído por | Motivo |
|---|---|---|
| PySpark | pandas | Sem espaço em disco para instalar localmente; ambiente 100% Google Colab |
| Delta Lake | Parquet | Mais simples, sem overhead de versionamento transacional para o escopo do projeto |
| Airflow | Notebook sequencial | Sem Docker/instalação pesada disponível |
| PostgreSQL (Docker local) | **DuckDB** | Testamos Supabase (Postgres gerenciado) primeiro, mas o Colab não tinha rota de saída IPv6 (exigida pela conexão direta gratuita do Supabase). DuckDB roda in-process, sem servidor, direto sobre os arquivos Parquet — elimina a dependência de rede |
| Docker | — | Não utilizado; todo o ambiente roda em Google Colab + Google Drive |

## Dependências

```bash
pip install duckdb plotly
```

(pandas, os, hashlib e datetime já vêm no ambiente padrão do Google Colab)

## Estrutura do repositório

```
enem-medallion-etl/
├── src/                     # funções do pipeline (ingestão, transformação, agregação)
├── data/
│   └── gold/                 # tabelas finais (únicas versionadas no Git)
├── dashboard_enem.html       # dashboard interativo (Plotly) — abrir direto no navegador
├── README.md
└── LICENSE
```

**Nota sobre dados**: apenas a camada **Gold** é versionada no Git. As camadas Raw, Bronze e Silver não são versionadas — são grandes (os CSVs brutos do INEP somam ~4,3GB para 3 anos) e ficam apenas no Google Drive do autor, sendo regeneráveis a partir do pipeline.

## Como rodar

1. Baixe os microdados do ENEM diretamente do [INEP](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enem) para os anos desejados (arquivo `.zip`, contém uma pasta `DADOS/` com o CSV principal)
2. Abra o notebook no Google Colab e monte seu Google Drive (`from google.colab import drive; drive.mount('/content/drive')`)
3. Extraia os `.zip` do INEP para o disco local da sessão (`/content/`) — não é necessário mover os CSVs brutos para o Drive
4. Ajuste o dicionário `CAMINHO_DADOS_REAIS` no notebook com o caminho de cada CSV extraído
5. Execute as células em sequência: leitura (`ler_csv_real_enem`) → Bronze (`ingestao_bronze`) → Silver (`transformar_silver`) → Gold (3 funções de agregação)
6. Rode as queries SQL de exemplo abaixo via DuckDB, direto sobre os arquivos Parquet da Gold
7. Gere o dashboard interativo com a célula de visualização (Plotly) — o resultado é `dashboard_enem.html`, abra direto no navegador

## Exemplos de queries SQL (via DuckDB, direto sobre o Parquet)

```sql
-- Ranking de UFs por gap escola privada vs pública, com posição regional
WITH base AS (
    SELECT * FROM read_parquet('data/gold/gold_publica_vs_privada_uf.parquet')
)
SELECT
    uf, regiao, nota_media_publica, nota_media_privada, gap_privada_publica,
    RANK() OVER (ORDER BY gap_privada_publica DESC) AS ranking_nacional,
    RANK() OVER (PARTITION BY regiao ORDER BY gap_privada_publica DESC) AS ranking_regional,
    ROUND(AVG(gap_privada_publica) OVER (PARTITION BY regiao), 2) AS gap_medio_regiao
FROM base
ORDER BY ranking_nacional;
```

```sql
-- Nota por faixa de renda, com gap acumulado em relação à faixa mais baixa
-- (ordem_renda é uma coluna auxiliar 1-5, necessária porque a ordenação
-- alfabética de "faixa_renda" não reflete a ordem lógica de renda)
SELECT
    faixa_renda,
    nota_media_geral,
    RANK() OVER (ORDER BY nota_media_geral DESC) AS ranking_desempenho,
    ROUND(nota_media_geral - LAG(nota_media_geral) OVER (ORDER BY ordem_renda), 2) AS variacao_vs_faixa_anterior,
    ROUND(nota_media_geral - FIRST_VALUE(nota_media_geral) OVER (ORDER BY ordem_renda), 2) AS gap_vs_baixa_renda
FROM read_parquet('data/gold/gold_nota_media_por_renda.parquet');
```

## Limitações conhecidas

- **Correlação, não causalidade**: o pipeline mede associação entre renda/tipo de escola e desempenho — não isola outros fatores (ex: infraestrutura escolar, acesso à internet, formação docente)
- **Não foi cruzado com o Censo Escolar do INEP** — uma análise mais completa incluiria dados de infraestrutura e corpo docente por escola
- **Critério de qualidade de dado recalibrado**: o threshold original de ≥70% de retenção foi definido com dados sintéticos e não se sustentou com dado real (~65% observado). Isso reflete a taxa real de abstenção do ENEM, não um problema do pipeline
- **Categoria "Não informado" em tipo de escola**: os microdados reais têm uma categoria de `TP_ESCOLA` que não existe no gerador sintético usado nos testes iniciais; tratada como categoria à parte, excluída do cálculo de gap público/privado
- **Data Quality Check automatizado**: ainda não implementado como gate formal entre camadas

## Próximos passos

- [ ] Implementar data quality checks formais entre camadas (validação de volume, schema e regras de negócio antes de cada transição de camada)
- [ ] Adicionar testes automatizados (pytest ou validações inline)
- [ ] Formalizar orquestração sequencial (hoje é execução manual célula a célula)
- [ ] Cruzar com dados do Censo Escolar para enriquecer a análise
- [ ] Recalibrar ou documentar formalmente o novo threshold de retenção (~65%) como padrão do projeto

## Licença

MIT — veja [LICENSE](LICENSE) para detalhes.
