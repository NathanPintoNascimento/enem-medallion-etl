"""
Camada Gold: agregações analíticas para medir desigualdade educacional no ENEM.
"""

import os

import pandas as pd


def gold_nota_media_por_renda(df_silver: pd.DataFrame) -> pd.DataFrame:
    """Gold 1: nota média por área (e geral), agrupado por faixa de renda."""
    ordem_faixas = ['Baixa renda', 'Média-baixa renda', 'Média renda',
                     'Média-alta renda', 'Alta renda']

    agg = df_silver.groupby('faixa_renda').agg(
        qtd_participantes=('numero_inscricao', 'count'),
        nota_media_ciencias_natureza=('nota_ciencias_natureza', 'mean'),
        nota_media_ciencias_humanas=('nota_ciencias_humanas', 'mean'),
        nota_media_linguagens=('nota_linguagens', 'mean'),
        nota_media_matematica=('nota_matematica', 'mean'),
        nota_media_redacao=('nota_redacao', 'mean'),
    ).reset_index()

    colunas_notas = [c for c in agg.columns if c.startswith('nota_media_')]
    agg['nota_media_geral'] = agg[colunas_notas].mean(axis=1)

    agg['faixa_renda'] = pd.Categorical(agg['faixa_renda'], categories=ordem_faixas, ordered=True)
    agg = agg.sort_values('faixa_renda').reset_index(drop=True)

    for col in colunas_notas + ['nota_media_geral']:
        agg[col] = agg[col].round(1)

    return agg


def gold_publica_vs_privada_uf(df_silver: pd.DataFrame) -> pd.DataFrame:
    """Gold 2: comparação pública x privada por UF, com gap de desempenho."""
    agg = df_silver.groupby(['uf', 'regiao', 'tipo_escola']).agg(
        qtd_participantes=('numero_inscricao', 'count'),
        nota_media_matematica=('nota_matematica', 'mean'),
        nota_media_redacao=('nota_redacao', 'mean'),
    ).reset_index()

    colunas_notas = ['nota_media_matematica', 'nota_media_redacao']
    agg['nota_media_geral'] = agg[colunas_notas].mean(axis=1)
    for col in colunas_notas + ['nota_media_geral']:
        agg[col] = agg[col].round(1)

    pivot = agg.pivot_table(
        index=['uf', 'regiao'],
        columns='tipo_escola',
        values='nota_media_geral'
    ).reset_index()

    pivot.columns.name = None
    pivot = pivot.rename(columns={'Pública': 'nota_media_publica', 'Privada': 'nota_media_privada'})
    pivot['gap_privada_publica'] = (pivot['nota_media_privada'] - pivot['nota_media_publica']).round(1)
    pivot = pivot.sort_values('gap_privada_publica', ascending=False).reset_index(drop=True)

    return pivot


def gold_evolucao_gap_anual(df_silver_todos_anos: pd.DataFrame) -> pd.DataFrame:
    """Gold 3: evolução do gap de desempenho (escola e renda) ao longo dos anos."""
    resultados = []

    for ano_iter, grupo_ano in df_silver_todos_anos.groupby('ano'):
        media_publica = grupo_ano.loc[grupo_ano['tipo_escola'] == 'Pública', 'nota_matematica'].mean()
        media_privada = grupo_ano.loc[grupo_ano['tipo_escola'] == 'Privada', 'nota_matematica'].mean()
        media_renda_alta = grupo_ano.loc[grupo_ano['faixa_renda'] == 'Alta renda', 'nota_matematica'].mean()
        media_renda_baixa = grupo_ano.loc[grupo_ano['faixa_renda'] == 'Baixa renda', 'nota_matematica'].mean()

        resultados.append({
            'ano': ano_iter,
            'qtd_participantes': len(grupo_ano),
            'nota_media_publica': round(media_publica, 1),
            'nota_media_privada': round(media_privada, 1),
            'gap_escola_privada_publica': round(media_privada - media_publica, 1),
            'nota_media_renda_baixa': round(media_renda_baixa, 1),
            'nota_media_renda_alta': round(media_renda_alta, 1),
            'gap_renda_alta_baixa': round(media_renda_alta - media_renda_baixa, 1),
        })

    return pd.DataFrame(resultados).sort_values('ano').reset_index(drop=True)


def salvar_gold(df: pd.DataFrame, base_path: str, nome_tabela: str) -> str:
    """Salva uma tabela Gold em Parquet (sem particionamento — tabelas pequenas, prontas pra consumo)."""
    caminho = f'{base_path}/data/gold'
    os.makedirs(caminho, exist_ok=True)
    caminho_arquivo = f'{caminho}/{nome_tabela}.parquet'
    df.to_parquet(caminho_arquivo, index=False)
    return caminho_arquivo
