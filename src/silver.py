"""
Camada Silver: limpeza, tipagem e enriquecimento dos dados vindos da Bronze.
"""

import os

import pandas as pd

# Dicionário oficial do INEP para Q006 (renda familiar mensal)
DICT_Q006 = {
    'A': 'Nenhuma renda',
    'B': 'Até R$ 1.320,00',
    'C': 'De R$ 1.320,01 até R$ 1.980,00',
    'D': 'De R$ 1.980,01 até R$ 2.640,00',
    'E': 'De R$ 2.640,01 até R$ 3.300,00',
    'F': 'De R$ 3.300,01 até R$ 3.960,00',
    'G': 'De R$ 3.960,01 até R$ 5.280,00',
    'H': 'De R$ 5.280,01 até R$ 6.600,00',
    'I': 'De R$ 6.600,01 até R$ 7.920,00',
    'J': 'De R$ 7.920,01 até R$ 9.240,00',
    'K': 'De R$ 9.240,01 até R$ 10.560,00',
    'L': 'De R$ 10.560,01 até R$ 11.880,00',
    'M': 'De R$ 11.880,01 até R$ 13.200,00',
    'N': 'De R$ 13.200,01 até R$ 15.840,00',
    'O': 'De R$ 15.840,01 até R$ 19.800,00',
    'P': 'De R$ 19.800,01 até R$ 26.400,00',
    'Q': 'Mais de R$ 26.400,00',
}

RENOMEAR_COLUNAS = {
    'NU_INSCRICAO': 'numero_inscricao',
    'NU_ANO': 'ano',
    'SG_UF_PROVA': 'uf',
    'TP_ESCOLA': 'tipo_escola_codigo',
    'Q006': 'renda_codigo',
    'NU_NOTA_CN': 'nota_ciencias_natureza',
    'NU_NOTA_CH': 'nota_ciencias_humanas',
    'NU_NOTA_LC': 'nota_linguagens',
    'NU_NOTA_MT': 'nota_matematica',
    'NU_NOTA_REDACAO': 'nota_redacao',
    'TP_STATUS_REDACAO': 'status_redacao_codigo',
}

REGIAO_POR_UF = {
    'AC': 'Norte', 'AP': 'Norte', 'AM': 'Norte', 'PA': 'Norte', 'RO': 'Norte', 'RR': 'Norte', 'TO': 'Norte',
    'AL': 'Nordeste', 'BA': 'Nordeste', 'CE': 'Nordeste', 'MA': 'Nordeste', 'PB': 'Nordeste',
    'PE': 'Nordeste', 'PI': 'Nordeste', 'RN': 'Nordeste', 'SE': 'Nordeste',
    'DF': 'Centro-Oeste', 'GO': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'MS': 'Centro-Oeste',
    'ES': 'Sudeste', 'MG': 'Sudeste', 'RJ': 'Sudeste', 'SP': 'Sudeste',
    'PR': 'Sul', 'RS': 'Sul', 'SC': 'Sul',
}


def classificar_faixa_renda(letra_q006: str) -> str:
    """Agrupa as 17 faixas granulares do Q006 em 5 faixas amplas, mais legíveis para análise."""
    if letra_q006 in ['A', 'B', 'C']:
        return 'Baixa renda'
    elif letra_q006 in ['D', 'E', 'F', 'G']:
        return 'Média-baixa renda'
    elif letra_q006 in ['H', 'I', 'J', 'K']:
        return 'Média renda'
    elif letra_q006 in ['L', 'M', 'N']:
        return 'Média-alta renda'
    elif letra_q006 in ['O', 'P', 'Q']:
        return 'Alta renda'
    else:
        return 'Não informado'


def transformar_silver(df_bronze: pd.DataFrame) -> pd.DataFrame:
    """Camada Silver: limpa, tipa e enriquece os dados vindos da Bronze."""
    df = df_bronze.copy()

    df = df.rename(columns=RENOMEAR_COLUNAS)

    colunas_notas = ['nota_ciencias_natureza', 'nota_ciencias_humanas',
                      'nota_linguagens', 'nota_matematica', 'nota_redacao']
    for col in colunas_notas:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df['ano'] = pd.to_numeric(df['ano'], errors='coerce').astype('Int64')
    df['tipo_escola_codigo'] = pd.to_numeric(df['tipo_escola_codigo'], errors='coerce').astype('Int64')
    df['status_redacao_codigo'] = pd.to_numeric(df['status_redacao_codigo'], errors='coerce').astype('Int64')

    linhas_antes = len(df)

    # Filtra registros inválidos: notas nulas ou redação eliminada/ausente (status != 1)
    df = df.dropna(subset=colunas_notas)
    df = df[df['status_redacao_codigo'] == 1]
    df = df.drop_duplicates(subset=['numero_inscricao'])

    linhas_depois = len(df)
    pct_retido = round(100 * linhas_depois / linhas_antes, 1) if linhas_antes else 0
    print(f"Silver: {linhas_antes} -> {linhas_depois} linhas ({pct_retido}% retido)")

    df['faixa_renda'] = df['renda_codigo'].apply(classificar_faixa_renda)
    df['renda_descricao'] = df['renda_codigo'].map(DICT_Q006)

    # TP_ESCOLA no dado real do INEP: 1 = Não respondeu, 2 = Pública, 3 = Privada
    # (diferente da suposição inicial no gerador sintético, que usava 1=Federal)
    df['tipo_escola'] = df['tipo_escola_codigo'].map({1: 'Não informado', 2: 'Pública', 3: 'Privada'})

    df['regiao'] = df['uf'].map(REGIAO_POR_UF)

    return df


def salvar_silver(df_silver: pd.DataFrame, base_path: str, ano: int) -> str:
    """Salva a camada Silver em Parquet, particionado por ano."""
    caminho = f'{base_path}/data/silver/ano={ano}'
    os.makedirs(caminho, exist_ok=True)
    df_silver.to_parquet(f'{caminho}/silver.parquet', index=False)
    return caminho
