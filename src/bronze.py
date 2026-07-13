"""
Camada Bronze: ingestão raw dos microdados do ENEM.
Regra de ouro: não transformar dado nenhum aqui, só preservar + auditar.
"""

import hashlib
from datetime import datetime

import numpy as np
import pandas as pd


def gerar_dados_sinteticos_enem(n: int = 50_000, ano: int = 2023) -> pd.DataFrame:
    """Gera dados sintéticos que simulam a estrutura real dos microdados do ENEM (INEP).

    Usado como fallback de desenvolvimento quando o CSV real (~1GB+) não está disponível.

    LIMITAÇÃO CONHECIDA: as notas são sorteadas de forma aleatória e independente
    de renda/tipo de escola. Isso testa o pipeline, mas não reproduz o padrão real
    de desigualdade educacional (só apareceria com os microdados reais do INEP).
    """
    np.random.seed(42 + ano)  # seed depende do ano, pra cada ano gerar dados diferentes

    ufs = ['SP', 'RJ', 'MG', 'BA', 'RS', 'PR', 'PE', 'CE', 'PA', 'SC',
           'GO', 'MA', 'PB', 'ES', 'RN', 'AL', 'PI', 'MT', 'MS', 'DF',
           'SE', 'RO', 'TO', 'AC', 'AM', 'AP', 'RR']

    tipo_escola = np.random.choice([1, 2, 3], size=n, p=[0.15, 0.75, 0.10])
    # 1 = Federal, 2 = Estadual/Municipal (pública), 3 = Privada

    q006 = np.random.choice(list('ABCDEFGHIJKLMNOPQ'), size=n)
    # Q006 = renda familiar, de "Nenhuma renda" (A) até "Mais de 20 salários" (Q)

    df = pd.DataFrame({
        'NU_INSCRICAO': [f'{ano}{i:012d}' for i in range(n)],
        'NU_ANO': ano,
        'SG_UF_PROVA': np.random.choice(ufs, size=n),
        'TP_ESCOLA': tipo_escola,
        'Q006': q006,
        'NU_NOTA_CN': np.round(np.random.normal(520, 90, n).clip(0, 1000), 1),
        'NU_NOTA_CH': np.round(np.random.normal(540, 85, n).clip(0, 1000), 1),
        'NU_NOTA_LC': np.round(np.random.normal(530, 80, n).clip(0, 1000), 1),
        'NU_NOTA_MT': np.round(np.random.normal(510, 100, n).clip(0, 1000), 1),
        'NU_NOTA_REDACAO': np.round(np.random.normal(600, 150, n).clip(0, 1000), 1),
        'TP_STATUS_REDACAO': np.random.choice([1, 2, 3, 4], size=n, p=[0.9, 0.05, 0.03, 0.02]),
    })
    return df


def ingestao_bronze(df: pd.DataFrame, fonte: str = 'sintetico') -> pd.DataFrame:
    """Camada Bronze: preserva os dados como estão (tudo string) + colunas de auditoria."""
    df_bronze = df.copy()

    for col in df_bronze.columns:
        df_bronze[col] = df_bronze[col].astype(str)

    df_bronze['_source'] = fonte
    df_bronze['_ingested_at'] = datetime.now().isoformat()

    conteudo = pd.util.hash_pandas_object(df, index=False).values.tobytes()
    df_bronze['_file_hash'] = hashlib.md5(conteudo).hexdigest()

    return df_bronze


def salvar_bronze(df_bronze: pd.DataFrame, base_path: str, ano: int) -> str:
    """Salva a camada Bronze em Parquet, particionado por ano."""
    import os
    caminho = f'{base_path}/data/bronze/ano={ano}'
    os.makedirs(caminho, exist_ok=True)
    df_bronze.to_parquet(f'{caminho}/bronze.parquet', index=False)
    return caminho
