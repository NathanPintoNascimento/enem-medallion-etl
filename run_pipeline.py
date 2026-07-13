"""
Orquestração sequencial do pipeline Medallion ENEM.

Substitui o Airflow (não instalável no ambiente atual por restrição de disco/infra).
Mesma lógica de um DAG: start -> bronze -> silver -> gold -> end, só que executada
localmente em sequência, com logs de progresso e interrupção em caso de erro.
"""

import sys

from src.bronze import gerar_dados_sinteticos_enem, ingestao_bronze, salvar_bronze
from src.silver import transformar_silver, salvar_silver
from src.gold import (
    gold_nota_media_por_renda,
    gold_publica_vs_privada_uf,
    gold_evolucao_gap_anual,
    salvar_gold,
)

import pandas as pd


def run_pipeline(base_path: str, anos: list[int] = [2021, 2022, 2023], n_linhas: int = 50_000):
    print("=" * 60)
    print("INICIANDO PIPELINE ENEM - MEDALLION ARCHITECTURE")
    print("=" * 60)

    dfs_silver_por_ano = {}

    # --- BRONZE + SILVER, por ano ---
    for ano in anos:
        print(f"\n[BRONZE] Processando ano {ano}...")
        df_raw = gerar_dados_sinteticos_enem(n=n_linhas, ano=ano)
        df_bronze = ingestao_bronze(df_raw, fonte='sintetico_dev')
        caminho_bronze = salvar_bronze(df_bronze, base_path, ano)
        print(f"[BRONZE] OK -> {caminho_bronze} ({len(df_bronze)} linhas)")

        print(f"[SILVER] Processando ano {ano}...")
        df_silver = transformar_silver(df_bronze)
        caminho_silver = salvar_silver(df_silver, base_path, ano)
        print(f"[SILVER] OK -> {caminho_silver} ({len(df_silver)} linhas)")

        dfs_silver_por_ano[ano] = df_silver

    df_silver_todos_anos = pd.concat(dfs_silver_por_ano.values(), ignore_index=True)
    print(f"\n[SILVER] Consolidado: {len(df_silver_todos_anos)} linhas, anos: {sorted(dfs_silver_por_ano.keys())}")

    # --- GOLD ---
    print("\n[GOLD] Gerando tabelas analíticas...")

    gold_renda = gold_nota_media_por_renda(df_silver_todos_anos)
    salvar_gold(gold_renda, base_path, 'gold_nota_media_por_renda')
    print(f"[GOLD] gold_nota_media_por_renda OK ({len(gold_renda)} linhas)")

    gold_uf = gold_publica_vs_privada_uf(df_silver_todos_anos)
    salvar_gold(gold_uf, base_path, 'gold_publica_vs_privada_uf')
    print(f"[GOLD] gold_publica_vs_privada_uf OK ({len(gold_uf)} linhas)")

    gold_evolucao = gold_evolucao_gap_anual(df_silver_todos_anos)
    salvar_gold(gold_evolucao, base_path, 'gold_evolucao_gap_anual')
    print(f"[GOLD] gold_evolucao_gap_anual OK ({len(gold_evolucao)} linhas)")

    print("\n" + "=" * 60)
    print("PIPELINE CONCLUÍDO COM SUCESSO")
    print("=" * 60)

    return {
        'silver_consolidado': df_silver_todos_anos,
        'gold_renda': gold_renda,
        'gold_uf': gold_uf,
        'gold_evolucao': gold_evolucao,
    }


if __name__ == '__main__':
    BASE_PATH = sys.argv[1] if len(sys.argv) > 1 else '.'
    run_pipeline(base_path=BASE_PATH)
