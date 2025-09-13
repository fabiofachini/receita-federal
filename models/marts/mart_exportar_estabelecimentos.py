# models/marts/mart_exportar_estabelecimentos.py
import os

def model(dbt, session):
    dbt.config(materialized="table")

    out_dir = "data"
    os.makedirs(out_dir, exist_ok=True)
    parquet_path = os.path.join(out_dir, "dados_estabelecimentos_brasil.parquet")
    parquet_abs = os.path.abspath(parquet_path)

    qualified = 'main.mart_estabelecimentos_completo_sem_mei'

    # Se já existir, apaga para permitir overwrite
    if os.path.exists(parquet_abs):
        os.remove(parquet_abs)

    # (opcional) deixa o output do DuckDB mais limpo
    session.execute("PRAGMA enable_progress_bar=false;")

    # COPY direto da tabela para Parquet (sem opções não suportadas)
    session.execute(f"""
        COPY {qualified}
        TO '{parquet_abs}'
        (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    print(f"Arquivo gerado: {parquet_abs}")
    return session.sql("SELECT 1 AS ok")
