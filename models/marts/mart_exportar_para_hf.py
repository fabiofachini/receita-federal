# models/marts/mart__exportar_para_hf.py
import os
from huggingface_hub import HfApi
from dotenv import load_dotenv

def model(dbt, session):
    dbt.config(materialized="table")

    # --- Configurações ---
    load_dotenv()

    repo_id = "fabiofachini/receitafederal"  # nome do dataset no Hugging Face
    out_dir = "data"
    parquet_name = "dados_estabelecimentos_brasil.parquet"
    parquet_path = os.path.join(out_dir, parquet_name)
    path_in_repo = f"data/{parquet_name}"

    # --- Verifica se o arquivo existe ---
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(
            f"O arquivo parquet '{parquet_path}' não foi encontrado. "
            "Execute primeiro o modelo 'mart_exportar_estabelecimentos'."
        )

    # --- Token do Hugging Face ---
    hf_token = os.getenv("HF_TOKEN")
    if not hf_token:
        raise ValueError("HF_TOKEN não encontrado. Configure no .env ou no ambiente.")

    print(f"[info] Enviando arquivo {parquet_path} para o Hugging Face Hub...")

    # --- Upload ---
    api = HfApi()
    api.upload_file(
        path_or_fileobj=parquet_path,
        path_in_repo=path_in_repo,
        repo_id=repo_id,
        repo_type="dataset",
        token=hf_token,
        commit_message="Atualização automática via dbt"
    )

    print(f"[ok] Upload concluído: https://huggingface.co/datasets/{repo_id}/blob/main/{path_in_repo}")

    # Retorna algo mínimo para o dbt
    return session.sql("SELECT 1 AS ok")
