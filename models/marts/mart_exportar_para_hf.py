# models/marts/mart__exportar_para_hf.py
import os
from pathlib import Path
from huggingface_hub import HfApi
from dotenv import load_dotenv, find_dotenv, dotenv_values

def model(dbt, session):
    dbt.config(materialized="table")
    _ = dbt.ref("mart_exportar_estabelecimentos")

    # --- Descoberta e carregamento do .env (Opção A) ---
    dotenv_file = find_dotenv(filename=".env", usecwd=True)
    if dotenv_file:
        load_dotenv(dotenv_file, override=True)
    else:
        # Carrega variáveis do ambiente mesmo sem .env (ex.: CI)
        load_dotenv(override=True)

    # Logs de debug úteis
    try:
        print(f"[debug] .env: {dotenv_file or 'não encontrado'}")
        print(f"[debug] CWD: {os.getcwd()}")
        print(f"[debug] __file__: {__file__}")
    except Exception:
        # Em alguns ambientes __file__ pode não estar disponível
        pass

    # --- Configurações ---
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
    hf_token = (
        os.getenv("HF_TOKEN")
    )
    # Fallback: ler diretamente o arquivo .env encontrado
    if not hf_token and dotenv_file:
        hf_token = dotenv_values(dotenv_file).get("HF_TOKEN")

    if not hf_token:
        raise ValueError(
            "HF_TOKEN não encontrado. Configure no .env (sem aspas) ou no ambiente."
        )

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
