import duckdb
import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Carregar as variáveis de ambiente do arquivo .env
load_dotenv()

# Conexão com o DuckDB
duckdb_conn = duckdb.connect('dados.duckdb')

# Configurar a string de conexão com o Azure SQL Database usando pyodbc
server = os.getenv("DB_SERVER")
database = os.getenv("DB_DATABASE")
username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
driver = os.getenv("DB_DRIVER")

connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
conn = pyodbc.connect(connection_string)
engine = create_engine(f'mssql+pyodbc:///?odbc_connect={connection_string}', 
                       connect_args={'fast_executemany': True})

# Função para transferir os dados em lotes e substituir a tabela
def transfer_marts_to_azure_sql_in_chunks(tables, chunk_size=10000):

    for table in tables:
        print(f"Iniciando transferência da tabela {table}.")

        # Remover a tabela existente no Azure SQL Database, se ela existir
        with engine.connect() as connection:
            drop_query = text(f"DROP TABLE IF EXISTS {table}")
            connection.execute(drop_query)
            print(f"Tabela {table} removida no Azure SQL Database.")

        # Contando o número total de linhas na tabela DuckDB
        total_rows = duckdb_conn.execute(f"SELECT COUNT(*) FROM main.{table}").fetchone()[0]
        print(f"Total de linhas na tabela {table}: {total_rows}")

        # Transferindo os dados em chunks
        offset = 0
        first_chunk = True  # Controla o modo de inserção da primeira iteração
        while offset < total_rows:
            query = f"SELECT * FROM main.{table} LIMIT {chunk_size} OFFSET {offset}"
            df = duckdb_conn.execute(query).fetchdf()

            # Para o primeiro chunk, cria a tabela. Para os demais, faz append
            df.to_sql(table, con=engine, if_exists='replace' if first_chunk else 'append', index=False)
            first_chunk = False

            # Calculando e exibindo o progresso
            offset += chunk_size
            progress = min(offset, total_rows) / total_rows * 100
            print(f"Progresso: {progress:.2f}%")

        print(f"Tabela {table} transferida com sucesso!")

# Lista das tabelas marts no DuckDB
marts_tables = [
    "mart_estabelecimentos_setores_count", 
    "mart_estabelecimentos_cidade_count",
    "mart_estabelecimentos_count",
    "mart_estabelecimentos_completo_fpolis"
]

# Transferindo as tabelas marts para o Azure SQL Database em chunks
transfer_marts_to_azure_sql_in_chunks(marts_tables)

# Fechando a conexão com DuckDB e SQL Server
duckdb_conn.close()
conn.close()
