import duckdb
import pandas as pd
import pyodbc
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Carregar as variáveis de ambiente do arquivo .env
load_dotenv()

# Conexão com o DuckDB
duckdb_conn = duckdb.connect('caminho_do_seu_banco_duckdb')  # Substitua pelo caminho do seu banco DuckDB

# Configurar a string de conexão com o Azure SQL Database usando pyodbc
server = os.getenv("DB_SERVER")
database = os.getenv("DB_NAME")
username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
driver = os.getenv("DB_DRIVER")

connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
conn = pyodbc.connect(connection_string)
engine = create_engine(f'mssql+pyodbc:///?odbc_connect={connection_string}')

# Função para extrair dados do DuckDB e enviar para o Azure SQL Database
def transfer_marts_to_azure_sql(tables):
    for table in tables:
        # Extraindo dados da tabela do DuckDB
        query = f"SELECT * FROM {table}"
        df = duckdb.query(query).df()

        # Carregando os dados no Azure SQL Database
        df.to_sql(table, con=engine, if_exists='replace', index=False)
        print(f'Tabela {table} transferida com sucesso!')

# Lista das tabelas marts no DuckDB
marts_tables = ["tabela_mart1", "tabela_mart2"]  # Substitua com os nomes das suas tabelas marts

# Transferindo as tabelas marts para o Azure SQL Database
transfer_marts_to_azure_sql(marts_tables)

# Fechando a conexão com DuckDB e SQL Server
duckdb_conn.close()
conn.close()
