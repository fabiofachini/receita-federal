import duckdb
import math
import os

# Conecta ao banco
con = duckdb.connect('receita-federal.duckdb')

# Lista de estados
estados = ['EX', 'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG',
           'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO']

# Limite de linhas por arquivo
MAX_LINHAS = 800_000

# Pasta de saída
pasta_saida = './estados/'  # pasta estados na raiz do projeto

# Cria a pasta estados se não existir
os.makedirs(pasta_saida, exist_ok=True)

# Exporta um ou mais CSVs por estado
for estado in estados:
    # Conta quantas linhas existem para o estado
    total_linhas = con.execute(f"""
        SELECT COUNT(*) FROM mart_estabelecimentos_completo_sem_mei
        WHERE estado = '{estado}'
    """).fetchone()[0]

    # Quantos arquivos serão necessários
    num_arquivos = math.ceil(total_linhas / MAX_LINHAS)

    for i in range(num_arquivos):
        offset = i * MAX_LINHAS
        caminho = os.path.join(pasta_saida, f"{estado}{i+1 if num_arquivos > 1 else ''}.csv")
        print(f"Exportando {caminho}...")

        query = f"""
            COPY (
                SELECT * FROM mart_estabelecimentos_completo_sem_mei
                WHERE estado = '{estado}'
                LIMIT {MAX_LINHAS} OFFSET {offset}
            ) TO '{caminho}' (HEADER, DELIMITER ';')
        """

        con.execute(query)

print("✅ Exportação concluída!")
