import subprocess
import os
import time
import sys


# Função para executar scripts Python
def executar_scripts():
    try:
        # Caminho para os arquivos
        source = os.path.join(os.getcwd())

        # Executa extração
        print("Baixando CSVs da Receita Federal")
        extract = os.path.join(source, 'extract.py')
        subprocess.run(['python3', extract], check=True)

        # Executa carregamento
        print("Importando no Banco de Dados DuckDB")
        load = os.path.join(source, 'load.py')
        subprocess.run(['python3', load], check=True)

        # Executa transformação
        print("Executando DBT")
        subprocess.run(['dbt', 'run'], check=True)
        subprocess.run(['dbt', 'test'], check=True)

    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar o script: {e}")
        raise

# Chamando a função para executar os scripts
if __name__ == "__main__":

    start_time = time.time()

    try:
        # Executa os scripts de ETL
        executar_scripts()
    except subprocess.CalledProcessError:
        sys.exit(1)

    # Marca o fim do tempo
    end_time = time.time()

    # Calcula o tempo total de execução
    elapsed_time = (end_time - start_time) / 60

    print("Extração, carregamento, transformação e transferência dos dados finalizados.")
    print(f"Tempo total de execução: {elapsed_time:.2f} minutos")
