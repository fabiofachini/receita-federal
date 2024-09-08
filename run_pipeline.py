import subprocess
import os
import time


# Função para executar scripts Python
def executar_scripts():
    try:
        # Caminho para os arquivos
        source = os.path.join(os.getcwd())

        # Executa extração
        print("Baixando CSVs da Receita Federal")
        extract = os.path.join(source, 'extract.py')
        subprocess.run(['python3', extract])

        # Executa carregamento
        print("Importando no Banco de Dados DuckDB")
        load = os.path.join(source, 'load.py')
        subprocess.run(['python3', load])

        # Executa transformação
        print("Executando DBT")
        subprocess.run(['dbt', 'run'])
        subprocess.run(['dbt', 'test'])

        # Executa transferência para nuvem
        print("Transferindo para nuvem")
        transfer = os.path.join(source, 'transfer.py')
        subprocess.run(['python3', transfer])

    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar o script: {e}")

# Chamando a função para executar os scripts
if __name__ == "__main__":

    start_time = time.time()

    # Executa os scripts de ETL
    executar_scripts()

    # Marca o fim do tempo
    end_time = time.time()

    # Calcula o tempo total de execução
    elapsed_time = (end_time - start_time) / 60

    print("Extração, carregamento, transformação e transferência dos dados finalizados.")
    print(f"Tempo total de execução: {elapsed_time:.2f} minutos")