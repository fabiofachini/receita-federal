import subprocess
import os
import time


# Função para executar scripts Python
def executar_scripts():
    try:
        # Caminho para os arquivos api_ibge.py e api_bacen.py na pasta source
        source = os.path.join(os.getcwd())

        # Executa api_ibge.py
        print("Baixando CSVs da Receita Federal")
        download_csv = os.path.join(source, 'download.py')
        subprocess.run(['python3', download_csv])

        # Executa api_bacen.py
        print("Importando no Banco de Dados")
        import_db = os.path.join(source, 'import_db.py')
        subprocess.run(['python3', import_db])

        # Executa dbt run
        print("Executando dbt run")
        subprocess.run(['dbt', 'run'])
        subprocess.run(['dbt', 'test'])

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
    elapsed_time = end_time - start_time

    print("Extração, carregamento e transformação dos dados do BACEN e do IBGE finalizados.")
    print(f"Tempo total de execução: {elapsed_time:.2f} segundos")