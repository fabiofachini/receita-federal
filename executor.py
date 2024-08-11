import subprocess
import os

# Função para executar scripts Python
def executar_scripts():
    try:
        # Caminho para os arquivos api_ibge.py e api_bacen.py na pasta source
        source = os.path.join(os.getcwd())

        # Executa api_ibge.py
        print("Baixando CSVs da Receita Federal")
        download_csv = os.path.join(source, 'download_csv.py')
        subprocess.run(['python3', download_csv])

        # Executa api_bacen.py
        print("Importando no Banco de Dados")
        import_db = os.path.join(source, 'import_db.py')
        subprocess.run(['python3', import_db])

    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar o script: {e}")

# Chamando a função para executar os scripts
if __name__ == "__main__":
    
    # Executa os scripts de ETL
    executar_scripts()

    print("Extração, carregamento e transformação dos dados do BACEN e do IBGE finalizados.")