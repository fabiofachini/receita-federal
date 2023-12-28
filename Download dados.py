import pandas as pd
import csv
import os
import requests
import zipfile
import concurrent.futures
import time
import shutil

# Criar pastas
diretorio = 'D:\\'

# Tentar criar os diretórios
os.makedirs(os.path.join(diretorio, 'RF'), exist_ok=True)
os.makedirs(os.path.join(diretorio, 'RF', 'temp'), exist_ok=True)

# Links para os arquivos ZIP
zip_links = [
    "http://200.152.38.155/CNPJ/Cnaes.zip",
    "http://200.152.38.155/CNPJ/Motivos.zip",
    "http://200.152.38.155/CNPJ/Municipios.zip",
    "http://200.152.38.155/CNPJ/Naturezas.zip",
    "http://200.152.38.155/CNPJ/Paises.zip",
    "http://200.152.38.155/CNPJ/Qualificacoes.zip",
    "http://200.152.38.155/CNPJ/Empresas0.zip",
    "http://200.152.38.155/CNPJ/Empresas1.zip",
    "http://200.152.38.155/CNPJ/Empresas2.zip",
    "http://200.152.38.155/CNPJ/Empresas3.zip",
    "http://200.152.38.155/CNPJ/Empresas4.zip",
    "http://200.152.38.155/CNPJ/Empresas5.zip",
    "http://200.152.38.155/CNPJ/Empresas6.zip",
    "http://200.152.38.155/CNPJ/Empresas7.zip",
    "http://200.152.38.155/CNPJ/Empresas8.zip",
    "http://200.152.38.155/CNPJ/Empresas9.zip",
    "http://200.152.38.155/CNPJ/Estabelecimentos0.zip",
    "http://200.152.38.155/CNPJ/Estabelecimentos1.zip",
    "http://200.152.38.155/CNPJ/Estabelecimentos2.zip",
    "http://200.152.38.155/CNPJ/Estabelecimentos3.zip",
    "http://200.152.38.155/CNPJ/Estabelecimentos4.zip",
    "http://200.152.38.155/CNPJ/Estabelecimentos5.zip",
    "http://200.152.38.155/CNPJ/Estabelecimentos6.zip",
    "http://200.152.38.155/CNPJ/Estabelecimentos7.zip",
    "http://200.152.38.155/CNPJ/Estabelecimentos8.zip",
    "http://200.152.38.155/CNPJ/Estabelecimentos9.zip",
    "http://200.152.38.155/CNPJ/Simples.zip",
    "http://200.152.38.155/CNPJ/Socios0.zip",
    "http://200.152.38.155/CNPJ/Socios1.zip",
    "http://200.152.38.155/CNPJ/Socios2.zip",
    "http://200.152.38.155/CNPJ/Socios3.zip",
    "http://200.152.38.155/CNPJ/Socios4.zip",
    "http://200.152.38.155/CNPJ/Socios5.zip",
    "http://200.152.38.155/CNPJ/Socios6.zip",
    "http://200.152.38.155/CNPJ/Socios7.zip",
    "http://200.152.38.155/CNPJ/Socios8.zip",
    "http://200.152.38.155/CNPJ/Socios9.zip"
]

# Diretório onde os arquivos serão baixados e descompactados
output_dir = "D:/RF/TEMP"

def download_and_extract(url):
    filename = url.split("/")[-1]
    file_path = os.path.join(output_dir, filename)
    
    print(f"Baixando {filename}...")
    response = requests.get(url, stream=True, timeout=240)
    
    total_size = int(response.headers.get("content-length", 0))
    block_size = 4096000
    downloaded = 0
    start_time = time.time()
    
    with open(file_path, "wb") as f:
        for data in response.iter_content(block_size):
            f.write(data)
            downloaded += len(data)
            percent = (downloaded / total_size) * 100
            elapsed_time = time.time() - start_time
            download_speed = downloaded / (elapsed_time * 1024)  # Em KB/s
            print(f"Progresso de {filename}: {int(percent)}%, Taxa: {download_speed:.2f} KB/s")
    
    print()  # Pula uma linha para limpar a linha de progresso
    
    # Descompactar os arquivos
    #print(f"Descompactando {filename}...")
    #with zipfile.ZipFile(file_path, "r") as zip_ref:
        #zip_ref.extractall(output_dir)
    
    print(f"{filename} foi baixado com sucesso.")

def main():
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(zip_links)) as executor:
        futures = [executor.submit(download_and_extract, link) for link in zip_links]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Ocorreu um erro: {e}")

if __name__ == "__main__":
    main()
print('Arquivos baixados.')


#AJUTE DOS ARQUIVOS PARA IMPORTAÇÃO
#ARQUIVOS EMPRESAS
# Lista de caminhos dos arquivos
arquivos = [os.path.join(output_dir, f'Empresas{i}.zip') for i in range(10)]

# Lista para armazenar os DataFrames de cada arquivo
dados_empresas = []

# Loop através dos arquivos e processamento em chunks
for arquivo in arquivos:
    chunks = pd.read_csv(arquivo, sep=';', compression='zip', encoding='latin1', header=None, dtype=str, chunksize=1000)
    for chunk in chunks:
        # Remover linhas inválidas
        chunk = chunk[chunk[1] != '']
        dados_empresas.append(chunk)

# Concatenar todos os DataFrames em um único DataFrame
dados_empresas = pd.concat(dados_empresas)
dados_empresas = dados_empresas[dados_empresas[1] != ''].drop_duplicates(subset=0, keep='first')

dados_empresas.to_csv('D:/RF/empresas.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
print('Arquivo Empresas ajustado.')

#ARQUIVOS ESTABELECIMENTOS
# Lista de caminhos dos arquivos
arquivos = ['D:/RF/TEMP/Estabelecimentos0.zip', 'D:/RF/TEMP/Estabelecimentos1.zip', 'D:/RF/TEMP/Estabelecimentos2.zip', 'D:/RF/TEMP/Estabelecimentos3.zip', 'D:/RF/TEMP/Estabelecimentos4.zip',
            'D:/RF/TEMP/Estabelecimentos5.zip', 'D:/RF/TEMP/Estabelecimentos6.zip', 'D:/RF/TEMP/Estabelecimentos7.zip', 'D:/RF/TEMP/Estabelecimentos8.zip', 'D:/RF/TEMP/Estabelecimentos9.zip']

# Lista para armazenar os DataFrames de cada arquivo
dados_estabelecimentos = []

# Loop através dos arquivos e processamento em chunks
for arquivo in arquivos:
    chunks = pd.read_csv(arquivo, sep=';', compression='zip', encoding='latin1', header=None, dtype=str, chunksize=200)
    for chunk in chunks:
        dados_estabelecimentos.append(chunk)

# Concatenar todos os DataFrames em um único DataFrame
dados_estabelecimentos = pd.concat(dados_estabelecimentos)

dados_estabelecimentos.to_csv('D:/RF/estabelecimentos.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
print('Arquivo Estabelecimentos ajustado.')

#ARQUIVOS SOCIOS
# Lista de caminhos dos arquivos
arquivos = ['D:/RF/TEMP/Socios0.zip', 'D:/RF/TEMP/Socios1.zip', 'D:/RF/TEMP/Socios2.zip', 'D:/RF/TEMP/Socios3.zip', 'D:/RF/TEMP/Socios4.zip',
            'D:/RF/TEMP/Socios5.zip', 'D:/RF/TEMP/Socios6.zip', 'D:/RF/TEMP/Socios7.zip', 'D:/RF/TEMP/Socios8.zip', 'D:/RF/TEMP/Socios9.zip']

# Lista para armazenar os DataFrames de cada arquivo
dados_socios = []

# Loop através dos arquivos e processamento em chunks
for arquivo in arquivos:
    chunks = pd.read_csv(arquivo, sep=';', compression='zip', encoding='latin1', header=None, dtype=str, chunksize=1000)
    for chunk in chunks:
        dados_socios.append(chunk)

# Concatenar todos os DataFrames em um único DataFrame
dados_socios = pd.concat(dados_socios)

dados_socios.to_csv('D:/RF/socios.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
print('Arquivo Socios ajustado.')

#ARQUIVOS SIMPLES
Simples = 'D:/RF/TEMP/Simples.zip'
dados_simples = pd.read_csv(Simples, sep=';', compression='zip', encoding='latin1', header=None, dtype=str, chunksize=1000)

# Abra o arquivo CSV de saída em modo de escrita
with open('D:/RF/simples.csv', mode='w', encoding='utf-8', newline='') as file:
    # Crie um escritor CSV
    csv_writer = csv.writer(file, quoting=csv.QUOTE_ALL, quotechar='"')

    # Itere sobre os chunks e escreva cada chunk no arquivo CSV
    for chunk in dados_simples:
        chunk.to_csv(file, index=False, header=False, encoding='utf-8', quoting=csv.QUOTE_NONE, quotechar='')
print('Arquivo Simples ajustado.')

#OUTROS ARQUIVOS
Cnaes = 'D:/RF/TEMP/Cnaes.zip'
Motivos = 'D:/RF/TEMP/Motivos.zip'
Municipios = 'D:/RF/TEMP/Municipios.zip'
Natureza = 'D:/RF/TEMP/Naturezas.zip'
Paises = 'D:/RF/TEMP/Paises.zip'
Qualificacoes = 'D:/RF/TEMP/Qualificacoes.zip'

dados_cnaes = pd.read_csv(Cnaes, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_motivos = pd.read_csv(Motivos, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_municipios = pd.read_csv(Municipios, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_natureza = pd.read_csv(Natureza, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_paises = pd.read_csv(Paises, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_qualificacoes = pd.read_csv(Qualificacoes, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

dados_cnaes.to_csv('D:/RF/cnaes.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_motivos.to_csv('D:/RF/motivos.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_municipios.to_csv('D:/RF/municipios.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_natureza.to_csv('D:/RF/natureza.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_paises.to_csv('D:/RF/paises.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_qualificacoes.to_csv('D:/RF/qualificacoes.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
print('Demais arquivos ajustados.')

shutil.rmtree('D:/RF/TEMP')

print('Arquivos prontos para importação no banco de dados.')
