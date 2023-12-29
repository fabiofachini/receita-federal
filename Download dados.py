import pandas as pd
import csv
import os
import requests
import zipfile
import concurrent.futures
import time
import shutil

# Criar pastas
diretorio_base = 'C:/Users/analista.mercado/Downloads'

# Tentar criar os diretórios
def criar_pastas(diretorio_das_pastas):
    os.makedirs(os.path.join(diretorio_das_pastas, 'RF'), exist_ok=True)
    os.makedirs(os.path.join(diretorio_das_pastas, 'RF', 'temp'), exist_ok=True)

criar_pastas(diretorio_base)

# Diretório onde os arquivos serão baixados
diretorio_temp = os.path.join(diretorio_base, 'RF', 'temp')

# Diretório onde os arquivos serão ajustados
diretorio_RF = os.path.join(diretorio_base, 'RF')

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

# Função para organizar o download dos arquivos
def baixar_extrair(url):
    nome_arquivos = url.split("/")[-1]
    diretorio_arquivos = os.path.join(diretorio_temp, nome_arquivos)
    
    print(f"Baixando {nome_arquivos}...")
    resposta = requests.get(url, stream=True, timeout=240)
    
    tamanho_total = int(resposta.headers.get("content-length", 0))
    tamanho_bloco = 4096000
    total_baixado = 0
    tempo_inicio = time.time()
    
    with open(diretorio_arquivos, "wb") as f:
        for dados in resposta.iter_content(tamanho_bloco):
            f.write(dados)
            total_baixado += len(dados)
            percentual = (total_baixado / tamanho_total) * 100
            tempo_decorrido = time.time() - tempo_inicio
            velocidade_download = total_baixado / (tempo_decorrido * 1024)  # Em KB/s
            print(f"Progresso de {nome_arquivos}: {int(percentual)}%, Taxa: {velocidade_download:.2f} KB/s")
    
    print()
    print(f"{nome_arquivos} foi baixado com sucesso.")

# Função para executar o download e torná-los simultâneos
def executar_download():
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(zip_links)) as executor:
        futures = [executor.submit(baixar_extrair, link) for link in zip_links]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Ocorreu um erro: {e}")

executar_download()
print('Arquivos baixados.')


# Corrigir erros dos arquivos e o encoding
print('Iniciando ajustes dos arquivos para importação no banco de dados')

# Arquivo Empresas
print('Iniciando ajustes no arquivo Empresas.')

# Lista de caminhos dos arquivos
arquivos_empresas = [os.path.join(diretorio_temp, f'Empresas{i}.zip') for i in range(10)]

# Lista para armazenar os DataFrames de cada arquivo
dados_empresas = []

# Loop através dos arquivos e processamento em chunks
for arquivo in arquivos_empresas:
    chunks = pd.read_csv(arquivo, sep=';', compression='zip', encoding='latin1', header=None, dtype=str, chunksize=1000)
    for chunk in chunks:
        dados_empresas.append(chunk)

print('Importação Empresas concluída. Iniciando criação do Dataframe.')

# Concatenar em um único DataFrame
dados_empresas = pd.concat(dados_empresas)
print('Dataframe Empresas criado. Excluíndo dados duplicados e ausentes.')

# Excluir linhas inválidas e duplicadas
dados_empresas = dados_empresas[dados_empresas[1] != ''].drop_duplicates(subset=0, keep='first')
print('Dados das Empresas excluídos. Iniciando exportação para CSV.')

# Exportar CSV Empresas
dados_empresas.to_csv(os.path.join(diretorio_RF, 'empresas.csv'), encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
print('Arquivo Empresas exportado.')


# Arquivo Estabelecimentos
print('Iniciando ajustes no arquivo Estabelecimentos.')

# Lista de caminhos dos arquivos
arquivos_estabelecimentos = [os.path.join(diretorio_temp, f'Estabelecimentos{i}.zip') for i in range(10)]

# Lista para armazenar os DataFrames de cada arquivo
dados_estabelecimentos = []

# Loop através dos arquivos e processamento em chunks
for arquivo in arquivos_estabelecimentos:
    chunks = pd.read_csv(arquivo, sep=';', compression='zip', encoding='latin1', header=None, dtype=str, chunksize=1000)
    for chunk in chunks:
        dados_estabelecimentos.append(chunk)

print('Importação Estabelecimentos concluída. Iniciando criação do Dataframe.')

# Concatenar em um único DataFrame
dados_estabelecimentos = pd.concat(dados_estabelecimentos)
print('Dataframe Estabelecimentos criado. Iniciando exportação para CSV.')

# Exportar CSV Estabelecimentos
dados_estabelecimentos.to_csv(os.path.join(diretorio_RF, 'empresas.csv'), encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
print('Arquivo Estabelecimentos exportado.')

# Arquivo Sócios
print('Iniciando ajustes no arquivo Sócios.')

# Lista de caminhos dos arquivos
arquivos_socios = [os.path.join(diretorio_temp, f'Socios{i}.zip') for i in range(10)]

# Lista para armazenar os DataFrames de cada arquivo
dados_socios = []

# Loop através dos arquivos e processamento em chunks
for arquivo in arquivos_socios:
    chunks = pd.read_csv(arquivo, sep=';', compression='zip', encoding='latin1', header=None, dtype=str, chunksize=1000)
    for chunk in chunks:
        dados_socios.append(chunk)

print('Importação Sócios concluída. Iniciando criação do Dataframe.')

# Concatenar em um único DataFrame
dados_socios = pd.concat(dados_socios)
print('Dataframe Sócios criado. Iniciando exportação para CSV.')

# Exportar CSV Sócios
dados_socios.to_csv(os.path.join(diretorio_RF, 'empresas.csv'), encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
print('Arquivo Sócios exportado.')

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
