import os
import requests
import zipfile
import concurrent.futures
import time

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
output_dir = "D:\RF"

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




import pandas as pd
import csv

# Definir o nome do arquivo CSV
Empresas0 = 'D:/Empresas0.zip'
Empresas1 = 'D:/Empresas1.zip'
Empresas2 = 'D:/Empresas2.zip'
Empresas3 = 'D:/Empresas3.zip'
Empresas4 = 'D:/Empresas4.zip'
Empresas5 = 'D:/Empresas5.zip'
Empresas6 = 'D:/Empresas6.zip'
Empresas7 = 'D:/Empresas7.zip'
Empresas8 = 'D:/Empresas8.zip'
Empresas9 = 'D:/Empresas9.zip'
Estabelecimentos0 = 'D:/Estabelecimentos0.zip'
Estabelecimentos1 = 'D:/Estabelecimentos1.zip'
Estabelecimentos2 = 'D:/Estabelecimentos2.zip'
Estabelecimentos3 = 'D:/Estabelecimentos3.zip'
Estabelecimentos4 = 'D:/Estabelecimentos4.zip'
Estabelecimentos5 = 'D:/Estabelecimentos5.zip'
Estabelecimentos6 = 'D:/Estabelecimentos6.zip'
Estabelecimentos7 = 'D:/Estabelecimentos7.zip'
Estabelecimentos8 = 'D:/Estabelecimentos8.zip'
Estabelecimentos9 = 'D:/Estabelecimentos9.zip'
Socios0 = 'D:/Socios0.zip'
Socios1 = 'D:/Socios1.zip'
Socios2 = 'D:/Socios2.zip'
Socios3 = 'D:/Socios3.zip'
Socios4 = 'D:/Socios4.zip'
Socios5 = 'D:/Socios5.zip'
Socios6 = 'D:/Socios6.zip'
Socios7 = 'D:/Socios7.zip'
Socios8 = 'D:/Socios8.zip'
Socios9 = 'D:/Socios9.zip'
Cnaes = 'D:/Cnaes.zip'
Motivos = 'D:/Motivos.zip'
Municipios = 'D:/Municipios.zip'
Natureza = 'D:/Naturezas.zip'
Paises = 'D:/Paises.zip'
Qualificacoes = 'D:/Qualificacoes.zip'
Simples = 'D:/Simples.zip'

# Importar os dados do CSV
dados_empresas0 = pd.read_csv(Empresas0, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_empresas1 = pd.read_csv(Empresas1, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_empresas2 = pd.read_csv(Empresas2, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_empresas3 = pd.read_csv(Empresas3, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_empresas4 = pd.read_csv(Empresas4, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_empresas5 = pd.read_csv(Empresas5, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_empresas6 = pd.read_csv(Empresas6, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_empresas7 = pd.read_csv(Empresas7, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_empresas8 = pd.read_csv(Empresas8, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_empresas9 = pd.read_csv(Empresas9, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

dados_estabelecimentos0 = pd.read_csv(Estabelecimentos0, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_estabelecimentos1 = pd.read_csv(Estabelecimentos1, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_estabelecimentos2 = pd.read_csv(Estabelecimentos2, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_estabelecimentos3 = pd.read_csv(Estabelecimentos3, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_estabelecimentos4 = pd.read_csv(Estabelecimentos4, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_estabelecimentos5 = pd.read_csv(Estabelecimentos5, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_estabelecimentos6 = pd.read_csv(Estabelecimentos6, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_estabelecimentos7 = pd.read_csv(Estabelecimentos7, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_estabelecimentos8 = pd.read_csv(Estabelecimentos8, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_estabelecimentos9 = pd.read_csv(Estabelecimentos9, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

dados_socios0 = pd.read_csv(Socios0, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_socios1 = pd.read_csv(Socios1, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_socios2 = pd.read_csv(Socios2, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_socios3 = pd.read_csv(Socios3, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_socios4 = pd.read_csv(Socios4, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_socios5 = pd.read_csv(Socios5, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_socios6 = pd.read_csv(Socios6, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_socios7 = pd.read_csv(Socios7, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_socios8 = pd.read_csv(Socios8, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_socios9 = pd.read_csv(Socios9, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

dados_cnaes = pd.read_csv(Cnaes, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_motivos = pd.read_csv(Motivos, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_municipios = pd.read_csv(Municipios, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_natureza = pd.read_csv(Natureza, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_paises = pd.read_csv(Paises, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_qualificacoes = pd.read_csv(Qualificacoes, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
dados_simples = pd.read_csv(Simples, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

# Remover linhas inválidas
dados_empresas0 = dados_empresas0[dados_empresas0.iloc[:,1] != '']
dados_empresas1 = dados_empresas1[dados_empresas1.iloc[:,1] != '']
dados_empresas2 = dados_empresas2[dados_empresas2.iloc[:,1] != '']
dados_empresas3 = dados_empresas3[dados_empresas3.iloc[:,1] != '']
dados_empresas4 = dados_empresas4[dados_empresas4.iloc[:,1] != '']
dados_empresas5 = dados_empresas5[dados_empresas5.iloc[:,1] != '']
dados_empresas6 = dados_empresas6[dados_empresas6.iloc[:,1] != '']
dados_empresas7 = dados_empresas7[dados_empresas7.iloc[:,1] != '']
dados_empresas8 = dados_empresas8[dados_empresas8.iloc[:,1] != '']
dados_empresas9 = dados_empresas9[dados_empresas9.iloc[:,1] != '']

# Exportar CSV
dados_empresas0.to_csv('D:/NOVO/empresas0.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_empresas1.to_csv('D:/NOVO/empresas1.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_empresas2.to_csv('D:/NOVO/empresas2.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_empresas3.to_csv('D:/NOVO/empresas3.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_empresas4.to_csv('D:/NOVO/empresas4.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_empresas5.to_csv('D:/NOVO/empresas5.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_empresas6.to_csv('D:/NOVO/empresas6.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_empresas7.to_csv('D:/NOVO/empresas7.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_empresas8.to_csv('D:/NOVO/empresas8.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_empresas9.to_csv('D:/NOVO/empresas9.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')

dados_estabelecimentos0.to_csv('D:/NOVO/estabelecimentos0.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_estabelecimentos1.to_csv('D:/NOVO/estabelecimentos1.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_estabelecimentos2.to_csv('D:/NOVO/estabelecimentos2.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_estabelecimentos3.to_csv('D:/NOVO/estabelecimentos3.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_estabelecimentos4.to_csv('D:/NOVO/estabelecimentos4.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_estabelecimentos5.to_csv('D:/NOVO/estabelecimentos5.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_estabelecimentos6.to_csv('D:/NOVO/estabelecimentos6.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_estabelecimentos7.to_csv('D:/NOVO/estabelecimentos7.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_estabelecimentos8.to_csv('D:/NOVO/estabelecimentos8.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_estabelecimentos9.to_csv('D:/NOVO/estabelecimentos9.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')

dados_socios0.to_csv('D:/NOVO/socios0.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_socios1.to_csv('D:/NOVO/socios1.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_socios2.to_csv('D:/NOVO/socios2.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_socios3.to_csv('D:/NOVO/socios3.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_socios4.to_csv('D:/NOVO/socios4.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_socios5.to_csv('D:/NOVO/socios5.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_socios6.to_csv('D:/NOVO/socios6.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_socios7.to_csv('D:/NOVO/socios7.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_socios8.to_csv('D:/NOVO/socios8.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_socios9.to_csv('D:/NOVO/socios9.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')

dados_cnaes.to_csv('D:/NOVO/cnaes.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_motivos.to_csv('D:/NOVO/motivos.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_municipios.to_csv('D:/NOVO/municipios.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_natureza.to_csv('D:/NOVO/natureza.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_paises.to_csv('D:/NOVO/paises.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_qualificacoes.to_csv('D:/NOVO/qualificacoes.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
dados_simples.to_csv('D:/NOVO/simples.csv', encoding='utf-8', errors='ignore', index=False, header=False, quoting=csv.QUOTE_ALL, quotechar='"')
