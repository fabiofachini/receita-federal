import os
import requests
import concurrent.futures
import time

# Definir o diretório base como o diretório atual do projeto
diretorio_base = os.path.join(os.getcwd(), 'source')

# Criar a pasta 'source' se não existir
os.makedirs(diretorio_base, exist_ok=True)

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
    caminho_zip = os.path.join(diretorio_base, nome_arquivos)
    
    print(f"Baixando {nome_arquivos}...")
    resposta = requests.get(url, stream=True, timeout=240)
    
    tamanho_total = int(resposta.headers.get("content-length", 0))
    tamanho_bloco = 4096000
    total_baixado = 0
    tempo_inicio = time.time()
    
    with open(caminho_zip, "wb") as f:
        for dados in resposta.iter_content(tamanho_bloco):
            f.write(dados)
            total_baixado += len(dados)
            percentual = (total_baixado / tamanho_total) * 100
            tempo_decorrido = time.time() - tempo_inicio
            velocidade_download = total_baixado / (tempo_decorrido * 1024)  # Em KB/s
            print(f"Progresso de {nome_arquivos}: {int(percentual)}%, Taxa: {velocidade_download:.2f} KB/s")
    
    print()
    print(f"{nome_arquivos} foi baixado com sucesso.")

# Função para executar o download de forma simultânea
def executar_download():
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(zip_links)) as executor:
        futures = [executor.submit(baixar_extrair, link) for link in zip_links]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Ocorreu um erro: {e}")

executar_download()
print('Arquivos baixados e extraídos.')
