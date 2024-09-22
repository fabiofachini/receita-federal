import os
import requests
import concurrent.futures
import time
from datetime import datetime, timedelta

# Definir o diretório base como o diretório atual do projeto
diretorio_base = '/media/fabio/SSD 2/RF'
#diretorio_base = os.path.join(os.getcwd(), 'source')

# Criar a pasta 'source' se não existirS
os.makedirs(diretorio_base, exist_ok=True)

# Links para os arquivos ZIP (apenas os nomes dos arquivos)
zip_files = [
    "Cnaes.zip",
    "Motivos.zip",
    "Municipios.zip",
    "Naturezas.zip",
    "Paises.zip",
    "Qualificacoes.zip",
    "Empresas0.zip",
    "Empresas1.zip",
    "Empresas2.zip",
    "Empresas3.zip",
    "Empresas4.zip",
    "Empresas5.zip",
    "Empresas6.zip",
    "Empresas7.zip",
    "Empresas8.zip",
    "Empresas9.zip",
    "Estabelecimentos0.zip",
    "Estabelecimentos1.zip",
    "Estabelecimentos2.zip",
    "Estabelecimentos3.zip",
    "Estabelecimentos4.zip",
    "Estabelecimentos5.zip",
    "Estabelecimentos6.zip",
    "Estabelecimentos7.zip",
    "Estabelecimentos8.zip",
    "Estabelecimentos9.zip",
    "Simples.zip",
    "Socios0.zip",
    "Socios1.zip",
    "Socios2.zip",
    "Socios3.zip",
    "Socios4.zip",
    "Socios5.zip",
    "Socios6.zip",
    "Socios7.zip",
    "Socios8.zip",
    "Socios9.zip"
]

# Função para construir a URL base e tentar o download
def obter_url_base():
    # Começar com o mês atual
    data_atual = datetime.now()
    
    # Tentar até encontrar o mês com dados disponíveis
    while True:
        mes_ano = data_atual.strftime('%Y-%m')
        url_base = f"http://200.152.38.155/CNPJ/dados_abertos_cnpj/{mes_ano}/"
        
        # Tentar verificar se a URL está acessível
        try:
            resposta = requests.head(url_base + "Empresas0.zip", timeout=10)
            if resposta.status_code == 200:
                print(f"Dados encontrados para o mês: {mes_ano}")
                return url_base
            else:
                print(f"Dados não encontrados para o mês: {mes_ano}. Tentando o mês anterior...")
        except requests.RequestException as e:
            print(f"Erro ao verificar o mês {mes_ano}: {e}")
        
        # Retroceder um mês
        data_atual -= timedelta(days=30)

# Função para organizar o download dos arquivos
def baixar_extrair(url_base, nome_arquivo):
    url = url_base + nome_arquivo
    caminho_zip = os.path.join(diretorio_base, nome_arquivo)
    
    print(f"Baixando {nome_arquivo} de {url}...")
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
            print(f"Progresso de {nome_arquivo}: {int(percentual)}%, Taxa: {velocidade_download:.2f} KB/s")
    
    print(f"{nome_arquivo} foi baixado com sucesso.")

# Função para executar o download de forma simultânea
def executar_download(url_base):
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(zip_files)) as executor:
        futures = [executor.submit(baixar_extrair, url_base, arquivo) for arquivo in zip_files]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Ocorreu um erro: {e}")

# Obter a URL base para o mês mais recente disponível
url_base = obter_url_base()

# Executar o download
if url_base:
    executar_download(url_base)
    print('Arquivos baixados com sucesso.')
else:
    print("Nenhum dado disponível.")
