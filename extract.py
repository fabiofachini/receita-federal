import os
import requests
import time
from datetime import datetime, timedelta
import random

# Definir o diretório base como o diretório atual do projeto
diretorio_base = os.path.join(os.getcwd(), 'source')

# Criar a pasta 'source' se não existir
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

# Headers para simular um navegador real
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'Connection': 'keep-alive',
    'DNT': '1',
}

def obter_url_base():
    data_atual = datetime.now()
    
    while True:
        mes_ano = data_atual.strftime('%Y-%m')
        url_base = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{mes_ano}/"
        
        try:
            resposta = requests.head(url_base + "Empresas0.zip", headers=HEADERS, timeout=10)
            if resposta.status_code == 200:
                print(f"Dados encontrados para o mês: {mes_ano}")
                return url_base
            else:
                print(f"Dados não encontrados para o mês: {mes_ano}. Tentando o mês anterior...")
        except requests.RequestException as e:
            print(f"Erro ao verificar o mês {mes_ano}: {e}")
        
        data_atual -= timedelta(days=30)

def baixar_arquivo(url_base, nome_arquivo, tentativas_max=3):
    url = url_base + nome_arquivo
    caminho_zip = os.path.join(diretorio_base, nome_arquivo)
    
    # Verificar se o arquivo já existe e está completo
    if os.path.exists(caminho_zip):
        try:
            # Verificar tamanho do arquivo no servidor
            resp_head = requests.head(url, headers=HEADERS, timeout=10)
            tamanho_servidor = int(resp_head.headers.get('content-length', 0))
            tamanho_local = os.path.getsize(caminho_zip)
            
            if tamanho_local == tamanho_servidor:
                print(f"Arquivo {nome_arquivo} já existe e está completo. Pulando...")
                return True
        except Exception as e:
            print(f"Erro ao verificar arquivo existente: {e}")
    
    for tentativa in range(tentativas_max):
        try:
            print(f"Baixando {nome_arquivo} (Tentativa {tentativa + 1}/{tentativas_max})...")
            
            # Adicionar um delay aleatório entre 1 e 5 segundos antes de cada download
            time.sleep(random.uniform(1, 5))
            
            resposta = requests.get(url, headers=HEADERS, stream=True, timeout=300)
            resposta.raise_for_status()
            
            tamanho_total = int(resposta.headers.get("content-length", 0))
            tamanho_bloco = 1024 * 1024  # 1MB por bloco
            total_baixado = 0
            tempo_inicio = time.time()
            
            with open(caminho_zip, "wb") as f:
                for dados in resposta.iter_content(tamanho_bloco):
                    if dados:
                        f.write(dados)
                        total_baixado += len(dados)
                        percentual = (total_baixado / tamanho_total) * 100
                        tempo_decorrido = time.time() - tempo_inicio
                        velocidade = total_baixado / (1024 * 1024 * tempo_decorrido)  # MB/s
                        
                        print(f"Progresso de {nome_arquivo}: {int(percentual)}%, {velocidade:.2f} MB/s")
                        
                        # Pequena pausa entre blocos para evitar sobrecarga
                        time.sleep(0.1)
            
            print(f"{nome_arquivo} foi baixado com sucesso.")
            
            # Aguardar entre 3 e 8 segundos antes do próximo arquivo
            tempo_espera = random.uniform(3, 8)
            print(f"Aguardando {tempo_espera:.1f} segundos antes do próximo download...")
            time.sleep(tempo_espera)
            
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"Erro no download de {nome_arquivo} (Tentativa {tentativa + 1}): {e}")
            if tentativa < tentativas_max - 1:
                tempo_espera = (tentativa + 1) * 10  # Aumenta o tempo de espera a cada tentativa
                print(f"Aguardando {tempo_espera} segundos antes de tentar novamente...")
                time.sleep(tempo_espera)
            else:
                print(f"Falha ao baixar {nome_arquivo} após {tentativas_max} tentativas.")
                return False

def main():
    url_base = obter_url_base()
    if not url_base:
        print("Não foi possível encontrar uma URL base válida.")
        return

    print("Iniciando downloads sequenciais...")
    
    arquivos_com_erro = []
    arquivos_ok = []
    
    for arquivo in zip_files:
        if baixar_arquivo(url_base, arquivo):
            arquivos_ok.append(arquivo)
        else:
            arquivos_com_erro.append(arquivo)
    
    # Relatório final
    print("\n=== Relatório de Downloads ===")
    print(f"Total de arquivos: {len(zip_files)}")
    print(f"Downloads com sucesso: {len(arquivos_ok)}")
    print(f"Downloads com erro: {len(arquivos_com_erro)}")
    
    if arquivos_com_erro:
        print("\nArquivos com erro:")
        for arquivo in arquivos_com_erro:
            print(f"- {arquivo}")

if __name__ == "__main__":
    main()
