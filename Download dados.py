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
    block_size = 512000
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
    print(f"Descompactando {filename}...")
    with zipfile.ZipFile(file_path, "r") as zip_ref:
        zip_ref.extractall(output_dir)
    
    print(f"{filename} foi baixado e descompactado com sucesso.")

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
