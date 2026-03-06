import os
import random
import re
import smtplib
import ssl
import sys
import time
import xml.etree.ElementTree as ET
from email.message import EmailMessage
from urllib.parse import unquote, urlparse

import requests
from dotenv import load_dotenv

MONTH_PATTERN = re.compile(r"^\d{4}-\d{2}$")
NAMESPACE = {"d": "DAV:"}
REQUEST_TIMEOUT = (20, 120)
DOWNLOAD_TIMEOUT = (20, 300)

# Definir o diretório base como o diretório atual do projeto
diretorio_base = os.path.join(os.getcwd(), "source")

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
    "Socios9.zip",
]

# Headers para simular um navegador real
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "DNT": "1",
}


def extrair_token_e_origem(share_url):
    parsed = urlparse(share_url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"URL inválida em RF_SHARE_URL: {share_url}")

    match = re.search(r"/s/([^/?#]+)", parsed.path)
    if not match:
        raise ValueError(
            "Não foi possível extrair o token da URL. "
            "Formato esperado: https://.../index.php/s/<token>"
        )

    token = match.group(1)
    origem = f"{parsed.scheme}://{parsed.netloc}"
    return origem, token


def montar_dav_root(origem, token):
    return f"{origem}/public.php/dav/files/{token}/"


def _prop_com_status_ok(response_node):
    for propstat in response_node.findall("d:propstat", NAMESPACE):
        status = propstat.findtext("d:status", default="", namespaces=NAMESPACE)
        if "200" in status:
            return propstat.find("d:prop", NAMESPACE)
    return None


def _nome_do_href(href):
    return os.path.basename(unquote(href).rstrip("/"))


def listar_entries_dav(session, url, token, depth=1):
    headers = {"Depth": str(depth)}
    resposta = session.request(
        "PROPFIND",
        url,
        headers=headers,
        auth=(token, ""),
        timeout=REQUEST_TIMEOUT,
    )
    resposta.raise_for_status()

    try:
        root = ET.fromstring(resposta.text)
    except ET.ParseError as exc:
        raise RuntimeError("Resposta WebDAV inválida ao listar diretório.") from exc

    entries = []
    for response_node in root.findall("d:response", NAMESPACE):
        href = response_node.findtext("d:href", default="", namespaces=NAMESPACE)
        prop = _prop_com_status_ok(response_node)
        if not prop or not href:
            continue

        is_collection = prop.find("d:resourcetype/d:collection", NAMESPACE) is not None
        content_length_text = prop.findtext(
            "d:getcontentlength", default=None, namespaces=NAMESPACE
        )
        content_length = (
            int(content_length_text)
            if content_length_text and content_length_text.isdigit()
            else None
        )

        entries.append(
            {
                "href": unquote(href),
                "name": _nome_do_href(href),
                "is_collection": is_collection,
                "content_length": content_length,
            }
        )

    return entries


def listar_meses_disponiveis(session, dav_root, token):
    entries = listar_entries_dav(session, dav_root, token, depth=1)
    meses = [
        entry["name"]
        for entry in entries
        if entry["is_collection"] and MONTH_PATTERN.fullmatch(entry["name"] or "")
    ]
    return sorted(meses, reverse=True)


def listar_arquivos_do_mes(session, dav_root, token, mes):
    url_mes = f"{dav_root}{mes}/"
    entries = listar_entries_dav(session, url_mes, token, depth=1)
    return {
        entry["name"]: entry
        for entry in entries
        if not entry["is_collection"] and entry["name"]
    }


def descobrir_mes_valido(session, dav_root, token):
    meses = listar_meses_disponiveis(session, dav_root, token)
    if not meses:
        raise RuntimeError("Nenhuma pasta de mês (YYYY-MM) encontrada no compartilhamento.")

    print(f"Meses encontrados no compartilhamento: {', '.join(meses[:6])}...")
    for mes in meses:
        arquivos_no_mes = listar_arquivos_do_mes(session, dav_root, token, mes)
        faltantes = [arquivo for arquivo in zip_files if arquivo not in arquivos_no_mes]
        if not faltantes:
            print(f"Dados encontrados para o mês: {mes}")
            return mes

        print(
            f"Mês {mes} incompleto ({len(faltantes)} arquivo(s) faltando). "
            "Tentando mês anterior..."
        )

    raise RuntimeError("Nenhum mês completo encontrado para os arquivos esperados.")


def tamanho_arquivo_servidor(session, url, token):
    resposta = session.head(
        url,
        headers=HEADERS,
        auth=(token, ""),
        timeout=REQUEST_TIMEOUT,
        allow_redirects=True,
    )
    resposta.raise_for_status()
    tamanho = resposta.headers.get("content-length")
    if tamanho and tamanho.isdigit():
        return int(tamanho)
    return None


def enviar_alerta_email(assunto, mensagem):
    smtp_host = os.getenv("SMTP_HOST")
    smtp_to = os.getenv("SMTP_TO")

    if not smtp_host or not smtp_to:
        print("SMTP não configurado. Alerta por e-mail não enviado.")
        return False

    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    smtp_from = os.getenv("SMTP_FROM", smtp_user or "noreply@localhost")
    destinatarios = [email.strip() for email in smtp_to.split(",") if email.strip()]

    if not destinatarios:
        print("SMTP_TO está vazio. Alerta por e-mail não enviado.")
        return False

    email = EmailMessage()
    email["Subject"] = assunto
    email["From"] = smtp_from
    email["To"] = ", ".join(destinatarios)
    email.set_content(mensagem)

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as smtp:
            smtp.ehlo()
            if smtp.has_extn("starttls"):
                smtp.starttls(context=ssl.create_default_context())
                smtp.ehlo()
            if smtp_user and smtp_password:
                smtp.login(smtp_user, smtp_password)
            smtp.send_message(email)
        print("Alerta por e-mail enviado com sucesso.")
        return True
    except Exception as exc:
        print(f"Falha ao enviar e-mail de alerta: {exc}")
        return False


def baixar_arquivo(session, dav_root, token, mes, nome_arquivo, tentativas_max=3):
    url = f"{dav_root}{mes}/{nome_arquivo}"
    caminho_zip = os.path.join(diretorio_base, nome_arquivo)

    # Verificar se o arquivo já existe e está completo
    if os.path.exists(caminho_zip):
        try:
            tamanho_servidor = tamanho_arquivo_servidor(session, url, token)
            tamanho_local = os.path.getsize(caminho_zip)

            if tamanho_servidor and tamanho_local == tamanho_servidor:
                print(f"Arquivo {nome_arquivo} já existe e está completo. Pulando...")
                return True
        except Exception as exc:
            print(f"Erro ao verificar arquivo existente {nome_arquivo}: {exc}")

    for tentativa in range(tentativas_max):
        try:
            print(f"Baixando {nome_arquivo} (Tentativa {tentativa + 1}/{tentativas_max})...")
            time.sleep(random.uniform(1, 3))

            resposta = session.get(
                url,
                headers=HEADERS,
                auth=(token, ""),
                stream=True,
                timeout=DOWNLOAD_TIMEOUT,
            )
            resposta.raise_for_status()

            tamanho_total = int(resposta.headers.get("content-length", 0))
            tamanho_bloco = 1024 * 1024  # 1MB por bloco
            total_baixado = 0
            tempo_inicio = time.time()

            with open(caminho_zip, "wb") as arquivo_zip:
                for dados in resposta.iter_content(tamanho_bloco):
                    if not dados:
                        continue

                    arquivo_zip.write(dados)
                    total_baixado += len(dados)
                    tempo_decorrido = max(time.time() - tempo_inicio, 1e-6)
                    velocidade = total_baixado / (1024 * 1024 * tempo_decorrido)

                    if tamanho_total > 0:
                        percentual = (total_baixado / tamanho_total) * 100
                        print(
                            f"Progresso de {nome_arquivo}: "
                            f"{int(percentual)}%, {velocidade:.2f} MB/s"
                        )
                    else:
                        print(
                            f"Progresso de {nome_arquivo}: "
                            f"{total_baixado / (1024 * 1024):.2f} MB, {velocidade:.2f} MB/s"
                        )

                    time.sleep(0.1)

            print(f"{nome_arquivo} foi baixado com sucesso.")
            tempo_espera = random.uniform(2, 5)
            print(f"Aguardando {tempo_espera:.1f} segundos antes do próximo download...")
            time.sleep(tempo_espera)
            return True

        except requests.RequestException as exc:
            print(f"Erro no download de {nome_arquivo} (Tentativa {tentativa + 1}): {exc}")
            if tentativa < tentativas_max - 1:
                tempo_espera = (tentativa + 1) * 10
                print(f"Aguardando {tempo_espera} segundos antes de tentar novamente...")
                time.sleep(tempo_espera)
            else:
                print(f"Falha ao baixar {nome_arquivo} após {tentativas_max} tentativas.")
                return False

    return False


def main():
    load_dotenv()
    share_url = os.getenv("RF_SHARE_URL")

    try:
        if not share_url:
            raise RuntimeError(
                "RF_SHARE_URL não configurada. Defina no ambiente ou no arquivo .env."
            )

        origem, token = extrair_token_e_origem(share_url)
        dav_root = montar_dav_root(origem, token)
        print(f"Usando compartilhamento: {share_url}")

        with requests.Session() as session:
            mes = descobrir_mes_valido(session, dav_root, token)
            print(f"Iniciando downloads sequenciais para o mês {mes}...")

            arquivos_com_erro = []
            arquivos_ok = []

            for arquivo in zip_files:
                if baixar_arquivo(session, dav_root, token, mes, arquivo):
                    arquivos_ok.append(arquivo)
                else:
                    arquivos_com_erro.append(arquivo)

        print("\n=== Relatório de Downloads ===")
        print(f"Total de arquivos: {len(zip_files)}")
        print(f"Downloads com sucesso: {len(arquivos_ok)}")
        print(f"Downloads com erro: {len(arquivos_com_erro)}")

        if arquivos_com_erro:
            print("\nArquivos com erro:")
            for arquivo in arquivos_com_erro:
                print(f"- {arquivo}")
            raise RuntimeError(
                f"Falha ao baixar {len(arquivos_com_erro)} arquivo(s) do mês {mes}."
            )

        return 0

    except Exception as exc:
        erro = f"{type(exc).__name__}: {exc}"
        print(f"Erro fatal na extração: {erro}")
        enviar_alerta_email(
            assunto="[receita-federal] Falha no extract.py",
            mensagem=(
                "Falha ao executar o download dos arquivos da Receita Federal.\n\n"
                f"Erro: {erro}\n"
                f"Share URL em uso: {share_url}\n"
            ),
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
