import pandas as pd
import os
import duckdb

# Corrigir erros dos arquivos e o encoding
print('Iniciando ajustes dos arquivos para importação no banco de dados')

diretorio = os.path.join(os.getcwd(), 'source')
con = duckdb.connect('receita-federal.duckdb')

# Definir os nomes dos arquivos e das tabelas
arquivos_estabelecimentos = [f'Estabelecimentos{i}.zip' for i in range(10)]
arquivos_socios = [f'Socios{i}.zip' for i in range(10)]
arquivos_empresas = [f'Empresas{i}.zip' for i in range(10)]
outros_arquivos = ['Cnaes.zip', 'Motivos.zip', 'Municipios.zip', 'Naturezas.zip', 'Paises.zip', 'Qualificacoes.zip', 'Simples.zip']

# Função para ler e carregar dados no banco de dados
def processar_arquivo(nome_arquivo, nome_tabela):
    print(f'Iniciando carregamento do arquivo {nome_arquivo}.')
    caminho_arquivo = os.path.join(diretorio, nome_arquivo)
    dados = pd.read_csv(caminho_arquivo, sep=';', compression='zip', encoding='latin1', header=None, dtype=str)
    nome_tabela_com_prefixo = f'src_{nome_tabela}'
    con.execute(f'CREATE OR REPLACE TABLE {nome_tabela_com_prefixo} AS SELECT * FROM dados')

# Processar arquivos de estabelecimentos
for i, arquivo in enumerate(arquivos_estabelecimentos):
    processar_arquivo(arquivo, f'estabelecimentos{i}')

# Processar arquivos de sócios
for i, arquivo in enumerate(arquivos_socios):
    processar_arquivo(arquivo, f'socios{i}')

# Processar arquivos de empresas
for i, arquivo in enumerate(arquivos_empresas):
    processar_arquivo(arquivo, f'empresas{i}')

# Processar outros arquivos
tabelas = ['cnaes', 'motivos', 'municipios', 'naturezas', 'paises', 'qualificacoes', 'simples']
for arquivo, tabela in zip(outros_arquivos, tabelas):
    processar_arquivo(arquivo, tabela)

print('Processamento concluído.')
