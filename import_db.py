import pandas as pd
import os
import duckdb

# Corrigir erros dos arquivos e o encoding
print('Iniciando ajustes dos arquivos para importação no banco de dados')

diretorio = os.path.join(os.getcwd(), 'source')

con = duckdb.connect('dados.duckdb')

# ARQUIVO ESTABELECIMENTOS0   
print('Iniciando ajustes no arquivo Estabelecimentos0.')
dados_estabelecimentos0 = pd.read_csv(os.path.join(diretorio, 'Estabelecimentos0.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE estabelecimentos0 AS SELECT * FROM dados_estabelecimentos0')

# ARQUIVO ESTABELECIMENTOS1   
print('Iniciando ajustes no arquivo Estabelecimentos1.')
dados_estabelecimentos1 = pd.read_csv(os.path.join(diretorio, 'Estabelecimentos1.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE estabelecimentos1 AS SELECT * FROM dados_estabelecimentos1')

# ARQUIVO ESTABELECIMENTOS2
print('Iniciando ajustes no arquivo Estabelecimentos2.')
dados_estabelecimentos2 = pd.read_csv(os.path.join(diretorio, 'Estabelecimentos2.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE estabelecimentos2 AS SELECT * FROM dados_estabelecimentos2')

# ARQUIVO ESTABELECIMENTOS3
print('Iniciando ajustes no arquivo Estabelecimentos3.')
dados_estabelecimentos3 = pd.read_csv(os.path.join(diretorio, 'Estabelecimentos3.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE estabelecimentos3 AS SELECT * FROM dados_estabelecimentos3')

# ARQUIVO ESTABELECIMENTOS4
print('Iniciando ajustes no arquivo Estabelecimentos4.')
dados_estabelecimentos4 = pd.read_csv(os.path.join(diretorio, 'Estabelecimentos4.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE estabelecimentos4 AS SELECT * FROM dados_estabelecimentos4')

# ARQUIVO ESTABELECIMENTOS5
print('Iniciando ajustes no arquivo Estabelecimentos5.')
dados_estabelecimentos5 = pd.read_csv(os.path.join(diretorio, 'Estabelecimentos5.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE estabelecimentos5 AS SELECT * FROM dados_estabelecimentos5')

# ARQUIVO ESTABELECIMENTOS6
print('Iniciando ajustes no arquivo Estabelecimentos6.')
dados_estabelecimentos6 = pd.read_csv(os.path.join(diretorio, 'Estabelecimentos6.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE estabelecimentos6 AS SELECT * FROM dados_estabelecimentos6')

# ARQUIVO ESTABELECIMENTOS7
print('Iniciando ajustes no arquivo Estabelecimentos7.')
dados_estabelecimentos7 = pd.read_csv(os.path.join(diretorio, 'Estabelecimentos7.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE estabelecimentos7 AS SELECT * FROM dados_estabelecimentos7')

# ARQUIVO ESTABELECIMENTOS8
print('Iniciando ajustes no arquivo Estabelecimentos8.')
dados_estabelecimentos8 = pd.read_csv(os.path.join(diretorio, 'Estabelecimentos8.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE estabelecimentos8 AS SELECT * FROM dados_estabelecimentos8')

# ARQUIVO ESTABELECIMENTOS9
print('Iniciando ajustes no arquivo Estabelecimentos9.')
dados_estabelecimentos9 = pd.read_csv(os.path.join(diretorio, 'Estabelecimentos9.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE estabelecimentos9 AS SELECT * FROM dados_estabelecimentos9')

# ARQUIVO CNAES
print('Iniciando ajustes no arquivo Cnaes.')
dados_cnaes = pd.read_csv(os.path.join(diretorio, 'Cnaes.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE cnaes AS SELECT * FROM dados_cnaes')

# ARQUIVO MOTIVOS
print('Iniciando ajustes no arquivo Motivos.')
dados_motivos = pd.read_csv(os.path.join(diretorio, 'Motivos.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE motivos AS SELECT * FROM dados_motivos')

# ARQUIVO MUNICIPIOS
print('Iniciando ajustes no arquivo Municipios.')
dados_municipios = pd.read_csv(os.path.join(diretorio, 'Municipios.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE municipios AS SELECT * FROM dados_municipios')

# ARQUIVO NATUREZA
print('Iniciando ajustes no arquivo Natureza.')
dados_natureza = pd.read_csv(os.path.join(diretorio, 'Naturezas.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE naturezas AS SELECT * FROM dados_natureza')

# ARQUIVO PAISES
print('Iniciando ajustes no arquivo Paises.')
dados_paises = pd.read_csv(os.path.join(diretorio, 'Paises.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE paises AS SELECT * FROM dados_paises')

# ARQUIVO QUALIFICACOES
print('Iniciando ajustes no arquivo Qualificacoes.')
dados_qualificacoes = pd.read_csv(os.path.join(diretorio, 'Qualificacoes.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE qualificacoes AS SELECT * FROM dados_qualificacoes')

# ARQUIVO SIMPLES   
print('Iniciando ajustes no arquivo Simples.')
dados_simples = pd.read_csv(os.path.join(diretorio, 'Simples.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE simples AS SELECT * FROM dados_simples')

# ARQUIVO SOCIOS0   
print('Iniciando ajustes no arquivo Socios0.')
dados_socios0 = pd.read_csv(os.path.join(diretorio, 'Socios0.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE socios0 AS SELECT * FROM dados_socios0')

# ARQUIVO SOCIOS1   
print('Iniciando ajustes no arquivo Socios1.')
dados_socios1 = pd.read_csv(os.path.join(diretorio, 'Socios1.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE socios1 AS SELECT * FROM dados_socios1')

# ARQUIVO SOCIOS2
print('Iniciando ajustes no arquivo Socios2.')
dados_socios2 = pd.read_csv(os.path.join(diretorio, 'Socios2.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE socios2 AS SELECT * FROM dados_socios2')

# ARQUIVO SOCIOS3
print('Iniciando ajustes no arquivo Socios3.')
dados_socios3 = pd.read_csv(os.path.join(diretorio, 'Socios3.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE socios3 AS SELECT * FROM dados_socios3')

# ARQUIVO SOCIOS4
print('Iniciando ajustes no arquivo Socios4.')
dados_socios4 = pd.read_csv(os.path.join(diretorio, 'Socios4.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE socios4 AS SELECT * FROM dados_socios4')

# ARQUIVO SOCIOS5
print('Iniciando ajustes no arquivo Socios5.')
dados_socios5 = pd.read_csv(os.path.join(diretorio, 'Socios5.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE socios5 AS SELECT * FROM dados_socios5')

# ARQUIVO SOCIOS6
print('Iniciando ajustes no arquivo Socios6.')
dados_socios6 = pd.read_csv(os.path.join(diretorio, 'Socios6.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE socios6 AS SELECT * FROM dados_socios6')

# ARQUIVO SOCIOS7
print('Iniciando ajustes no arquivo Socios7.')
dados_socios7 = pd.read_csv(os.path.join(diretorio, 'Socios7.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE socios7 AS SELECT * FROM dados_socios7')

# ARQUIVO SOCIOS8
print('Iniciando ajustes no arquivo Socios8.')
dados_socios8 = pd.read_csv(os.path.join(diretorio, 'Socios8.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE socios8 AS SELECT * FROM dados_socios8')

# ARQUIVO SOCIOS9
print('Iniciando ajustes no arquivo Socios9.')
dados_socios9 = pd.read_csv(os.path.join(diretorio, 'Socios9.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE socios9 AS SELECT * FROM dados_socios9')

# ARQUIVO EMPRESAS0   
print('Iniciando ajustes no arquivo Empresas0.')
dados_empresas0 = pd.read_csv(os.path.join(diretorio, 'Empresas0.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE empresas0 AS SELECT * FROM dados_empresas0')

# ARQUIVO EMPRESAS1   
print('Iniciando ajustes no arquivo Empresas1.')
dados_empresas1 = pd.read_csv(os.path.join(diretorio, 'Empresas1.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE empresas1 AS SELECT * FROM dados_empresas1')

# ARQUIVO EMPRESAS2
print('Iniciando ajustes no arquivo Empresas2.')
dados_empresas2 = pd.read_csv(os.path.join(diretorio, 'Empresas2.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE empresas2 AS SELECT * FROM dados_empresas2')

# ARQUIVO EMPRESAS3
print('Iniciando ajustes no arquivo Empresas3.')
dados_empresas3 = pd.read_csv(os.path.join(diretorio, 'Empresas3.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE empresas3 AS SELECT * FROM dados_empresas3')

# ARQUIVO EMPRESAS4
print('Iniciando ajustes no arquivo Empresas4.')
dados_empresas4 = pd.read_csv(os.path.join(diretorio, 'Empresas4.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE empresas4 AS SELECT * FROM dados_empresas4')

# ARQUIVO EMPRESAS5
print('Iniciando ajustes no arquivo Empresas5.')
dados_empresas5 = pd.read_csv(os.path.join(diretorio, 'Empresas5.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE empresas5 AS SELECT * FROM dados_empresas5')

# ARQUIVO EMPRESAS6
print('Iniciando ajustes no arquivo Empresas6.')
dados_empresas6 = pd.read_csv(os.path.join(diretorio, 'Empresas6.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE empresas6 AS SELECT * FROM dados_empresas6')

# ARQUIVO EMPRESAS7
print('Iniciando ajustes no arquivo Empresas7.')
dados_empresas7 = pd.read_csv(os.path.join(diretorio, 'Empresas7.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE empresas7 AS SELECT * FROM dados_empresas7')

# ARQUIVO EMPRESAS8
print('Iniciando ajustes no arquivo Empresas8.')
dados_empresas8 = pd.read_csv(os.path.join(diretorio, 'Empresas8.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE empresas8 AS SELECT * FROM dados_empresas8')

# ARQUIVO EMPRESAS9
print('Iniciando ajustes no arquivo Empresas9.')
dados_empresas9 = pd.read_csv(os.path.join(diretorio, 'Empresas9.zip'), sep=';', compression='zip', encoding='latin1', header=None, dtype=str)

con.execute('CREATE OR REPLACE TABLE empresas9 AS SELECT * FROM dados_empresas9')



con.close()