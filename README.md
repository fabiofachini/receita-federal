# receita-federal
 
# Introdução
Repositório criado para auxiliar no download e utilização da base de dados de CNPJs da Receita Federal do Brasil.

# Software
Postgres 15 versão.

# Como usar
- Faça o download dos arquivos na página dos Dados Abertos https://www.gov.br/receitafederal/pt-br/acesso-a-informacao/dados-abertos/cadastros
- Crie a base de dados e as tabelas através do arquivo Criação_das_Tabelas.sql;
- Utilize o arquivo Empresas_Brasil.sql para selecionar as empresas ativas do Brasil e suas características.

# Problemas
É comum virem um ou dois erros em nos arquivos da Receita Federal, como linhas duplicados ou caracteres inválidos. Nestes casos, a importação informará quais são os erros, devendo ser ajustados para realizar nova importação.
