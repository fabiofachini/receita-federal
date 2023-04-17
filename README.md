# receita-federal
 
# Introdução
Repositório criado para auxiliar no download e utilização da base de dados de CNPJs da Receita Federal do Brasil. Além disso, está disponível dados para criação de dados das empresas da cidade de Florianópolis com Latitude e Longitude.

# Software
Postgres 15 versão

# Como usar
1 - Faça o download dos arquivos na página dos Dados Abertos https://www.gov.br/receitafederal/pt-br/acesso-a-informacao/dados-abertos/cadastros
2 - Crie a base de dados e as tabelas através do arquivo Criação_das_Tabelas.sql
3 - Utilize o arquivo Empresas_Brasil.sql para selecionar as empresas ativas do Brasil
4 - Através do arquivo Empresas_Florianópolis.sql e Geolocalizacao_Florianopolis.csv crie e selecione as empresas ativas da cidade de Florianópolis com latitude e longitude

# Problemas
É comum virem um ou dois erros em nos arquivos da Receita Federal, como linhas duplicados ou caracteres inválidos. Nestes casos, a importação informará quais são os erros, podendo ser ajustados e realizar nova importação.

Outra possibilidade de erro é que tenha sido criada uma nova empresa em Florianópolis em um novo CEP. Neste caso o arquivo Geolocalizacao_Florianopolis.csv precisa ser atualizado ou os valores precisam ser incluídos no banco de dados.
