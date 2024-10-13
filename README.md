# Receita Federal - Consulta de Dados de Empresas do Brasil

Este repositório contém scripts e instruções para consultar, extrair, carregar e transformar (ELT) os dados das empresas do Brasil, disponibilizados pela Receita Federal através do site Dados Abertos Receita Federal. O processo envolve a extração automática dos dados mais recentes, a carga para um banco de dados local (DuckDB) e a construção dos modelos de consulta.

## Exemplos de Dados Disponíveis
- **CNPJ**
- **Razão Social**
- **Capital Social**
- **Porte**
- **Setor**
- **CNAE**
- **Sócios**
- **Endereço**
- **Telefone**
- **E-mail**

### Principais Documentos

- `extract.py`: Responsável por identificar e baixar o arquivo mais recente com os dados de empresas da Receita Federal. Utiliza as bibliotecas `requests` e `concurrent` para baixar os dados em paralelo, acelerando o processo.
  
- `load.py`: Cria um banco de dados local utilizando **DuckDB** e carrega todos os arquivos baixados para este banco.

- `dbt`: Ferramenta para transformação dos dados para organizar e preparar os modelos para análise.

- `run_pipeline_local.py`: Executa todo o pipeline de forma automatizada.

## Requisitos de Software

- **Python**
- **DuckDB** (banco de dados local)
- **Espaço em disco** 30GB
- **Memória** 8GB

## Como Usar

1. **Instalar Dependências**: Certifique-se de ter o Python 3 instalado e instale as dependências listadas no arquivo `requirements.txt`:
   ```bash
   pip install -r requirements.txt

2. **Executar o Projeto**:O fluxo de trabalho segue um pipeline ELT (Extract, Load, Transform), automatizado e executado pelo script principal `run_pipeline_local.py`.