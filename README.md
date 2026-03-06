# Receita Federal - Base de Dados de Empresas Brasileiras

[![Python Version](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![DuckDB](https://img.shields.io/badge/DuckDB-0.8.1-orange.svg)](https://duckdb.org/)

## 📋 Visão Geral

Este projeto oferece uma solução completa para gerenciar e analisar dados de empresas brasileiras disponibilizados pela Receita Federal através do portal de Dados Abertos. A solução implementa um pipeline ELT (Extração, Carga e Transformação) moderno e eficiente, utilizando DuckDB como banco de dados local de alta performance.

### 🎯 Principais Benefícios

- **Processo Automatizado**: Pipeline completo executado com um único comando
- **Alta Performance**: Utilização de DuckDB para processamento eficiente de grandes volumes de dados
- **Baixo Consumo de Recursos**: Processamento otimizado que funciona em máquinas convencionais
- **Dados Organizados**: Estruturação clara dos dados para análises e consultas

## 📊 Dados Disponíveis
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

## 🛠️ Arquitetura do Projeto

O projeto é organizado em módulos independentes que trabalham em conjunto:

### Pipeline Principal (Automático)

1. **`run_pipeline.py`**
   - Script principal que orquestra todo o processo
   - Executa automaticamente as etapas de extração, carga e transformação
   - Única ferramenta que você precisa executar para ter o ambiente funcionando

2. **`extract.py`**
   - Responsável pela extração dos dados da Receita Federal
   - Utiliza download paralelo para otimizar o tempo de download
   - Gerencia automaticamente as atualizações dos dados

3. **`load.py`**
   - Configura e popula o banco de dados DuckDB
   - Processa os arquivos baixados e estrutura os dados
   - Otimiza o armazenamento para consultas eficientes

4. **`dbt/`**
   - Modelos de transformação de dados
   - Cria visões otimizadas para análise
   - Implementa regras de negócio e agregações

### Ferramentas Auxiliares

5. **`estados.py`**
   - Ferramenta para análise regionalizada
   - Divide a base de empresas por estado
   - Gera arquivos CSV separados na pasta `estados/`
   - Exclui MEIs para análises específicas
   - Deve ser executado após o pipeline principal

## 💻 Requisitos do Sistema

### Hardware Recomendado
- **Processador**: 2 cores ou mais
- **Memória RAM**: 8GB (mínimo)
- **Armazenamento**: 30GB de espaço livre
- **Internet**: Conexão estável para download dos dados

### Software Necessário
- **Sistema Operacional**: Ubuntu 22.04 ou 24.04
- **Python**: Versão 3.8 ou superior
- **DuckDB**: Instalado automaticamente com as dependências

## 📊 Estrutura dos Modelos DBT

O projeto utiliza uma arquitetura em camadas para organizar os dados:

### 1. Source
- Camada que reflete as tabelas brutas carregadas no DuckDB
- Localizada em `models/source/`
- Não deve ser usada para consultas diretas
- Exemplo: `source_empresas`, `source_socios`, `source_estabelecimentos`

### 2. Staging
- Primeira camada de transformação
- Limpeza inicial e padronização de dados
- Localizada em `models/staging/`
- Nomenclatura: `stg_*`
- Exemplo: `stg_empresas` (dados limpos e padronizados)

### 3. Intermediate
- Camada intermediária com transformações complexas
- Combina dados de diferentes fontes staging
- Localizada em `models/intermediate/`
- Nomenclatura: `int_*`
- Exemplo: `int_empresas_socios` (relacionamento entre empresas e sócios)

### 4. Marts
- Camada final otimizada para consultas
- Modelos prontos para análise de negócio
- Localizada em `models/marts/`
- Dividida por área de negócio

#### Marts Disponíveis:

1. **`mart_estabelecimentos_completo`**
   - Visão completa de todos os estabelecimentos
   - Inclui dados de endereço, contato e situação cadastral
   - Contém todos os tipos de empresas, incluindo MEIs
   - Ideal para análises detalhadas e cruzamentos de dados

2. **`mart_estabelecimentos_completo_sem_mei`**
   - Similar ao mart_estabelecimentos_completo
   - Exclui Microempreendedores Individuais (MEIs)
   - Útil para análises focadas em empresas de maior porte
   - Base para geração dos arquivos por estado

3. **`mart_estabelecimentos_setores_count`**
   - Contagem de estabelecimentos ativos por setor econômico
   - Setores: agricultura, indústria, construção, comércio, serviços
   - Baseado no CNAE principal
   - Ideal para análises macroeconômicas

4. **`mart_estabelecimentos_cidade_count`**
   - Distribuição de estabelecimentos por cidade
   - Inclui contagem total por município
   - Permite análises de concentração geográfica
   - Útil para estudos de desenvolvimento regional

5. **`mart_estabelecimentos_count`**
   - Contagem geral de estabelecimentos
   - Visão agregada do número total de empresas
   - Pode ser usado para acompanhamento de crescimento
   - Métrica base para outros indicadores

### Exemplos de Consultas

```sql
-- Contagem de estabelecimentos ativos por setor
SELECT * FROM mart_estabelecimentos_setores_count;

-- Top 10 cidades com mais estabelecimentos
SELECT * FROM mart_estabelecimentos_cidade_count 
ORDER BY total_estabelecimentos DESC LIMIT 10;

-- Estabelecimentos completos de uma cidade específica
SELECT * FROM mart_estabelecimentos_completo
WHERE municipio = 'SAO PAULO';
```

### Visualização do Modelo de Dados

Para visualizar o fluxograma dos modelos e suas dependências:
```bash
dbt docs generate
dbt docs serve
```
Acesse http://localhost:8080 para ver a documentação completa e o lineage graph.

## 🔍 Acessando os Dados

### Opção 1: DBeaver (Recomendado para Análises)
1. Instale o DBeaver Community:
   ```bash
   sudo apt update
   sudo apt install dbeaver-ce
   ```
2. Abra o DBeaver e adicione uma nova conexão
3. Escolha DuckDB como driver
4. Selecione o arquivo `receita-federal.duckdb`

### Opção 2: CLI DuckDB
1. Instale o DuckDB CLI:
   ```bash
   sudo apt install duckdb
   ```
2. Acesse o banco:
   ```bash
   duckdb receita-federal.duckdb
   ```
3. Execute consultas SQL diretamente:
   ```sql
   SELECT * FROM mart_empresas_ativas LIMIT 5;
   ```

## 🚀 Guia de Instalação

1. **Clone o Repositório**
   ```bash
   git clone https://github.com/fabiofachini/receita-federal.git
   cd receita-federal
   ```

2. **Configure o Ambiente**
   ```bash
   # Crie um ambiente virtual (recomendado)
   python -m venv venv
   
   # Ative o ambiente virtual
   # Windows:
   venv\Scripts\activate
   # Linux/macOS:
   source venv/bin/activate
   
   # Instale as dependências
   pip install -r requirements.txt
   ```

## 📦 Executando o Projeto

1. **Pipeline Principal**
   ```bash
   python run_pipeline.py
   ```
   Este comando executará todo o processo automaticamente. O tempo de execução pode variar dependendo da sua conexão e do hardware disponível.

2. **Análise por Estados** (Opcional)
   ```bash
   python estados.py
   ```
   Execute este comando após o pipeline principal para gerar arquivos CSV separados por estado na pasta `estados/`.

## ⚙️ Configurações do Extract

O `extract.py` usa o compartilhamento público da Receita via WebDAV e descobre automaticamente o mês mais recente completo.
As variáveis podem ser definidas no ambiente ou em arquivo `.env` (carregado automaticamente).

- `RF_SHARE_URL` (obrigatória): URL pública do compartilhamento (com token).
- `HF_TOKEN` (obrigatória para upload no Hugging Face): token usado pelo modelo `mart_exportar_para_hf.py` para publicar o parquet no dataset. Se o dataset for privado, a aplicação também precisará dele para download.
- `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`, `SMTP_FROM`, `SMTP_TO` (opcionais): envio de alerta por e-mail quando o extract falhar.

Exemplo de `.env`:
```env
RF_SHARE_URL=https://arquivos.receitafederal.gov.br/index.php/s/SEU_TOKEN_AQUI
HF_TOKEN=hf_seu_token_aqui
SMTP_HOST=smtp.seudominio.com
SMTP_PORT=587
SMTP_USER=usuario@seudominio.com
SMTP_PASSWORD=senha
SMTP_FROM=alertas@seudominio.com
SMTP_TO=dev1@seudominio.com,dev2@seudominio.com
```

## 📊 Estrutura dos Dados

Após a execução, você terá acesso a:
- Base de dados completa no arquivo `receita-federal.duckdb`
- Arquivos CSV por estado na pasta `estados/` (se executar o script opcional)

## 🤝 Contribuindo

Contribuições são bem-vindas! Por favor, sinta-se à vontade para abrir issues ou enviar pull requests com melhorias.

## 📝 Licença

Este projeto está sob a licença MIT. Veja o arquivo `LICENSE` para mais detalhes.
