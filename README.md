# Projeto de Integração de Dados - Dengue (Grupo 06)

Pipeline ETL/ELT para análise de dados de notificações de dengue no Brasil, utilizando dados do Sistema de Informação de Agravos de Notificação (SINAN) do OpenDataSUS.

[Relatório](https://docs.google.com/document/d/1vgZkXXLSiDTwJ_Ra-N6O_aeIuo5cmfFL4RfVDWkRZWw/edit?tab=t.0)

## Sumário

- [Sobre o Projeto](#sobre-o-projeto)
- [Arquitetura](#arquitetura)
- [Tecnologias Utilizadas](#tecnologias-utilizadas)
- [Pré-requisitos](#pré-requisitos)
- [Instalação](#instalação)
- [Configuração](#configuração)
- [Executando o Projeto](#executando-o-projeto)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Modelo Dimensional](#modelo-dimensional)
- [Referências](#referências)

## Sobre o Projeto

Este projeto implementa um Data Warehouse dimensional para análise de casos de dengue no Brasil nos anos de 2020 a 2022. Os dados são extraídos do SINAN (Sistema de Informação de Agravos de Notificação) e transformados em um modelo dimensional usando dbt (Data Build Tool) para facilitar análises epidemiológicas.

### Fontes de Dados

- **Fonte**: OpenDataSUS - SINAN/Dengue
- **URL**: https://opendatasus.saude.gov.br/dataset/arboviroses-dengue
- **Período**: 2020-2022
- **Volume**: ~3.9 milhões de registros
- **Formato**: CSV compactado (ZIP)
- **Dicionário de Dados**: https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINAN/Dengue/dic_dados_dengue.pdf

## Tecnologias Utilizadas

- **Python 3.10+**: Linguagem principal para ETL
- **PostgreSQL**: Banco de dados relacional
- **dbt-core 1.10.15**: Framework de transformação de dados
- **dbt-postgres 1.9.1**: Adaptador PostgreSQL para dbt
- **Pandas**: Manipulação de dados em Python
- **Jupyter Notebook**: Análise exploratória e prototipagem
- **dbt-utils**: Pacote de utilitários para dbt

## Pré-requisitos

Antes de começar, certifique-se de ter instalado:

- Python 3.10 ou superior
- PostgreSQL 12 ou superior
- pip (gerenciador de pacotes Python)
- Git

## Instalação

### 1. Clone o Repositório

```bash
git clone <url-do-repositorio>
cd projeto-integracao
```

### 2. Crie e Ative o Ambiente Virtual

**Linux/Mac:**
```bash
python3 -m venv venv
source venv/bin/activate
```

**Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

### 3. Instale as Dependências

```bash
pip install -r requirements.txt
```

Isso instalará todas as dependências necessárias, incluindo:
- dbt-core e dbt-postgres
- pandas, numpy
- psycopg2-binary (driver PostgreSQL)
- jupyter (para notebooks)

### 4. Configure o PostgreSQL

#### Instale o PostgreSQL (se ainda não tiver)

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
```

**Mac (usando Homebrew):**
```bash
brew install postgresql
brew services start postgresql
```

**Windows:**
- Baixe e instale do site oficial: https://www.postgresql.org/download/windows/

#### Crie o Banco de Dados

```bash
# Acesse o PostgreSQL como usuário postgres
sudo -u postgres psql

# Dentro do psql, execute:
CREATE DATABASE dengue_db;
CREATE USER postgres WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE dengue_db TO postgres;
\q
```

## Configuração

### 1. Configure o dbt Profile

O arquivo de configuração do dbt já está configurado em `~/.dbt/profiles.yml` com as seguintes configurações:

```yaml
dengue_project:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: postgres
      password: password
      port: 5432
      dbname: dengue_db
      schema: public
```

Se você precisar modificar essas configurações (por exemplo, usar uma senha diferente), edite o arquivo:

```bash
nano ~/.dbt/profiles.yml
```

### 2. Verifique a Conexão do dbt

```bash
cd transform
dbt debug
```

Este comando verificará se todas as configurações estão corretas e se o dbt consegue se conectar ao PostgreSQL.

### 3. Instale os Pacotes dbt

```bash
cd transform
dbt deps
```

Isso instalará o pacote `dbt-utils` necessário para as transformações.

## Executando o Projeto

### Passo 1: Extrair e Carregar os Dados (Extract & Load)

O notebook [notebooks/ETL.ipynb](notebooks/ETL.ipynb) contém o código para:
- Baixar os CSVs do OpenDataSUS (anos 2020-2022)
- Carregar os dados no PostgreSQL

**Opção 1: Usando Jupyter Notebook**

```bash
# Inicie o Jupyter Notebook
jupyter notebook

# Navegue até notebooks/ETL.ipynb e execute as células
```

**Opção 2: Carregar Manualmente os CSVs**

Se você já tem os arquivos CSV em [notebooks/](notebooks/):

```bash
# Crie as tabelas raw no PostgreSQL
psql -U postgres -d dengue_db

-- Dentro do psql:
CREATE TABLE dengue_raw_2020 AS TABLE dengue_raw_2020 WITH NO DATA;
-- (repita para 2021 e 2022)
```

Depois, use o comando COPY do PostgreSQL ou o script Python no notebook para carregar os dados.

### Passo 2: Verificar os Dados Raw

Após carregar os dados, verifique se as tabelas foram criadas:

```bash
psql -U postgres -d dengue_db -c "\dt"
```

Você deve ver as tabelas:
- `dengue_raw_2020`
- `dengue_raw_2021`
- `dengue_raw_2022`

Verifique a contagem de registros:

```sql
SELECT
    'dengue_raw_2020' as tabela, COUNT(*) as registros
FROM dengue_raw_2020
UNION ALL
SELECT
    'dengue_raw_2021', COUNT(*)
FROM dengue_raw_2021
UNION ALL
SELECT
    'dengue_raw_2022', COUNT(*)
FROM dengue_raw_2022;
```

### Passo 3: Executar as Transformações dbt (Transform)

Com os dados carregados no banco, execute as transformações dbt:

```bash
cd transform

# Execute todas as transformações
dbt run
```

Este comando irá:
1. Criar a tabela staging `stg_dengue` (união dos 3 anos)
2. Criar as dimensões:
   - `dim_tempo` - Datas e períodos temporais
   - `dim_localidade` - Municípios e UFs
   - `dim_paciente` - Dados demográficos dos pacientes
   - `dim_unidade_saude` - Unidades de saúde notificadoras
   - `dim_notificacao` - Informações da notificação
   - `dim_exames` - Resultados de exames laboratoriais
   - `dim_quadro_clinico` - Sintomas e comorbidades
3. Criar a tabela fato `fato_casos` com métricas agregadas

**Executar modelos específicos:**

```bash
# Apenas a staging
dbt run --select stg_dengue

# Apenas as dimensões
dbt run --select marts.dim_*

# Apenas a tabela fato
dbt run --select fato_casos
```

### Passo 4: Visualizar os Resultados

Conecte-se ao banco e consulte as tabelas transformadas:

```bash
psql -U postgres -d dengue_db
```

**Exemplo de consultas:**

```sql
-- Contagem de casos por UF
SELECT
    l.nome_uf,
    COUNT(*) as total_casos
FROM fato_casos f
JOIN dim_localidade l ON f.sk_localidade = l.sk_localidade
GROUP BY l.nome_uf
ORDER BY total_casos DESC;

-- Casos por mês e classificação final
SELECT
    t.ano,
    t.mes,
    n.classificacao_final_desc,
    COUNT(*) as casos
FROM fato_casos f
JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
JOIN dim_notificacao n ON f.sk_notificacao = n.sk_notificacao
GROUP BY t.ano, t.mes, n.classificacao_final_desc
ORDER BY t.ano, t.mes;

-- Taxa de hospitalização por faixa etária
SELECT
    CASE
        WHEN p.idade < 18 THEN 'Menor de 18'
        WHEN p.idade BETWEEN 18 AND 59 THEN '18-59'
        ELSE '60+'
    END as faixa_etaria,
    COUNT(*) as total_casos,
    SUM(CASE WHEN n.hospitalizacao = 1 THEN 1 ELSE 0 END) as hospitalizados,
    ROUND(100.0 * SUM(CASE WHEN n.hospitalizacao = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) as taxa_hosp
FROM fato_casos f
JOIN dim_paciente p ON f.sk_paciente = p.sk_paciente
JOIN dim_notificacao n ON f.sk_notificacao = n.sk_notificacao
GROUP BY faixa_etaria
ORDER BY faixa_etaria;
```

### Passo 5: Gerar Documentação

```bash
cd transform
dbt docs generate
dbt docs serve
```

Acesse http://localhost:8080 para visualizar a documentação interativa do modelo.

## Estrutura do Projeto

```
projeto-integracao/
├── notebooks/                      # Notebooks Jupyter
│   ├── ETL.ipynb                  # Pipeline de extração e carga
│   ├── ELT.ipynb                  # Análise exploratória
│   ├── DENGBR20.csv               # Dados raw 2020 (gitignored)
│   ├── DENGBR21.csv               # Dados raw 2021 (gitignored)
│   └── DENGBR22.csv               # Dados raw 2022 (gitignored)
│
├── transform/                      # Projeto dbt
│   ├── dbt_project.yml            # Configuração do projeto dbt
│   ├── packages.yml               # Dependências dbt (dbt-utils)
│   ├── models/
│   │   ├── sources.yml            # Definição das tabelas raw
│   │   ├── staging/
│   │   │   └── stg_dengue.sql     # Staging: união e limpeza dos dados
│   │   └── marts/
│   │       ├── dim_tempo.sql      # Dimensão Tempo
│   │       ├── dim_localidade.sql # Dimensão Localidade
│   │       ├── dim_paciente.sql   # Dimensão Paciente
│   │       ├── dim_unidade_saude.sql # Dimensão Unidade de Saúde
│   │       ├── dim_notificacao.sql   # Dimensão Notificação
│   │       ├── dim_exames.sql        # Dimensão Exames
│   │       ├── dim_quadro_clinico.sql # Dimensão Quadro Clínico
│   │       └── fato_casos.sql        # Tabela Fato de Casos
│   ├── target/                    # Artefatos compilados (gitignored)
│   └── logs/                      # Logs do dbt (gitignored)
│
├── logs/                          # Logs gerais do projeto
├── analysis/                      # Análises SQL ad-hoc
├── requirements.txt               # Dependências Python
├── .gitignore                     # Arquivos ignorados pelo git
└── README.md                      # Este arquivo
```

## Modelo Dimensional

O Data Warehouse utiliza um modelo estrela (star schema) com as seguintes dimensões e fatos:

### Tabela Fato

**fato_casos**: Registra cada caso de dengue notificado
- `sk_caso` (PK): Chave surrogate do caso
- `sk_tempo` (FK): Chave para dim_tempo
- `sk_localidade` (FK): Chave para dim_localidade
- `sk_paciente` (FK): Chave para dim_paciente
- `sk_unidade_saude` (FK): Chave para dim_unidade_saude
- `sk_notificacao` (FK): Chave para dim_notificacao
- `sk_exames` (FK): Chave para dim_exames
- `sk_quadro_clinico` (FK): Chave para dim_quadro_clinico
- Métricas de negócio

### Dimensões

1. **dim_tempo**: Datas e períodos temporais
   - Ano, mês, dia, trimestre
   - Semana epidemiológica
   - Dia da semana

2. **dim_localidade**: Localização geográfica
   - Município (código IBGE)
   - UF (sigla e nome)
   - Região

3. **dim_paciente**: Dados demográficos
   - Idade
   - Sexo
   - Raça/cor
   - Escolaridade
   - Gestante (se aplicável)

4. **dim_unidade_saude**: Unidade notificadora
   - Código da unidade
   - Município e UF da unidade
   - Regional de saúde

5. **dim_notificacao**: Informações da notificação
   - Classificação final
   - Critério de confirmação
   - Evolução do caso
   - Tipo de notificação
   - Hospitalização

6. **dim_exames**: Resultados laboratoriais
   - Resultado NS1
   - Resultado IgM
   - Resultado RT-PCR
   - Sorotipo
   - Datas de coleta

7. **dim_quadro_clinico**: Sintomas e comorbidades
   - Sintomas clínicos (febre, cefaleia, mialgia, etc.)
   - Doenças pré-existentes (diabetes, hipertensão, etc.)
   - Sinais de alarme
   - Sinais de dengue grave

## Solução de Problemas

### Erro: "could not connect to server"

Verifique se o PostgreSQL está rodando:
```bash
sudo systemctl status postgresql
sudo systemctl start postgresql
```

### Erro: "FATAL: password authentication failed"

Verifique suas credenciais em `~/.dbt/profiles.yml` e certifique-se de que o usuário existe:
```bash
sudo -u postgres psql -c "\du"
```

### Erro: "relation does not exist"

Certifique-se de que você executou o notebook ETL.ipynb completamente antes de rodar `dbt run`.

### Problema: Alto uso de memória

O processamento dos CSVs requer bastante RAM. Se seu sistema tem menos de 4GB de RAM:
- Processe um ano por vez
- Use chunks menores no pandas
- Considere usar um servidor com mais recursos

### Erro: "dbt command not found"

Certifique-se de que o ambiente virtual está ativado:
```bash
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate      # Windows
```

## Referências

- [OpenDataSUS - SINAN/Dengue](https://opendatasus.saude.gov.br/dataset/arboviroses-dengue)
- [Dicionário de Dados - SINAN Dengue](https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINAN/Dengue/dic_dados_dengue.pdf)
- [Documentação dbt](https://docs.getdbt.com/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
