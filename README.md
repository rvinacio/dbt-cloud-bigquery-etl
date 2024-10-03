
# DBT Cloud BigQuery ETL Project

## Descrição do Projeto

Este projeto tem como objetivo realizar o processo de ETL (Extração, Transformação e Carga) utilizando o **dbt** (Data Build Tool) com o **Google BigQuery**. Ele processa, transforma e consolida dados de faturas de cartão de crédito, gerando insights financeiros e estruturando as informações para relatórios e análises.

## Estrutura do Projeto

A estrutura de diretórios do projeto segue o padrão do dbt:

```
dbt_cloud_bigquery_etl_project/
│
├── analyses/             # Consultas ad hoc ou temporárias
├── macros/               # Macros SQL para reaproveitamento de código
├── models/               # Modelos de transformação de dados (SQL)
│   ├── financeiro/       # Modelos relacionados a dados financeiros
│   │   ├── cartao_xp/    # Modelos específicos para cartão de crédito XP
│   └── source.yml        # Definições de fontes de dados (BigQuery)
├── seeds/                # Dados estáticos
├── snapshots/            # Histórico de tabelas com snapshots
├── tests/                # Testes de qualidade de dados
├── .gitignore            # Arquivos e pastas ignorados no controle de versão
├── dbt_project.yml       # Configurações gerais do projeto dbt
└── README.md             # Documentação do projeto
```

## Fonte de Dados

Este projeto se conecta ao **Google BigQuery** para realizar o ETL. As fontes de dados são tabelas armazenadas no BigQuery, conforme definidas no arquivo `source.yml`. O arquivo `source.yml` mapeia as tabelas de origem para os dados brutos que são processados, agregados e transformados pelo dbt.

### Principais Tabelas

- **fato_cartao_xp**: Dados brutos das transações de cartão de crédito.
- **tb_tempo**: Tabela de dimensão de tempo para consultas agregadas.
- **estabelecimentos**: Informações sobre os estabelecimentos comerciais.

## Macros

As macros são utilizadas para reaproveitar lógicas de transformação de dados ao longo do projeto. Alguns exemplos incluem:

- `gerador_id.sql`: Geração de identificadores únicos para movimentações financeiras.
- `timestamp_brasilia.sql`: Converte timestamps para o fuso horário de Brasília.

## Como Executar o Projeto

### Pré-requisitos

- Conta no **Google Cloud Platform** com permissões para acessar o **BigQuery**.
- Arquivo de credenciais JSON da conta de serviço com permissões para leitura e escrita no BigQuery.
- Python 3.9+ com ambiente virtual configurado.

### Configuração

1. **Instalar as dependências**:
   Certifique-se de que o dbt e o suporte ao BigQuery estão instalados:

   ```bash
   pip install dbt-bigquery
   ```

2. **Configurar o arquivo de credenciais**:
   Defina a variável de ambiente para apontar para o seu arquivo de credenciais JSON:

   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/keyfile.json
   ```

3. **Configurar o perfil `profiles.yml`**:
   No diretório `~/.dbt/`, adicione o arquivo `profiles.yml` com as seguintes configurações:

   ```yaml
   dbt_cloud_bigquery_etl_project:
     target: dev
     outputs:
       dev:
         type: bigquery
         method: service-account
         project: rafael-data
         dataset: dbt_cloud
         keyfile: /path/to/your/keyfile.json
         location: US
         threads: 4
   ```

### Executando o Projeto

Para rodar o projeto e materializar os modelos no BigQuery:

1. **Compilar e executar todos os modelos**:

   ```bash
   dbt run
   ```

2. **Testar os modelos**:

   ```bash
   dbt test
   ```

---

**Observação**: Este projeto está focado apenas em documentar o pipeline de ETL para o BigQuery usando o dbt. Ele pode ser expandido com mais modelos, macros e análises conforme necessário.