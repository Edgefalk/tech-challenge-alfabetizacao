# Tech Challenge · Fase 2 — Pipeline híbrido para análise da alfabetização no Brasil


## 1. Contexto do problema

O Compromisso Nacional Criança Alfabetizada quer que todas as crianças estejam alfabetizadas até o fim do 2º ano do ensino fundamental. O INEP define que um aluno está alfabetizado a partir de **743 pontos** na escala do Saeb, e o indicador é o **percentual de alunos que atingem esse patamar**.

Este projeto constrói, em nuvem, um pipeline que integra as tabelas do Indicador Criança Alfabetizada e entrega uma camada analítica confiável, em arquitetura medalhão (Bronze, Silver e Gold), com uma parte em lote (batch) e outra com eventos simulados (streaming).

**Fonte de dados:** [Base dos Dados](https://basedosdados.org/), dataset `br_inep_avaliacao_alfabetizacao`, disponível no BigQuery.

## 2. Arquitetura

```
 Base dos Dados (BigQuery, público)
          │
          │  bronze/bronze_ingestao.py
          ▼
 ┌───────────────┐     ┌───────────────┐     ┌───────────────┐
 │    BRONZE     │ ──► │    SILVER     │ ──► │     GOLD      │
 │   bronze_tc   │     │   silver_tc   │     │    gold_tc    │
 └───────────────┘     └───────────────┘     └───────────────┘
   implementado         em construção         em construção

 Streaming (Pub/Sub) .......... em construção
 Qualidade e observabilidade .. em construção (dataset de ops)
```

Todas as camadas são datasets dentro do mesmo projeto do Google Cloud, e cada camada lê a anterior.

<!--AJUSTAR DEPOIS-->

## 3. Estrutura do repositório

| Pasta | Conteúdo |
|---|---|
| `bronze/` | Ingestão batch: dado bruto, só acrescenta (`bronze_ingestao.py` e o notebook equivalente `bronze_ingestao.ipynb`) |
| `silver/` | Limpeza, padronização e integração das tabelas |
| `gold/` | Indicadores prontos para análise |
| `quality/` | Regras de qualidade de dados |
| `ops/` | Métricas de execução e monitoramento |
| `docs/` | Documentação e evidências de execução (`evidencias/`) |

Arquivos na raiz: `README.md`, `requirements.txt`, `.env.example` (modelo de configuração) e `.gitignore`.


## 3.1 Bronze — dado bruto

A Bronze guarda os dados **exatamente como vieram da fonte**: sem filtro, sem limpeza e sem mudança de nomes ou tipos de colunas. Ela é o histórico do pipeline: se uma etapa seguinte der errado, reprocessa-se a partir dela sem precisar buscar a fonte de novo.

### O que entra na Bronze

Origem: `basedosdados.br_inep_avaliacao_alfabetizacao` (BigQuery público da Base dos Dados).
Destino: `<projeto>.bronze_tc.<tabela>`, com o mesmo nome da tabela de origem.

| Tabela | Conteúdo |
|---|---|
| `alunos` | Microdado individual: proficiência, presença, preenchimento do caderno, alfabetizado, peso amostral |
| `municipio` | Indicador por município, ano, série e rede |
| `uf` | Indicador por UF, ano, série e rede |
| `meta_alfabetizacao_brasil` | Trajetória nacional de metas (2024–2030) |
| `meta_alfabetizacao_uf` | Trajetória estadual de metas |
| `meta_alfabetizacao_municipio` | Trajetória municipal de metas |
| `dicionario` | Tradução oficial dos códigos (rede, série, presença etc.) |

### Colunas de controle

| Coluna | Tipo | Descrição |
|---|---|---|
| `_ingestion_timestamp` | TIMESTAMP | Data e hora (UTC) da execução. É igual para todas as tabelas da mesma execução |
| `_source` | STRING | Tabela de origem completa (por exemplo, `basedosdados.br_inep_avaliacao_alfabetizacao.municipio`) |

### Política de escrita

- **Só acrescenta.** Cada execução usa `WRITE_APPEND`.
- **Quando a fonte oficial for atualizada:** uma nova execução acrescenta uma nova cópia completa, com um novo `_ingestion_timestamp`. As cópias anteriores continuam na tabela.
- **Linhas repetidas entre execuções são esperadas.** Rodar duas vezes sem que a fonte mude dobra a contagem de linhas. A Bronze não remove duplicatas.
- **Consequência para a Silver:** ela deve trabalhar com a ingestão mais recente de cada tabela (maior `_ingestion_timestamp`). Essa regra será implementada na Silver.


### O que o script faz, por tabela

1. Lê o tamanho da origem. Se passar do `LIMITE_GB`, pula a tabela e avisa.
2. Baixa a tabela inteira, sem filtro e sem transformação.
3. Mostra `shape` e `info()`.
4. Acrescenta `_ingestion_timestamp` e `_source`.
5. Carrega na Bronze com `WRITE_APPEND`, com os mesmos tipos da origem.
6. Confere a contagem: linhas da origem = linhas baixadas = linhas acrescentadas. Se divergir, o script termina com erro.

### Evidências de execução

Cada execução do script grava um CSV em [`docs/evidencias/`](docs/evidencias/) com a contagem de linhas antes e depois, o ID do job do BigQuery e a duração de cada tabela. O notebook gera o mesmo resultado no arquivo `evidencias_bronze.csv` (dentro da sessão do Colab, e deve ser baixado e guardado em `docs/evidencias/`).


### Limitações

- A ingestão passa pela memória de quem roda o script. Tabelas acima de `LIMITE_GB` são puladas.
- A Bronze não é particionada.


## 3.2 Silver - XXX

<!-- Editar aqui -->


## 3.3 Gold - XXX

<!-- Editar aqui -->


## 4. Como reproduzir

### Pré-requisitos

- Python 3.10 ou superior.
- Um projeto no Google Cloud com o BigQuery ativo e os datasets das camadas já criados (nomes em `.env.example`).
- Permissão no projeto para criar tabelas e rodar jobs no BigQuery.

### Passo a passo (computador local)

1. Clone o repositório e entre na pasta:
   ```bash
   git clone https://github.com/lu-feijao/tech-challenge-alfabetizacao.git
   cd tech-challenge-alfabetizacao
   ```
2. Crie o ambiente virtual e instale as dependências:
   ```bash
   python -m venv .venv
   source .venv/bin/activate        # no Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```
3. Crie o arquivo de configuração a partir do modelo:
   ```bash
   cp .env.example .env             # no Windows: copy .env.example .env
   ```
   O modelo já vem com os IDs do projeto do grupo. Para rodar em outro projeto, edite o `.env` e troque o `GCP_PROJECT_ID` (e os datasets, se tiverem outros nomes).
4. Faça o login no Google Cloud (nenhuma chave fica no repositório):
   ```bash
   gcloud auth application-default login
   ```
5. Rode a ingestão da Bronze:
   ```bash
   python bronze/bronze_ingestao.py
   ```

### Passo a passo (Google Colab)

1. Em um notebook novo, clone o repositório e instale as dependências:
   ```python
   !git clone https://github.com/lu-feijao/tech-challenge-alfabetizacao.git
   %cd tech-challenge-alfabetizacao
   !pip install -q -r requirements.txt
   ```
2. Faça o login e crie o `.env` (o modelo já vem com os IDs do projeto do grupo; para outro projeto, edite o `GCP_PROJECT_ID` no `.env`):
   ```python
   from google.colab import auth
   auth.authenticate_user()
   !cp .env.example .env
   ```
3. Rode:
   ```python
   !python bronze/bronze_ingestao.py
   ```

## 5. Decisões e trade-offs

- **Nuvem: Google Cloud.** Os dados oficiais já estão no BigQuery, então não há transferência entre nuvens. Projeto do grupo: `Tech Challenge` (ID `project-ce782257-1c9c-4b6f-87a`).
- **Fonte: leitura direta do BigQuery, sem baixar arquivos manualmente.** A leitura é feita pela API de leitura de tabelas, sem consulta SQL. Quando a Base dos Dados atualizar, basta uma nova execução. O volume lido será medido pela frente de FinOps.

## 6. Monitoramento, FinOps, qualidade e aplicação em IA

*Em construção.* 

<!--Estas seções serão preenchidas pelas frentes responsáveis, somente com o que existir de fato no repositório.-->

## 7. Equipe

| Frente | Responsável |
|---|---|
| 1 · Plataforma + Batch | Luana |
| 2 · Streaming | Cauã |
| 3 · Silver/Gold + Qualidade | Ruan |
| 4 · Observabilidade + FinOps + Docs | Edgard |

## 8. Vídeo executivo

*Link a ser incluído.*
