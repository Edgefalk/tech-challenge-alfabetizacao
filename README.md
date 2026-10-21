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
   implementado         implementado          implementado
                     silver_transformacao.py  gold_transformacao.py
                            │                        ▲
                            ▼                        │ portão: só roda se a
                   regras de qualidade ──► ops_tc.dq_results   última Silver foi aprovada

 Streaming (Pub/Sub) .......... em construção
 Qualidade (ops_tc.dq_results)  implementado (seção 3.4)
 Observabilidade .............. em construção
```

Todas as camadas são datasets dentro do mesmo projeto do Google Cloud, e cada camada lê a anterior.

<!--AJUSTAR DEPOIS-->

## 3. Estrutura do repositório

| Pasta | Conteúdo |
|---|---|
| `bronze/` | Ingestão batch: dado bruto, só acrescenta (`bronze_ingestao.py` e o notebook equivalente `bronze_ingestao.ipynb`) |
| `silver/` | Limpeza, padronização e integração das tabelas (`silver_transformacao.py` e os SQL em `silver/sql/`) |
| `gold/` | Indicadores prontos para análise (`gold_transformacao.py` e os SQL em `gold/sql/`) |
| `quality/` | Regras de qualidade de dados (`regras.py`) e funções de execução e registro em `ops.dq_results` (`dq.py`) |
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
- **Consequência para a Silver:** ela trabalha só com a ingestão mais recente de cada tabela (maior `_ingestion_timestamp`), conforme a seção 3.2.


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


## 3.2 Silver — dado limpo, padronizado e integrado

A Silver lê **só a ingestão mais recente** de cada tabela da Bronze (maior `_ingestion_timestamp`), padroniza e integra as bases. As tabelas são recriadas a cada execução (`CREATE OR REPLACE`), o que é seguro porque o histórico fica na Bronze.

Execução: `python silver/silver_transformacao.py` roda, em ordem, os arquivos de [`silver/sql/`](silver/sql/) e depois as regras de qualidade (seção 3.4).

### Tabelas

| Tabela | Linhas | Grão (chave única) | O que muda em relação à Bronze |
|---|---|---|---|
| `municipio` | 23.995 | ano + id_municipio + serie + rede | Códigos de rede e série traduzidos; UF derivada do código IBGE |
| `uf` | 145 | ano + sigla_uf + serie + rede | Códigos traduzidos; código IBGE da UF |
| `alunos` | 3.867.999 | ano + id_aluno + caderno | Todos os códigos traduzidos; coluna `avaliado`; particionada por ano e clusterizada por UF e município |
| `meta_municipio` | 37.464 | id_municipio + rede + ano_meta | Metas em formato longo (uma linha por ano de meta, 2024 a 2030); rede texto → código |
| `meta_uf` | 567 | ano_referencia + sigla_uf + rede + ano_meta | Formato longo, guardando o ano de referência de cada linha da fonte |
| `meta_brasil` | 21 | ano_referencia + rede + ano_meta | Formato longo, como `meta_uf` |
| `de_para_uf` | 27 | cod_uf | Código IBGE da UF → sigla |
| `de_para_rede_meta` | 2 | rede_texto | `Municipal` → `3`; `Pública` → `5` |

### Decisões de tratamento

- **Tipos preservados.** O que é INT64 na fonte continua INT64 e o que é STRING continua STRING (identificadores e códigos como `rede`, `serie` e `caderno` ficam texto). Comparações com códigos usam aspas (`rede = '3'`).
- **Nulo continua nulo.** Nenhuma medida recebe valor padrão. Exemplos: as proporções por nível de 2023 (nulas na fonte), a proficiência e o peso de quem não foi avaliado e as metas nulas.
- **Códigos traduzidos pelo dicionário oficial.** Código sem tradução vira `desconhecido`, e uma regra bloqueante exige que essa contagem seja zero.
- **`avaliado`** = o aluno tem peso amostral (presente e com caderno preenchido). Na fonte, `alfabetizado = 0` também aparece para quem não fez a prova; `avaliado` separa "não avaliado" de "não alfabetizado" e define o denominador correto da taxa.
- **Rede das metas.** As tabelas de meta trazem a rede como texto (`Municipal`, `Pública`) e as de resultado como código. `Pública` foi mapeada para `5` (Pública Estadual e Municipal), porque o código `6` não aparece nas tabelas de resultado.
- **Série das metas.** As metas não têm série e foram tratadas como 2º ano, que é o recorte de todos os resultados e do compromisso nacional.
- **Metas em formato longo.** As 7 colunas de meta viram linhas. Na meta municipal as metas são idênticas nas linhas de 2023 e 2024 e são deduplicadas; nas metas de UF e Brasil a linha de 2025 traz valores arredondados, por isso cada linha guarda o seu ano de referência.
- **UF dos municípios** derivada dos 2 primeiros dígitos do código IBGE.

Detalhes: [ficha de perfil da Bronze](docs/frente3/ficha_perfil_bronze.md) e [mapa de joins](docs/frente3/mapa_joins.md).

### Custo medido

Uma execução completa lê cerca de **1,0 GB**, quase todo na criação de `silver_tc.alunos` (1,009 GB); as outras tabelas leem menos de 10 MB cada (execução de 21/10/2026).


## 3.3 Gold — indicadores prontos para análise

Execução: `python gold/gold_transformacao.py`. Antes de criar qualquer tabela, o script consulta `ops.dq_results` e **só segue se a execução mais recente da Silver tiver todas as regras bloqueantes aprovadas**. Os arquivos ficam em [`gold/sql/`](gold/sql/).

| Tabela | Conteúdo | Recorte |
|---|---|---|
| `municipio_meta` | Taxa do município x meta pactuada do mesmo ano, distância à meta, status e motivo | Rede Municipal (`3`), 2º ano |
| `uf_meta` | Taxa da UF x meta do mesmo ano | Rede Pública (`5`), 2º ano |
| `brasil_meta` | Indicador nacional oficial x meta do mesmo ano | Rede Pública (`5`), 2º ano |
| `evolucao_municipio` | Taxa 2023, taxa 2024, variação e tendência por município e rede | Todas as redes publicadas |
| `validacao_cruzada` | Taxa recalculada a partir de `alunos` x taxa oficial, por município, UF e Brasil | Redes publicadas; redes agregadas reconstruídas a partir das redes dos alunos |

**Regras da Gold:**
- Resultado só é comparado com meta do **mesmo recorte de rede e série**.
- Meta usada: a **meta pactuada da fonte** (meta 2030 = 80). A leitura de universalização (100%) não é usada.
- **3 estados**: `atingiu`, `nao_atingiu` e `sem_meta`. Ausência de meta nunca vira "não atingiu"; a coluna `motivo_sem_meta` diz por quê (metas começam em 2024, sem meta pactuada ou meta nula na fonte).
- A variação no tempo só é calculada quando o município tem os dois anos. Nenhuma projeção até 2030 é feita.

**Resultados (execução de 21/10/2026):**

| Recorte | 2024 |
|---|---|
| Brasil, rede pública | 59,2 contra a meta de 59,9: não atingiu |
| UFs, rede pública | 11 atingiram, 13 não atingiram, 1 sem meta |
| Municípios, rede municipal | 2.788 atingiram, 2.444 não atingiram, 216 sem meta (96 sem meta pactuada e 120 com meta nula na fonte) |
| Evolução 2023 → 2024, rede municipal | 3.134 municípios subiram, 2.250 caíram, 12 estáveis, 104 sem os dois anos |

Em 2023 todos os recortes ficam em `sem_meta`, porque as metas começam em 2024. Em 2025, só existe resultado nacional: 66,0 contra a meta de 64,0.

### Validação cruzada

A taxa foi recalculada a partir dos 3,87 milhões de registros de alunos: soma do peso amostral dos avaliados alfabetizados dividida pela soma do peso de todos os avaliados.

| Cálculo (Brasil 2024, rede pública) | Taxa | Diferença da oficial |
|---|---|---|
| Oficial publicada | 59,2 | — |
| Recalculada (peso amostral, só avaliados) | **59,20** | **0,00** |
| Sem o peso amostral | 59,78 | +0,58 |
| Dividindo por todos os registros (inclui ausentes) | 52,21 | −6,99 |

Em 2024, 99,5% dos municípios (rede municipal) ficam a até 0,5 p.p. da taxa oficial, e as UFs ficam a no máximo 0,09 p.p. (redes municipal e pública).

**Anomalia documentada:** para o Brasil em 2023, o recálculo dá 57,46 e a tabela de meta publica 55,9. Como UFs e municípios de 2023 batem com o microdado (diferença média de 0,03 a 0,05 p.p.), a divergência está no número nacional de 2023 da tabela de meta, e não no pipeline. A causa não foi confirmada.

**Para a Fase 3:** `status_meta` e `distancia_meta` são derivadas de `taxa_alfabetizacao`. Se a taxa for o alvo de um modelo, essas colunas não podem entrar como features (vazamento de alvo).


## 3.4 Qualidade de dados

As regras ficam em [`quality/regras.py`](quality/regras.py) e o catálogo completo, com limiares e justificativas, em [`docs/frente3/regras_qualidade.md`](docs/frente3/regras_qualidade.md).

- **Bloqueantes** (34 na Silver e 6 na Gold): carga completa da Bronze, chave única, nulo em chave, código sem tradução, faixas válidas, alinhamento de proficiência e peso, coerência dos status e **taxa de match de cada join** (municipio × meta municipal e uf × meta estadual, limiar de 95% justificado no catálogo). Se alguma reprova, o script termina com código 1 e a Gold não é publicada.
- **Informativas** (5 na Silver e 3 na Gold): registram e seguem. Três reprovam de propósito com o dado atual: taxas de 2023 divergentes entre duas tabelas da fonte (R09), 18 cadernos fora da faixa (R10) e 25 UFs em vez de 27 (R11).
- **Registro persistente:** cada regra vira uma linha em `ops.dq_results` (execução, horário, tabela, regra, severidade, valor medido, limiar e status), gravada por **load job em modo append**. O histórico acumula entre execuções. Uma cópia de cada execução fica em `docs/evidencias/dq_<execucao>.csv`.
- **Sem `except` genérico:** só a exceção específica de regra bloqueante é tratada, e o processo termina com erro.

**Falha demonstrada:** `python silver/silver_transformacao.py --simular-falha` eleva o limiar de match da R04 para 99,9%. Na execução `silver_20261021_160213_34ac17` a R04 reprovou (97,32%), o resultado ficou gravado em `ops.dq_results`, o script terminou com código 1 e a Gold se recusou a rodar. Logs em [`docs/evidencias/falha_demonstrada/`](docs/evidencias/falha_demonstrada/).

Revisão contra as armadilhas do enunciado: [`docs/frente3/checklist_armadilhas.md`](docs/frente3/checklist_armadilhas.md).


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
6. Rode a Silver e depois a Gold (cada uma termina com código 1 se uma regra bloqueante reprovar):
   ```bash
   python silver/silver_transformacao.py
   python gold/gold_transformacao.py
   ```
   Para demonstrar a falha: `python silver/silver_transformacao.py --simular-falha`. Depois, rode a Silver normal de novo para liberar a Gold.

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
   !python silver/silver_transformacao.py
   !python gold/gold_transformacao.py
   ```

## 5. Decisões e trade-offs

- **Nuvem: Google Cloud.** Os dados oficiais já estão no BigQuery, então não há transferência entre nuvens. Projeto do grupo: `Tech Challenge` (ID `project-ce782257-1c9c-4b6f-87a`).
- **Fonte: leitura direta do BigQuery, sem baixar arquivos manualmente.** A leitura é feita pela API de leitura de tabelas, sem consulta SQL. Quando a Base dos Dados atualizar, basta uma nova execução. O volume lido será medido pela frente de FinOps.

## 6. Monitoramento, FinOps, qualidade e aplicação em IA

*Em construção.* Qualidade de dados: ver a seção 3.4.

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
