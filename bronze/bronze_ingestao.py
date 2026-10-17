#!/usr/bin/env python3
"""Tech Challenge Fase 2 - Indicador Criança Alfabetizada.
Ingestão Bronze: download dos dados brutos (versão em script do bronze_ingestao.ipynb).

Baixa as tabelas oficiais da Base dos Dados exatamente como vêm (sem filtro e sem
transformação) e ACRESCENTA nas tabelas da camada Bronze, no dataset bronze_tc do
BigQuery. Cada execução acrescenta linhas (WRITE_APPEND): nada é sobrescrito nem apagado.

Uso (a partir da raiz do repositório):
    python bronze/bronze_ingestao.py                         # todas as tabelas
    python bronze/bronze_ingestao.py --tabelas uf municipio  # só algumas

Configuração: arquivo .env na raiz (modelo em .env.example).
Login (nenhuma chave no código):
    - Local:  gcloud auth application-default login
    - Colab:  from google.colab import auth; auth.authenticate_user()

Código de saída: 0 = ok | 1 = contagem de linhas divergente | 2 = tabela pulada
"""
import argparse
import gc
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery

# ============================================================
# ETAPA 1 - IMPORTS E CONFIGURAÇÕES GLOBAIS
# ============================================================
RAIZ = Path(__file__).resolve().parents[1]
PASTA_EVIDENCIAS = RAIZ / "docs" / "evidencias"

TABELAS = [
    "uf",
    "municipio",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
    "dicionario",
    "alunos",
]


def carregar_config():
    """Lê as configurações do .env (ou das variáveis de ambiente)."""
    load_dotenv(RAIZ / ".env")
    project_id = os.getenv("GCP_PROJECT_ID", "").strip()
    bronze_dataset = os.getenv("BQ_BRONZE_DATASET", "").strip()
    if not project_id or not bronze_dataset:
        sys.exit(
            "Configuração incompleta. Copie .env.example para .env e confira "
            "GCP_PROJECT_ID e BQ_BRONZE_DATASET."
        )
    dataset_origem = os.getenv("SOURCE_DATASET", "basedosdados.br_inep_avaliacao_alfabetizacao").strip()
    limite_gb = float(os.getenv("LIMITE_GB", "2"))  # Trava de segurança por tabela
    return project_id, bronze_dataset, dataset_origem, limite_gb


# ============================================================
# ETAPA 3 - FUNÇÃO DE INGESTÃO
# ============================================================
def ingerir_bronze(client, tabela, project_id, bronze_dataset, dataset_origem, limite_gb, ingestion_ts):
    """
    Baixa uma tabela da origem sem filtro e sem transformação e acrescenta na Bronze do BigQuery.

    Returns:
        dict: Status da ingestão, contagem de linhas, checagem de contagem e
        ID do job de carga no BigQuery.
    """
    ref = f"{dataset_origem}.{tabela}"
    destino = f"{project_id}.{bronze_dataset}.{tabela}"
    info_origem = client.get_table(ref)
    gb = info_origem.num_bytes / 1e9
    print(f"== {tabela} ==")
    print(f"   origem: {info_origem.num_rows} linhas, {gb:.3f} GB")

    # Trava de segurança: tabela grande demais pode derrubar a memória da máquina
    if gb > limite_gb:
        print(f"   PULADA: acima do limite de {limite_gb} GB")
        return {"tabela": tabela, "status": "PULADA", "linhas_origem": info_origem.num_rows}

    df = client.list_rows(ref).to_dataframe()

    # ── Checagens ──
    print(f"   shape: {df.shape}")
    df.info()

    # ── Metadados de ingestão (colunas novas; os dados originais não mudam) ──
    df["_ingestion_timestamp"] = pd.Timestamp(ingestion_ts)
    df["_source"] = ref

    # ── Schema: mesmos nomes e tipos da origem + as 2 colunas de controle ──
    schema = [bigquery.SchemaField(c.name, c.field_type) for c in info_origem.schema]
    schema += [
        bigquery.SchemaField("_ingestion_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("_source", "STRING"),
    ]

    # ── Contagem antes da carga ──
    try:
        antes = client.get_table(destino).num_rows
    except NotFound:
        antes = 0

    # ── Carga na Bronze (acrescenta; cria a tabela se ainda não existir) ──
    # Nunca usar WRITE_TRUNCATE nem CREATE OR REPLACE: apagaria o histórico.
    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(df, destino, job_config=job_config)
    job.result()

    depois = client.get_table(destino).num_rows
    linhas_baixadas = len(df)
    del df
    gc.collect()  # libera memória antes da próxima tabela

    return {
        "tabela": tabela,
        "status": "OK",
        "linhas_origem": info_origem.num_rows,
        "linhas_baixadas": linhas_baixadas,
        "linhas_antes": antes,
        "linhas_depois": depois,
        "linhas_acrescentadas": depois - antes,
        "checagem": info_origem.num_rows == linhas_baixadas == depois - antes,
        "job_id": job.job_id,
        "duracao_s": round((job.ended - job.started).total_seconds(), 1),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="Ingestão Bronze (só acrescenta) no BigQuery.")
    parser.add_argument(
        "--tabelas",
        nargs="+",
        choices=TABELAS,
        default=TABELAS,
        metavar="TABELA",
        help=f"tabelas a ingerir (padrão: todas). Opções: {', '.join(TABELAS)}",
    )
    args = parser.parse_args(argv)
    project_id, bronze_dataset, dataset_origem, limite_gb = carregar_config()

    # ============================================================
    # ETAPA 2 - CONEXÃO COM O BIGQUERY
    # ============================================================
    try:
        client = bigquery.Client(project=project_id)
    except DefaultCredentialsError:
        sys.exit(
            "Sem credenciais do Google. Local: rode 'gcloud auth application-default login'. "
            "Colab: rode 'from google.colab import auth; auth.authenticate_user()' antes."
        )
    try:
        bronze = client.get_dataset(f"{project_id}.{bronze_dataset}")
    except NotFound:
        sys.exit(
            f"Dataset {project_id}.{bronze_dataset} não encontrado. "
            "Crie o dataset no BigQuery ou corrija BQ_BRONZE_DATASET no .env."
        )
    print("Conectado ao projeto:", client.project)
    print("Dataset Bronze:", bronze.dataset_id, "| região:", bronze.location)

    # ============================================================
    # ETAPA 4 - EXECUÇÃO
    # ============================================================
    ingestion_ts = datetime.now(timezone.utc)
    print("Timestamp de ingestão:", ingestion_ts.isoformat(), "\n")
    resultados = []
    try:
        for tabela in args.tabelas:
            resultados.append(
                ingerir_bronze(client, tabela, project_id, bronze_dataset, dataset_origem, limite_gb, ingestion_ts)
            )
            print()
    finally:
        # Mesmo se uma tabela falhar, guarda o que já foi carregado
        arquivo = None
        if resultados:
            PASTA_EVIDENCIAS.mkdir(parents=True, exist_ok=True)
            arquivo = PASTA_EVIDENCIAS / f"evidencias_bronze_{ingestion_ts:%Y%m%d_%H%M%S}.csv"
            pd.DataFrame(resultados).to_csv(arquivo, index=False)

    # ============================================================
    # ETAPA 5 - CONFERÊNCIA
    # ============================================================
    df_resultados = pd.DataFrame(resultados)
    print(df_resultados.to_string(index=False))
    if arquivo:
        print(f"\nEvidências salvas em: {arquivo}")

    print("\nTabelas hoje na Bronze:")
    for t in client.list_tables(f"{project_id}.{bronze_dataset}"):
        print(f"  {t.table_id}: {client.get_table(t).num_rows} linhas")

    if "checagem" in df_resultados.columns:  # a coluna só existe se alguma tabela foi carregada
        divergentes = list(df_resultados.loc[df_resultados["checagem"] == False, "tabela"])  # noqa: E712
        if divergentes:
            print(f"\nERRO: contagem de linhas diverge em: {divergentes}")
            return 1
    pulados = list(df_resultados.loc[df_resultados["status"] == "PULADA", "tabela"])
    if pulados:
        print(f"\nAtenção, tabelas puladas: {pulados}")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
