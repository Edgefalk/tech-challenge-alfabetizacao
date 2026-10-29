"""Mede linhas e bytes de todas as tabelas das camadas. Não executa consultas pagas.

Para cada tabela:
  - linhas e bytes lógicos vêm dos metadados (get_table), sem custo;
  - os bytes de um SELECT * vêm de um dry run, que é gratuito e não cria job.

Uso:
    python ops/medir_tabelas.py

Saída: docs/finops/medicoes_dry_run.csv
"""
import csv
import os
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

PROJETO = os.getenv("GCP_PROJECT_ID", "project-ce782257-1c9c-4b6f-87a")
DATASETS = os.getenv("BQ_DATASETS_MEDIR", "bronze_tc,silver_tc,gold_tc,ops_tc").split(",")
SAIDA = Path(__file__).resolve().parents[1] / "docs" / "finops" / "medicoes_dry_run.csv"


def main() -> None:
    cliente = bigquery.Client(project=PROJETO)
    cfg = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    agora = datetime.now(timezone.utc).isoformat(timespec="seconds")
    SAIDA.parent.mkdir(parents=True, exist_ok=True)
    with open(SAIDA, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["medido_em_utc", "dataset", "tabela", "linhas", "bytes_logicos", "bytes_select_star", "mib_select_star"])
        for ds in DATASETS:
            for t in cliente.list_tables(f"{PROJETO}.{ds}"):
                tid = f"{PROJETO}.{ds}.{t.table_id}"
                info = cliente.get_table(tid)
                dry = None
                if info.table_type == "TABLE":
                    dry = cliente.query(f"SELECT * FROM `{tid}`", job_config=cfg).total_bytes_processed
                w.writerow([agora, ds, t.table_id, info.num_rows, info.num_bytes, dry,
                            round(dry / 1024**2, 2) if dry is not None else ""])
    print(f"Medição gravada em {SAIDA}")


if __name__ == "__main__":
    main()
