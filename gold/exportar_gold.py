#!/usr/bin/env python3
"""Tech Challenge Fase 2 - Exporta a camada Gold para arquivos Parquet (guardada para a Fase 3).

As tabelas da Gold vivem no BigQuery e podem expirar (o sandbox expira tabelas em 60 dias).
Este script copia cada tabela de gold_tc para gold/export/<tabela>.parquet e grava
gold/export/manifesto.csv com linhas, horário da cópia e última modificação da tabela de origem.

A leitura usa a API de leitura de tabelas (list_rows), sem consulta SQL: não gera bytes cobrados.

Uso (a partir da raiz do repositório, depois da Gold):
    python gold/exportar_gold.py

Código de saída: 0 = todas as tabelas exportadas com a contagem conferida | 1 = alguma contagem divergiu
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from quality.dq import carregar_config, conectar  # noqa: E402

PASTA_EXPORT = RAIZ / "gold" / "export"


def main() -> int:
    cfg = carregar_config()
    client = conectar(cfg)
    PASTA_EXPORT.mkdir(parents=True, exist_ok=True)
    exportado_em = datetime.now(timezone.utc)

    manifesto = []
    divergencias = []
    for item in sorted(client.list_tables(f"{cfg['projeto']}.{cfg['gold']}"), key=lambda t: t.table_id):
        tabela = client.get_table(item.reference)
        if tabela.table_type != "TABLE":
            continue
        df = client.list_rows(tabela).to_dataframe()
        arquivo = PASTA_EXPORT / f"{tabela.table_id}.parquet"
        df.to_parquet(arquivo, index=False)

        conferido = len(df) == tabela.num_rows
        if not conferido:
            divergencias.append(tabela.table_id)
        manifesto.append({
            "tabela": f"{cfg['gold']}.{tabela.table_id}",
            "arquivo": f"gold/export/{arquivo.name}",
            "linhas_bigquery": tabela.num_rows,
            "linhas_exportadas": len(df),
            "colunas": len(df.columns),
            "tabela_modificada_em": tabela.modified.isoformat(),
            "exportado_em": exportado_em.isoformat(),
        })
        status = "ok" if conferido else "DIVERGENTE"
        print(f"   {tabela.table_id}: {len(df)} linhas, {len(df.columns)} colunas -> {arquivo.name} ({status})")

    pd.DataFrame(manifesto).to_csv(PASTA_EXPORT / "manifesto.csv", index=False)
    print(f"Manifesto: gold/export/manifesto.csv ({len(manifesto)} tabelas)")

    if divergencias:
        print(f"ERRO: contagem divergente em {', '.join(divergencias)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
