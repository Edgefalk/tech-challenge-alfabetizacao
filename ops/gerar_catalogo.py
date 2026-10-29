"""Gera o catálogo de dados (docs/catalogo/) a partir do INFORMATION_SCHEMA.

Só faz leitura: roda ops/sql/catalogo/catalogo_tabelas.sql, que é um SELECT em
metadados, e escreve arquivos locais. Não altera nada no BigQuery.

Uso:
    python ops/gerar_catalogo.py                     # consulta o BigQuery (precisa de credenciais)
    python ops/gerar_catalogo.py --json catalogo.json  # usa um resultado já salvo

Saídas em docs/catalogo/:
    catalogo_dados.md     visão legível, uma seção por tabela
    catalogo_tabelas.csv  uma linha por tabela
    catalogo_colunas.csv  uma linha por coluna
    catalogo.json         resultado bruto da consulta (para regerar sem acessar o BigQuery)
    ddl/<dataset>.<tabela>.sql  DDL de cada tabela, só como documentação
"""
from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
SQL = RAIZ / "ops" / "sql" / "catalogo" / "catalogo_tabelas.sql"
SAIDA = RAIZ / "docs" / "catalogo"

CAMADAS = {"bronze_tc": "Bronze", "silver_tc": "Silver", "gold_tc": "Gold", "ops_tc": "Operação"}

# Dono provável e finalidade de cada tabela. Inferido pela Frente 4 a partir do
# nome e das colunas; o dono de cada tabela deve confirmar ou corrigir.
INFO = {
    "bronze_tc.alunos": ("Frente 1", "Microdados de alunos da avaliação estadual, como vieram da fonte. Acréscimo a cada carga, com _ingestion_timestamp e _source."),
    "bronze_tc.dicionario": ("Frente 1", "Dicionário de códigos da fonte (id_tabela, nome_coluna, chave → valor)."),
    "bronze_tc.dicionario_copy": ("a confirmar", "Mesmo conteúdo de bronze_tc.dicionario (54 linhas), criada em 18/10. Parece cópia de teste."),
    "bronze_tc.eventos_alunos": ("Frente 2", "Eventos simulados de alunos recebidos pelo streaming, com origem, horário de produção, horário de recebimento e latência."),
    "bronze_tc.meta_alfabetizacao_brasil": ("Frente 1", "Taxa de alfabetização e metas 2024–2030 para o Brasil, por rede."),
    "bronze_tc.meta_alfabetizacao_municipio": ("Frente 1", "Taxa de alfabetização e metas 2024–2030 por município e rede."),
    "bronze_tc.meta_alfabetizacao_uf": ("Frente 1", "Taxa de alfabetização e metas 2024–2030 por UF e rede."),
    "bronze_tc.municipio": ("Frente 1", "Resultado por município: taxa de alfabetização, média em português e proporção de alunos por nível."),
    "bronze_tc.uf": ("Frente 1", "Resultado por UF: taxa de alfabetização, média em português e proporção de alunos por nível."),
    "silver_tc.alunos": ("Frente 3", "Alunos limpos e padronizados, com códigos traduzidos pelo dicionário. Particionada por ano e agrupada por UF e município."),
    "silver_tc.alunos_eventos": ("Frente 2", "Eventos simulados no formato da Silver de alunos, mantendo origem e horários do evento."),
    "silver_tc.de_para_rede_meta": ("Frente 3", "De-para do texto de rede usado nas metas para o código de rede."),
    "silver_tc.de_para_uf": ("Frente 3", "De-para do código IBGE da UF para a sigla."),
    "silver_tc.meta_brasil": ("Frente 3", "Metas do Brasil em formato longo (uma linha por ano de meta)."),
    "silver_tc.meta_municipio": ("Frente 3", "Metas por município em formato longo (uma linha por ano de meta)."),
    "silver_tc.meta_uf": ("Frente 3", "Metas por UF em formato longo (uma linha por ano de meta)."),
    "silver_tc.municipio": ("Frente 3", "Resultado por município padronizado, com UF e descrições de série e rede."),
    "silver_tc.uf": ("Frente 3", "Resultado por UF padronizado, com descrições de série e rede."),
    "gold_tc.brasil_meta": ("Frente 3", "Taxa x meta para o Brasil, com distância da meta e status."),
    "gold_tc.evolucao_municipio": ("Frente 3", "Evolução da taxa por município entre 2023 e 2024 (variação em p.p. e tendência)."),
    "gold_tc.indicador_por_origem": ("Frente 2 / Frente 3", "Indicador por município separado por origem (dado oficial x simulado)."),
    "gold_tc.municipio_meta": ("Frente 3", "Taxa x meta por município, com status da meta (atingiu, não atingiu, sem meta)."),
    "gold_tc.uf_meta": ("Frente 3", "Taxa x meta por UF, com status da meta."),
    "gold_tc.validacao_cruzada": ("Frente 3", "Taxa recalculada a partir dos alunos (com peso amostral) comparada à taxa oficial."),
    "ops_tc.dq_results": ("Frente 3", "Resultado de cada regra de qualidade por execução (bloqueante ou informativa)."),
    "ops_tc.eventos_rejeitados": ("Frente 2", "Eventos de streaming rejeitados, com motivo e mensagem original."),
    "ops_tc.pipeline_runs": ("Frente 4", "Uma linha por etapa por execução do pipeline: início, fim, duração, linhas, status e bytes."),
    "ops_tc.vw_custo_por_etapa": ("Frente 4", "View: bytes faturados por camada e etapa nos últimos 30 dias, a partir dos labels dos jobs."),
}


def mib(b) -> str:
    if b in (None, ""):
        return "—"
    b = int(b)
    if b < 1024:
        return f"{b} B"
    fmt = lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    if b < 1024**2:
        return f"{fmt(b / 1024)} KiB"
    return f"{fmt(b / 1024**2)} MiB"


def milhar(n) -> str:
    return "—" if n in (None, "") else f"{int(n):,}".replace(",", ".")


def expira(r) -> str:
    if r.get("exp_particao_dias"):
        return f"partições expiram em {float(r['exp_particao_dias']):.0f} dias"
    e = r.get("expira_em")
    if not e:
        return "não expira"
    data = e.split('"')[1][:10]
    a, m, d = data.split("-")
    return f"{d}/{m}/{a}"


def consultar_bigquery() -> list[dict]:
    from google.cloud import bigquery

    projeto = os.getenv("GCP_PROJECT_ID", "project-ce782257-1c9c-4b6f-87a")
    cliente = bigquery.Client(project=projeto)
    cfg = bigquery.QueryJobConfig(labels={"pipeline": "tc-fase2", "camada": "ops", "etapa": "catalogo"})
    linhas = cliente.query(SQL.read_text(encoding="utf-8"), job_config=cfg).result()
    return [dict(r.items()) for r in linhas]


def gerar(rows: list[dict], gerado_em: str) -> None:
    SAIDA.mkdir(parents=True, exist_ok=True)
    (SAIDA / "ddl").mkdir(exist_ok=True)
    for r in rows:
        r["_cols"] = json.loads(r["colunas"]) if isinstance(r["colunas"], str) else r["colunas"]

    # JSON bruto
    bruto = [{k: v for k, v in r.items() if k != "_cols"} for r in rows]
    (SAIDA / "catalogo.json").write_text(json.dumps(bruto, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    # CSV por tabela
    with open(SAIDA / "catalogo_tabelas.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["dataset", "tabela", "camada", "dono_provavel", "finalidade", "linhas", "bytes_logicos",
                    "colunas", "particao", "cluster", "expiracao", "criada_em", "modificada_em"])
        for r in rows:
            fq = f"{r['dataset']}.{r['tabela']}"
            dono, fin = INFO.get(fq, ("a confirmar", ""))
            w.writerow([r["dataset"], r["tabela"], CAMADAS.get(r["dataset"], ""), dono, fin, r["linhas"],
                        r["bytes_logicos"], len(r["_cols"]), r.get("coluna_particao") or "",
                        r.get("colunas_cluster") or "", expira(r), r["criada_em"], r["modificada_em"]])

    # CSV por coluna
    with open(SAIDA / "catalogo_colunas.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["dataset", "tabela", "posicao", "coluna", "tipo", "obrigatoria", "descricao"])
        for r in rows:
            for c in r["_cols"]:
                w.writerow([r["dataset"], r["tabela"], c["pos"], c["nome"], c["tipo"],
                            "sim" if c["nulo"] == "NO" else "não", c.get("descricao") or ""])

    # DDL de documentação
    for r in rows:
        cab = ("-- DDL de " + f"{r['dataset']}.{r['tabela']}" + ", extraído do INFORMATION_SCHEMA em " + gerado_em + ".\n"
               "-- SOMENTE DOCUMENTAÇÃO. NÃO EXECUTAR: a tabela já existe e pertence ao dono indicado no catálogo.\n\n")
        (SAIDA / "ddl" / f"{r['dataset']}.{r['tabela']}.sql").write_text(cab + r["ddl"].rstrip() + "\n", encoding="utf-8")

    # Markdown
    tot = {}
    for r in rows:
        t = tot.setdefault(r["dataset"], [0, 0, 0])
        t[0] += 1
        t[1] += int(r["linhas"] or 0)
        t[2] += int(r["bytes_logicos"] or 0)
    md = [
        "# Catálogo de dados",
        "",
        f"**Projeto:** `project-ce782257-1c9c-4b6f-87a` · **Região:** `southamerica-east1` · **Gerado em:** {gerado_em}",
        "",
        "Gerado por `ops/gerar_catalogo.py` a partir de `ops/sql/catalogo/catalogo_tabelas.sql`. A consulta só lê o INFORMATION_SCHEMA.",
        "",
        "O **dono provável** e a **finalidade** de cada tabela foram inferidos pela Frente 4 a partir do nome e das colunas. "
        "O dono de cada tabela deve confirmar ou corrigir esses campos em `INFO`, dentro de `ops/gerar_catalogo.py`.",
        "",
        "## Resumo por dataset",
        "",
        "| Dataset | Camada | Tabelas | Linhas | Bytes lógicos |",
        "|---|---|---:|---:|---:|",
    ]
    for ds in ["bronze_tc", "silver_tc", "gold_tc", "ops_tc"]:
        if ds in tot:
            n, l, b = tot[ds]
            md.append(f"| `{ds}` | {CAMADAS[ds]} | {n} | {milhar(l)} | {mib(b)} |")
    n = sum(v[0] for v in tot.values()); l = sum(v[1] for v in tot.values()); b = sum(v[2] for v in tot.values())
    md += [f"| **Total** | | **{n}** | **{milhar(l)}** | **{mib(b)}** |", "",
           "## Tabelas", "",
           "| Tabela | Dono provável | Linhas | Bytes lógicos | Partição / cluster | Expira |",
           "|---|---|---:|---:|---|---|"]
    for r in rows:
        fq = f"{r['dataset']}.{r['tabela']}"
        dono = INFO.get(fq, ("a confirmar", ""))[0]
        pc = " / ".join(x for x in [r.get("coluna_particao") or "", r.get("colunas_cluster") or ""] if x) or "—"
        md.append(f"| [`{fq}`](#{r['dataset']}{r['tabela']}) | {dono} | {milhar(r['linhas'])} | {mib(r['bytes_logicos'])} | {pc} | {expira(r)} |")
    md.append("")
    for r in rows:
        fq = f"{r['dataset']}.{r['tabela']}"
        dono, fin = INFO.get(fq, ("a confirmar", ""))
        md += [f"### `{fq}`", "", fin, "",
               f"- **Camada:** {CAMADAS.get(r['dataset'], '')} · **Dono provável:** {dono}",
               f"- **Linhas:** {milhar(r['linhas'])} · **Bytes lógicos:** {mib(r['bytes_logicos'])}",
               f"- **Criada em:** {r['criada_em']} · **Última modificação:** {r['modificada_em']} · **Expira:** {expira(r)}"]
        if r.get("coluna_particao") or r.get("colunas_cluster"):
            md.append(f"- **Partição:** {r.get('coluna_particao') or '—'} · **Cluster:** {r.get('colunas_cluster') or '—'}")
        md += [f"- **DDL:** [`ddl/{fq}.sql`](ddl/{fq}.sql)", "",
               "| # | Coluna | Tipo | Obrigatória | Descrição |", "|---:|---|---|---|---|"]
        for c in r["_cols"]:
            md.append(f"| {c['pos']} | `{c['nome']}` | {c['tipo']} | {'sim' if c['nulo'] == 'NO' else 'não'} | {c.get('descricao') or ''} |")
        md.append("")
    (SAIDA / "catalogo_dados.md").write_text("\n".join(md), encoding="utf-8")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--json", help="arquivo com o resultado da consulta (lista de objetos)")
    p.add_argument("--gerado-em", default=datetime.now(timezone.utc).astimezone().strftime("%d/%m/%Y %H:%M"))
    a = p.parse_args()
    rows = json.loads(Path(a.json).read_text(encoding="utf-8")) if a.json else consultar_bigquery()
    gerar(rows, a.gerado_em)
    print(f"Catálogo gerado em {SAIDA} ({len(rows)} tabelas).")


if __name__ == "__main__":
    main()
