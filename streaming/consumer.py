#!/usr/bin/env python3
"""Tech Challenge Fase 2 - Streaming: consumidor em micro-lotes.

Lê a assinatura do Pub/Sub, classifica cada mensagem (válido, duplicado ou rejeitado), grava por LOAD JOB
(append) em bronze.eventos_alunos e ops.eventos_rejeitados e só então confirma (ack) as mensagens.
Se a gravação falhar, o erro sobe e as mensagens não são confirmadas: o Pub/Sub as entrega de novo.

Uso (a partir da raiz do repositório; inicie ANTES do producer para medir a latência de verdade):
    python streaming/consumer.py
    python streaming/consumer.py --max-segundos 600 --ocioso 60
"""
import argparse
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from google.api_core.exceptions import DeadlineExceeded  # noqa: E402
from google.cloud import bigquery, pubsub_v1  # noqa: E402

from quality.dq import carregar_config  # noqa: E402
from streaming.config import assinatura  # noqa: E402
from streaming.validacao import CAMPOS_EVENTOS, classificar_mensagem  # noqa: E402

# A assinatura confirma em 10 s por padrão e uma gravação no BigQuery leva alguns segundos:
# pedimos mais prazo antes de gravar para o Pub/Sub não reentregar a mesma mensagem.
PRAZO_ACK_SEGUNDOS = 60

TIPOS = {  # tipo de cada coluna de bronze.eventos_alunos (códigos como texto, como na Silver)
    "horario_producao": "TIMESTAMP", "horario_recebimento": "TIMESTAMP", "latencia_segundos": "FLOAT64",
    "ano": "INT64", "avaliado": "BOOL", "proficiencia": "FLOAT64", "peso_aluno": "FLOAT64",
}
SCHEMA_EVENTOS = [bigquery.SchemaField(c, TIPOS.get(c, "STRING")) for c in CAMPOS_EVENTOS]
SCHEMA_REJEITADOS = [
    bigquery.SchemaField("id_evento", "STRING"),
    bigquery.SchemaField("horario_recebimento", "TIMESTAMP"),
    bigquery.SchemaField("motivo_rejeicao", "STRING"),
    bigquery.SchemaField("mensagem_bruta", "STRING"),
]


def gravar(bq, linhas, tabela, schema):
    """Load job em append (o sandbox bloqueia streaming insert). Se falhar, levanta erro de propósito."""
    config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", schema=schema)
    job = bq.load_table_from_json(linhas, tabela, job_config=config)
    job.result()
    print(f"{len(linhas)} linhas gravadas em {tabela} (job {job.job_id})")


def consumir_lote(subscriber, caminho, bq, tabelas, vistos, max_mensagens=10, espera=15) -> int:
    try:
        resposta = subscriber.pull(
            request={"subscription": caminho, "max_messages": max_mensagens}, timeout=espera
        )
    except DeadlineExceeded:  # fila vazia durante a espera: não é erro
        return 0
    mensagens = resposta.received_messages
    if not mensagens:
        return 0

    ack_ids = [m.ack_id for m in mensagens]
    subscriber.modify_ack_deadline(
        request={"subscription": caminho, "ack_ids": ack_ids, "ack_deadline_seconds": PRAZO_ACK_SEGUNDOS}
    )

    validas, rejeitadas, duplicadas = [], [], 0
    for m in mensagens:
        categoria, linha = classificar_mensagem(m.message.data.decode("utf-8", "replace"), vistos)
        if categoria == "valido":
            validas.append(linha)
        elif categoria == "rejeitado":
            rejeitadas.append(linha)
        else:
            duplicadas += 1

    # grava primeiro, confirma depois
    if validas:
        gravar(bq, validas, tabelas["eventos"], SCHEMA_EVENTOS)
    if rejeitadas:
        gravar(bq, rejeitadas, tabelas["rejeitados"], SCHEMA_REJEITADOS)
    subscriber.acknowledge(request={"subscription": caminho, "ack_ids": ack_ids})

    print(f"Lote: {len(mensagens)} recebidas | {len(validas)} válidas | "
          f"{duplicadas} duplicadas ignoradas | {len(rejeitadas)} rejeitadas")
    if validas:
        lat = [v["latencia_segundos"] for v in validas]
        print(f"Latência (s): mín {min(lat):.2f} | média {sum(lat) / len(lat):.2f} | máx {max(lat):.2f}")
    return len(mensagens)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Consumer de eventos em micro-lotes.")
    parser.add_argument("--max-segundos", type=int, default=300, help="trava de segurança (padrão 300)")
    parser.add_argument("--ocioso", type=int, default=45, help="para após N s sem mensagens novas (padrão 45)")
    args = parser.parse_args(argv)

    cfg = carregar_config()
    subscriber = pubsub_v1.SubscriberClient()
    caminho = subscriber.subscription_path(cfg["projeto"], assinatura())
    bq = bigquery.Client(project=cfg["projeto"])
    tabelas = {
        "eventos": f"{cfg['projeto']}.{cfg['bronze']}.eventos_alunos",
        "rejeitados": f"{cfg['projeto']}.{cfg['ops']}.eventos_rejeitados",
    }

    vistos = set()  # dedup em memória; a Silver (alunos_eventos) deduplica de novo por id_evento
    inicio = ultima = time.time()
    recebeu, total = False, 0
    print(f"Escutando {caminho} (para após {args.ocioso}s sem mensagens ou em {args.max_segundos}s)...")
    while time.time() - inicio < args.max_segundos:
        n = consumir_lote(subscriber, caminho, bq, tabelas, vistos)
        if n:
            recebeu, ultima, total = True, time.time(), total + n
        elif recebeu and time.time() - ultima >= args.ocioso:
            break
    print(f"Fim da escuta. Mensagens recebidas no total: {total}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
