#!/usr/bin/env python3
"""Tech Challenge Fase 2 - Streaming: simulador (producer).

Publica eventos simulados de alunos no tópico do Pub/Sub. Por padrão envia 5 eventos válidos, 1 duplicado
(o mesmo id_evento duas vezes) e 1 inválido (sem id_municipio), para demonstrar a dedup e a rejeição.

Uso (a partir da raiz do repositório; o consumer deve estar escutando em outro terminal ou sessão):
    python streaming/producer.py
    python streaming/producer.py --qtd 10 --quebrado
"""
import argparse
import json
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from google.cloud import pubsub_v1  # noqa: E402

from quality.dq import carregar_config  # noqa: E402
from streaming.config import topico  # noqa: E402
from streaming.eventos import MENSAGEM_QUEBRADA, gerar_evento, gerar_evento_rejeitado  # noqa: E402


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Producer de eventos simulados.")
    parser.add_argument("--qtd", type=int, default=5, help="eventos válidos (padrão 5)")
    parser.add_argument("--pausa", type=float, default=2.0, help="segundos entre eventos (padrão 2)")
    parser.add_argument("--sem-duplicado", action="store_true")
    parser.add_argument("--sem-invalido", action="store_true")
    parser.add_argument("--quebrado", action="store_true", help="envia também uma mensagem com JSON quebrado")
    args = parser.parse_args(argv)

    cfg = carregar_config()
    publisher = pubsub_v1.PublisherClient()
    caminho = publisher.topic_path(cfg["projeto"], topico())
    print("Publicando em:", caminho)

    def publicar(dados: bytes):
        # .result() espera a confirmação e levanta erro se falhar (permissão, API, billing)
        return publisher.publish(caminho, data=dados).result()

    ultimo = None
    for i in range(args.qtd):
        ultimo = gerar_evento()
        publicar(json.dumps(ultimo).encode("utf-8"))
        print(f"[{i + 1}/{args.qtd}] válido enviado | id_evento={ultimo['id_evento']}")
        time.sleep(args.pausa)

    if not args.sem_duplicado and ultimo:
        publicar(json.dumps(ultimo).encode("utf-8"))  # MESMO id_evento
        print(f"duplicado enviado | id_evento={ultimo['id_evento']}")
    if not args.sem_invalido:
        invalido = gerar_evento_rejeitado()
        publicar(json.dumps(invalido).encode("utf-8"))
        print(f"inválido enviado (sem id_municipio) | id_evento={invalido['id_evento']}")
    if args.quebrado:
        publicar(MENSAGEM_QUEBRADA)
        print("mensagem com JSON quebrado enviada")
    return 0


if __name__ == "__main__":
    sys.exit(main())
