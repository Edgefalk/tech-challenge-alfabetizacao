"""Nomes do Pub/Sub lidos do .env (com padrão do projeto do grupo). Projeto e datasets vêm de quality.dq."""
import os

from dotenv import load_dotenv

from quality.dq import RAIZ

load_dotenv(RAIZ / ".env")


def topico() -> str:
    return os.getenv("PUBSUB_TOPIC", "topico-alfabetizacao").strip()


def assinatura() -> str:
    return os.getenv("PUBSUB_SUBSCRIPTION", "topico-alfabetizacao-sub").strip()
