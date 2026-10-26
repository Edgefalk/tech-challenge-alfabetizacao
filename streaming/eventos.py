"""Geração dos eventos simulados (mesmo formato e tipos da silver.alunos). Sem dependência de GCP."""
import random
import uuid
from datetime import datetime, timezone

from streaming.validacao import LIMIAR_ALFABETIZADO


def gerar_evento() -> dict:
    # Proficiência na faixa real do microdado (cerca de 578 a 904); `alfabetizado` é DERIVADA dela,
    # como na fonte oficial (nunca sorteada à parte: daria "Sim" com 600 pontos).
    proficiencia = round(random.uniform(580.0, 900.0), 2)
    alfabetizado = "1" if proficiencia >= LIMIAR_ALFABETIZADO else "0"
    return {
        # controle do streaming
        "id_evento": str(uuid.uuid4()),
        "horario_producao": datetime.now(timezone.utc).isoformat(),  # com fuso: a latência depende disso
        "origem": "simulado",  # dado gerado pelo simulador nunca se passa por indicador oficial
        # colunas da silver.alunos (códigos como texto)
        "ano": 2026,
        "id_municipio": "3550308",
        "cod_uf": "35",
        "sigla_uf": "SP",
        "id_escola": str(random.randint(10000, 99999)),
        "id_aluno": str(random.randint(1000000, 9999999)),
        "caderno": str(random.randint(1, 21)),
        "serie": "2",
        "serie_descricao": "2º ano do Ensino Fundamental",
        "rede": "3",
        "rede_descricao": "Municipal",
        "presenca": "1",
        "presenca_descricao": "Presente",
        "preenchimento_caderno": "1",
        "preenchimento_caderno_descricao": "Prova preenchida",
        "alfabetizado": alfabetizado,
        "alfabetizado_descricao": "Sim" if alfabetizado == "1" else "Não",
        "avaliado": True,
        "proficiencia": proficiencia,
        "peso_aluno": round(random.uniform(0.5, 1.5), 2),
    }


def gerar_evento_rejeitado() -> dict:
    """Evento inválido de propósito (sem id_municipio), com id_evento novo."""
    evento = gerar_evento()
    del evento["id_municipio"]
    return evento


MENSAGEM_QUEBRADA = b'{"id_evento": "json-quebrado", '  # JSON incompleto, de propósito
