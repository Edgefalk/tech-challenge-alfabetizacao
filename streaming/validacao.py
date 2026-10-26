"""Validação e triagem dos eventos de streaming. Sem dependência de GCP: pode ser testada localmente.

Cada mensagem do Pub/Sub cai em uma de três categorias (função `classificar_mensagem`):
- valido: passou em todas as checagens; vira uma linha de bronze.eventos_alunos
- duplicado: o id_evento já foi aceito nesta execução; é ignorado (o Pub/Sub entrega "pelo menos uma vez")
- rejeitado: vira uma linha de ops.eventos_rejeitados com o motivo e a mensagem original
"""
import json
from datetime import datetime, timezone

LIMIAR_ALFABETIZADO = 743  # corte do INEP: a partir daqui o aluno é alfabetizado

# Colunas de bronze.eventos_alunos (mesmos nomes e tipos da silver.alunos, mais as do streaming)
CAMPOS_EVENTOS = [
    "id_evento", "origem", "horario_producao", "horario_recebimento", "latencia_segundos", "ano",
    "id_municipio", "cod_uf", "sigla_uf", "id_escola", "id_aluno", "caderno", "serie", "serie_descricao",
    "rede", "rede_descricao", "presenca", "presenca_descricao", "preenchimento_caderno",
    "preenchimento_caderno_descricao", "alfabetizado", "alfabetizado_descricao", "avaliado",
    "proficiencia", "peso_aluno",
]

OBRIGATORIOS = ["id_evento", "horario_producao", "ano", "id_municipio",
                "id_aluno", "proficiencia", "peso_aluno", "alfabetizado"]


def agora_utc() -> datetime:
    return datetime.now(timezone.utc)  # sempre com fuso, para subtrair do horário do producer


def motivo_invalido(dados: dict):
    """Devolve o motivo da rejeição, ou None se o evento é válido."""
    # `is None` (e não `not`): "0" e 0 são valores legítimos e não contam como ausentes
    faltando = [c for c in OBRIGATORIOS if dados.get(c) is None]
    if faltando:
        return f"campos ausentes: {faltando}"
    if len(str(dados["id_municipio"])) != 7:
        return "id_municipio fora do padrão IBGE (7 dígitos)"
    try:
        proficiencia = float(dados["proficiencia"])
        peso = float(dados["peso_aluno"])
    except (TypeError, ValueError):
        return "proficiência ou peso não numérico"
    if not (0 < proficiencia < 1000):
        return "proficiência fora da faixa"
    if peso <= 0:
        return "peso_aluno menor ou igual a zero"
    esperado = "1" if proficiencia >= LIMIAR_ALFABETIZADO else "0"
    if str(dados["alfabetizado"]) != esperado:
        return f"alfabetizado inconsistente com a proficiência (esperado {esperado})"
    try:
        datetime.fromisoformat(str(dados["horario_producao"]))
    except ValueError:
        return "horario_producao inválido"
    return None


def montar_rejeitado(texto_bruto: str, motivo: str, id_evento=None, agora=None) -> dict:
    """Linha de ops.eventos_rejeitados: 4 colunas fixas, a mensagem original fica em `mensagem_bruta`."""
    return {
        "id_evento": id_evento,
        "horario_recebimento": (agora or agora_utc()).isoformat(),
        "motivo_rejeicao": motivo,
        "mensagem_bruta": texto_bruto,
    }


def classificar_mensagem(texto: str, vistos: set, agora=None):
    """Triagem de uma mensagem. Devolve (categoria, linha): linha é None para 'duplicado'.
    `vistos` é o conjunto de id_evento já aceitos; só recebe o id depois de o evento ser validado,
    assim um inválido reenviado é rejeitado de novo e não some como duplicado."""
    agora = agora or agora_utc()
    try:
        dados = json.loads(texto)
    except json.JSONDecodeError:
        return "rejeitado", montar_rejeitado(texto, "JSON inválido", agora=agora)
    if not isinstance(dados, dict):
        return "rejeitado", montar_rejeitado(texto, "mensagem não é um objeto JSON", agora=agora)

    id_evento = dados.get("id_evento")
    if id_evento is not None and id_evento in vistos:
        return "duplicado", None

    motivo = motivo_invalido(dados)
    if motivo:
        return "rejeitado", montar_rejeitado(texto, motivo, id_evento, agora=agora)

    vistos.add(id_evento)
    producao = datetime.fromisoformat(str(dados["horario_producao"]))
    if producao.tzinfo is None:  # tolera evento sem fuso, assumindo UTC
        producao = producao.replace(tzinfo=timezone.utc)
    dados["horario_recebimento"] = agora.isoformat()
    dados["latencia_segundos"] = (agora - producao).total_seconds()
    return "valido", {c: dados.get(c) for c in CAMPOS_EVENTOS}
