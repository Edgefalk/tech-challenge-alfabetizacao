"""Registro de execução do pipeline (Frente 4: observabilidade).

Cada etapa do pipeline roda dentro de `registrar_etapa(...)`. Ao terminar, com
sucesso ou falha, a função grava uma linha em ops_tc.pipeline_runs e escreve um
log no Cloud Logging, no log "tc-pipeline": INFO no sucesso, ERROR na falha.
É o log ERROR que dispara o alerta por e-mail "Pipeline TC - falha".

Uso com o executar_sql da Frente 3 (quality/dq.py):

    from ops.run_logger import registrar_etapa, job_config

    with registrar_etapa("silver_02_municipio", camada="silver", execucao_id=execucao_id) as etapa:
        info = executar_sql(client, arquivo, cfg, job_config=job_config("silver_02_municipio", "silver"))
        etapa.registrar_sql(client, info)   # guarda o job e conta as linhas das tabelas criadas

Uso com um job qualquer:

    with registrar_etapa("minha_etapa", camada="gold", tabela="gold_tc.x") as etapa:
        job = client.query(sql, job_config=job_config("minha_etapa", "gold"))
        job.result()
        etapa.registrar_job(job)

O que conta como falha:
- qualquer exceção dentro do bloco (ela é registrada e depois relançada);
- sys.exit com código diferente de 0, ou interrupção (Ctrl+C);
- etapa.falhar("motivo"), para quando o código decide parar sem exceção
  (por exemplo, o portão da Gold).

Decisões:
- A gravação usa load job em modo append (load_table_from_json + WRITE_APPEND),
  e não streaming (insert_rows_json). Load job é gratuito; streaming é cobrado
  e era bloqueado no BigQuery Sandbox, onde o projeto começou.
- O log é escrito antes da linha: o e-mail não pode depender do BigQuery.
- Uma falha ao registrar nunca esconde o erro original da etapa, nem para o pipeline.
- execucao_id segue o formato de ops_tc.dq_results (Frente 3):
  <prefixo>_<AAAAMMDD>_<HHMMSS>_<6 hex>, em UTC. Passe o mesmo execucao_id das
  regras de qualidade para ligar pipeline_runs e dq_results.
"""
from __future__ import annotations

import getpass
import os
import re
import traceback
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

try:  # mesmo padrão do resto do repositório: lê o .env da raiz, se existir
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except ImportError:
    pass

PROJETO = os.getenv("GCP_PROJECT_ID", "project-ce782257-1c9c-4b6f-87a")
DATASET_OPS = os.getenv("BQ_OPS_DATASET", "ops_tc")  # mesmo nome usado no .env.example do grupo
TABELA_RUNS = f"{PROJETO}.{DATASET_OPS}.pipeline_runs"
NOME_LOG = os.getenv("PIPELINE_LOG_NAME", "tc-pipeline")
MAX_BYTES = int(os.getenv("BQ_MAX_BYTES_BILLED", str(10 * 1024**3)))  # teto por consulta: 10 GiB
LABEL_PIPELINE = "tc-fase2"

# Tabelas criadas por um SQL: CREATE [OR REPLACE] TABLE [IF NOT EXISTS] `projeto.dataset.tabela`
_CREATE_TABLE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?([\w\-]+(?:\.[\w\-]+){1,2})`?",
    re.IGNORECASE,
)

_bq = None
_schema = None
_logger = None


def novo_execucao_id(prefixo: str = "pipeline") -> str:
    """Gera um ID no mesmo formato usado pela Frente 3 em ops_tc.dq_results."""
    agora = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{prefixo}_{agora}_{uuid.uuid4().hex[:6]}"


# Uma execução do pipeline inteiro pode fixar o ID por variável de ambiente,
# para que todas as etapas fiquem agrupadas.
EXECUCAO_ID = os.getenv("PIPELINE_EXECUCAO_ID") or novo_execucao_id()


def _cliente() -> bigquery.Client:
    global _bq
    if _bq is None:
        _bq = bigquery.Client(project=PROJETO)
    return _bq


def _log():
    global _logger
    if _logger is None:
        from google.cloud import logging as cloud_logging
        _logger = cloud_logging.Client(project=PROJETO).logger(NOME_LOG)
    return _logger


def _label(valor: str) -> str:
    """Labels do BigQuery aceitam só minúsculas, números, _ e - (acentos são permitidos), até 63 caracteres."""
    v = "".join(c if c.isalnum() or c in "_-" else "_" for c in valor.lower())
    return v[:63]


def job_config(etapa: str, camada: str, **kwargs) -> bigquery.QueryJobConfig:
    """QueryJobConfig padrão: labels para medir custo por etapa e teto de bytes por consulta."""
    labels = {"pipeline": LABEL_PIPELINE, "camada": _label(camada), "etapa": _label(etapa)}
    labels.update(kwargs.pop("labels", {}))
    return bigquery.QueryJobConfig(labels=labels, maximum_bytes_billed=MAX_BYTES, **kwargs)


def tabelas_criadas(sql: str) -> list[str]:
    """Tabelas que um SQL cria, na ordem em que aparecem, sem repetir."""
    vistas: list[str] = []
    for nome in _CREATE_TABLE.findall(sql or ""):
        if nome not in vistas:
            vistas.append(nome)
    return vistas


class Etapa:
    def __init__(self, tabela: str | None = None) -> None:
        self.tabela = tabela
        self.linhas_entrada: int | None = None
        self.linhas_saida: int | None = None
        self.job_ids: list[str] = []
        self.bytes_processados = 0
        self.bytes_faturados = 0
        self.motivo_falha: str | None = None

    def registrar_job(self, job) -> None:
        """Chame depois de job.result(). Soma bytes e guarda o ID do job."""
        self.job_ids.append(job.job_id)
        self.bytes_processados += getattr(job, "total_bytes_processed", None) or 0
        self.bytes_faturados += getattr(job, "total_bytes_billed", None) or 0

    def registrar_sql(self, cliente, info: dict) -> None:
        """Para o retorno de quality.dq.executar_sql: guarda o job e conta as linhas
        das tabelas criadas pelo SQL (só metadados, sem custo de consulta)."""
        job = info.get("job")
        if job is not None:
            self.registrar_job(job)
        tabelas = tabelas_criadas(info.get("sql", ""))
        if not tabelas:
            return
        if not self.tabela:
            self.tabela = ", ".join(".".join(t.split(".")[-2:]) for t in tabelas)
        try:
            self.linhas_saida = sum(cliente.get_table(t).num_rows or 0 for t in tabelas)
        except Exception:
            print("[run_logger] não consegui contar as linhas de " + ", ".join(tabelas))

    def falhar(self, motivo: str) -> None:
        """Marca a etapa como falha sem lançar exceção (ex.: o portão da Gold recusou a Silver)."""
        self.motivo_falha = motivo


def _gravar_linha(linha: dict) -> None:
    global _schema
    cliente = _cliente()
    if _schema is None:
        _schema = cliente.get_table(TABELA_RUNS).schema
    cfg = bigquery.LoadJobConfig(
        schema=_schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        labels={"pipeline": LABEL_PIPELINE, "camada": "ops", "etapa": "registro_pipeline_runs"},
    )
    cliente.load_table_from_json([linha], TABELA_RUNS, job_config=cfg).result()


@contextmanager
def registrar_etapa(etapa: str, camada: str, tabela: str | None = None, execucao_id: str | None = None):
    e = Etapa(tabela)
    inicio = datetime.now(timezone.utc)
    erro = None
    try:
        yield e
    except SystemExit as exc:
        if exc.code not in (None, 0):
            erro = f"SystemExit: {exc.code}"
        raise
    except KeyboardInterrupt:
        erro = "KeyboardInterrupt: execução interrompida"
        raise
    except Exception as exc:
        erro = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        if erro is None and e.motivo_falha:
            erro = f"Falha: {e.motivo_falha}"
        status = "falha" if erro else "sucesso"
        fim = datetime.now(timezone.utc)
        linha = {
            "execucao_id": execucao_id or EXECUCAO_ID,
            "etapa": etapa,
            "camada": camada,
            "tabela": e.tabela,
            "inicio": inicio.isoformat(),
            "fim": fim.isoformat(),
            "duracao_seg": round((fim - inicio).total_seconds(), 3),
            "linhas_entrada": e.linhas_entrada,
            "linhas_saida": e.linhas_saida,
            "status": status,
            "erro": erro[:4000] if erro else None,
            "job_ids": e.job_ids,
            "bytes_processados": e.bytes_processados,
            "bytes_faturados": e.bytes_faturados,
            "executado_por": getpass.getuser(),
        }
        # 1) Log primeiro: é ele que dispara o e-mail, e não pode depender do BigQuery.
        try:
            _log().log_struct(linha, severity="ERROR" if status == "falha" else "INFO")
        except Exception:
            print("[run_logger] não consegui escrever no Cloud Logging:\n" + traceback.format_exc(limit=1))
        # 2) Depois, a linha em ops_tc.pipeline_runs.
        try:
            _gravar_linha(linha)
        except Exception:
            print("[run_logger] não consegui gravar em pipeline_runs:\n" + traceback.format_exc(limit=1))
