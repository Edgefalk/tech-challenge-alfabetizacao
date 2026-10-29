#!/usr/bin/env python3
"""Tech Challenge Fase 2 - Camada Gold + regras de qualidade.

1. Portão: só segue se a execução mais recente da Silver tiver todas as regras bloqueantes aprovadas
   (consulta ops.dq_results).
2. Executa, em ordem, os arquivos de gold/sql/ (meta x resultado, evolução e validação cruzada).
3. Avalia as regras da Gold e grava em ops.dq_results (load job) e em docs/evidencias/.
4. O portão, cada arquivo SQL e a etapa de regras viram uma linha em ops.pipeline_runs (Frente 4,
   ops/run_logger.py). Portão recusado ou regra bloqueante reprovada gravam status "falha" e um log
   ERROR em tc-pipeline, que dispara o alerta por e-mail.

Uso (a partir da raiz do repositório, depois da Silver):
    python gold/gold_transformacao.py

Código de saída: 0 = Gold publicada e aprovada | 1 = Silver não aprovada ou regra bloqueante da Gold reprovada
"""
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from quality.dq import (  # noqa: E402
    RegraBloqueanteReprovada,
    avaliar_regras,
    carregar_config,
    conectar,
    executar_sql,
    exigir_bloqueantes_aprovadas,
    gravar_resultados,
    novo_execucao_id,
    requisitos_pendentes,
    tabela_existe,
    ultima_validacao_silver_aprovada,
)
from quality.regras import regras_gold  # noqa: E402
from ops.run_logger import job_config, registrar_etapa  # noqa: E402

PASTA_SQL = RAIZ / "gold" / "sql"


def main() -> int:
    cfg = carregar_config()
    client = conectar(cfg)

    # O ID é criado antes do portão para que uma recusa também fique registrada em ops.pipeline_runs
    execucao_id = novo_execucao_id("gold")

    with registrar_etapa("gold_portao", camada="gold", tabela=f"{cfg['ops']}.dq_results",
                         execucao_id=execucao_id) as registro:
        print("== Portão: última validação da Silver ==")
        if not ultima_validacao_silver_aprovada(client, cfg):
            registro.falhar("a última execução da Silver não foi aprovada (ou não existe)")
            print("ERRO: a última execução da Silver não foi aprovada (ou não existe). Gold não publicada.")
            print("Rode 'python silver/silver_transformacao.py' e corrija as regras reprovadas.")
            return 1
        print("   Silver aprovada.\n")

    print("Execução:", execucao_id, "\n")

    print("== Gold: transformações ==")
    for arquivo in sorted(PASTA_SQL.glob("*.sql")):
        pendentes = requisitos_pendentes(client, arquivo, cfg)
        if pendentes:
            print(f"   {arquivo.name}: PULADO (etapa opcional; falta {', '.join(pendentes)})")
            continue
        etapa = f"gold_{arquivo.stem}"
        with registrar_etapa(etapa, camada="gold", execucao_id=execucao_id) as registro:
            info = executar_sql(client, arquivo, cfg, job_config=job_config(etapa, "gold"))
            registro.registrar_sql(client, info)

    try:
        # A etapa de regras fica dentro do registro: regra bloqueante reprovada = falha + log ERROR + e-mail
        with registrar_etapa("gold_regras", camada="gold", tabela=f"{cfg['ops']}.dq_results",
                             execucao_id=execucao_id) as registro:
            print("\n== Gold: regras de qualidade ==")
            com_eventos = tabela_existe(client, f"{cfg['projeto']}.{cfg['gold']}.indicador_por_origem")
            resultados = avaliar_regras(client, regras_gold(incluir_eventos=com_eventos), cfg, execucao_id,
                                        job_config=job_config("gold_regras", "gold"))

            print("\n== Gravação dos resultados ==")
            gravar_resultados(client, cfg, resultados, "gold")
            registro.linhas_saida = len(resultados)

            exigir_bloqueantes_aprovadas(resultados)
    except RegraBloqueanteReprovada as erro:
        print(f"\nERRO: {erro}")
        return 1

    print("\nGold publicada: todas as regras bloqueantes aprovadas.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
