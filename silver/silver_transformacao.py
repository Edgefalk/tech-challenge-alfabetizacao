#!/usr/bin/env python3
"""Tech Challenge Fase 2 - Camada Silver + regras de qualidade.

1. Executa, em ordem, os arquivos de silver/sql/ (lê a última rodada da Bronze e recria as tabelas da Silver).
2. Avalia as regras de qualidade da Silver (quality/regras.py).
3. Grava o resultado de todas as regras em ops.dq_results (load job, só acrescenta) e em docs/evidencias/.
4. Se alguma regra bloqueante reprovar, termina com código 1. A Gold verifica esse resultado
   antes de rodar e não é publicada sobre uma Silver reprovada.

Uso (a partir da raiz do repositório):
    python silver/silver_transformacao.py
    python silver/silver_transformacao.py --simular-falha   # aperta o limiar da R04 para 99,9% e demonstra a falha

Código de saída: 0 = Silver publicada e aprovada | 1 = regra bloqueante reprovada
"""
import argparse
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
)
from quality.regras import regras_silver  # noqa: E402

PASTA_SQL = RAIZ / "silver" / "sql"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Camada Silver + regras de qualidade.")
    parser.add_argument(
        "--simular-falha", action="store_true",
        help="aperta o limiar da R04 (match municipio x meta) para 99,9%% e demonstra o pipeline parando",
    )
    args = parser.parse_args(argv)

    cfg = carregar_config()
    client = conectar(cfg)
    execucao_id = novo_execucao_id("silver")
    print("Execução:", execucao_id, "\n")

    print("== Silver: transformações ==")
    for arquivo in sorted(PASTA_SQL.glob("*.sql")):
        executar_sql(client, arquivo, cfg)

    print("\n== Silver: regras de qualidade ==")
    limiar_r04 = 99.9 if args.simular_falha else 95.0
    if args.simular_falha:
        print("   (simulação de falha: limiar da R04 elevado para 99,9%)")
    resultados = avaliar_regras(client, regras_silver(limiar_match_municipio=limiar_r04), cfg, execucao_id)

    print("\n== Gravação dos resultados ==")
    gravar_resultados(client, cfg, resultados, "silver")

    try:
        exigir_bloqueantes_aprovadas(resultados)
    except RegraBloqueanteReprovada as erro:
        print(f"\nERRO: {erro}")
        print("A Gold não deve ser publicada sobre esta Silver.")
        return 1

    print("\nSilver publicada: todas as regras bloqueantes aprovadas.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
