"""Teste da observabilidade (Frente 4).

Roda duas etapas falsas, marcadas com etapa='teste_frente4':
  1. uma que termina com sucesso;
  2. uma que falha de propósito.

Resultado esperado:
  - 2 linhas novas em ops_tc.pipeline_runs (status sucesso e falha);
  - 1 log ERROR no log "tc-pipeline" do Cloud Logging;
  - 1 e-mail do alerta "Pipeline TC - falha" em alguns minutos.

Uso:
    python -m ops.testar_alerta
"""
from datetime import datetime, timezone

from ops.run_logger import novo_execucao_id, registrar_etapa

execucao = novo_execucao_id("teste")
print(f"execucao_id = {execucao}")

with registrar_etapa("teste_frente4", camada="ops", tabela="ops_tc.pipeline_runs", execucao_id=execucao) as e:
    e.linhas_entrada = 0
    e.linhas_saida = 0
print("etapa de sucesso registrada")

momento_falha = datetime.now(timezone.utc)
try:
    with registrar_etapa("teste_frente4", camada="ops", tabela="ops_tc.pipeline_runs", execucao_id=execucao):
        raise RuntimeError("falha simulada pela Frente 4 para testar o alerta por e-mail")
except RuntimeError:
    print(f"falha simulada registrada em {momento_falha.isoformat()} (UTC)")
