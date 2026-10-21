-- ops · dq_results: resultado de cada regra de qualidade, uma linha por regra e execução.
-- IF NOT EXISTS (nunca REPLACE): o histórico de validações acumula entre execuções.
-- As linhas são gravadas por load job (WRITE_APPEND) em quality/dq.py, que funciona inclusive no sandbox.
CREATE TABLE IF NOT EXISTS `{projeto}.{ops}.dq_results` (
  execucao_id   STRING    NOT NULL,
  executado_em  TIMESTAMP NOT NULL,
  camada        STRING,
  tabela        STRING,
  regra_id      STRING,
  descricao     STRING,
  severidade    STRING,
  valor_medido  FLOAT64,
  limiar        STRING,
  status        STRING
)
PARTITION BY DATE(executado_em);
