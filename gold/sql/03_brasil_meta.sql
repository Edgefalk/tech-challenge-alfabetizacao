-- Gold · brasil_meta: indicador nacional oficial (da própria meta_brasil) x meta do mesmo ano
CREATE OR REPLACE TABLE `{projeto}.{gold}.brasil_meta` AS
WITH base AS (
  SELECT
    ano_referencia AS ano, rede, serie, taxa_referencia AS taxa_alfabetizacao, percentual_participacao,
    MAX(IF(ano_meta = ano_referencia, meta_alfabetizacao, NULL)) AS meta_alfabetizacao
  FROM `{projeto}.{silver}.meta_brasil`
  GROUP BY ano, rede, serie, taxa_alfabetizacao, percentual_participacao
)
SELECT
  ano, 'BR' AS local, serie, rede, taxa_alfabetizacao, percentual_participacao, meta_alfabetizacao,
  ROUND(taxa_alfabetizacao - meta_alfabetizacao, 2) AS distancia_meta,
  CASE
    WHEN meta_alfabetizacao IS NULL THEN 'sem_meta'
    WHEN taxa_alfabetizacao >= meta_alfabetizacao THEN 'atingiu'
    ELSE 'nao_atingiu'
  END AS status_meta,
  IF(meta_alfabetizacao IS NULL AND ano < 2024, 'metas começam em 2024', NULL) AS motivo_sem_meta,
  CURRENT_TIMESTAMP() AS _gold_timestamp
FROM base;
