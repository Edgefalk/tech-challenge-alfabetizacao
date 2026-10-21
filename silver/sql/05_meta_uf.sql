-- Silver · meta_uf (formato longo com ano_referencia)
-- As metas NÃO são deduplicadas: a linha de referência de 2025 traz metas arredondadas,
-- diferentes das de 2023 e 2024. A Gold compara o resultado do ano X com a linha de referência do ano X.
CREATE OR REPLACE TABLE `{projeto}.{silver}.meta_uf` AS
WITH bronze AS (
  SELECT *
  FROM `{projeto}.{bronze}.meta_alfabetizacao_uf`
  WHERE _ingestion_timestamp = (SELECT MAX(_ingestion_timestamp) FROM `{projeto}.{bronze}.meta_alfabetizacao_uf`)
),
longo AS (
  SELECT ano AS ano_referencia, sigla_uf, rede AS rede_texto, taxa_alfabetizacao AS taxa_referencia,
         percentual_participacao, _ingestion_timestamp, _source,
         CAST(SUBSTR(coluna_meta, -4) AS INT64) AS ano_meta, meta_alfabetizacao
  FROM bronze
  UNPIVOT INCLUDE NULLS (meta_alfabetizacao FOR coluna_meta IN (
    meta_alfabetizacao_2024, meta_alfabetizacao_2025, meta_alfabetizacao_2026, meta_alfabetizacao_2027,
    meta_alfabetizacao_2028, meta_alfabetizacao_2029, meta_alfabetizacao_2030
  ))
)
SELECT
  l.ano_referencia,
  u.cod_uf,
  TRIM(l.sigla_uf) AS sigla_uf,
  '2' AS serie,
  l.rede_texto,
  COALESCE(r.rede, 'desconhecido') AS rede,
  l.taxa_referencia,
  l.percentual_participacao,
  l.ano_meta,
  l.meta_alfabetizacao,
  l._ingestion_timestamp,
  l._source
FROM longo l
LEFT JOIN `{projeto}.{silver}.de_para_rede_meta` r ON r.rede_texto = TRIM(l.rede_texto)
LEFT JOIN `{projeto}.{silver}.de_para_uf` u ON u.sigla_uf = TRIM(l.sigla_uf);
