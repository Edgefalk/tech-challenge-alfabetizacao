-- Silver · meta_municipio (formato longo: uma linha por município e ano da meta, 2024 a 2030)
-- As metas são idênticas nas linhas de 2023 e 2024 da Bronze; usa-se a linha mais recente de cada município.
-- D1: rede 'Municipal' -> '3'. D2: meta tratada como 2º ano. Meta nula continua nula.
CREATE OR REPLACE TABLE `{projeto}.{silver}.meta_municipio` AS
WITH bronze AS (
  SELECT *
  FROM `{projeto}.{bronze}.meta_alfabetizacao_municipio`
  WHERE _ingestion_timestamp = (SELECT MAX(_ingestion_timestamp) FROM `{projeto}.{bronze}.meta_alfabetizacao_municipio`)
),
uma_linha_por_municipio AS (
  SELECT *
  FROM bronze
  WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id_municipio, rede ORDER BY ano DESC) = 1
),
longo AS (
  SELECT id_municipio, rede AS rede_texto, _ingestion_timestamp, _source,
         CAST(SUBSTR(coluna_meta, -4) AS INT64) AS ano_meta, meta_alfabetizacao
  FROM uma_linha_por_municipio
  UNPIVOT INCLUDE NULLS (meta_alfabetizacao FOR coluna_meta IN (
    meta_alfabetizacao_2024, meta_alfabetizacao_2025, meta_alfabetizacao_2026, meta_alfabetizacao_2027,
    meta_alfabetizacao_2028, meta_alfabetizacao_2029, meta_alfabetizacao_2030
  ))
)
SELECT
  TRIM(l.id_municipio) AS id_municipio,
  SUBSTR(TRIM(l.id_municipio), 1, 2) AS cod_uf,
  u.sigla_uf,
  '2' AS serie,
  l.rede_texto,
  COALESCE(r.rede, 'desconhecido') AS rede,
  l.ano_meta,
  l.meta_alfabetizacao,
  l._ingestion_timestamp,
  l._source
FROM longo l
LEFT JOIN `{projeto}.{silver}.de_para_rede_meta` r ON r.rede_texto = TRIM(l.rede_texto)
LEFT JOIN `{projeto}.{silver}.de_para_uf` u ON u.cod_uf = SUBSTR(TRIM(l.id_municipio), 1, 2);
