-- Gold · municipio_meta: taxa do município x meta pactuada do mesmo ano
-- Recorte: rede '3' (Municipal) e serie '2', o mesmo da meta municipal.
-- 3 estados: atingiu / nao_atingiu / sem_meta (ausência de meta nunca vira "não atingiu").
CREATE OR REPLACE TABLE `{projeto}.{gold}.municipio_meta` AS
WITH res AS (
  SELECT * FROM `{projeto}.{silver}.municipio` WHERE rede = '3' AND serie = '2'
),
municipios_com_meta AS (
  SELECT DISTINCT id_municipio, rede FROM `{projeto}.{silver}.meta_municipio`
)
SELECT
  r.ano, r.id_municipio, r.cod_uf, r.sigla_uf, r.serie, r.serie_descricao, r.rede, r.rede_descricao,
  r.taxa_alfabetizacao,
  m.meta_alfabetizacao,
  ROUND(r.taxa_alfabetizacao - m.meta_alfabetizacao, 2) AS distancia_meta,
  CASE
    WHEN m.meta_alfabetizacao IS NULL THEN 'sem_meta'
    WHEN r.taxa_alfabetizacao >= m.meta_alfabetizacao THEN 'atingiu'
    ELSE 'nao_atingiu'
  END AS status_meta,
  CASE
    WHEN m.meta_alfabetizacao IS NOT NULL THEN NULL
    WHEN r.ano < 2024 THEN 'metas começam em 2024'
    WHEN c.id_municipio IS NULL THEN 'município sem meta pactuada'
    ELSE 'meta nula na fonte'
  END AS motivo_sem_meta,
  r.media_portugues,
  CURRENT_TIMESTAMP() AS _gold_timestamp
FROM res r
LEFT JOIN `{projeto}.{silver}.meta_municipio` m
  ON m.id_municipio = r.id_municipio AND m.rede = r.rede AND m.ano_meta = r.ano
LEFT JOIN municipios_com_meta c
  ON c.id_municipio = r.id_municipio AND c.rede = r.rede;
