-- Gold · uf_meta: taxa da UF x meta pactuada do mesmo ano (linha de referência do mesmo ano)
-- Recorte: rede '5' (Pública Estadual e Municipal) e serie '2', o mesmo da meta estadual.
CREATE OR REPLACE TABLE `{projeto}.{gold}.uf_meta` AS
WITH res AS (
  SELECT * FROM `{projeto}.{silver}.uf` WHERE rede = '5' AND serie = '2'
),
met AS (
  SELECT * FROM `{projeto}.{silver}.meta_uf` WHERE ano_meta = ano_referencia
),
ufs_com_linha_de_meta AS (
  SELECT DISTINCT ano_referencia, sigla_uf, rede FROM `{projeto}.{silver}.meta_uf`
)
SELECT
  r.ano, r.cod_uf, r.sigla_uf, r.serie, r.serie_descricao, r.rede, r.rede_descricao,
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
    WHEN c.sigla_uf IS NULL THEN 'UF sem meta pactuada'
    ELSE 'meta nula na fonte'
  END AS motivo_sem_meta,
  r.media_portugues,
  CURRENT_TIMESTAMP() AS _gold_timestamp
FROM res r
LEFT JOIN met m ON m.sigla_uf = r.sigla_uf AND m.rede = r.rede AND m.ano_referencia = r.ano
LEFT JOIN ufs_com_linha_de_meta c ON c.sigla_uf = r.sigla_uf AND c.rede = r.rede AND c.ano_referencia = r.ano;
