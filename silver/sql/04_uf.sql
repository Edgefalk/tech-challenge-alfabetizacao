-- Silver · uf (mesmo padrão de municipio; cod_uf vem do de-para)
CREATE OR REPLACE TABLE `{projeto}.{silver}.uf` AS
WITH bronze AS (
  SELECT *
  FROM `{projeto}.{bronze}.uf`
  WHERE _ingestion_timestamp = (SELECT MAX(_ingestion_timestamp) FROM `{projeto}.{bronze}.uf`)
),
dic AS (
  SELECT nome_coluna, chave, valor
  FROM `{projeto}.{bronze}.dicionario`
  WHERE _ingestion_timestamp = (SELECT MAX(_ingestion_timestamp) FROM `{projeto}.{bronze}.dicionario`)
    AND id_tabela = 'uf'
)
SELECT
  b.ano,
  u.cod_uf,
  TRIM(b.sigla_uf) AS sigla_uf,
  TRIM(b.serie) AS serie,
  COALESCE(ds.valor, 'desconhecido') AS serie_descricao,
  TRIM(b.rede) AS rede,
  COALESCE(dr.valor, 'desconhecido') AS rede_descricao,
  b.taxa_alfabetizacao,
  b.media_portugues,
  b.proporcao_aluno_nivel_0, b.proporcao_aluno_nivel_1, b.proporcao_aluno_nivel_2,
  b.proporcao_aluno_nivel_3, b.proporcao_aluno_nivel_4, b.proporcao_aluno_nivel_5,
  b.proporcao_aluno_nivel_6, b.proporcao_aluno_nivel_7, b.proporcao_aluno_nivel_8,
  b._ingestion_timestamp,
  b._source
FROM bronze b
LEFT JOIN dic dr ON dr.nome_coluna = 'rede'  AND dr.chave = TRIM(b.rede)
LEFT JOIN dic ds ON ds.nome_coluna = 'serie' AND ds.chave = TRIM(b.serie)
LEFT JOIN `{projeto}.{silver}.de_para_uf` u ON u.sigla_uf = TRIM(b.sigla_uf);
