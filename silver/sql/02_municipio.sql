-- Silver · municipio
-- Última rodada da Bronze; tipos e nulos preservados; rede e serie traduzidas pelo dicionário
-- (código sem tradução vira 'desconhecido'); UF derivada do código IBGE.
CREATE OR REPLACE TABLE `{projeto}.{silver}.municipio` AS
WITH bronze AS (
  SELECT *
  FROM `{projeto}.{bronze}.municipio`
  WHERE _ingestion_timestamp = (SELECT MAX(_ingestion_timestamp) FROM `{projeto}.{bronze}.municipio`)
),
dic AS (
  SELECT nome_coluna, chave, valor
  FROM `{projeto}.{bronze}.dicionario`
  WHERE _ingestion_timestamp = (SELECT MAX(_ingestion_timestamp) FROM `{projeto}.{bronze}.dicionario`)
    AND id_tabela = 'municipio'
)
SELECT
  b.ano,
  TRIM(b.id_municipio) AS id_municipio,
  SUBSTR(TRIM(b.id_municipio), 1, 2) AS cod_uf,
  u.sigla_uf,
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
LEFT JOIN `{projeto}.{silver}.de_para_uf` u ON u.cod_uf = SUBSTR(TRIM(b.id_municipio), 1, 2);
