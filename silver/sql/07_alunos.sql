-- Silver · alunos (única tabela pesada: lê cerca de 1 GB da Bronze)
-- Coluna derivada `avaliado` (TRUE quando há peso amostral = presente e com caderno preenchido),
-- sem alterar as colunas originais. É o denominador correto da taxa.
-- FinOps: particionada por ano e clusterizada por UF e município.
CREATE OR REPLACE TABLE `{projeto}.{silver}.alunos`
PARTITION BY RANGE_BUCKET(ano, GENERATE_ARRAY(2023, 2031, 1))
CLUSTER BY sigla_uf, id_municipio
AS
WITH bronze AS (
  SELECT *
  FROM `{projeto}.{bronze}.alunos`
  WHERE _ingestion_timestamp = (SELECT MAX(_ingestion_timestamp) FROM `{projeto}.{bronze}.alunos`)
),
dic AS (
  SELECT nome_coluna, chave, valor
  FROM `{projeto}.{bronze}.dicionario`
  WHERE _ingestion_timestamp = (SELECT MAX(_ingestion_timestamp) FROM `{projeto}.{bronze}.dicionario`)
    AND id_tabela = 'alunos'
)
SELECT
  b.ano,
  TRIM(b.id_municipio) AS id_municipio,
  SUBSTR(TRIM(b.id_municipio), 1, 2) AS cod_uf,
  u.sigla_uf,
  TRIM(b.id_escola) AS id_escola,
  TRIM(b.id_aluno) AS id_aluno,
  TRIM(b.caderno) AS caderno,
  TRIM(b.serie) AS serie,
  COALESCE(d_serie.valor, 'desconhecido') AS serie_descricao,
  TRIM(b.rede) AS rede,
  COALESCE(d_rede.valor, 'desconhecido') AS rede_descricao,
  TRIM(b.presenca) AS presenca,
  COALESCE(d_pres.valor, 'desconhecido') AS presenca_descricao,
  TRIM(b.preenchimento_caderno) AS preenchimento_caderno,
  COALESCE(d_pre.valor, 'desconhecido') AS preenchimento_caderno_descricao,
  TRIM(b.alfabetizado) AS alfabetizado,
  COALESCE(d_alf.valor, 'desconhecido') AS alfabetizado_descricao,
  b.peso_aluno IS NOT NULL AS avaliado,
  b.proficiencia,
  b.peso_aluno,
  b._ingestion_timestamp,
  b._source
FROM bronze b
LEFT JOIN dic d_serie ON d_serie.nome_coluna = 'serie'                 AND d_serie.chave = TRIM(b.serie)
LEFT JOIN dic d_rede  ON d_rede.nome_coluna  = 'rede'                  AND d_rede.chave  = TRIM(b.rede)
LEFT JOIN dic d_pres  ON d_pres.nome_coluna  = 'presenca'              AND d_pres.chave  = TRIM(b.presenca)
LEFT JOIN dic d_pre   ON d_pre.nome_coluna   = 'preenchimento_caderno' AND d_pre.chave   = TRIM(b.preenchimento_caderno)
LEFT JOIN dic d_alf   ON d_alf.nome_coluna   = 'alfabetizado'          AND d_alf.chave   = TRIM(b.alfabetizado)
LEFT JOIN `{projeto}.{silver}.de_para_uf` u ON u.cod_uf = SUBSTR(TRIM(b.id_municipio), 1, 2);
