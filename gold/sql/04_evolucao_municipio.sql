-- Gold · evolucao_municipio: variação da taxa 2023 -> 2024 por município e rede
-- A variação só é calculada quando há os dois anos (nulo preservado nos demais).
CREATE OR REPLACE TABLE `{projeto}.{gold}.evolucao_municipio` AS
SELECT
  id_municipio, cod_uf, sigla_uf, serie, rede, ANY_VALUE(rede_descricao) AS rede_descricao,
  MAX(IF(ano = 2023, taxa_alfabetizacao, NULL)) AS taxa_2023,
  MAX(IF(ano = 2024, taxa_alfabetizacao, NULL)) AS taxa_2024,
  ROUND(MAX(IF(ano = 2024, taxa_alfabetizacao, NULL)) - MAX(IF(ano = 2023, taxa_alfabetizacao, NULL)), 2) AS variacao_pp,
  CASE
    WHEN COUNTIF(ano = 2023) = 0 OR COUNTIF(ano = 2024) = 0 THEN 'sem_dois_anos'
    WHEN MAX(IF(ano = 2024, taxa_alfabetizacao, NULL)) > MAX(IF(ano = 2023, taxa_alfabetizacao, NULL)) THEN 'subiu'
    WHEN MAX(IF(ano = 2024, taxa_alfabetizacao, NULL)) < MAX(IF(ano = 2023, taxa_alfabetizacao, NULL)) THEN 'caiu'
    ELSE 'estavel'
  END AS tendencia,
  CURRENT_TIMESTAMP() AS _gold_timestamp
FROM `{projeto}.{silver}.municipio`
GROUP BY id_municipio, cod_uf, sigla_uf, serie, rede;
