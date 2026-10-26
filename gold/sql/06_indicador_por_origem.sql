-- @requer: {projeto}.{silver}.alunos_eventos
-- Gold · indicador_por_origem: a taxa de alfabetização por município, com a COLUNA `origem`.
--   origem = 'oficial'  : taxa publicada pelo INEP (silver.municipio, rede Municipal, 2º ano)
--   origem = 'simulado' : taxa recalculada dos eventos de streaming (peso amostral, só avaliados)
-- Os dois ficam na mesma tabela só porque `origem` permite separá-los. Número gerado pelo simulador
-- NÃO é indicador oficial: nenhuma consulta deve somar ou comparar as duas origens sem filtrar.
-- Só roda se a Silver de eventos existir.
CREATE OR REPLACE TABLE `{projeto}.{gold}.indicador_por_origem` AS
SELECT
  'oficial' AS origem,
  ano, id_municipio, cod_uf, sigla_uf, serie, rede,
  taxa_alfabetizacao,
  CAST(NULL AS INT64) AS n_eventos,
  CAST(NULL AS INT64) AS n_avaliados,
  'taxa publicada pelo INEP' AS metodo,
  CURRENT_TIMESTAMP() AS _gold_timestamp
FROM `{projeto}.{silver}.municipio`
WHERE rede = '3' AND serie = '2'

UNION ALL

SELECT
  origem,
  ano, id_municipio, cod_uf, sigla_uf, serie, rede,
  ROUND(100 * SAFE_DIVIDE(SUM(IF(avaliado AND alfabetizado = '1', peso_aluno, 0)), SUM(IF(avaliado, peso_aluno, 0))), 2)
    AS taxa_alfabetizacao,
  COUNT(*) AS n_eventos,
  COUNTIF(avaliado) AS n_avaliados,
  'recalculada dos eventos simulados (peso amostral, só avaliados)' AS metodo,
  CURRENT_TIMESTAMP() AS _gold_timestamp
FROM `{projeto}.{silver}.alunos_eventos`
GROUP BY origem, ano, id_municipio, cod_uf, sigla_uf, serie, rede;
