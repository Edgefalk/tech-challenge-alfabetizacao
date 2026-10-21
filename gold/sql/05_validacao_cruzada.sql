-- Gold · validacao_cruzada: taxa recalculada a partir de alunos x taxa oficial (município, UF e Brasil)
-- taxa = 100 * soma(peso dos avaliados alfabetizados) / soma(peso dos avaliados)
-- Redes agregadas (dicionário): '5' = alunos das redes '2' + '3'; '0' = '1' + '2' + '3' + '4'.
-- Contrastes: taxa_sem_peso (sem peso amostral) e taxa_todos_registros (denominador errado, inclui ausentes).
-- Para municipio rede '3', traz também a taxa que aparece na tabela de meta (investigação da anomalia de 2023).
CREATE OR REPLACE TABLE `{projeto}.{gold}.validacao_cruzada` AS
WITH mapa_rede AS (
  SELECT * FROM UNNEST([
    STRUCT('2' AS rede_alvo, '2' AS rede_aluno), ('3', '3'),
    ('5', '2'), ('5', '3'),
    ('0', '1'), ('0', '2'), ('0', '3'), ('0', '4')
  ])
),
a AS (
  SELECT
    ano, sigla_uf, id_municipio, rede,
    SUM(IF(avaliado, peso_aluno, 0)) AS soma_peso,
    SUM(IF(avaliado AND alfabetizado = '1', peso_aluno, 0)) AS soma_peso_alf,
    COUNTIF(avaliado) AS n_avaliados,
    COUNTIF(avaliado AND alfabetizado = '1') AS n_alf_avaliados,
    COUNT(*) AS n_registros,
    COUNTIF(alfabetizado = '1') AS n_alf_registros
  FROM `{projeto}.{silver}.alunos`
  GROUP BY ano, sigla_uf, id_municipio, rede
),
ax AS (
  SELECT a.*, m.rede_alvo FROM a JOIN mapa_rede m ON m.rede_aluno = a.rede
),
agregado AS (
  SELECT 'municipio' AS nivel, ano, id_municipio AS local, rede_alvo AS rede,
         SUM(soma_peso) AS sp, SUM(soma_peso_alf) AS spa, SUM(n_avaliados) AS nav, SUM(n_alf_avaliados) AS naa,
         SUM(n_registros) AS nreg, SUM(n_alf_registros) AS nar
  FROM ax GROUP BY ano, id_municipio, rede_alvo
  UNION ALL
  SELECT 'uf', ano, sigla_uf, rede_alvo,
         SUM(soma_peso), SUM(soma_peso_alf), SUM(n_avaliados), SUM(n_alf_avaliados), SUM(n_registros), SUM(n_alf_registros)
  FROM ax GROUP BY ano, sigla_uf, rede_alvo
  UNION ALL
  SELECT 'brasil', ano, 'BR', rede_alvo,
         SUM(soma_peso), SUM(soma_peso_alf), SUM(n_avaliados), SUM(n_alf_avaliados), SUM(n_registros), SUM(n_alf_registros)
  FROM ax GROUP BY ano, rede_alvo
),
recalc AS (
  SELECT nivel, ano, local, rede,
         ROUND(100 * SAFE_DIVIDE(spa, sp), 2) AS taxa_recalculada,
         ROUND(100 * SAFE_DIVIDE(naa, nav), 2) AS taxa_sem_peso,
         ROUND(100 * SAFE_DIVIDE(nar, nreg), 2) AS taxa_todos_registros,
         nav AS n_avaliados, nreg AS n_registros
  FROM agregado
),
oficial AS (
  SELECT 'municipio' AS nivel, ano, id_municipio AS local, rede, taxa_alfabetizacao AS taxa_oficial
  FROM `{projeto}.{silver}.municipio`
  UNION ALL
  SELECT 'uf', ano, sigla_uf, rede, taxa_alfabetizacao
  FROM `{projeto}.{silver}.uf`
  UNION ALL
  SELECT 'brasil', ano_referencia, 'BR', rede, ANY_VALUE(taxa_referencia)
  FROM `{projeto}.{silver}.meta_brasil`
  GROUP BY ano_referencia, rede
),
taxa_tabela_meta AS (
  SELECT ano, TRIM(id_municipio) AS id_municipio, taxa_alfabetizacao
  FROM `{projeto}.{bronze}.meta_alfabetizacao_municipio`
  WHERE _ingestion_timestamp = (SELECT MAX(_ingestion_timestamp) FROM `{projeto}.{bronze}.meta_alfabetizacao_municipio`)
)
SELECT
  o.nivel, o.ano, o.local, o.rede,
  o.taxa_oficial,
  r.taxa_recalculada,
  ROUND(r.taxa_recalculada - o.taxa_oficial, 2) AS diferenca_pp,
  r.taxa_sem_peso,
  r.taxa_todos_registros,
  r.n_avaliados,
  r.n_registros,
  IF(o.nivel = 'municipio' AND o.rede = '3', t.taxa_alfabetizacao, NULL) AS taxa_na_tabela_meta,
  CURRENT_TIMESTAMP() AS _gold_timestamp
FROM oficial o
LEFT JOIN recalc r ON r.nivel = o.nivel AND r.ano = o.ano AND r.local = o.local AND r.rede = o.rede
LEFT JOIN taxa_tabela_meta t ON o.nivel = 'municipio' AND t.ano = o.ano AND t.id_municipio = o.local;
