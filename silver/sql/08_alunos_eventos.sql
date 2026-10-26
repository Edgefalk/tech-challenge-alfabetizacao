-- @requer: {projeto}.{bronze}.eventos_alunos
-- Silver · alunos_eventos: eventos de streaming (Pub/Sub) deduplicados por id_evento.
-- Só roda se a Bronze de eventos existir (o streaming é opcional para a parte batch do pipeline).
-- O Pub/Sub entrega "pelo menos uma vez": o consumer deduplica em memória, mas um reinício do consumer
-- pode deixar o mesmo id_evento duas vezes na Bronze. Aqui fica a linha recebida primeiro.
-- Mesmos nomes, tipos e traduções da silver.alunos; `origem` acompanha o evento até a Gold.
CREATE OR REPLACE TABLE `{projeto}.{silver}.alunos_eventos` AS
WITH ordenado AS (
  SELECT
    b.*,
    ROW_NUMBER() OVER (PARTITION BY b.id_evento ORDER BY b.horario_recebimento ASC) AS ordem
  FROM `{projeto}.{bronze}.eventos_alunos` b
)
SELECT
  o.id_evento,
  o.origem,
  o.horario_producao,
  o.horario_recebimento,
  o.latencia_segundos,
  o.ano,
  TRIM(o.id_municipio) AS id_municipio,
  SUBSTR(TRIM(o.id_municipio), 1, 2) AS cod_uf,
  u.sigla_uf,
  TRIM(o.id_escola) AS id_escola,
  TRIM(o.id_aluno) AS id_aluno,
  TRIM(o.caderno) AS caderno,
  TRIM(o.serie) AS serie,
  o.serie_descricao,
  TRIM(o.rede) AS rede,
  o.rede_descricao,
  TRIM(o.presenca) AS presenca,
  o.presenca_descricao,
  TRIM(o.preenchimento_caderno) AS preenchimento_caderno,
  o.preenchimento_caderno_descricao,
  TRIM(o.alfabetizado) AS alfabetizado,
  o.alfabetizado_descricao,
  o.peso_aluno IS NOT NULL AS avaliado,
  o.proficiencia,
  o.peso_aluno
FROM ordenado o
LEFT JOIN `{projeto}.{silver}.de_para_uf` u ON u.cod_uf = SUBSTR(TRIM(o.id_municipio), 1, 2)
WHERE o.ordem = 1;
