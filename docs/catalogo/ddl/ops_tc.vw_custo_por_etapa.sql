-- DDL de ops_tc.vw_custo_por_etapa, extraído do INFORMATION_SCHEMA em 28/10/2026 21:02.
-- SOMENTE DOCUMENTAÇÃO. NÃO EXECUTAR: a tabela já existe e pertence ao dono indicado no catálogo.

CREATE VIEW `project-ce782257-1c9c-4b6f-87a.ops_tc.vw_custo_por_etapa`
OPTIONS(
  description="Frente 4: bytes faturados por camada e etapa nos últimos 30 dias (jobs com label pipeline)."
)
AS SELECT
  (SELECT value FROM UNNEST(labels) WHERE key = 'camada') AS camada,
  (SELECT value FROM UNNEST(labels) WHERE key = 'etapa')  AS etapa,
  COUNT(*) AS jobs,
  COUNTIF(error_result IS NOT NULL) AS jobs_com_erro,
  SUM(IFNULL(total_bytes_billed, 0)) / POW(1024, 3) AS gib_faturados,
  SUM(IFNULL(total_bytes_billed, 0)) / POW(1024, 4) * 11.25 AS usd_preco_tabela_sao_paulo
FROM `project-ce782257-1c9c-4b6f-87a.region-southamerica-east1.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND job_type = 'QUERY'
  AND parent_job_id IS NULL  -- só jobs de primeiro nível: num script, o job pai já soma os filhos
  AND EXISTS (SELECT 1 FROM UNNEST(labels) WHERE key = 'pipeline')
GROUP BY camada, etapa;
