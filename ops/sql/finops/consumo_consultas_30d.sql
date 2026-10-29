-- FinOps (Frente 4): consumo real de consultas do projeto nos últimos 30 dias.
-- Somente leitura. Lê INFORMATION_SCHEMA.JOBS_BY_PROJECT (região southamerica-east1).
-- Requer a permissão bigquery.jobs.listAll no projeto.
SELECT
  job_type,
  statement_type,
  COUNT(*) AS jobs,
  COUNTIF(error_result IS NOT NULL) AS jobs_com_erro,
  COUNTIF(EXISTS(SELECT 1 FROM UNNEST(labels) l WHERE l.key = 'pipeline')) AS jobs_com_label_pipeline,
  SUM(IFNULL(total_bytes_processed, 0)) AS bytes_processados,
  SUM(IFNULL(total_bytes_billed, 0)) AS bytes_faturados,
  ROUND(SUM(IFNULL(total_bytes_billed, 0)) / POW(1024, 3), 3) AS gib_faturados,
  FORMAT_TIMESTAMP('%Y-%m-%d', MIN(creation_time), 'America/Sao_Paulo') AS primeiro_dia,
  FORMAT_TIMESTAMP('%Y-%m-%d', MAX(creation_time), 'America/Sao_Paulo') AS ultimo_dia
FROM `project-ce782257-1c9c-4b6f-87a.region-southamerica-east1.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY 1, 2
ORDER BY bytes_faturados DESC
