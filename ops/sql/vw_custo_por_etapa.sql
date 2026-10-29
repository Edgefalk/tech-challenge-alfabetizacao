-- Frente 4: custo real por camada e etapa, a partir dos labels dos jobs (últimos 30 dias).
-- IF NOT EXISTS: não substitui nada que já exista.
-- Requer bigquery.jobs.listAll no projeto (papel "BigQuery Resource Viewer") para criar e para consultar.
-- Só conta jobs com label "pipeline" (aplicado por ops/run_logger.job_config).
-- Versão de 28/10/2026: acrescenta o filtro parent_job_id IS NULL. A view do BigQuery foi atualizada
-- em 28/10 com esta mesma consulta, via CREATE OR REPLACE VIEW (só na view da Frente 4, com aprovação).
CREATE VIEW IF NOT EXISTS `project-ce782257-1c9c-4b6f-87a.ops_tc.vw_custo_por_etapa`
OPTIONS(description = "Frente 4: bytes faturados por camada e etapa nos últimos 30 dias (jobs com label pipeline).")
AS
SELECT
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
