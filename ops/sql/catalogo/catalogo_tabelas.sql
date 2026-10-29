-- Catálogo das tabelas das camadas (somente leitura).
-- Lê só INFORMATION_SCHEMA. Não altera nada. Custo mínimo de 10 MB por consulta.
-- Região do projeto: southamerica-east1.
WITH
t AS (
  SELECT table_schema, table_name, table_type, creation_time, ddl
  FROM `project-ce782257-1c9c-4b6f-87a.region-southamerica-east1.INFORMATION_SCHEMA.TABLES`
  WHERE table_schema IN ('bronze_tc', 'silver_tc', 'gold_tc', 'ops_tc')
),
s AS (
  -- Linhas e bytes lógicos via metatabela __TABLES__ de cada dataset
  -- (INFORMATION_SCHEMA.TABLE_STORAGE não está habilitado neste projeto).
  SELECT dataset_id AS table_schema, table_id AS table_name, row_count AS total_rows,
         size_bytes AS total_logical_bytes, TIMESTAMP_MILLIS(last_modified_time) AS storage_last_modified_time
  FROM `project-ce782257-1c9c-4b6f-87a.bronze_tc.__TABLES__`
  UNION ALL
  SELECT dataset_id, table_id, row_count, size_bytes, TIMESTAMP_MILLIS(last_modified_time)
  FROM `project-ce782257-1c9c-4b6f-87a.silver_tc.__TABLES__`
  UNION ALL
  SELECT dataset_id, table_id, row_count, size_bytes, TIMESTAMP_MILLIS(last_modified_time)
  FROM `project-ce782257-1c9c-4b6f-87a.gold_tc.__TABLES__`
  UNION ALL
  SELECT dataset_id, table_id, row_count, size_bytes, TIMESTAMP_MILLIS(last_modified_time)
  FROM `project-ce782257-1c9c-4b6f-87a.ops_tc.__TABLES__`
),
o AS (
  SELECT table_schema, table_name,
         MAX(IF(option_name = 'description', option_value, NULL)) AS descricao,
         MAX(IF(option_name = 'expiration_timestamp', option_value, NULL)) AS expira_em,
         MAX(IF(option_name = 'partition_expiration_days', option_value, NULL)) AS exp_particao_dias,
         MAX(IF(option_name = 'labels', option_value, NULL)) AS labels
  FROM `project-ce782257-1c9c-4b6f-87a.region-southamerica-east1.INFORMATION_SCHEMA.TABLE_OPTIONS`
  WHERE table_schema IN ('bronze_tc', 'silver_tc', 'gold_tc', 'ops_tc')
  GROUP BY 1, 2
),
c AS (
  SELECT c.table_schema, c.table_name,
         STRING_AGG(IF(c.is_partitioning_column = 'YES', c.column_name, NULL), ',') AS coluna_particao,
         STRING_AGG(IF(c.clustering_ordinal_position IS NOT NULL, c.column_name, NULL), ',' ORDER BY c.clustering_ordinal_position) AS colunas_cluster,
         TO_JSON_STRING(ARRAY_AGG(STRUCT(c.ordinal_position AS pos, c.column_name AS nome, c.data_type AS tipo,
                                         c.is_nullable AS nulo, f.description AS descricao) ORDER BY c.ordinal_position)) AS colunas
  FROM `project-ce782257-1c9c-4b6f-87a.region-southamerica-east1.INFORMATION_SCHEMA.COLUMNS` c
  LEFT JOIN `project-ce782257-1c9c-4b6f-87a.region-southamerica-east1.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` f
    ON f.table_schema = c.table_schema AND f.table_name = c.table_name AND f.field_path = c.column_name
  WHERE c.table_schema IN ('bronze_tc', 'silver_tc', 'gold_tc', 'ops_tc')
  GROUP BY 1, 2
)
SELECT
  t.table_schema AS dataset,
  t.table_name AS tabela,
  t.table_type AS tipo,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', t.creation_time, 'America/Sao_Paulo') AS criada_em,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', s.storage_last_modified_time, 'America/Sao_Paulo') AS modificada_em,
  s.total_rows AS linhas,
  s.total_logical_bytes AS bytes_logicos,
  o.descricao, o.expira_em, o.exp_particao_dias, o.labels,
  c.coluna_particao, c.colunas_cluster, c.colunas,
  t.ddl
FROM t
LEFT JOIN s USING (table_schema, table_name)
LEFT JOIN o USING (table_schema, table_name)
LEFT JOIN c USING (table_schema, table_name)
ORDER BY 1, 2
