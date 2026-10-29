-- Números do streaming para README e vídeo (somente leitura).
SELECT 'bronze_tc.eventos_alunos' AS tabela, origem, COUNT(*) AS linhas,
       ROUND(MIN(latencia_segundos), 3) AS lat_min_s, ROUND(AVG(latencia_segundos), 3) AS lat_media_s,
       ROUND(MAX(latencia_segundos), 3) AS lat_max_s
FROM `project-ce782257-1c9c-4b6f-87a.bronze_tc.eventos_alunos`
GROUP BY origem
UNION ALL
SELECT 'gold_tc.indicador_por_origem', origem, COUNT(*), NULL, NULL, NULL
FROM `project-ce782257-1c9c-4b6f-87a.gold_tc.indicador_por_origem`
GROUP BY origem
UNION ALL
SELECT 'ops_tc.eventos_rejeitados', motivo_rejeicao, COUNT(*), NULL, NULL, NULL
FROM `project-ce782257-1c9c-4b6f-87a.ops_tc.eventos_rejeitados`
GROUP BY motivo_rejeicao
ORDER BY 1, 2
