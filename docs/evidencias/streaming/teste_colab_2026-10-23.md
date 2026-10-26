# Evidência do streaming: teste de 23/10/2026 (Colab)

Execução do producer e do consumer na mesma sessão do Google Colab (consumer em segundo plano, producer publicando),
contra o tópico `topico-alfabetizacao` e a assinatura `topico-alfabetizacao-sub` do projeto do grupo.
As tabelas `bronze_tc.eventos_alunos` e `ops_tc.eventos_rejeitados` foram excluídas antes da rodada, para a evidência partir do zero.

## Saída do producer

```
[1/5] válido enviado | id_evento=afa4b3c9-a72f-4796-8929-abdee7243ba2
[2/5] válido enviado | id_evento=096a15fc-e8a5-4b7b-b4f2-38d1ae9767d1
[3/5] válido enviado | id_evento=2f57c541-e162-4d8e-a04b-14e831080224
[4/5] válido enviado | id_evento=c7fd0ef2-da49-4f17-80c9-80602265cf1e
[5/5] válido enviado | id_evento=33c9ef9c-16af-4571-b6c9-8bd88d428a5a
duplicado enviado | id_evento=33c9ef9c-16af-4571-b6c9-8bd88d428a5a
inválido enviado (sem id_municipio) | id_evento=69ebe13b-5322-4b48-92b0-2b0499387da2
```

## Saída do consumer

```
1 linhas gravadas em project-ce782257-1c9c-4b6f-87a.bronze_tc.eventos_alunos (job a99ae79d-5223-40ef-9eed-b4e0670b2706)
Lote: 1 recebidas | 1 válidas | 0 duplicadas ignoradas | 0 rejeitadas
Latência (s): mín 0.91 | média 0.91 | máx 0.91
1 linhas gravadas em project-ce782257-1c9c-4b6f-87a.bronze_tc.eventos_alunos (job 3940625c-bfcb-4214-b84c-fd0592b4c71e)
Lote: 1 recebidas | 1 válidas | 0 duplicadas ignoradas | 0 rejeitadas
Latência (s): mín 14.45 | média 14.45 | máx 14.45
3 linhas gravadas em project-ce782257-1c9c-4b6f-87a.bronze_tc.eventos_alunos (job 2c626cc7-77ff-43a4-955a-c90abb8a32c5)
1 linhas gravadas em project-ce782257-1c9c-4b6f-87a.ops_tc.eventos_rejeitados (job 5898b898-f8b5-4842-9005-572f10ec4800)
Lote: 5 recebidas | 3 válidas | 1 duplicadas ignoradas | 1 rejeitadas
Latência (s): mín 13.00 | média 15.05 | máx 17.11
Fim da escuta. Mensagens recebidas no total: 7
```

## Conferência no BigQuery (leitura, 23/10/2026)

| Tabela | Linhas | Observação |
|---|---|---|
| `bronze_tc.eventos_alunos` | 5 | nenhum `id_evento` repetido; o duplicado enviado aparece uma vez |
| `ops_tc.eventos_rejeitados` | 1 | `id_evento` 69ebe13b…, motivo `campos ausentes: ['id_municipio']` |

Latência por evento (s): 0,91; 12,99; 14,45; 15,04; 17,11 (mediana 14,45; média 12,10). Todas maiores que zero.

Nota: uma rodada anterior, em que o consumer foi iniciado depois do producer, mediu 213 a 248 s, porque as mensagens esperaram na fila. Essa rodada foi descartada (tabelas excluídas) e não entra nos números do README.

## Depois da Bronze: Silver e Gold (execuções de 23/10/2026)

- `silver_tc.alunos_eventos`: 5 linhas, 5 `id_evento` distintos; regras E01 a E07 aprovadas (execução `silver_20261023_224832_50621d`).
- `gold_tc.indicador_por_origem`: 10.896 linhas `oficial` (2023 e 2024) e 1 linha `simulado` (município 3550308, 2026: taxa 38,61 sobre 5 eventos avaliados); regras G03 a G07 aprovadas (execução `gold_20261023_224946_7a366b`).
