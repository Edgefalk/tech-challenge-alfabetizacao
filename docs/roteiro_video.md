# Roteiro do vídeo (até 5 minutos)

**Público:** liderança, não técnico. **Regra:** todo número falado está em [`docs/numeros.md`](numeros.md); o número entre parênteses é o item de lá. Se um número mudar, o roteiro muda junto.

| Bloco | Tempo | Quem fala | Na tela |
|---|---|---|---|
| 1. O problema | 0:00–0:40 | a definir | título e uma frase-problema |
| 2. O que construímos | 0:40–1:40 | Frente 1 | diagrama de `docs/arquitetura.md` |
| 3. Demonstração | 1:40–3:30 | Frentes 1, 2 e 3 | console do BigQuery e terminal |
| 4. Quanto custa e como sabemos que quebrou | 3:30–4:30 | Frente 4 | `docs/finops/tabela_custo.md` e o e-mail de alerta |
| 5. Limitações e próximos passos | 4:30–5:00 | a definir | lista curta |

---

## Bloco 1. O problema (40 s)

> "O MEC acompanha quantas crianças estão alfabetizadas ao fim do 2º ano, por município e por estado, e compara com metas até 2030. Esses dados são públicos, mas chegam em tabelas soltas, com códigos e sem histórico. Nosso trabalho foi transformar isso num pipeline confiável: que guarda tudo o que chega, limpa, compara com as metas e avisa quando algo dá errado."

## Bloco 2. O que construímos (60 s)

Mostre o diagrama e percorra da esquerda para a direita:

> "São três camadas. A **Bronze** guarda o dado exatamente como veio e nunca apaga. Hoje são 7,7 milhões de linhas de alunos (2), de duas cargas. A **Silver** limpa e padroniza: 3,9 milhões de alunos (3), com os códigos traduzidos. A **Gold** entrega o indicador pronto: cada município comparado com a sua meta. Ao todo são 26 tabelas e 1,7 GB (1, 4). Em paralelo, um simulador gera eventos em tempo quase real, que chegam em cerca de 12 segundos (11) e aparecem separados do dado oficial (13)."

## Bloco 3. Demonstração (1 min 50 s)

*Roteiro técnico a definir com as Frentes 1, 2 e 3.* Sequência sugerida:

1. **Frente 1:** rodar a ingestão e mostrar que a Bronze dobrou, sem apagar nada (2).
2. **Frente 2:** enviar um evento, mostrar a latência e o evento rejeitado indo para a tabela de rejeitados (11, 12).
3. **Frente 3:** mostrar a Gold, município x meta; depois provocar a falha de qualidade e mostrar o pipeline parando (8).

## Bloco 4. Quanto custa e como sabemos que quebrou (60 s)

> "Hoje o projeto custa zero: está no ambiente gratuito do Google (14). Mas medimos o que ele custaria em produção, com o preço de São Paulo, que é 80% mais caro que nos Estados Unidos (15).
>
> A principal descoberta foi esta. Como a Bronze nunca apaga e a Silver relê tudo a cada execução, o custo de consulta cresce sem parar: com uma carga por dia, em seis meses chegaria a uns 26 dólares por mês (19). Lendo só a carga nova, fica em uns 55 centavos (20). Medimos também que ler só as colunas necessárias corta 81% da leitura (17).
>
> E quando algo quebra: cada etapa grava o que aconteceu numa tabela de execuções, e uma falha manda e-mail para o responsável. No teste, o e-mail chegou cerca de 1 minuto depois da falha (23)."

*Mostrar:* o e-mail "Pipeline TC - falha" recebido às 20:21 de 28/10 e o print do alerta em `docs/frente4/evidencias/alerta_aberto_20261028.jpg`.

## Bloco 5. Limitações e próximos passos (30 s)

> "Três limitações honestas. O ambiente gratuito apaga as tabelas em 60 dias. O alerta por e-mail só cobre as etapas que já usam o registro de execuções. E o dado de streaming é simulado: nunca se mistura ao oficial, porque tem a marca de origem. Os próximos passos são particionar a Bronze por data de carga e ligar o pipeline a uma conta de produção."

---

## Checklist antes de gravar

- [ ] Rodar `python ops/medir_tabelas.py` e `python ops/gerar_catalogo.py` de novo e atualizar `docs/numeros.md`.
- [ ] Conferir se cada número falado bate com `docs/numeros.md`.
- [x] Alerta testado em 28/10: item 23 e seção 8 do README preenchidos.
- [ ] Cronometrar: no máximo 5 minutos.
