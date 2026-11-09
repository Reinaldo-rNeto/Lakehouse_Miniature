# 05. Plano de estudo: aprender de verdade

Ler o código não ensina. O que ensina é prever o resultado, rodar, errar de propósito e explicar com
suas palavras. Este plano alterna leitura, laboratório e autoteste.

Regra de ouro de cada etapa: **antes de rodar qualquer coisa, escreva o que você espera que aconteça**.
Depois compare. A diferença entre o esperado e o real é onde está o aprendizado.

## Etapas

| Etapa | Foco | Leitura | Faça | Você domina quando consegue... |
|---|---|---|---|---|
| 1 | Subir a stack e ver as camadas | `docs/01-conceitos.md` (seções de camadas) | Passos 0 a 4 do `02-passo-a-passo.md`; abra o navegador do SeaweedFS | Desenhar de memória a arquitetura e dizer o papel de cada contêiner |
| 2 | Bronze e Iceberg por dentro | `01-conceitos.md` (Iceberg) | Rode `03_iceberg_por_dentro.sql`; liste `metadata/` no SeaweedFS | Explicar o que é um snapshot e por que a escrita é atômica |
| 3 | Silver e qualidade | `transforms.py`: `parse_*`, `aplicar_qualidade` | Laboratórios 1 a 3 abaixo | Justificar cada regra R01 a R03 e dizer o que acontece com a linha rejeitada |
| 4 | Modelagem dimensional | Livro: Cap. 1 e Cap. 2 (granularidade p. 39; técnicas em p. 37 em diante) | `06-modelo-gold.md`; reconstrua a matriz de barramento sem olhar | Dizer a granularidade de cada fato e classificar cada medida (aditiva, semi, não aditiva) |
| 5 | SCD tipo 2 | Livro: Cap. 2 (p. 53 a 54) e Cap. 5 (p. 148 a 164); `aplicar_scd2` | `04_demo_scd2.py`; laboratórios 4 e 5 | Explicar por que a chave substituta existe e fazer a busca por data de vigência no quadro |
| 6 | Engenharia do pipeline | Livro: Cap. 19 (ETL, p. 443 em diante; subsistemas em p. 451 a 486) | Laboratórios 6 a 8; receitas 4, 11 e 14 do `04-alteracoes.md` | Explicar idempotência, recarga e como recuperar de uma falha no meio |
| 7 | Apresentar | `03-apresentacao.md` | Ensaie 12 minutos cronometrado, duas vezes | Responder as perguntas prováveis sem consultar |

Ritmo sugerido: uma etapa por sessão de estudo. Não avance se não passar no autoteste da etapa.

## Laboratórios "quebrar de propósito"

Para cada um: faça a mudança, **preveja** o efeito, rode, compare, desfaça. Anote o que aprendeu em duas linhas.

1. **Valor lixo.** Edite `tests/fixtures/populacao.json` trocando um valor por `"abc"`. Rode `pytest`. Qual regra pegou? Em que tabela a linha foi parar na versão completa (`silver.dq_rejeitados`)?
2. **Duplicata.** Duplique uma observação no fixture. Aparece duas vezes na silver? Por quê? (Pista: deduplicação por chave, a carga mais recente ganha.)
3. **Símbolo do IBGE.** Troque um valor por `"..."`. A linha é rejeitada ou vira nulo com status `indisponivel`? Por que essa decisão é melhor do que descartar?
4. **Mudança de nome do município.** Rode `04_demo_scd2.py` e depois consulte a história do município no Trino. Quantas linhas existem? Qual é a `registro_atual`? Rode de novo: o que muda? (Deve não mudar nada: idempotência.)
5. **Vigência retroativa.** Rode a demo com `--vigencia` anterior ao início da versão atual. O código recusa (proteção em `aplicar_scd2`). Por que aceitar isso corromperia a dimensão?
6. **Rodar o pipeline duas vezes.** Compare contagens antes e depois em cada tabela. Os fatos dobraram? Por quê não?
7. **Variável trocada.** Em `VARIAVEIS_PIB`, troque a palavra esperada de uma variável por outra qualquer. O que acontece? É para falhar: é a trava `validar_variaveis` contra o IBGE mudar o significado de um código.
8. **Viagem no tempo e rollback.** Rode a gold, altere um fato com a receita 14, consulte a versão anterior com `FOR VERSION AS OF`. O que o Iceberg preservou? O que `expire_snapshots` destruiria?

Dica: ao terminar cada laboratório, use `--restaurar` na demo SCD2 ou recrie a stack para voltar ao estado limpo.

## Autoteste (responda em voz alta, sem olhar)

**Lakehouse e Iceberg**
1. Qual a diferença entre arquivo Parquet e tabela Iceberg?
2. O que existe em `metadata/` e o que existe em `data/`?
3. Por que a escrita no Iceberg é atômica? (Pista: o catálogo troca um ponteiro.)
4. O que é um snapshot e o que permite fazer?
5. Por que o Spark e o Trino enxergam a mesma tabela? Quem é a fonte da verdade sobre "qual é o metadata atual"?

**Medallion**
6. O que é proibido alterar na bronze? Por quê guardar o JSON cru?
7. Por que a silver tem uma tabela de rejeitados em vez de apagar o dado ruim?
8. O que torna a gold diferente da silver além de "estar mais limpa"?

**Kimball**
9. Defina granularidade e diga a dos dois fatos.
10. O que é dimensão conformada e que consulta deste projeto depende dela?
11. Por que chave substituta se o IBGE já tem código de município?
12. SCD tipo 1, 2 e 3: o que cada um faz com o histórico? Quando você escolheria cada um?
13. Por que população não soma entre anos? E o PIB?
14. Por que PIB per capita não fica guardado no fato?
15. Para que serve o membro 0 na dimensão?
16. O que a dimensão de auditoria permite responder?

**Engenharia**
17. O que significa o pipeline ser idempotente e como cada camada garante isso?
18. Como o fato escolhe a versão certa de município? (Data de referência dentro do intervalo de vigência.)
19. O que acontece se um fato chega antes do membro da dimensão? (Cap. 19, p. 478, membros tardios.)
20. O que você precisaria mudar para ir a produção? (Última seção do `04-alteracoes.md`.)

## Glossário mínimo

| Termo | Em uma frase |
|---|---|
| Lakehouse | Arquivos abertos em armazenamento barato com tabelas transacionais por cima |
| Parquet | Formato de arquivo colunar comprimido |
| Iceberg | Formato de tabela: metadados que dizem quais arquivos compõem a tabela em cada momento |
| Snapshot | Foto do estado da tabela após uma escrita |
| Catálogo | Serviço que guarda o ponteiro para o metadado atual de cada tabela |
| Medallion | Organização em bronze (cru), silver (limpo) e gold (consumo) |
| Granularidade | O que representa uma linha da tabela fato |
| Fato | Medida numérica de um evento ou retrato |
| Dimensão | Contexto descritivo (quem, onde, quando) |
| Chave substituta | Identificador artificial do data warehouse |
| SCD | Slowly Changing Dimension: como tratar atributos que mudam devagar |
| Drill across | Combinar fatos diferentes por dimensões conformadas |
| Idempotência | Rodar várias vezes tem o mesmo efeito que rodar uma |

## Depois deste projeto (próximo nível)

- Trocar a recarga por carga incremental e MERGE (receitas 4 e 20).
- Colocar um orquestrador (Airflow ou Dagster) e agendamento.
- Testes de dados automáticos entre camadas (contagens, unicidade, integridade referencial fato-dimensão).
- Modelar um segundo processo (ex.: um fato transacional) para praticar a matriz de barramento.
- Livro: Cap. 3 (varejo) e Cap. 5 (SCD completo) para consolidar; depois um estudo de caso do seu setor.
- Governo e dados abertos: ler a documentação das APIs do IBGE e da CKAN do governo; os dados da Xerlock são o próximo exercício natural.
