# 01. Conceitos: entender cada peça e ligar com o livro

As páginas citadas são as **impressas no livro** (Kimball e Ross, *The Data Warehouse Toolkit*, 3ª ed.),
não as do PDF.

## 1. A ideia central: camadas separadas

Num banco tradicional, motor, formato de arquivo, armazenamento e catálogo vêm juntos. Num lakehouse
cada camada é uma peça independente, e é isso que torna o conjunto aberto e trocável.

| Camada | Pergunta que responde | Neste projeto | Alternativas comuns |
|---|---|---|---|
| Armazenamento | Onde os bytes ficam? | SeaweedFS (API S3) | MinIO, AWS S3, Azure ADLS, GCS |
| Formato de arquivo | Como os dados são codificados? | Parquet (colunar) | ORC, Avro |
| Formato de tabela | Como arquivos viram uma tabela transacional? | Apache Iceberg | Delta Lake, Apache Hudi |
| Catálogo | Qual é a versão atual de cada tabela? | Iceberg REST (imagem de teste) | Polaris, Nessie, Lakekeeper, Hive Metastore, AWS Glue |
| Motor | Quem lê e escreve? | Spark (ETL), Trino (SQL) | Flink, DuckDB, Dremio, Snowflake |

Frase para guardar: **a tabela não é o arquivo e nem o motor; é o metadado que o catálogo aponta.**
Por isso Spark e Trino enxergam a mesma tabela sem copiar nada.

## 2. Iceberg por dentro

Uma tabela Iceberg é uma árvore de metadados sobre arquivos de dados imutáveis:

```
catálogo: "gold.fato_pib" -> aponta para metadata/v7.metadata.json
                                  |
                    metadata.json: esquema, partições, lista de snapshots
                                  |
                  snapshot atual -> manifest list (lista de manifestos)
                                  |
                       manifest: lista de arquivos de dados + estatísticas por coluna
                                  |
                     arquivos Parquet no SeaweedFS (data/)
```

- **Snapshot:** estado completo da tabela num instante. Cada escrita (append, delete, replace) cria um
  novo. Ler uma tabela é ler um snapshot, então leitores nunca veem escrita pela metade.
- **Transação (ACID):** a escrita só "vale" quando o catálogo troca, de forma atômica, o ponteiro para o
  novo `metadata.json`. Se o job morre no meio, o ponteiro não muda e nada aparece.
- **Viagem no tempo:** como snapshots antigos continuam existindo, dá para consultar ou voltar a um
  anterior (`FOR VERSION AS OF` no Trino, `rollback_to_snapshot` no Spark).
- **Evolução de esquema:** adicionar, renomear ou remover coluna é mudança de metadado, sem reescrever
  dados. As colunas são identificadas por id, não por posição.
- **Poda de arquivos:** as estatísticas nos manifestos deixam o motor pular arquivos que não podem ter
  o dado procurado.
- **DELETE e UPDATE:** como os arquivos são imutáveis, o Iceberg reescreve arquivos (copy-on-write) ou
  grava arquivos de "deleção" (merge-on-read). O padrão do Iceberg é copy-on-write.

Veja isso de verdade: `sql/trino/03_iceberg_por_dentro.sql` e a pasta `metadata/` da tabela no
navegador do SeaweedFS (`http://localhost:8888/buckets/lakehouse/`).

### Por que o catálogo importa

O catálogo é o único ponto que precisa ser consistente: ele faz a troca atômica do ponteiro. A imagem
`iceberg-rest-fixture` usada aqui é a de **teste** do projeto Iceberg, com SQLite e sem autenticação.
Serve para estudo. Em produção se usa Polaris, Nessie, Lakekeeper, Hive Metastore ou o Glue.

### Spark e Trino, por que os dois

| | Spark | Trino |
|---|---|---|
| Perfil | Processamento em lote, código (Python/Scala), transformações complexas | Consulta SQL interativa, muitos usuários, BI |
| Aqui | Escreve bronze, silver e gold | Lê a gold para análise |
| Leitura do Iceberg | Sim | Sim, o mesmo catálogo |

## 3. Arquitetura medalhão

| Camada | O que guarda | Regra de ouro | Aqui |
|---|---|---|---|
| Bronze | Dado como veio da origem | Não interpretar, não perder, só acrescentar | `bronze.ibge_respostas_brutas`: uma linha por resposta de API (JSON em texto, hash, lote, data) |
| Silver | Dado limpo, tipado, validado, sem regra de negócio analítica | Reconstruível a partir da bronze | `silver.municipio`, `silver.observacao_agregado`, `silver.dq_rejeitados` |
| Gold | Dado modelado para consumo | Modelo que o negócio entende | Estrela dimensional: 2 fatos, 3 dimensões |

Por que guardar o JSON cru na bronze? Porque **a interpretação (schema-on-read) fica na silver**. Se
descobrirmos um erro de parsing, reprocessamos a silver sem chamar a API de novo. O `payload_sha256`
permite detectar se a origem mudou entre cargas.

## 4. Onde cada conceito do Kimball aparece

| Conceito | Onde no projeto | Livro |
|---|---|---|
| Processo de 4 passos | Seção 5 abaixo | Cap. 3, p. 70; Cap. 18, p. 434 |
| Grão | `fato_*`: um município por ano | Cap. 2, p. 39; Cap. 3, p. 71 |
| Fatos aditivos, semiaditivos | População não soma entre anos (ver seção 6) | Cap. 2, p. 42; Cap. 3, p. 76 |
| Dimensão desnormalizada (sem snowflake) | `dim_municipio` achata micro, meso, UF, região | Cap. 2, p. 47; Cap. 3, p. 84 |
| Chave substituta | `municipio_key` inteira, nunca o código IBGE | Cap. 2, p. 46; Cap. 3, p. 98 |
| Chave durável (natural estável) | `municipio_id` (código IBGE de 7 dígitos) | Cap. 2, p. 46; Cap. 3, p. 100 |
| SCD tipo 2 | `dim_municipio` (vigência e registro atual) | Cap. 5, p. 150; Cap. 2, p. 54 |
| Sem FK nula: registro "não informado" | `municipio_key = 0` | Cap. 2, p. 42; Cap. 3, p. 92 |
| Dimensão de auditoria | `dim_auditoria` + `auditoria_key` nos fatos | Cap. 2, p. 66; Cap. 19, p. 460 |
| Esquema de eventos de erro | `silver.dq_rejeitados` | Cap. 2, p. 68; Cap. 19, p. 458 |
| Dimensões conformadas | `dim_municipio` e `dim_ano` servem aos dois fatos | Cap. 2, p. 51; Cap. 4, p. 130 |
| Drill across | PIB per capita em `sql/trino/02_...sql` (consulta 3) | Cap. 2, p. 51; Cap. 4, p. 130 |
| Matriz de barramento | `docs/06-modelo-gold.md` | Cap. 2, p. 52; Cap. 4, p. 125 |
| Pipeline de chaves substitutas | `atribuir_chaves` em `transforms.py` | Cap. 19, p. 475 |
| Dado que chega atrasado | Município novo vira versão com vigência desde 1900 | Cap. 19, p. 478 |
| Captura de mudança (CDC) | Aqui: recarga completa por lote. Alternativas em `04-alteracoes.md` | Cap. 19, p. 451 |
| Idempotência e recomeço | Silver recriada; fatos apagados por ano e reinseridos | Cap. 19, p. 486 |

## 5. O processo de 4 passos aplicado

1. **Processo de negócio:** divulgação anual de indicadores municipais do IBGE (população estimada e PIB).
2. **Grão:** uma linha por município por ano, em cada fato. É a promessa que todo o resto respeita.
3. **Dimensões:** quem/onde (`dim_municipio`), quando (`dim_ano`), de qual carga (`dim_auditoria`).
4. **Fatos:** população (`populacao`) e PIB com valor adicionado por setor, todos coerentes com o grão.

Se alguém perguntar "por que dois fatos e não um?": porque são **processos com fontes e coberturas
diferentes** (a tabela de população e a de PIB têm períodos próprios). Cada processo gera ao menos uma
tabela de fatos (Cap. 2, p. 52, "Value Chain"). A integração acontece pelas dimensões conformadas, não
juntando fato com fato.

## 6. Aditividade: o ponto que mais derruba em entrevista

| Medida | Soma entre municípios | Soma entre anos | Observação |
|---|---|---|---|
| `populacao` | Sim | **Não** (estoque, semiaditivo) | Somar anos não faz sentido; use o ano ou a média |
| `pib_mil_reais` e VABs | Sim | Cuidado | Valores a **preços correntes**: somar anos mistura inflação |
| PIB per capita | **Não** (razão) | Não | Guarde os componentes e calcule a razão depois (Cap. 2, p. 42) |

É por isso que o PIB per capita **não** é coluna do fato: ele é calculado na consulta, somando PIB e
população separadamente e só então dividindo.

## 7. SCD tipo 2 na prática

Município "Recife" muda de mesorregião em 2022 (o demo simula isso):

| municipio_key | municipio_id | mesorregiao_nome | início | fim | atual |
|---|---|---|---|---|---|
| 12 | 2611606 | Metropolitana de Recife | 1900-01-01 | 2021-12-31 | não |
| 190 | 2611606 | Mesorregião ALTERADA (demo) | 2022-01-01 | 9999-12-31 | sim |

- Fatos de 2019 a 2021 apontam para a chave 12 (**como era**). Fatos de 2022 em diante, para a 190.
- Por isso a chave natural **não pode** ser a chave primária da dimensão: ela repete.
- A associação é feita pela **data de referência do fato** caindo entre início e fim (`atribuir_chaves`).
- Rodar a gold de novo sem mudança na origem não cria versão nova (comparação por hash dos atributos).

Limitação consciente: aqui a vigência da mudança é a **data da carga** (ou a que você informa em
`--vigencia`). Num sistema real, usa-se a data em que a mudança aconteceu no negócio, quando a origem
informa.

## 8. Idempotência: poder rodar duas vezes

| Camada | Estratégia | Efeito de rodar de novo |
|---|---|---|
| Bronze | Só acrescenta | Mais uma carga no histórico (desejado) |
| Silver | Recriada a partir da última carga da bronze | Mesmo resultado |
| Gold, dimensões | SCD2 por hash | Sem mudança, sem linhas novas |
| Gold, fatos | `DELETE` dos anos da carga e `INSERT` | Sem duplicatas |

Essa propriedade é o que permite reprocessar e usar um orquestrador com tentativas automáticas.

## 9. Perguntas para você se fazer (e responder em voz alta)

1. Se eu apagar a pasta `data/` de uma tabela direto no SeaweedFS, o catálogo ainda lista a tabela? E o que acontece ao consultar?
2. Por que dois jobs gravando na mesma tabela Iceberg ao mesmo tempo não corrompem nada?
3. Por que a bronze guarda texto JSON e não já colunas?
4. Qual a diferença entre chave substituta e chave durável, e por que a dimensão tem as duas?
5. O que quebraria se a chave primária da dimensão fosse o código IBGE?
6. Por que o fato aponta para `municipio_key = 0` em vez de ficar nulo?
7. Por que não existe a coluna "PIB per capita" no fato?
8. O que é um drill across e por que não se junta fato com fato?

As respostas estão distribuídas nas seções acima e em `03-apresentacao.md`.
