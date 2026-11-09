# 04. Alterações: "se pedirem X, faça Y"

Receitas para mudanças prováveis. Cada uma diz onde mexer, o trecho de código e como conferir.
Os trechos marcados **(não executado)** seguem a documentação do Iceberg e do Spark, mas não foram
rodados nesta stack: teste antes de prometer.

## Fluxo para qualquer mudança

1. Edite o código em `lake/` (o Compose monta a pasta do projeto dentro do container, então não precisa reconstruir a imagem).
2. Rode os testes: `python -m pytest -q tests` (ou `docker compose run --rm spark python -m pytest -q tests`).
3. Rode os jobs afetados e confira as contagens e uma consulta no Trino.
4. Atualize o teste correspondente. Mudança sem teste é mudança que volta a quebrar.

Mapa de "onde mora cada coisa":

| Quero mexer em... | Arquivo |
|---|---|
| De onde vêm os dados, URLs, variáveis | `lake/ibge.py` |
| Leitura e achatamento do JSON, regras de qualidade, SCD2, fatos | `lake/transforms.py` |
| Ordem das etapas, recarga, auditoria | `lake/pipeline.py` |
| Como grava (partição, propriedades da tabela) | `lake/io.py` |
| Conexão com o catálogo e o S3 | `lake/session.py` e `docker-compose.yml` |
| Consultas de análise | `sql/trino/` |

---

## Dados

### 1. Mudar período ou UF

No `.env`: `IBGE_PERIODOS=2015|2016|2017|...` e `IBGE_UF=35` (35 é São Paulo; 26 é Pernambuco).
Os períodos aceitam vários anos separados por `|`. **Trocar a UF mistura dados de UFs diferentes** na
gold, porque os fatos só apagam os anos que estão sendo recarregados. Para trocar de UF, recrie tudo
(`docker compose down -v`) ou implemente a receita 6.

### 2. Adicionar uma medida ao fato de PIB

1. Descubra o id da variável: `https://servicodados.ibge.gov.br/api/v3/agregados/5938/metadados`.
2. Em `lake/ibge.py`, acrescente em `VARIAVEIS_PIB`: `"id": ("nome_da_coluna", "palavra esperada no nome")`.
3. A tabela Iceberg já existe, então acrescente a coluna antes de recarregar:

```bash
docker compose run --rm spark python -i jobs/05_explorar.py
>>> spark.sql("ALTER TABLE lake.gold.fato_pib ADD COLUMNS (nova_medida_mil_reais decimal(24,3))")
```

4. Rode silver e gold. Conferir: `DESCRIBE iceberg.gold.fato_pib` no Trino mostra a coluna nova.

Isso é a **evolução de esquema** do Iceberg: foi só metadado, nenhum arquivo foi reescrito.

### 3. Adicionar uma fonte nova e um terceiro fato

1. `lake/ibge.py`: crie a `Fonte` em `fontes()` (URL, nome do arquivo offline, id do agregado).
2. Salve uma amostra em `tests/fixtures/` no mesmo formato para testar.
3. `lake/transforms.py`: inclua o agregado em `parse_observacoes` (parâmetro `agregados`) e crie `preparar_fato_xxx`.
4. `lake/pipeline.py`: acrescente a tabela no laço de fatos de `executar_gold`.
5. Atualize `docs/06-modelo-gold.md` (nova linha na matriz de barramento).
6. Conferir: o novo fato reaproveita `dim_municipio` e `dim_ano` (dimensões conformadas); a consulta de drill across funciona sem alterar as dimensões.

### 4. Acumular períodos em vez de recarregar tudo (carga incremental simples)

Hoje a silver usa **só a última carga** da bronze. Se você buscar apenas `2024`, a silver perde os anos
anteriores. Para acumular, use todas as cargas válidas para as observações (a deduplicação por chave
já escolhe a mais recente). Em `lake/pipeline.py`, `executar_silver`:

```python
ultimo = t.ultimo_lote_por_fonte(bronze)               # municipios: so o ultimo cadastro
todas = bronze.where("http_status = 200")              # observacoes: todas as cargas
observacoes = t.parse_observacoes(todas)               # dedupe por chave ja esta em aplicar_qualidade
```

Conferir: carregue `2019|2020`, depois só `2021`; a silver deve ter os três anos.

### 5. Pular a silver quando a origem não mudou

A bronze guarda `payload_sha256`. Compare o hash da carga nova com o da anterior; se for igual,
não há o que reprocessar. É a versão "barata" de captura de mudanças (Cap. 19, p. 451, técnica de
comparação completa com checksum).

### 6. Mais de uma UF

Em `fontes()`, a localidade `N6[N3[26]]` aceita várias UFs: `N6[N3[26,35]]`. O cadastro de municípios
é por estado: faça um laço sobre as UFs e grave uma resposta bruta por UF (com a UF na coluna `fonte`
ou numa coluna nova).

---

## Modelagem

### 7. Nova dimensão ou atributo derivado (ex.: faixa de população)

Para um atributo derivado, calcule na silver ou na `aplicar_scd2`. Para uma dimensão nova (ex.:
`dim_porte`): construa o DataFrame (como `construir_dim_ano`), grave em `gold`, adicione a chave nos
fatos dentro de `atribuir_chaves` e inclua a coluna na matriz. Ideia do livro (Cap. 2): faixas de valor como
atributo de dimensão (seções "Aggregated Facts as Dimension Attributes" e "Dynamic Value Bands"; confira a página na sua edição).
Se o atributo entrar em `ATRIBUTOS_MUNICIPIO`, ele passa a ser rastreado pelo SCD2 automaticamente.

### 8. Misturar tipo 1 e tipo 2 na mesma dimensão

Hoje **todos** os atributos são tipo 2. Para que, por exemplo, o nome seja tipo 1 (sobrescreve, sem
histórico):

1. Separe `ATRIBUTOS_TIPO2` e `ATRIBUTOS_TIPO1` em `transforms.py`.
2. Calcule `_hash_atributos()` só com os de tipo 2.
3. Para os de tipo 1, depois do SCD2, atualize o valor em **todas** as linhas do mesmo `municipio_id` com um `join` pela chave natural.

Conferir: mudar um atributo tipo 1 não cria linha; mudar um tipo 2 cria. O livro descreve ambos no
Cap. 5 (tipo 1 em p. 149, tipo 2 em p. 150). Tem teste a adaptar:
`test_scd2_mudanca_cria_nova_versao_e_encerra_a_antiga`.

### 9. Fato diário e dimensão de data

Os fatos do IBGE são anuais, então `dim_ano` basta. Para fatos diários crie uma `dim_data` com chave
inteira `AAAAMMDD` (o livro admite chave inteira significativa só para a dimensão de data, Cap. 2):

```python
datas = spark.sql(
    "SELECT explode(sequence(to_date('2015-01-01'), to_date('2035-12-31'), interval 1 day)) AS data"
).selectExpr(
    "cast(date_format(data, 'yyyyMMdd') as int) AS data_key", "data",
    "year(data) AS ano", "month(data) AS mes", "quarter(data) AS trimestre",
    "date_format(data, 'EEEE') AS dia_semana",
    "dayofweek(data) IN (1, 7) AS fim_de_semana",
)
```

Inclua também uma linha para "data desconhecida" (Cap. 2).

### 10. Dimensão com muitos-para-muitos (tabela ponte)

Se um fato precisar de vários valores de uma dimensão ao mesmo tempo (ex.: município em várias
regiões de planejamento), a solução é uma tabela ponte (Cap. 2, seção sobre bridge tables; confira a página na sua edição). Não existe no projeto hoje.

---

## Qualidade

### 11. Nova regra de qualidade

Em `aplicar_qualidade` (`transforms.py`), acrescente uma ramificação em `F.when(...)`. A ordem importa:
a primeira regra que casar vence. Exemplo, população negativa:

```python
.when((F.col("variavel_id") == "9324") & F.col("valor_texto").rlike(r"^-\d"), F.lit("R04_POPULACAO_NEGATIVA"))
```

Coloque **antes** da regra de valor não numérico. Adicione um caso em
`test_qualidade_rejeita_lixo_e_remove_duplicata`. Conferir no Trino: `SELECT regra, count(*) FROM iceberg.silver.dq_rejeitados GROUP BY regra`.

### 12. Parar o pipeline quando a qualidade for ruim

Em `executar_silver`, depois de contar rejeitadas, levante um erro se passar de um limite
(`if n_rej / total > 0.01: raise ValueError(...)`). O orquestrador marca a execução como falha e a gold
não roda sobre dado ruim.

---

## Iceberg e armazenamento

### 13. Particionar tabelas

Em `lake/io.py`, `substituir`, antes de `.createOrReplace()`:

```python
writer = df.writeTo(nome).using("iceberg").tableProperty("format-version", "2")
writer = writer.partitionedBy(F.col("ano_key"))      # fato: por ano
# bronze: F.days("_ingerido_em") particiona por dia de ingestao (particionamento "oculto")
writer.createOrReplace()
```

Conferir no Trino: `SELECT * FROM iceberg.gold."fato_pib$partitions"`. Com poucos dados, partição
demais só cria arquivos pequenos; mostre que sabe disso.

### 14. Voltar a um estado anterior (rollback) e viagem no tempo

Ver snapshots (Trino): `SELECT snapshot_id, committed_at, operation FROM iceberg.gold."fato_pib$snapshots"`.
Consultar o passado: `SELECT * FROM iceberg.gold.fato_pib FOR VERSION AS OF <snapshot_id>`.
Desfazer a tabela inteira (Spark, **não executado**):

```sql
CALL lake.system.rollback_to_snapshot('gold.fato_pib', <snapshot_id>)
```

### 15. Manutenção das tabelas (não executado)

Muitas escritas pequenas geram muitos arquivos e snapshots. No Spark:

```sql
CALL lake.system.rewrite_data_files(table => 'gold.fato_pib');
CALL lake.system.expire_snapshots(table => 'gold.fato_pib', retain_last => 5);
CALL lake.system.remove_orphan_files(table => 'gold.fato_pib');
```

No Trino: `ALTER TABLE ... EXECUTE optimize` e
`EXECUTE expire_snapshots(retention_threshold => '7d')` (já comentados em `03_iceberg_por_dentro.sql`).
`expire_snapshots` apaga a possibilidade de viagem no tempo além do que for mantido.

### 16. Separar as camadas em buckets diferentes (não executado)

1. No `docker-compose.yml`, em `s3-init`, troque o laço por `for b in bronze silver gold; do $$S3 s3 mb s3://$$b || true; done`.
2. Em `io.py`, `substituir`, defina a localização: `.tableProperty("location", f"s3://{camada}/{tabela}")`.

Conferir no navegador do SeaweedFS que cada camada ficou no seu bucket. A propriedade `location` na
criação da tabela é suportada pelo Iceberg, mas confirme com o catálogo REST usado.

### 17. Trocar o SeaweedFS por MinIO ou S3 da AWS

Em `.env`/compose: endpoint (`S3_ENDPOINT`), chaves e região. Para S3 da AWS, remova o `endpoint` e
`path-style-access` de `session.py`, do compose (`rest`) e de `iceberg.properties`. O código das
transformações não muda: é a vantagem do S3 como interface.

### 18. Trocar o catálogo (não executado)

O que muda é só configuração, em três lugares:

| Onde | O que |
|---|---|
| Compose | Trocar o serviço `rest` pelo catálogo escolhido (Polaris, Nessie, Lakekeeper) com seu banco e autenticação |
| `lake/session.py` | `uri` do novo catálogo, mais autenticação (`credential`/`token`) e `warehouse` se o catálogo exigir |
| `config/trino/iceberg.properties` | `iceberg.rest-catalog.uri` e as propriedades de segurança do catálogo |

Para Hive Metastore: no Spark `type=hive` e `uri=thrift://...:9083`; no Trino `iceberg.catalog.type=hive_metastore`
e `hive.metastore.uri=thrift://...:9083`. Tabelas já criadas precisam ser registradas no catálogo novo.

---

## Operação

### 19. Agendar e orquestrar

Cron simples (para demonstrar):

```
0 6 * * *  cd /caminho/lakehouse-ibge && docker compose run --rm spark python jobs/run_all.py
```

Orquestrador de verdade (Airflow, Dagster, Prefect): cada job vira uma tarefa, com dependência
bronze, silver, gold; tentativas automáticas e alerta de falha. Como os jobs são idempotentes,
repetir uma tarefa é seguro.

### 20. Carga incremental de verdade com MERGE (exercício, não executado)

Para a dimensão, o equivalente em SQL do que `aplicar_scd2` faz é o padrão clássico de `MERGE INTO` com
uma união que duplica as linhas alteradas (uma para encerrar a versão antiga, outra para inserir a nova):

```sql
MERGE INTO lake.gold.dim_municipio t
USING (
  SELECT s.municipio_id AS chave_merge, s.* FROM staging s
  UNION ALL
  SELECT NULL AS chave_merge, s.* FROM staging s
  JOIN lake.gold.dim_municipio d
    ON s.municipio_id = d.municipio_id AND d.registro_atual AND s.atributos_hash <> d.atributos_hash
) u
ON t.municipio_id = u.chave_merge AND t.registro_atual
WHEN MATCHED AND t.atributos_hash <> u.atributos_hash
  THEN UPDATE SET registro_atual = false, data_fim_vigencia = date_sub(current_date(), 1)
WHEN NOT MATCHED THEN INSERT (...) VALUES (...)
```

É um bom exercício adaptar isso, gerando a chave substituta e a data de início, e comparar com a
versão em DataFrame do projeto. O livro trata do gerenciador de SCD no Cap. 19, p. 464.

### 21. Conectar uma ferramenta de BI

Trino tem conectores para Superset, Metabase e Tableau; para Power BI, via driver ODBC de terceiros.
Aponte para o catálogo `iceberg`, esquema `gold`. A gold desnormalizada (estrela) é o que o BI espera.

### 22. Rodar o Spark em cluster

Em `session.py` troque `master("local[*]")` por o master do cluster (Kubernetes, YARN ou um serviço
gerenciado) e inclua os jars do Iceberg no cluster. As transformações não mudam.

---

## Para produção, o que falta

- Catálogo com autenticação e autorização; segredo em cofre; TLS entre serviços.
- Backup do catálogo e do armazenamento; política de retenção de snapshots.
- Orquestração, monitoramento, alertas, linhagem.
- Testes de dados no pipeline e contratos com as fontes.
- Spark e Trino em cluster, com limites de recurso.
