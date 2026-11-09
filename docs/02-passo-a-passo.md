# 02. Passo a passo: subir, rodar, conferir

Tempo estimado na primeira vez: 20 a 40 minutos, quase tudo download de imagens e de bibliotecas.
Nas vezes seguintes, 2 a 3 minutos.

## Pré-requisitos

- Docker Desktop (ou Docker Engine) com Compose v2 (`docker compose version`).
- Memória: reserve 6 GB para o Docker. No Windows com WSL2, ajuste em `.wslconfig`.
- Internet para baixar imagens, bibliotecas do Spark (uma vez) e chamar a API do IBGE.
- Portas livres em `127.0.0.1`: 8333, 8888, 9333, 8181 e 8080.

## Passo 0. Configurar

```bash
cp .env.example .env
```

Abra o `.env` e troque `S3_ACCESS_KEY` e `S3_SECRET_KEY` por valores seus (só letras e números).
Essas são as credenciais do SeaweedFS. Nunca coloque o `.env` no Git (já está no `.gitignore`).

## Passo 1. Subir a infraestrutura

```bash
docker compose up -d
docker compose ps
```

O que sobe, nesta ordem (o Compose espera cada um):

| Serviço | O que faz | Esperado em `ps` |
|---|---|---|
| `seaweedfs` | Armazenamento S3 | `healthy` |
| `s3-init` | Cria o bucket e as pastas do medalhão, depois termina | `exited (0)` |
| `rest` | Catálogo Iceberg | `running` |
| `trino` | SQL | `running` (leva 30 a 60 s para ficar pronto) |

Conferir cada peça:

```bash
curl http://localhost:8181/v1/config            # catalogo responde um JSON
docker compose logs s3-init                     # deve terminar com "estrutura medalhao criada"
docker compose logs trino | tail -5             # procure "SERVER STARTED"
```

**Estrutura de medalhão no SeaweedFS:** abra http://localhost:8888/buckets/lakehouse/ . Você deve ver
as pastas `bronze/`, `silver/` e `gold/` (por enquanto vazias). Esse é o item "monta uma estrutura de
medalhão no SeaweedFS" do exercício. Em S3 não existem pastas de verdade; elas são só prefixos nos
nomes dos objetos, e o `s3-init` cria marcadores para elas aparecerem.

## Passo 2. Construir a imagem do Spark (uma vez)

```bash
docker compose build spark
```

## Passo 3. Bronze: buscar na internet e guardar cru

```bash
docker compose run --rm spark python jobs/01_bronze_ingest.py
```

Na primeira execução o Spark baixa os jars do Iceberg (1 a 2 minutos; ficam em cache no volume
`ivy_cache`). Saída esperada, no formato:

```
  municipios: HTTP 200, ... bytes
  populacao: HTTP 200, ... bytes
  pib: HTTP 200, ... bytes
bronze: lote 20261109T120000-ab12cd gravado (3 respostas)
```

Conferir:

```bash
docker compose exec trino trino --execute "SELECT fonte, http_status, payload_bytes, _batch_id FROM iceberg.bronze.ibge_respostas_brutas"
```

No navegador do SeaweedFS, entre em `bronze/ibge_respostas_brutas/`: aparecem as pastas `metadata/`
(JSONs e listas do Iceberg) e `data/` (arquivos Parquet).

## Passo 4. Silver: interpretar, tipar, validar

```bash
docker compose run --rm spark python jobs/02_silver_transform.py
```

Esperado: `silver: 185 municipios, N observacoes validas, 0 rejeitadas` (Pernambuco tem 185 municípios;
o número de observações depende dos períodos e do que o IBGE publicou). Se houver rejeitadas, veja:

```bash
docker compose exec trino trino --execute "SELECT regra, count(*) FROM iceberg.silver.dq_rejeitados GROUP BY regra"
```

Os símbolos do IBGE (`-`, `...`, `X`) **não** são erro: viram valor nulo com `valor_status = 'indisponivel'`.

## Passo 5. Gold: modelo dimensional

```bash
docker compose run --rm spark python jobs/03_gold_dimensional.py
```

Esperado: `gold: dim_municipio=186, dim_ano=<anos com dado>, fato_populacao=..., fato_pib=...`
(186 = 185 municípios + o registro "não informado"). As linhas de cada fato são municípios vezes anos
com dado. Se o IBGE não publicou algum ano para a tabela de população, esse ano some do fato
correspondente: não é erro do pipeline.

## Passo 6. Consultar no Trino

```bash
docker compose exec trino trino -f /sql/trino/01_explorar.sql
docker compose exec trino trino -f /sql/trino/02_consultas_gold.sql
```

Ou interativo: `docker compose exec -it trino trino` e depois `USE iceberg.gold;`.
A consulta 3 do arquivo `02_...` é o **drill across** (PIB per capita), o ponto alto para mostrar.
Interface web do Trino: http://localhost:8080 (qualquer usuário).

## Passo 7. Demonstrar o SCD tipo 2

```bash
docker compose run --rm spark python jobs/04_demo_scd2.py
```

Saída: as versões do município Recife (código 2611606) antes (1 linha) e depois (2 linhas: a antiga
encerrada em 2021-12-31 e a nova vigente desde 2022-01-01). Depois, no Trino, rode a consulta 5 de
`02_consultas_gold.sql`: os fatos de 2019 a 2021 continuam na mesorregião antiga, os de 2022 em diante
na nova. Para desfazer: `python jobs/04_demo_scd2.py --restaurar` (gera uma terceira versão, porque
SCD2 nunca apaga histórico).

## Passo 8. Olhar o Iceberg por dentro

```bash
docker compose exec trino trino -f /sql/trino/03_iceberg_por_dentro.sql
```

Mostra snapshots, arquivos, manifestos e como fazer viagem no tempo. Compare com a pasta
`gold/fato_pib/metadata/` no navegador do SeaweedFS.

## Passo 9. Explorar com Spark

```bash
docker compose run --rm spark python -i jobs/05_explorar.py
>>> lake.ler("gold", "dim_municipio").show()
>>> spark.sql("SELECT * FROM lake.gold.fato_pib.snapshots").show()
```

## Passo 10. Desligar

```bash
docker compose down        # para tudo e MANTEM os dados (volumes)
docker compose down -v     # para tudo e APAGA os dados
```

Atenção: o estado do catálogo está no volume `catalog_data` e os arquivos no volume `seaweed_data`.
Apagar só um dos dois deixa as tabelas "quebradas" (catálogo sem arquivos ou arquivos sem catálogo).
Apague os dois juntos.

---

## Rodar sem Docker (só para estudar a lógica)

Precisa de Python 3.10 ou mais novo (testado na 3.13) e Java 17 ou mais novo.

```bash
pip install -r requirements.txt
LAKE_BACKEND=local IBGE_OFFLINE_DIR=tests/fixtures python jobs/run_all.py
python -m pytest -q tests
```

Isso usa Parquet em `data/local_lake` e os JSONs de teste, cujos **valores são fictícios**. Serve para
depurar transformações. Não mostra Iceberg, SeaweedFS nem Trino.

## Plano B para a apresentação: guardar as respostas reais antes

Se a internet ou a API do IBGE falhar na hora, salve uma cópia das respostas reais com antecedência:

```bash
docker compose run --rm spark python -m lake.ibge salvar data/ibge_snapshot
```

e rode os jobs com `IBGE_OFFLINE_DIR=/app/data/ibge_snapshot` (acrescente a variável no `.env`).
Como são respostas reais salvas, os números continuam válidos; diga na apresentação que a fonte foi
lida de uma cópia local.

## Problemas prováveis

| Sintoma | Causa provável | O que fazer |
|---|---|---|
| `manifest for ... not found` ou `pull access denied` no `up` | Tag de imagem inexistente | Troque a tag no `.env` (`SEAWEEDFS_TAG`, `TRINO_TAG`) ou use `latest` |
| `port is already allocated` | Porta em uso | Pare o que usa a porta ou mude o mapeamento no compose |
| `seaweedfs` nunca fica `healthy` | Falha ao iniciar | `docker compose logs seaweedfs`; confira se as chaves do `.env` têm só letras e números |
| `s3-init` repete "aguardando a API S3" | Credenciais ou serviço S3 fora do ar | Veja `logs seaweedfs`; confirme que `.env` está preenchido |
| Spark: `Catalogo Iceberg nao respondeu` | `rest` ainda subindo ou caiu | `docker compose logs rest`; confira o `s3-init` (o `rest` só inicia depois dele) |
| Spark: erro de download de pacotes (`unresolved dependency`) | Sem acesso ao Maven Central, proxy | Libere o acesso, ou baixe os dois jars e use `spark.jars` |
| `SignatureDoesNotMatch` / `403` em S3 | Chave diferente entre serviços | Garanta uma só `.env`; recrie com `docker compose down` e `up -d` |
| `NoSuchBucket` | `s3-init` não rodou | `docker compose up s3-init` |
| Erro de checksum ao gravar no S3 | SDK novo com armazenamento compatível | As variáveis `AWS_*_CHECKSUM_*` já estão no compose; confirme que não foram removidas |
| Trino: `Failed to list directory` ou erro de S3 | `iceberg.properties` ou chaves | Confira `config/trino/iceberg.properties` e o `.env` |
| Trino: tabela não aparece | O job Spark ainda não rodou ou usou outro catálogo | Rode os jobs; verifique `SHOW SCHEMAS FROM iceberg` |
| `Variaveis do IBGE com nome inesperado` | O IBGE mudou ou o id está errado | Ajuste `VARIAVEIS_PIB` em `lake/ibge.py` |
| Timeout na API do IBGE | Instabilidade do serviço | Tente de novo (há 3 tentativas) ou use o plano B |
| Spark sem memória | Pouca RAM para o Docker | Aumente a memória do Docker ou reduza `IBGE_PERIODOS` |

Para investigar qualquer coisa: `docker compose logs <servico>` e, no Spark, rode o job de novo; as
mensagens de erro do Python aparecem no terminal.
