# 03. Como apresentar

## A mensagem em uma frase

> Construí um lakehouse completo e reprodutível: dado real do IBGE entra cru na bronze, é validado na
> silver e vira um modelo dimensional na gold, tudo em tabelas Iceberg no SeaweedFS, lidas por Spark
> e por Trino a partir do mesmo catálogo.

Se o tempo apertar, essa frase mais o demo de 3 minutos (seção "Demo") já sustentam a apresentação.

## Roteiro de 12 minutos

| Min | Bloco | O que dizer | O que mostrar |
|---|---|---|---|
| 0 a 1 | Objetivo | O exercício pedia stack aberta, medalhão e Iceberg de ponta a ponta. Escolhi dado público do IBGE para ser reproduzível por qualquer pessoa | Slide ou README, só o diagrama |
| 1 a 3 | Arquitetura | Cinco camadas independentes: armazenamento, formato de arquivo, formato de tabela, catálogo, motor. Spark escreve, Trino lê, ambos pelo mesmo catálogo | Diagrama do README e `docker compose ps` |
| 3 a 5 | Medalhão | Bronze cru e só acrescenta. Silver tipada e validada, reconstruível. Gold modelada para consumo | Navegador do SeaweedFS: `lakehouse/bronze`, `silver`, `gold` |
| 5 a 8 | Demo | Rodar `run_all` (ou mostrar a saída já pronta) e consultar a gold no Trino | Seção "Demo" abaixo |
| 8 a 10 | Modelagem | Grão município por ano. Dimensões conformadas. SCD2. Registro "não informado". Auditoria | `docs/06-modelo-gold.md` e demo do SCD2 |
| 10 a 12 | Limites e próximos passos | O que é de estudo e o que mudaria em produção | Seção "Limites" abaixo |

## Demo (ordem sugerida, com tempo)

Prepare **antes**: stack no ar (`docker compose up -d`), jobs já rodados uma vez, terminal limpo, abas
abertas (SeaweedFS `:8888`, Trino `:8080`).

1. **(30 s) Estrutura.** Abra `http://localhost:8888/buckets/lakehouse/`. "Essa é a estrutura medalhão
   no armazenamento de objetos."
2. **(60 s) Bronze e silver ao vivo.** Rode `docker compose run --rm spark python jobs/02_silver_transform.py`
   (rápido, porque a bronze já existe). Aponte: "a silver é reconstruída da bronze; se eu errar o
   parsing, não preciso chamar a API de novo".
3. **(60 s) Gold.** Rode `jobs/03_gold_dimensional.py` de novo e mostre que **não duplica nada**
   (idempotência): as contagens são as mesmas.
4. **(60 s) Consulta de negócio.** No Trino, a consulta 3 de `02_consultas_gold.sql` (PIB per capita).
   Explique em uma frase: "dois fatos, ligados pela dimensão conformada, sem juntar fato com fato".
5. **(60 s) SCD2.** `jobs/04_demo_scd2.py`: mostre as duas versões de Recife e a consulta 5, em que
   os anos antigos continuam na mesorregião antiga.
6. **(30 s) Iceberg por dentro.** `03_iceberg_por_dentro.sql`: snapshots e arquivos. "Cada escrita é
   um snapshot; posso voltar no tempo."

Dica: grave a tela de uma execução completa de antemão e tenha o vídeo como reserva.

## Plano B

- API do IBGE fora do ar: use a cópia local das respostas reais (`docs/02-passo-a-passo.md`, "Plano B").
  Avise que a leitura é de uma cópia salva.
- Docker lento: mostre a saída que você já capturou em `docs/img/`.
- Pergunta fora do que sabe: "não testei isso, mas o caminho seria..." é melhor que improvisar.

## Perguntas prováveis e respostas curtas

**Por que Iceberg?** Formato de tabela aberto, com transações, snapshots e evolução de esquema, que
vários motores leem (Spark, Trino, Flink). Evita ficar preso a um motor. Delta e Hudi resolvem
problemas parecidos; Iceberg tem o catálogo REST padronizado.

**Por que um catálogo REST?** Porque o catálogo é o que faz a troca atômica do ponteiro de metadados
e precisa ser o mesmo para todos os motores. O REST é uma interface padrão: troco a implementação
(Polaris, Nessie, Lakekeeper) sem mudar Spark nem Trino.

**A imagem do catálogo serve para produção?** Não. É a imagem de teste do projeto Iceberg: SQLite,
sem autenticação. Em produção: catálogo com banco robusto, autenticação e autorização.

**Por que SeaweedFS e não MinIO ou S3?** O exercício pediu SeaweedFS. Como ele fala a API S3, o resto
da stack não percebe diferença. Trocar por MinIO ou S3 é mudar endpoint e credenciais.

**Spark e Trino, por que os dois?** Spark para processamento em lote e transformações em código; Trino
para SQL interativo e BI. O mesmo catálogo faz os dois enxergarem as mesmas tabelas.

**O que é um snapshot?** O estado completo da tabela num instante. Toda escrita cria um novo. Leitor
sempre lê um snapshot consistente, e dá para consultar ou voltar a um antigo.

**Como garante que uma carga com falha não deixa lixo?** A escrita só vale quando o catálogo troca o
ponteiro de metadados. Se o job cai antes, nada aparece. Os arquivos órfãos são removidos depois pela
manutenção (`remove_orphan_files`).

**É idempotente?** Sim, por camada: bronze só acrescenta (histórico), silver é recriada da bronze,
dimensão por hash dos atributos, fatos por apagar o ano e reinserir. Rodar duas vezes dá o mesmo.

**Por que a bronze guarda o JSON como texto?** Para não perder nada e poder reinterpretar. O parsing
(schema-on-read) fica na silver.

**O que é grão e por que declarar primeiro?** É o que uma linha do fato representa. Aqui, um município
em um ano. Todo fato e dimensão precisa ser coerente com o grão; misturar grãos quebra somas.

**Por que chave substituta?** A chave natural (código IBGE) se repete quando há várias versões do
mesmo município. A substituta identifica a versão. Também isola o DW de mudanças e erros da origem.

**O que é SCD2 e como você provou?** Dimensão que guarda histórico: mudança de atributo gera nova
linha com vigência. Provei com o demo: mudei a mesorregião de Recife e os fatos antigos continuaram
na versão antiga.

**Por que existe a chave 0?** Para o fato nunca ficar com chave estrangeira nula. Município não
encontrado aponta para "Não informado" e a consulta não perde linha (Cap. 2, p. 42).

**Por que PIB per capita não é coluna do fato?** É uma razão, não aditiva. Guardo os componentes e
calculo na consulta, somando antes de dividir.

**Como trata dado que chega atrasado?** Município novo entra com vigência desde 1900 para casar com
fatos antigos. Fato com município desconhecido usa a chave 0 e pode ser corrigido depois.

**Como escalaria?** Spark sai do modo local para um cluster (Kubernetes, YARN ou serviço gerenciado),
Trino ganha workers, o armazenamento é distribuído por natureza. O código das transformações não muda.

**E dados incrementais e tempo real?** Hoje é recarga completa por lote. Incremental: capturar
mudanças (CDC, Cap. 19, p. 451) e usar `MERGE INTO`. Tempo real: Kafka e Flink ou Spark Streaming
escrevendo em Iceberg. Receitas em `04-alteracoes.md`.

**Como orquestra?** Hoje os jobs são scripts. Em produção, Airflow ou Dagster chamam os mesmos scripts
com tentativas e alertas. A idempotência é o que torna isso seguro.

**E qualidade e governança?** Há regras na silver com tabela de rejeitados e uma dimensão de
auditoria ligando cada fato à carga. Evolução: contratos de dados, testes em pipeline, linhagem.

**E segurança?** Portas só em localhost, segredos no `.env` fora do Git. Em produção: autenticação no
catálogo, permissões por tabela, TLS, segredo em cofre.

## Limites (diga antes que perguntem)

- Catálogo de teste, sem autenticação.
- Spark em modo local, não em cluster.
- Recarga completa em vez de incremental.
- Vigência do SCD2 é a data da carga, não a data real da mudança no negócio.
- Fatos anuais: a dimensão de tempo é de ano. Para fatos diários, usaria uma `dim_data`.
- A stack Docker foi escrita a partir da documentação oficial e exemplos, e deve ser validada na
  primeira execução na sua máquina.

## Próximos passos (para fechar a apresentação)

1. Orquestração com Airflow ou Dagster.
2. Transformações versionadas com dbt ou SQLMesh.
3. Carga incremental com `MERGE INTO`.
4. Testes de qualidade e contratos de dados.
5. Catálogo de produção com autenticação.
6. Camada de consumo: BI conectado ao Trino (Superset, Metabase, Power BI).
