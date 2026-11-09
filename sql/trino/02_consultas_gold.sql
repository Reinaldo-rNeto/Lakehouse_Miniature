-- Consultas de negocio na gold. Rodar: docker compose exec trino trino -f /sql/trino/02_consultas_gold.sql

-- 1) Os 10 maiores PIBs do ano mais recente carregado (so versoes atuais da dimensao)
SELECT m.municipio_nome, m.mesorregiao_nome, f.ano_key, f.pib_mil_reais
FROM iceberg.gold.fato_pib f
JOIN iceberg.gold.dim_municipio m ON m.municipio_key = f.municipio_key
WHERE f.ano_key = (SELECT max(ano_key) FROM iceberg.gold.fato_pib)
ORDER BY f.pib_mil_reais DESC NULLS LAST
LIMIT 10;

-- 2) Composicao do PIB por mesorregiao (agregacao simples: um fato, uma dimensao)
SELECT m.mesorregiao_nome,
       sum(f.pib_mil_reais)               AS pib,
       sum(f.vab_agropecuaria_mil_reais)  AS agropecuaria,
       sum(f.vab_industria_mil_reais)     AS industria,
       sum(f.vab_servicos_mil_reais)      AS servicos,
       sum(f.vab_adm_publica_mil_reais)   AS adm_publica
FROM iceberg.gold.fato_pib f
JOIN iceberg.gold.dim_municipio m ON m.municipio_key = f.municipio_key
WHERE f.ano_key = (SELECT max(ano_key) FROM iceberg.gold.fato_pib)
GROUP BY m.mesorregiao_nome
ORDER BY pib DESC;

-- 3) DRILL ACROSS: PIB per capita por mesorregiao.
-- Dois fatos (PIB e populacao) NUNCA se juntam direto. Cada um e agregado separadamente
-- e o resultado e alinhado pelo atributo conformado (mesorregiao, ano). Cap. 2 do livro.
WITH pib AS (
    SELECT m.mesorregiao_nome, f.ano_key, sum(f.pib_mil_reais) AS pib_mil_reais
    FROM iceberg.gold.fato_pib f
    JOIN iceberg.gold.dim_municipio m ON m.municipio_key = f.municipio_key
    GROUP BY m.mesorregiao_nome, f.ano_key
),
pop AS (
    SELECT m.mesorregiao_nome, f.ano_key, sum(f.populacao) AS populacao
    FROM iceberg.gold.fato_populacao f
    JOIN iceberg.gold.dim_municipio m ON m.municipio_key = f.municipio_key
    GROUP BY m.mesorregiao_nome, f.ano_key
)
SELECT pib.mesorregiao_nome,
       pib.ano_key,
       pib.pib_mil_reais,
       pop.populacao,
       round(pib.pib_mil_reais * 1000.0 / pop.populacao, 2) AS pib_per_capita_reais
FROM pib
JOIN pop ON pop.mesorregiao_nome = pib.mesorregiao_nome AND pop.ano_key = pib.ano_key
ORDER BY pib.ano_key DESC, pib_per_capita_reais DESC;

-- 4) SCD tipo 2: historico de um municipio (rode depois do jobs/04_demo_scd2.py)
SELECT municipio_key, municipio_nome, mesorregiao_nome,
       data_inicio_vigencia, data_fim_vigencia, registro_atual
FROM iceberg.gold.dim_municipio
WHERE municipio_id = '2611606'
ORDER BY data_inicio_vigencia;

-- 5) Como era x como ficou: populacao por mesorregiao "como era na epoca" (chave da versao)
SELECT m.mesorregiao_nome, f.ano_key, sum(f.populacao) AS populacao
FROM iceberg.gold.fato_populacao f
JOIN iceberg.gold.dim_municipio m ON m.municipio_key = f.municipio_key
GROUP BY m.mesorregiao_nome, f.ano_key
ORDER BY f.ano_key, m.mesorregiao_nome;

-- 6) Qualidade: o que foi rejeitado na silver
SELECT regra, count(*) AS qtd FROM iceberg.silver.dq_rejeitados GROUP BY regra;

-- 7) Auditoria: de qual carga e qual versao do codigo vieram os fatos
SELECT a.auditoria_key, a.batch_id, a.carregado_em, a.versao_pipeline, count(*) AS linhas_pib
FROM iceberg.gold.fato_pib f
JOIN iceberg.gold.dim_auditoria a ON a.auditoria_key = f.auditoria_key
GROUP BY a.auditoria_key, a.batch_id, a.carregado_em, a.versao_pipeline;
