-- Rodar: docker compose exec trino trino -f /sql/trino/01_explorar.sql
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.bronze;
SHOW TABLES FROM iceberg.silver;
SHOW TABLES FROM iceberg.gold;
DESCRIBE iceberg.gold.dim_municipio;
DESCRIBE iceberg.gold.fato_pib;
SHOW CREATE TABLE iceberg.gold.fato_pib;
