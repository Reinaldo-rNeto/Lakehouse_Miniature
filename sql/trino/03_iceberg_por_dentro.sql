-- Olhando o Iceberg por dentro. Rodar: docker compose exec trino trino -f /sql/trino/03_iceberg_por_dentro.sql

-- Historico de snapshots: cada escrita (append, delete, replace) cria um snapshot
SELECT snapshot_id, parent_id, committed_at, operation
FROM iceberg.gold."fato_pib$snapshots"
ORDER BY committed_at;

-- Linha do tempo "quem era o snapshot atual em cada momento"
SELECT * FROM iceberg.gold."fato_pib$history" ORDER BY made_current_at;

-- Arquivos de dados de fato: onde cada Parquet esta no SeaweedFS e quantas linhas tem
SELECT file_path, record_count, file_size_in_bytes
FROM iceberg.gold."fato_pib$files";

-- Manifestos: o indice que aponta para os arquivos de dados
SELECT path, length, added_data_files_count, existing_data_files_count, deleted_data_files_count
FROM iceberg.gold."fato_pib$manifests";

-- Viagem no tempo: troque <SNAPSHOT_ID> por um valor da primeira consulta
-- SELECT count(*) FROM iceberg.gold.fato_pib FOR VERSION AS OF <SNAPSHOT_ID>;
-- SELECT count(*) FROM iceberg.gold.fato_pib FOR TIMESTAMP AS OF TIMESTAMP '2026-11-09 12:00:00 UTC';

-- Manutencao (descomente para testar; reescreve arquivos pequenos e remove snapshots antigos)
-- ALTER TABLE iceberg.gold.fato_pib EXECUTE optimize;
-- ALTER TABLE iceberg.gold.fato_pib EXECUTE expire_snapshots(retention_threshold => '7d');
