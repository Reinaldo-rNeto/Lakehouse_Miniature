"""Bronze: busca os dados do IBGE e guarda a resposta crua em lake.bronze.ibge_respostas_brutas."""
from _boot import iniciar

from lake.pipeline import executar_bronze

if __name__ == "__main__":
    settings, spark, lake = iniciar("01_bronze_ingest")
    executar_bronze(spark, lake, settings)
