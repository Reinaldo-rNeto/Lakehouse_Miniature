"""Roda bronze, silver e gold em sequencia."""
from _boot import iniciar

from lake.pipeline import executar_bronze, executar_gold, executar_silver

if __name__ == "__main__":
    settings, spark, lake = iniciar("run_all")
    print("== bronze ==")
    executar_bronze(spark, lake, settings)
    print("== silver ==")
    executar_silver(spark, lake)
    print("== gold ==")
    executar_gold(spark, lake)
