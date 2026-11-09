"""Silver: interpreta o JSON da bronze, tipa, valida e separa o que foi rejeitado."""
from _boot import iniciar

from lake.pipeline import executar_silver

if __name__ == "__main__":
    settings, spark, lake = iniciar("02_silver_transform")
    executar_silver(spark, lake)
