"""Console Python com `spark`, `lake` e `settings` prontos. Rode com: python -i jobs/05_explorar.py"""
from _boot import iniciar

settings, spark, lake = iniciar("05_explorar")
print("Variaveis: spark, lake, settings. Exemplo: lake.ler('gold', 'fato_pib').show()")
