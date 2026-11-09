"""Demonstracao do SCD tipo 2: muda um atributo de um municipio e mostra as versoes.

  python jobs/04_demo_scd2.py                      # altera o municipio padrao (Recife)
  python jobs/04_demo_scd2.py --restaurar          # volta ao dado original (gera uma 3a versao)
"""
import argparse
from datetime import date

from _boot import iniciar

from lake.pipeline import demo_scd2, executar_gold, executar_silver, versoes_do_municipio

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--municipio", default="2611606", help="codigo IBGE de 7 digitos (padrao: Recife)")
    ap.add_argument("--vigencia", default="2022-01-01", help="a partir de quando a mudanca vale")
    ap.add_argument("--restaurar", action="store_true", help="reprocessa silver e gold com o dado original")
    args = ap.parse_args()

    settings, spark, lake = iniciar("04_demo_scd2")
    print("\nVersoes ANTES:")
    versoes_do_municipio(lake, args.municipio).show(truncate=False)

    if args.restaurar:
        executar_silver(spark, lake)
        executar_gold(spark, lake, date.today())
    else:
        demo_scd2(spark, lake, args.municipio, date.fromisoformat(args.vigencia))

    print("\nVersoes DEPOIS:")
    versoes_do_municipio(lake, args.municipio).show(truncate=False)
