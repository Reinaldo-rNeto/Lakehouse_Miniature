"""Gold: dimensoes (municipio SCD2, ano, auditoria) e fatos (populacao, PIB)."""
import argparse
from datetime import date

from _boot import iniciar

from lake.pipeline import executar_gold

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--vigencia", help="AAAA-MM-DD: data de inicio de vigencia de mudancas (padrao: hoje)")
    args = ap.parse_args()
    vigencia = date.fromisoformat(args.vigencia) if args.vigencia else None

    settings, spark, lake = iniciar("03_gold_dimensional")
    executar_gold(spark, lake, vigencia)
