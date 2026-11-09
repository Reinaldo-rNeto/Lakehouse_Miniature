"""Preparacao comum dos jobs: configuracao, sessao Spark e acesso ao lake."""
import os
import sys

# Permite rodar "python jobs/01_..." de dentro da pasta do projeto
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lake.config import Settings  # noqa: E402
from lake.io import Lake  # noqa: E402
from lake.session import build_spark  # noqa: E402


def iniciar(nome_job):
    settings = Settings.from_env()
    spark = build_spark(settings, nome_job)
    spark.sparkContext.setLogLevel("ERROR")
    return settings, spark, Lake(spark, settings)
