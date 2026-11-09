"""Criacao da SparkSession. No backend iceberg, liga o catalogo REST e o S3FileIO."""
import time
import urllib.request

from pyspark.sql import SparkSession

from lake.config import Settings


def _esperar_catalogo(rest_uri, tentativas=40):
    # O catalogo pode ainda estar subindo quando o job comeca
    url = f"{rest_uri}/v1/config"
    ultimo_erro = None
    for _ in range(tentativas):
        try:
            with urllib.request.urlopen(url, timeout=3) as resp:
                if resp.status == 200:
                    return
        except Exception as erro:  # noqa: BLE001
            ultimo_erro = erro
        time.sleep(3)
    raise RuntimeError(f"Catalogo Iceberg nao respondeu em {url}: {ultimo_erro}")


def build_spark(settings: Settings, app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )

    if settings.backend == "iceberg":
        _esperar_catalogo(settings.rest_uri)
        v = settings.iceberg_version
        c = settings.catalog
        pacotes = ",".join(
            [
                f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{v}",
                f"org.apache.iceberg:iceberg-aws-bundle:{v}",
            ]
        )
        builder = (
            builder.config("spark.jars.packages", pacotes)
            .config("spark.jars.ivy", "/root/.ivy2")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(f"spark.sql.catalog.{c}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{c}.type", "rest")
            .config(f"spark.sql.catalog.{c}.uri", settings.rest_uri)
            .config(f"spark.sql.catalog.{c}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config(f"spark.sql.catalog.{c}.s3.endpoint", settings.s3_endpoint)
            .config(f"spark.sql.catalog.{c}.s3.path-style-access", "true")
            .config(f"spark.sql.catalog.{c}.s3.access-key-id", settings.s3_access_key)
            .config(f"spark.sql.catalog.{c}.s3.secret-access-key", settings.s3_secret_key)
            .config(f"spark.sql.catalog.{c}.client.region", settings.s3_region)
        )

    return builder.getOrCreate()
