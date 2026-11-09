import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("testes")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture()
def settings(tmp_path):
    from lake.config import Settings

    fixtures = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
    return Settings(
        backend="local",
        catalog="lake",
        rest_uri="",
        s3_endpoint="",
        s3_access_key="",
        s3_secret_key="",
        s3_region="us-east-1",
        iceberg_version="",
        local_dir=str(tmp_path / "lake"),
        ibge_uf="26",
        ibge_periodos="2019|2020|2021",
        ibge_offline_dir=fixtures,
    )
