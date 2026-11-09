"""Configuracao lida de variaveis de ambiente. Nenhum segredo fica no codigo."""
import os
import re
from dataclasses import dataclass

CAMADAS = ("bronze", "silver", "gold")


def _env(nome, padrao=None):
    valor = os.environ.get(nome)
    return valor if valor not in (None, "") else padrao


@dataclass(frozen=True)
class Settings:
    backend: str          # "iceberg" (stack completa) ou "local" (Parquet em disco, sem Docker)
    catalog: str          # nome do catalogo Spark
    rest_uri: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_region: str
    iceberg_version: str
    local_dir: str
    ibge_uf: str
    ibge_periodos: str
    ibge_offline_dir: str

    @staticmethod
    def from_env():
        return Settings._validar(Settings._ler())

    @staticmethod
    def _validar(cfg):
        # Estes valores entram na URL da API do IBGE: aceite so o formato esperado.
        if not re.fullmatch(r"\d{2}", cfg.ibge_uf):
            raise ValueError("IBGE_UF deve ter 2 digitos (ex.: 26)")
        if not re.fullmatch(r"\d{4}(\|\d{4})*", cfg.ibge_periodos):
            raise ValueError("IBGE_PERIODOS deve ser anos separados por | (ex.: 2022|2023)")
        return cfg

    @staticmethod
    def _ler():
        return Settings(
            backend=_env("LAKE_BACKEND", "iceberg"),
            catalog=_env("LAKE_CATALOG", "lake"),
            rest_uri=_env("ICEBERG_REST_URI", "http://localhost:8181"),
            s3_endpoint=_env("S3_ENDPOINT", "http://localhost:8333"),
            s3_access_key=_env("S3_ACCESS_KEY", ""),
            s3_secret_key=_env("S3_SECRET_KEY", ""),
            s3_region=_env("S3_REGION", "us-east-1"),
            iceberg_version=_env("ICEBERG_VERSION", "1.10.1"),
            local_dir=_env("LAKE_LOCAL_DIR", "data/local_lake"),
            ibge_uf=_env("IBGE_UF", "26"),
            ibge_periodos=_env("IBGE_PERIODOS", "2019|2020|2021|2022|2023"),
            ibge_offline_dir=_env("IBGE_OFFLINE_DIR", ""),
        )
