"""Leitura e escrita de tabelas, escondendo o backend.

backend "iceberg": tabelas Iceberg no catalogo REST (SeaweedFS por baixo).
backend "local":   Parquet em disco, so para testar a logica sem Docker.
Os jobs e as transformacoes nao sabem qual backend esta ativo.
"""
import os
import shutil

from pyspark.sql import DataFrame, SparkSession

from lake.config import CAMADAS, Settings


class Lake:
    def __init__(self, spark: SparkSession, settings: Settings):
        self.spark = spark
        self.settings = settings
        self.iceberg = settings.backend == "iceberg"

    # nomes e caminhos

    def _nome(self, camada, tabela):
        return f"{self.settings.catalog}.{camada}.{tabela}"

    def _caminho(self, camada, tabela):
        return os.path.join(self.settings.local_dir, camada, tabela)

    # estrutura

    def criar_camadas(self):
        for camada in CAMADAS:
            if self.iceberg:
                self.spark.sql(
                    f"CREATE NAMESPACE IF NOT EXISTS {self.settings.catalog}.{camada}"
                )
            else:
                os.makedirs(os.path.join(self.settings.local_dir, camada), exist_ok=True)

    def existe(self, camada, tabela) -> bool:
        if self.iceberg:
            return self.spark.catalog.tableExists(self._nome(camada, tabela))
        return os.path.isdir(self._caminho(camada, tabela))

    # leitura

    def ler(self, camada, tabela) -> DataFrame:
        if self.iceberg:
            return self.spark.table(self._nome(camada, tabela))
        return self.spark.read.parquet(self._caminho(camada, tabela))

    # escrita

    def substituir(self, df: DataFrame, camada, tabela):
        """Cria a tabela ou troca todo o conteudo. Idempotente."""
        # Materializa antes: evita ler e escrever a mesma tabela no mesmo plano
        df = df.localCheckpoint(eager=True)
        if self.iceberg:
            (
                df.writeTo(self._nome(camada, tabela))
                .using("iceberg")
                .tableProperty("format-version", "2")
                .createOrReplace()
            )
        else:
            self._escrever_local(df, camada, tabela, "overwrite")

    def acrescentar(self, df: DataFrame, camada, tabela):
        """Adiciona linhas; cria a tabela se ainda nao existir."""
        if not self.existe(camada, tabela):
            self.substituir(df, camada, tabela)
        elif self.iceberg:
            df.writeTo(self._nome(camada, tabela)).append()
        else:
            self._escrever_local(df.localCheckpoint(eager=True), camada, tabela, "append")

    def apagar_onde(self, camada, tabela, condicao_sql: str):
        """DELETE FROM tabela WHERE condicao. Usado para recarga idempotente de fatos."""
        if not self.existe(camada, tabela):
            return
        if self.iceberg:
            self.spark.sql(f"DELETE FROM {self._nome(camada, tabela)} WHERE {condicao_sql}")
        else:
            restante = self.ler(camada, tabela).where(f"NOT ({condicao_sql})")
            self._escrever_local(restante.localCheckpoint(eager=True), camada, tabela, "overwrite")

    def _escrever_local(self, df, camada, tabela, modo):
        caminho = self._caminho(camada, tabela)
        if modo == "overwrite" and os.path.isdir(caminho):
            shutil.rmtree(caminho)
        df.coalesce(1).write.mode("append" if modo == "append" else "overwrite").parquet(caminho)
