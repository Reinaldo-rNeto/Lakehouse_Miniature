"""Transformacoes puras (DataFrame entra, DataFrame sai). Nao sabem onde os dados moram.

Por serem puras, sao as pecas mais faceis de testar e de alterar.
Cada bloco aponta para o capitulo do livro (Kimball, 3a edicao) que ele implementa.
"""
import hashlib
from datetime import date

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession, Window

from lake.ibge import VARIAVEIS_PIB, VARIAVEL_POPULACAO, nome_confere

INICIO_PADRAO = "1900-01-01"
FIM_ABERTO = "9999-12-31"

# Simbolos que o IBGE usa no lugar de numero (sem dado, sigilo, nao se aplica...)
SIMBOLOS_IBGE = ["-", "..", "...", "X"]


# --------------------------------------------------------------------------
# BRONZE: guarda a resposta crua, sem interpretar (Cap. 19, subsistema de extracao)
# --------------------------------------------------------------------------

BRONZE_SCHEMA = T.StructType(
    [
        T.StructField("_batch_id", T.StringType(), False),
        T.StructField("_ingerido_em", T.TimestampType(), False),
        T.StructField("fonte", T.StringType(), False),
        T.StructField("url", T.StringType(), False),
        T.StructField("http_status", T.IntegerType(), False),
        T.StructField("payload", T.StringType(), False),
        T.StructField("payload_sha256", T.StringType(), False),
        T.StructField("payload_bytes", T.LongType(), False),
    ]
)


def bronze_df(spark: SparkSession, respostas, batch_id: str, ingerido_em) -> DataFrame:
    """respostas: lista de dicts com fonte, url, http_status, payload."""
    linhas = []
    for r in respostas:
        bruto = r["payload"].encode("utf-8")
        linhas.append(
            (
                batch_id,
                ingerido_em,
                r["fonte"],
                r["url"],
                int(r["http_status"]),
                r["payload"],
                hashlib.sha256(bruto).hexdigest(),
                len(bruto),
            )
        )
    return spark.createDataFrame(linhas, BRONZE_SCHEMA)


def ultimo_lote_por_fonte(bronze: DataFrame) -> DataFrame:
    """A silver le so a carga mais recente de cada fonte (a bronze guarda todas)."""
    w = Window.partitionBy("fonte").orderBy(F.col("_ingerido_em").desc(), F.col("_batch_id").desc())
    return (
        bronze.where(F.col("http_status") == 200)
        .withColumn("_rn", F.row_number().over(w))
        .where("_rn = 1")
        .drop("_rn")
    )


# --------------------------------------------------------------------------
# SILVER: tipos, estrutura plana, qualidade (Cap. 19, limpeza e conformidade)
# --------------------------------------------------------------------------

_REGIAO = T.StructType(
    [T.StructField("id", T.LongType()), T.StructField("sigla", T.StringType()), T.StructField("nome", T.StringType())]
)
_UF = T.StructType(
    [
        T.StructField("id", T.LongType()),
        T.StructField("sigla", T.StringType()),
        T.StructField("nome", T.StringType()),
        T.StructField("regiao", _REGIAO),
    ]
)
_MESO = T.StructType(
    [T.StructField("id", T.LongType()), T.StructField("nome", T.StringType()), T.StructField("UF", _UF)]
)
_MICRO = T.StructType(
    [T.StructField("id", T.LongType()), T.StructField("nome", T.StringType()), T.StructField("mesorregiao", _MESO)]
)
_INTERMEDIARIA = T.StructType(
    [T.StructField("id", T.LongType()), T.StructField("nome", T.StringType()), T.StructField("UF", _UF)]
)
_IMEDIATA = T.StructType(
    [
        T.StructField("id", T.LongType()),
        T.StructField("nome", T.StringType()),
        T.StructField("regiao-intermediaria", _INTERMEDIARIA),
    ]
)
MUNICIPIO_JSON = T.ArrayType(
    T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("nome", T.StringType()),
            T.StructField("microrregiao", _MICRO),
            T.StructField("regiao-imediata", _IMEDIATA),
        ]
    )
)

_SERIE = T.StructType(
    [
        T.StructField(
            "localidade", T.StructType([T.StructField("id", T.StringType()), T.StructField("nome", T.StringType())])
        ),
        T.StructField("serie", T.MapType(T.StringType(), T.StringType())),
    ]
)
AGREGADO_JSON = T.ArrayType(
    T.StructType(
        [
            T.StructField("id", T.StringType()),
            T.StructField("variavel", T.StringType()),
            T.StructField("unidade", T.StringType()),
            T.StructField(
                "resultados", T.ArrayType(T.StructType([T.StructField("series", T.ArrayType(_SERIE))]))
            ),
        ]
    )
)


def parse_municipios(ultimo_lote: DataFrame) -> DataFrame:
    """Uma linha por municipio, com a hierarquia territorial achatada (dimensao desnormalizada, Cap. 2)."""
    base = (
        ultimo_lote.where(F.col("fonte") == "municipios")
        .select(F.explode(F.from_json("payload", MUNICIPIO_JSON)).alias("m"), "_batch_id", "_ingerido_em")
    )
    return base.select(
        F.lpad(F.col("m.id").cast("string"), 7, "0").alias("municipio_id"),
        F.col("m.nome").alias("municipio_nome"),
        F.col("m.microrregiao.id").cast("string").alias("microrregiao_id"),
        F.col("m.microrregiao.nome").alias("microrregiao_nome"),
        F.col("m.microrregiao.mesorregiao.id").cast("string").alias("mesorregiao_id"),
        F.col("m.microrregiao.mesorregiao.nome").alias("mesorregiao_nome"),
        F.col("m.microrregiao.mesorregiao.UF.id").cast("string").alias("uf_id"),
        F.col("m.microrregiao.mesorregiao.UF.sigla").alias("uf_sigla"),
        F.col("m.microrregiao.mesorregiao.UF.nome").alias("uf_nome"),
        F.col("m.microrregiao.mesorregiao.UF.regiao.sigla").alias("regiao_sigla"),
        F.col("m.microrregiao.mesorregiao.UF.regiao.nome").alias("regiao_nome"),
        F.col("m.`regiao-imediata`.id").cast("string").alias("regiao_imediata_id"),
        F.col("m.`regiao-imediata`.nome").alias("regiao_imediata_nome"),
        F.col("m.`regiao-imediata`.`regiao-intermediaria`.id").cast("string").alias("regiao_intermediaria_id"),
        F.col("m.`regiao-imediata`.`regiao-intermediaria`.nome").alias("regiao_intermediaria_nome"),
        "_batch_id",
        "_ingerido_em",
    ).dropDuplicates(["municipio_id"])


def parse_observacoes(ultimo_lote: DataFrame, agregados=None) -> DataFrame:
    """Formato longo: uma linha por variavel x municipio x periodo. Valor ainda em texto."""
    agregados = agregados or {"populacao": "6579", "pib": "5938"}
    partes = []
    for fonte, agregado_id in agregados.items():
        base = ultimo_lote.where(F.col("fonte") == fonte).select(
            F.explode(F.from_json("payload", AGREGADO_JSON)).alias("v"), "_batch_id", "_ingerido_em"
        )
        achatado = (
            base.select(
                F.col("v.id").alias("variavel_id"),
                F.col("v.variavel").alias("variavel_nome"),
                F.col("v.unidade").alias("unidade"),
                F.explode("v.resultados").alias("r"),
                "_batch_id",
                "_ingerido_em",
            )
            .select("variavel_id", "variavel_nome", "unidade", F.explode("r.series").alias("s"), "_batch_id", "_ingerido_em")
            .select(
                "variavel_id",
                "variavel_nome",
                "unidade",
                F.col("s.localidade.id").alias("localidade_id"),
                F.col("s.localidade.nome").alias("municipio_nome_fonte"),
                F.explode("s.serie").alias("periodo", "valor_texto"),
                "_batch_id",
                "_ingerido_em",
            )
        )
        partes.append(
            achatado.select(
                F.lit(fonte).alias("fonte"),
                F.lit(agregado_id).alias("agregado_id"),
                "variavel_id",
                "variavel_nome",
                "unidade",
                F.lpad(F.col("localidade_id"), 7, "0").alias("municipio_id"),
                "municipio_nome_fonte",
                "periodo",
                "valor_texto",
                "_batch_id",
                "_ingerido_em",
            )
        )
    resultado = partes[0]
    for p in partes[1:]:
        resultado = resultado.unionByName(p)
    return resultado


def validar_variaveis(observacoes: DataFrame):
    """Para o pipeline se um id de variavel veio com nome diferente do esperado."""
    pares = observacoes.select("variavel_id", "variavel_nome").distinct().collect()
    erros = [f"{p.variavel_id}: '{p.variavel_nome}'" for p in pares if not nome_confere(p.variavel_id, p.variavel_nome)]
    if erros:
        raise ValueError(
            "Variaveis do IBGE com nome inesperado (confira lake/ibge.py, VARIAVEIS_PIB): " + "; ".join(erros)
        )


def aplicar_qualidade(observacoes: DataFrame):
    """Separa linhas validas das rejeitadas (esquema de eventos de erro, Cap. 19, subsistema 5).

    Simbolos do IBGE ("-", "...", "X") nao sao erro: o valor vira nulo e o status registra o motivo.
    """
    valor_numerico = F.col("valor_texto").rlike(r"^-?\d+(\.\d+)?$")
    valor_simbolo = F.col("valor_texto").isin(SIMBOLOS_IBGE) | F.col("valor_texto").isNull()

    marcado = observacoes.withColumn(
        "_regra",
        F.when(~F.col("municipio_id").rlike(r"^\d{7}$"), F.lit("R01_MUNICIPIO_ID_INVALIDO"))
        .when(~F.col("periodo").rlike(r"^\d{4}$"), F.lit("R02_PERIODO_INVALIDO"))
        .when(~(valor_numerico | valor_simbolo), F.lit("R03_VALOR_NAO_NUMERICO")),
    )

    rejeitadas = marcado.where(F.col("_regra").isNotNull()).select(
        "fonte",
        F.col("_regra").alias("regra"),
        F.concat_ws("|", "variavel_id", "municipio_id", "periodo").alias("chave"),
        F.col("valor_texto").alias("valor_original"),
        "_batch_id",
        F.current_timestamp().alias("_rejeitado_em"),
    )

    w = Window.partitionBy("fonte", "variavel_id", "municipio_id", "periodo").orderBy(
        F.col("_ingerido_em").desc()
    )
    validas = (
        marcado.where(F.col("_regra").isNull())
        .withColumn("valor_status", F.when(valor_numerico, F.lit("ok")).otherwise(F.lit("indisponivel")))
        .withColumn("valor", F.when(valor_numerico, F.col("valor_texto").cast("decimal(24,3)")))
        .withColumn("_rn", F.row_number().over(w))
        .where("_rn = 1")
        .drop("_rn", "_regra")
    )
    return validas, rejeitadas


# --------------------------------------------------------------------------
# GOLD: modelo dimensional (Cap. 2 a 5, 19)
# --------------------------------------------------------------------------

ATRIBUTOS_MUNICIPIO = [
    "municipio_nome",
    "microrregiao_nome",
    "mesorregiao_nome",
    "regiao_imediata_nome",
    "regiao_intermediaria_nome",
    "uf_sigla",
    "uf_nome",
    "regiao_nome",
]

DIM_MUNICIPIO_SCHEMA = T.StructType(
    [T.StructField("municipio_key", T.LongType(), False), T.StructField("municipio_id", T.StringType(), False)]
    + [T.StructField(c, T.StringType(), True) for c in ATRIBUTOS_MUNICIPIO]
    + [
        T.StructField("atributos_hash", T.StringType(), True),
        T.StructField("data_inicio_vigencia", T.DateType(), False),
        T.StructField("data_fim_vigencia", T.DateType(), False),
        T.StructField("registro_atual", T.BooleanType(), False),
    ]
)
_COLUNAS_DIM = [f.name for f in DIM_MUNICIPIO_SCHEMA.fields]


def registro_desconhecido(spark: SparkSession) -> DataFrame:
    """Linha de chave 0: destino dos fatos cujo municipio nao foi encontrado (sem FK nula, Cap. 2)."""
    linha = (
        [0, "0000000"]
        + ["Nao informado"] * len(ATRIBUTOS_MUNICIPIO)
        + [None, date(1900, 1, 1), date(9999, 12, 31), True]
    )
    return spark.createDataFrame([tuple(linha)], DIM_MUNICIPIO_SCHEMA)


def _hash_atributos():
    partes = [F.coalesce(F.col(c), F.lit("")) for c in ATRIBUTOS_MUNICIPIO]
    return F.sha2(F.concat_ws("||", *partes), 256)


def aplicar_scd2(spark: SparkSession, dim_atual, entrada: DataFrame, vigencia: date) -> DataFrame:
    """Dimensao tipo 2: mudanca de atributo gera nova linha (Cap. 5; subsistema 9 do Cap. 19).

    - membro novo: entra com vigencia desde 1900-01-01 (para casar com fatos antigos)
    - membro alterado: a linha atual e encerrada na vespera de `vigencia` e nasce uma nova
    - membro igual: nada muda (a recarga e idempotente)
    Devolve a dimensao inteira; quem chama decide como gravar.
    """
    desconhecido = registro_desconhecido(spark)
    novos_dados = (
        entrada.select("municipio_id", *ATRIBUTOS_MUNICIPIO)
        .dropDuplicates(["municipio_id"])
        .withColumn("atributos_hash", _hash_atributos())
    )

    if dim_atual is None:
        w = Window.orderBy("municipio_id")
        inicial = (
            novos_dados.withColumn("municipio_key", F.row_number().over(w).cast("long"))
            .withColumn("data_inicio_vigencia", F.lit(INICIO_PADRAO).cast("date"))
            .withColumn("data_fim_vigencia", F.lit(FIM_ABERTO).cast("date"))
            .withColumn("registro_atual", F.lit(True))
            .select(_COLUNAS_DIM)
        )
        return desconhecido.unionByName(inicial)

    historico = dim_atual.where("municipio_key <> 0").select(_COLUNAS_DIM)
    atuais = historico.where("registro_atual").select(
        "municipio_id",
        F.col("atributos_hash").alias("hash_atual"),
        F.col("data_inicio_vigencia").alias("inicio_atual"),
    )
    comparado = novos_dados.join(atuais, "municipio_id", "left")
    membros_novos = comparado.where(F.col("hash_atual").isNull())
    alterados = comparado.where(F.col("hash_atual").isNotNull() & (F.col("hash_atual") != F.col("atributos_hash")))

    invalidos = alterados.where(F.col("inicio_atual") >= F.lit(vigencia)).count()
    if invalidos:
        raise ValueError(
            f"{invalidos} membro(s) mudaram, mas a vigencia {vigencia} e anterior ao inicio da versao atual."
        )

    marca = alterados.select("municipio_id").withColumn("_alterado", F.lit(True))
    encerra = F.col("registro_atual") & F.col("_alterado").isNotNull()
    historico_atualizado = (
        historico.join(marca, "municipio_id", "left")
        .withColumn(
            "data_fim_vigencia",
            F.when(encerra, F.date_sub(F.lit(vigencia).cast("date"), 1)).otherwise(F.col("data_fim_vigencia")),
        )
        .withColumn("registro_atual", F.when(encerra, F.lit(False)).otherwise(F.col("registro_atual")))
        .select(_COLUNAS_DIM)
    )

    maior_chave = historico.agg(F.max("municipio_key")).first()[0] or 0
    versoes = membros_novos.withColumn("data_inicio_vigencia", F.lit(INICIO_PADRAO).cast("date")).unionByName(
        alterados.withColumn("data_inicio_vigencia", F.lit(vigencia).cast("date"))
    )
    w = Window.orderBy("municipio_id", "data_inicio_vigencia")
    novas_versoes = (
        versoes.withColumn("municipio_key", (F.lit(maior_chave) + F.row_number().over(w)).cast("long"))
        .withColumn("data_fim_vigencia", F.lit(FIM_ABERTO).cast("date"))
        .withColumn("registro_atual", F.lit(True))
        .select(_COLUNAS_DIM)
    )
    return desconhecido.unionByName(historico_atualizado).unionByName(novas_versoes)


def construir_dim_ano(anos) -> DataFrame:
    """Dimensao de tempo no grao ano (os fatos do IBGE sao anuais). Para fatos diarios, use uma dim_data."""
    spark = SparkSession.getActiveSession()
    linhas = [
        (int(a), int(a), (int(a) // 10) * 10, date(int(a), 1, 1), date(int(a), 12, 31))
        for a in sorted(set(anos))
    ]
    schema = T.StructType(
        [
            T.StructField("ano_key", T.IntegerType(), False),
            T.StructField("ano", T.IntegerType(), False),
            T.StructField("decada", T.IntegerType(), False),
            T.StructField("primeiro_dia", T.DateType(), False),
            T.StructField("ultimo_dia", T.DateType(), False),
        ]
    )
    return spark.createDataFrame(linhas, schema)


def linha_auditoria(spark, auditoria_key, batch_id, carregado_em, versao, qtd_validas, qtd_rejeitadas) -> DataFrame:
    """Dimensao de auditoria: de qual carga e de qual versao do codigo veio cada linha de fato (Cap. 2)."""
    schema = T.StructType(
        [
            T.StructField("auditoria_key", T.LongType(), False),
            T.StructField("batch_id", T.StringType(), True),
            T.StructField("carregado_em", T.TimestampType(), False),
            T.StructField("versao_pipeline", T.StringType(), False),
            T.StructField("qtd_observacoes_validas", T.LongType(), False),
            T.StructField("qtd_rejeitadas", T.LongType(), False),
        ]
    )
    return spark.createDataFrame(
        [(int(auditoria_key), batch_id, carregado_em, versao, int(qtd_validas), int(qtd_rejeitadas))], schema
    )


def preparar_fato_populacao(observacoes_validas: DataFrame) -> DataFrame:
    return observacoes_validas.where(
        (F.col("fonte") == "populacao") & (F.col("variavel_id") == VARIAVEL_POPULACAO[0])
    ).select(
        "municipio_id",
        F.col("periodo").cast("int").alias("ano_key"),
        F.col("valor").cast("long").alias("populacao"),
    )


def preparar_fato_pib(observacoes_validas: DataFrame) -> DataFrame:
    base = observacoes_validas.where(F.col("fonte") == "pib")
    ids = list(VARIAVEIS_PIB)
    largo = base.groupBy("municipio_id", "periodo").pivot("variavel_id", ids).agg(F.first("valor", ignorenulls=True))
    for variavel_id, (coluna, _) in VARIAVEIS_PIB.items():
        largo = largo.withColumnRenamed(variavel_id, coluna)
    return largo.withColumn("ano_key", F.col("periodo").cast("int")).drop("periodo")


def atribuir_chaves(fatos: DataFrame, dim_municipio: DataFrame, auditoria_key: int) -> DataFrame:
    """Pipeline de chaves substitutas (Cap. 19, subsistema 14).

    Troca a chave natural (municipio_id) pela chave substituta da versao da dimensao
    que valia na data de referencia do fato. Sem correspondencia, usa a chave 0.
    """
    dim = (
        dim_municipio.where("municipio_key <> 0")
        .select(
            F.col("municipio_key").alias("_dim_key"),
            F.col("municipio_id").alias("_dim_id"),
            F.col("data_inicio_vigencia").alias("_ini"),
            F.col("data_fim_vigencia").alias("_fim"),
        )
    )
    com_data = fatos.withColumn(
        "_data_ref", F.to_date(F.concat(F.col("ano_key").cast("string"), F.lit("-12-31")))
    )
    junto = com_data.join(
        dim,
        (com_data["municipio_id"] == dim["_dim_id"]) & com_data["_data_ref"].between(dim["_ini"], dim["_fim"]),
        "left",
    )
    return (
        junto.withColumn("municipio_key", F.coalesce(F.col("_dim_key"), F.lit(0).cast("long")))
        .withColumn("auditoria_key", F.lit(int(auditoria_key)).cast("long"))
        .drop("_dim_key", "_dim_id", "_ini", "_fim", "_data_ref", "municipio_id")
    )
