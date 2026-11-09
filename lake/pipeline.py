"""Orquestracao das etapas. Cada funcao e uma camada do medalhao.

bronze -> guarda a resposta crua da API (append, historico completo)
silver -> interpreta, tipa e valida (reconstruida a partir da bronze)
gold   -> modelo dimensional: dimensoes com SCD2 e fatos com chaves substitutas
"""
import uuid
from datetime import date, datetime, timezone

import pyspark.sql.functions as F

from lake import transforms as t
from lake.ibge import fontes, obter

VERSAO_PIPELINE = "1.0.0"
TABELA_BRONZE = "ibge_respostas_brutas"


def _agora():
    return datetime.now(timezone.utc)


def executar_bronze(spark, lake, settings):
    lake.criar_camadas()
    agora = _agora()
    batch_id = agora.strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]

    respostas = []
    for fonte in fontes(settings):
        status, texto = obter(fonte, settings)
        print(f"  {fonte.nome}: HTTP {status}, {len(texto)} bytes")
        respostas.append({"fonte": fonte.nome, "url": fonte.url, "http_status": status, "payload": texto})

    lake.acrescentar(t.bronze_df(spark, respostas, batch_id, agora), "bronze", TABELA_BRONZE)
    print(f"bronze: lote {batch_id} gravado ({len(respostas)} respostas)")
    return batch_id


def executar_silver(spark, lake):
    bronze = lake.ler("bronze", TABELA_BRONZE)
    ultimo = t.ultimo_lote_por_fonte(bronze)

    municipios = t.parse_municipios(ultimo)
    observacoes = t.parse_observacoes(ultimo)
    t.validar_variaveis(observacoes)
    validas, rejeitadas = t.aplicar_qualidade(observacoes)

    lote_ids = [r[0] for r in observacoes.select("_batch_id").distinct().collect()]
    lake.substituir(municipios, "silver", "municipio")
    lake.substituir(validas, "silver", "observacao_agregado")

    # Rejeitadas acumulam entre cargas; reprocessar o mesmo lote nao duplica
    lista = ", ".join(f"'{b}'" for b in lote_ids)
    lake.apagar_onde("silver", "dq_rejeitados", f"_batch_id IN ({lista})")
    lake.acrescentar(rejeitadas, "silver", "dq_rejeitados")

    n_mun = lake.ler("silver", "municipio").count()
    n_val = lake.ler("silver", "observacao_agregado").count()
    n_rej = rejeitadas.count()
    print(f"silver: {n_mun} municipios, {n_val} observacoes validas, {n_rej} rejeitadas")
    return n_mun, n_val, n_rej


def executar_gold(spark, lake, vigencia: date = None):
    vigencia = vigencia or date.today()
    municipios = lake.ler("silver", "municipio")
    obs = lake.ler("silver", "observacao_agregado")

    # 1) dimensao de municipio, tipo 2
    atual = lake.ler("gold", "dim_municipio") if lake.existe("gold", "dim_municipio") else None
    nova = t.aplicar_scd2(spark, atual, municipios, vigencia)
    lake.substituir(nova, "gold", "dim_municipio")
    dim_mun = lake.ler("gold", "dim_municipio")

    # 2) dimensao de ano (acumula os anos ja vistos)
    anos = [r[0] for r in obs.select(F.col("periodo").cast("int")).distinct().collect()]
    if lake.existe("gold", "dim_ano"):
        anos += [r[0] for r in lake.ler("gold", "dim_ano").select("ano").collect()]
    lake.substituir(t.construir_dim_ano(anos), "gold", "dim_ano")

    # 3) auditoria desta carga
    proxima = 1
    if lake.existe("gold", "dim_auditoria"):
        proxima = (lake.ler("gold", "dim_auditoria").agg(F.max("auditoria_key")).first()[0] or 0) + 1
    lote = obs.agg(F.max("_batch_id")).first()[0]
    n_rej = lake.ler("silver", "dq_rejeitados").count() if lake.existe("silver", "dq_rejeitados") else 0
    lake.acrescentar(
        t.linha_auditoria(spark, proxima, lote, _agora(), VERSAO_PIPELINE, obs.count(), n_rej),
        "gold",
        "dim_auditoria",
    )

    # 4) fatos: apaga os anos que vao ser recarregados e insere de novo (recarga idempotente)
    for tabela, preparar in (("fato_populacao", t.preparar_fato_populacao), ("fato_pib", t.preparar_fato_pib)):
        pronto = t.atribuir_chaves(preparar(obs), dim_mun, proxima)
        anos_fato = sorted(r[0] for r in pronto.select("ano_key").distinct().collect())
        if not anos_fato:
            print(f"gold: {tabela} sem linhas para carregar")
            continue
        lake.apagar_onde("gold", tabela, f"ano_key IN ({', '.join(str(a) for a in anos_fato)})")
        lake.acrescentar(pronto.select(_ordem_colunas(tabela, pronto)), "gold", tabela)

    resumo = {
        "dim_municipio": lake.ler("gold", "dim_municipio").count(),
        "dim_ano": lake.ler("gold", "dim_ano").count(),
        "fato_populacao": lake.ler("gold", "fato_populacao").count(),
        "fato_pib": lake.ler("gold", "fato_pib").count(),
    }
    print("gold:", ", ".join(f"{k}={v}" for k, v in resumo.items()))
    return resumo


def _ordem_colunas(tabela, df):
    chaves = ["municipio_key", "ano_key", "auditoria_key"]
    medidas = [c for c in df.columns if c not in chaves]
    return chaves + medidas


def versoes_do_municipio(lake, municipio_id):
    return (
        lake.ler("gold", "dim_municipio")
        .where(F.col("municipio_id") == municipio_id)
        .select(
            "municipio_key",
            "municipio_id",
            "municipio_nome",
            "mesorregiao_nome",
            "data_inicio_vigencia",
            "data_fim_vigencia",
            "registro_atual",
        )
        .orderBy("data_inicio_vigencia")
    )


def demo_scd2(spark, lake, municipio_id: str, vigencia: date):
    """Altera um atributo na silver e recarrega a gold para mostrar o tipo 2 funcionando."""
    mun = lake.ler("silver", "municipio")
    alterado = mun.withColumn(
        "mesorregiao_nome",
        F.when(F.col("municipio_id") == municipio_id, F.lit("Mesorregiao ALTERADA (demo)")).otherwise(
            F.col("mesorregiao_nome")
        ),
    )
    lake.substituir(alterado, "silver", "municipio")
    executar_gold(spark, lake, vigencia)
    return versoes_do_municipio(lake, municipio_id)
