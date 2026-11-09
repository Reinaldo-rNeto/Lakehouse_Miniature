"""Pipeline inteiro (bronze, silver, gold) no backend local, com os JSONs de teste."""
from datetime import date

import pyspark.sql.functions as F

from lake.io import Lake
from lake.pipeline import (demo_scd2, executar_bronze, executar_gold, executar_silver,
                           versoes_do_municipio)


def _rodar_tudo(spark, settings):
    lake = Lake(spark, settings)
    executar_bronze(spark, lake, settings)
    executar_silver(spark, lake)
    executar_gold(spark, lake, date(2026, 11, 9))
    return lake


def test_pipeline_completo_e_idempotente(spark, settings):
    lake = _rodar_tudo(spark, settings)
    assert lake.ler("bronze", "ibge_respostas_brutas").count() == 3
    assert lake.ler("silver", "municipio").count() == 5
    assert lake.ler("gold", "dim_municipio").count() == 6        # 5 + registro desconhecido
    assert lake.ler("gold", "fato_populacao").count() == 15
    assert lake.ler("gold", "fato_pib").count() == 15
    assert lake.ler("gold", "dim_ano").count() == 3

    # segunda carga: bronze acumula, o resto nao duplica
    executar_bronze(spark, lake, settings)
    executar_silver(spark, lake)
    executar_gold(spark, lake, date(2026, 11, 10))
    assert lake.ler("bronze", "ibge_respostas_brutas").count() == 6
    assert lake.ler("gold", "dim_municipio").count() == 6
    assert lake.ler("gold", "fato_populacao").count() == 15
    assert lake.ler("gold", "fato_pib").count() == 15
    assert lake.ler("gold", "dim_auditoria").count() == 2


def test_nenhum_fato_aponta_para_municipio_desconhecido(spark, settings):
    lake = _rodar_tudo(spark, settings)
    for tabela in ("fato_populacao", "fato_pib"):
        assert lake.ler("gold", tabela).where("municipio_key = 0").count() == 0
        assert lake.ler("gold", tabela).where("municipio_key IS NULL OR ano_key IS NULL").count() == 0


def test_integridade_referencial_fatos_x_dimensoes(spark, settings):
    lake = _rodar_tudo(spark, settings)
    dim = lake.ler("gold", "dim_municipio").select("municipio_key")
    anos = lake.ler("gold", "dim_ano").select("ano_key")
    for tabela in ("fato_populacao", "fato_pib"):
        f = lake.ler("gold", tabela)
        assert f.join(dim, "municipio_key", "left_anti").count() == 0
        assert f.join(anos, "ano_key", "left_anti").count() == 0


def test_drill_across_populacao_e_pib_por_mesorregiao(spark, settings):
    """Dois fatos ligados pela dimensao conformada, sem juntar fato com fato."""
    lake = _rodar_tudo(spark, settings)
    dim = lake.ler("gold", "dim_municipio")
    pop = (lake.ler("gold", "fato_populacao").join(dim, "municipio_key")
           .groupBy("mesorregiao_nome", "ano_key").agg(F.sum("populacao").alias("pop")))
    pib = (lake.ler("gold", "fato_pib").join(dim, "municipio_key")
           .groupBy("mesorregiao_nome", "ano_key").agg(F.sum("pib_mil_reais").alias("pib")))
    r = pop.join(pib, ["mesorregiao_nome", "ano_key"]).where(
        "mesorregiao_nome = 'Metropolitana de Recife' AND ano_key = 2019").first()
    assert r.pop == 100000 + 350000 + 1600000


def test_demo_scd2_remapeia_os_fatos_pela_vigencia(spark, settings):
    lake = _rodar_tudo(spark, settings)
    versoes = demo_scd2(spark, lake, "2611606", date(2020, 1, 1))
    linhas = versoes.collect()
    assert len(linhas) == 2
    antiga, nova = linhas
    assert antiga.registro_atual is False and antiga.data_fim_vigencia == date(2019, 12, 31)
    assert nova.mesorregiao_nome == "Mesorregiao ALTERADA (demo)"

    f = lake.ler("gold", "fato_populacao").where("populacao = 1600000 OR populacao = 1610000 OR populacao = 1620000")
    por_ano = {r.ano_key: r.municipio_key for r in f.collect()}
    assert por_ano[2019] == antiga.municipio_key   # antes da vigencia: como era
    assert por_ano[2020] == nova.municipio_key     # a partir da vigencia: como ficou
    assert por_ano[2021] == nova.municipio_key
    assert versoes_do_municipio(lake, "2611606").count() == 2

    # nenhuma linha de fato foi duplicada pela recarga
    assert lake.ler("gold", "fato_populacao").count() == 15
