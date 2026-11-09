from datetime import date

import pytest
import pyspark.sql.functions as F

from lake import transforms as t
from lake.ibge import nome_confere


def _bronze_das_fixtures(spark, settings):
    from lake.ibge import fontes, obter
    from datetime import datetime, timezone

    respostas = []
    for f in fontes(settings):
        status, texto = obter(f, settings)
        respostas.append({"fonte": f.nome, "url": f.url, "http_status": status, "payload": texto})
    agora = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
    return t.bronze_df(spark, respostas, "lote-1", agora)


def test_parse_municipios_achata_hierarquia(spark, settings):
    ultimo = t.ultimo_lote_por_fonte(_bronze_das_fixtures(spark, settings))
    mun = t.parse_municipios(ultimo)
    assert mun.count() == 5
    recife = mun.where("municipio_id = '2611606'").first()
    assert recife.municipio_nome == "Recife"
    assert recife.mesorregiao_nome == "Metropolitana de Recife"
    assert recife.uf_sigla == "PE" and recife.regiao_sigla == "NE"
    assert recife.regiao_imediata_nome == "Recife"
    assert recife.regiao_intermediaria_nome == "Recife"


def test_ultimo_lote_pega_a_carga_mais_recente(spark, settings):
    from datetime import datetime, timezone

    antigo = _bronze_das_fixtures(spark, settings)
    novo = antigo.withColumn("_batch_id", F.lit("lote-2")).withColumn(
        "_ingerido_em", F.lit(datetime(2026, 11, 10, tzinfo=timezone.utc)).cast("timestamp")
    )
    ultimo = t.ultimo_lote_por_fonte(antigo.unionByName(novo))
    assert ultimo.count() == 3
    assert {r[0] for r in ultimo.select("_batch_id").distinct().collect()} == {"lote-2"}


def test_observacoes_e_qualidade(spark, settings):
    ultimo = t.ultimo_lote_por_fonte(_bronze_das_fixtures(spark, settings))
    obs = t.parse_observacoes(ultimo)
    t.validar_variaveis(obs)
    validas, rejeitadas = t.aplicar_qualidade(obs)

    # 5 municipios x 3 anos x (1 populacao + 5 variaveis de pib)
    assert obs.count() == 5 * 3 * 6
    assert rejeitadas.count() == 0
    olinda = validas.where(
        "variavel_id = '9324' AND municipio_id = '2609600' AND periodo = '2021'"
    ).first()
    assert olinda.valor_status == "indisponivel" and olinda.valor is None
    pib = validas.where("variavel_id = '37' AND municipio_id = '2611606' AND periodo = '2019'").first()
    assert pib.valor_status == "ok" and float(pib.valor) == pytest.approx(5000000.0)


def test_qualidade_rejeita_lixo_e_remove_duplicata(spark):
    linhas = [
        ("pib", "5938", "37", "PIB", "Mil Reais", "2611606", "Recife", "2020", "100.5", "b1"),
        ("pib", "5938", "37", "PIB", "Mil Reais", "2611606", "Recife", "2020", "100.5", "b0"),  # duplicata
        ("pib", "5938", "37", "PIB", "Mil Reais", "26116", "Recife", "2020", "10", "b1"),  # id curto demais
        ("pib", "5938", "37", "PIB", "Mil Reais", "2611606", "Recife", "20X0", "10", "b1"),  # periodo ruim
        ("pib", "5938", "37", "PIB", "Mil Reais", "2609600", "Olinda", "2020", "abc", "b1"),  # valor ruim
        ("pib", "5938", "37", "PIB", "Mil Reais", "2600054", "Abreu", "2020", "X", "b1"),  # simbolo
    ]
    cols = ["fonte", "agregado_id", "variavel_id", "variavel_nome", "unidade", "municipio_id",
            "municipio_nome_fonte", "periodo", "valor_texto", "_batch_id"]
    df = spark.createDataFrame(linhas, cols).withColumn("_ingerido_em", F.current_timestamp())
    validas, rejeitadas = t.aplicar_qualidade(df)
    regras = sorted(r.regra for r in rejeitadas.collect())
    assert regras == ["R01_MUNICIPIO_ID_INVALIDO", "R02_PERIODO_INVALIDO", "R03_VALOR_NAO_NUMERICO"]
    assert validas.count() == 2  # recife (sem a duplicata) + abreu com simbolo
    assert validas.where("municipio_id = '2600054'").first().valor_status == "indisponivel"


def test_validar_variaveis_para_com_nome_trocado(spark):
    cols = ["variavel_id", "variavel_nome"]
    obs = spark.createDataFrame([("37", "Area plantada")], cols)
    with pytest.raises(ValueError, match="nome inesperado"):
        t.validar_variaveis(obs)
    assert nome_confere("37", "Produto Interno Bruto a preços correntes")
    assert nome_confere("9324", "População residente estimada")


def _mun_df(spark, linhas):
    cols = ["municipio_id"] + t.ATRIBUTOS_MUNICIPIO
    return spark.createDataFrame(linhas, cols)


def _linha(id_, nome, meso="Metropolitana"):
    return (id_, nome, "Micro", meso, "Imediata", "Intermediaria", "PE", "Pernambuco", "Nordeste")


def test_scd2_carga_inicial_tem_registro_desconhecido(spark):
    dim = t.aplicar_scd2(spark, None, _mun_df(spark, [_linha("2611606", "Recife"), _linha("2609600", "Olinda")]),
                         date(2026, 11, 9))
    linhas = {r.municipio_key: r for r in dim.collect()}
    assert set(linhas) == {0, 1, 2}
    assert linhas[0].municipio_nome == "Nao informado"
    assert linhas[1].municipio_id == "2609600"  # chaves seguem a ordem do codigo
    assert all(r.registro_atual for r in linhas.values())
    assert linhas[1].data_inicio_vigencia == date(1900, 1, 1)


def test_scd2_mudanca_cria_nova_versao_e_encerra_a_antiga(spark):
    inicial = t.aplicar_scd2(spark, None, _mun_df(spark, [_linha("2611606", "Recife")]), date(2026, 1, 1))
    mudou = _mun_df(spark, [_linha("2611606", "Recife", meso="Outra mesorregiao")])
    nova = t.aplicar_scd2(spark, inicial, mudou, date(2022, 1, 1))
    v = sorted(nova.where("municipio_id = '2611606'").collect(), key=lambda r: r.data_inicio_vigencia)
    assert len(v) == 2
    assert v[0].registro_atual is False and v[0].data_fim_vigencia == date(2021, 12, 31)
    assert v[1].registro_atual is True and v[1].data_inicio_vigencia == date(2022, 1, 1)
    assert v[1].municipio_key == 2  # continua a sequencia
    assert v[1].mesorregiao_nome == "Outra mesorregiao"


def test_scd2_recarga_sem_mudanca_e_idempotente_e_membro_novo_entra(spark):
    base = _mun_df(spark, [_linha("2611606", "Recife")])
    d1 = t.aplicar_scd2(spark, None, base, date(2026, 1, 1))
    d2 = t.aplicar_scd2(spark, d1, base, date(2026, 6, 1))
    assert d2.count() == d1.count()
    com_novo = _mun_df(spark, [_linha("2611606", "Recife"), _linha("2600054", "Abreu e Lima")])
    d3 = t.aplicar_scd2(spark, d2, com_novo, date(2026, 7, 1))
    novo = d3.where("municipio_id = '2600054'").first()
    assert novo.municipio_key == 2 and novo.data_inicio_vigencia == date(1900, 1, 1)


def test_scd2_recusa_vigencia_anterior_ao_inicio_da_versao_atual(spark):
    d1 = t.aplicar_scd2(spark, None, _mun_df(spark, [_linha("2611606", "Recife")]), date(2026, 1, 1))
    d2 = t.aplicar_scd2(spark, d1, _mun_df(spark, [_linha("2611606", "Recife", meso="B")]), date(2022, 1, 1))
    with pytest.raises(ValueError, match="anterior ao inicio"):
        t.aplicar_scd2(spark, d2, _mun_df(spark, [_linha("2611606", "Recife", meso="C")]), date(2021, 1, 1))


def test_atribuir_chaves_respeita_a_vigencia_e_usa_chave_zero(spark):
    inicial = t.aplicar_scd2(spark, None, _mun_df(spark, [_linha("2611606", "Recife")]), date(2026, 1, 1))
    dim = t.aplicar_scd2(spark, inicial, _mun_df(spark, [_linha("2611606", "Recife", meso="B")]), date(2021, 1, 1))
    fatos = spark.createDataFrame(
        [("2611606", 2019, 10), ("2611606", 2021, 20), ("9999999", 2021, 30)],
        ["municipio_id", "ano_key", "populacao"],
    )
    r = {(x.ano_key, x.populacao): x.municipio_key for x in t.atribuir_chaves(fatos, dim, 7).collect()}
    assert r[(2019, 10)] == 1   # versao antiga
    assert r[(2021, 20)] == 2   # versao nova (vigente desde 2021-01-01)
    assert r[(2021, 30)] == 0   # municipio desconhecido
    assert all(x.auditoria_key == 7 for x in t.atribuir_chaves(fatos, dim, 7).collect())


def test_fato_pib_vira_colunas_de_medida(spark, settings):
    ultimo = t.ultimo_lote_por_fonte(_bronze_das_fixtures(spark, settings))
    validas, _ = t.aplicar_qualidade(t.parse_observacoes(ultimo))
    pib = t.preparar_fato_pib(validas)
    assert {"pib_mil_reais", "vab_agropecuaria_mil_reais", "vab_industria_mil_reais",
            "vab_servicos_mil_reais", "vab_adm_publica_mil_reais", "ano_key", "municipio_id"} <= set(pib.columns)
    assert pib.count() == 15
    olinda21 = pib.where("municipio_id = '2609600' AND ano_key = 2021").first()
    assert olinda21.vab_adm_publica_mil_reais is None  # veio como "..." do IBGE
    assert olinda21.pib_mil_reais is not None
