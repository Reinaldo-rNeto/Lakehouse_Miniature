import pytest

from lake.config import Settings


def test_config_rejeita_uf_e_periodos_malformados(monkeypatch):
    monkeypatch.setenv("IBGE_UF", "26/../x")
    with pytest.raises(ValueError):
        Settings.from_env()
    monkeypatch.setenv("IBGE_UF", "26")
    monkeypatch.setenv("IBGE_PERIODOS", "2022?x=1")
    with pytest.raises(ValueError):
        Settings.from_env()


def test_config_aceita_valores_validos(monkeypatch):
    monkeypatch.setenv("IBGE_UF", "35")
    monkeypatch.setenv("IBGE_PERIODOS", "2021|2022")
    cfg = Settings.from_env()
    assert cfg.ibge_uf == "35" and cfg.ibge_periodos == "2021|2022"
