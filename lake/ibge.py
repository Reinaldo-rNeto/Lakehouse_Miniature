"""Cliente das APIs abertas do IBGE (servicodados.ibge.gov.br). Sem chave, sem cadastro.

Tres fontes:
  municipios  -> localidades da UF, com a hierarquia territorial (cadastro)
  populacao   -> agregado 6579, variavel 9324 (estimativa da populacao residente)
  pib         -> agregado 5938 (PIB dos municipios e valor adicionado por setor)

Para guardar uma copia local das respostas reais (util antes de uma apresentacao):
  python -m lake.ibge salvar data/ibge_snapshot
e depois rode os jobs com IBGE_OFFLINE_DIR=data/ibge_snapshot
"""
import json
import os
import sys
import time
import unicodedata
import urllib.parse
import urllib.request
from dataclasses import dataclass

from lake.config import Settings

BASE = "https://servicodados.ibge.gov.br/api"
USER_AGENT = "lakehouse-estudo/1.0"


@dataclass(frozen=True)
class Fonte:
    nome: str
    url: str
    arquivo_offline: str
    agregado_id: str = ""


# Variaveis do PIB que viram colunas na fato_pib. O texto esperado protege contra
# um id trocado: se o IBGE devolver outro nome, o pipeline para e avisa.
VARIAVEIS_PIB = {
    "37": ("pib_mil_reais", "produto interno bruto"),
    "513": ("vab_agropecuaria_mil_reais", "agropecuaria"),
    "517": ("vab_industria_mil_reais", "industria"),
    "6575": ("vab_servicos_mil_reais", "servicos"),
    "525": ("vab_adm_publica_mil_reais", "administracao"),
}
VARIAVEL_POPULACAO = ("9324", "populacao")


def _sem_acento(texto: str) -> str:
    base = unicodedata.normalize("NFKD", texto or "")
    return "".join(c for c in base if not unicodedata.combining(c)).lower()


def nome_confere(variavel_id: str, nome_api: str) -> bool:
    if variavel_id in VARIAVEIS_PIB:
        esperado = VARIAVEIS_PIB[variavel_id][1]
    elif variavel_id == VARIAVEL_POPULACAO[0]:
        esperado = VARIAVEL_POPULACAO[1]
    else:
        return True
    return esperado in _sem_acento(nome_api)


def fontes(settings: Settings):
    uf = settings.ibge_uf
    per = settings.ibge_periodos
    loc = f"N6[N3[{uf}]]"  # todos os municipios (N6) dentro da UF (N3)
    vars_pib = "|".join(VARIAVEIS_PIB)
    return [
        Fonte(
            "municipios",
            f"{BASE}/v1/localidades/estados/{uf}/municipios",
            "municipios.json",
        ),
        Fonte(
            "populacao",
            f"{BASE}/v3/agregados/6579/periodos/{per}/variaveis/{VARIAVEL_POPULACAO[0]}?localidades={loc}",
            "populacao.json",
            "6579",
        ),
        Fonte(
            "pib",
            f"{BASE}/v3/agregados/5938/periodos/{per}/variaveis/{vars_pib}?localidades={loc}",
            "pib.json",
            "5938",
        ),
    ]


def _baixar(url: str, tentativas=3, espera=3):
    # Colchetes e barras verticais fazem parte da sintaxe do IBGE e nao podem ser codificados
    url_ok = urllib.parse.quote(url, safe=":/?=&[],|-_.")
    ultimo = None
    for n in range(1, tentativas + 1):
        try:
            req = urllib.request.Request(url_ok, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=90) as resp:
                return resp.status, resp.read().decode("utf-8")
        except Exception as erro:  # noqa: BLE001
            ultimo = erro
            time.sleep(espera * n)
    raise RuntimeError(f"Falha ao baixar {url_ok}: {ultimo}")


def obter(fonte: Fonte, settings: Settings):
    """Devolve (http_status, texto_json). Com IBGE_OFFLINE_DIR, le de arquivo."""
    if settings.ibge_offline_dir:
        caminho = os.path.join(settings.ibge_offline_dir, fonte.arquivo_offline)
        with open(caminho, encoding="utf-8") as f:
            return 200, f.read()
    status, texto = _baixar(fonte.url)
    json.loads(texto)  # falha cedo se a resposta nao for JSON valido
    return status, texto


def salvar_snapshot(pasta: str, settings: Settings):
    os.makedirs(pasta, exist_ok=True)
    online = Settings(**{**settings.__dict__, "ibge_offline_dir": ""})
    for fonte in fontes(online):
        _, texto = obter(fonte, online)
        with open(os.path.join(pasta, fonte.arquivo_offline), "w", encoding="utf-8") as f:
            f.write(texto)
        print(f"salvo: {fonte.arquivo_offline} ({len(texto)} bytes)")


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "salvar":
        salvar_snapshot(sys.argv[2], Settings.from_env())
    else:
        print("uso: python -m lake.ibge salvar <pasta>")
