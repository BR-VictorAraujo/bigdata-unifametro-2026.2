"""
Testes da DAG api_contratos_ceara_postgresql.

- Estrutura: a DAG carrega sem erros, tem as 4 tasks na ordem certa,
  agendamento diário às 19:00 (America/Fortaleza) e catchup desligado.
- Unidade: limpeza de valores, sessão HTTP com retries e montagem da
  requisição à API.
- Pipeline (offline): definir_periodo >> coletar >> preparar com a API
  simulada, verificando os arquivos em /tmp e que nada vai para o XCom.
- Integração (PostgreSQL): salvar cria a tabela, converte os tipos e o
  upsert não duplica registros. Só roda se PG_TEST_HOST estiver definido.

Executar: pytest -v
"""

import json
import os
import sys
from decimal import Decimal

import pytest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import dag_contratos_ceara as mod  # noqa: E402


# ---------------------------------------------------------------------------
# Estrutura da DAG
# ---------------------------------------------------------------------------
def test_dagbag_sem_erros_de_importacao():
    from airflow.models.dagbag import DagBag

    dagbag = DagBag(
        dag_folder=os.path.join(RAIZ, "dag_contratos_ceara.py"),
        include_examples=False,
    )
    assert dagbag.import_errors == {}
    assert "api_contratos_ceara_postgresql" in dagbag.dag_ids


def test_quatro_tasks_em_sequencia():
    dag = mod.dag
    assert len(dag.tasks) == 4
    assert dag.get_task("definir_periodo").downstream_task_ids == {"coletar"}
    assert dag.get_task("coletar").downstream_task_ids == {"preparar"}
    assert dag.get_task("preparar").downstream_task_ids == {"salvar"}
    assert dag.get_task("salvar").downstream_task_ids == set()


def test_agendamento_diario_19h_fortaleza():
    import pendulum

    dag = mod.dag
    assert dag.timetable.summary == "0 19 * * *"
    # O fuso do cron é o que define "19:00 em Fortaleza" (= 22:00 UTC).
    assert dag.timetable._timezone.name == "America/Fortaleza"
    assert dag.catchup is False
    # O Airflow guarda o start_date em UTC; o instante é o mesmo.
    assert dag.start_date == pendulum.datetime(2025, 1, 1, tz="America/Fortaleza")


def test_retries_configurados():
    for task in mod.dag.tasks:
        assert task.retries >= 1


# ---------------------------------------------------------------------------
# Funções auxiliares
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "entrada, esperado",
    [
        ("", None),
        ("   ", None),
        ("0001-01-01T00:00:00.000-02:34", None),  # data-coringa da API
        ("0001-01-01", None),
        ("  PREGÃO  ", "PREGÃO"),
        ("08000000", "08000000"),  # zeros à esquerda preservados
        (0, 0),
        (False, False),
        (None, None),
    ],
)
def test_limpar_valor(entrada, esperado):
    assert mod.limpar_valor(entrada) == esperado


def test_sessao_tem_retries_para_429_e_5xx():
    session = mod.criar_sessao()
    retry = session.get_adapter("https://exemplo").max_retries
    assert retry.total == 5
    assert {429, 500, 502, 503, 504} <= set(retry.status_forcelist)


def test_buscar_pagina_monta_parametros_e_valida_status():
    chamadas = {}

    class RespostaFalsa:
        def raise_for_status(self):
            chamadas["raise_for_status"] = True

        def json(self):
            return {"sumary": {"total_pages": 1}, "data": []}

    class SessaoFalsa:
        def get(self, url, params, timeout):
            chamadas.update(url=url, params=params, timeout=timeout)
            return RespostaFalsa()

    mod.buscar_pagina(SessaoFalsa(), 3, "01/01/2025", "31/12/2025")

    assert chamadas["url"].endswith("/transparencia/contratos/contratos")
    assert chamadas["params"] == {
        "page": 3,
        "data_assinatura_inicio": "01/01/2025",
        "data_assinatura_fim": "31/12/2025",
    }
    assert chamadas["timeout"] == mod.HTTP_TIMEOUT
    assert chamadas["raise_for_status"] is True


# ---------------------------------------------------------------------------
# Pipeline com a API simulada
# ---------------------------------------------------------------------------
def contrato(id_, data="2025-03-10T00:00:00.000-03:00", **extras):
    """Registro mínimo no formato da API (campos ausentes viram NULL)."""
    return {
        "id": id_,
        "data_assinatura": data,
        "valor_contrato": "52820.0",
        "cod_orgao": "08000000",
        "emergency": False,
        "data_publicacao_doe": "0001-01-01T00:00:00.000-02:34",
        "campo_novo_da_api": "ignorado",
        **extras,
    }


# 2 páginas por exercício; o id 3 aparece duas vezes e o id 99 não tem data.
PAGINAS_FALSAS = {
    ("01/01/2025", 1): [contrato(1), contrato(2)],
    ("01/01/2025", 2): [contrato(3), contrato(99, data=None)],
    ("01/01/2026", 1): [contrato(3), contrato(4, data="2026-01-05T00:00:00.000-03:00")],
    ("01/01/2026", 2): [contrato(5, data="2026-02-01T00:00:00.000-03:00")],
}


def buscar_pagina_falsa(session, page, data_inicio, data_fim):
    return {
        "sumary": {"total_pages": 2, "total_records": 4},
        "data": PAGINAS_FALSAS[(data_inicio, page)],
    }


@pytest.fixture
def tmp_pipeline(tmp_path, monkeypatch):
    """Redireciona os arquivos de /tmp para uma pasta temporária do teste."""
    monkeypatch.setattr(mod, "TMP_DIR", str(tmp_path))
    monkeypatch.setattr(mod, "TMP_PERIODO", str(tmp_path / "periodo.json"))
    monkeypatch.setattr(mod, "TMP_RAW", str(tmp_path / "raw.jsonl"))
    monkeypatch.setattr(mod, "TMP_PREP", str(tmp_path / "prep.jsonl"))
    monkeypatch.setattr(mod, "buscar_pagina", buscar_pagina_falsa)
    return tmp_path


def executar_ate_preparar():
    # As callables não retornam nada: assim nenhum dado vai para o XCom.
    assert mod.definir_periodo() is None
    assert mod.coletar() is None
    assert mod.preparar() is None


def test_pipeline_gera_arquivos_e_remove_invalidos(tmp_pipeline):
    executar_ate_preparar()

    periodos = json.loads((tmp_pipeline / "periodo.json").read_text(encoding="utf-8"))
    assert [p["exercicio"] for p in periodos] == [2025, 2026]

    brutos = list(mod.ler_jsonl(mod.TMP_RAW))
    assert len(brutos) == 7  # tudo o que a API devolveu

    preparados = list(mod.ler_jsonl(mod.TMP_PREP))
    assert [r["id"] for r in preparados] == [1, 2, 3, 4, 5]  # sem duplicado nem id 99
    assert all(set(r) == set(mod.COLUNAS) for r in preparados)
    assert preparados[0]["data_publicacao_doe"] is None  # data-coringa vira NULL
    assert preparados[0]["cod_orgao"] == "08000000"


def test_preparar_falha_sem_registros_validos(tmp_pipeline, monkeypatch):
    monkeypatch.setattr(
        mod,
        "buscar_pagina",
        lambda *a: {
            "sumary": {"total_pages": 1, "total_records": 1},
            "data": [contrato(1, data=None)],
        },
    )
    mod.definir_periodo()
    mod.coletar()
    with pytest.raises(ValueError):
        mod.preparar()


def test_ddl_e_colunas_consistentes():
    for coluna in mod.COLUNAS:
        assert f"\n    {coluna} " in mod.DDL_CONTRATOS
    assert len(mod.COLUNAS) == len(set(mod.COLUNAS)) == 68


# ---------------------------------------------------------------------------
# Integração com PostgreSQL (CI: serviço postgres; local: PG_TEST_HOST=...)
# ---------------------------------------------------------------------------
@pytest.mark.skipif(
    not os.environ.get("PG_TEST_HOST"), reason="defina PG_TEST_HOST para rodar"
)
def test_salvar_upsert_no_postgres(tmp_pipeline, monkeypatch):
    monkeypatch.setattr(mod, "PG_HOST", os.environ["PG_TEST_HOST"])
    monkeypatch.setattr(mod, "PG_PORT", int(os.environ.get("PG_TEST_PORT", 5432)))
    monkeypatch.setattr(mod, "PG_DATABASE", os.environ.get("PG_TEST_DATABASE", "aula_teste"))

    def consultar(sql):
        conn = mod.psycopg2.connect(
            host=mod.PG_HOST, port=mod.PG_PORT, user=mod.PG_USER,
            password=mod.PG_PASSWORD, dbname=mod.PG_DATABASE,
        )
        try:
            with conn, conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall() if cur.description else None
        finally:
            conn.close()

    consultar("DROP TABLE IF EXISTS contratos")

    # 1ª carga
    executar_ate_preparar()
    assert mod.salvar() is None
    assert consultar("SELECT COUNT(*) FROM contratos") == [(5,)]

    id_, data_assinatura, valor, doe, emergency, cod_orgao = consultar(
        "SELECT id, data_assinatura, valor_contrato, data_publicacao_doe, "
        "emergency, cod_orgao FROM contratos WHERE id = 1"
    )[0]
    assert data_assinatura.year == 2025 and data_assinatura.tzinfo is not None
    assert valor == Decimal("52820.0")
    assert doe is None
    assert emergency is False
    assert cod_orgao == "08000000"
    carga_1 = consultar("SELECT MAX(data_carga) FROM contratos")[0][0]

    # 2ª carga com um valor alterado na API: atualiza sem duplicar
    PAGINAS_FALSAS[("01/01/2025", 1)][0]["valor_contrato"] = "60000.5"
    try:
        executar_ate_preparar()
        mod.salvar()
    finally:
        PAGINAS_FALSAS[("01/01/2025", 1)][0]["valor_contrato"] = "52820.0"

    assert consultar("SELECT COUNT(*) FROM contratos") == [(5,)]
    assert consultar("SELECT valor_contrato FROM contratos WHERE id = 1") == [
        (Decimal("60000.5"),)
    ]
    assert consultar("SELECT MAX(data_carga) FROM contratos")[0][0] > carga_1

    consultar("DROP TABLE contratos")
