"""
DAG: api_contratos_ceara_postgresql

Atividade 01 - Apache Airflow 3
Carrega os contratos dos exercícios 2025 e 2026 da API de dados abertos do
Ceará Transparente no PostgreSQL (banco "aula", tabela "contratos"),
com upsert, todos os dias às 19:00 (America/Fortaleza).

Fluxo: definir_periodo >> coletar >> preparar >> salvar
Os dados passam entre as tasks por arquivos em /tmp (não por XCom),
porque o volume (~90 mil contratos) excede o limite do XCom.

API: GET /transparencia/contratos/contratos
     ?page=N&data_assinatura_inicio=DD/MM/AAAA&data_assinatura_fim=DD/MM/AAAA
     Paginação: payload["sumary"]["total_pages"] (100 registros por página)
"""

from datetime import timedelta
import json
import os

import pendulum
import psycopg2
import psycopg2.extras
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

# --- API -------------------------------------------------------------------
API_URL = (
    "https://api-dados-abertos.cearatransparente.ce.gov.br"
    "/transparencia/contratos/contratos"
)
EXERCICIOS = [2025, 2026]  # anos a carregar
HTTP_TIMEOUT = (10, 120)   # segundos: (conectar, aguardar resposta)

# --- PostgreSQL ------------------------------------------------------------
PG_HOST = "host.docker.internal"  # o host Windows visto de dentro do contêiner
PG_PORT = 5432
PG_USER = "postgres"
PG_PASSWORD = "1234"
PG_DATABASE = "aula"
PG_TABLE = "contratos"

TIMEZONE = "America/Fortaleza"

# --- Arquivos de troca entre as tasks (no lugar do XCom) ------------------
# JSONL = um registro JSON por linha (permite ler/gravar aos poucos).
TMP_DIR = "/tmp/contratos_ceara"
TMP_PERIODO = os.path.join(TMP_DIR, "periodo.json")
TMP_RAW = os.path.join(TMP_DIR, "contratos_raw.jsonl")
TMP_PREP = os.path.join(TMP_DIR, "contratos_prep.jsonl")

# --- Tabela ------------------------------------------------------------------
# DDL gerada a partir da inspeção de respostas reais da API.
# (O IF NOT EXISTS garante que a DAG também funciona num banco novo.)
DDL_CONTRATOS = """
CREATE TABLE IF NOT EXISTS contratos (
    id                                BIGINT PRIMARY KEY,
    cod_concedente                    TEXT,
    cod_financiador                   TEXT,
    cod_gestora                       TEXT,
    cod_orgao                         TEXT,
    cod_secretaria                    TEXT,
    descricao_modalidade              TEXT,
    descricao_objeto                  TEXT,
    descricao_tipo                    TEXT,
    descricao_url                     TEXT,
    data_assinatura                   TIMESTAMPTZ,
    data_processamento                TIMESTAMPTZ,
    data_termino                      TIMESTAMPTZ,
    flg_tipo                          INTEGER,
    isn_parte_destino                 BIGINT,
    isn_sic                           BIGINT,
    num_spu                           TEXT,
    valor_contrato                    NUMERIC,
    isn_modalidade                    INTEGER,
    isn_entidade                      INTEGER,
    tipo_objeto                       TEXT,
    num_spu_licitacao                 TEXT,
    descricao_justificativa           TEXT,
    valor_can_rstpg                   NUMERIC,
    data_publicacao_portal            TIMESTAMPTZ,
    descricao_url_pltrb               TEXT,
    descricao_url_ddisp               TEXT,
    descricao_url_inexg               TEXT,
    cod_plano_trabalho                TEXT,
    num_certidao                      TEXT,
    descriaco_edital                  TEXT,
    cpf_cnpj_financiador              TEXT,
    num_contrato                      TEXT,
    valor_original_concedente         NUMERIC,
    valor_original_contrapartida      NUMERIC,
    valor_atualizado_concedente       NUMERIC,
    valor_atualizado_contrapartida    NUMERIC,
    created_at                        TIMESTAMPTZ,
    updated_at                        TIMESTAMPTZ,
    plain_num_contrato                TEXT,
    calculated_valor_aditivo          NUMERIC,
    calculated_valor_ajuste           NUMERIC,
    calculated_valor_empenhado        NUMERIC,
    calculated_valor_pago             NUMERIC,
    contract_type                     TEXT,
    infringement_status               TEXT,
    cod_financiador_including_zeroes  TEXT,
    accountability_status             TEXT,
    plain_cpf_cnpj_financiador        TEXT,
    descricao_situacao                TEXT,
    data_publicacao_doe               TIMESTAMPTZ,
    descricao_nome_credor             TEXT,
    isn_parte_origem                  TEXT,
    data_auditoria                    DATE,
    data_termino_original             TIMESTAMPTZ,
    data_inicio                       TIMESTAMPTZ,
    data_rescisao                     TIMESTAMPTZ,
    confidential                      BOOLEAN,
    gestor_contrato                   TEXT,
    data_finalizacao_prestacao_contas DATE,
    sequential                        INTEGER,
    emergency                         BOOLEAN,
    law                               TEXT,
    has_non_profit_transfer           BOOLEAN,
    nome_fiscal                       TEXT,
    emenda_parlamentar                TEXT,
    ano_emenda_parlamentar            TEXT,
    codigo_emenda_parlamentar         TEXT,
    data_carga                        TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

# Colunas que vêm da API, na mesma ordem da DDL (data_carga fica de fora:
# ela é preenchida pelo próprio banco).
COLUNAS = [
    "id", "cod_concedente", "cod_financiador", "cod_gestora", "cod_orgao",
    "cod_secretaria", "descricao_modalidade", "descricao_objeto",
    "descricao_tipo", "descricao_url", "data_assinatura", "data_processamento",
    "data_termino", "flg_tipo", "isn_parte_destino", "isn_sic", "num_spu",
    "valor_contrato", "isn_modalidade", "isn_entidade", "tipo_objeto",
    "num_spu_licitacao", "descricao_justificativa", "valor_can_rstpg",
    "data_publicacao_portal", "descricao_url_pltrb", "descricao_url_ddisp",
    "descricao_url_inexg", "cod_plano_trabalho", "num_certidao",
    "descriaco_edital", "cpf_cnpj_financiador", "num_contrato",
    "valor_original_concedente", "valor_original_contrapartida",
    "valor_atualizado_concedente", "valor_atualizado_contrapartida",
    "created_at", "updated_at", "plain_num_contrato",
    "calculated_valor_aditivo", "calculated_valor_ajuste",
    "calculated_valor_empenhado", "calculated_valor_pago", "contract_type",
    "infringement_status", "cod_financiador_including_zeroes",
    "accountability_status", "plain_cpf_cnpj_financiador",
    "descricao_situacao", "data_publicacao_doe", "descricao_nome_credor",
    "isn_parte_origem", "data_auditoria", "data_termino_original",
    "data_inicio", "data_rescisao", "confidential", "gestor_contrato",
    "data_finalizacao_prestacao_contas", "sequential", "emergency", "law",
    "has_non_profit_transfer", "nome_fiscal", "emenda_parlamentar",
    "ano_emenda_parlamentar", "codigo_emenda_parlamentar",
]


# --- Funções auxiliares ------------------------------------------------------
def criar_sessao():
    """requests.Session com retries automáticos: tenta de novo até 5 vezes
    (esperando 2s, 4s, 8s...) em falhas de rede, 429 e erros 5xx."""
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def buscar_pagina(session, page, data_inicio, data_fim):
    """Busca uma página da API e devolve o JSON."""
    params = {
        "page": page,
        "data_assinatura_inicio": data_inicio,
        "data_assinatura_fim": data_fim,
    }
    response = session.get(API_URL, params=params, timeout=HTTP_TIMEOUT)
    response.raise_for_status()  # erro HTTP (4xx/5xx) faz a task falhar
    return response.json()


def limpar_valor(valor):
    """Texto vazio vira NULL. A API usa a data 0001-01-01 para dizer
    "sem data", então ela também vira NULL."""
    if isinstance(valor, str):
        valor = valor.strip()
        if valor == "" or valor.startswith("0001-01-01"):
            return None
    return valor


def ler_jsonl(caminho):
    """Lê um arquivo JSONL linha a linha (sem carregar tudo na memória)."""
    with open(caminho, encoding="utf-8") as arquivo:
        for linha in arquivo:
            if linha.strip():
                yield json.loads(linha)


# --- Tasks -------------------------------------------------------------------
# Nenhuma função retorna valor: assim nada vai para o XCom.

def definir_periodo(**context):
    """Define um período por exercício (01/01 a 31/12) e grava em /tmp.
    A carga é completa todo dia, para pegar atualizações de contratos
    antigos (aditivos, pagamentos, situação). O upsert evita duplicar."""
    os.makedirs(TMP_DIR, exist_ok=True)
    periodos = [
        {"exercicio": ano, "data_inicio": f"01/01/{ano}", "data_fim": f"31/12/{ano}"}
        for ano in EXERCICIOS
    ]
    with open(TMP_PERIODO, "w", encoding="utf-8") as arquivo:
        json.dump(periodos, arquivo)
    print(f"Períodos: {periodos}")


def coletar(**context):
    """Percorre todas as páginas de cada período e grava os contratos
    brutos em JSONL."""
    with open(TMP_PERIODO, encoding="utf-8") as arquivo:
        periodos = json.load(arquivo)

    session = criar_sessao()
    total = 0
    try:
        with open(TMP_RAW, "w", encoding="utf-8") as saida:
            for periodo in periodos:
                inicio, fim = periodo["data_inicio"], periodo["data_fim"]

                # 1ª página: descobre o total de páginas em payload["sumary"]
                payload = buscar_pagina(session, 1, inicio, fim)
                total_pages = int(payload["sumary"]["total_pages"])
                print(f"{periodo['exercicio']}: {payload['sumary']['total_records']} "
                      f"registros em {total_pages} páginas")

                for page in range(1, total_pages + 1):
                    if page > 1:
                        payload = buscar_pagina(session, page, inicio, fim)
                    dados = payload.get("data") or []
                    if not dados:  # página vazia: acabou antes do previsto
                        break
                    for registro in dados:
                        saida.write(json.dumps(registro, ensure_ascii=False) + "\n")
                    total += len(dados)
                    if page % 50 == 0:
                        print(f"  página {page}/{total_pages}")
    finally:
        session.close()

    print(f"Coletados {total} contratos em {TMP_RAW}")


def preparar(**context):
    """Mantém só as colunas da tabela, limpa os valores e descarta
    registros sem id, sem data de assinatura ou repetidos."""
    ids_vistos = set()
    lidos = gravados = 0

    with open(TMP_PREP, "w", encoding="utf-8") as saida:
        for bruto in ler_jsonl(TMP_RAW):
            lidos += 1
            # Campo ausente vira NULL; campo novo que a API inventar é ignorado.
            registro = {col: limpar_valor(bruto.get(col)) for col in COLUNAS}

            if registro["id"] is None or registro["data_assinatura"] is None:
                continue
            if registro["id"] in ids_vistos:
                continue
            ids_vistos.add(registro["id"])

            saida.write(json.dumps(registro, ensure_ascii=False) + "\n")
            gravados += 1

    print(f"Lidos: {lidos} | preparados: {gravados}")
    if gravados == 0:
        raise ValueError("Nenhum contrato válido para carregar.")


def salvar(**context):
    """Cria a tabela (se não existir) e faz o upsert em lotes de 500."""
    colunas_sql = ", ".join(COLUNAS)
    placeholders = ", ".join(["%s"] * len(COLUNAS))
    atualizacoes = ", ".join(f"{c} = EXCLUDED.{c}" for c in COLUNAS if c != "id")
    upsert_sql = (
        f"INSERT INTO {PG_TABLE} ({colunas_sql}) VALUES ({placeholders}) "
        f"ON CONFLICT (id) DO UPDATE SET {atualizacoes}, data_carga = now()"
    )

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER,
        password=PG_PASSWORD, dbname=PG_DATABASE,
    )
    total = 0
    try:
        with conn:  # commit no final; se der erro, rollback de tudo
            with conn.cursor() as cur:
                cur.execute(DDL_CONTRATOS)

                # Envia em blocos de 10 mil linhas para não lotar a memória;
                # o execute_batch agrupa de 500 em 500 por ida ao banco.
                lote = []
                for registro in ler_jsonl(TMP_PREP):
                    lote.append(tuple(registro[c] for c in COLUNAS))
                    if len(lote) == 10_000:
                        psycopg2.extras.execute_batch(cur, upsert_sql, lote, page_size=500)
                        total += len(lote)
                        lote = []
                if lote:
                    psycopg2.extras.execute_batch(cur, upsert_sql, lote, page_size=500)
                    total += len(lote)
    finally:
        conn.close()

    print(f"Upsert concluído: {total} contratos.")


# --- DAG -----------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 2,                         # se a task falhar, tenta mais 2 vezes
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="api_contratos_ceara_postgresql",
    schedule="0 19 * * *",                # todo dia às 19:00
    start_date=pendulum.datetime(2025, 1, 1, tz=TIMEZONE),
    catchup=False,                        # não executa os dias passados
    max_active_runs=1,                    # duas execuções não disputam o /tmp
    default_args=default_args,
    tags=["ceara", "contratos", "postgresql"],
) as dag:
    t1 = PythonOperator(task_id="definir_periodo", python_callable=definir_periodo)
    t2 = PythonOperator(
        task_id="coletar",
        python_callable=coletar,
        execution_timeout=timedelta(hours=1),  # se a API travar, a task não fica presa
    )
    t3 = PythonOperator(task_id="preparar", python_callable=preparar)
    t4 = PythonOperator(task_id="salvar", python_callable=salvar)

    t1 >> t2 >> t3 >> t4  # pipeline sequencial