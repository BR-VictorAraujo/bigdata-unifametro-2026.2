# Contratos do Ceará Transparente → PostgreSQL com Apache Airflow 3

[![CI](https://github.com/BR-VictorAraujo/bigdata-unifametro-2026.2/actions/workflows/ci.yml/badge.svg)](https://github.com/BR-VictorAraujo/bigdata-unifametro-2026.2/actions/workflows/ci.yml)

Atividade 01 da disciplina **Armazenamento e Processamento de Dados Massivos na Nuvem** (pós-graduação Unifametro, 2026.2).

Pipeline de ETL que coleta os **contratos dos exercícios 2025 e 2026** da [API de Dados Abertos do Ceará Transparente](https://api-dados-abertos.cearatransparente.ce.gov.br/api-docs/) e carrega no **PostgreSQL** (banco `aula`, tabela `contratos`) com **upsert**, agendado **todos os dias às 19:00** (America/Fortaleza).

```mermaid
flowchart LR
    API[(API Ceará<br/>Transparente)] --> T1
    subgraph Airflow["DAG api_contratos_ceara_postgresql (0 19 * * *)"]
        T1[definir_periodo] --> T2[coletar] --> T3[preparar] --> T4[salvar]
    end
    T1 -. periodo.json .-> T2
    T2 -. contratos_raw.jsonl .-> T3
    T3 -. contratos_prep.jsonl .-> T4
    T4 -->|INSERT ... ON CONFLICT DO UPDATE| PG[(PostgreSQL<br/>aula.contratos)]
```

## Resultado

| Exercício | Contratos | Valor total (R$) |
|---|---:|---:|
| 2025 | 47.655 | 10.964.207.834,04 |
| 2026 | 45.287 | 8.614.465.021,43 |
| **Total** | **92.942** | |

<sub>Carga de 23/10/2026. A base muda diariamente, então os números crescem com o tempo.</sub>

| Execução | Tipo | Duração | Estado |
|---|---|---|---|
| `manual__2026-10-23T12:40:40` | `airflow dags test` | ~16 min | ✅ success |
| `scheduled__2026-10-22T22:00:00` | agendada (19:00 Fortaleza = 22:00 UTC) | ~7 min | ✅ success |

Depois da execução agendada, o total continuou em **92.942** e o `MAX(data_carga)` avançou. Isso comprova que o upsert **atualiza sem duplicar**.

## Estrutura do repositório

```
.
├── dag_contratos_ceara.py          # a DAG (arquivo único entregue na atividade)
├── sql/
│   ├── ddl_contratos.sql           # DDL da tabela contratos
│   └── consultas_validacao.sql     # consultas para conferir a carga
├── tests/
│   └── test_dag_contratos_ceara.py # testes (pytest)
├── .github/workflows/ci.yml        # pipeline de CI (GitHub Actions)
├── requirements.txt                # dependências da DAG
├── requirements-dev.txt            # + pytest
└── .airflowignore                  # impede o Airflow de ler tests/ e sql/ como DAGs
```

## Requisitos atendidos

| Requisito | Onde |
|---|---|
| Airflow 3 (`airflow.sdk.DAG` + `PythonOperator` do provider standard) | imports da DAG |
| 4 tasks: `definir_periodo >> coletar >> preparar >> salvar` | fim do arquivo |
| Dados entre tasks por arquivo em `/tmp`, **não por XCom** | `TMP_*`; nenhuma callable retorna valor |
| `requests.Session`, `timeout`, `raise_for_status` e retries | `criar_sessao()` e `buscar_pagina()` |
| DDL gerada a partir de uma resposta real | `DDL_CONTRATOS` / `sql/ddl_contratos.sql` |
| Upsert `ON CONFLICT (id) DO UPDATE` com `execute_batch(page_size=500)` | `salvar()` |
| Cron `0 19 * * *`, `start_date` com pendulum em `America/Fortaleza`, `catchup=False` | `with DAG(...)` |
| Testado com `airflow dags test` | seção [Passo 6](#passo-6--testar-a-dag) |

---

## A API

```
GET https://api-dados-abertos.cearatransparente.ce.gov.br/transparencia/contratos/contratos
    ?page=1
    &data_assinatura_inicio=01/01/2025
    &data_assinatura_fim=31/12/2025
```

São os mesmos parâmetros do endpoint de convênios. As datas vão em `DD/MM/AAAA`. A resposta tem 100 registros por página:

```json
{
  "sumary": {"total_pages": 477, "current_page": "1", "total_records": 47651, "records_per_page": 100},
  "data": [{"id": 590499, "valor_contrato": "52820.0", "data_assinatura": "2025-01-02T00:00:00.000-03:00", "...": "..."}]
}
```

A coleta pede a página 1, lê `payload["sumary"]["total_pages"]` e percorre as páginas seguintes.

### Decisões de modelagem (DDL)

Inspecionando respostas reais de 2025 e 2026, cada contrato tem **68 campos**: os campos de convênios mais `data_finalizacao_prestacao_contas`, `sequential`, `emergency`, `law`, `has_non_profit_transfer`, `nome_fiscal` e três campos de emenda parlamentar.

| Como vem na API | Exemplo | Tipo no Postgres |
|---|---|---|
| Inteiro | `"flg_tipo": 51` | `INTEGER` / `BIGINT` |
| Valor monetário em texto | `"52820.0"` | `NUMERIC` (sem perda de precisão) |
| Data com hora e fuso | `"2025-01-02T00:00:00.000-03:00"` | `TIMESTAMPTZ` |
| Data simples | `"2026-01-03"` | `DATE` |
| Booleano | `"emergency": false` | `BOOLEAN` |
| Código com zero à esquerda | `"cod_orgao": "08000000"` | `TEXT` |

Tratamentos aplicados na task `preparar`:
- **Data-coringa:** a API usa `0001-01-01` (ex.: `"0001-01-01T00:00:00.000-02:34"`) para dizer "sem data". Esse valor vira `NULL`.
- **Texto vazio:** vira `NULL`.
- **Registros descartados:** os sem `id`, os sem `data_assinatura` e os `id` repetidos.
- **Nome da API mantido:** o campo `descriaco_edital` tem um erro de digitação, mas fica como a API manda.
- **Coluna de controle:** `data_carga` registra quando cada linha foi inserida ou atualizada pela última vez.

---

## Passo a passo para reproduzir

Pré-requisito: [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado. Os comandos abaixo são para o PowerShell do Windows.

### Passo 1 — Subir o PostgreSQL

```powershell
docker run --name aula_postgres -e POSTGRES_PASSWORD=1234 -p 5432:5432 -d postgres
```

Crie o banco da atividade:

```powershell
docker exec -it aula_postgres psql -U postgres -c "CREATE DATABASE aula;"
```

### Passo 2 — Criar a tabela

Copie a DDL para o contêiner e execute no banco `aula`. A DAG também cria a tabela sozinha se ela não existir.

```powershell
docker cp .\sql\ddl_contratos.sql aula_postgres:/tmp/ddl_contratos.sql
```

```powershell
docker exec -it aula_postgres psql -U postgres -d aula -f /tmp/ddl_contratos.sql
```

Conferência: o resultado deve ser **69** (68 campos da API + `data_carga`).

```powershell
docker exec -it aula_postgres psql -U postgres -d aula -c "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'contratos';"
```

### Passo 3 — Subir o Airflow 3

Usa a imagem oficial em modo `standalone`, que já traz Airflow, `requests` e `psycopg2`. O `-v` liga a pasta deste repositório à pasta de DAGs do Airflow, então não é preciso copiar arquivos.

```powershell
docker run -d --name aula_airflow -p 8080:8080 -v "${PWD}:/opt/airflow/dags" apache/airflow:3.1.0 standalone
```

> Rode o comando de dentro da pasta do repositório (é o `${PWD}`), ou troque pelo caminho completo, ex.: `-v "D:\Projetos\Python\Ceara-transparente:/opt/airflow/dags"`.

Acompanhe até aparecer `Airflow is ready` e depois aperte Ctrl+C:

```powershell
docker logs -f aula_airflow
```

Pegue a senha do usuário `admin` e acesse http://localhost:8080:

```powershell
docker exec aula_airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

### Passo 4 — Conferir se a DAG carregou

O esperado é `No data found`:

```powershell
docker exec aula_airflow airflow dags list-import-errors
```

Deve listar `api_contratos_ceara_postgresql`:

```powershell
docker exec aula_airflow airflow dags list
```

### Passo 5 — Testar a conexão com o banco

A DAG acessa o Postgres do Windows por `host.docker.internal`:

```powershell
docker exec aula_airflow python -c "import psycopg2; psycopg2.connect(host='host.docker.internal', user='postgres', password='1234', dbname='aula'); print('Conectou no banco aula!')"
```

### Passo 6 — Testar a DAG

Leva ~15 minutos: são ~930 páginas da API.

```powershell
docker exec aula_airflow airflow dags test api_contratos_ceara_postgresql
```

O Airflow só mostra os `print` das tasks no fim (ou na aba **Logs** da interface). Para acompanhar a coleta em tempo real, veja o arquivo crescer (até ~230 MB):

```powershell
docker exec aula_airflow ls -la /tmp/contratos_ceara
```

### Passo 7 — Conferir os dados

```powershell
docker exec -it aula_postgres psql -U postgres -d aula
```

```sql
SELECT EXTRACT(YEAR FROM data_assinatura AT TIME ZONE 'America/Fortaleza') AS exercicio,
       COUNT(*) AS contratos, SUM(valor_contrato) AS valor_total
FROM contratos GROUP BY 1 ORDER BY 1;
```

Mais consultas, incluindo a do teste do upsert, em [`sql/consultas_validacao.sql`](sql/consultas_validacao.sql).

### Passo 8 — Ligar o agendamento das 19:00

Toda DAG nova nasce pausada. Ative pelo botão na tela **DAGs** ou pelo terminal:

```powershell
docker exec aula_airflow airflow dags unpause api_contratos_ceara_postgresql
```

> O agendamento só roda com o Docker Desktop e os dois contêineres ligados. Depois de reiniciar o PC: `docker start aula_postgres aula_airflow`.

---

## Testes automatizados (CI)

A cada `push`, o GitHub Actions ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) instala o **Airflow 3.1.0** com a constraints file oficial, sobe um **PostgreSQL** de teste e roda o `pytest`:

| Grupo | O que valida |
|---|---|
| Estrutura | A DAG carrega sem erros (`DagBag`), tem 4 tasks na ordem certa, cron `0 19 * * *` no fuso de Fortaleza, `catchup=False` e retries |
| Unidade | Limpeza de valores (texto vazio, data-coringa, zeros à esquerda), sessão HTTP com retries para 429/5xx, parâmetros da requisição e `raise_for_status` |
| Pipeline offline | `definir_periodo → coletar → preparar` com a API simulada: arquivos em `/tmp`, nenhum retorno para o XCom, descarte de duplicados e de registros sem data |
| Integração | `salvar` no PostgreSQL: tipos corretos (`TIMESTAMPTZ`, `NUMERIC`, `BOOLEAN`), e uma segunda carga **atualiza sem duplicar** |

Para rodar localmente, dentro do contêiner do Airflow:

```powershell
docker exec aula_airflow python -m pip install --target /tmp/pytestlib pytest
```

```powershell
docker exec -w /opt/airflow/dags -e PYTHONPATH=/tmp/pytestlib aula_airflow python -m pytest -v
```

O teste de integração só roda se `PG_TEST_HOST` estiver definido (ex.: `-e PG_TEST_HOST=host.docker.internal -e PG_TEST_DATABASE=aula_teste`, com o banco `aula_teste` criado antes).

## Problemas comuns

| Erro | Causa e solução |
|---|---|
| `could not connect ... host.docker.internal` | `aula_postgres` parado: `docker start aula_postgres` |
| `database "aula" does not exist` | Falta o `CREATE DATABASE aula;` do Passo 1 |
| DAG não aparece | Veja `airflow dags list-import-errors` |
| `Conflict. The container name ... is already in use` | Já existe um contêiner com esse nome: `docker rm -f aula_airflow` |
| Terminal "parado" durante o teste | Normal: os logs das tasks só aparecem no fim. Acompanhe pelo tamanho do arquivo em `/tmp/contratos_ceara` |

> As credenciais (`postgres`/`1234`) são as do ambiente local da aula. Em produção, use uma Airflow Connection ou variáveis de ambiente.
