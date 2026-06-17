# Guia de Demonstração — Plataforma de Detecção de Fraudes em Saúde

Este guia percorre cada camada da plataforma de ponta a ponta, desde a modelagem do banco de dados até o resultado do modelo de Machine Learning. O objetivo é que qualquer pessoa consiga entender **o que acontece, por quê acontece e como verificar** em cada etapa.

> Para o deploy completo no Linode LKE, ver `docs/linode_deploy.md`.
> Para a história da migração Python → Flink/Spark, ver `docs/flink_spark_implantacao_real.md`.

---

## Opção A — Demo Local (Docker Compose)

Não precisa de cluster Kubernetes. Usa o mesmo Linode Object Storage do LKE.

```bash
# 1. Exporte as credenciais do Object Storage (Terraform já provisionou o bucket)
export AWS_ACCESS_KEY_ID=$(cd infra/terraform && terraform output -raw object_storage_access_key)
export AWS_SECRET_ACCESS_KEY=$(cd infra/terraform && terraform output -raw object_storage_secret_key)
export AWS_ENDPOINT_URL_S3=$(cd infra/terraform && terraform output -raw object_storage_endpoint)
export BUCKET=$(cd infra/terraform && terraform output -raw object_storage_bucket)

# 2. Suba todos os serviços (~3 min na primeira vez — Flink e Spark compilam do source)
cd demo/
make start

# 3. Acompanhe as detecções em tempo real
make logs-alerts

# 4. Após alguns minutos com dados acumulando no Object Storage, rode os ETLs
make etl-silver    # Spark: bronze Object Storage → silver Iceberg
make etl-gold      # Spark: silver Iceberg → gold feature tables
```

| Serviço | URL local |
|---|---|
| Redpanda Console | http://localhost:8080 |
| Fraud Scorer API | http://localhost:8000/docs |
| MLflow | http://localhost:5000 |
| Grafana | http://localhost:3000 (admin/admin) |
| Prometheus | http://localhost:9090 |
| Nessie API | http://localhost:19120/api/v1/config |

---

## Opção B — Cluster LKE (Akamai Linode)

```bash
# Configure o acesso ao cluster
export KUBECONFIG=/caminho/para/.kubeconfig-demo

# Confirme que os nodes estão acessíveis
kubectl get nodes
```

```
NAME                                  STATUS   ROLES    AGE
lke609841-894061-03ea6e730000   Ready    <none>   1d
lke609841-894061-102f59980000   Ready    <none>   1d
lke609841-894061-1a1490870000   Ready    <none>   1d
```

**IP de acesso:** `104.64.45.51` (qualquer um dos três nós funciona para NodePorts)

| Serviço | URL |
|---|---|
| Grafana | http://104.64.45.51:30300 — admin / admin |
| Redpanda Console | http://104.64.45.51:30808 |
| Fraud Scorer API | http://104.64.45.51:30800/docs |
| MLflow | http://104.64.45.51:30500 |
| Prometheus | http://104.64.45.51:30909 |

---

## Visão Geral do Fluxo

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  CAMADA 1 — BANCO DE DADOS                                                   │
│  PostgreSQL: tabelas providers, members, claims                               │
│  WAL (Write-Ahead Log) habilitado para CDC                                   │
└───────────────────────────┬──────────────────────────────────────────────────┘
                            │  INSERT em claims
                            ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  CAMADA 2 — CDC (Change Data Capture)                                        │
│  Debezium lê o WAL do PostgreSQL em tempo real                               │
│  Publica cada INSERT como evento JSON no Redpanda                            │
└───────────────────────────┬──────────────────────────────────────────────────┘
                            │  tópico: raw.claims.new
                            ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  CAMADA 3 — STREAMING (Redpanda)                                             │
│  Broker Kafka-compatível, 3 tópicos, sem ZooKeeper, sem JVM                 │
└───────────────┬─────────────────────────────────────┬────────────────────────┘
                │                                     │
                ▼                                     ▼
┌──────────────────────────┐          ┌───────────────────────────────────────┐
│  CAMADA 4 — STREAM       │          │  CAMADA 6 — DATA LAKE BRONZE          │
│  PROCESSING              │          │  Bronze Consumer escreve NDJSON       │
│  Enriquecimento + Regras │          │  no Object Storage (S3)               │
│  + ML scoring            │          └──────────────┬────────────────────────┘
└──────────┬───────────────┘                         │
           │                                         ▼
           ▼                          ┌───────────────────────────────────────┐
┌──────────────────────────┐          │  CAMADA 7 — SPARK SILVER ETL          │
│  CAMADA 5 — MACHINE      │          │  Tokeniza PHI, normaliza, salva       │
│  LEARNING (Fraud Scorer) │          │  em Iceberg via Nessie catalog        │
│  GBM — 8 features        │          └──────────────┬────────────────────────┘
└──────────┬───────────────┘                         │
           │                                         ▼
           │ scored.claims                ┌──────────────────────────────────┐
           │ alerts.fraud                 │  CAMADA 8 — SPARK GOLD FEATURES  │
           ▼                              │  Agrega janelas 30d/90d          │
┌────────────────────────┐                │  Gera vetores de features para ML │
│  CAMADA 9 —            │                └──────────────────────────────────┘
│  OBSERVABILIDADE       │
│  Prometheus + Grafana   │
└────────────────────────┘
```

---

## Camada 1 — Banco de Dados (PostgreSQL)

### O que é e por que está aqui

O PostgreSQL representa o sistema de origem — o sistema transacional que registra as autorizações de procedimentos médicos. Em produção, seria um banco de dados de um sistema de gestão de planos de saúde. Na demonstração, um gerador sintético insere claims automaticamente.

### Modelagem de dados

O banco tem três tabelas principais:

**`providers`** — cadastro de prestadores de serviço (clínicas, hospitais, laboratórios)

```sql
CREATE TABLE providers (
    npi         VARCHAR(20)  PRIMARY KEY,    -- National Provider Identifier
    name        VARCHAR(200) NOT NULL,
    specialty   VARCHAR(100) NOT NULL,       -- CARDIOLOGY, ORTHOPEDICS, LABORATORY...
    license_no  VARCHAR(50),
    status      VARCHAR(20)  DEFAULT 'ACTIVE',
    created_at  TIMESTAMP    DEFAULT NOW()
);
```

**`members`** — cadastro de beneficiários do plano

```sql
CREATE TABLE members (
    member_id   VARCHAR(20) PRIMARY KEY,
    plan_id     VARCHAR(20),                 -- PLAN-GOLD, PLAN-SILVER, PLAN-BASIC
    status      VARCHAR(20) DEFAULT 'ACTIVE',
    enrolled_at DATE        DEFAULT CURRENT_DATE
);
```

**`claims`** — solicitações de reembolso (foco do sistema de fraude)

```sql
CREATE TABLE claims (
    id              SERIAL      PRIMARY KEY,
    claim_id        VARCHAR(30) UNIQUE NOT NULL,   -- identificador único ex: CLM-A3F2B891
    provider_npi    VARCHAR(20) NOT NULL REFERENCES providers(npi),
    member_id       VARCHAR(20) NOT NULL,
    procedure_code  VARCHAR(10) NOT NULL,           -- código TUSS do procedimento
    diagnosis_code  VARCHAR(15) NOT NULL,           -- CID-10
    amount          NUMERIC(12,2) NOT NULL,         -- valor solicitado em R$
    service_date    DATE        NOT NULL,
    submitted_at    TIMESTAMP   NOT NULL DEFAULT NOW(),
    status          VARCHAR(20) DEFAULT 'PENDING',  -- PENDING, APPROVED, REJECTED
    created_at      TIMESTAMP   DEFAULT NOW()
);
```

> **Por que `REPLICA IDENTITY FULL`?**
> Por padrão o PostgreSQL só loga o valor das colunas da chave primária no WAL. Com `FULL`, ele loga o valor de **todas** as colunas antes e depois de cada mudança. O Debezium precisa disso para capturar o estado completo de cada UPDATE e DELETE.

```sql
ALTER TABLE claims REPLICA IDENTITY FULL;
```

**Índices de performance:**

```sql
CREATE INDEX idx_claims_provider  ON claims(provider_npi);   -- queries por prestador
CREATE INDEX idx_claims_member    ON claims(member_id);      -- queries por beneficiário
CREATE INDEX idx_claims_submitted ON claims(submitted_at);   -- queries temporais
CREATE INDEX idx_claims_status    ON claims(status);         -- filtros por status
```

### Prestadores cadastrados para a demo

| NPI | Nome | Especialidade | Comportamento |
|---|---|---|---|
| NPI-001 | Cardio Clinic SP | CARDIOLOGY | Legítimo, com desvios ocasionais de valor |
| NPI-002 | Ortho Hospital | ORTHOPEDICS | Legítimo |
| NPI-003 | MedLab Diagnostics | LABORATORY | Legítimo, valores baixos |
| NPI-004 | PhysioPlus | PHYSIOTHERAPY | Legítimo |
| NPI-005 | Suspect Clinic | CARDIOLOGY | **Fraudulento** — 100% das claims são suspeitas |

### Como verificar o banco em execução

```bash
# Acessar o PostgreSQL no cluster
kubectl exec -n data -it postgres-postgresql-0 -- \
  psql -U fraud_user -d claims_db
```

```sql
-- Quantas claims foram inseridas até agora
SELECT COUNT(*) FROM claims;

-- Ver as últimas 5 claims inseridas
SELECT claim_id, provider_npi, procedure_code, diagnosis_code, amount, submitted_at
FROM claims
ORDER BY submitted_at DESC
LIMIT 5;

-- Comparar valores médios por prestador — NPI-005 terá médias muito maiores
SELECT provider_npi,
       COUNT(*)         AS total,
       AVG(amount)::NUMERIC(10,2) AS avg_amount,
       MAX(amount)      AS max_amount
FROM claims
GROUP BY provider_npi
ORDER BY avg_amount DESC;
```

Resultado esperado:

```
 provider_npi | total | avg_amount | max_amount
--------------+-------+------------+------------
 NPI-005      |   89  |  9421.33   |  18750.00   ← valores muito elevados
 NPI-001      |  142  |  1203.45   |   8200.00
 NPI-002      |  115  |   876.22   |   4100.00
 NPI-004      |   98  |   210.50   |    890.00
 NPI-003      |  203  |   145.30   |    620.00
```

### Verificar o WAL (Write-Ahead Log)

O CDC funciona porque o PostgreSQL grava toda mudança no WAL antes de aplicar no disco. Podemos confirmar que o `wal_level` está como `logical` (obrigatório para Debezium):

```bash
kubectl exec -n data -it postgres-postgresql-0 -- \
  psql -U fraud_user -d claims_db -c "SHOW wal_level;"
```

```
 wal_level
-----------
 logical
```

---

## Camada 2 — CDC com Debezium

### O que é CDC e por que usamos

**Change Data Capture (CDC)** é a técnica de capturar cada inserção, atualização ou exclusão no banco de dados em tempo real, sem modificar a aplicação. Em vez de fazer polling (`SELECT * WHERE updated_at > ?`), o Debezium lê diretamente o **WAL** (Write-Ahead Log) do PostgreSQL — o mesmo log de transações que o banco usa para garantir durabilidade.

Vantagens do CDC sobre polling:
- Captura **toda** mudança, inclusive deletes e múltiplos updates rápidos
- Latência de milissegundos (não de segundos ou minutos)
- Sem carga adicional no banco (apenas leitura de log)
- Preserva a ordem exata das transações

### Como o Debezium funciona

```
PostgreSQL WAL  →  Debezium (replication slot)  →  Redpanda (tópico)
```

1. O PostgreSQL mantém um **replication slot** chamado `debezium_claims_slot`
2. O Debezium se conecta a esse slot como se fosse uma réplica
3. Cada transação confirmada no WAL chega ao Debezium como um evento
4. O Debezium serializa o evento para JSON e publica no Redpanda

### Configuração do conector

O conector foi registrado via API REST do Debezium (Kafka Connect):

```json
{
  "name": "claims-postgres-connector",
  "config": {
    "connector.class":                    "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname":                  "postgres",
    "database.port":                      "5432",
    "database.user":                      "fraud_user",
    "database.dbname":                    "claims_db",
    "topic.prefix":                       "dbz",
    "table.include.list":                 "public.claims",
    "plugin.name":                        "pgoutput",
    "slot.name":                          "debezium_claims_slot",
    "publication.name":                   "debezium_publication",
    "transforms":                         "unwrap",
    "transforms.unwrap.type":             "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.add.fields":       "op,ts_ms",
    "snapshot.mode":                      "initial"
  }
}
```

**Parâmetros importantes:**

| Parâmetro | Valor | Explicação |
|---|---|---|
| `plugin.name` | `pgoutput` | Plugin de replicação nativo do PostgreSQL 10+ (não precisa instalar nada extra) |
| `snapshot.mode` | `initial` | Na primeira execução, faz snapshot de todas as rows existentes antes de passar para streaming |
| `transforms.unwrap` | `ExtractNewRecordState` | Por padrão o Debezium publica `{"before": {...}, "after": {...}}`. O `unwrap` extrai apenas o estado novo (campo `after`), simplificando o consumo downstream |
| `transforms.unwrap.add.fields` | `op,ts_ms` | Mantém `__op` (tipo da operação: `c`=create, `u`=update, `d`=delete) e `__ts_ms` (timestamp da transação) |

### Mensagem bruta do Debezium (antes do unwrap)

```json
{
  "before": null,
  "after": {
    "id": 1024,
    "claim_id": "CLM-A3F2B891",
    "provider_npi": "NPI-001",
    "member_id": "MBR-4521",
    "procedure_code": "93000",
    "diagnosis_code": "I10",
    "amount": "362.50",
    "service_date": "2026-05-28",
    "submitted_at": "2026-05-28T14:22:10.432000Z",
    "status": "PENDING"
  },
  "op": "c",
  "ts_ms": 1748432400000
}
```

### Mensagem após o unwrap (o que chega ao Redpanda)

```json
{
  "id": 1024,
  "claim_id": "CLM-A3F2B891",
  "provider_npi": "NPI-001",
  "member_id": "MBR-4521",
  "procedure_code": "93000",
  "diagnosis_code": "I10",
  "amount": "362.50",
  "service_date": "2026-05-28",
  "submitted_at": "2026-05-28T14:22:10.432000Z",
  "status": "PENDING",
  "__op": "c",
  "__ts_ms": 1748432400000
}
```

### Como verificar o Debezium em execução

```bash
# Status do conector
kubectl exec -n streaming deploy/debezium -- \
  curl -s http://localhost:8083/connectors/claims-postgres-connector/status \
  | python3 -m json.tool
```

Resultado esperado:

```json
{
  "name": "claims-postgres-connector",
  "connector": { "state": "RUNNING" },
  "tasks": [
    { "id": 0, "state": "RUNNING" }
  ]
}
```

```bash
# Verificar o replication slot no PostgreSQL
kubectl exec -n data -it postgres-postgresql-0 -- \
  psql -U fraud_user -d claims_db \
  -c "SELECT slot_name, plugin, active, confirmed_flush_lsn FROM pg_replication_slots;"
```

```
     slot_name          | plugin   | active | confirmed_flush_lsn
------------------------+----------+--------+---------------------
 debezium_claims_slot   | pgoutput | t      | 0/4A3B2C10
```

`active = t` confirma que o Debezium está consumindo o WAL ativamente.

---

## Camada 3 — Redpanda (Streaming)

### O que é e por que usamos

O **Redpanda** é um broker de mensagens compatível com a API do Apache Kafka, porém escrito em C++ (sem JVM, sem ZooKeeper). Ele recebe os eventos do Debezium e os distribui para todos os consumidores interessados. Na arquitetura de streaming, ele é o **"barramento central"** — nenhum serviço se comunica diretamente com outro, tudo passa pelo Redpanda.

Benefícios para detecção de fraudes:
- **Múltiplos consumidores independentes**: o Stream Processor e o Bronze Consumer leem o mesmo tópico cada um com seu próprio offset, sem interferência
- **Replay**: se um consumidor cair, ele retoma exatamente de onde parou
- **Persistência**: as mensagens ficam armazenadas (por padrão 7 dias), permitindo re-processar o histórico
- **Particionamento por `provider_npi`**: todas as claims do mesmo prestador chegam na mesma partição, em ordem, facilitando detecção de padrões

### Tópicos criados

```bash
kubectl exec -n streaming redpanda-0 -- rpk topic list
```

```
NAME               PARTITIONS  REPLICAS
raw.claims.new     4           1    ← claims entrando (Debezium + gerador)
scored.claims      4           1    ← claims com score de fraude
alerts.fraud       1           1    ← apenas alertas HIGH e CRITICAL
_schemas           1           1    ← schema registry interno (automático)
_debezium.*        -           -    ← controle interno do Debezium (automático)
```

**Por que 4 partições em `raw.claims.new`?**
Cada partição pode ser processada por um consumidor diferente em paralelo. Com 4 partições e 2 réplicas do Stream Processor, cada réplica processa 2 partições simultaneamente — dobrando o throughput sem conflito.

**Por que `alerts.fraud` tem apenas 1 partição?**
Alertas são poucos (~8% das claims) e precisam ser lidos em ordem por sistemas de case management. Uma única partição garante ordenação total.

### Navegar pelos tópicos no Redpanda Console

Abra **http://104.64.45.51:30808** e clique em **Topics**.

**`raw.claims.new` → aba Messages:**

Cada mensagem tem `provider_npi` como chave (garante que claims do mesmo prestador vão para a mesma partição):

```json
{
  "claim_id": "CLM-A3F2B891",
  "provider_npi": "NPI-001",
  "provider_name": "Cardio Clinic SP",
  "member_id": "MBR-4521",
  "procedure_code": "93000",
  "diagnosis_code": "I10",
  "amount": 362.50,
  "service_date": "2026-05-28",
  "submitted_at": "2026-05-28T14:22:10.432"
}
```

**`alerts.fraud` → aba Messages:**

```json
{
  "claim_id": "CLM-D9E4A102",
  "provider_npi": "NPI-005",
  "amount": 8750.00,
  "fraud_score": 0.9312,
  "risk_level": "CRITICAL",
  "triggered_rules": ["PROCEDURE_DIAGNOSIS_MISMATCH", "AMOUNT_3X_PROVIDER_AVG"],
  "rule_score": 0.93,
  "ml_score": 0.891,
  "provider_stats": {
    "claim_count_30d": 47,
    "avg_amount_30d": 920.40
  }
}
```

### Verificar consumer groups (lag)

No Redpanda Console → **Consumer Groups**, você verá os grupos ativos:

| Group ID | Tópico | Lag | Significado |
|---|---|---|---|
| `flink-fraud-stream-v2` | `raw.claims.new` | 0–5 | Job Flink (`fraud-stream-job`) consumindo em tempo real |
| `bronze-consumer-demo` | `raw.claims.new` | 0–50 | Bronze Consumer, slight lag (escreve em batches de 30s) |

Lag próximo de zero indica que os consumidores estão acompanhando a produção. Se o lag crescer, indica que o processamento está mais lento do que a ingestão.

```bash
# Verificar via linha de comando
kubectl exec -n streaming redpanda-0 -- \
  rpk group describe flink-fraud-stream-v2
```

---

## Camada 4 — Stream Processing (Apache Flink)

### O que é e por que está aqui

O **Apache Flink** é o cérebro em tempo real da plataforma. Ele consome cada claim do Redpanda, enriquece com contexto acumulado (keyed state em RocksDB), aplica regras determinísticas e consulta o modelo de ML — tudo antes de publicar o resultado, com latência sub-segundo.

**No LKE:** `FlinkDeployment` CRD gerenciado pelo Flink Kubernetes Operator (`infra/k8s/14-flink.yaml`). O job Python (`infra/flink-jobs/fraud_stream_job.py`) roda em PyFlink 1.18 com RocksDB como state backend e checkpoints automáticos a cada 60s para o Object Storage.

**Na demo local (Docker Compose):** mesmo `fraud_stream_job.py` rodando em modo local (mini-cluster PyFlink em processo único).

O antigo processo Python (`demo/stream_processor/processor.py`) fica em `05-stream-processor.yaml` com `replicas: 0` — fallback documentado, desativado. Nunca delete: serve para troubleshooting se o Flink job precisar ser interrompido.

Para a história completa da migração (causas raiz, bugs encontrados, validação), ver `docs/flink_spark_implantacao_real.md`.

### Fluxo de processamento de uma claim

```
raw.claims.new
    │
    ▼
[1] ENRIQUECIMENTO (Redis)
    │  Busca estatísticas do prestador nos últimos 30 dias
    │  Busca histórico do beneficiário nos últimos 90 dias
    ▼
[2] SCORE POR REGRAS (determinístico)
    │  4 regras de negócio hardcoded
    │  Score entre 0.0 e 0.93
    ▼
[3] SCORE ML (Fraud Scorer API)
    │  Chama POST /score com 8 features
    │  Retorna probabilidade entre 0.0 e 1.0
    ▼
[4] COMBINAÇÃO DE SCORES
    │  final = max(rule_score, ml_score*0.6 + rule_score*0.4)
    ▼
[5] PUBLICAÇÃO
    │  → scored.claims (sempre)
    │  → alerts.fraud (se score ≥ 0.65)
    └  → log WARNING se FRAUD
```

### Etapa 1 — Enriquecimento com Flink Keyed State (RocksDB)

O Flink mantém estatísticas acumuladas **dentro do próprio job**, em **keyed state** gerenciado pelo RocksDB — sem depender do Redis para essa função.

**Como funciona:** o operador `ProviderEnrichFunction` é particionado por `provider_npi`. Todas as claims do mesmo prestador chegam sempre ao mesmo TaskManager, que mantém o estado localmente:

```python
class ProviderEnrichFunction(KeyedProcessFunction):
    def open(self, runtime_context):
        descriptor = ValueStateDescriptor("provider-stats", Types.PICKLED_BYTE_ARRAY())
        descriptor.enable_time_to_live(_ttl(30))   # expira após 30 dias sem atividade
        self.state = runtime_context.get_state(descriptor)

    def process_element(self, claim, ctx):
        stats = self.state.value() or {"sum": 0.0, "count": 0}
        stats["sum"] += claim["amount"]
        stats["count"] += 1
        self.state.update(stats)
        claim["claim_count_30d"] = stats["count"]
        claim["avg_amount_30d"] = round(stats["sum"] / stats["count"], 2)
        yield claim
```

Para inspecionar o estado acumulado por prestador, consulte os logs do TaskManager:
```bash
kubectl logs -n processing -l component=taskmanager | grep "provider=NPI-005" | tail -5
```

**Por que RocksDB e não Redis?**
- Estado co-localizado com o processamento: **zero latência de rede** na leitura/escrita
- Checkpoints automáticos a cada 60s vão para o Object Storage — estado sobrevive a reinicializações
- Redis continua presente no cluster para o **Feast feature store** (features de serving em tempo real), não para o estado do Flink

### Etapa 2 — Score por Regras

Quatro regras determinísticas, com scores fixos baseados em conhecimento de negócio:

| Regra | Condição | Score | Código |
|---|---|---|---|
| **R1** `AMOUNT_3X_PROVIDER_AVG` | Valor > 3× média do prestador nos últimos 30 dias | 0.78 | Phantom billing |
| **R2** `EXTREME_AMOUNT` | Valor absoluto > R$ 50.000 | 0.88 | Faturamento incomum |
| **R3** `PROCEDURE_DIAGNOSIS_MISMATCH` | Procedimento ortopédico + diagnóstico cardiovascular | 0.93 | Upcoding |
| **R4** `HIGH_VOLUME_PROVIDER` | Prestador com > 200 claims em 30 dias | 0.62 | Faturamento em massa |

> **Por que regras determinísticas se temos ML?**
> Regras explícitas são auditáveis — em saúde é obrigatório explicar por que uma claim foi bloqueada. O regulador e o prestador têm direito a saber exatamente qual regra foi violada. O ML complementa com padrões que as regras não conseguem expressar.

O score das regras é o **máximo** entre as regras disparadas. Uma claim que viola R1 (0.78) **e** R3 (0.93) tem score de regra = 0.93.

### Etapa 3 — Score ML

O Stream Processor chama a API do Fraud Scorer:

```bash
# Exemplo manual via curl
curl -s -X POST http://104.64.45.51:30800/score \
  -H "Content-Type: application/json" \
  -d '{
    "claim_id":               "CLM-D9E4A102",
    "amount":                 8750.00,
    "claim_count_30d":        47,
    "avg_amount_30d":         920.40,
    "hour_of_day":            14,
    "is_weekend":             0,
    "procedure_risk":         0.90,
    "member_claim_count_90d": 1
  }' | python3 -m json.tool
```

```json
{
  "claim_id": "CLM-D9E4A102",
  "fraud_probability": 0.891,
  "risk_level": "HIGH",
  "model_version": "gbm-synthetic-v1"
}
```

### Etapa 4 — Combinação dos Scores

```python
final = max(rule_score, ml_score * 0.6 + rule_score * 0.4)
```

**Lógica:** quando as regras disparam (rule_score > 0), elas têm mais peso (40%) do que o ML isolado, porque são auditáveis. Se as regras não disparam (rule_score = 0), o ML decide sozinho.

| Cenário | rule_score | ml_score | final | Explicação |
|---|---|---|---|---|
| Regra forte + ML alto | 0.93 | 0.891 | max(0.93, 0.53+0.37) = **0.93** | Regra domina |
| Só ML alto, sem regra | 0.00 | 0.85 | max(0.0, 0.51+0.0) = **0.51** | ML moderado |
| Legítimo | 0.00 | 0.05 | max(0.0, 0.03+0.0) = **0.03** | Baixo risco |

**Mapeamento de risk_level:**

| Score | Nível |
|---|---|
| ≥ 0.90 | CRITICAL |
| ≥ 0.70 | HIGH |
| ≥ 0.40 | MEDIUM |
| < 0.40 | LOW |
| ≥ 0.65 | → publica em `alerts.fraud` |

### Ver o Flink job em execução

```bash
# Status do FlinkDeployment (LKE)
kubectl get flinkdeployment -n processing
# NAME               JOB STATUS   LIFECYCLE STATE
# fraud-stream-job   RUNNING      STABLE

# Logs de fraudes em tempo real (TaskManager)
kubectl logs -f -n processing -l component=taskmanager | grep FRAUD
```

```
14:22:18  WARNING  FRAUD [CRITICAL ] CLM-D9E4A102  provider=NPI-005  amount=R$  8,750.00  score=0.9312  rules=['PROCEDURE_DIAGNOSIS_MISMATCH', 'AMOUNT_3X_PROVIDER_AVG']
14:22:24  WARNING  FRAUD [HIGH    ] CLM-F1B3C007  provider=NPI-001  amount=R$  3,140.00  score=0.7821  rules=['AMOUNT_3X_PROVIDER_AVG']
14:22:31  WARNING  FRAUD [CRITICAL ] CLM-88EA5D12  provider=NPI-005  amount=R$ 12,050.00  score=0.9601  rules=['EXTREME_AMOUNT', 'PROCEDURE_DIAGNOSIS_MISMATCH']
```

**Na demo local (Docker Compose):**
```bash
make logs-alerts    # equivalente ao grep FRAUD acima
# ou
docker compose logs -f flink-fraud-stream | grep FRAUD
```

---

## Camada 5 — Machine Learning (Fraud Scorer)

### O que o modelo faz

O **Fraud Scorer** é uma API FastAPI que serve um modelo de **Gradient Boosting Machine (GBM)** treinado ao inicializar. O modelo recebe 8 features numéricas sobre uma claim e retorna a probabilidade de fraude (0.0 a 1.0).

Em produção, este modelo seria treinado semanalmente sobre os dados da camada Gold do data lake, versionado no MLflow e servido via BentoML. Na demonstração, é treinado em dados sintéticos para ser auto-suficiente.

### As 8 features do modelo

| # | Feature | Tipo | Descrição | Por que importa |
|---|---|---|---|---|
| 0 | `amount` | float | Valor da claim em R$ | Valores extremos são suspeitos |
| 1 | `claim_count_30d` | int | Qtd de claims do prestador em 30 dias | Prestadores fraudulentos têm volume anormal |
| 2 | `avg_amount_30d` | float | Valor médio do prestador em 30 dias | Baseline para detectar desvio |
| 3 | `amount_ratio` | float | `amount / avg_amount_30d` | Desvio proporcional — calculado na API |
| 4 | `hour_of_day` | int | Hora da submissão (0-23) | Fraudes ocorrem mais em horários atípicos (2h, 3h) |
| 5 | `is_weekend` | int | 1 se sábado ou domingo | Prestadores legítimos operam menos no fim de semana |
| 6 | `procedure_risk` | float | Score de risco do procedimento (0.0–1.0) | Procedimentos caros têm risco base mais alto |
| 7 | `member_claim_count_90d` | int | Qtd de claims do beneficiário em 90 dias | Beneficiários novos (count=1) são sinal de fraude |

**Tabela de risco por procedimento:**

| Procedimento | Código | Risco | Especialidade |
|---|---|---|---|
| Artroplastia total do joelho | 27447 | 0.90 | Ortopedia |
| Reconstrução do manguito rotador | 29827 | 0.85 | Ortopedia |
| Prótese total de quadril | 27130 | 0.88 | Ortopedia |
| ECG simples | 93000 | 0.20 | Cardiologia |
| Hemograma | 85025 | 0.05 | Laboratório |
| Fisioterapia | 97110 | 0.10 | Fisioterapia |

### Como o modelo aprende

O modelo é treinado com **dados sintéticos** que simulam o padrão esperado:

```
Dados legítimos (9.500 amostras):
  amount       → LogNormal (média ~R$650)
  claim_count  → Poisson (média 15/mês)
  amount_ratio → Normal (média 1.0, desvio ±0.15)
  hour_of_day  → Horário comercial (8h–18h)
  is_weekend   → 28% dos casos
  procedure_risk → Beta(2,8) — baixo risco concentrado

Dados fraudulentos (500 amostras):
  amount       → LogNormal (média ~R$8.000)
  claim_count  → Poisson (média 120/mês)
  amount_ratio → Uniforme (4× a 12× a média)
  hour_of_day  → Horários atípicos (2h, 3h, 4h, 22h, 23h)
  is_weekend   → 65% dos casos
  procedure_risk → Beta(7,2) — alto risco concentrado
```

O GBM aprende os padrões que separam esses dois grupos. Métricas do modelo treinado:

```
AUC-ROC:   ~0.943   (1.0 = perfeito, 0.5 = aleatório)
Avg Precision: ~0.721
```

### Testar o modelo manualmente

Acesse **http://104.64.45.51:30800/docs** → `POST /score` → **Try it out**

**Cenário A — Claim legítima (exame de laboratório):**
```json
{
  "claim_id":               "TEST-LEGIT-001",
  "amount":                 145.00,
  "claim_count_30d":        18,
  "avg_amount_30d":         160.00,
  "hour_of_day":            10,
  "is_weekend":             0,
  "procedure_risk":         0.05,
  "member_claim_count_90d": 8
}
```
Resultado esperado: `fraud_probability` ≈ 0.02–0.06, `risk_level`: **LOW**

**Cenário B — Upcoding (cirurgia de joelho cobrada por clínica de cardiologia):**
```json
{
  "claim_id":               "TEST-FRAUD-001",
  "amount":                 11500.00,
  "claim_count_30d":        145,
  "avg_amount_30d":         820.00,
  "hour_of_day":            3,
  "is_weekend":             1,
  "procedure_risk":         0.90,
  "member_claim_count_90d": 1
}
```
Resultado esperado: `fraud_probability` ≈ 0.88–0.97, `risk_level`: **CRITICAL**

**Cenário C — Caso limítrofe (valor elevado, mas em horário comercial):**
```json
{
  "claim_id":               "TEST-BORDERLINE-001",
  "amount":                 4200.00,
  "claim_count_30d":        35,
  "avg_amount_30d":         1100.00,
  "hour_of_day":            14,
  "is_weekend":             0,
  "procedure_risk":         0.65,
  "member_claim_count_90d": 12
}
```
Resultado esperado: `fraud_probability` ≈ 0.40–0.65, `risk_level`: **MEDIUM** ou **HIGH**

### Ver o MLflow (rastreamento de experimentos)

Acesse **http://104.64.45.51:30500**. Aqui ficam registrados os experimentos de treinamento com métricas, parâmetros e artefatos do modelo. Em produção, cada re-treino semanal cria um novo run no MLflow, permitindo comparar versões e fazer rollback.

---

## Camada 6 — Data Lake Bronze

### O que é e por que existe

A camada **Bronze** é o primeiro nível do data lake — o "arquivo fiel" de tudo que chegou. Cada mensagem do Redpanda é salva como **NDJSON** (JSON lines) no Akamai Object Storage sem nenhuma transformação. PHI (dados pessoais) está presente. Os dados são imutáveis — nada é deletado desta camada.

Por que NDJSON e não Parquet nesta camada?
- Simplicidade: o consumer não precisa de Spark, apenas de um cliente S3
- Schema flexível: se o schema mudar no futuro, o arquivo histórico continua válido
- Debuggability: você pode abrir o arquivo com qualquer editor ou `jq`

### O Bronze Consumer

O `bronze-consumer` é um processo Python simples: consome `raw.claims.new` e escreve em lotes de até 200 mensagens ou a cada 30 segundos — o que vier primeiro.

```bash
kubectl logs -f -n processing deploy/bronze-consumer
```

```
2026-05-29T14:30:00Z  INFO  Subscribed to raw.claims.new | batch_size=200 flush_interval=30s
2026-05-29T14:30:30Z  INFO  Flushed 61 records → s3://fraud-datalake/bronze/claims/year=2026/month=05/day=29/143000_4820.jsonl
2026-05-29T14:31:00Z  INFO  Flushed 59 records → s3://fraud-datalake/bronze/claims/year=2026/month=05/day=29/143100_4940.jsonl
```

### Estrutura de arquivos no Object Storage

```
s3://fraud-datalake/
  bronze/
    claims/
      year=2026/
        month=05/
          day=29/
            143000_4820.jsonl    ← 61 records, flush das 14:30
            143100_4940.jsonl    ← 59 records, flush das 14:31
            ...
```

O nome do arquivo é `{HHMMSS}_{offset}.jsonl` — permite ordenar cronologicamente e rastrear qual offset do Redpanda foi processado.

### Verificar os arquivos no Object Storage

```bash
export AWS_ACCESS_KEY_ID=<access-key>
export AWS_SECRET_ACCESS_KEY=<secret-key>
export AWS_ENDPOINT_URL_S3=https://br-gru-1.linodeobjects.com

# Listar arquivos de hoje
aws s3 ls s3://fraud-datalake/bronze/claims/year=2026/month=05/day=29/

# Ver conteúdo de um arquivo (primeiras 3 linhas)
aws s3 cp s3://fraud-datalake/bronze/claims/year=2026/month=05/day=29/143000_4820.jsonl - \
  | head -3 | python3 -m json.tool
```

Cada linha do arquivo é uma claim completa:

```json
{"claim_id": "CLM-A3F2B891", "provider_npi": "NPI-001", "member_id": "MBR-4521", "amount": 362.50, ...}
{"claim_id": "CLM-D9E4A102", "provider_npi": "NPI-005", "member_id": "MBR-1003", "amount": 8750.00, ...}
```

---

## Camada 7 — Spark Silver ETL

### O que é e por que existe

A camada **Silver** é onde os dados são limpos, normalizados e **desidentificados**. O PHI (dados pessoais como `member_id` e `provider_npi`) é substituído por tokens SHA-256, garantindo que a camada Silver possa ser usada por analistas sem exposição a dados sensíveis.

Esta transformação roda diariamente às 02:00 via `ScheduledSparkApplication` gerenciado pelo Spark Operator.

### O que o Silver ETL faz

```
Bronze (NDJSON no S3)
    │
    ▼ Spark lê todos os arquivos da partição
    │
    ├── Filtra registros malformados (claim_id, member_id, provider_npi ou amount nulos)
    │
    ├── Normaliza event_time para timestamp
    │
    ├── Tokeniza PHI:
    │     member_id   → member_id_token   (SHA-256 dos 16 primeiros hex chars)
    │     provider_npi → provider_npi_token
    │     (colunas originais são removidas)
    │
    ├── Adiciona processing_date (data de execução do job)
    │
    └── Escreve em nessie.silver.claims (Iceberg, Parquet/Snappy)
```

**Por que SHA-256 truncado?**

```python
hashlib.sha256("MBR-4521".encode()).hexdigest()[:16]
# → "a3f2b8913c4d7e09"
```

O token é **determinístico** — o mesmo `member_id` sempre gera o mesmo token. Isso permite fazer joins entre tabelas ("quantas claims este beneficiário teve?") sem expor o identificador real. Os 16 primeiros caracteres hex são suficientes para unicidade em bases de milhões de registros.

### Executar o Silver ETL manualmente

```bash
# Trigger imediato (sem esperar o agendamento das 02:00)
kubectl create -f - <<'EOF'
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  generateName: silver-etl-manual-
  namespace: processing
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: 104.64.45.51:32500/spark-jobs:demo
  imagePullPolicy: IfNotPresent
  mainApplicationFile: local:///app/silver_etl.py
  sparkVersion: "3.5.8"
  restartPolicy:
    type: Never
  driver:
    cores: 1
    memory: "1024m"
    serviceAccount: spark
    envFrom:
      - secretRef:
          name: object-storage-credentials
  executor:
    cores: 2
    instances: 2
    memory: "2048m"
    envFrom:
      - secretRef:
          name: object-storage-credentials
EOF
```

### Monitorar a execução

```bash
# Acompanhar o status
kubectl get sparkapplication -n processing -w

# Acompanhar os logs do driver
kubectl logs -f -n processing \
  $(kubectl get pod -n processing -l spark-role=driver \
    --sort-by=.metadata.creationTimestamp -o name | tail -1)
```

Saída esperada do driver:

```
[silver-etl] Read 3660 records from s3a://fraud-datalake/bronze/claims/
[silver-etl] Dropped 2 malformed records
[silver-etl] Done. Silver table total rows: 3658
```

### Schema da tabela Silver (Iceberg)

```
nessie.silver.claims
├── claim_id           STRING
├── procedure_code     STRING
├── diagnosis_code     STRING
├── amount             DOUBLE
├── service_date       DATE
├── event_time         TIMESTAMP       ← normalizado de submitted_at
├── status             STRING
├── member_id_token    STRING          ← SHA-256[:16] de member_id
├── provider_npi_token STRING          ← SHA-256[:16] de provider_npi
├── processing_date    STRING          ← data de execução do job (partição)
└── ingested_at        TIMESTAMP
```

As colunas `member_id` e `provider_npi` (PHI) foram **removidas**. A tabela Silver é segura para analistas.

### Consultar a tabela Silver via Spark SQL

```bash
# Abrir o Spark SQL shell (requer um pod de driver rodando)
kubectl exec -n processing \
  $(kubectl get pod -n processing -l spark-role=driver -o name | tail -1) \
  -- /opt/spark/bin/spark-sql \
    --conf spark.sql.extensions="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions" \
    --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
    --conf spark.sql.catalog.nessie.uri=http://nessie.data.svc.cluster.local:19120/api/v1 \
    --conf spark.sql.catalog.nessie.ref=main \
    --conf spark.sql.catalog.nessie.warehouse=s3a://fraud-datalake/warehouse
```

```sql
-- Total de claims na camada Silver
SELECT COUNT(*) FROM nessie.silver.claims;

-- Claims por data de processamento
SELECT processing_date, COUNT(*) AS claims
FROM nessie.silver.claims
GROUP BY processing_date
ORDER BY processing_date DESC;

-- Snapshots (time travel) — cada run do ETL cria um snapshot imutável
SELECT snapshot_id, committed_at, operation
FROM nessie.silver.claims.snapshots
ORDER BY committed_at DESC;
```

```
snapshot_id         | committed_at              | operation
--------------------+---------------------------+-----------
3891047283012345    | 2026-05-29 02:00:45.123   | append
1234567890123456    | 2026-05-28 02:00:38.456   | append
```

> **Time travel:** o Iceberg mantém todos os snapshots históricos. Você pode consultar o estado da tabela em qualquer momento passado com `VERSION AS OF <snapshot_id>`.

---

## Camada 8 — Spark Gold Features

### O que é e por que existe

A camada **Gold** contém as **features prontas para ML** — agregados estatísticos calculados sobre janelas temporais. Em vez de o modelo ML receber uma claim bruta, ele recebe um vetor de features rico: "este prestador fez 47 claims no último mês com valor médio de R$920". Isso é o que torna o modelo poderoso.

Este job roda semanalmente (domingo às 04:00) porque os agregados de janela de 30/90 dias precisam de todos os dados da semana para serem precisos.

### Três tabelas Gold produzidas

#### `nessie.gold.provider_features` — perfil de cada prestador

Calculado com **janela deslizante de 30 dias** por `provider_npi_token`:

| Coluna | Descrição |
|---|---|
| `provider_npi_token` | Chave do prestador (SHA-256 token) |
| `claim_count_30d` | Quantidade de claims nos últimos 30 dias |
| `avg_amount_30d` | Valor médio das claims em 30 dias |
| `stddev_amount_30d` | Desvio padrão — alta variância é suspeita |
| `max_amount_30d` | Valor máximo cobrado em 30 dias |
| `distinct_procedures_30d` | Diversidade de procedimentos — prestadores legítimos têm mais diversidade |
| `computed_at` | Timestamp do cálculo |

#### `nessie.gold.member_features` — perfil de cada beneficiário

Calculado com **janela deslizante de 90 dias** por `member_id_token`:

| Coluna | Descrição |
|---|---|
| `member_id_token` | Chave do beneficiário |
| `claim_count_90d` | Quantidade de claims em 90 dias |
| `total_amount_90d` | Total gasto em 90 dias |
| `distinct_providers_90d` | Quantos prestadores diferentes foram usados |

#### `nessie.gold.claim_features` — vetor completo por claim

Cada claim recebe todas as features do prestador e do beneficiário, mais features calculadas da própria claim:

| Coluna | Descrição | Feature ML |
|---|---|---|
| `amount_ratio` | `amount / avg_amount_30d` | Desvio proporcional |
| `hour_of_day` | Hora da submissão | Padrão temporal |
| `is_weekend` | 1 se fim de semana | Padrão temporal |
| + todas as colunas de provider_features | | Contexto do prestador |
| + todas as colunas de member_features | | Contexto do beneficiário |

Esta é a tabela que alimentaria o re-treino do modelo ML.

### Executar o Gold Features manualmente

```bash
kubectl create -f - <<'EOF'
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  generateName: gold-features-manual-
  namespace: processing
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: 104.64.45.51:32500/spark-jobs:demo
  imagePullPolicy: IfNotPresent
  mainApplicationFile: local:///app/gold_features.py
  sparkVersion: "3.5.8"
  restartPolicy:
    type: Never
  driver:
    cores: 1
    memory: "1024m"
    serviceAccount: spark
    envFrom:
      - secretRef:
          name: object-storage-credentials
  executor:
    cores: 2
    instances: 2
    memory: "2048m"
    envFrom:
      - secretRef:
          name: object-storage-credentials
EOF
```

Saída esperada:

```
[gold-features] Silver rows read: 3658
[gold-features] provider_features: 5 rows    ← um por prestador (NPI-001..005)
[gold-features] member_features:   5 rows    ← um por beneficiário (MBR-1001..1005)
[gold-features] claim_features:    3658 rows ← um vetor por claim
```

### Consultar as tabelas Gold

```sql
-- Perfil dos prestadores — NPI-005 deve ter valores extremos
SELECT provider_npi_token,
       claim_count_30d,
       avg_amount_30d::NUMERIC(10,2),
       max_amount_30d,
       distinct_procedures_30d
FROM nessie.gold.provider_features
ORDER BY avg_amount_30d DESC;

-- Claims com maior desvio de valor (amount_ratio alto = suspeito)
SELECT claim_id,
       amount,
       avg_amount_30d::NUMERIC(10,2),
       amount_ratio::NUMERIC(6,2),
       claim_count_30d,
       hour_of_day,
       is_weekend
FROM nessie.gold.claim_features
WHERE amount_ratio > 3.0
ORDER BY amount_ratio DESC
LIMIT 10;
```

```
 claim_id      | amount    | avg_amount | amount_ratio | claim_count | hour | weekend
---------------+-----------+------------+--------------+-------------+------+---------
 CLM-88EA5D12  | 18750.00  |  920.40    |    20.37     |     89      |  3   |    1
 CLM-D9E4A102  |  8750.00  |  920.40    |     9.51     |     47      | 14   |    0
 CLM-F1B3C007  |  3140.00  | 1203.45    |     2.61     |     34      | 16   |    0
```

### Verificar o Nessie Catalog

O **Nessie** funciona como um "Git para dados" — mantém versões de todas as tabelas Iceberg com branches, commits e merges.

```bash
# Port-forward para acessar a API localmente
kubectl port-forward -n data svc/nessie 19120:19120 &

# Listar todas as tabelas no branch main
curl -s "http://localhost:19120/api/v1/trees/tree/main/entries" | python3 -m json.tool
```

```json
{
  "entries": [
    { "name": { "elements": ["silver", "claims"] },          "type": "ICEBERG_TABLE" },
    { "name": { "elements": ["gold", "provider_features"] }, "type": "ICEBERG_TABLE" },
    { "name": { "elements": ["gold", "member_features"] },   "type": "ICEBERG_TABLE" },
    { "name": { "elements": ["gold", "claim_features"] },    "type": "ICEBERG_TABLE" }
  ]
}
```

---

## Camada 9 — Observabilidade

### Prometheus — coleta de métricas

O Prometheus faz scrape automático das métricas expostas pelos pods a cada 5 segundos. Acesse **http://NODE_IP:30909** (LKE) ou **http://localhost:9090** (Docker Compose).

**Métricas do Fraud Scorer:**

| Métrica | Tipo | Descrição |
|---|---|---|
| `fraud_scorer_inferences_total{risk_level="..."}` | Counter | Inferências por nível de risco |
| `fraud_scorer_latency_seconds` | Histogram | Latência do modelo (p50/p95/p99) |

**Métricas do Redpanda (consumer lag):**

| Métrica | Tipo | Descrição |
|---|---|---|
| `vectorized_kafka_consumer_group_committed_offset` | Gauge | Offset commitado por grupo |
| `vectorized_kafka_replicas_committed_offset` | Gauge | Offset atual da partição |

A diferença entre os dois dá o **consumer lag** — se crescer, o Flink está processando mais devagar que o Redpanda está recebendo.

```bash
# Ver consumer lag do grupo Flink via CLI
kubectl exec -n streaming redpanda-0 -- \
  rpk group describe flink-fraud-stream-v2
```

**Queries úteis no Prometheus:**

```promql
# Latência p99 do modelo de ML
histogram_quantile(0.99, sum(rate(fraud_scorer_latency_seconds_bucket[2m])) by (le)) * 1000

# Total de inferências por nível de risco
sum(fraud_scorer_inferences_total) by (risk_level)
```

> **Nota:** O Flink expõe métricas JVM nativas (heap, GC, checkpoint duration) via o Flink Metrics Reporter. Para visualizá-las no Grafana, é necessário configurar o `PrometheusReporter` no `flinkConfiguration` do `FlinkDeployment` — etapa prevista para a fase de produção (ver `docs/architecture.md`, seção 5).

### Grafana — dashboard em tempo real

Acesse **http://104.64.45.51:30300** com admin/admin. O dashboard **Fraud Detection Platform — LKE** carrega automaticamente.

```
┌─────────────────┬─────────────────┬─────────────────┬──────────────────┐
│ Total Claims    │ Fraud Alerts    │ ML Latency p99  │ Active Providers │
│ [contador ↑]    │ [vermelho >10]  │ [verde <50ms]   │ [5 prestadores]  │
├─────────────────┴─────────────────┴─────────────────┴──────────────────┤
│ Claims Throughput (série temporal)   │ Fraud Alerts by Risk Level       │
│ verde: legítimas ~1.8/s              │ CRITICAL (vermelho) — upcoding   │
│ vermelho: fraudes ~0.16/s            │ HIGH (laranja) — desvio valor    │
│                                      │ MEDIUM (amarelo) — anomalias     │
├──────────────────────────────────────┴──────────────────────────────────┤
│ Scoring Latency p50/p95/p99          │ ML Inferences por Risk Level     │
│ p50 ~30ms  p99 ~120ms                │ distribuição do modelo           │
└──────────────────────────────────────┴──────────────────────────────────┘
```

---

## Rastreamento Completo — Uma Claim do Início ao Fim

Este é o percurso exato de uma claim fraudulenta detectada:

```
T+0ms    Data Generator insere no PostgreSQL:
         provider=NPI-005, procedure=27447 (artroplastia de joelho)
         diagnosis=I10 (hipertensão arterial), amount=R$8.750

         claims_db=# SELECT claim_id, provider_npi, amount FROM claims ORDER BY id DESC LIMIT 1;
          claim_id     | provider_npi | amount
         --------------+--------------+---------
          CLM-D9E4A102 | NPI-005      | 8750.00

T+2ms    PostgreSQL grava no WAL (replication slot: debezium_claims_slot)

T+5ms    Debezium lê o WAL, serializa para JSON, publica em raw.claims.new
         Chave: "NPI-005" (routing para partição 1 dos 4)

T+8ms    Flink (ProviderEnrichFunction) consome da partição 1
         keyBy(provider_npi="NPI-005") → TaskManager 1

T+9ms    Enriquecimento (RocksDB keyed state):
         state["NPI-005"] → {sum: 432180.50, count: 47}
         avg_amount_30d = 432180.50 / 47 = R$9.195

         keyBy(member_id="MBR-1003") → MemberEnrichFunction
         state["MBR-1003"] → count=1 (beneficiário com poucas claims)

T+11ms   Score por Regras (ScoreFunction):
         R1: 8750 > 9195 * 3.0? → 8750 > 27.585? Não  (prestador já tem média alta)
         R3: procedure=27447 (ortopedia) + diagnosis=I10 (cardiovascular) → MISMATCH
             rule_score = 0.93, triggered_rules = ["PROCEDURE_DIAGNOSIS_MISMATCH"]

T+38ms   Score ML (POST /score):
         features = [8750, 47, 9195, 0.95, 14, 0, 0.90, 1]
         fraud_probability = 0.891

T+39ms   Combinação:
         final = max(0.93, 0.891*0.6 + 0.93*0.4) = max(0.93, 0.906) = 0.93
         risk_level = CRITICAL (≥ 0.90)

T+40ms   Publica em scored.claims (sempre)
         Publica em alerts.fraud (0.93 ≥ threshold 0.65)

         Log: FRAUD [CRITICAL] CLM-D9E4A102  provider=NPI-005
              amount=R$ 8,750.00  score=0.9312
              rules=['PROCEDURE_DIAGNOSIS_MISMATCH']

T+45s    Prometheus scrape: fraud_scorer_inferences_total{risk_level="CRITICAL"} += 1

T+50s    Grafana atualiza dashboard (intervalo de 5s)
         → spike vermelho visível no painel "Fraud Alerts by Risk Level"

T+30s    Bronze Consumer flushes lote:
         → s3://fraud-datalake/bronze/claims/year=2026/month=05/day=29/143000_4820.jsonl
         (CLM-D9E4A102 está na linha 37 do arquivo, com PHI completo)

02:00    [próximo dia] Spark Silver ETL:
         - Lê bronze/claims/year=2026/month=05/day=29/*.jsonl
         - member_id "MBR-1003" → token "c2f8a391d4e7b0c1"
         - provider_npi "NPI-005" → token "7a3d9f2c1b4e8a0d"
         - Escreve em nessie.silver.claims (Parquet, partição 2026-05-29)
         - PHI removido; claim preservada com tokens

Dom 04:00  [próximo domingo] Spark Gold Features:
           - Lê nessie.silver.claims
           - Calcula janela 30d para token "7a3d9f2c1b4e8a0d":
             claim_count_30d=89, avg_amount_30d=9421, stddev_amount_30d=4230
           - Escreve vetor em nessie.gold.claim_features
           - Este vetor é usado no próximo ciclo de re-treino do modelo ML
```

---

## Stress Test — Aumentar Taxa de Fraude

Para demonstrar resiliência, aumente a taxa de fraude e o volume:

```bash
# Aumentar: 30% de fraude, 5 claims por segundo
kubectl set env -n processing deploy/data-generator \
  FRAUD_RATE=0.30 \
  CLAIMS_PER_SECOND=5

# Observe o dashboard do Grafana — os alertas CRITICAL disparam
# A latência do ML deve permanecer estável (< 50ms p99)

# Restaurar configuração original
kubectl set env -n processing deploy/data-generator \
  FRAUD_RATE=0.08 \
  CLAIMS_PER_SECOND=2
```

---

## Cenários de Fraude Injetados pelo Gerador

| Cenário | Prestador | Regra Disparada | Score esperado |
|---|---|---|---|
| **Upcoding** — cirurgia ortopédica cobrada por cardiologista | NPI-001 | `PROCEDURE_DIAGNOSIS_MISMATCH` | ≥ 0.90 |
| **Desvio de valor** — claim 5–10× acima da média do prestador | NPI-001 | `AMOUNT_3X_PROVIDER_AVG` | ≥ 0.78 |
| **Phantom billing** — volume muito alto de claims | NPI-005 | `HIGH_VOLUME_PROVIDER` | ≥ 0.62 |
| **Clínica suspeita** — todo o perfil de NPI-005 é fraudulento | NPI-005 | Múltiplas | ≥ 0.70 |

---

## Verificação de Saúde de Todos os Componentes

```bash
# Status de todos os pods
kubectl get pods -A --sort-by=.metadata.namespace

# Flink job — status e últimas detecções
kubectl get flinkdeployment -n processing
kubectl logs -n processing -l component=taskmanager --tail=20 | grep -E "FRAUD|ERROR"

# Bronze Consumer — último flush
kubectl logs -n processing deploy/bronze-consumer --tail=5

# Spark jobs agendados
kubectl get scheduledsparkapplication -n processing

# Redpanda — broker saudável
kubectl exec -n streaming redpanda-0 -- rpk cluster health

# Consumer lag do Flink
kubectl exec -n streaming redpanda-0 -- rpk group describe flink-fraud-stream-v2

# Debezium — conector ativo
kubectl exec -n streaming deploy/debezium -- \
  curl -s http://localhost:8083/connectors/claims-postgres-connector/status | python3 -m json.tool
```

**Na demo local (Docker Compose):**
```bash
make status          # docker compose ps
make logs-flink      # logs do flink-fraud-stream
make logs-bronze     # logs do bronze-consumer
```
