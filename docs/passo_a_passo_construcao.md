# Como Construímos a Plataforma de Detecção de Fraudes — Passo a Passo

> Documentação educativa: cada seção explica **o que foi feito**, **por que** e **como** funciona.

---

## Índice

1. [O Problema](#1-o-problema)
2. [Visão Geral da Solução](#2-visão-geral-da-solução)
3. [Passo 1 — Modelagem do Banco de Dados Fonte](#passo-1--modelagem-do-banco-de-dados-fonte)
4. [Passo 2 — Ambiente de Demo Local (Docker Compose)](#passo-2--ambiente-de-demo-local-docker-compose)
5. [Passo 3 — CDC com Debezium](#passo-3--cdc-com-debezium)
6. [Passo 4 — Backbone de Streaming com Redpanda](#passo-4--backbone-de-streaming-com-redpanda)
7. [Passo 5 — Modelo de ML e Serviço de Scoring](#passo-5--modelo-de-ml-e-serviço-de-scoring)
8. [Passo 6 — Processador de Stream](#passo-6--processador-de-stream)
9. [Passo 7 — Infraestrutura Kubernetes no Linode (LKE)](#passo-7--infraestrutura-kubernetes-no-linode-lke)
10. [Passo 8 — Data Lake Medallion (Bronze → Silver → Gold)](#passo-8--data-lake-medallion-bronze--silver--gold)
11. [Passo 9 — Flink em Produção](#passo-9--flink-em-produção)
12. [Passo 10 — Observabilidade](#passo-10--observabilidade)
13. [Problemas Reais que Encontramos e Como Resolvemos](#problemas-reais-que-encontramos-e-como-resolvemos)
14. [Arquitetura Final em Camadas](#arquitetura-final-em-camadas)

---

## 1. O Problema

Uma operadora de saúde processa centenas de milhares de guias (claims) por dia. Uma fração delas é fraudulenta: cobrança por procedimentos não realizados, upcoding (cobrar procedimento mais caro do que o executado), billing phantom (provedor fantasma), e anéis de fraude entre prestadores e beneficiários.

**Requisitos principais:**
- Detectar fraude em tempo real (< 1 segundo após a submissão da guia)
- Analisar histórico para identificar padrões ao longo de 30–90 dias
- Não ter dependência de cloud proprietária (AWS/Azure/GCP) por questões de custo e soberania de dados
- Cumprir LGPD: PHI (dados de saúde) deve ser tokenizado e auditável

---

## 2. Visão Geral da Solução

```
Claims DB (PostgreSQL)
        │
        │  CDC (Debezium)  ← captura todo INSERT/UPDATE na tabela de guias
        ▼
   Redpanda (Kafka-compatible)
        │
        ├──► Flink (stream real-time)   → pontuação instantânea → alertas
        │
        └──► Bronze Consumer → Object Storage (Data Lake)
                                    │
                                    ├──► Spark Silver ETL    (limpeza, tokenização PHI)
                                    └──► Spark Gold Features (features para ML)
                                                 │
                                            Modelo XGBoost (fraud-scorer API)
```

Toda a infraestrutura roda no **Akamai Cloud / Linode** — nenhum vendor lock-in.

---

## Passo 1 — Modelagem do Banco de Dados Fonte

**Arquivo:** [demo/init_db/01_schema.sql](../demo/init_db/01_schema.sql)

### O que foi feito

Criamos o schema PostgreSQL que simula o sistema de guias de uma operadora. São três tabelas:

```sql
providers   -- prestadores de saúde (médicos, hospitais) identificados por NPI
members     -- beneficiários do plano
claims      -- guias submetidas: quem atendeu, quem foi atendido, procedimento, valor
```

### Por que `wal_level=logical`?

O CDC (Change Data Capture) funciona lendo o **WAL (Write-Ahead Log)** do PostgreSQL — o log de transações que o banco já grava internamente para garantir durabilidade. O nível `logical` aumenta a granularidade desse log para incluir os valores antes/depois de cada linha modificada.

```sql
-- Configurado no docker-compose / Helm:
wal_level=logical
max_replication_slots=5
max_wal_senders=5
```

### Por que `REPLICA IDENTITY FULL`?

Por padrão, o WAL registra apenas a chave primária nos eventos de UPDATE/DELETE. Com `REPLICA IDENTITY FULL`, o Debezium recebe o valor completo da linha **antes** e **depois** da mudança — essencial para detectar alterações de status em guias.

```sql
ALTER TABLE claims REPLICA IDENTITY FULL;
```

### O que aprendemos

Sem essa configuração, o Debezium consegue capturar INSERT, mas perde o contexto nos UPDATE (ex: uma guia aprovada que é estornada fraudulentamente).

---

## Passo 2 — Ambiente de Demo Local (Docker Compose)

**Arquivo:** [demo/docker-compose.yml](../demo/docker-compose.yml)

### O que foi feito

Antes de ir para Kubernetes, construímos um ambiente local completo com Docker Compose. Cada serviço no compose **mapeia exatamente** para um componente de produção:

| Serviço no Compose | Equivalente no LKE (atual) |
|---|---|
| `redpanda` (1 nó) | Redpanda Enterprise cluster (3 nós em VMs dedicadas) |
| `postgres` (1 instância) | PostgreSQL gerenciado no Linode |
| `debezium` standalone | Debezium no LKE (Kafka Connect cluster) |
| `fraud-scorer` (FastAPI + GBM simples) | BentoML servindo modelo XGBoost do MLflow |
| `stream-processor` (Python) | **Apache Flink** — `FlinkDeployment` via Flink Operator (`14-flink.yaml`) |
| `bronze_consumer` (Python) | Consumidor de bronze no LKE (`11-bronze-consumer.yaml`) |
| ETL não existia no compose | **Apache Spark** — `ScheduledSparkApplication` via Spark Operator (`13-spark-jobs.yaml`) |
| `data-generator` | Sistema real de guias da operadora |
| `prometheus + grafana` | Stack de observabilidade completa no LKE |

> **Nota importante:** O `stream-processor` Python foi mantido no LKE com `replicas: 0` (desativado). O caminho ativo de processamento de stream é o Flink job em `14-flink.yaml`. Os Spark jobs (Silver ETL e Gold Features) não existiam no compose — foram adicionados diretamente no LKE.

### Por que começar com Docker Compose?

Iterar localmente é **10x mais rápido** do que deployar no Kubernetes. O compose permitiu validar toda a lógica de negócio (regras de fraude, scoring, alertas) antes de lidar com a complexidade de K8s, Helm charts e infra cloud.

### Evolução da Demo: da versão Python para Flink + Spark

A demo passou por duas versões:

**Versão 1 (inicial):** usava um `stream-processor` Python que imitava o Flink e não tinha Data Lake local.

**Versão 2 (atual):** usa os componentes reais — mesma imagem Docker que vai para o LKE:

| Componente | Antes | Agora |
|---|---|---|
| Stream processing | `stream-processor` Python (imitação) | `flink-fraud-stream` — PyFlink real (`fraud_stream_job.py`) |
| Object storage | não existia na demo | **Linode Object Storage** — mesmo bucket do LKE |
| Iceberg catalog | não existia na demo | **Nessie** — in-memory mode |
| Bronze persistence | não existia na demo | **bronze-consumer** → Linode Object Storage |
| Batch ETL | não existia na demo | **Spark** via `make etl-silver` e `make etl-gold` |

### Como os healthchecks foram usados

Cada serviço só inicia depois que suas dependências estão prontas, usando `depends_on` com `condition: service_healthy`:

```yaml
flink-fraud-stream:
  depends_on:
    redpanda-init:
      condition: service_completed_successfully  # tópicos criados
    fraud-scorer:
      condition: service_healthy                 # modelo carregado

bronze-consumer:
  depends_on:
    redpanda-init:
      condition: service_completed_successfully  # tópicos criados
```

O bronze-consumer usa diretamente o **Linode Object Storage** (mesmo bucket do LKE), autenticando via variáveis de ambiente exportadas do `terraform output`. Sem os tópicos criados, a subscrição Kafka falha silenciosamente.

---

## Passo 3 — CDC com Debezium

**Arquivo:** [demo/debezium/connector.json](../demo/debezium/connector.json)

### O que é CDC?

Change Data Capture é uma técnica para capturar **cada mudança** no banco de dados em tempo real, sem polling. O Debezium lê o WAL do PostgreSQL e publica cada INSERT/UPDATE/DELETE como um evento no Redpanda.

### Por que não usar triggers SQL ou polling?

| Abordagem | Problema |
|---|---|
| Polling (`SELECT WHERE updated_at > last_check`) | Latência mínima de segundos; carrega o banco; perde DELETEs |
| Triggers SQL | Acoplamento forte; overhead transacional; difícil de manter |
| CDC (Debezium + WAL) | Zero overhead no banco; sub-segundo; captura tudo incluindo DELETEs |

### Problema real que encontramos

O Debezium precisa de permissão `REPLICATION` no PostgreSQL e de uma entrada específica no `pg_hba.conf`. Sem isso, a conexão de replicação é recusada mesmo com usuário e senha corretos.

**Solução (commit `ea18c94`):**
```sql
-- Conceder a permissão de replicação ao usuário
ALTER USER fraud_user REPLICATION;

-- Criar a publication para que o Debezium saiba quais tabelas monitorar
CREATE PUBLICATION fraud_pub FOR TABLE claims, providers, members;
```

### Tópico gerado

O Debezium publica no tópico `dbz.public.claims` com o payload:
```json
{
  "before": null,          // null em INSERTs; valor anterior em UPDATEs
  "after": {
    "claim_id": "CLM-001",
    "provider_npi": "1234567890",
    "amount": 15000.00,
    "status": "PENDING"
  },
  "op": "c"                // c=create, u=update, d=delete
}
```

---

## Passo 4 — Backbone de Streaming com Redpanda

**Arquivo:** [infra/helm/redpanda-values.yaml](../infra/helm/redpanda-values.yaml)

### Por que Redpanda e não Kafka?

Kafka precisa do ZooKeeper (ou KRaft) + JVM, o que significa 3+ processos por broker e overhead de memória significativo. O Redpanda é um único binário em C++:

| | Redpanda | Apache Kafka |
|---|---|---|
| Dependências | Binário único, sem JVM | ZooKeeper + JVM |
| Latência p99 | ~2–10ms | ~5–20ms |
| Schema Registry | Embutido | Confluent separado |
| Compatibilidade | API Kafka 100% compatível | — |

**Compatibilidade com Kafka** é crucial: todo o ecossistema (Debezium, Flink, Spark) usa o protocolo Kafka nativamente e funciona com Redpanda sem nenhuma mudança de código.

### Design de tópicos

Os tópicos foram nomeados com prefixo semântico para indicar o estágio de processamento:

```
raw.claims.new     → guias recém-submetidas (fonte: data-generator)
raw.claims.status  → mudanças de status (fonte: Debezium CDC)
scored.claims      → guias com score de fraude calculado
alerts.fraud       → apenas as guias com score ≥ threshold
```

### Particionamento por `provider_npi`

Guias do mesmo prestador são sempre roteadas para a mesma partição. Isso permite que o Flink mantenha estado acumulado por prestador **sem precisar de comunicação entre partições** — essencial para calcular janelas de 30 dias de forma eficiente.

---

## Passo 5 — Modelo de ML e Serviço de Scoring

**Arquivo:** [demo/fraud_scorer/](../demo/fraud_scorer/)

### O que foi feito

Um serviço FastAPI que, ao inicializar, **treina um modelo GBM** (Gradient Boosting Machine) em dados sintéticos e expõe um endpoint `/score` para scoring em tempo real.

### Features do modelo

```python
features = [
    "amount",                   # valor da guia
    "claim_count_30d",          # quantas guias o prestador submeteu nos últimos 30 dias
    "avg_amount_30d",           # valor médio das guias do prestador nos últimos 30 dias
    "hour_of_day",              # hora da submissão (fraudes têm padrão noturno)
    "is_weekend",               # final de semana (menor supervisão)
    "procedure_risk",           # risco intrínseco do procedimento (baseado em tabela)
    "member_claim_count_90d",   # frequência de uso do beneficiário nos últimos 90 dias
]
```

### Por que combinar regras + ML?

Nenhum dos dois isoladamente é suficiente:

- **Regras sozinhas:** não detectam padrões novos; fácil de contornar ajustando valor levemente abaixo do threshold
- **ML sozinho:** caixa preta difícil de auditar; pode ter alta taxa de falsos positivos em procedimentos raros legítimos

A solução usa os dois combinados:
```python
final_score = max(rule_score, ml_score * 0.6 + rule_score * 0.4)
```

Se qualquer um dos dois sinalizar fraude com alta confiança, o score final reflete isso.

### Regras determinísticas implementadas

```python
AMOUNT_3X_PROVIDER_AVG       # valor 3x acima da média do prestador nos 30 dias
EXTREME_AMOUNT               # valor absoluto > R$50.000
PROCEDURE_DIAGNOSIS_MISMATCH # procedimento ortopédico com diagnóstico cardíaco
HIGH_VOLUME_PROVIDER         # mais de 200 guias em 30 dias
```

---

## Passo 6 — Processador de Stream

**Arquivos:** [demo/stream_processor/processor.py](../demo/stream_processor/processor.py) (demo) e [infra/flink-jobs/fraud_stream_job.py](../infra/flink-jobs/fraud_stream_job.py) (produção)

### O que foi feito em duas fases

**Fase 1 (demo local — Docker Compose):** Um único processo Python que faz o papel dos 4 jobs Flink. Consome `raw.claims.new`, consulta o Redis para features acumuladas, chama o fraud-scorer, e publica em `scored.claims` e `alerts.fraud`. Útil para validar a lógica de negócio rapidamente.

**Fase 2 (LKE — substituição completa):** O Python stream-processor foi **desativado** (`replicas: 0` em `05-stream-processor.yaml`) e substituído por um `FlinkDeployment` real gerenciado pelo Flink Kubernetes Operator. O estado das janelas de tempo saiu do Redis e passou para o RocksDB nativo do Flink.

O manifesto `05-stream-processor.yaml` manteve o Deployment com `replicas: 0` como **fallback documentado** — se o Flink job precisar ser desligado para troubleshooting, basta escalar de volta para 1.

### Arquitetura do Flink Job

```
raw.claims.new
    │
    ▼
keyBy(provider_npi) → ProviderEnrichFunction
    │  estado: {sum_amount, count} com TTL de 30 dias
    │  calcula: claim_count_30d, avg_amount_30d
    ▼
keyBy(member_id) → MemberEnrichFunction
    │  estado: count com TTL de 90 dias
    │  calcula: member_claim_count_90d
    ▼
ScoreFunction (regras + chamada HTTP ao fraud-scorer)
    │
    ├──► scored.claims     (todas as guias com score)
    └──► alerts.fraud      (apenas score ≥ 0.65)
```

### Por que RocksDB e não Redis para o estado?

O Redis funciona bem para o demo, mas em produção temos um problema de escala: o estado de cada partição precisa estar no mesmo processo que processa aquela partição. Com Redis centralizado, cada chamada do Flink vira uma latência de rede. Com RocksDB embutido no TaskManager do Flink:

- Estado fica co-localizado com o processamento: **zero latência de rede**
- Checkpoints incrementais vão automaticamente para o Object Storage
- Failover: o Flink restaura o estado do último checkpoint automaticamente

### TTL (Time-to-Live) no estado

```python
def _ttl(days: int) -> StateTtlConfig:
    return (
        StateTtlConfig.new_builder(Time.days(days))
        .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .never_return_expired()
        .build()
    )
```

Isso garante que as janelas de 30/90 dias se comportem corretamente: após 30 dias sem novas guias de um prestador, o estado é automaticamente expirado e o próximo evento começa um contador zerado.

---

## Passo 7 — Infraestrutura Kubernetes no Linode (LKE)

**Arquivos:** [infra/terraform/](../infra/terraform/) e [infra/deploy.sh](../infra/deploy.sh)

### O que foi feito

Automatizamos o provisionamento completo do cluster Kubernetes no Linode com Terraform + um script de deploy sequencial de 15 passos.

### Terraform: provisionando o cluster

```hcl
resource "linode_lke_cluster" "fraud_demo" {
  label       = var.cluster_label
  k8s_version = var.k8s_version
  region      = var.region

  pool {
    type  = var.node_type    # ex: g6-standard-4 (4 vCPU, 8GB RAM)
    count = var.node_count   # 3 nós por padrão

    autoscaler {
      min = var.node_count
      max = var.node_count + 2   # expande durante model training
    }
  }
}
```

O Terraform também provisiona o **Object Storage bucket** (equivalente ao S3) que serve como Data Lake.

### Namespaces Kubernetes

Separamos os workloads por função para isolamento e controle de recursos:

```
streaming    → Redpanda
data         → PostgreSQL, Redis, Nessie
processing   → Flink, Spark, stream-processor, data-generator
ml           → fraud-scorer, MLflow
monitoring   → Prometheus, Grafana
tools        → registry, DaemonSets auxiliares
```

### Registry in-cluster

O LKE não tem acesso ao Docker Hub por padrão no ambiente de demo. A solução foi rodar um registry Docker **dentro do próprio cluster** (NodePort 32500) e configurar o containerd de todos os nós para confiar nele como insecure registry via DaemonSet:

```
deploy.sh → build imagens localmente
         → push para NODE_IP:32500 (registry in-cluster)
         → manifests K8s referenciam NODE_IP:32500/imagem:tag
         → pods puxam do registry interno
```

### O script deploy.sh em 15 passos

```
Passo 1:  Terraform apply → cria cluster LKE + Object Storage
Passo 2:  Captura IP do nó para definir endereço do registry
Passo 3:  Cria namespaces
Passo 4:  Deploy do registry in-cluster
Passo 5:  Configura containerd em todos os nós (DaemonSet)
Passo 6:  Build + push das imagens custom
Passo 7:  Cria secrets com credenciais do Object Storage
Passo 8:  Helm install: Redpanda, PostgreSQL, Redis, Spark Operator, Flink Operator
Passo 9:  Cria tópicos Redpanda
Passo 10: Deploy dos serviços custom (Debezium, fraud-scorer, stream-processor, data-generator, MLflow)
Passo 11: Deploy do Data Lake (Nessie, bronze-consumer)
Passo 12: Aplica os Spark Scheduled Jobs (silver-etl e gold-features)
Passo 13: Deploy do Flink job (substitui stream-processor)
Passo 14: Deploy de Prometheus + Grafana
Passo 15: Aguarda pods ficarem prontos e imprime URLs de acesso
```

---

## Passo 8 — Data Lake Medallion (Bronze → Silver → Gold)

**Arquivos:** [infra/spark-jobs/silver_etl.py](../infra/spark-jobs/silver_etl.py) e [infra/spark-jobs/gold_features.py](../infra/spark-jobs/gold_features.py)

### Arquitetura Medallion

O padrão Medallion organiza os dados em três zonas com qualidade crescente:

```
BRONZE  → dados brutos, exatamente como chegaram, imutáveis
SILVER  → dados limpos, PHI tokenizado, sem duplicatas
GOLD    → features agregadas, prontas para ML, sem PHI
```

### Por que Apache Iceberg?

O Object Storage (S3/Linode) é essencialmente um sistema de arquivos sem transações. O Iceberg adiciona:

- **ACID transactions**: dois jobs Spark não corrompem a mesma tabela ao escrever simultaneamente
- **Time travel**: `SELECT * FROM claims VERSION AS OF 'yesterday'` — essencial para auditoria
- **Schema evolution**: adicionar uma coluna não exige reescrever todos os dados históricos
- **Partition pruning**: consultas em janelas de tempo leem apenas os arquivos relevantes

### Por que Project Nessie (catálogo)?

O Nessie funciona como um "Git para dados": cada tabela Iceberg é versionada, e você pode criar um branch para experimentar mudanças no schema ou nos jobs Spark sem afetar a produção.

```
main branch       → produção (silver.claims, gold.provider_features)
experiment/v2     → testando novo schema silver antes de promover
```

### Silver ETL (executa diariamente às 02:00)

```python
# 1. Lê dados brutos do bronze (JSONL no Object Storage)
df = spark.read.json("s3a://fraud-datalake/bronze/claims/")

# 2. Tokeniza PHI — transforma member_id e provider_npi em hashes SHA-256
#    O dado original existe apenas no bronze (criptografado)
df_silver = df \
    .withColumn("member_id_token",    tokenize_udf(F.col("member_id"))) \
    .withColumn("provider_npi_token", tokenize_udf(F.col("provider_npi"))) \
    .drop("member_id", "provider_npi")   # remove PHI da silver

# 3. Remove registros malformados
    .filter(F.col("claim_id").isNotNull() & ...)

# 4. Escreve como tabela Iceberg particionada por data
df_silver.writeTo("nessie.silver.claims") \
    .partitionedBy(F.col("processing_date")) \
    .createOrReplace()
```

### Gold Features (executa semanalmente aos domingos às 04:00)

Gera três tabelas de features para o modelo de ML:

**`gold.provider_features`** — perfil do prestador nos últimos 30 dias:
- `claim_count_30d`: volume de guias
- `avg_amount_30d`: valor médio
- `stddev_amount_30d`: desvio padrão (prestadores fraudulentos têm desvio alto — mistura procedimentos baratos e caros)
- `distinct_procedures_30d`: diversidade de procedimentos

**`gold.member_features`** — perfil do beneficiário nos últimos 90 dias:
- `claim_count_90d`: frequência de uso
- `total_amount_90d`: gasto total
- `distinct_providers_90d`: número de prestadores diferentes

**`gold.claim_features`** — vetor completo por guia (join das três tabelas):
- Features da guia + features do prestador + features do beneficiário
- `amount_ratio`: quanto essa guia representa vs. a média do prestador (`amount / avg_amount_30d`)

### Problema real no Gold ETL (conhecido, ainda não resolvido)

O job `gold_features.py` falha com `executor exit 134` (OOM) no dataset de demo porque um prestador fraudulento ("phantom billing") tem volume anormalmente alto de guias. A janela `rangeBetween(-30_days, 0)` carrega todas as linhas de uma única partição em memória do executor — não é falta de memória do cluster, é uma **chave distorcida (skewed key)**.

A correção é aplicar *salting*: distribuir artificialmente as linhas de prestadores de alto volume entre múltiplas partições antes do cálculo da janela. Ainda pendente.

---

## Passo 9 — Flink e Spark como Cidadãos de Primeira Classe no LKE

**Arquivos:**
- [infra/flink-jobs/fraud_stream_job.py](../infra/flink-jobs/fraud_stream_job.py) — job Python do Flink
- [infra/k8s/14-flink.yaml](../infra/k8s/14-flink.yaml) — FlinkDeployment (CRD)
- [infra/helm/flink-operator-values.yaml](../infra/helm/flink-operator-values.yaml) — Helm do Flink Operator
- [infra/k8s/13-spark-jobs.yaml](../infra/k8s/13-spark-jobs.yaml) — ScheduledSparkApplication (CRD)

### O que foi feito

O ambiente no LKE evoluiu para usar Flink e Spark como recursos Kubernetes nativos, gerenciados por seus respectivos Operators. Nenhum processo Python ficou no caminho crítico de processamento.

| Componente | Antes (Fase 1) | Depois (Fase 2 — LKE atual) |
|---|---|---|
| Stream processing | Python `stream-processor` Deployment | `FlinkDeployment` CRD (`fraud-stream-job`) |
| Bronze → Silver ETL | (não existia no K8s) | `ScheduledSparkApplication` `silver-etl-scheduled` |
| Silver → Gold ETL | (não existia no K8s) | `ScheduledSparkApplication` `gold-features-scheduled` |
| Estado das janelas | Redis (externo) | RocksDB nativo no TaskManager do Flink |

O Python `stream-processor` foi definido com `replicas: 0` — desativado, preservado como fallback.

### Como o Flink Operator gerencia o job

O `FlinkDeployment` é um CRD (Custom Resource Definition) — o Kubernetes não sabe o que é Flink, mas o Flink Operator sim. Quando aplicamos o YAML, o Operator:

1. Sobe o **JobManager** (coordenador) e os **TaskManagers** (workers) como pods no namespace `processing`
2. Submete o job Python via `PythonDriver` usando o JAR `flink-python_2.12-1.18.1.jar`
3. Monitora a saúde do job e reinicia automaticamente se falhar
4. Gerencia checkpoints conforme configurado

```yaml
job:
  jarURI: local:///opt/flink/opt/flink-python_2.12-1.18.1.jar
  entryClass: "org.apache.flink.client.python.PythonDriver"
  args: ["-pyclientexec", "/usr/bin/python3", "-py", "/opt/flink/usrlib/fraud_stream_job.py"]
  parallelism: 2       # 2 partições processadas em paralelo
  upgradeMode: stateless
```

### Como o Spark Operator gerencia os ETL jobs

Os jobs Spark no LKE são `ScheduledSparkApplication` — um CRD do Spark Operator que combina um `SparkApplication` com um cron schedule:

```yaml
# silver-etl: todo dia às 02:00 UTC
spec:
  schedule: "0 2 * * *"
  concurrencyPolicy: Forbid       # não roda overlap se o anterior ainda estiver rodando
  template:
    executor:
      instances: 1
      memory: "1024m"

# gold-features: todo domingo às 04:00 UTC
spec:
  schedule: "0 4 * * 0"
  template:
    executor:
      instances: 4                # 4 executores — job mais pesado
      memory: "2560m"
```

O Spark Operator cria pods de Driver + Executors dinamicamente para cada execução e os termina ao final — **zero idle capacity**.

### Por que Flink e não Spark Streaming?

| Aspecto | Apache Flink | Spark Structured Streaming |
|---|---|---|
| Modelo | Streaming verdadeiro (evento a evento) | Micro-batch (acumula por X segundos) |
| Latência | Sub-segundo | 1–30 segundos de latência mínima |
| Estado | RocksDB nativo, escala com disco | Stateful limitado, escala com memória |
| Exactly-once | Nativo com checkpoints | Com duas fases, mais complexo |

Para detecção de fraude, latência sub-segundo é a diferença entre bloquear uma transação fraudulenta e processá-la.

### Checkpointing

```python
env.enable_checkpointing(60_000)   # salva estado a cada 60 segundos
```

O Flink Operator salva checkpoints incrementais no Object Storage. Se o job cair, na reinicialização ele:
1. Encontra o último checkpoint no Object Storage
2. Restaura o estado de todos os operadores (contadores de prestadores/beneficiários)
3. Retoma a leitura do Redpanda a partir do offset registrado no checkpoint

**RPO efetivo: ~60 segundos** (perda máxima de dados em caso de falha).

---

## Passo 10 — Observabilidade

**Arquivos:** [infra/k8s/08-prometheus.yaml](../infra/k8s/08-prometheus.yaml), [infra/k8s/09-grafana.yaml](../infra/k8s/09-grafana.yaml)

### O que foi feito

Prometheus coleta métricas de todos os serviços; Grafana as visualiza em dashboards pré-configurados.

### Métricas-chave monitoradas

| Métrica | Alerta se |
|---|---|
| Consumer lag do Redpanda | > 10.000 mensagens (pipeline atrasado) |
| Taxa de checkpoint do Flink | < 99% de sucesso |
| Latência p99 do fraud-scorer | > 100ms |
| Fraud score médio | Sobe muito → possível ataque em massa |
| Taxa de falsos positivos | Revisão semanal manual |

### Acesso

```
Grafana    → http://NODE_IP:30300  (admin/admin)
Prometheus → http://NODE_IP:30909
```

---

## Problemas Reais que Encontramos e Como Resolvemos

| Problema | Commit | Solução |
|---|---|---|
| Debezium rejeitava conexão de replicação ao PostgreSQL | `ea18c94` | `ALTER USER fraud_user REPLICATION` + `CREATE PUBLICATION` |
| Spark Operator usava imagem errada e falhava no submit | `3872065` | Pin da imagem do Spark Operator para versão compatível |
| Firewall do LKE bloqueava tráfego IPIP entre nós | `565918f` | Regra de firewall permitindo protocolo 4 (IPIP) dentro do cluster |
| Registry in-cluster sem TLS rejeitado pelo containerd | `6042bcf` | DaemonSet que configura `insecure_registries` no containerd de cada nó |
| Gold ETL OOM em chave distorcida (phantom billing provider) | *pendente* | Precisa de salting antes do window; documentado no código |
| Spark image não encontrava JARs do Iceberg/Nessie | `9a64842` | Adicionado `download-deps.sh` que baixa os JARs no build da imagem |

---

## Arquitetura Final em Camadas

```
┌──────────────────────────────────────────────────────────────────────┐
│  FONTES DE DADOS                                                      │
│  PostgreSQL (claims_db) — WAL ativo para CDC                         │
└──────────────────────────────┬───────────────────────────────────────┘
                               │
                    Debezium (CDC via WAL)
                               │
┌──────────────────────────────▼───────────────────────────────────────┐
│  STREAMING BACKBONE — Redpanda                                        │
│  Tópicos: raw.claims.new │ scored.claims │ alerts.fraud               │
│  Particionado por provider_npi para localidade de estado             │
└──────────┬───────────────────────────────────────────────────────────┘
           │
     ┌─────┴─────────────────┐
     │                       │
     ▼                       ▼
┌─────────────┐      ┌───────────────────────────────────────────┐
│   Flink     │      │  Bronze Consumer → Object Storage         │
│  (tempo    │      │  (JSONL bruto particionado por dia)        │
│   real)    │      └──────────────────────┬────────────────────┘
│            │                             │
│  Estado:  │              Spark Silver ETL (diário)
│  RocksDB  │                             │
│           │             nessie.silver.claims
│  Saída:   │             (Iceberg, PHI tokenizado)
│  scored.  │                             │
│  claims   │              Spark Gold ETL (semanal)
│  alerts.  │                             │
│  fraud    │             nessie.gold.{provider,member,claim}_features
└─────┬─────┘             (vetores de features para ML)
      │
      │  HTTP POST /score
      ▼
┌────────────┐
│ fraud-     │   XGBoost model
│ scorer API │   Regras determinísticas
│            │   Score final = max(regras, ML*0.6 + regras*0.4)
└────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│  OBSERVABILIDADE                                                      │
│  Prometheus → métricas de todos os serviços                          │
│  Grafana    → dashboards (fraud rate, latência, consumer lag)        │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│  INFRAESTRUTURA — Akamai Cloud / Linode LKE                          │
│  Terraform → cluster K8s + Object Storage                            │
│  Helm      → Redpanda, PostgreSQL, Redis, Spark Operator, Flink Op  │
│  Namespaces: streaming | data | processing | ml | monitoring | tools │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Conceitos-Chave para Revisar

| Conceito | Onde aparece | Por que importa |
|---|---|---|
| CDC (Change Data Capture) | Debezium + PostgreSQL WAL | Captura mudanças em tempo real sem impacto no banco |
| Kafka-compatible streaming | Redpanda | Todo ecossistema (Flink, Spark, Debezium) usa o mesmo protocolo |
| Stateful stream processing | Flink + RocksDB | Janelas de 30/90 dias sem banco externo |
| Medallion architecture | Bronze/Silver/Gold | Qualidade crescente; PHI isolado no bronze |
| Apache Iceberg | Silver + Gold tables | ACID + time travel em object storage |
| Feature engineering | gold_features.py | Transforma guias brutas em vetores numéricos para ML |
| Skewed keys | gold_features OOM | Um prestador com volume anormal quebra a janela de agregação |
| Exactly-once | Flink checkpoints | Garantia que cada guia é pontuada exatamente uma vez, mesmo após falha |
| GitOps | deploy.sh + Terraform | Infraestrutura como código; reproduzível e auditável |
