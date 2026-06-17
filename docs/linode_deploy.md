# Linode LKE — Guia de Deploy

Deploy completo da plataforma de detecção de fraudes no **Akamai Cloud Linode** usando LKE (Linode Kubernetes Engine).

---

## Arquitetura no LKE

```
Akamai Cloud Linode — br-gru (São Paulo)
│
├── Linode Object Storage  (S3-compatible)
│   └── s3://fraud-datalake/   bronze/ · silver/ · gold/ · warehouse/
│
└── LKE Cluster  (3 × g6-standard-4: 12 vCPU / 24GB RAM + autoscaler +2)
    │
    ├── Namespace: streaming
    │   ├── Redpanda          StatefulSet  (Helm)  — broker + schema registry
    │   ├── Redpanda Console  Deployment   (Helm)  — NodePort 30808
    │   └── Debezium          Deployment          — CDC PostgreSQL → Redpanda
    │
    ├── Namespace: data
    │   ├── PostgreSQL        StatefulSet  (Helm)  — claims source DB
    │   ├── Redis             StatefulSet  (Helm)  — feature cache online
    │   └── Nessie            Deployment          — catálogo Iceberg (Git para dados)
    │
    ├── Namespace: processing
    │   ├── Flink Operator    Deployment          — gerencia FlinkDeployments (Helm)
    │   ├── fraud-stream-job  FlinkDeployment     — stream real-time (Flink 1.18 + RocksDB)
    │   ├── stream-processor  Deployment (replicas=0) — fallback Python, desativado
    │   ├── bronze-consumer   Deployment          — Redpanda → Object Storage (NDJSON)
    │   ├── data-generator    Deployment          — gerador sintético de claims
    │   ├── Spark Operator    Deployment          — gerencia SparkApplications (Helm)
    │   ├── silver-etl        ScheduledSparkApp   — bronze → silver Iceberg (diário 02:00)
    │   └── gold-features     ScheduledSparkApp   — silver → gold features (domingo 04:00)
    │
    ├── Namespace: ml
    │   ├── fraud-scorer      Deployment + HPA    — FastAPI ML inference  NodePort 30800
    │   └── mlflow            Deployment + PVC    — experiment tracking   NodePort 30500
    │
    ├── Namespace: monitoring
    │   ├── prometheus        Deployment + PVC    — métricas    NodePort 30909
    │   └── grafana           Deployment + PVC    — dashboards  NodePort 30300
    │
    └── Namespace: tools
        ├── registry          Deployment          — registry Docker in-cluster (NodePort 32500)
        └── containerd-cfg    DaemonSet           — configura containerd em todos os nós
```

**Custo estimado:** ~$147/mês (3 × g6-standard-4 @ ~$48/nó + ~$3 em Block Storage). Destrua quando não estiver em uso.

---

## Pré-requisitos

| Ferramenta | Versão | Instalar |
|---|---|---|
| Terraform | ≥ 1.6 | `brew install terraform` |
| kubectl | ≥ 1.28 | `brew install kubectl` |
| Helm | ≥ 3.14 | `brew install helm` |
| Docker Desktop | 24.x | docker.com/products/docker-desktop |

Você também precisará de um **Linode API token** com permissão Read/Write → https://cloud.linode.com/profile/tokens

---

## Deploy Passo a Passo

### Passo 1 — Variáveis de ambiente

```bash
export TF_VAR_linode_token="seu-token-aqui"

# IP público da sua máquina (para o firewall do LKE)
export TF_VAR_allowed_ip=$(curl -s https://api.ipify.org)

# Tag para as imagens Docker (qualquer string, ex: "demo")
export TAG="demo"
```

### Passo 2 — Executar o script de deploy

```bash
cd infra/
./deploy.sh
```

O script executa **15 passos** em sequência e leva ~20–25 minutos na primeira execução:

```
Passo 1:  terraform apply         → provisiona cluster LKE + bucket Object Storage (~5 min)
Passo 2:  kubectl get nodes       → captura IP do nó para endereço do registry
Passo 3:  kubectl apply           → cria namespaces (streaming, data, processing, ml, monitoring, tools)
Passo 4:  deploy registry         → sobe registry Docker in-cluster no nó (NodePort 32500)
Passo 5:  DaemonSet containerd    → configura todos os nós para aceitar o registry HTTP
Passo 6:  build + push imagens    → [PAUSA MANUAL] configure Docker Desktop → insecure registry
                                     builds: fraud-scorer, stream-processor, bronze-consumer,
                                             data-generator, spark-jobs, flink-jobs
Passo 7:  cria secret S3          → credenciais do Object Storage nos namespaces processing + data
Passo 8:  helm install            → Redpanda, PostgreSQL, Redis, Spark Operator, Flink Operator
Passo 9:  cria tópicos            → raw.claims.new, scored.claims, alerts.fraud
Passo 10: apply serviços custom   → debezium, fraud-scorer, stream-processor(0), data-generator, mlflow
Passo 11: apply data lake         → Nessie, Spark RBAC, bronze-consumer
Passo 12: apply Spark ETL jobs    → ScheduledSparkApplication silver-etl (diário) + gold-features (semanal)
Passo 13: apply Flink job         → FlinkDeployment fraud-stream-job (substitui stream-processor)
Passo 14: apply observabilidade   → Prometheus + Grafana
Passo 15: aguarda pods + URLs     → imprime endereços de acesso
```

> **Passo 6 requer ação manual:** o registry in-cluster usa HTTP. O Docker Desktop precisa confiar nele como insecure registry antes de aceitar o push das imagens. O script pausa e exibe as instruções exatas.

### Passo 3 — Acessar a demo

Após o deploy, o script imprime:

```
══════════════════════════════════════════════════════════
  FRAUD DETECTION DEMO — ACCESS URLS
══════════════════════════════════════════════════════════

  Node IP : 143.42.xxx.xxx

  Grafana           →  http://143.42.xxx.xxx:30300   (admin / admin)
  Redpanda Console  →  http://143.42.xxx.xxx:30808
  Fraud Scorer API  →  http://143.42.xxx.xxx:30800/docs
  MLflow            →  http://143.42.xxx.xxx:30500
  Prometheus        →  http://143.42.xxx.xxx:30909
  Image Registry    →  http://143.42.xxx.xxx:32500/v2/_catalog
══════════════════════════════════════════════════════════
```

---

## O Que Mostrar

### Grafana
`http://NODE_IP:30300` → admin/admin → **Fraud Detection Platform — LKE**

Painéis populam automaticamente em 2–3 minutos após os pods ficarem prontos:
- Claims throughput (legítimas vs fraude)
- Fraud alerts por nível de risco (MEDIUM / HIGH / CRITICAL)
- Latência de inferência ML p50/p95/p99
- Consumer lag do Redpanda

### Redpanda Console
`http://NODE_IP:30808` → **Topics**

| Tópico | Esperado |
|---|---|
| `raw.claims.new` | ~2 mensagens/seg |
| `scored.claims` | ~2 mensagens/seg |
| `alerts.fraud` | ~1 mensagem a cada 6–10 seg |

Clique em qualquer tópico → **Messages** → observe fraudes chegando em tempo real.

### Flink Job em execução
```bash
export KUBECONFIG=.kubeconfig-demo

# Status do FlinkDeployment
kubectl get flinkdeployment -n processing

# Logs do TaskManager (alertas de fraude)
kubectl logs -f -n processing -l component=taskmanager | grep FRAUD
```

```
FRAUD [CRITICAL ] CLM-D9E4A102  provider=NPI-005  amount=R$  8,750.00  score=0.9312
FRAUD [HIGH    ] CLM-F1B3C007  provider=NPI-001  amount=R$  3,140.00  score=0.7821
```

### Fraud Scorer API
`http://NODE_IP:30800/docs` → `POST /score` → teste com uma claim fraudulenta:

```json
{
  "claim_id":               "DEMO-001",
  "amount":                 12500.00,
  "claim_count_30d":        148,
  "avg_amount_30d":         890.00,
  "hour_of_day":            3,
  "is_weekend":             1,
  "procedure_risk":         0.90,
  "member_claim_count_90d": 1
}
```
Esperado: `fraud_probability` > 0.90, `risk_level`: **CRITICAL**

### Spark ETL (Bronze → Silver → Gold)
```bash
# Ver jobs Spark agendados
kubectl get scheduledsparkapplication -n processing

# Forçar execução imediata do silver ETL (sem esperar o cron das 02:00)
kubectl create -f - <<EOF
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: silver-etl-manual
  namespace: processing
spec:
  # copiar spec do silver-etl-scheduled
EOF

# Acompanhar logs do driver
kubectl logs -f -n processing -l spark-role=driver
```

### Data lake no Object Storage
```bash
export AWS_ACCESS_KEY_ID=$(cd infra/terraform && terraform output -raw object_storage_access_key)
export AWS_SECRET_ACCESS_KEY=$(cd infra/terraform && terraform output -raw object_storage_secret_key)
export AWS_ENDPOINT_URL_S3=$(cd infra/terraform && terraform output -raw object_storage_endpoint)

# Ver arquivos bronze de hoje
aws s3 ls s3://fraud-datalake/bronze/claims/ --recursive | tail -10

# Ver tabelas Iceberg registradas no Nessie
curl -s http://NODE_IP:$(kubectl get svc nessie -n data -o jsonpath='{.spec.ports[0].nodePort}')/api/v1/trees/tree/main/entries \
  | python3 -m json.tool | grep name
```

---

## Demo Local (Docker Compose)

Para iterar rapidamente sem provisionar o cluster LKE, use o ambiente Docker Compose na pasta `demo/`.

```bash
# 1. Exporte as credenciais do Object Storage (usa o mesmo bucket que o LKE)
export AWS_ACCESS_KEY_ID=$(cd infra/terraform && terraform output -raw object_storage_access_key)
export AWS_SECRET_ACCESS_KEY=$(cd infra/terraform && terraform output -raw object_storage_secret_key)
export AWS_ENDPOINT_URL_S3=$(cd infra/terraform && terraform output -raw object_storage_endpoint)
export BUCKET=$(cd infra/terraform && terraform output -raw object_storage_bucket)

# 2. Suba a demo
cd demo/
make start

# 3. Após alguns minutos, rode os ETLs Spark
make etl-silver    # bronze Object Storage → silver Iceberg
make etl-gold      # silver → gold feature tables
```

Serviços disponíveis localmente:

| Serviço | URL |
|---|---|
| Redpanda Console | http://localhost:8080 |
| Fraud Scorer API | http://localhost:8000/docs |
| MLflow | http://localhost:5000 |
| Grafana | http://localhost:3000 (admin/admin) |
| Prometheus | http://localhost:9090 |
| Nessie API | http://localhost:19120/api/v1/config |

---

## Escalando a Demo

### Aumentar taxa de fraude
```bash
kubectl set env -n processing deploy/data-generator FRAUD_RATE=0.30 CLAIMS_PER_SECOND=5
```

### Escalar fraud-scorer
```bash
kubectl scale -n ml deploy/fraud-scorer --replicas=4
```

### Escalar Flink TaskManagers
Edite `infra/k8s/14-flink.yaml` e aumente `parallelism`, depois:
```bash
kubectl apply -f infra/k8s/14-flink.yaml
```

O Flink Operator detecta a mudança e escala os TaskManagers automaticamente.

---

## Personalizando o Cluster

Edite `infra/terraform/variables.tf` ou passe variáveis:

```bash
# Região (LGPD — dados no Brasil)
export TF_VAR_region="br-gru"

# Nós maiores se houver pressão de memória
export TF_VAR_node_type="g6-standard-6"   # 6 vCPU / 16GB cada

# Mais nós
export TF_VAR_node_count=4
```

---

## Estimativa de Custo

| Recurso | Tipo | Custo mensal |
|---|---|---|
| 3× Worker Nodes | g6-standard-4 (4 vCPU / 8GB) | ~$144 |
| LKE Control Plane | Gerenciado (gratuito) | $0 |
| Block Storage (PVCs) | ~30GB (MLflow + Prometheus + Grafana + PG) | ~$3 |
| Object Storage | ~50GB (bronze/silver/gold) | ~$2.50 |
| **Total** | | **~$150/mês** |

> Destrua o cluster quando não estiver em uso:
> ```bash
> cd infra/ && ./destroy.sh
> ```

---

## Troubleshooting

**Pods em Pending:**
```bash
kubectl describe pod -n <namespace> <pod-name>
# Geralmente: CPU/memória insuficiente → use node_type maior
```

**ImagePullBackOff nos serviços custom:**
```bash
kubectl describe pod -n processing <pod>
# Verifique: nome da imagem correto? Registry acessível? IP do nó mudou?
# O registry usa o IP do nó — se o nó foi recriado, o endereço muda.
```

**Tópicos Redpanda não criados:**
```bash
kubectl logs -n streaming job/redpanda-topic-init
# Se falhou: delete o job e re-aplique 02-redpanda-topics.yaml
kubectl delete job redpanda-topic-init -n streaming
kubectl apply -f infra/k8s/02-redpanda-topics.yaml
```

**Debezium connector não registrando:**
```bash
kubectl logs -n streaming job/debezium-connector-init
kubectl exec -n streaming deploy/debezium -- \
  curl -s http://localhost:8083/connectors/claims-postgres-connector/status
```

**Flink job não inicia:**
```bash
kubectl describe flinkdeployment fraud-stream-job -n processing
kubectl logs -n processing -l component=jobmanager
# Verifique: imagem correta? Kafka brokers acessíveis?
```

**Spark job falha (silver ETL):**
```bash
kubectl get sparkapplication -n processing
kubectl logs -n processing -l spark-role=driver
# Causa mais comum: credenciais S3 incorretas ou endpoint errado
```

**Spark gold-features — OOM com exit 134:**
```bash
# Causa conhecida: data skew no provider de alto volume (phantom billing)
# Não é falta de memória do cluster — é uma chave distorcida na janela de 30d
# Ver detalhes: docs/flink_spark_implantacao_real.md seção 6
```

**Ver todos os pods de uma vez:**
```bash
kubectl get pods -A --sort-by=.metadata.namespace
```

---

## Teardown

```bash
cd infra/
./destroy.sh
```

Executa `terraform destroy`, remove o cluster LKE, todos os node pools e PVCs. O arquivo `.kubeconfig-demo` é removido localmente.

> **Atenção:** o bucket do Object Storage **não é destruído** pelo `destroy.sh` para preservar os dados do data lake. Para remover também, acesse o Linode Cloud Manager → Object Storage → delete manualmente.

---

## Referência de Arquivos

```
infra/
├── terraform/
│   ├── main.tf              LKE cluster + kubeconfig output
│   ├── object_storage.tf    bucket + access key do Linode Object Storage
│   ├── firewall.tf          regras de firewall (allow-list por IP)
│   ├── variables.tf         token, region, node_type, node_count, allowed_ip
│   └── outputs.tf           cluster_id, object_storage_endpoint/key/bucket
│
├── helm/
│   ├── redpanda-values.yaml       single-node, NodePort console
│   ├── postgres-values.yaml       WAL logical + initdb configmap
│   ├── redis-values.yaml          standalone, sem auth (demo)
│   ├── spark-operator-values.yaml jobNamespaces: [processing]
│   └── flink-operator-values.yaml watchNamespaces: [processing], webhook desativado
│
├── k8s/
│   ├── 00-namespaces.yaml         streaming, data, processing, ml, monitoring, tools
│   ├── 00b-registry.yaml          registry Docker in-cluster (NodePort 32500)
│   ├── 00c-containerd-config.yaml DaemonSet configura containerd em todos os nós
│   ├── 01-postgres-initdb.yaml    ConfigMap com schema SQL + seed de prestadores
│   ├── 02-redpanda-topics.yaml    Job: cria raw.claims.new, scored.claims, alerts.fraud
│   ├── 03-debezium.yaml           Deployment + Job (registro do conector)
│   ├── 04-fraud-scorer.yaml       Deployment + NodePort 30800 + HPA
│   ├── 05-stream-processor.yaml   Deployment replicas=0 (desativado, fallback Python)
│   ├── 06-data-generator.yaml     Deployment
│   ├── 07-mlflow.yaml             Deployment + PVC + NodePort 30500
│   ├── 08-prometheus.yaml         Deployment + RBAC + ConfigMap + NodePort 30909
│   ├── 09-grafana.yaml            Deployment + ConfigMaps + NodePort 30300
│   ├── 10-nessie.yaml             Deployment (catálogo Iceberg via PostgreSQL)
│   ├── 11-bronze-consumer.yaml    Deployment (Redpanda → Object Storage NDJSON)
│   ├── 12-spark-rbac.yaml         ServiceAccount + Role para o Spark Operator
│   ├── 13-spark-jobs.yaml         ScheduledSparkApplication: silver-etl + gold-features
│   └── 14-flink.yaml              FlinkDeployment: fraud-stream-job (PyFlink 1.18)
│
├── flink-jobs/
│   ├── Dockerfile             flink:1.18 + Python + apache-flink + kafka connector
│   └── fraud_stream_job.py    PyFlink DataStream: enrich → rules → ML → score/alert
│
├── spark-jobs/
│   ├── Dockerfile             apache/spark:3.5 + JARs Iceberg/Nessie/S3A
│   ├── silver_etl.py          bronze NDJSON → silver Iceberg (tokeniza PHI)
│   ├── gold_features.py       silver Iceberg → gold feature tables (agregações 30d/90d)
│   └── download-deps.sh       baixa JARs Iceberg/Nessie/Hadoop (não versionados no Git)
│
├── build-push.sh   builds e faz push de todas as imagens custom para o registry
├── deploy.sh       deploy completo: terraform → helm → kubectl → URLs
└── destroy.sh      terraform destroy + limpeza local
```
