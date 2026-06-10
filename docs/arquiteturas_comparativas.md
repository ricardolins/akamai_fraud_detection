# Arquiteturas Comparativas: Full Akamai vs. Híbrido Multi-Cloud

**Versão:** 1.0  
**Contexto:** O cliente pediu duas variantes de arquitetura para a plataforma de detecção de fraudes. Este documento descreve ambas, com diagramas, trade-offs e critérios de escolha.

---

## Resumo Executivo

| Dimensão                  | Arquitetura A — Full Akamai        | Arquitetura B — Híbrido            |
|---------------------------|------------------------------------|------------------------------------|
| ML onde?                  | Akamai (LKE)                       | Outra cloud (AWS/GCP/Azure)        |
| Streaming/Data Lake onde? | Akamai                             | Akamai                             |
| Complexidade operacional  | Média (1 cloud, mas tudo DIY)      | Alta (2 clouds, governança dupla)  |
| Custo                     | Menor (1 provedor)                 | Maior (egress + 2 provedores)      |
| Velocidade de ML          | Depende de GPUs na Akamai          | GPUs gerenciadas na cloud de ML    |
| Latência de inferência    | Menor (ML e dados co-localizados)  | Maior (cross-cloud para inferência)|
| Lock-in                   | Akamai Cloud                       | Akamai + cloud de ML               |
| Indicado para             | Operação centralizada, foco em custo | Time de ML já na outra cloud       |

---

## Arquitetura A — Full Akamai (All-in-One)

### Descrição

Todo o stack reside na Akamai Cloud: ingestão, streaming, data lake, treinamento de modelos e inferência. A equipe opera um único provedor de cloud.

### Diagrama

```
┌──────────────────────────────────────────────────────────────────────────┐
│                     FONTES DE DADOS (externas à Akamai)                  │
│                                                                           │
│  ┌───────────────┐  ┌────────────────┐  ┌─────────────────────────────┐ │
│  │ DBs On-Prem   │  │ DBs em outras  │  │ Arquivos / APIs externas    │ │
│  │ (Oracle, PG,  │  │ clouds (RDS,   │  │ (HL7, EDI 837, FHIR, TISS) │ │
│  │  SQL Server)  │  │  Cloud SQL)    │  │                             │ │
│  └───────┬───────┘  └───────┬────────┘  └──────────────┬──────────────┘ │
└──────────┼──────────────────┼────────────────────────────┼───────────────┘
           │  VPN Site-to-Site│  VPN / Private Link         │ SFTP/API via VPN
           │  (WireGuard/IPSec│                             │
┌──────────▼──────────────────▼─────────────────────────────▼──────────────┐
│                           AKAMAI CLOUD — LKE                              │
│                                                                           │
│  ═══════════════════════════════════════════════════════════════════════  │
│  CAMADA DE INGESTÃO                                                       │
│  ┌────────────────┐  ┌──────────────────┐  ┌──────────────────────────┐  │
│  │  Debezium      │  │  Redpanda Connect│  │  Custom Ingestors        │  │
│  │  (CDC / slots  │  │  (FileStream,    │  │  (HTTP/REST, FHIR,       │  │
│  │  de replicação)│  │   S3 Source)     │  │   HL7 parsers)           │  │
│  └───────┬────────┘  └──────┬───────────┘  └──────────────┬───────────┘  │
│          └──────────────────┼────────────────────────────── ┘             │
│                             ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │                    REDPANDA CLUSTER                                  │ │
│  │           (Kafka-compatible, Schema Registry, mTLS)                 │ │
│  │  Tópicos: raw.claims  raw.providers  raw.members  raw.files         │ │
│  └─────────────────────────────┬────────────────────────────────────────┘ │
│                                 │                                          │
│  ═══════════════════════════════╪══════════════════════════════════════╪═  │
│  CAMADA DE PROCESSAMENTO        │                                      │   │
│          ┌──────────────────────▼──────────────┐    ┌─────────────────▼┐  │
│          │    Apache Flink (LKE)               │    │  Apache Spark    │  │
│          │    • Scoring em tempo real          │    │  (LKE)           │  │
│          │    • Janelas deslizantes (1min/1h)  │    │  • ETL histórico │  │
│          │    • Detecção de padrões            │    │  • Feature eng.  │  │
│          │    • Enriquecimento de features     │    │  • Backfill      │  │
│          └──────────────────────┬──────────────┘    └────────┬─────────┘  │
│                                 │                            │             │
│  ═══════════════════════════════╪════════════════════════════╪════════════ │
│  DATA LAKE (Linode Object Storage + Apache Iceberg)          │             │
│                                 ▼                            ▼             │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │  bronze/raw/   ──►  silver/clean/  ──►  gold/features/  ──► gold/ml/│ │
│  │  (dados brutos)     (validados,         (agregados,          (treino,│ │
│  │                      tokenizados)        feature sets)        serve) │ │
│  │                                                                      │ │
│  │  Catalog: Project Nessie  │  Format: Parquet + Iceberg              │ │
│  └──────────────────────────────────────────────────────────────────────┘ │
│                                 │                                          │
│  ═══════════════════════════════╪══════════════════════════════════════╪═  │
│  PLATAFORMA DE ML               │                                      │   │
│          ┌──────────────────────▼──────────────┐    ┌─────────────────▼┐  │
│          │    Feature Store: Feast              │    │  MLflow          │  │
│          │    • Online store: Redis             │    │  • Experiments   │  │
│          │    • Offline store: Iceberg/S3       │    │  • Model registry│  │
│          └──────────────────────┬──────────────┘    └────────┬─────────┘  │
│                                 │                            │             │
│          ┌──────────────────────▼────────────────────────────▼──────────┐  │
│          │    Treinamento: Ray Cluster / Kubeflow Pipelines (LKE)       │  │
│          │    • Distributed training em CPU (fraud — geralmente ok)     │  │
│          │    • Agendado via Airflow (Apache)                           │  │
│          │    • Retreino automático por drift (Evidently AI)            │  │
│          └──────────────────────┬──────────────────────────────────────┘  │
│                                 │                                          │
│  ═══════════════════════════════╪════════════════════════════════════════  │
│  SERVING E ALERTAS              ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │  BentoML / Seldon Core  ──►  FastAPI (REST)  ──►  Redpanda (alerts)│  │
│  │  (model serving, LKE)         (scoring API)        (resultado bus) │  │
│  │                                                                     │  │
│  │  Grafana (dashboards)  │  Prometheus  │  Alertmanager               │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                            │
│  ═══════════════════════════════════════════════════════════════════════   │
│  PLATAFORMA (Cross-cutting)                                                │
│  LKE (Kubernetes)  │  ArgoCD (GitOps)  │  Vault  │  Keycloak  │  OPA     │
└────────────────────────────────────────────────────────────────────────────┘
```

### Stack Completo — Arquitetura A

| Camada          | Tecnologia                        | Onde Roda         |
|-----------------|-----------------------------------|-------------------|
| CDC / Ingestão  | Debezium, Redpanda Connect        | LKE (pods)        |
| Streaming       | Redpanda                          | LKE               |
| Stream Process  | Apache Flink                      | LKE               |
| Batch Process   | Apache Spark                      | LKE               |
| Data Lake       | Linode Object Storage + Iceberg   | Object Storage    |
| Catalog         | Project Nessie                    | LKE               |
| Feature Store   | Feast (online: Redis, offline: S3)| LKE               |
| ML Training     | Ray + Kubeflow Pipelines          | LKE               |
| Orchestration   | Apache Airflow                    | LKE               |
| Experiment Track| MLflow                            | LKE               |
| Model Serving   | BentoML ou Seldon Core            | LKE               |
| API de Scoring  | FastAPI                           | LKE               |
| Monit. de Drift | Evidently AI                      | LKE               |
| Dashboards      | Grafana + Prometheus              | LKE               |
| Secrets         | HashiCorp Vault                   | LKE               |
| IAM             | Keycloak                          | LKE               |
| GitOps          | ArgoCD                            | LKE               |

### Prós e Contras — Arquitetura A

**Prós:**
- Sem egress de dados entre clouds — dados nunca saem da Akamai
- Menor latência de inferência (ML co-localizado com dados)
- Um único provedor para cobrança, suporte e compliance
- Menor custo total (sem egress cross-cloud)
- Governança de dados simplificada (1 perímetro de segurança)

**Contras:**
- Akamai não oferece GPUs gerenciadas — treinamento pesado exige provisionamento manual de nós GPU
- Todos os serviços são self-managed (não há SageMaker, Vertex AI, etc.)
- Time de ML precisa operar Kubernetes diretamente
- Escalar o cluster de treinamento é manual

**Quando escolher Arquitetura A:**
- Time de ML ainda não existe ou está sendo formado
- Orçamento cloud é restrito e egress seria significativo
- Volume de dados é alto e o custo de mover cross-cloud seria proibitivo
- Preferência por operação centralizada e compliance em perímetro único

---

## Arquitetura B — Híbrido: Streaming/Data Lake na Akamai + ML em Outra Cloud

### Descrição

A Akamai Cloud é responsável pelo que faz melhor: ingestão segura de dados, streaming de alta performance, e data lake como repositório central. O treinamento e servimento de modelos de ML migram para uma cloud especializada (AWS, GCP ou Azure) que oferece serviços gerenciados de ML.

### Diagrama

```
┌──────────────────────────────────────────────────────────────────────────┐
│                     FONTES DE DADOS (externas)                           │
│  DBs On-Prem  │  DBs em outras clouds  │  Arquivos / APIs externas      │
└───────────────┼────────────────────────┼───────────────────────────────┘
                │    VPN / Private Link  │
┌───────────────▼────────────────────────▼───────────────────────────────┐
│                         AKAMAI CLOUD — LKE                              │
│                                                                          │
│  ═══════════════════════════════════════════════════════════════════    │
│  INGESTÃO + STREAMING (igual à Arq. A)                                  │
│                                                                          │
│  Debezium / Redpanda Connect ──► REDPANDA CLUSTER                       │
│                                       │                                  │
│  ═══════════════════════════════════ ╪ ═══════════════════════════════   │
│  PROCESSAMENTO DE STREAM              │                                  │
│                                       ▼                                  │
│                          Apache Flink (LKE)                              │
│                          • Pré-processamento em tempo real               │
│                          • Regras determinísticas de fraude              │
│                          • Feature extraction em tempo real              │
│                          • Alertas imediatos (sem ML)                    │
│                                       │                                  │
│  ═══════════════════════════════════ ╪ ═══════════════════════════════   │
│  DATA LAKE (Repositório Central)      │                                  │
│                                       ▼                                  │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  Linode Object Storage + Apache Iceberg + Project Nessie         │  │
│  │                                                                   │  │
│  │  bronze/ ──► silver/ ──► gold/features/  ◄── exportação para ML  │  │
│  │                                           (S3-compatible API)    │  │
│  │                                                                   │  │
│  │  ESTA É A ÚNICA SAÍDA DE DADOS PARA A CLOUD DE ML                │  │
│  │  Dados tokenizados, somente gold/features (sem PHI)              │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                  │                                       │
└──────────────────────────────────┼───────────────────────────────────────┘
                                   │
                         Conectividade Privada
                         (VPN / Private Endpoint)
                         Somente dados gold/features
                         Sem PHI, sem dados brutos
                                   │
┌──────────────────────────────────▼────────────────────────────────────────┐
│                      CLOUD DE ML (AWS / GCP / Azure)                       │
│                                                                             │
│  ═══════════════════════════════════════════════════════════════════════   │
│  INGESTÃO DE FEATURES                                                       │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │  S3 / GCS / ADLS (espelho do gold/features da Akamai)               │  │
│  │  Atualizado via pipeline incremental (Iceberg → S3 sync)            │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  ═══════════════════════════════════════════════════════════════════════   │
│  FEATURE STORE + TREINAMENTO                                                │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │  AWS: SageMaker Feature Store + SageMaker Training                  │  │
│  │  GCP: Vertex AI Feature Store + Vertex AI Training                  │  │
│  │  Azure: Azure ML Feature Store + Azure ML Training                  │  │
│  │                                                                      │  │
│  │  • GPU instances gerenciadas                                         │  │
│  │  • AutoML para baseline rápido                                       │  │
│  │  • Distributed training (Horovod, DeepSpeed)                        │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  ═══════════════════════════════════════════════════════════════════════   │
│  MODEL REGISTRY + SERVING                                                   │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │  MLflow (open source, pode rodar aqui ou na Akamai)                 │  │
│  │  Modelo exportado como ONNX ou artefato MLflow                      │  │
│  │                                                                      │  │
│  │  Opção 1: Serving na cloud de ML (API cross-cloud) ──────────────►  │  │
│  │  Opção 2: Modelo exportado e implantado de volta na Akamai  ──────► │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                    │ Opção 1: API de scoring     │ Opção 2: artefato exportado
                    ▼                             ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                      AKAMAI CLOUD — SERVING                               │
│                                                                           │
│  Opção 1: Flink chama API REST da cloud de ML para scoring em stream     │
│  Opção 2: Modelo ONNX carregado direto no Flink (zero latência adicional)│
│                                                                           │
│  FastAPI (scoring endpoint) ──► Redpanda (alertas) ──► Case Management  │
│  Grafana + Prometheus + Alertmanager                                      │
└────────────────────────────────────────────────────────────────────────────┘
```

### Stack por Camada — Arquitetura B

| Camada                | Onde         | Tecnologia                              |
|-----------------------|--------------|-----------------------------------------|
| Ingestão / CDC        | Akamai       | Debezium, Redpanda Connect              |
| Streaming             | Akamai       | Redpanda                                |
| Stream Processing     | Akamai       | Apache Flink                            |
| Batch ETL             | Akamai       | Apache Spark                            |
| Data Lake             | Akamai       | Object Storage + Iceberg + Nessie       |
| Feature Store (offline)| Cloud ML    | SageMaker / Vertex AI / Azure ML        |
| Feature Store (online) | Akamai      | Redis (features em tempo real)          |
| ML Training           | Cloud ML     | SageMaker / Vertex AI / Azure ML        |
| Experiment Tracking   | Cloud ML     | MLflow (managed ou self-hosted)         |
| Model Registry        | Cloud ML     | MLflow ou nativo da cloud               |
| Model Serving         | Akamai OU Cloud ML | BentoML (Akamai) ou endpoint gerenciado |
| Orchestração ML       | Cloud ML     | Step Functions / Vertex Pipelines / ADF |
| Drift Monitoring      | Akamai       | Evidently AI                            |
| Dashboards            | Akamai       | Grafana + Prometheus                    |
| Secrets               | Akamai       | HashiCorp Vault                         |

### Variante B1 — Inferência na Cloud de ML (API cross-cloud)

O Flink na Akamai chama o endpoint de scoring da cloud de ML a cada evento. Latência adicional de 20–80ms por chamada cross-cloud.

```
Evento → Flink (Akamai) → POST /score → SageMaker Endpoint (AWS) → score → Alerta
```

**Quando usar B1:** Se o modelo é muito grande para rodar na Akamai, ou se a cloud de ML exige que o modelo nunca saia do ambiente gerenciado (compliance).

### Variante B2 — Modelo Exportado para a Akamai (zero latência cross-cloud)

O modelo é treinado na cloud de ML, exportado como ONNX/TorchScript, e implantado na Akamai via CI/CD. Inferência ocorre localmente na Akamai.

```
Treinamento (AWS) → MLflow export → ArgoCD → BentoML (Akamai) → scoring local
```

**Quando usar B2:** Latência é crítica (< 10ms), volume de inferência é alto, ou o custo de chamadas cross-cloud seria significativo.

### Prós e Contras — Arquitetura B

**Prós:**
- Serviços gerenciados de ML (sem operar Kubeflow, Ray manualmente)
- GPUs gerenciadas e elásticas para treinamento intensivo
- Equipes de ML que já operam em AWS/GCP/Azure continuam no ambiente familiar
- Possibilidade de usar AutoML para iteração rápida

**Contras:**
- Custo de egress de dados da Akamai para a cloud de ML (pode ser significativo em alto volume)
- Dois perímetros de segurança — compliance e auditoria são mais complexos
- Latência de inferência maior na variante B1
- Dois contratos de cloud, duas contas, duas equipes de billing
- Dados de features precisam ser sincronizados e mantidos em dois lugares
- Requer conectividade privada entre Akamai e a cloud de ML

**Quando escolher Arquitetura B:**
- Time de ML já existe e opera em AWS, GCP ou Azure — migrar seria fricção desnecessária
- Modelos requerem GPU pesada (transformers, deep learning complexo)
- A cloud de ML já tem contratos ou investimentos existentes (reservas EC2, etc.)
- Orçamento para egress existe e é justificado pelo ganho de agilidade em ML

---

## Comparação de Custo (Estimativa Qualitativa)

```
                        ARQUITETURA A         ARQUITETURA B
                        (Full Akamai)         (Híbrido)
                        
Compute (VMs/K8s)       ████████████          ████████░░░░  (menos na Akamai)
ML Training             ████████████          ████░░░░░░░░  (gerenciado)
Data Lake               ████████████          ████████████  (igual)
Egress de dados         ░░░░░░░░░░░░          ████████████  (novo custo)
Licença/serviços ML     ░░░░░░░░░░░░          ████████████  (novo custo)
Operação (horas time)   ████████████          ████████████  (similar ou maior)

Total estimado          MENOR                 MAIOR (10-30% dependendo do volume)
```

---

## Critério de Decisão Recomendado

Use este fluxo para guiar a escolha com o cliente:

```
O time de ML já opera em AWS, GCP ou Azure?
├── SIM → Há volume suficiente para justificar o custo de egress?
│         ├── SIM → Arquitetura B (Híbrido)
│         └── NÃO → Avaliar migração do time para Akamai (Arq. A)
└── NÃO → O projeto requer GPU heavy (deep learning, LLMs)?
          ├── SIM → Arquitetura B com B2 (exporta modelo de volta)
          └── NÃO → Arquitetura A (Full Akamai) — fraude clássica
                     (GBM, XGBoost, Random Forest roda bem em CPU)
```

**Nota sobre fraude em saúde:** A maioria dos casos de uso de fraude em saúde suplementar (upcoding, ghost billing, padrões anômalos) funciona muito bem com Gradient Boosting (XGBoost, LightGBM) e modelos de isolamento (Isolation Forest, Autoencoders simples). Esses modelos rodam eficientemente em CPU, o que reduz substancialmente a necessidade de GPU e torna a Arquitetura A mais viável do que parece à primeira vista.

---

## Plano de Migração entre Arquiteturas

Se o cliente começar com Arquitetura A e precisar migrar para B no futuro (ou vice-versa), o Data Lake Iceberg na Akamai é o ponto de estabilidade — ele não muda. A migração envolve apenas mover as camadas de ML, não os dados.

```
Arquitetura A → B:
1. Provisionar feature store + training na cloud de ML
2. Exportar gold/features do Iceberg (Akamai) via S3-compatible API
3. Re-treinar modelos na nova plataforma
4. Validar resultados (shadow mode — ambas as plataformas servem em paralelo)
5. Desligar ML da Akamai

Arquitetura B → A:
1. Provisionar Ray + Kubeflow no LKE da Akamai
2. Migrar pipelines de treinamento
3. Exportar modelos para BentoML na Akamai
4. Shadow mode
5. Desligar cloud de ML externa
```

A portabilidade é garantida porque:
- Dados vivem em formato aberto (Iceberg + Parquet) — sem lock-in de formato
- Modelos são exportáveis como ONNX (independente de cloud)
- Redpanda é Kafka-compatible — conectores funcionam em qualquer ambiente
