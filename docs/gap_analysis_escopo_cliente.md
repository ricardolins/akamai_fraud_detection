# Gap Analysis — Escopo do Cliente vs. Arquitetura Atual

**Versão:** 1.0  
**Contexto:** Mapeamento do escopo recebido contra a plataforma de detecção de fraudes atual

---

## Resumo Executivo

A arquitetura atual **cobre o núcleo do escopo** — streaming, data lake medallion (bronze/silver/gold), ML batch/realtime/on-demand, e serving via API e SQL. Existem **três gaps reais** que precisam de trabalho adicional e **um ponto crítico de inconsistência de volumetria** que precisa ser esclarecido com o cliente antes de qualquer decisão de sizing.

| Área | Status |
|---|---|
| CDC multi-banco (MySQL, Oracle, MongoDB, etc.) | ⚠️ Parcial — Debezium arquitetado, só PostgreSQL configurado |
| Arquivos de mídia (imagem, PDF, áudio, vídeo) | ❌ Gap — pipeline binário não existe |
| Syslog JSON (WAF, CDN, GEO, SIEM) | ✅ Coberto — Vector já na arquitetura |
| Arquivos CSV/dataframe | ✅ Coberto — Redpanda Connect já arquitetado |
| Volumetria — Parquet + hierarquia de storage | ✅ Coberto — Iceberg + Parquet medallion |
| ML Batch / RealTime / On-demand em Python | ✅ Coberto — Flink + Spark + BentoML |
| Camada Gold — API, URL, SQL, NoSQL | ✅ Coberto — FastAPI + Trino + Redis |
| Spark / PySpark / SparkSQL na camada Gold | ✅ Coberto |
| Operational DB suplementar (opcional) | ✅ Coberto — PostgreSQL + arquitetura extensível |
| **Volumetria nginx (360GB/dia)** | 🚨 **Inconsistência — requer clarificação urgente** |

---

## 1. Ingestão — Fontes de Dados

### 1.1 Bancos de Dados (CDC)

**Status: ⚠️ Parcialmente coberto**

A arquitetura usa Debezium, que suporta todos os bancos listados. Porém, apenas o conector PostgreSQL está configurado. Cada banco exige trabalho específico:

| Banco | Debezium Connector | Observação |
|---|---|---|
| PostgreSQL | ✅ Configurado e testado | CDC via logical replication |
| MySQL | ✅ Disponível | Requer `binlog_format=ROW` no servidor |
| MongoDB | ✅ Disponível | Usa Change Streams — requer replica set |
| Oracle | ⚠️ Disponível | Requer **Oracle LogMiner** — pode ter implicação de licença Oracle |
| Informix | ⚠️ Disponível (experimental) | IBM Informix CDC é complexo; avaliar se Debezium ou extração batch é mais viável |
| Cassandra | ❌ Sem suporte nativo Debezium | Debezium Cassandra é experimental/mantido pela comunidade; alternativa: Spark batch |
| MariaDB | ✅ Disponível | Mesmo conector do MySQL |

**Ação necessária:**
- Configurar conectores para MySQL, MongoDB e Oracle
- Para Oracle: validar se o cliente tem licença LogMiner habilitada
- Para Informix e Cassandra: propor extração batch via Spark como alternativa mais estável
- Todos os conectores publicam no Redpanda com o mesmo modelo de tópico já definido

---

### 1.2 Arquivos de Mídia (Imagem, PDF, Áudio, Vídeo)

**Status: ❌ Gap real — pipeline não existe**

Este é o gap mais significativo. A arquitetura atual não tem pipeline para ingestão de binários. Em contexto de fraude em saúde, esses arquivos são tipicamente: laudos médicos (PDF), comprovantes de procedimento (imagem), receitas digitalizadas (imagem/PDF).

**O que precisamos construir:**

```
Arquivo chega (S3 event / API upload)
        │
        ▼
  Redpanda Connect     → Evento de metadados publicado no tópico raw.files.media
  (file watcher /      → Arquivo bruto armazenado em bronze/media/ (Object Storage)
   S3 trigger)
        │
        ▼
  Pipeline de extração de conteúdo (roda assíncrono):
  • PDF → extração de texto (Apache Tika / pdfplumber)
  • Imagem → OCR (Tesseract / AWS Textract se aprovado)
  • Áudio/Vídeo → transcrição (Whisper) — apenas se for caso de uso real
        │
        ▼
  Conteúdo extraído → silver/media/ (Parquet, indexado por claim_id)
  Metadados → Feature Store (tamanho, tipo, data, hash de conteúdo)
```

**Pergunta crítica para o cliente:** Qual é o caso de uso de fraude que requer análise de conteúdo de mídia? Se o objetivo é apenas armazenar e vincular ao sinistro (não analisar o conteúdo), o pipeline é muito mais simples.

---

### 1.3 Syslog JSON (WAF, CDN, GEO-LOCATION, SIEM)

**Status: ✅ Coberto pela arquitetura — Vector já está no design**

Vector (instalado como DaemonSet no LKE) é o coletor de logs. Já suporta nativamente:
- Arquivos JSON estruturados
- Formatos de WAF (Akamai, Cloudflare, AWS WAF)
- Nginx access/error logs
- Syslog RFC5424
- Netskope (via HTTP source ou S3 sink)

**Configuração necessária** (não é gap arquitetural, é trabalho de configuração):
- Criar fontes Vector para cada feed (WAF, CDN, GEO, SIEM, Netskope, Nginx)
- Mapear schemas para tópicos Redpanda correspondentes
- Normalizar timestamps e campos comuns (IP, user-agent, geo) antes de publicar

---

### 1.4 Arquivos CSV / Dataframe

**Status: ✅ Coberto — Redpanda Connect já arquitetado**

Redpanda Connect tem processadores nativos para CSV e dataframe. Pipeline padrão:

```
CSV drop no Object Storage → S3 event → Redpanda Connect lê, parseia → tópico raw.files.tabular
```

Para arquivos de até 70GB/mês, o volume não é um desafio — um único job Spark processa isso em minutos.

---

## 2. Volumetria — Ponto Crítico ⚠️

### Inconsistência no escopo recebido

O escopo do cliente apresenta uma **inconsistência matemática** que precisa ser resolvida antes de qualquer decisão de infraestrutura:

| Fonte | Volume informado | Volume mensal calculado |
|---|---|---|
| Syslog (WAF, SIEM, CDN) | 150 GB/mês | 150 GB |
| Netskope | 270 GB/mês | 270 GB |
| **Nginx** | **360 GB/dia** | **~10.800 GB (~10,8 TB)** |
| CSV | 70 GB/mês | 70 GB |
| **Total calculado** | | **~11.290 GB** |
| **Total informado** | | **850–900 GB** |

**O nginx a 360 GB/dia resulta em ~10,8 TB/mês — mais de 12x o total declarado.**

**Possíveis explicações a validar com o cliente:**

1. **360 GB é total mensal, não diário** → volumetria total bate (~840 GB). Provavelmente é isso.
2. **Apenas uma fração dos logs nginx é ingerida** (ex: só erros 4xx/5xx, ou amostragem) → confirmar regra de filtro
3. **360 GB/dia é um pico eventual**, não o fluxo contínuo → confirmar padrão
4. **Nginx está em escopo diferente** (processado externamente, só resultado chega aqui)

**Impacto no sizing de infraestrutura:**
- A 850 GB/mês: cluster atual de 3 nós g6-standard-4 é adequado para a fase inicial
- A 10,8 TB/mês só de nginx: precisaríamos rever Redpanda brokers, Object Storage estimado, e custo mensal significativamente

**Recomendação:** Confirmar com o cliente antes da proposta de arquitetura final.

---

## 3. Storage — Formato e Hierarquia

**Status: ✅ Coberto**

A arquitetura já usa exatamente o que o cliente descreve:

- **Parquet** como formato padrão (via Apache Iceberg)
- **Hierarquia por workload** = medallion bronze/silver/gold já implementado
- Iceberg permite segmentação por workload e time-travel (auditoria)

```
fraud-datalake/
  bronze/     → dados brutos, imutáveis, por fonte
  silver/     → limpos, normalizados, PHI tokenizado
  gold/       → features ML-ready, agregações, sem PHI direto
  media/      → binários (PDF, imagem) — bucket separado, acesso restrito
  models/     → artefatos de modelos treinados
```

Para **arquivos de mídia** (quando o pipeline for construído), recomendamos bucket separado com hierarquia própria:

```
fraud-media/
  bronze/images/year=2026/month=05/claim_id=xxx/
  bronze/pdfs/year=2026/month=05/claim_id=xxx/
  silver/ocr_results/year=2026/month=05/claim_id=xxx/
```

---

## 4. Prep & Train — ML (Batch / RealTime / On-demand)

**Status: ✅ Coberto — todos os três modos estão arquitetados**

| Modo | Componente | Status |
|---|---|---|
| **Batch** | Spark + Airflow + MLflow | ✅ silver-etl rodando no LKE (3.1M linhas → Iceberg). ⚠️ gold-features com bug de skew conhecido (ver `infra/spark-jobs/gold_features.py`), não corrigido ainda |
| **Real-Time** | Flink + BentoML (inference endpoint) | ✅ Rodando no LKE (Flink Kubernetes Operator, fraud-stream-job) |
| **On-demand** | FastAPI → BentoML | Arquitetado, configuração necessária |

Todos os modelos são Python-native — XGBoost, LightGBM, scikit-learn, PyTorch são suportados pelo BentoML sem adaptação.

Detalhes da implantação real (passo a passo, causas raiz e roteiro de demonstração) em `docs/flink_spark_implantacao_real.md`.

---

## 5. Camada Gold — Acesso

**Status: ✅ Coberto**

| Tipo de acesso | Componente | Status |
|---|---|---|
| **API REST** | FastAPI (scoring API) | Arquitetado |
| **URLs customizadas** | FastAPI rotas customizadas | Arquitetado |
| **SQL** | Trino sobre Iceberg | Arquitetado |
| **Spark / PySpark / SparkSQL** | Spark Operator no LKE | ✅ Rodando |
| **NoSQL** | Redis (feature store online) | Arquitetado |

Para o acesso SQL por analistas (fase Viewer), Trino é o motor recomendado — expõe as tabelas Iceberg como se fossem tabelas SQL padrão, sem mover dados.

---

## 6. Operational DB Suplementar

**Status: ✅ Coberto — não é bloqueante**

PostgreSQL já está na stack como banco de metadados (Feast registry, Nessie catalog, Airflow). Se novos bancos operacionais forem necessários como resultado do projeto, a infraestrutura LKE suporta o deployment de novos workloads sem mudança arquitetural.

---

## 7. Resumo de Ações por Prioridade

### Prioridade 1 — Clarificação com o cliente (antes de qualquer sizing)
- [ ] Confirmar se nginx é **360 GB/mês ou 360 GB/dia**
- [ ] Confirmar caso de uso de **arquivos de mídia** — armazenar e vincular, ou analisar conteúdo?
- [ ] Confirmar se Oracle LogMiner está disponível (licença)
- [ ] Confirmar viabilidade de CDC no Informix (ou migrar para extração batch)

### Prioridade 2 — Configuração (sem mudança arquitetural)
- [ ] Conectores Debezium: MySQL, MongoDB, Oracle
- [ ] Fontes Vector: Netskope, Nginx, WAF, SIEM
- [ ] Redpanda Connect: pipeline CSV/dataframe
- [ ] Tópicos Redpanda adicionais para novos feeds

### Prioridade 3 — Desenvolvimento de pipeline novo
- [ ] Pipeline de ingestão de arquivos binários (PDF, imagem)
- [ ] Pipeline de extração de conteúdo (Tika para PDF, OCR para imagem) — apenas se necessário
- [ ] Bucket separado `fraud-media/` com hierarquia própria

### Não há mudança necessária em
- Formato de storage (Parquet + Iceberg)
- ML platform (Batch/RealTime/On-demand)
- Camada Gold (FastAPI + Trino + Redis)
- Spark/PySpark/SparkSQL
- Operational DB

---

*Documento interno — gap analysis confidencial*
