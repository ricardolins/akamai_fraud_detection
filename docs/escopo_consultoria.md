# Proposta de Escopo — Plataforma de Detecção de Fraudes em Saúde
## DataLake Security & Fraud Analytics

**Versão:** 1.0  
**Classificação:** Confidencial  
**Destinatário:** [Nome do Cliente]  
**Elaborado por:** [Nome da Consultoria]  
**Data:** Junho 2026

---

## 1. Contexto e Objetivo

O mercado de saúde suplementar no Brasil registra perdas estimadas entre 15% e 25% do faturamento com fraudes, desperdícios e abusos — um problema que cresce à medida que os volumes de sinistros aumentam e os esquemas de fraude se sofisticam. A detecção manual e baseada em regras estáticas não escala e reage tarde demais.

Este projeto entrega uma **plataforma de dados e inteligência artificial** para detecção de fraudes, estruturada em três capacidades centrais:

1. **Ingestão unificada em tempo real** de todas as fontes de dados relevantes — bancos relacionais e NoSQL, logs de segurança, arquivos de mídia e tabelas estruturadas
2. **Data Lake Security** com armazenamento hierárquico (Bronze/Silver/Gold), governança de dados, rastreabilidade e conformidade com LGPD
3. **Modelos de Machine Learning** operacionais nos três modos de inferência — batch, tempo real e on-demand — com monitoramento contínuo de performance

A plataforma é construída sobre infraestrutura **Akamai Cloud (Linode)**, com componentes open source de mercado e arquitetura desacoplada que permite evolução independente de cada camada.

---

## 2. Escopo do Projeto

### 2.1 Dentro do Escopo

#### Camada de Ingestão
- Configuração de conectores CDC (Change Data Capture) para os bancos de dados:
  - PostgreSQL, MySQL, MongoDB, Oracle (via LogMiner), MariaDB
  - Informix e Cassandra: avaliação técnica na Fase 1; CDC ou extração batch dependendo da viabilidade
- Pipeline de ingestão de **arquivos de log estruturados** (JSON): nginx, WAF, CDN, GEO-LOCATION, SIEM, Netskope — via agente Vector
- Pipeline de ingestão de **arquivos tabulares** (CSV, dataframe) com processamento automatizado
- Pipeline de ingestão de **arquivos de mídia**: imagens, PDFs e documentos; suporte a áudio e vídeo avaliado na Fase 2
- Toda ingestão operando em **tempo real**, sem janelas fixas de processamento
- Plataforma de streaming **Redpanda** (Kafka-compatible) como backbone de eventos

#### Data Lake Security
- Arquitetura medallion com três zonas: **Bronze** (raw), **Silver** (limpo, normalizado), **Gold** (features, agregações, ML-ready)
- Formato **Parquet + Apache Iceberg** para todos os dados tabulares
- Segmentação de storage por workload e hierarquia (sinistros, logs de segurança, mídia, modelos)
- Catálogo de dados com versionamento (Project Nessie) — time travel e auditoria de snapshots
- Tokenização de dados sensíveis (CPF, nome, CID) via HashiCorp Vault
- Conformidade com **LGPD**: classificação de dados, controle de acesso por camada, log de acesso imutável

#### Processamento e ML
- Processamento em **tempo real** com Apache Flink: enriquecimento de eventos, aplicação de regras determinísticas, scoring de modelos
- Processamento em **batch** com Apache Spark: ETL bronze→silver→gold, treinamento de modelos, relatórios
- **Feature Store** (Feast): armazenamento e servimento de features com garantia de consistência treino-serving
- **Modelos de ML iniciais**:
  - Classificador supervisionado de sinistros (XGBoost/LightGBM)
  - Detector de anomalias não supervisionado (Isolation Forest)
  - Modelo de risco de prestador (batch, janelas 30/60/90 dias)
- Ciclo completo de ML: treinamento, validação, registro (MLflow), deploy, monitoramento de drift
- Suporte a inferência **batch**, **tempo real** e **on-demand** (API REST)

#### Camada Gold — Acesso e Serving
- **API REST** para scoring on-demand e consulta de alertas (FastAPI)
- **SQL** sobre dados Gold via Trino — acesso para analistas e Data Engineers sem mover dados
- **Spark / PySpark / SparkSQL** disponível para as fases Data Engineer e Viewer
- **NoSQL** via Redis para consultas de features online com latência < 10ms
- Dashboards operacionais com **Grafana**: alertas de fraude, performance dos modelos, saúde da plataforma

#### Plataforma e Operações
- Cluster **Kubernetes (LKE)** como runtime de todos os workloads
- **GitOps com ArgoCD**: deploy automatizado a partir de commits no repositório
- Observabilidade completa: métricas (Prometheus), logs (Loki), rastreamento (Tempo)
- Gestão de identidade e acesso: **Keycloak** (SSO/OAuth2) + **OPA** (políticas de autorização)
- Gestão de segredos: **HashiCorp Vault** (credenciais dinâmicas, sem senhas hardcoded)
- Documentação técnica de cada componente e runbooks operacionais

---

### 2.2 Fora do Escopo

Os itens abaixo **não fazem parte** desta entrega, salvo negociação de aditivo:

- Desenvolvimento de interfaces de usuário (UI/UX) para analistas de fraude — apenas APIs e dashboards Grafana
- Migração de dados históricos de sistemas legados (apenas ingestão de dados novos/incrementais)
- Integração com sistemas de case management ou workflow de investigação já existentes no cliente
- Construção de novos bancos de dados operacionais suplementares (Operational DB) — avaliado como fase futura
- Modelos de ML para casos de uso além dos especificados na Fase 3 (ex: detecção de abuso em odontologia, farmácia)
- Treinamento da equipe do cliente em ciência de dados ou engenharia de ML
- Suporte operacional pós-entrega (coberto por contrato de sustentação separado)
- Infraestrutura de DR (Disaster Recovery) em segunda região — arquitetura preparada, não implantada

---

## 3. Arquitetura da Solução

```
FONTES DE DADOS
  Bancos (CDC)        Logs (Syslog/JSON)      Arquivos (CSV, PDF, Mídia)
  MySQL, Oracle,      nginx, WAF, CDN,        Tabelas estruturadas,
  MongoDB, PostgreSQL SIEM, Netskope, GEO     Imagens, PDFs, Documentos
       │                     │                        │
       └─────────────────────┴────────────────────────┘
                             │
                    CAMADA DE INGESTÃO
              Debezium (CDC)  │  Vector (logs)  │  Redpanda Connect (arquivos)
                             │
                      ┌──────▼──────┐
                      │  REDPANDA   │  ← Backbone de streaming
                      │  (Kafka API)│    Tópicos: raw.*, enriched.*, scored.*, alerts.*
                      └──────┬──────┘
               ┌─────────────┴─────────────┐
               │                           │
        STREAM PROCESSING            BATCH PROCESSING
         Apache Flink                 Apache Spark
        (tempo real,                 (ETL diário,
         regras + ML)                 treinamento)
               │                           │
               └─────────────┬─────────────┘
                             │
                      DATA LAKE (Iceberg + Parquet)
                      Linode Object Storage
                      ┌──────────────────────────┐
                      │  Bronze → Silver → Gold  │
                      │  Nessie Catalog          │
                      │  7 anos de retenção      │
                      └──────────────────────────┘
                             │
                      ML PLATFORM
                      Feature Store (Feast)  +  MLflow  +  BentoML
                             │
                      SERVING LAYER
                      FastAPI (REST)  +  Trino (SQL)  +  Redis (NoSQL)
                      Grafana (Dashboards)
```

---

## 4. Fases e Entregas

### Fase 1 — Fundação e Ingestão *(Semanas 1–6)*

**Objetivo:** Plataforma operacional recebendo dados das fontes principais.

| # | Entrega | Critério de aceite |
|---|---|---|
| 1.1 | Cluster LKE provisionado com namespaces, RBAC e rede configurados | `kubectl get nodes` retorna todos os nós Ready |
| 1.2 | Redpanda cluster (3 brokers) com Schema Registry e tópicos iniciais | Produção e consumo de mensagens validados via rpk |
| 1.3 | Object Storage + Iceberg + Nessie catalog | Escrita e leitura de tabela Iceberg via Spark validadas |
| 1.4 | Vault + Keycloak + ArgoCD operacionais | Credencial dinâmica de banco gerada pelo Vault; login via Keycloak |
| 1.5 | Conectores CDC: PostgreSQL + MySQL + MongoDB | Eventos de INSERT/UPDATE/DELETE chegando nos tópicos raw.* |
| 1.6 | Pipeline Vector: nginx + Netskope + WAF | Logs chegando em raw.logs.* com GeoIP enriquecido |
| 1.7 | Pipeline CSV/dataframe via Redpanda Connect | Upload de arquivo CSV → evento em raw.files.tabular |
| 1.8 | Zona Bronze operacional | Todos os eventos persistidos em bronze/ com particionamento por data |
| 1.9 | Avaliação técnica Oracle (LogMiner) e Informix | Documento de viabilidade com recomendação de abordagem |

**Dependências do cliente na Fase 1:**
- Acesso aos bancos de dados de origem (usuário com permissão de leitura de binlog/WAF/replication)
- Amostra de logs nginx, Netskope e SIEM para validação de parsers
- Token de API Linode com permissões de LKE e Object Storage
- Aprovação do DPO para acesso a dados de sinistros em ambiente de desenvolvimento

---

### Fase 2 — Processamento e Data Lake *(Semanas 7–12)*

**Objetivo:** Dados organizados, limpos e prontos para ML na camada Gold.

| # | Entrega | Critério de aceite |
|---|---|---|
| 2.1 | ETL Spark: Bronze → Silver | Dados deduplicados, tipados, com PHI tokenizado em silver/ |
| 2.2 | ETL Spark: Silver → Gold | Features de prestador e beneficiário calculadas em gold/ |
| 2.3 | Flink: job de enriquecimento em tempo real | Eventos em enriched.claims com features de contexto anexadas |
| 2.4 | Flink: regras determinísticas de fraude | Alertas gerados para padrões conhecidos (duplicate claim, upcoding) |
| 2.5 | Feature Store (Feast) operacional | Feature group provider_stats_30d disponível online e offline |
| 2.6 | Orquestração Airflow: DAGs de ETL | DAGs daily_silver_etl e weekly_gold_features rodando em schedule |
| 2.7 | Pipeline de mídia: PDF e imagem | Binários em bronze/media/, metadados extraídos em silver/media/ |
| 2.8 | Observabilidade: Prometheus + Grafana + Loki | Dashboard de saúde da plataforma com alertas configurados |
| 2.9 | Documentação de schemas e catálogo de dados | Catálogo Nessie com descrição de todas as tabelas Gold |

---

### Fase 3 — Machine Learning *(Semanas 13–20)*

**Objetivo:** Modelos em produção gerando scores e alertas de fraude.

| # | Entrega | Critério de aceite |
|---|---|---|
| 3.1 | MLflow operacional | Experimento registrado com métricas e artefatos versionados |
| 3.2 | Modelo 1: Classificador de sinistros (XGBoost) | AUC-ROC ≥ 0,85 no conjunto de validação |
| 3.3 | Modelo 2: Detector de anomalias (Isolation Forest) | Taxa de alerta ≤ 5% do volume total (calibração com equipe de auditoria) |
| 3.4 | Modelo 3: Score de risco de prestador (batch diário) | Score disponível no Feature Store para 100% dos prestadores ativos |
| 3.5 | BentoML: serving de todos os modelos | Endpoint /score com p99 < 100ms sob carga de 100 req/s |
| 3.6 | Flink: job de scoring em tempo real | scored.claims com ML score em < 2s após chegada do sinistro |
| 3.7 | API on-demand: FastAPI + Trino | GET /score/{claim_id} e POST /score/batch operacionais |
| 3.8 | Pipeline de retreino | DAG monthly_model_train com shadow deploy e promoção automática |
| 3.9 | Monitoramento de drift | Dashboard de PSI por feature group com alerta quando PSI > 0,2 |
| 3.10 | Dashboard de fraude | Grafana: alertas por tipo, por prestador, por período, falsos positivos |

---

### Fase 4 — Hardening e Entrega em Produção *(Semanas 21–26)*

**Objetivo:** Plataforma auditada, documentada e aceita para operação em produção.

| # | Entrega | Critério de aceite |
|---|---|---|
| 4.1 | Auditoria de segurança | Relatório sem findings críticos; mTLS entre todos os serviços |
| 4.2 | Revisão LGPD com DPO | Parecer favorável do DPO; log de acesso a dados PHI auditável |
| 4.3 | Conectores Oracle e Informix | Conectores operacionais ou solução batch documentada e aceita |
| 4.4 | Teste de carga | Plataforma sustenta volumetria declarada + 50% de margem |
| 4.5 | Runbooks operacionais | 5 runbooks entregues: falha de broker, restart de job Flink, rollback de modelo, incidente de qualidade de dados, resposta a alerta de fraude |
| 4.6 | Documentação de arquitetura final | Diagrama atualizado + decisões técnicas registradas |
| 4.7 | Treinamento operacional | Sessão de handoff com equipe técnica do cliente (mínimo 8h) |
| 4.8 | Aceite formal | Sign-off do cliente em todos os critérios de aceite das Fases 1–3 |

---

## 5. Modelo de Time

### Time da Consultoria

| Papel | Responsabilidade | Dedicação estimada |
|---|---|---|
| **Arquiteto de Dados** | Decisões de arquitetura, gestão técnica do projeto | 50% durante todo o projeto |
| **Engenheiro de Dados Sênior** | Pipelines de ingestão, ETL Spark, Airflow | 100% Fases 1–2; 50% Fases 3–4 |
| **Engenheiro de Streaming** | Redpanda, Flink, conectores CDC, Vector | 100% Fase 1; 50% Fases 2–4 |
| **ML Engineer** | Feature Store, treino, BentoML, MLflow, drift | 50% Fase 2; 100% Fase 3; 50% Fase 4 |
| **Engenheiro de Plataforma** | LKE, ArgoCD, Vault, Keycloak, observabilidade | 100% Fase 1; 30% Fases 2–4 |
| **Gerente de Projeto** | Cerimônias, riscos, comunicação com cliente | 30% durante todo o projeto |

### Time do Cliente (necessário para o projeto)

| Papel | Responsabilidade | Quando necessário |
|---|---|---|
| **DBA / Engenheiro de Banco** | Acesso e configuração das fontes de dados (binlog, LogMiner) | Fase 1 |
| **Engenheiro de Infraestrutura** | Acesso à rede, VPN, firewalls, servidores nginx | Fase 1 |
| **Auditor Médico / Domain Expert** | Validação de alertas, feedback de ground truth para ML | Fases 3–4 |
| **DPO / Jurídico** | Aprovação LGPD, base legal para uso de dados em ML | Fase 1 e Fase 4 |
| **Product Owner** | Priorização de casos de uso, aceite de entregas | Ao longo do projeto |

---

## 6. Premissas e Dependências

O escopo e o prazo deste projeto assumem:

1. **Acesso às fontes de dados** será provido pelo cliente em até 10 dias úteis do início de cada fase
2. **Volumetria declarada** de 850–900 GB/mês é o baseline para sizing; variações superiores a 50% requerem revisão de infraestrutura
3. A **confirmação de 360 GB/dia de nginx** (vs. declaração mensal de 900 GB total) está pendente — o sizing atual assume 360 GB/mês; se for diário, haverá revisão de escopo de infraestrutura
4. O cliente possui ou providenciará **licença Oracle LogMiner** para CDC; sem licença, a abordagem será extração batch com latência de 15–60 minutos
5. **Dados de treinamento inicial** (histórico de sinistros com labels de fraude confirmada) serão disponibilizados em até 30 dias do início da Fase 3
6. **Aprovação do DPO** para uso de dados em ML será obtida antes do início da Fase 3
7. O ambiente de desenvolvimento terá acesso a uma **amostra anonimizada** dos dados de produção

---

## 7. Riscos e Mitigações

| Risco | Probabilidade | Impacto | Mitigação |
|---|---|---|---|
| Oracle LogMiner indisponível por licença | Média | Alto | Fallback para extração batch via Spark; latência de 15–60 min |
| Dados históricos insuficientes para treino (< 12 meses) | Média | Alto | Começar com modelo não supervisionado (anomalia); supervisioando em Fase 4 após acúmulo |
| DPO não aprova acesso a dados PHI para ML | Baixa | Alto | Usar apenas dados anonimizados/pseudonimizados desde o início (silver/gold sem PHI) |
| Volumetria real > 50% acima do declarado | Baixa | Médio | Sizing com 50% de margem; monitoramento de capacidade na Fase 1 |
| Instabilidade na conectividade com Informix | Alta | Baixo | Isolado em componente próprio; fallback para batch sem impacto no restante |
| Time do cliente indisponível para validações | Média | Médio | Processo assíncrono documentado; prazo congelado até recebermos feedback |

---

## 8. Fora do Controle da Consultoria

Os seguintes itens estão fora do nosso controle e podem impactar prazo:

- Liberação de acesso a bancos de dados de produção pelo time de segurança do cliente
- Aprovação de licenças de software (Oracle, Redpanda Enterprise)
- Disponibilidade da equipe técnica do cliente para sessões de validação
- Mudanças regulatórias da ANS que alterem requisitos de retenção ou acesso a dados

Nesses casos, o prazo será revisado proporcionalmente, sem impacto no valor contratado.

---

## 9. Modelo de Governança

### Cerimônias

| Cerimônia | Frequência | Participantes | Objetivo |
|---|---|---|---|
| Status semanal | Semanal | GP + PO do cliente | Progresso, impedimentos, próximos passos |
| Review de fase | Ao final de cada fase | Times técnicos + PO | Demonstração de entregas e aceite formal |
| Comitê de risco | Mensal ou sob demanda | Arquiteto + gerência do cliente | Decisões de escopo e risco |

### Gestão de Mudanças

Qualquer solicitação fora do escopo descrito neste documento seguirá o processo:

1. Solicitação registrada por escrito pelo cliente
2. Análise de impacto em prazo e custo em até 5 dias úteis
3. Aprovação formal antes da execução

Mudanças não aprovadas formalmente não serão executadas.

---

## 10. Critérios de Aceite Global

O projeto será considerado concluído quando:

- [ ] Todos os conectores de ingestão declarados no escopo estão operacionais e validados
- [ ] Data Lake com zonas Bronze, Silver e Gold populadas com dados reais
- [ ] Mínimo de 2 modelos de ML em produção gerando scores em tempo real
- [ ] API de scoring on-demand com p99 < 100ms validada em teste de carga
- [ ] Dashboard de fraude operacional com alertas chegando ao time de auditoria
- [ ] Auditoria de segurança sem findings críticos
- [ ] DPO emitiu parecer favorável
- [ ] Documentação técnica e runbooks entregues
- [ ] Sessão de handoff técnico realizada

---

## 11. Próximos Passos

| Ação | Responsável | Prazo |
|---|---|---|
| Confirmar volumetria nginx (360 GB/dia ou /mês) | Cliente | 5 dias úteis |
| Confirmar disponibilidade de Oracle LogMiner | Cliente / DBA | 5 dias úteis |
| Aprovação do escopo e assinatura do contrato | Ambos | 10 dias úteis |
| Kick-off oficial e início da Fase 1 | Ambos | Após assinatura |
| Disponibilização de acessos às fontes de dados | Cliente | Semana 1 |

---

*Documento confidencial — uso restrito às partes envolvidas na negociação*
