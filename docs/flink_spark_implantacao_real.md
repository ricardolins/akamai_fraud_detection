# Apache Flink e Apache Spark Rodando de Verdade no LKE

## 1. Contexto

A demonstração inicial do projeto provou o conceito da arquitetura de detecção de fraude, mas dois componentes centrais ainda eram simulados:

- **Stream processing**: um processo Python (`demo/stream_processor/processor.py`) replicava o comportamento de um job Flink — usando Redis para imitar o que seria *state* gerenciado pelo Flink.
- **Batch processing (camada Gold/Silver)**: o Spark Operator estava instalado no cluster, mas os jobs nunca tinham executado de fato.

Este documento registra o que foi corrigido e implantado para que **Apache Flink** (processamento em tempo real) e **Apache Spark** (processamento batch) passem a rodar de verdade no cluster Kubernetes (LKE — Linode Kubernetes Engine), e serve como roteiro de apresentação para demonstrar isso ao cliente de forma didática.

---

## 2. Resumo executivo

| Componente | Antes | Depois |
|---|---|---|
| Stream processing | Processo Python fingindo ser Flink, estado em Redis | Job **Apache Flink real**, estado *keyed* em RocksDB, checkpoints automáticos |
| Batch ETL (Bronze→Silver) | Spark Operator instalado, mas nunca executava | Job Spark real, **3.151.276 linhas** escritas na tabela Iceberg `nessie.silver.claims` |
| Batch features (Silver→Gold) | Nunca executava | Executa, mas encontra um bug real de *data skew* em produtores de alto volume (ver seção 6) |

---

## 3. Spark — causa raiz e correção

### 3.1 Por que o Spark nunca executava

O Spark Operator (chart `spark-operator/spark-operator`, versão 2.5.0) é configurado por `infra/helm/spark-operator-values.yaml`. O arquivo continha:

```yaml
sparkJobNamespace: processing
```

Essa chave **não existe** no chart 2.5.0 — a chave correta é `spark.jobNamespaces` (uma lista). Como o valor não foi reconhecido, o controller do operator subiu observando apenas o namespace `default`:

```
--namespaces=default
```

Os `SparkApplication`/`ScheduledSparkApplication` eram criados no namespace `processing`, então o operator nunca os via. Um job submetido manualmente 14 dias antes desta correção não tinha **nenhum evento registrado** — prova de que o operator nunca chegou a processá-lo.

**Correção** (`infra/helm/spark-operator-values.yaml`):

```yaml
spark:
  jobNamespaces:
    - processing
```

Depois de `helm upgrade`, o controller passou a iniciar com `--namespaces=processing` e imediatamente começou a processar a fila de jobs pendente.

### 3.2 Bug real no job de ETL (Silver)

Com o operator corrigido, o primeiro job (`silver_etl.py`) rodou e revelou um segundo problema, este de lógica:

```python
.withColumn("event_time", F.to_timestamp("event_time"))   # coluna não existe!
```

Os dados gerados pelo `data-generator` usam o campo `submitted_at`, não `event_time`. Esse bug nunca tinha sido percebido porque o job nunca tinha chegado a rodar contra dados reais. Correção:

```python
.withColumn("event_time", F.to_timestamp("submitted_at"))
```

### 3.3 Dependência faltante para escrever no Iceberg via S3

Depois de corrigir o schema, o job avançou e falhou em um terceiro ponto, ao tentar escrever a tabela Iceberg:

```
NoClassDefFoundError: software/amazon/awssdk/services/s3/model/S3Exception
```

O catálogo Nessie/Iceberg usa `org.apache.iceberg.aws.s3.S3FileIO`, que depende do **AWS SDK v2** (`software.amazon.awssdk.*`). A imagem do job só tinha o **AWS SDK v1** (`aws-java-sdk-bundle`, usado pelo `S3AFileSystem`/Hadoop para ler o bronze via `s3a://`). São dois SDKs diferentes para dois caminhos de leitura/escrita diferentes dentro do mesmo job.

**Correção**: adicionado `iceberg-aws-bundle-1.5.2.jar` (30 MB, baixado por `infra/spark-jobs/download-deps.sh`, não versionado no Git) à imagem (`infra/spark-jobs/Dockerfile`).

### 3.4 Região da AWS não configurada

Quarto e último erro, já durante a escrita real do Parquet/Iceberg nos executors:

```
SdkClientException: Unable to load region from any of the providers in the chain
```

O AWS SDK v2 exige uma região configurada (mesmo usando um endpoint S3-compatível de outro provedor — neste caso, Linode Object Storage). Correção, adicionada ao `sparkConf` de ambos os jobs:

```yaml
spark.sql.catalog.nessie.client.region: "br-gru-1"
```

### 3.5 Ajuste de capacidade

O cluster LKE usado tem 4 nós × 4 vCPU / 8 GiB (≈16 vCPU / 24 GiB no total). A configuração original do `gold-features` pedia sozinha `driver(2cpu/2Gi) + 4×executor(2cpu/4Gi)` — quase o cluster inteiro. Os requests de CPU/memória de ambos os jobs (`infra/k8s/13-spark-jobs.yaml`) foram reduzidos para caber ao lado do Flink e dos demais serviços do cluster, e a política de imagem foi alterada para `imagePullPolicy: Always` (com `IfNotPresent`, o Kubernetes reaproveitava a imagem antiga em cache mesmo após um novo `docker push` na mesma tag, mascarando as correções).

### 3.6 Resultado validado

```
[silver-etl] Read 3151276 records from s3a://fraud-datalake/bronze/claims/
[silver-etl] Done. Silver table total rows: 3151276
```

A tabela `nessie.silver.claims` foi criada e populada de fato, com tokenização de PHI (`member_id`/`provider_npi` via SHA-256) já aplicada pelo job, exatamente como descrito na arquitetura.

---

## 4. Flink — implantação do zero

Diferente do Spark, não havia nenhuma instalação prévia de Flink no cluster — foi implantado do início.

### 4.1 Flink Kubernetes Operator

Instalado via Helm (`infra/helm/flink-operator-values.yaml`), versão 1.14.0, escopado ao namespace `processing`:

```yaml
watchNamespaces:
  - processing
webhook:
  create: false   # o webhook padrão exige cert-manager, que não faz parte do stack
```

O operator cria os CRDs `flinkdeployments.flink.apache.org`, `flinksessionjobs.flink.apache.org`, entre outros, e passa a gerenciar o ciclo de vida (submissão, upgrade, checkpoints, restart) de jobs Flink declarados como recursos Kubernetes — o mesmo padrão operacional já usado para o Spark Operator.

### 4.2 O job de stream processing (`infra/flink-jobs/fraud_stream_job.py`)

Reescrita em **PyFlink** (DataStream API) da lógica que antes vivia em `processor.py`, mantendo as mesmas regras de negócio, mas usando primitivas reais do Flink:

| Conceito | Implementação Python (antiga) | Flink real (atual) |
|---|---|---|
| Estado por provider/member | Hash no Redis, com TTL manual | `ValueState` *keyed*, gerenciado pelo Flink, com `StateTtlConfig` (30d/90d) |
| Backend de estado | Redis externo | **RocksDB** embarcado no TaskManager |
| Tolerância a falhas | Nenhuma — se o pod morre, o estado seria perdido se não fosse o Redis externo | **Checkpoints automáticos a cada 60s**, com restart a partir do último checkpoint |
| Consumo do Kafka | Cliente `confluent_kafka` manual, com retry próprio | `KafkaSource`/`KafkaSink` nativos do Flink, com *exactly-once* via checkpoint |

Pipeline do job:

```
raw.claims.new (Kafka/Redpanda)
   │
   ▼
keyBy(provider_npi) → ProviderEnrichFunction   # state: claim_count_30d, avg_amount_30d
   │
   ▼
keyBy(member_id) → MemberEnrichFunction        # state: member_claim_count_90d
   │
   ▼
ScoreFunction  # regras determinísticas + chamada HTTP ao fraud-scorer (ML)
   │
   ├──────────────► scored.claims (sempre)
   └──(score ≥ 0.65)─► alerts.fraud
```

As regras de fraude (valor 3x acima da média do provider, valor extremo, combinação procedimento/diagnóstico suspeita, provider de alto volume) são as mesmas já validadas na demo anterior — só a infraestrutura de execução mudou.

### 4.3 Empacotamento

`infra/flink-jobs/Dockerfile` parte da imagem oficial `flink:1.18-scala_2.12-java11` e adiciona:
- Python 3 + `apache-flink==1.18.1` (PyFlink)
- `flink-sql-connector-kafka-3.1.0-1.18.jar` (conector Kafka, compatível com o protocolo do Redpanda sem nenhuma configuração especial)
- o script do job em `/opt/flink/usrlib/`

### 4.4 `FlinkDeployment` (`infra/k8s/14-flink.yaml`)

Declara o job em **Application Mode** (um cluster Flink dedicado por job, não um cluster compartilhado):

```yaml
flinkVersion: v1_18
flinkConfiguration:
  state.backend.type: "rocksdb"
  execution.checkpointing.interval: "60000"
jobManager:
  resource: { memory: "1024m", cpu: 1 }
taskManager:
  resource: { memory: "2048m", cpu: 1 }
job:
  entryClass: "org.apache.flink.client.python.PythonDriver"
  args: ["-pyclientexec", "/usr/bin/python3", "-py", "/opt/flink/usrlib/fraud_stream_job.py"]
  parallelism: 2
```

### 4.5 Ajuste de consumo: `earliest` vs. `latest`

Na primeira subida, o job foi configurado para ler o tópico `raw.claims.new` desde o início (`earliest`). Como o tópico já tinha ~18 dias de histórico (vários milhões de mensagens), o job entrou em um reprocessamento gigante, sobrecarregando o `fraud-scorer` com chamadas síncronas e causando timeouts de leitura no Kafka. Para uma demonstração — onde o objetivo é mostrar claims **novos** sendo processados em tempo real — o consumo foi trocado para `latest`, com um novo `group_id` (`flink-fraud-stream-v2`) para garantir um início limpo. O lag caiu de ~1,37 milhão de mensagens para menos de 100, acompanhando a produção em tempo real.

### 4.6 Corte do processo antigo

Com o Flink validado, o `Deployment` antigo (`stream-processor`) foi escalado para `replicas: 0` em `infra/k8s/05-stream-processor.yaml` — mantido no repositório como referência/fallback documentado, não apagado. Isso evita que dois consumidores diferentes gerem alertas duplicados para o mesmo claim.

### 4.7 Resultado validado

- `kubectl get flinkdeployment` → `JOB STATUS: RUNNING`, `LIFECYCLE STATE: STABLE`.
- Checkpoints completando a cada 60s sem falhas.
- Consumer group `flink-fraud-stream-v2` com lag baixo e estável.
- Tópicos `scored.claims` e `alerts.fraud` recebendo mensagens novas exclusivamente do Flink (confirmado depois de desligar o processo Python antigo).

---

## 5. Como isso se encaixa na arquitetura de detecção de fraude

```
┌─────────────┐     ┌───────────────┐     ┌──────────────────────────┐
│  Fontes de  │────▶│   Redpanda    │────▶│   Apache Flink (real)    │──┐
│   dados     │     │ raw.claims.new│     │  fraud-stream-job        │  │
└─────────────┘     └───────┬───────┘     │  - enrich (keyed state)  │  │
                             │             │  - regras determinísticas│  │
                             │             │  - score ML (HTTP)       │  │
                             │             └──────────────────────────┘  │
                             │                                            ▼
                             │                                   scored.claims
                             │                                   alerts.fraud
                             ▼
                     ┌───────────────┐
                     │ bronze-consumer│  (grava raw em Object Storage, sem transformação)
                     └───────┬───────┘
                              ▼
                     s3://fraud-datalake/bronze/claims/
                              │
                              ▼
                     ┌───────────────────────────┐
                     │ Apache Spark (real)        │
                     │ silver-etl (diário 02:00)  │──▶ nessie.silver.claims (Iceberg)
                     │ gold-features (semanal)    │──▶ nessie.gold.* (features p/ ML)
                     └───────────────────────────┘
```

**Por que dois motores diferentes, e não só um?**

- **Flink** cobre o caminho de **detecção em tempo real**: cada claim precisa de uma decisão (aprovar, sinalizar, bloquear) em milissegundos, com contexto acumulado (quantos claims esse provider já fez nos últimos 30 dias, etc.) mantido continuamente em memória/estado — exatamente o que o *keyed state* do Flink resolve nativamente, sem depender de um banco externo para isso.
- **Spark** cobre o caminho de **enriquecimento e treinamento em lote**: tokenizar PHI, consolidar o histórico completo em um formato de tabela versionado (Iceberg via Nessie) e calcular features agregadas (médias, desvios, contagens por janela) que alimentam o retrain periódico dos modelos de ML. Esse tipo de agregação pesada sobre grandes volumes é o ponto forte do Spark, não do Flink.

Os dois motores compartilham a mesma fonte de eventos (Redpanda) e o mesmo data lake (Iceberg/Nessie sobre Object Storage), mantendo a arquitetura medallion (bronze → silver → gold) coerente entre o caminho de streaming e o caminho batch.

---

## 6. Pendência conhecida: data skew no `gold-features`

O job semanal `gold_features.py` (silver → gold) falha de forma consistente com `exit code 134` ao tentar materializar a tabela `claim_features`. A causa identificada **não é falta de memória no cluster** (aumentar o tamanho dos executors não resolveu) — é **distorção de dados (data skew)**:

- As janelas de 30/90 dias em `build_provider_features`/`build_member_features` usam `Window.partitionBy(...).rangeBetween(-30*86400, 0)`, o que faz o Spark manter, em memória, todas as linhas de uma mesma chave (provider/member) dentro da janela.
- O gerador de dados da demo injeta de propósito um cenário de "phantom billing" — um provider com volume de claims muito acima da média (é exatamente o padrão que a regra `HIGH_VOLUME_PROVIDER` foi desenhada para capturar). Esse único provider concentra dezenas de milhares de linhas dentro de uma janela de 30 dias, e a tarefa responsável por essa chave específica estoura a memória do executor que a processa — independente de quantos executors ou quanta memória total o job recebe, porque o trabalho dessa chave está todo concentrado em uma única tarefa.

**Caminhos de correção (não implementados ainda):**
1. *Salting* da chave (`provider_npi_token`) antes da janela, distribuindo artificialmente as linhas de um provider de alto volume entre múltiplas sub-chaves, e agregando o resultado depois.
2. Trocar o `rangeBetween` (baseado em tempo, sem limite de linhas) por um `rowsBetween` com limite superior de linhas, aceitando uma aproximação no cálculo da janela.
3. Pré-agregar o histórico antigo (ex.: rollups diários por provider) antes de aplicar a janela final, reduzindo o número de linhas por chave.

Esse bug está documentado também em `infra/spark-jobs/gold_features.py` (comentário no topo do arquivo) e em `docs/gap_analysis_escopo_cliente.md`.

---

## 7. Guia de demonstração educacional

Roteiro sugerido para apresentar esta evolução ao cliente, com o objetivo de explicar **o que mudou e por quê**, não só mostrar telas.

### Passo 1 — Recapitular o que a demo anterior mostrou

> "Na última demo, vocês viram o pipeline completo funcionando: claim chega, é enriquecido, pontuado e gera um alerta de fraude. A lógica de negócio já estava certa. O que mudou agora é a engine por baixo — antes era uma simulação em Python, agora são as ferramentas reais que vão para produção: Apache Flink e Apache Spark, rodando no mesmo cluster Kubernetes que vocês já conhecem."

### Passo 2 — Mostrar o Flink rodando

```bash
export KUBECONFIG=.kubeconfig-demo-new
kubectl get flinkdeployment -n processing
kubectl get pods -n processing | grep fraud-stream
```

Pontos a destacar:
- **JobManager** e **TaskManager** são processos reais do Flink, não scripts Python.
- "Lifecycle state: STABLE" significa que o Flink considera o job saudável e sob seu gerenciamento automático.

Abrir a UI do Flink (via port-forward) e mostrar o grafo do job (as etapas: source → enrich → score → sinks) e a aba de **Checkpoints** — explicar que cada checkpoint é uma "foto" do estado do job, e que se um TaskManager cair, o Flink reinicia a partir do último checkpoint sem perder o controle de quantos claims cada provider já fez.

### Passo 3 — Mostrar um claim novo sendo processado em tempo real

No Redpanda Console (ou `rpk topic consume`), mostrar uma claim chegando em `raw.claims.new` e, segundos depois, o resultado correspondente aparecendo em `scored.claims` e (se for um caso de fraude) em `alerts.fraud`. Isso prova visualmente que é o Flink — não mais o processo Python — quem está produzindo esse resultado (o processo antigo está com zero réplicas).

### Passo 4 — Mostrar o Spark rodando o ETL

```bash
kubectl get scheduledsparkapplication -n processing
kubectl logs <pod-do-driver> -n processing | grep "silver-etl"
```

Mostrar a linha final do log (`Done. Silver table total rows: 3151276`) e explicar:

> "Esse job lê os dados brutos que chegaram via streaming, aplica a tokenização de dados sensíveis (PHI) e grava numa tabela versionada — formato Iceberg, com Nessie fazendo o papel de controle de versão, como um Git para tabelas. Isso roda automaticamente todo dia às 2h da manhã."

### Passo 5 — Ser transparente sobre o que ainda falta

> "O próximo job dessa cadeia, que calcula features agregadas para retreinar os modelos de ML, encontrou um problema real de performance quando um provider tem um volume de claims muito fora do padrão — que é exatamente o tipo de comportamento suspeito que o sistema foi desenhado para detectar. Já identificamos a causa exata e as opções de correção; é um ajuste de engenharia de dados, não um problema de arquitetura."

Mostrar transparência aqui constrói confiança: prova que o time entende profundamente o que está rodando, não só que "está verde".

### Glossário rápido (para apoiar a conversa com áreas não-técnicas)

| Termo | Explicação simples |
|---|---|
| **Keyed state** | A "memória de curto prazo" do Flink — por exemplo, "esse provider já fez 47 claims este mês", mantida automaticamente, sem precisar de um banco externo. |
| **Checkpoint** | Uma cópia de segurança automática do estado do job, tirada periodicamente, para recuperação rápida em caso de falha. |
| **Data lake / Iceberg / Nessie** | Um "banco de dados de arquivos" com controle de versão — permite consultar como os dados estavam em qualquer ponto no passado, útil para auditoria. |
| **Medallion (bronze/silver/gold)** | Camadas de qualidade crescente: bronze é o dado bruto, silver é o dado limpo/tokenizado, gold são as métricas agregadas prontas para os modelos de ML. |
| **Data skew** | Quando uma "chave" (ex.: um provider específico) concentra muito mais dados que as outras, sobrecarregando a tarefa que processa essa chave. |
