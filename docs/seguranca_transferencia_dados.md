# Transferência Segura de Dados para a Plataforma de Fraudes

**Versão:** 1.0  
**Contexto:** Os bancos de dados do cliente estão distribuídos em múltiplas clouds e on-premises. O sistema de detecção de fraudes reside na Akamai Cloud. Este documento define como transferir dados de forma segura sem expor os bancos de produção publicamente.

---

## Princípio Central: Nenhuma Porta de Banco de Dados Exposta à Internet

O erro mais comum em integrações multi-cloud é abrir portas de banco de dados (5432, 3306, 1521, 1433) com ACLs por IP. Isso cria superfície de ataque e viola controles LGPD/HIPAA.

**A regra:** Toda conexão é **iniciada de dentro da rede de origem**, nunca de fora. Os conectores vivem dentro do perímetro da fonte e fazem push para a plataforma de fraudes.

```
ERRADO:  Akamai → (internet) → DB de produção (porta aberta)
CORRETO: Agente de captura (dentro da rede origem) → (túnel criptografado) → Redpanda na Akamai
```

---

## 1. Topologia de Conectividade por Tipo de Origem

### 1.1 On-Premises → Akamai Cloud

```
┌─────────────────────────────────────────────────────────────────┐
│                    REDE DO CLIENTE (On-Premises)                 │
│                                                                  │
│  ┌──────────────┐    ┌─────────────────────────────────────┐    │
│  │  DB Produção │    │  Zona Isolada (DMZ de Integração)   │    │
│  │  PostgreSQL  │───►│                                     │    │
│  │  Oracle      │    │  ┌────────────┐  ┌───────────────┐  │    │
│  │  SQL Server  │    │  │  Debezium  │  │  Redpanda     │  │    │
│  └──────────────┘    │  │  (CDC)     │  │  Connect      │  │    │
│                      │  │            │  │  (file/batch) │  │    │
│  Replicação          │  └─────┬──────┘  └──────┬────────┘  │    │
│  lógica somente      │        │                │           │    │
│  (read-only)         └────────┼────────────────┼───────────┘    │
│                               │                │                 │
│                        ┌──────▼────────────────▼──────┐         │
│                        │   VPN Gateway / Site-to-Site  │         │
│                        │   (IPSec IKEv2 / WireGuard)   │         │
│                        └──────────────┬────────────────┘         │
└──────────────────────────────────────┼─────────────────────────┘
                                        │ Túnel criptografado
                                        │ TLS 1.3 over VPN
┌──────────────────────────────────────┼─────────────────────────┐
│                    AKAMAI CLOUD       │                          │
│                                       ▼                          │
│                        ┌─────────────────────────┐              │
│                        │   Redpanda Cluster (LKE) │              │
│                        │   (listener interno VPC) │              │
│                        └─────────────────────────┘              │
└─────────────────────────────────────────────────────────────────┘
```

**Componentes necessários no cliente:**
- **Debezium** rodando em VM isolada na DMZ — acessa o DB via replicação lógica (slot de replicação, não query direta)
- **VPN Site-to-Site** (WireGuard ou IPSec) entre o datacenter do cliente e a VPC na Akamai
- Redpanda exposto **apenas no endereço privado** da VPC, nunca em IP público

**Configuração do banco de origem (PostgreSQL como exemplo):**
```sql
-- Habilitar replicação lógica (sem expor porta)
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 4;

-- Usuário mínimo para CDC — somente leitura de replicação
CREATE USER debezium_user REPLICATION LOGIN PASSWORD '...';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;

-- Publicação somente das tabelas necessárias (não tabelas com dados sensíveis desnecessários)
CREATE PUBLICATION fraude_pub FOR TABLE sinistros, prestadores, beneficiarios;
```

---

### 1.2 Cloud A (AWS/GCP/Azure) → Akamai Cloud

Para fontes em outras clouds, o padrão é o mesmo: agente de dentro empurra para fora.

```
┌──────────────────────────────────────┐
│         AWS / GCP / Azure VPC        │
│                                      │
│  ┌────────────┐   ┌───────────────┐  │
│  │  RDS/Cloud │   │  Debezium /   │  │
│  │  SQL       │──►│  Kafka Connect│  │
│  └────────────┘   └───────┬───────┘  │
│                           │          │
│              ┌────────────▼───────┐  │
│              │  VPN ou            │  │
│              │  Private Link /    │  │
│              │  Dedicated Connect │  │
│              └────────────┬───────┘  │
└──────────────────────────┼──────────┘
                            │ Criptografado
┌──────────────────────────┼──────────┐
│  AKAMAI CLOUD             ▼          │
│              ┌─────────────────────┐ │
│              │  Redpanda (interno) │ │
│              └─────────────────────┘ │
└──────────────────────────────────────┘
```

**Opções de conectividade privada por cloud:**

| Cloud Origem | Opção Preferida             | Alternativa          |
|--------------|-----------------------------|----------------------|
| AWS          | AWS Transit Gateway + VPN   | PrivateLink + túnel  |
| GCP          | Cloud VPN (HA) + BGP        | Interconnect         |
| Azure        | VPN Gateway + ExpressRoute  | Azure Private Link   |
| On-Premises  | WireGuard Site-to-Site      | IPSec IKEv2          |

---

### 1.3 Padrão para Dados em Batch / Arquivos (HL7, EDI 837, FHIR)

Para arquivos que não vêm de banco de dados (laudos, XMLs de TISS, EDI):

```
Origem (SFTP interno ou bucket privado)
        │
        │  Agente de coleta dentro da rede origem
        ▼
  Redpanda Connect (FileStream / S3 Source Connector)
        │  via VPN / túnel TLS
        ▼
  Redpanda na Akamai (tópico raw.files)
        │
        ▼
  Data Lake (Linode Object Storage — bucket privado)
```

**Nunca usar SFTP público.** Se o cliente já usa SFTP, o agente coleta de dentro e faz push via tunnel.

---

## 2. Mascaramento e Tokenização em Trânsito

Dados PHI (nome, CPF, número de carteirinha) devem ser tokenizados **antes de sair da rede de origem**. O sistema de fraudes trabalha com tokens, não com dados reais.

```
┌────────────────────────────────────────────────────────────────┐
│           PIPELINE DE MASCARAMENTO (dentro da rede origem)     │
│                                                                 │
│  DB Original          Debezium           Tokenizador           │
│  ┌─────────┐          ┌────────┐         ┌──────────────────┐  │
│  │CPF: 123 │──CDC──►  │CPF:123 │──────►  │CPF → TKN-a7f3b2 │  │
│  │Nome: Ana│          │Nome:Ana│         │Nome → TKN-9c2e11 │  │
│  └─────────┘          └────────┘         └────────┬─────────┘  │
│                                                   │             │
│                                          Vault (chave de        │
│                                          mapeamento) fica       │
│                                          on-premises            │
└───────────────────────────────────────────────────┼────────────┘
                                                     │
                                          ┌──────────▼─────────┐
                                          │ Redpanda (Akamai)  │
                                          │ CPF: TKN-a7f3b2    │
                                          │ (nunca o dado real) │
                                          └────────────────────┘
```

**Vault de tokenização fica on-premises.** A plataforma de fraudes trabalha com tokens. Quando uma investigação humana precisa do dado real, o investigador acessa o Vault do cliente via portal seguro — o dado nunca migra para a cloud.

---

## 3. Controles de Segurança em Camadas

### 3.1 Autenticação e Autorização

```
Componente          Mecanismo                     Notas
──────────────────  ──────────────────────────────────────────────────────
Debezium → DB       Service account dedicado       Mínimo: REPLICATION + SELECT
                    Senha rotacionada (Vault)       Sem acesso a tabelas de pagamento

Debezium → Redpanda mTLS (certificados)            Cada conector tem cert próprio
                    SASL/SCRAM-SHA-512             Sem autenticação por IP

VPN túnel           IKEv2 com certificados PKI     Sem PSK (pre-shared key) simples
                    Rotação automática de chaves

Redpanda ACL        Por tópico e por serviço       Conector só escreve em raw.*
                                                   Flink só lê de raw.*, escreve em processed.*

LKE (Kubernetes)    RBAC + Network Policies        Pods não falam entre si sem policy
                    Service Accounts por serviço
```

### 3.2 Criptografia

| Caminho                        | Criptografia em Trânsito | Criptografia em Repouso     |
|--------------------------------|--------------------------|-----------------------------|
| DB → Debezium                  | TLS (conexão local ou SSL DB) | N/A (mesma rede)       |
| Debezium → Redpanda (via VPN)  | TLS 1.3 dentro do túnel IPSec | N/A                    |
| Redpanda → Data Lake           | TLS interno (LKE)        | AES-256 (Object Storage)   |
| Data Lake → ML Training        | TLS (interno LKE)        | AES-256 (Object Storage)   |
| Backup / snapshots             | N/A                      | AES-256 com chave própria  |

### 3.3 Segmentação de Rede (Network Policies no LKE)

```yaml
# Redpanda só aceita conexões de namespaces autorizados
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: redpanda-ingress-policy
  namespace: streaming
spec:
  podSelector:
    matchLabels:
      app: redpanda
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          role: ingestion     # Debezium, Connect
    - namespaceSelector:
        matchLabels:
          role: processing    # Flink, Spark
    ports:
    - port: 9092   # Kafka API
    - port: 8081   # Schema Registry
  # Sem ingress de fora do cluster
```

---

## 4. Auditoria e Rastreabilidade

Todo evento de transferência de dados deve ser auditável para fins de LGPD e compliance interno.

**O que logar:**
- Cada mensagem publicada no Redpanda inclui metadados de origem: `source_system`, `source_env`, `extracted_at`, `connector_id`
- Debezium emite o LSN (Log Sequence Number) do PostgreSQL — rastreabilidade até o registro original
- Logs do conector persistidos no Elasticsearch / Loki com retenção mínima de 1 ano

**Schema de metadados de auditoria (Avro/JSON):**
```json
{
  "event_id": "uuid-v4",
  "source_system": "core_claims_aws",
  "source_env": "production",
  "connector_id": "debezium-claims-v1",
  "extracted_at": "2026-06-10T14:23:00Z",
  "transport": "vpn-site-to-site",
  "encryption": "tls1.3",
  "record_count": 1,
  "schema_version": "claims.v3",
  "phi_tokenized": true
}
```

---

## 5. Checklist de Implantação

### Fase 0 — Pré-requisito (responsabilidade do cliente)
- [ ] VPN Site-to-Site provisionada entre cada ambiente de origem e a VPC da Akamai
- [ ] Usuário de replicação criado em cada banco de dados de origem (mínimo privilégio)
- [ ] Firewall liberado apenas para o IP do servidor Debezium na DMZ
- [ ] Vault (HashiCorp ou nativo da cloud) configurado para gerenciar credenciais dos conectores

### Fase 1 — Conectores de CDC
- [ ] Debezium implantado na DMZ de integração
- [ ] Replicação lógica testada com tabelas não-produtivas
- [ ] Schema Registry da Akamai alcançável via VPN (porta 8081 interna)
- [ ] mTLS entre Debezium e Redpanda testado

### Fase 2 — Tokenização
- [ ] Vault de tokenização implantado on-premises
- [ ] Plugin de mascaramento integrado ao pipeline Debezium
- [ ] Validação: Redpanda não contém CPF, nome ou número de carteirinha em texto claro

### Fase 3 — Validação de Segurança
- [ ] Teste de penetração nos endpoints de VPN
- [ ] Auditoria dos ACLs do Redpanda (apenas serviços autorizados produzem/consomem por tópico)
- [ ] Network Policy validada: nenhum pod no LKE é alcançável externamente sem LoadBalancer explícito
- [ ] Rotação de credenciais testada sem downtime (Vault dynamic secrets)

### Fase 4 — LGPD e Compliance
- [ ] DPA (Data Processing Agreement) assinado entre cliente e Akamai
- [ ] Mapeamento de fluxo de dados documentado (quais dados, de onde, para onde, com qual finalidade)
- [ ] Procedimento de exclusão de dados (right to be forgotten) documentado e testado
- [ ] Retenção de logs de auditoria configurada (mínimo 1 ano)

---

## 6. O Que Nunca Fazer

| Prática Proibida                              | Risco                                          |
|-----------------------------------------------|------------------------------------------------|
| Abrir porta 5432/3306 com ACL por IP público  | Exposição do banco a scanning/brute-force      |
| Usar credentials hardcoded nos conectores     | Credenciais em logs, Git history               |
| Replicar dados sem tokenização de PHI         | Violação LGPD Art. 46 — medidas de segurança   |
| VPN com PSK (pre-shared key) estático         | Comprometimento de longo prazo se chave vazar  |
| Dar ao conector acesso de escrita no banco    | CDC requer apenas replicação, não escrita       |
| Expor Schema Registry publicamente            | Engenharia reversa dos schemas de dados        |
| Usar S3/Object Storage com bucket público     | Dados brutos acessíveis a qualquer um          |
