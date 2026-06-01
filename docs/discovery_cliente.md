# Discovery: Plataforma de Detecção de Fraudes em Saúde

**Versão:** 1.0  
**Público:** Reunião de discovery com o cliente  
**Contexto:** Mapeamento de requisitos para implantação de plataforma de detecção de fraudes baseada em ML em ambiente de saúde suplementar

---

## Como usar este documento

Cada seção traz **contexto de mercado** — o que empresas do setor geralmente fazem e por quê — seguido das perguntas concretas. O objetivo é que a conversa flua como consultoria, não como um formulário.

---

## 1. Governança e Times

### Contexto de mercado

Em operadoras de saúde suplementar, a responsabilidade pela detecção de fraudes historicamente ficou dentro das áreas de **Auditoria Médica** ou **Controle de Perdas**. Com a adoção de ML, surgiu um terceiro stakeholder: **Dados e Analytics**. O atrito entre esses três times é um dos principais motivos de falha em projetos de fraud analytics — o modelo é entregue, mas ninguém opera, ninguém treina de novo, e em seis meses ele deteriora em silêncio.

O mercado maduro separa três papéis claros:
- **Model Owner** (geralmente Data Science / ML Engineering): treina, versiona, monitora drift
- **Domain Expert** (Auditoria Médica): valida alertas, fornece feedback de ground truth
- **Platform Owner** (Engenharia de Dados / DevOps): garante que os dados chegam no tempo certo, com qualidade

### Perguntas

1. Quem hoje é responsável pelas regras de auditoria e detecção de fraudes? Esse time está dentro de Auditoria Médica, TI ou uma área de Dados?
2. Existe um time de Data Science ou ML Engineering constituído? Qual o tamanho e senioridade?
3. Como é a relação hoje entre o time de auditoria médica e o time de dados? Eles trabalham no mesmo projeto ou em silos separados?
4. Quem será o **Model Owner** — responsável por retreinar e monitorar os modelos em produção? Está definido?
5. Existe um time de Engenharia de Dados (ou Data Platform) responsável por pipelines e qualidade de dados?
6. Há previsão de contratação ou crescimento de time para suportar a plataforma pós-entrega?
7. Quem toma a decisão final quando um alerta de fraude é gerado — o sistema bloqueia automaticamente ou é sempre revisão humana?

---

## 2. Casos de Uso e Priorização

### Contexto de mercado

Operadoras brasileiras de saúde perdem entre **15% e 25% do faturamento** com fraudes, segundo estimativas da ANS e da FenaSaúde. Os padrões mais comuns são:

- **Upcoding / Cobranças indevidas**: procedimento cobrado diferente do realizado
- **Ghost billing**: cobrança por paciente ou procedimento que não aconteceu
- **Fragmentação de procedimentos** (unbundling): dividir um procedimento em vários para maximizar reembolso
- **Cumplicidade médico-beneficiário**: esquemas combinados entre prestador e paciente
- **Uso indevido de credencial**: terceiros usando carteirinha de beneficiário

Projetos que tentam atacar todos os casos de uso ao mesmo tempo invariavelmente falham. O padrão de sucesso no mercado é começar com **1 caso de uso de alto impacto e baixa complexidade operacional**, provar ROI, e expandir.

### Perguntas

1. Quais são os **tipos de fraude mais prevalentes** hoje, em volume e em valor financeiro?
2. Existe algum mapeamento recente (últimos 12 meses) de perdas por tipo de fraude?
3. **Curto prazo (0–6 meses):** Qual o caso de uso mínimo que, se entregue, já justifica o projeto internamente?
4. **Médio prazo (6–18 meses):** Quais capacidades adicionais são esperadas? Detecção em tempo real? Integração com sistema de auditoria?
5. **Longo prazo (18 meses+):** Existe visão de expandir para outros produtos (dental, hospitalar, farmácia)?
6. Hoje a detecção é **reativa** (investigação pós-pagamento) ou **preventiva** (bloqueio antes do pagamento)? Qual a meta?
7. Há algum caso de uso que **não deve ser automatizado** por decisão regulatória ou política interna?

---

## 3. Timeline e Entregas

### Contexto de mercado

Projetos de fraud analytics em operadoras de saúde têm um desafio particular de timing: a **sazonalidade da sinistralidade**. Picos de utilização (início de ano, pós-férias) geram volumes atípicos que podem confundir modelos treinados em períodos normais. Além disso, a ANS exige que mudanças em sistemas críticos passem por janelas de homologação.

Outro fator recorrente: o **ciclo de feedback do label** em saúde é longo. Uma fraude identificada hoje pode levar 90 a 180 dias para ser confirmada judicialmente e virar ground truth confiável para retreino.

### Perguntas

1. Existe um **prazo fixo** para entrega de alguma fase do projeto? O que motiva essa data (auditoria interna, ANS, resultado financeiro do ciclo)?
2. Quais são as **janelas de congelamento de sistemas** (change freeze) ao longo do ano?
3. Em quanto tempo, historicamente, uma fraude identificada vira um caso encerrado (confirmado ou refutado)? Isso afeta o ciclo de retreino dos modelos.
4. Existe um processo de **homologação e UAT** definido? Quem assina o aceite?
5. Há dependência de outros projetos em andamento (migração de ERP, novo sistema de beneficiários) que possam afetar o timeline?
6. Qual o processo de **go-live**? Existe período de operação paralela (modelo novo + regras antigas rodando em conjunto)?

---

## 4. Dados, Volumetrias e Integrações

### Contexto de mercado

Em plataformas de fraud detection para saúde, a qualidade dos dados é mais crítica do que a sofisticação dos modelos. Os problemas mais comuns que encontramos são:

- **Dados de TISS fragmentados**: a tabela TISS (padrão ANS para troca de informações) está frequentemente distribuída em sistemas legados com histórico incompleto
- **Latência de sinistro**: dependendo do prestador (clínica vs. hospital), o sinistro pode demorar dias ou semanas para chegar na operadora
- **Falta de padronização de CID/TUSS**: codificação inconsistente entre prestadores dificulta a criação de features de ML
- **Volume de eventos vs. volume de fraudes**: a proporção típica é de 0,1% a 2% de fraudes no total — isso exige técnicas específicas de balanceamento e avaliação

### Perguntas

#### Volumetria
1. Quantas **guias/sinistros por mês** são processados hoje (em média e no pico)?
2. Qual o volume de **beneficiários ativos**?
3. Quantos **prestadores credenciados** existem na rede?
4. Qual o volume de **arquivos de TISS** recebidos por dia? Em qual formato (XML, JSON, flat file)?
5. Qual o crescimento esperado do volume nos próximos 2 anos?

#### Latência e Tempo Real
6. Existe necessidade de análise **em tempo real** (antes da autorização do procedimento) ou o foco é em lote (batch pós-faturamento)?
7. Qual a **latência máxima aceitável** para um alerta de fraude chegar ao auditor médico?
8. Os sistemas transacionais publicam eventos (Kafka, filas) ou é necessário polling no banco de dados?

#### Storage e Retenção
9. Qual o **período de histórico de sinistros** disponível para treino inicial dos modelos?
10. Existe política de retenção de dados definida? Por quanto tempo os dados de sinistros devem ser armazenados na plataforma de detecção?
11. Há dados **externos** relevantes disponíveis (CRM de prestadores, dados de geolocalização de clínicas, histórico de sanções da ANS)?

#### Qualidade e Governança
12. Existe um **catálogo de dados** ou data dictionary dos sistemas de origem?
13. Como é feita hoje a **deduplicação** de sinistros (mesmo procedimento cobrado duas vezes por sistemas diferentes)?
14. Quem é o **Data Owner** dos dados de sinistros? Existe processo de aprovação para acesso?

---

## 5. Infraestrutura e Arquitetura

### Contexto de mercado

Operadoras de médio e grande porte no Brasil têm dividido sua estratégia de infraestrutura em três padrões principais:

- **Cloud-first**: workloads de analytics em cloud pública (AWS, GCP, Azure), com dados sensíveis em nuvem com certificação de compliance
- **Hybrid**: core transacional on-premises, analytics e ML em cloud
- **On-premises**: mais comum em operadoras com restrições regulatórias internas ou que passaram por auditorias de segurança recentes

A tendência do mercado é cloud ou hybrid, mas a LGPD e as diretrizes internas de segurança frequentemente criam restrições que precisam ser mapeadas antes de qualquer decisão arquitetural.

### Perguntas

1. Qual é a estratégia de infraestrutura atual — **on-premises, cloud ou híbrido**?
2. Se cloud: qual provedor(es)? Existe contrato ativo? Há restrição para uso de provedores específicos?
3. Existe política de **residência de dados** que exija que dados de beneficiários permaneçam no Brasil?
4. Qual é a política de **segurança de acesso** a dados de saúde — quem pode acessar o quê, e como isso é auditado?
5. Existe um ambiente de **Sandbox/Homologação** separado de Produção para dados sensíveis?
6. Há **SLA de disponibilidade** definido para a plataforma de detecção? (ex: 99,9% uptime, RTO/RPO definidos)
7. Qual a capacidade do time de infraestrutura para operar Kubernetes e plataformas de streaming (Kafka/Redpanda)?

---

## 6. Compliance, Regulatório e LGPD

### Contexto de mercado

A saúde suplementar no Brasil opera sob regulação da **ANS** e, desde 2020, sob a **LGPD**. Dados de saúde são classificados como **dados sensíveis** pela LGPD (Art. 11), o que exige tratamento diferenciado: consentimento explícito ou base legal específica para uso em analytics e ML.

Na prática, isso significa que:
- **Anonimização ou pseudonimização** é frequentemente exigida para dados usados em treino de modelos
- O **DPO (Encarregado de Dados)** precisa ser envolvido cedo no projeto
- **Auditorias de uso de dados** precisam ser rastreáveis

Além disso, a ANS possui normas específicas sobre bloqueio de procedimentos (RN 566 e correlatas) que afetam como alertas de fraude podem ser operacionalizados.

### Perguntas

1. O projeto já passou pelo **DPO e jurídico**? Existe uma base legal definida para uso dos dados em ML?
2. Como é feita hoje a **anonimização ou pseudonimização** de dados de beneficiários para uso em analytics?
3. Existe exigência de **explicabilidade dos modelos** (modelo precisa justificar cada alerta)? Isso é comum em ambientes regulados.
4. A plataforma precisará gerar **logs de auditoria** de cada decisão do modelo para compliance?
5. Existe alguma restrição da ANS sobre **bloqueio automático** de procedimentos baseado em score de risco?
6. Há histórico de auditoria externa (ANS ou auditores independentes) sobre o processo de detecção de fraudes atual?

---

## 7. Sucesso e ROI

### Contexto de mercado

Projetos de fraud analytics são frequentemente aprovados com base em estimativas de ROI, mas raramente têm métricas de sucesso claramente definidas antes do início. O risco é que, sem um baseline estabelecido, o projeto seja avaliado por critérios subjetivos.

O mercado usa tipicamente dois tipos de métricas:
- **Financeiras**: valor recuperado ou evitado por período, taxa de sinistralidade antes vs. depois
- **Operacionais**: precision/recall dos modelos, tempo médio de investigação por caso, volume de falsos positivos (cada falso positivo tem custo de auditoria humana)

Um alerta: **falsos positivos têm custo relacional**. Bloquear um procedimento legítimo de um prestador credenciado gera atrito e risco de perda de rede.

### Perguntas

1. Qual é o **baseline atual** de detecção? Quantos casos de fraude são identificados por mês hoje, e com qual método?
2. Existe uma **meta financeira** para o projeto (ex: reduzir sinistralidade em X%, recuperar R$ Y por mês)?
3. Qual o **custo atual** de uma investigação manual de fraude (horas de auditor médico por caso)?
4. Qual a tolerância a **falsos positivos**? Quantos alertas incorretos por mês o time de auditoria consegue absorver?
5. Como o sucesso do projeto será **reportado internamente** — para diretoria, para o conselho?
6. Existe um **período de avaliação** formal definido (ex: revisão em 6 meses com go/no-go)?

---

## 8. Próximos Passos Sugeridos

Ao final da reunião, sugerir:

| Ação | Responsável | Prazo sugerido |
|---|---|---|
| Compartilhar amostra anonimizada de sinistros (3–6 meses) | Cliente | 2 semanas |
| Mapear sistemas de origem e contatos técnicos | Cliente + TI | 1 semana |
| Alinhar DPO e jurídico sobre base legal | Cliente | 2 semanas |
| Definir caso de uso prioritário (Fase 1) | Conjunto | Reunião de follow-up |
| Apresentar proposta de arquitetura de referência | Consultor | 3 semanas |

---

*Documento de uso interno — discovery confidencial*
