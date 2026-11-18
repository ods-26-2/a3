# Testes automatizados — A3 (Monitoramento de Eventos)

Este documento descreve os testes existentes em `safety_app/tests/`, organizados por responsabilidade. A suíte contém **95 funções `test_*` distribuídas em 12 arquivos**, nesta versão (89 testes anteriores e 6 testes adicionais de conectividade). A contagem é feita pelas funções de teste existentes no código; não representa 89 cenários de integração real.

## Como executar

A partir de `safety_app/`, com as dependências de desenvolvimento instaladas:

```bash
python -m pytest tests/ -v
```

Para executar apenas um arquivo:

```bash
python -m pytest tests/test_a3_decision_matrix.py -v
```

> A execução pode exigir dependências do projeto. A suíte foi concebida para rodar localmente, sem exigir acesso ao broker Kafka ou à Jetson. O resultado dos testes locais **não equivale à homologação com os componentes externos**.

## Grupo 1 — Regras de negócio e matriz de decisão (11 testes)

**Arquivo:** `test_a3_decision_matrix.py`

Verifica classificação contextual de risco, regras por zona e movimento, escalonamento por recorrência e ocupação, tratamento de incerteza e baixa confiança, desempate/validação de regras, preservação de `event_id`, atualização e encerramento do mesmo evento, prevenção de duplicações e separação entre metadados de entidade e eventos. Também verifica que respostas ambíguas do S3 não são consideradas confirmações.

**Escopo:** lógica interna do A3, usando entradas controladas; não comprova o formato emitido pelo S2 real.

## Grupo 2 — Contratos, zonas e integração interna (8 testes)

**Arquivos:** `test_a3_contracts.py` (4), `test_a3_integration.py` (4).

Verifica separação entre regras temporais do S2 e políticas do A3, validação de regras e requisitos de roteamento, registro/publicação imutável de artefatos, IDs estáveis, conversões, rejeição de polígonos inválidos e inicialização opcional/simulável do Kafka.

**Escopo:** validações locais e construção de contratos pelo A3; não comprova que S1 ou S2 consumam os artefatos publicados.

## Grupo 3 — Simulação integrada e dados de teste (22 testes)

**Arquivos:** `test_a3_integrated_simulator.py` (16), `test_a3_mock_validation.py` (6).

Verifica execução offline dos sete cenários do simulador integrado; continuidade do `event_id` nas fases de um evento; decisões calculadas pela matriz em vez de resultados pré-definidos; correlação simulada com S3; consultas simuladas de clipes S4; cenários sem violações; isolamento dos bancos SQLite; envelopes/identificadores esperados; tratamento de eventos incertos; rejeição de cenários inválidos; ausência de consumidores Kafka em segundo plano. Os mocks adicionais verificam cinco cenários (três de violação e dois normais), correspondência de IDs e ausência de vídeo no payload Kafka.

**Escopo:** testes de integração **simulada**. Os mocks não substituem mensagens capturadas dos produtores reais.

## Grupo 4 — Alertas S3 e interfaces visuais B6 (31 testes)

**Arquivos:** `test_a3_s3_operational.py` (21), `test_a3_b6_component_consumption.py` (10).

**S3:** verifica destinatários/canais configuráveis, versionamento imutável, validação de comandos, correlação comando–resposta–evento, estados aceitos/rejeitados/desconhecidos, consulta e paginação do histórico, reconciliação sem reenvio, persistência dos eventos recebidos, timeouts e ausência de confirmação fictícia.

**B6:** verifica adaptação de gravidades/estados S3 para os componentes visuais, escape de conteúdo, rejeição de dados inválidos, integridade dos módulos compilados de B6, uso de APIs públicas, contratos de ações do operador e tratamento de erros/timeouts.

**Escopo:** lógica de integração e pontes locais; não executa o serviço S3 real nem homologa as interfaces B6 contra a versão instalada em outro ambiente.

## Grupo 5 — Evidências e clipes S4 (5 testes)

**Arquivos:** `test_a3_s4_clips.py` (4), `test_a3_kafka_rpc.py` (1).

Verifica envelope de consulta, rejeição de consultas ambíguas, exigência de endpoint HTTP para reprodução, comportamento com broker desabilitado e correlação de respostas Kafka com o `message_id` original.

**Escopo:** não comprova execução do serviço S4, existência de clipes nem reprodução de mídia real.

## Grupo 6 — Configuração Kafka/Jetson e regressões (12 testes)

**Arquivos:** `test_a3_jetson_kafka_config.py` (7), `test_a3_regressions.py` (5).

**Kafka/Jetson:** verifica endereço padrão `200.17.66.56:9092`, sobrescrita/desativação por variável de ambiente, inicialização explícita e funcionamento offline, inclusive na consulta S4.

**Regressões:** verifica consistência dos campos de confiança esperados, imutabilidade/versionamento de regras, comportamento de eventos `ENDED`, atualizações incertas e rejeição de estados inválidos no replay.

**Escopo:** não estabelece conexão real com a Jetson ou com o Kafka.

## Resumo da cobertura

| Grupo | Arquivos | Testes |
|---|---:|---:|
| 1. Matriz de decisão | 1 | 11 |
| 2. Contratos, zonas e integração interna | 2 | 8 |
| 3. Simulação integrada e mocks | 2 | 22 |
| 4. S3 e B6 | 2 | 31 |
| 5. S4 e RPC Kafka | 2 | 5 |
| 6. Kafka/Jetson e regressões | 2 | 12 |
| **Subtotal — grupos 1 a 6** | **11** | **89** |

---

## Grupo 7 — Testes futuros (a implementar)

**Esta seção é um planejamento. Nenhum item abaixo deve ser interpretado como teste existente ou aprovado.** Os testes deverão validar o A3 como **consumidor/cliente** dos componentes externos, sem copiar suas responsabilidades ou alterar seus repositórios.

| Prioridade | Teste proposto | Evidência necessária | Situação |
|---|---|---|---|
| Alta | Contrato S2 → A3 (`ods.s2.event` e `ods.s2.entity`) | Mensagens reais/fixtures extraídas do produtor S2, com schemas, IDs, fases, estados e tipos | Pendente |
| Alta | Publicação de configurações A3 → S1 | Contrato oficial de zonas e verificação de aceitação pelo consumidor S1 | Pendente |
| Alta | Comandos e retornos A3 ↔ S3 | Fixtures e execução do serviço S3 para comandos, respostas, eventos e histórico | Pendente |
| Alta | Consulta de evidências A3 ↔ S4 | Serviço S4 operacional, descritores reais e URL de mídia acessível | Pendente |
| Média | Compatibilidade das interfaces B6 | Build/API exportada de B6, montagem dos componentes e callbacks reais | Pendente |
| Alta | Integração Kafka na rede da UFSJ | Broker acessível, tópicos provisionados, consumidores/produtores em execução | Pendente |
| Média | Reinício, mensagens fora de ordem e duplicadas | Reexecução com histórico e sequência controlada de mensagens | Pendente |
| Média | Operação com múltiplas sessões do Streamlit | Duas ou mais sessões recebendo e apresentando eventos sem perda indevida | Pendente |
| Alta | Suíte oficial de integração | Casos de teste padronizados fornecidos pela equipe de integração | Pendente |

### Critérios para implementar os próximos testes

1. **Fonte independente:** obter os contratos e exemplos diretamente do repositório do componente produtor/consumidor, ou da suíte oficial; não definir fixtures apenas pelo que o A3 já aceita.
2. **Falha informativa:** registrar explicitamente qual tópico, campo, tipo, valor ou operação diverge.
3. **Responsabilidades preservadas:** corrigir apenas o A3 quando o defeito for dele; documentar divergências dos outros componentes sem reimplementá-los.
4. **Separar resultados:** distinguir `APROVADO`, `REPROVADO` e `BLOQUEADO` por indisponibilidade de infraestrutura ou dependências.
5. **Reprodutibilidade:** registrar commit/versão do componente externo, comando de execução e evidências relevantes.

### Organização sugerida para testes futuros

Manter os testes atuais intactos e criar, quando necessário, um diretório `tests/contract/` para testes de contrato independentes e `tests/e2e/` para testes operacionais. Esses diretórios **ainda não fazem parte da suíte implementada**; devem ser criados junto com os respectivos testes e sua configuração de execução.

## Limites da conclusão

A aprovação dos 89 testes indica que as verificações locais da suíte passaram. **Não prova** compatibilidade integral com os repositórios atuais de S1/S2/S3/S4/B6, com a suíte oficial de integração ou com a infraestrutura Kafka/Jetson. Essas verificações permanecem explicitamente pendentes no Grupo 7.


## Grupo 8 — Diagnóstico público de conexão Kafka (6 testes)

**Arquivo:** `test_a3_public_connectivity.py`.

Verifica, com sockets simulados, a configuração do IP público `200.17.66.56`, as portas `9092` e `9093`, a distinção entre conectividade TCP e disponibilidade de metadados Kafka, a não execução de consultas Kafka quando o TCP falha e a validação de portas.

**Limite:** os testes automatizados não realizam conexão real. A verificação real é manual pela página **Teste de conexão Kafka**, sem S1/S2/S3/S4. Os 89 testes anteriores continuam preservados; a suíte passa a ter 95 testes.

**Total geral com o Grupo 8:** 12 arquivos e 95 testes.
