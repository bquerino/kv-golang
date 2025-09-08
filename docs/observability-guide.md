# 🚀 KV-Store Observability Stack - Guia Completo

Este guia mostra como executar e monitorar completamente o sistema KV-Store distribuído com observabilidade completa.

## 📋 Pré-requisitos

- Docker e Docker Compose
- Go 1.23.2+
- Bash (Linux/macOS) ou Git Bash (Windows)
- Portas disponíveis: 8080-8083, 9090-9093, 3000

## 🚀 Início Rápido

### 1. **Iniciar Stack Completa**

```bash
# Executar script principal de orquestração
./scripts/run-observability-stack.sh
```

OU executar manualmente:

```bash
# Limpar ambiente anterior
docker-compose -f docker-compose-observability.yml down -v

# Atualizar dependências
go mod tidy

# Iniciar stack completa
docker-compose -f docker-compose-observability.yml up --build -d

# Aguardar serviços ficarem disponíveis (~30s)
```

### 2. **Verificar Status dos Serviços**

```bash
docker-compose -f docker-compose-observability.yml ps
```

Serviços esperados:
- ✅ **node1, node2, node3**: KV-Store nodes
- ✅ **prometheus**: Coleta de métricas (port 9090)
- ✅ **grafana**: Dashboards (port 3000)
- ✅ **alertmanager**: Alertas (port 9093)

## 📊 Acessar Dashboards

### **Grafana Dashboard**
- URL: http://localhost:3000
- Login: `admin` / `admin`
- Dashboard: "KV-Store Performance" (auto-carregado)

### **Prometheus Metrics**
- URL: http://localhost:9090
- Targets: http://localhost:9090/targets
- Queries: http://localhost:9090/graph

### **AlertManager**
- URL: http://localhost:9093
- Alertas ativos: http://localhost:9093/#/alerts

## 🧪 Executar Testes

### **1. Teste de Carga K6**
```bash
# Via script
./scripts/run-observability-stack.sh
# Escolher opção 2

# Ou diretamente
docker-compose -f docker-compose-observability.yml run --rm k6 run /scripts/load-test.js
```

**Métricas coletadas:**
- Latência P50/P95/P99
- Throughput (RPS)
- Testes de consistência (read-your-writes, monotonic reads)
- Medição de staleness
- Tempo de convergência

### **2. Teste de Failover K6**
```bash
# Via script
./scripts/run-observability-stack.sh
# Escolher opção 3

# Ou diretamente
docker-compose -f docker-compose-observability.yml run --rm k6 run /scripts/failover-test.js
```

**Cenários testados:**
- Eleição de líder
- Recuperação de nó
- Split-brain detection
- Data loss measurement
- Disponibilidade durante falhas

### **3. Teste Manual de Consistência**
```bash
# Via script
./scripts/run-observability-stack.sh
# Escolher opção 4

# Ou diretamente
./scripts/test-consistency-checks.sh
```

**Verificações:**
- ✅ Read-Your-Writes Consistency
- ✅ Eventual Consistency
- ✅ Monotonic Reads
- ✅ Staleness Test (< 5s)
- ✅ Conflict Detection & Resolution

## 📈 Métricas Principais

### **Latência (Performance)**
```promql
# P50 latência requests
quantile(0.5, kvstore_request_duration_seconds)

# P95 latência requests
quantile(0.95, kvstore_request_duration_seconds)

# P99 latência requests
quantile(0.99, kvstore_request_duration_seconds)
```

### **Throughput**
```promql
# Requests por segundo
rate(kvstore_requests_total[1m])

# Total de requests
sum(kvstore_requests_total)
```

### **Replicação**
```promql
# Latência de replicação P95
quantile(0.95, kvstore_replication_latency_seconds)

# Taxa de falhas de replicação
rate(kvstore_replication_failures_total[5m])

# Replicações bem-sucedidas
rate(kvstore_replication_success_total[1m])
```

### **Consistência**
```promql
# Violações de consistência
increase(kvstore_consistency_violations_total[5m])

# Taxa de conflitos
rate(kvstore_conflicts_total[5m])

# Tempo de resolução de conflitos
kvstore_conflict_resolution_duration_seconds
```

### **Saúde do Cluster**
```promql
# Nós ativos
sum(kvstore_node_status)

# Líder atual
kvstore_is_leader == 1

# Eleições de líder
increase(kvstore_elections_total[1h])
```

## 🔧 Simulação de Falhas

### **Parar um Nó**
```bash
# Via script interativo
./scripts/run-observability-stack.sh
# Escolher opção 8

# Ou diretamente
docker-compose -f docker-compose-observability.yml stop node2
```

### **Reiniciar um Nó**
```bash
# Via script interativo
./scripts/run-observability-stack.sh
# Escolher opção 9

# Ou diretamente
docker-compose -f docker-compose-observability.yml start node2
```

### **Split-Brain Simulation**
```bash
# Isolar node1 da rede
docker network disconnect kv-golang_kvstore-network kv-golang_node1_1

# Aguardar eleição (~10s)
# Reconectar
docker network connect kv-golang_kvstore-network kv-golang_node1_1
```

## 📋 Geração de Relatórios

### **Relatório Completo**
```bash
# Via script
./scripts/run-observability-stack.sh
# Escolher opção 10

# Ou diretamente
./scripts/generate-test-report.sh
```

**Relatório inclui:**
- 📊 Métricas Prometheus snapshot
- 📋 Logs de todos os nós
- 🧪 Resultados dos testes K6
- 💻 Uso de recursos do sistema
- 📝 Resumo executivo
- 📦 Arquivo comprimido para compartilhamento

## 🚨 Alertas Configurados

### **Alertas de Performance**
- **HighRequestLatency**: P95 > 1.0s por 1min
- **HighReplicationLatency**: P95 replication > 2.0s por 1min

### **Alertas de Confiabilidade**
- **ReplicationFailures**: >10 falhas em 5min
- **ConsistencyViolations**: >5 violações em 5min
- **NodeDown**: Nó indisponível por 30s

### **Alertas de Cluster**
- **NoLeader**: Sem líder por 30s
- **MultipleLeaders**: Split-brain detectado
- **HighConflictRate**: >0.1 conflitos/s

## 🔍 Troubleshooting

### **Serviços não iniciam**
```bash
# Verificar logs
docker-compose -f docker-compose-observability.yml logs

# Verificar portas em uso
netstat -tulpn | grep -E ':(808[0-9]|909[0-9]|300[0-9])'

# Limpar ambiente
docker-compose -f docker-compose-observability.yml down -v
docker system prune -f
```

### **Métricas não aparecem**
```bash
# Verificar targets Prometheus
curl http://localhost:9090/api/v1/targets

# Verificar endpoints de métricas dos nós
curl http://localhost:9091/metrics
curl http://localhost:9092/metrics
curl http://localhost:9093/metrics
```

### **Testes K6 falham**
```bash
# Verificar se nós estão respondendo
curl http://localhost:8080/health
curl http://localhost:8081/health
curl http://localhost:8082/health

# Verificar logs dos nós
docker-compose -f docker-compose-observability.yml logs node1 node2 node3
```

### **Dashboard Grafana vazio**
1. Verificar data source Prometheus em Grafana
2. Verificar se métricas estão sendo coletadas: http://localhost:9090/graph
3. Reimportar dashboard: `grafana/dashboards/kvstore-dashboard.json`

## 📚 Estrutura de Arquivos

```
📁 observability/
├── 📄 docker-compose-observability.yml    # Stack completa
├── 📄 prometheus.yml                       # Config Prometheus
├── 📄 alert_rules.yml                      # Regras de alerta
├── 📄 alertmanager.yml                     # Config AlertManager
├── 📁 grafana/dashboards/
│   └── 📄 kvstore-dashboard.json          # Dashboard customizado
├── 📁 k6/
│   ├── 📄 load-test.js                    # Teste de carga
│   └── 📄 failover-test.js                # Teste de failover
└── 📁 scripts/
    ├── 📄 run-observability-stack.sh      # Script principal
    ├── 📄 test-consistency-checks.sh      # Testes manuais
    └── 📄 generate-test-report.sh         # Geração de relatórios
```

## 🎯 Casos de Uso

### **1. Desenvolvimento Local**
```bash
# Iniciar stack mínima
docker-compose up --build -d

# Executar testes rápidos
./scripts/test-consistency-checks.sh
```

### **2. Teste de Performance**
```bash
# Iniciar stack completa
./scripts/run-observability-stack.sh

# Executar load test
# Opção 2 no menu

# Analisar resultados no Grafana
# http://localhost:3000
```

### **3. Teste de Resiliência**
```bash
# Iniciar stack
./scripts/run-observability-stack.sh

# Simular falhas
# Opções 8-9 no menu

# Executar failover test
# Opção 3 no menu
```

### **4. Análise Completa**
```bash
# Executar todos os testes
./scripts/run-observability-stack.sh

# Gerar relatório completo
# Opção 10 no menu

# Analisar relatório
cat reports/*/executive_summary.md
```

## 🌟 Features Implementadas

### ✅ **Observabilidade Completa**
- Métricas Prometheus customizadas
- Dashboards Grafana específicos
- Alertas em tempo real
- Logs estruturados

### ✅ **Testes Avançados**
- Load testing com K6
- Testes de consistência automáticos
- Simulação de falhas
- Medição de convergência

### ✅ **Relatórios Profissionais**
- Análise de performance automatizada
- Resumos executivos
- Métricas de SLA
- Evidências de testes

### ✅ **Production Ready**
- Alertas configurados
- Monitoring dashboards
- Health checks
- Resource monitoring

---

## 💡 Próximos Passos

1. **Deploy**: Usar docker-compose-observability.yml em ambiente de produção
2. **Scaling**: Adicionar mais nós modificando o docker-compose
3. **Integration**: Integrar com CI/CD pipeline
4. **Monitoring**: Configurar alertas para Slack/PagerDuty

---

**🎉 Stack de observabilidade completa implementada!**

Para começar: `./scripts/run-observability-stack.sh`
