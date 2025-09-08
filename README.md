# 🚀 KV-Store Distribuído com Observabilidade Completa

Sistema de Key-Value Store distribuído implementado em Go com suporte a dois modos de operação e observabilidade completa integrada.

## 🎯 Features

### **Modos de Operação**
- 🔄 **LEADERLESS**: Replicação eventual com gossip protocol
- 👑 **LEADER-FOLLOWER**: Consenso com eleição de líder e heartbeats

### **Observabilidade Integrada**
- 📊 **Prometheus**: 15+ métricas customizadas (P50/P95/P99, throughput, replication lag)
- 📈 **Grafana**: Dashboards específicos para performance e consistência
- 🚨 **AlertManager**: Alertas automáticos para degradação do sistema
- 🧪 **K6 Testing**: Testes de carga e failover automatizados

### **Testes de Consistência**
- ✅ Read-your-writes consistency
- ✅ Monotonic reads
- ✅ Eventual consistency
- ✅ Staleness measurement
- ✅ Conflict detection & resolution

## 🚀 Início Rápido

### **Opção 1: Script Interativo (Recomendado)**
```bash
./scripts/run-complete-stack.sh
```

### **Opção 2: Docker Compose Direto**

#### **Modo LEADERLESS (padrão) com Observabilidade**
```bash
docker-compose up --build -d
```

#### **Modo LEADERLESS (explícito) com Observabilidade**
```bash
docker-compose -f docker-compose-leaderless.yml up --build -d
```

#### **Modo LEADER-FOLLOWER com Observabilidade**
```bash
docker-compose -f docker-compose-leader-follower.yml up --build -d
```

#### **Desenvolvimento (Observability Only)**
```bash
docker-compose -f docker-compose-observability.yml up --build -d
```

## 📊 Acessar Dashboards

| Serviço | URL | Credenciais |
|---------|-----|-------------|
| KV-Store | http://localhost:8080 | - |
| Grafana | http://localhost:3000 | admin/admin |
| Prometheus | http://localhost:9090 | - |
| AlertManager | http://localhost:9093 | - |

## 🧪 Executar Testes

### **Teste de Carga com K6**
```bash
docker-compose --profile testing run --rm k6 run /scripts/load-test.js
```

### **Teste de Failover**
```bash
docker-compose --profile testing run --rm k6 run /scripts/failover-test.js
```

### **Teste Manual de Consistência**
```bash
./scripts/test-consistency-checks.sh
```

### **Relatório Completo**
```bash
./scripts/generate-test-report.sh
```

## 🔧 API Endpoints

### **PUT - Armazenar Chave**
```bash
curl -X POST http://localhost:8080/store \
  -H "Content-Type: application/json" \
  -d '{"key":"test","value":"hello"}'
```

### **GET - Buscar Chave**
```bash
curl http://localhost:8080/store/test
```

### **Health Check**
```bash
curl http://localhost:8080/health
```

### **Métricas Prometheus**
```bash
curl http://localhost:9091/metrics  # Node 1
curl http://localhost:9092/metrics  # Node 2  
curl http://localhost:9093/metrics  # Node 3
```

## 📈 Métricas Principais

### **Performance**
- `kvstore_request_duration_seconds` - Latência de requests (P50/P95/P99)
- `kvstore_requests_total` - Total de requests (throughput)
- `kvstore_replication_latency_seconds` - Latência de replicação

### **Consistência**
- `kvstore_conflicts_total` - Conflitos detectados
- `kvstore_consistency_violations_total` - Violações de consistência
- `kvstore_conflict_resolution_duration_seconds` - Tempo de resolução

### **Cluster Health**
- `kvstore_node_status` - Status dos nós (0=down, 1=up)
- `kvstore_is_leader` - Líder atual (modo leader-follower)
- `kvstore_elections_total` - Total de eleições

## 🚨 Alertas Configurados

- **HighRequestLatency**: P95 > 1.0s
- **HighReplicationLatency**: P95 replication > 2.0s  
- **ReplicationFailures**: >10 falhas em 5min
- **ConsistencyViolations**: >5 violações em 5min
- **NodeDown**: Nó indisponível por 30s
- **NoLeader**: Sem líder por 30s
- **MultipleLeaders**: Split-brain detectado

## 📁 Estrutura do Projeto

```
📁 kv-golang/
├── 📄 docker-compose.yml                    # LEADERLESS (padrão) + observability
├── 📄 docker-compose-leaderless.yml         # LEADERLESS (explícito) + observability  
├── 📄 docker-compose-leader-follower.yml    # LEADER-FOLLOWER + observability
├── 📄 docker-compose-observability.yml      # Desenvolvimento completo
│
├── 📁 cmd/
│   ├── 📁 server/                           # Servidor KV-Store
│   └── 📁 client/                           # Cliente de exemplo
│
├── 📁 internal/
│   ├── 📁 store/                            # Core do KV-Store
│   ├── 📁 metrics/                          # Métricas Prometheus
│   ├── 📁 config/                           # Configurações
│   └── 📁 vectorclock/                      # Vector clocks
│
├── 📁 grafana/
│   ├── 📁 dashboards/                       # Dashboards customizados
│   └── 📁 datasources/                      # Configuração data sources
│
├── 📁 k6/
│   ├── 📄 load-test.js                      # Teste de carga + consistência
│   └── 📄 failover-test.js                  # Teste de failover
│
├── 📁 scripts/
│   ├── 📄 run-complete-stack.sh             # Script principal interativo
│   ├── 📄 test-consistency-checks.sh        # Testes manuais
│   └── 📄 generate-test-report.sh           # Relatórios automáticos
│
└── 📁 docs/
    ├── 📄 observability-guide.md            # Guia completo de observabilidade
    ├── 📄 testing-guide.md                  # Guia de testes
    └── 📄 usage-guide.md                    # Guia de uso
```

## 🔍 Troubleshooting

### **Problema**: Serviços não iniciam
```bash
# Verificar logs
docker-compose logs

# Limpar ambiente
docker-compose down -v
docker system prune -f
```

### **Problema**: Métricas não aparecem
```bash
# Verificar endpoints
curl http://localhost:9091/metrics

# Verificar targets no Prometheus
curl http://localhost:9090/api/v1/targets
```

### **Problema**: Tests K6 falham
```bash
# Verificar saúde dos nós
curl http://localhost:8081/health
curl http://localhost:8082/health  
curl http://localhost:8083/health
```

## 🌟 Casos de Uso

### **1. Desenvolvimento Local**
```bash
# Iniciar modo mínimo
docker-compose up -d

# Testes rápidos
./scripts/test-consistency-checks.sh
```

### **2. Performance Testing**
```bash
# Iniciar com observabilidade
./scripts/run-complete-stack.sh

# Executar testes K6
# (via menu interativo)
```

### **3. Teste de Resiliência**
```bash
# Simular falhas de nó
docker-compose stop node2

# Executar testes de failover
docker-compose --profile testing run --rm k6 run /scripts/failover-test.js
```

### **4. Análise de Produção**
```bash
# Gerar relatórios completos
./scripts/generate-test-report.sh

# Analisar no Grafana
# http://localhost:3000
```

## 📚 Documentação Completa

- 📖 [Guia de Observabilidade](docs/observability-guide.md)
- 🧪 [Guia de Testes](docs/testing-guide.md)  
- 📋 [Guia de Uso](docs/usage-guide.md)
- 🏗️ [Arquitetura](docs/arquitetura.md)

## 🎯 Próximos Passos

1. **Scaling**: Adicionar mais nós modificando docker-compose
2. **CI/CD**: Integrar testes automatizados no pipeline
3. **Production**: Deploy em Kubernetes com Helm charts
4. **Monitoring**: Integrar alertas com Slack/PagerDuty

---

## 🚀 Para Começar

```bash
# Clonar repositório
git clone https://github.com/bquerino/kv-golang.git
cd kv-golang

# Executar stack completa
./scripts/run-complete-stack.sh

# Acessar dashboards
# Grafana: http://localhost:3000 (admin/admin)
# Prometheus: http://localhost:9090
```

**🎉 Stack completa de observabilidade implementada para todos os modos!**

## 🎯 Modos de Operação

### 🔄 Modo Leaderless (Padrão)
- **Eventual Consistency** com Vector Clocks
- **Alta Disponibilidade** - qualquer nó pode falhar
- **Writes/Reads** em qualquer nó
- **Gossip Protocol** para propagação

### 👑 Modo Leader-Follower (Novo)
- **Strong Consistency** para writes
- **Leader Election** automática
- **Writes** apenas no líder (com redirecionamento)
- **Reads** em qualquer nó
- **Failover** automático

## Funcionalidades Principais

- **🔄 Dual-Mode Architecture**: Escolha entre leaderless ou leader-follower
- **🗣️ Gossip Protocol**: Comunicação peer-to-peer entre nós distribuídos
- **💾 Persistência em disco**: Dados salvos em append-only log local
- **⏰ Vector Clocks**: Controle de versões distribuído e resolução de conflitos
- **🔄 Hinted Handoff**: Tolerância a falhas com entrega garantida
- **⚖️ Consistent Hashing**: Distribuição equilibrada de dados
- **🗳️ Leader Election**: Algoritmo de eleição com heartbeat (modo leader-follower)
- **📦 Load Balancing**: Nginx integrado para distribuição de carga

## Requisitos

- [Go](https://golang.org/dl/) (v1.16 ou superior)
- [Docker](https://www.docker.com/) e Docker Compose (para execução via contêineres)
- Um ambiente que permita múltiplas instâncias rodando (múltiplos terminais ou servidores).

## Execução com Docker

É possível levantar três nós da aplicação e um balanceador de carga Nginx usando o Docker Compose incluso neste repositório:

```bash
docker-compose up --build -d
```

O Nginx ficará exposto na porta **8080**, encaminhando o tráfego para os três nós. Para interagir com o cluster, utilize o cliente apontando para o load balancer:


```bash
go run ./cmd/client/main.go localhost 8080
```

Assim, qualquer comando enviado será roteado para um dos nós do cluster automaticamente.

## Como Testar o Projeto sem Docker

### 1. Clonar o Repositório

Clone o repositório para a sua máquina:

```bash
git clone https://github.com/bquerino/kv-golang.git
cd kv-golang
```

### 2. Escolher Modo de Operação

#### 🔄 Modo Leaderless (Padrão - Comportamento Original)

Para usar o modo atual com eventual consistency:

**Terminal 1: Nó 1**
```bash
go run ./cmd/server/main.go node1 8081
```

**Terminal 2: Nó 2**
```bash
go run ./cmd/server/main.go node2 8082
```

**Terminal 3: Nó 3**
```bash
go run ./cmd/server/main.go node3 8083
```

#### 👑 Modo Leader-Follower (Nova Funcionalidade)

Para usar o modo com strong consistency e leader election:

**Terminal 1: Nó 1**
```bash
go run ./cmd/server/main.go node1 8081 --mode=leader-follower
```

**Terminal 2: Nó 2**
```bash
go run ./cmd/server/main.go node2 8082 --mode=leader-follower
```

**Terminal 3: Nó 3**
```bash
go run ./cmd/server/main.go node3 8083 --mode=leader-follower
```

#### ⚙️ Configurações Avançadas (Leader-Follower)

```bash
go run ./cmd/server/main.go node1 8081 \
  --mode=leader-follower \
  --election-timeout=5s \
  --heartbeat-interval=1s
```

**Parâmetros disponíveis:**
- `--mode=leaderless|leader-follower` - Modo de operação
- `--election-timeout=duration` - Timeout para eleição (ex: 5s, 3000ms)
- `--heartbeat-interval=duration` - Intervalo de heartbeat (ex: 1s, 500ms)

### 3. Usar o Cliente Interativo

Abra um terminal separado e execute o cliente apontando para qualquer nó:

```bash
go run ./cmd/client/main.go localhost 8081
```

No cliente, use os comandos:

- `put chave valor` — armazena uma chave/valor
- `get chave` — consulta uma chave
- `nodes` — lista os nós ativos
- `exit` — encerra o cliente

#### 🔄 Comportamento por Modo:

**Modo Leaderless:**
- `PUT/GET` funcionam em qualquer nó
- Dados são replicados via gossip protocol
- Eventual consistency com resolução de conflitos

**Modo Leader-Follower:**
- `PUT` em follower → automático redirect para leader
- `GET` funciona em qualquer nó
- Strong consistency garantida
- Veja logs para identificar o leader atual

### 4. Monitoramento e Logs

#### 🔄 Logs do Modo Leaderless:
```
INFO Node node1 started in LEADERLESS mode on node1:8081
DEBUG [Put] Iniciando PUT key=user1 value=John
INFO [Put] PUT enviado com sucesso node=node2 key=user1
```

#### 👑 Logs do Modo Leader-Follower:
```
INFO Node node1 started in LEADER-FOLLOWER mode on node1:8081
INFO Starting leader election term=1 node=node1
INFO Became leader term=1 node=node1
INFO New leader announced leader=node1 term=1
```

### 5. Testando Funcionalidades Específicas

#### 🔄 Testar Eventual Consistency (Leaderless):
```bash
# Conecte a nós diferentes e insira a mesma chave
# Terminal 1 (cliente → node1)
put user1 John

# Terminal 2 (cliente → node2)  
put user1 Jane

# Aguarde alguns segundos e consulte em qualquer nó
get user1  # Valor será reconciliado via Vector Clock
```

#### 👑 Testar Leader Election e Failover (Leader-Follower):
```bash
# 1. Inicie 3 nós em modo leader-follower
# 2. Observe nos logs qual nó virou leader
# 3. Conecte cliente a um follower e faça PUT (será redirecionado)
# 4. Mate o processo do leader (Ctrl+C)
# 5. Observe nova eleição nos logs
# 6. Teste operações após failover
```

#### 🧪 Testar Hinted Handoff (Ambos os Modos):
```bash
# 1. Inicie 3 nós
# 2. Mate um nó (Ctrl+C)
# 3. Faça operações PUT nos nós ativos
# 4. Reinicie o nó que estava down
# 5. Observe dados sendo sincronizados via hinted handoff
```

## 📊 Comparação de Modos

| Aspecto | Leaderless | Leader-Follower |
|---------|------------|-----------------|
| **Comando** | `./server node1 8081` | `./server node1 8081 --mode=leader-follower` |
| **Writes** | Qualquer nó | Apenas leader (redirecionado) |
| **Reads** | Qualquer nó | Qualquer nó |
| **Consistência** | Eventual | Strong (writes) |
| **Disponibilidade** | Muito alta | Alta |
| **Tolerância a Partição** | Excelente | Boa (requer maioria) |
| **Latência Writes** | Baixa | Média |
| **Complexidade** | Baixa | Média |
| **Casos de Uso** | Alta disponibilidade, geo-distribuição | Aplicações críticas, ACID |

## 🏗️ Arquitetura e Estrutura do Código

### Organização do Projeto
```
kv-golang/
├── cmd/
│   ├── client/main.go          # Cliente CLI interativo
│   └── server/main.go          # Servidor com suporte dual-mode
├── internal/
│   ├── config/
│   │   └── config.go           # Sistema de configuração
│   ├── store/
│   │   ├── gossip.go           # Gossip + Leader Election
│   │   ├── kvstore.go          # KV Store + Hinted Handoff
│   │   ├── hashing.go          # Consistent Hashing
│   │   ├── operation_mode.go   # Interface para modos
│   │   └── persistence.go      # Gerenciamento de páginas
│   └── vectorclock/
│       └── vectorclock.go      # Vector Clock implementation
├── docs/                       # Documentação técnica
├── scripts/                    # Scripts de teste
└── docker-compose.yml          # Orquestração Docker
```

### Componentes Principais

#### **Gossip Protocol** (`internal/store/gossip.go`)
- 🗣️ Comunicação peer-to-peer entre nós
- 💓 Health checking com PING/PONG
- 🗳️ Leader election (modo leader-follower)
- 📡 Propagação de operações PUT/GET

#### **Key-Value Store** (`internal/store/kvstore.go`)
- 💾 Gerenciamento de dados em memória e disco
- ⏰ Resolução de conflitos com Vector Clocks
- 🔄 Replicação entre nós
- 📦 Hinted Handoff para tolerância a falhas

#### **Operation Modes** (`internal/store/operation_mode.go`)
- 🎭 Interface abstrata para diferentes comportamentos
- 🔄 LeaderlessMode: eventual consistency
- 👑 LeaderFollowerMode: strong consistency

#### **Consistent Hashing** (`internal/store/hashing.go`)
- 🎯 Distribuição de chaves entre nós
- 🔄 Suporte a nós virtuais (vNodes)
- ⚖️ Balanceamento automático de carga

## 🧪 Scripts de Teste Automatizados

### Opções de Docker Compose

O sistema oferece três arquivos de configuração Docker:

1. **docker-compose.yml** - Modo leaderless (padrão)
2. **docker-compose-leaderless.yml** - Modo leaderless (explícito)  
3. **docker-compose-leader-follower.yml** - Modo leader-follower

#### ⚠️ Importante: Qual arquivo será usado?

Quando você executa `docker-compose up`, o Docker Compose **sempre usa o arquivo `docker-compose.yml` por padrão**. Para usar outros arquivos, você deve especificar explicitamente com a flag `-f`.

#### Comandos para cada modo:

```bash
# Modo leaderless (padrão) - usa docker-compose.yml automaticamente
docker-compose up --build -d

# Modo leaderless (explícito) - especifica o arquivo
docker-compose -f docker-compose-leaderless.yml up --build -d

# Modo leader-follower - especifica o arquivo
docker-compose -f docker-compose-leader-follower.yml up --build -d

# Parar serviços (para qualquer arquivo)
docker-compose down
# ou especificar o arquivo usado:
docker-compose -f docker-compose-leader-follower.yml down
```

#### Verificar qual configuração será usada:
```bash
# Mostra a configuração que será aplicada
docker-compose config
```

### Scripts de Teste por Plataforma

#### Linux/macOS (Bash)
```bash
# Teste modo leaderless
chmod +x scripts/test-leaderless.sh
./scripts/test-leaderless.sh

# Teste modo leader-follower
chmod +x scripts/test-leader-follower.sh
./scripts/test-leader-follower.sh

# Comparação entre modos
chmod +x scripts/compare-modes.sh
./scripts/compare-modes.sh

# Testa compilação
./test-build.sh
```

#### Windows (PowerShell)
```powershell
# Teste modo leaderless
.\scripts\test-leaderless.ps1

# Teste modo leader-follower
.\scripts\test-leader-follower.ps1
```

### Documentação de Testes

Para instruções detalhadas de teste, consulte:
- 📋 [Guia de Testes Completo](docs/testing-guide.md)

## ❓ **Perguntas Frequentes (FAQ)**

### **P: Como funciona a replicação no modo leader-follower?**

**R:** No modo leader-follower, quando você faz um PUT:

1. **Cliente envia PUT para qualquer nó**
2. **Se for follower**: redireciona para o leader 
3. **Leader processa**:
   - Persiste localmente primeiro
   - Replica para todos os followers
   - Retorna sucesso apenas se maioria confirmou
4. **Followers recebem** e aplicam a replicação

```bash
# Teste de replicação
./scripts/test-replication.sh        # Linux/macOS
.\scripts\test-replication.ps1       # Windows
```

### **P: Como verificar se a replicação está funcionando?**

**R:** Use os comandos de teste:

```bash
# 1. Fazer PUT no leader
echo "put test_key test_value" | nc localhost 8081

# 2. Verificar em todos os nós
echo "get test_key" | nc localhost 8081
echo "get test_key" | nc localhost 8082  
echo "get test_key" | nc localhost 8083

# Todos devem retornar o mesmo valor
```

### **P: O que acontece se um follower estiver offline durante o PUT?**

**R:** O sistema usa **Hinted Handoff**:
- Leader armazena a operação como "hint"
- Quando follower volta online, recebe as operações perdidas
- Garante eventual consistência

### **P: Como resolver Split-Brain Election (eleição infinita)?**

**R:** Este problema ocorre quando dois nós ficam em loop de eleição após um terceiro nó sair:

```bash
# Sintomas nos logs:
# node2 | Starting leader election term=270
# node3 | Starting leader election term=269  
# node2 | Denied vote candidate=node3 term=269 currentTerm=270
# node3 | Denied vote candidate=node2 term=270 currentTerm=269
```

**Soluções:**

```bash
# Opção 1: Script automatizado
./scripts/fix-split-brain.sh        # Linux/macOS
.\scripts\fix-split-brain.ps1       # Windows

# Opção 2: Reiniciar um nó manualmente
docker restart kv-golang-node3-1

# Opção 3: Reinicar cluster com delay
docker-compose stop node2 node3
docker-compose start node2 && sleep 3 && docker-compose start node3
```

### **P: Por que PUT falha em nó único com "failed to replicate to majority"?**

**R:** Problema corrigido! Antes o sistema tentava replicar mesmo em cluster de 1 nó:

```bash
# Antes (ERRO):
Response: ERROR: failed to replicate to majority of followers (0/2 successful)

# Agora (CORRETO):
Response: PUT_ACK key bruno persisted successfully
```

**Correções implementadas:**
- **Detecção de nó único**: `aliveFollowers == 0` → sem replicação
- **Contagem correta**: Só conta nós vivos como followers elegíveis
- **Maioria dinâmica**: Baseada em nós ativos, não configuração total

**Teste da correção:**
```bash
./scripts/test-single-put.sh       # Linux/macOS
.\scripts\test-single-put.ps1      # Windows
```

### **P: O que acontece quando só sobra 1 nó no cluster?**

**R:** O sistema detecta automaticamente quando fica sozinho e se elege como leader:

```bash
# Cenário: 3 nós → 2 nós → 1 nó
# Comportamento esperado:
node1 | Detected as single active node, becoming leader immediately
node1 | Became leader term=X node=node1
```

**Características:**
- **Detecção automática** de nó único
- **Auto-eleição** sem necessidade de votos
- **Operações PUT/GET** funcionam normalmente
- **Recuperação automática** quando outros nós retornam

**Teste do cenário:**
```bash
./scripts/test-single-node.sh       # Linux/macOS
.\scripts\test-single-node.ps1      # Windows
```

**R:** O sistema detecta automaticamente quando fica sozinho e se elege como leader:

```bash
# Cenário: 3 nós → 2 nós → 1 nó
# Comportamento esperado:
node1 | Detected as single active node, becoming leader immediately
node1 | Became leader term=X node=node1
```

**Características:**
- **Detecção automática** de nó único
- **Auto-eleição** sem necessidade de votos
- **Operações PUT/GET** funcionam normalmente
- **Recuperação automática** quando outros nós retornam

**Teste do cenário:**
```bash
./scripts/test-single-node.sh       # Linux/macOS
.\scripts\test-single-node.ps1      # Windows
```

### **P: Como monitorar eleições e identificar problemas?**

**R:** Use os scripts de monitoramento:

```bash
# Monitoramento completo
./scripts/test-docker-shutdown.sh    # Linux/macOS
.\scripts\test-docker-shutdown.ps1   # Windows

# Verificação rápida de status
echo "status" | nc localhost 8081
echo "status" | nc localhost 8082  
echo "status" | nc localhost 8083
```

**R:** Use os scripts de monitoramento:

```bash
# Monitoramento completo
./scripts/test-docker-shutdown.sh    # Linux/macOS
.\scripts\test-docker-shutdown.ps1   # Windows

# Verificação rápida de status
echo "status" | nc localhost 8081
echo "status" | nc localhost 8082  
echo "status" | nc localhost 8083
```

### ⚠️ **Split-Brain Election (Problemas Conhecidos)**

#### **Problema: Loop Infinito de Eleição**

Quando um nó sai do cluster com 3 nós, os 2 restantes podem entrar em **split-brain election**:

```bash
# Sintomas nos logs:
node2 | Starting leader election term=270
node3 | Starting leader election term=269  
node2 | Denied vote candidate=node3 term=269 currentTerm=270
node3 | Denied vote candidate=node2 term=270 currentTerm=269
# Loop infinito...
```

#### **Causa Raiz:**
- Algoritmo tradicional precisa de maioria absoluta (2 de 3 nós)
- Cada nó vota em si mesmo primeiro 
- Terms diferentes impedem votos cruzados
- Sem maioria, reinicia eleição em loop

#### **Solução Implementada: Dynamic Majority**

O sistema agora usa **maioria dinâmica** baseada em nós ativos:

```go
// Antes: Maioria fixa (sempre 2 de 3)
staticMajority := len(g.Nodes)/2 + 1

// Agora: Maioria dinâmica (baseada em nós respondentes)
activeNodes := responses + 1 
dynamicMajority := activeNodes/2 + 1
```

#### **Como Funciona:**
1. **Conta respostas** (sucesso ou falha) de outros nós
2. **Calcula maioria** baseada apenas em nós ativos
3. **Requer mínimo** de 2 votos para segurança
4. **Resolve split-brain** automaticamente

#### **Scripts de Teste e Correção:**

```bash
# Teste automatizado da correção
./scripts/test-split-brain-fix.sh      # Linux/macOS
.\scripts\test-split-brain-fix.ps1     # Windows

# Correção manual se necessário
./scripts/fix-split-brain.sh           # Linux/macOS  
.\scripts\fix-split-brain.ps1          # Windows
```

#### **Monitoramento Preventivo:**

```bash
# Identificar problemas antes que escalem
./scripts/test-docker-shutdown.sh      # Linux/macOS
.\scripts\test-docker-shutdown.ps1     # Windows

# Verificação rápida manual
echo "status" | nc localhost 8081
echo "status" | nc localhost 8082
echo "status" | nc localhost 8083
```

---

---

## 🎯 Identificando o Leader e Testando Failover

### Como Identificar o Node Leader

No modo leader-follower, você pode identificar qual node é o leader usando:

#### Via Client Interativo
```bash
# Conecte-se a qualquer node
go run ./cmd/client/main.go localhost 8081

# Execute o comando status
> status
Response: STATUS node_id:node1, address:node1:8081, mode:leader-follower, is_leader:true, current_leader:node1, term:1

# Execute o comando nodes para ver todos os nodes
> nodes
Response: NODES node1:node1:8081:alive, node2:node2:8082:alive, node3:node3:8083:alive
```

#### Via netcat (nc)
```bash
# Verificar status de um node específico
echo "status" | nc localhost 8081

# Verificar todos os nodes
echo "nodes" | nc localhost 8082
```

#### Via Scripts Automatizados
```bash
# Linux/macOS
chmod +x scripts/test-leader-shutdown.sh
./scripts/test-leader-shutdown.sh

# Windows PowerShell
.\scripts\test-leader-shutdown.ps1
```

### Como Interpretar o Status

**Campos do comando STATUS:**
- `node_id`: ID do node atual
- `address`: Endereço do node
- `mode`: Modo de operação (leaderless/leader-follower)
- `is_leader`: Se este node é o leader (true/false)
- `current_leader`: ID do leader atual conhecido
- `term`: Term atual da eleição

### Testando Shutdown e Failover

#### 1. Shutdown via Comando TCP (Recomendado para Docker)
```bash
# Conectar ao node e enviar comando shutdown
echo "shutdown" | nc localhost 8081

# Ou via client interativo
go run ./cmd/client/main.go localhost 8081
> shutdown
Response: SHUTDOWN_ACK node node1 shutting down
```

#### 2. Scripts Automatizados para Docker
```bash
# Linux/macOS
chmod +x scripts/test-docker-shutdown.sh
./scripts/test-docker-shutdown.sh

# Windows PowerShell
.\scripts\test-docker-shutdown.ps1
```

#### 3. Shutdown via Docker Stop
```bash
# Parar um container específico
docker stop kv-golang-node1-1

# Verificar se nova eleição aconteceu
echo "status" | nc localhost 8082
```

#### 4. Shutdown Programático do Leader (Para desenvolvimento local)
```bash
# Identificar o leader primeiro
./scripts/test-leader-shutdown.sh
# Escolha opção 1 para identificar leader
# Escolha opção 4 para shutdown do leader atual
```

### Observando a Eleição de Novo Leader

Após o shutdown do leader:

1. **Aguarde 5-10 segundos** para nova eleição
2. **Consulte o status** dos nodes restantes:
   ```bash
   echo "status" | nc localhost 8082
   echo "status" | nc localhost 8083
   ```
3. **Verifique logs** para ver processo de eleição
4. **Teste operações PUT** no novo leader

### Logs de Debugging

Para ver logs detalhados da eleição:
```bash
# Os logs mostrarão mensagens como:
# [Election] Starting election process
# [Election] Sending election message
# [Election] Becoming leader
# [Election] New leader elected
```

## 📚 Documentação Adicional

- 📖 [Proposta Técnica Completa](docs/leader-follower-proposal.md)
- 📋 [Resumo Executivo](docs/leader-follower-summary.md)
- 🎯 [Guia de Uso Detalhado](docs/usage-guide.md)
- 🏗️ [Documentação de Arquitetura](docs/arquitetura.md)

## 🚀 Roadmap e Melhorias Futuras

### ✅ Implementado
- ✅ Dual-mode architecture (leaderless + leader-follower)
- ✅ Leader election com algoritmo Bully
- ✅ Heartbeat system e failover automático
- ✅ Strong consistency para writes (modo leader-follower)
- ✅ Backward compatibility total

### 🔄 Em Consideração
- 🎯 Quorum-based replication (R/W/N tunáveis)
- 🔄 Anti-entropy para reconciliação periódica
- 🗜️ Compactação de logs
- 🌳 Merkle trees para sincronização eficiente
- 📊 Métricas Prometheus/Grafana
- 🔐 Autenticação e autorização

## 🤝 Contribuição

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo `LICENSE` para detalhes.

## 🎯 Referências

- 📚 [Dynamo: Amazon's Highly Available Key-value Store](https://www.cs.cornell.edu/courses/cs5414/2017fa/papers/dynamo.pdf)
- 🗳️ [Raft Consensus Algorithm](https://raft.github.io/)
- 🗣️ [Gossip Protocol and Failure Detection](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf)
- ⏰ [Vector Clocks in Distributed Systems](https://en.wikipedia.org/wiki/Vector_clock)

---

> **Nota**: Este README foi atualizado para refletir a nova arquitetura dual-mode. O sistema mantém 100% de compatibilidade com o comportamento anterior enquanto oferece funcionalidades avançadas através do modo leader-follower.
