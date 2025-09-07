# Guia de Teste dos Modos de Operação

Este documento explica como testar ambos os modos de operação do KV-Store usando Docker e scripts locais.

## Opções de Teste

### 1. Usando Docker Compose

#### Modo Leaderless (Padrão)
```bash
# Usar o docker-compose.yml padrão
docker-compose up --build -d

# Ou explicitamente o modo leaderless
docker-compose -f docker-compose-leaderless.yml up --build -d
```

#### Modo Leader-Follower
```bash
docker-compose -f docker-compose-leader-follower.yml up --build -d
```

#### Parando os serviços
```bash
# Para o compose padrão
docker-compose down

# Para composes específicos
docker-compose -f docker-compose-leader-follower.yml down
docker-compose -f docker-compose-leaderless.yml down
```

### 2. Usando Scripts Locais

#### Teste do Modo Leaderless
```bash
chmod +x scripts/test-leaderless.sh
./scripts/test-leaderless.sh
```

#### Teste do Modo Leader-Follower
```bash
chmod +x scripts/test-leader-follower.sh
./scripts/test-leader-follower.sh
```

#### Comparação Entre Modos
```bash
chmod +x scripts/compare-modes.sh
./scripts/compare-modes.sh
```

### 3. Teste Manual

#### Modo Leaderless
```bash
# Terminal 1
go run ./cmd/server/main.go node1 8081 --mode leaderless

# Terminal 2
go run ./cmd/server/main.go node2 8082 --mode leaderless

# Terminal 3
go run ./cmd/server/main.go node3 8083 --mode leaderless
```

#### Modo Leader-Follower
```bash
# Terminal 1
go run ./cmd/server/main.go node1 8081 --mode leader-follower --election-timeout 5000

# Terminal 2
go run ./cmd/server/main.go node2 8082 --mode leader-follower --election-timeout 6000

# Terminal 3
go run ./cmd/server/main.go node3 8083 --mode leader-follower --election-timeout 7000
```

## Testando as Operações

### Via nc (netcat)
```bash
# PUT operation
echo "put key1 value1" | nc localhost 8081

# GET operation
echo "get key1" | nc localhost 8082

# Listar nós (mostra qual é o leader no modo leader-follower)
echo "nodes" | nc localhost 8083

# Verificar status detalhado (novo comando)
echo "status" | nc localhost 8081

# Shutdown controlado de um nó (novo comando)
echo "shutdown" | nc localhost 8081
```

### Via curl (através do nginx)
```bash
# PUT através do load balancer
curl -X POST http://localhost:8080 -d "put key1 value1"

# GET através do load balancer
curl -X POST http://localhost:8080 -d "get key1"
```

## Diferenças Observáveis

### Modo Leaderless
- Qualquer nó pode processar PUT operations
- Conflitos podem ocorrer e são resolvidos via vector clocks
- GET retorna a versão mais recente conhecida

### Modo Leader-Follower
- Apenas o leader processa PUT operations
- Followers redirecionam PUTs para o leader
- GET pode ser feito em qualquer nó
- Em caso de falha do leader, nova eleição acontece automaticamente

## Logs e Debugging

### Ver logs dos containers
```bash
docker-compose logs node1
docker-compose logs -f  # follow logs de todos os serviços
```

### Ver status dos nós
```bash
# Verificar qual nó é o leader atual (modo leader-follower)
echo "nodes" | nc localhost 8081
echo "nodes" | nc localhost 8082
echo "nodes" | nc localhost 8083
```

## Testando Shutdown e Failover no Docker

### Comando SHUTDOWN via TCP

O sistema suporta shutdown controlado via comando TCP, ideal para testes em Docker:

```bash
# Shutdown de um nó específico
echo "shutdown" | nc localhost 8081

# Via client interativo
go run ./cmd/client/main.go localhost 8082
> shutdown
```

### Scripts Automatizados para Docker

#### Linux/macOS
```bash
chmod +x scripts/test-docker-shutdown.sh
./scripts/test-docker-shutdown.sh
```

#### Windows PowerShell
```powershell
.\scripts\test-docker-shutdown.ps1
```

### Funcionalidades dos Scripts Docker

1. **Identificação automática do leader**
2. **Shutdown via comando TCP**
3. **Shutdown via Docker stop**
4. **Monitoramento de containers**
5. **Teste de nova eleição**

### Exemplo de Uso

```bash
# 1. Iniciar cluster
docker-compose -f docker-compose-leader-follower.yml up --build -d

# 2. Identificar leader
echo "status" | nc localhost 8081
echo "status" | nc localhost 8082
echo "status" | nc localhost 8083

# 3. Shutdown do leader via TCP
echo "shutdown" | nc localhost 8081

# 4. Aguardar nova eleição (5-10 segundos)
sleep 10

# 5. Verificar novo leader
echo "status" | nc localhost 8082
```

## Limpeza

### Parar todos os processos locais
```bash
pkill -f "go run.*server"
```

### Remover containers Docker
```bash
docker-compose down
docker system prune -f  # remover containers/imagens não utilizados
```

## Configurações Avançadas

### Parâmetros do Modo Leader-Follower

- `--election-timeout`: Tempo em ms para iniciar eleição (padrão: 5000ms)
- `--heartbeat-interval`: Intervalo de heartbeat em ms (padrão: 1000ms)

```bash
go run ./cmd/server/main.go node1 8081 \
  --mode leader-follower \
  --election-timeout 8000 \
  --heartbeat-interval 500
```

### Teste de Failover

1. Identifique o leader atual
2. Mate o processo do leader
3. Observe a nova eleição
4. Teste operações no novo cluster
