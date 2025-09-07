# Documentação de Arquitetura - KV-Store Distribuído

## Visão Geral

Este documento descreve a arquitetura do sistema de Key-Value Store distribuído implementado em Go, que utiliza Gossip Protocol para comunicação entre nós, Consistent Hashing para distribuição de dados, Vector Clocks para controle de versões e Hinted Handoff para tolerância a falhas.

## Índice

1. [Nível 1 - Arquitetura de Alto Nível](#nível-1---arquitetura-de-alto-nível)
2. [Nível 2 - Arquitetura de Componentes](#nível-2---arquitetura-de-componentes)
3. [Nível 3 - Arquitetura de Implementação](#nível-3---arquitetura-de-implementação)

---

## Nível 1 - Arquitetura de Alto Nível

### Visão Geral do Sistema

O sistema é um Key-Value Store distribuído que permite armazenamento e recuperação de dados através de múltiplos nós, garantindo alta disponibilidade, tolerância a falhas e eventual consistência.

```mermaid
graph TB
    subgraph "Cliente"
        Client[Cliente CLI]
    end
    
    subgraph "Load Balancer"
        LB[Nginx]
    end
    
    subgraph "Cluster KV-Store"
        Node1[Nó 1<br/>:8081]
        Node2[Nó 2<br/>:8082]
        Node3[Nó 3<br/>:8083]
    end
    
    subgraph "Persistência"
        DB1[(Disco Local 1)]
        DB2[(Disco Local 2)]
        DB3[(Disco Local 3)]
    end
    
    Client --> LB
    LB --> Node1
    LB --> Node2
    LB --> Node3
    
    Node1 -.-> Node2
    Node1 -.-> Node3
    Node2 -.-> Node3
    
    Node1 --> DB1
    Node2 --> DB2
    Node3 --> DB3
    
    style Node1 fill:#e1f5fe
    style Node2 fill:#e1f5fe
    style Node3 fill:#e1f5fe
    style LB fill:#fff3e0
    style Client fill:#f3e5f5
```

### Características Principais

- **Distribuição**: Dados distribuídos entre múltiplos nós usando Consistent Hashing
- **Replicação**: Cada chave é replicada em todos os nós do cluster
- **Tolerância a Falhas**: Sistema continua operacional mesmo com falha de nós
- **Eventual Consistência**: Utilizando Vector Clocks para resolução de conflitos
- **Auto-recuperação**: Hinted Handoff para entrega de dados quando nós voltam online

### Fluxo de Operações Básicas

#### Operação PUT
1. Cliente envia requisição PUT através do load balancer
2. Nó receptor armazena localmente e replica para todos os outros nós
3. Dados são persistidos em disco local (append-only log)
4. Vector Clock é incrementado para controle de versão

#### Operação GET
1. Cliente envia requisição GET através do load balancer
2. Nó tenta buscar localmente primeiro
3. Se não encontrado, consulta outros nós do cluster
4. Retorna valor com Vector Clock para controle de versão

---

## Nível 2 - Arquitetura de Componentes

### Componentes Principais

```mermaid
graph TB
    subgraph "Nó KV-Store"
        subgraph "Camada de Comunicação"
            TC[TCP Server]
            GossipProto[Gossip Protocol]
            Election[Leader Election]
        end
        
        subgraph "Camada de Dados"
            KVStore[Key-Value Store]
            VectorClock[Vector Clock]
            ConsistentHash[Consistent Hashing]
        end
        
        subgraph "Camada de Persistência"
            PageManager[Page Manager]
            LogFile[Append-Only Log]
            HintedHandoff[Hinted Handoff]
        end
        
        subgraph "Camada de Aplicação"
            ClientHandler[Client Handler]
            CommandProcessor[Command Processor]
        end
    end
    
    TC --> GossipProto
    GossipProto --> Election
    GossipProto --> KVStore
    KVStore --> VectorClock
    KVStore --> ConsistentHash
    KVStore --> PageManager
    PageManager --> LogFile
    KVStore --> HintedHandoff
    ClientHandler --> CommandProcessor
    CommandProcessor --> KVStore
```

### Detalhamento dos Componentes

#### 1. Gossip Protocol (`internal/store/gossip.go`)
- **Responsabilidade**: Comunicação peer-to-peer entre nós
- **Funcionalidades**:
  - Health checking (PING/PONG)
  - Propagação de operações PUT/GET
  - Detecção de falhas de nós
  - Leader election (algoritmo Bully)

#### 2. Key-Value Store (`internal/store/kvstore.go`)
- **Responsabilidade**: Gerenciamento de dados e operações
- **Funcionalidades**:
  - Armazenamento em memória e disco
  - Resolução de conflitos com Vector Clocks
  - Replicação entre nós
  - Hinted Handoff para tolerância a falhas

#### 3. Consistent Hashing (`internal/store/hashing.go`)
- **Responsabilidade**: Distribuição de chaves entre nós
- **Funcionalidades**:
  - Mapeamento de chaves para nós usando SHA-1
  - Suporte a nós virtuais (vNodes) para balanceamento
  - Adição/remoção dinâmica de nós

#### 4. Vector Clock (`internal/vectorclock/vectorclock.go`)
- **Responsabilidade**: Controle de versões distribuído
- **Funcionalidades**:
  - Ordenação parcial de eventos
  - Detecção e resolução de conflitos
  - Serialização/deserialização para rede

### Fluxo de Dados Detalhado

```mermaid
sequenceDiagram
    participant C as Cliente
    participant N1 as Nó 1
    participant N2 as Nó 2
    participant N3 as Nó 3
    participant D as Disco
    
    Note over C, D: Operação PUT
    C->>N1: PUT key=user:123 value=John
    N1->>N1: Incrementa Vector Clock
    N1->>D: Persiste no log local
    
    par Replicação
        N1->>N2: PUT key=user:123 value=John VC=[1,0,0]
        N1->>N3: PUT key=user:123 value=John VC=[1,0,0]
    end
    
    N2->>N2: Resolve conflitos (Vector Clock)
    N3->>N3: Resolve conflitos (Vector Clock)
    
    par Persistência
        N2->>D: Persiste no log local
        N3->>D: Persiste no log local
    end
    
    N1->>C: STORED
```

### Tolerância a Falhas

```mermaid
graph LR
    subgraph "Cenário: Nó 2 Offline"
        N1[Nó 1<br/>Online]
        N2[Nó 2<br/>Offline]
        N3[Nó 3<br/>Online]
        HH[(Hinted<br/>Handoff<br/>Queue)]
    end
    
    N1 -.->|PUT falha| N2
    N1 -->|Armazena hint| HH
    N3 -.->|PUT falha| N2
    N3 -->|Armazena hint| HH
    
    N2 -->|Volta online| N1
    HH -->|Entrega dados| N2
```

---

## Nível 3 - Arquitetura de Implementação

### Estrutura de Código

```
kv-golang/
├── cmd/
│   ├── client/main.go      # Cliente CLI interativo
│   └── server/main.go      # Servidor principal
├── internal/
│   ├── store/
│   │   ├── gossip.go       # Protocolo Gossip
│   │   ├── kvstore.go      # Armazenamento K-V
│   │   ├── hashing.go      # Consistent Hashing
│   │   └── persistence.go  # Gerenciamento de páginas
│   └── vectorclock/
│       └── vectorclock.go  # Vector Clock
└── main.go                 # Ponto de entrada legacy
```

### Estruturas de Dados Principais

#### DataItem
```go
type DataItem struct {
    Value       string
    VectorClock *vectorclock.VectorClock
}
```

#### Hint (para Hinted Handoff)
```go
type Hint struct {
    Key         string
    Value       string
    TargetID    string
    Timestamp   time.Time
    VectorClock *vectorclock.VectorClock
}
```

#### Node
```go
type Node struct {
    ID        string
    Address   string
    Alive     bool
    LastCheck time.Time
}
```

### Algoritmos Implementados

#### 1. Algoritmo de Hash Consistente

```mermaid
graph LR
    subgraph "Anel de Hash"
        A[Hash A<br/>Node1-0]
        B[Hash B<br/>Node2-0]
        C[Hash C<br/>Node1-1]
        D[Hash D<br/>Node3-0]
        E[Hash E<br/>Node2-1]
        F[Hash F<br/>Node1-2]
    end
    
    Key1[key: user123<br/>hash: X] -.-> A
    Key2[key: data456<br/>hash: Y] -.-> C
    Key3[key: file789<br/>hash: Z] -.-> E
    
    A --> B --> C --> D --> E --> F --> A
```

**Implementação:**
```go
func (ch *ConsistentHashing) GetNode(key string) *Node {
    hash := ch.HashFunction(key)
    idx := sort.Search(len(ch.SortedHashes), func(i int) bool {
        return ch.SortedHashes[i] >= hash
    })
    if idx == len(ch.SortedHashes) {
        idx = 0
    }
    return ch.HashMap[ch.SortedHashes[idx]]
}
```

#### 2. Vector Clock Comparison

```mermaid
graph TB
    subgraph "Comparação de Vector Clocks"
        VC1["VC1: {A:1, B:2, C:1}"]
        VC2["VC2: {A:1, B:1, C:2}"]
        
        VC1 --> Compare{Compare}
        VC2 --> Compare
        
        Compare --> Concurrent["Resultado: Concurrent<br/>(Conflito)"]
    end
```

**Algoritmo de Comparação:**
```go
func (vc *VectorClock) Compare(other *VectorClock) int {
    isLess := false
    isGreater := false
    
    // Compara cada contador
    for nodeID, counter := range vc.Clock {
        if otherCounter, exists := other.Clock[nodeID]; exists {
            if counter < otherCounter {
                isLess = true
            } else if counter > otherCounter {
                isGreater = true
            }
        }
    }
    
    if isLess && !isGreater {
        return -1 // vc é mais antigo
    } else if isGreater && !isLess {
        return 1  // vc é mais recente
    }
    return 0 // Conflito (concurrent)
}
```

#### 3. Gossip Protocol para Health Check

```mermaid
sequenceDiagram
    participant N1 as Nó 1
    participant N2 as Nó 2
    participant N3 as Nó 3
    
    loop A cada 3 segundos
        N1->>N2: PING from N1
        N1->>N3: PING from N1
        N2->>N1: ACK (implicit)
        N2->>N3: PING from N2
        N3->>N1: ACK (implicit)
        N3->>N2: ACK (implicit)
    end
    
    Note over N1, N3: Se não há resposta, marca nó como morto
```

### Persistência de Dados

#### Append-Only Log
```mermaid
graph LR
    subgraph "Arquivo de Log"
        L1["user:123:John\n"]
        L2["data:456:Value1\n"]
        L3["user:123:Jane\n"]
        L4["config:789:Setting\n"]
    end
    
    Write[Operação PUT] --> L4
    
    Note1[Leitura: busca última<br/>ocorrência da chave]
    L1 -.-> Note1
    L3 -.-> Note1
```

**Implementação de Escrita:**
```go
func (kv *KeyValueStore) writeDataToDisk(key, value string) {
    line := key + ":" + value + "\n"
    if kv.LogFile != nil {
        _, err := kv.LogFile.WriteString(line)
        if err != nil {
            slog.Error("Falha ao persistir dado", "key", key, "err", err)
        }
    }
}
```

### Hinted Handoff Implementation

```mermaid
stateDiagram-v2
    [*] --> CheckingNodes
    CheckingNodes --> NodeDown: Nó não responde
    CheckingNodes --> NodeUp: Nó responde
    
    NodeDown --> StoringHint: Armazena hint
    StoringHint --> CheckingNodes: Continue verificando
    
    NodeUp --> DeliveryAttempt: Tenta entregar hints
    DeliveryAttempt --> HintDelivered: Sucesso
    DeliveryAttempt --> HintPending: Falha
    
    HintDelivered --> [*]
    HintPending --> CheckingNodes: Reagenda tentativa
```

**Implementação do Processamento:**
```go
func (kv *KeyValueStore) processHintedHandoff() {
    for key, hints := range kv.HintedData {
        for targetID, hint := range hints {
            if kv.Gossip.IsNodeAlive(targetID) {
                if node, ok := kv.Gossip.Nodes[targetID]; ok {
                    kv.Gossip.sendPutToNode(node, hint.Key, hint.Value, hint.VectorClock)
                }
                delete(hints, targetID)
            }
        }
    }
}
```

### Protocolos de Comunicação

#### Formato de Mensagens TCP

```
PUT <key> <value> <vector_clock>
GET <key>
PING from <node_id>
NODES
ELECTION from <node_id>
COORDINATOR <node_id>
```

#### Exemplo de Troca de Mensagens

```mermaid
sequenceDiagram
    participant C as Cliente
    participant N1 as Nó 1
    participant N2 as Nó 2
    
    C->>N1: PUT user:123 John
    Note over N1: VC = {N1:1}
    N1->>N2: PUT user:123 John {N1:1}
    N2->>N1: STORED
    N1->>C: STORED
    
    C->>N2: GET user:123
    N2->>C: VALUE John {N1:1}
```

### Configuração e Deploy

#### Docker Compose Setup
```yaml
services:
  node1:
    build: .
    command: ["node1", "8081"]
    networks: [kvnet]
  
  nginx:
    image: nginx:alpine
    ports: ["8080:8080"]
    volumes: ["./nginx.conf:/etc/nginx/nginx.conf:ro"]
    networks: [kvnet]
```

#### Nginx Load Balancer Config
```nginx
upstream kvstore {
    server node1:8081;
    server node2:8082;
    server node3:8083;
}

server {
    listen 8080;
    location / {
        proxy_pass http://kvstore;
    }
}
```

### Métricas e Monitoramento

O sistema implementa logging estruturado usando `slog` para:

- **Health Checks**: Status dos nós e detecção de falhas
- **Operações de Dados**: PUT/GET com timestamps e Vector Clocks
- **Replicação**: Sucesso/falha na propagação entre nós
- **Hinted Handoff**: Entrega de dados pendentes
- **Eleição de Líder**: Mudanças de coordenador

### Limitações e Considerações

1. **Consistência**: Sistema oferece eventual consistência, não forte consistência
2. **Particionamento de Rede**: Não implementa algoritmos para split-brain
3. **Compactação**: Log append-only pode crescer indefinidamente
4. **Segurança**: Não implementa autenticação/autorização
5. **Performance**: Replicação para todos os nós pode não escalar bem

### Futuras Melhorias

1. **Quorum-based Replication**: Implementar R/W/N tunáveis
2. **Anti-Entropy**: Reconciliação periódica entre nós
3. **Compactação de Log**: Limpeza de entradas antigas
4. **Merkle Trees**: Para sincronização eficiente
5. **Instrumentação**: Métricas Prometheus/Grafana
