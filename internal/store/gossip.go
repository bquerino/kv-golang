package store

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bquerino/kv-golang/internal/config"
	"github.com/bquerino/kv-golang/internal/vectorclock"
)

type Node struct {
	ID        string
	Address   string
	Alive     bool
	LastCheck time.Time
}

// LeaderState representa o estado atual do leader
type LeaderState struct {
	CurrentTerm int64
	LeaderID    string
	LastActive  time.Time
}

type Gossip struct {
	Nodes          map[string]*Node
	Self           *Node
	Coordinator    *Node
	Interval       time.Duration
	ConsistentHash *ConsistentHashing
	KeyValueStore  *KeyValueStore // Integração com o KeyValueStore
	Mutex          sync.Mutex

	// Campos para leader-follower mode
	Config        *config.Config
	LeaderState   LeaderState
	LastHeartbeat time.Time
	ElectionTimer *time.Timer
	IsLeader      bool
	Term          int64
	VotedFor      string
	OperationMode OperationMode
}

// Inicializa o Gossip Protocol e configura o Consistent Hashing com vNodes
func NewGossip(selfID, address string, interval time.Duration, vNodes int) *Gossip {
	return NewGossipWithConfig(selfID, address, interval, vNodes, nil)
}

// Inicializa o Gossip Protocol com configuração específica
func NewGossipWithConfig(selfID, address string, interval time.Duration, vNodes int, cfg *config.Config) *Gossip {
	self := &Node{
		ID:      selfID,
		Address: address,
		Alive:   true,
	}

	// Se não foi passada configuração, usa padrão leaderless
	if cfg == nil {
		cfg = config.NewConfig()
		slog.Info("No config provided, using default leaderless mode")
	} else {
		slog.Info("Config provided", "mode", cfg.Mode, "isLeaderFollower", cfg.IsLeaderFollowerMode())
	}

	gossip := &Gossip{
		Nodes:          make(map[string]*Node),
		Self:           self,
		Interval:       interval,
		ConsistentHash: NewConsistentHashing(vNodes),
		Config:         cfg,
		LastHeartbeat:  time.Now(),
		Term:           0,
		VotedFor:       "",
	}

	// Adiciona o próprio nó à lista e ao anel de hash consistente
	gossip.Nodes[selfID] = self
	gossip.ConsistentHash.AddNode(self)

	// Inicializa o KeyValueStore integrado com o Gossip e PageManager
	gossip.KeyValueStore, _ = NewKeyValueStore(gossip, gossip.ConsistentHash, 5*time.Second, "data_pages.db")

	// Configura o modo de operação
	if cfg != nil && cfg.IsLeaderFollowerMode() {
		slog.Info("Configuring LEADER-FOLLOWER mode")
		gossip.OperationMode = NewLeaderFollowerMode(gossip.KeyValueStore, gossip)
	} else {
		slog.Info("Configuring LEADERLESS mode")
		gossip.OperationMode = NewLeaderlessMode(gossip.KeyValueStore, gossip)
	}

	return gossip
}

// Adiciona um novo nó e seus vNodes à rede de Gossip
func (g *Gossip) AddNode(nodeID, address string) {
	g.Mutex.Lock()
	defer g.Mutex.Unlock()

	node := &Node{
		ID:      nodeID,
		Address: address,
		Alive:   true,
	}
	g.Nodes[nodeID] = node
	g.ConsistentHash.AddNode(node)
}

// Remove um nó e seus vNodes da rede de Gossip
func (g *Gossip) RemoveNode(nodeID string) {
	g.Mutex.Lock()
	defer g.Mutex.Unlock()

	delete(g.Nodes, nodeID)
	g.ConsistentHash.RemoveNode(nodeID)
}

// Envia mensagens para todos os nós conhecidos
func (g *Gossip) GossipOut() {
	g.Mutex.Lock()
	defer g.Mutex.Unlock()

	for _, node := range g.Nodes {
		go g.sendMessage(node)
	}
}

// Recebe mensagens e atualiza o estado dos nós
func (g *Gossip) GossipIn() {
	listener, err := net.Listen("tcp", g.Self.Address)
	if err != nil {
		slog.Error("Error starting TCP server", "err", err)
		return
	}

	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			slog.Error("Error accepting connection", "err", err)
			continue
		}

		go g.handleConnection(conn)
	}
}

// Envia uma mensagem de verificação de saúde para o nó
func (g *Gossip) sendMessage(node *Node) {
	conn, err := net.Dial("tcp", node.Address)
	if err != nil {
		slog.Warn("Error connecting to node", "node", node.ID, "err", err)
		g.markNodeDead(node)
		return
	}
	defer conn.Close()

	// Envia um ping simples
	// slog.Debug("Sending PING", "node", node.ID) // Desabilitado para evitar ruído
	fmt.Fprintf(conn, "PING from %s\n", g.Self.ID)
}

// Lida com uma conexão recebida (PING ou operações de dados)
func (g *Gossip) handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		slog.Error("Error reading connection", "err", err)
		return
	}

	if !strings.HasPrefix(line, "PING") {
		slog.Info("[TCP] Comando recebido", "raw", line)
	}
	parts := strings.Fields(strings.TrimSpace(line))
	slog.Info("[handleConnection] Parsed parts", "parts", parts)
	if len(parts) == 0 {
		slog.Warn("[handleConnection] Linha recebida vazia ou inválida", "raw", line)
		return
	}

	cmd := strings.ToUpper(parts[0])
	slog.Info("[handleConnection] Comando identificado", "cmd", cmd, "parts", parts)

	switch cmd {
	case "NODES":
		g.Mutex.Lock()
		var nodesList []string
		for id, node := range g.Nodes {
			status := "alive"
			if !node.Alive {
				status = "dead"
			}
			nodesList = append(nodesList, fmt.Sprintf("%s:%s:%s", id, node.Address, status))
		}
		g.Mutex.Unlock()
		fmt.Fprintf(conn, "NODES %s\n", strings.Join(nodesList, ", "))
		return
	case "STATUS":
		g.Mutex.Lock()
		var statusInfo []string

		// Informações básicas do nó
		statusInfo = append(statusInfo, fmt.Sprintf("node_id:%s", g.Self.ID))
		statusInfo = append(statusInfo, fmt.Sprintf("address:%s", g.Self.Address))

		// Informações de modo de operação
		if g.Config != nil {
			statusInfo = append(statusInfo, fmt.Sprintf("mode:%s", g.Config.Mode))

			if g.Config.IsLeaderFollowerMode() {
				// Informações específicas do modo leader-follower
				statusInfo = append(statusInfo, fmt.Sprintf("is_leader:%t", g.IsLeader))
				statusInfo = append(statusInfo, fmt.Sprintf("current_leader:%s", g.LeaderState.LeaderID))
				statusInfo = append(statusInfo, fmt.Sprintf("term:%d", g.LeaderState.CurrentTerm))
			}
		} else {
			statusInfo = append(statusInfo, "mode:leaderless")
		}

		g.Mutex.Unlock()
		fmt.Fprintf(conn, "STATUS %s\n", strings.Join(statusInfo, ", "))
		return
	case "PING":
		if len(parts) < 3 {
			return
		}
		nodeID := parts[2]
		g.Mutex.Lock()
		if node, exists := g.Nodes[nodeID]; exists {
			node.LastCheck = time.Now()
			node.Alive = true
			// slog.Debug("Received PING", "node", node.ID) // Desabilitado para evitar ruído
		} else {
			slog.Warn("Unknown node", "node", nodeID)
		}
		g.Mutex.Unlock()
	case "PUT":
		if len(parts) < 3 {
			slog.Warn("[handleConnection] PUT recebido com argumentos insuficientes", "parts", parts)
			return
		}
		key := parts[1]
		value := parts[2]
		slog.Info("[handleConnection] PUT recebido para processamento", "key", key, "value", value, "parts", parts)

		if len(parts) >= 4 {
			// PUT com VectorClock (replicação)
			vc := vectorclock.Deserialize(parts[3])
			fromNode := ""
			if len(parts) >= 5 {
				fromNode = parts[4]
			}
			slog.Info("[handleConnection] PUT com VectorClock propagado", "key", key, "vc", vc.String(), "from", fromNode)
			if g.OperationMode != nil {
				g.OperationMode.HandlePut(key, value, vc, fromNode)
			} else {
				g.KeyValueStore.ResolveConflicts(key, value, vc)
			}
		} else {
			// PUT direto do cliente
			if g.OperationMode != nil {
				err := g.OperationMode.Put(key, value)
				if err != nil {
					// Se for leader-follower e não for leader, redireciona
					if g.Config != nil && g.Config.IsLeaderFollowerMode() && !g.IsLeader {
						g.Mutex.Lock()
						leaderID := g.LeaderState.LeaderID
						g.Mutex.Unlock()
						if leaderID != "" && leaderID != g.Self.ID {
							if leaderNode, exists := g.Nodes[leaderID]; exists {
								fmt.Fprintf(conn, "REDIRECT %s\n", leaderNode.Address)
								return
							}
						}
						fmt.Fprintf(conn, "ERROR: No leader available\n")
						return
					}
					fmt.Fprintf(conn, "ERROR: %s\n", err.Error())
					return
				}
			} else {
				g.Put(key, value)
			}
		}
		// Responde ao client que o dado foi armazenado
		fmt.Fprintf(conn, "STORED\n")

	case "GET":
		if len(parts) < 2 {
			return
		}
		key := parts[1]

		value, vc, found := g.KeyValueStore.getLocal(key)
		if found {
			vcStr := ""
			if vc != nil {
				vcStr = vc.Serialize()
			}
			fmt.Fprintf(conn, "VALUE %s %s\n", value, vcStr)

		} else {
			fmt.Fprintf(conn, "NOTFOUND\n")
		}
	case "ELECTION":
		if len(parts) >= 3 {
			nodeID := parts[2]
			g.Mutex.Lock()
			if node, exists := g.Nodes[nodeID]; exists {
				node.LastCheck = time.Now()
				node.Alive = true
			}
			g.Mutex.Unlock()
			fmt.Fprintf(conn, "OK\n")
		}
	case "COORDINATOR":
		if len(parts) >= 2 {
			coordID := parts[1]
			g.Mutex.Lock()
			if node, exists := g.Nodes[coordID]; exists {
				g.Coordinator = node
			}
			g.Mutex.Unlock()
		}
	// Novos handlers para leader-follower mode
	case "VOTE_REQUEST":
		g.handleVoteRequest(parts, conn)
	case "HEARTBEAT":
		g.handleHeartbeat(parts, conn)
	case "LEADER_ANNOUNCE":
		g.handleLeaderAnnounce(parts, conn)
	case "SHUTDOWN":
		g.handleShutdown(parts, conn)
	default:
		slog.Warn("Unknown message", "msg", line)
	}
}

// Marca um nó como morto se ele não responder
func (g *Gossip) markNodeDead(node *Node) {
	g.Mutex.Lock()
	defer g.Mutex.Unlock()

	node.Alive = false
	slog.Warn("Node is marked as dead", "node", node.ID)
	if g.Coordinator != nil && g.Coordinator.ID == node.ID {
		slog.Warn("Coordinator is down! Initiating election.", "coordinator", node.ID)
		go g.initiateElection()
	}
}

// Função de loop para enviar pings periodicamente
func (g *Gossip) StartGossip() {
	ticker := time.NewTicker(g.Interval)
	for range ticker.C {
		g.GossipOut()
	}
}

// Função que inicia uma eleição quando o coordenador falha
func (g *Gossip) initiateElection() {
	slog.Info("Starting election...")

	g.Mutex.Lock()
	defer g.Mutex.Unlock()

	higherNodes := g.getHigherNodes()

	if len(higherNodes) == 0 {
		// Se não há nós com IDs maiores, o nó atual se torna o coordenador
		g.becomeCoordinator()
	} else {
		// Envia mensagens para os nós com IDs maiores
		for _, node := range higherNodes {
			go g.sendElectionMessage(node)
		}
	}
}

// Retorna uma lista de nós com IDs maiores que o do nó atual
func (g *Gossip) getHigherNodes() []*Node {
	var higherNodes []*Node
	for _, node := range g.Nodes {
		if node.ID > g.Self.ID && node.Alive {
			higherNodes = append(higherNodes, node)
		}
	}
	return higherNodes
}

// Envia uma mensagem de eleição para um nó com ID maior
func (g *Gossip) sendElectionMessage(node *Node) {
	conn, err := net.Dial("tcp", node.Address)
	if err != nil {
		slog.Error("[Election] Falha ao conectar para eleição", "node", node.ID, "err", err)
		g.markNodeDead(node)
		return
	}
	defer conn.Close()

	slog.Info("[Election] Enviando mensagem de eleição", "node", node.ID)
	fmt.Fprintf(conn, "ELECTION from %s\n", g.Self.ID)

	// Espera resposta de "OK"
	var response string
	fmt.Fscanf(conn, "%s\n", &response)
	if response == "OK" {
		slog.Info("[Election] Nó respondeu OK", "node", node.ID)
		return
	}
}

// Define o nó atual como coordenador
func (g *Gossip) becomeCoordinator() {
	slog.Info("Becoming the coordinator.")
	g.Coordinator = g.Self

	// Anuncia para todos os nós que este nó é o novo coordenador
	g.announceCoordinator()
}

// Anuncia que o nó atual é o coordenador para todos os outros nós
func (g *Gossip) announceCoordinator() {
	g.Mutex.Lock()
	defer g.Mutex.Unlock()

	for _, node := range g.Nodes {
		go g.sendCoordinatorMessage(node)
	}
}

// Envia uma mensagem de anúncio de coordenador para um nó
func (g *Gossip) sendCoordinatorMessage(node *Node) {
	conn, err := net.Dial("tcp", node.Address)
	if err != nil {
		slog.Warn("Error connecting to node to announce coordinator", "node", node.ID, "err", err)
		g.markNodeDead(node)
		return
	}
	defer conn.Close()

	slog.Debug("Announcing self as COORDINATOR", "node", node.ID)
	fmt.Fprintf(conn, "COORDINATOR %s\n", g.Self.ID)
}

// Mapeia uma chave para o nó apropriado
func (g *Gossip) GetNodeForKey(key string) *Node {
	return g.ConsistentHash.GetNode(key)
}

// Verifica se um nó está vivo
func (g *Gossip) IsNodeAlive(nodeID string) bool {
	g.Mutex.Lock()
	defer g.Mutex.Unlock()

	if node, exists := g.Nodes[nodeID]; exists {
		return node.Alive
	}
	return false
}

// isSingleActiveNode verifica se este é o único nó ativo no cluster
func (g *Gossip) isSingleActiveNode() bool {
	g.Mutex.Lock()
	defer g.Mutex.Unlock()

	activeNodes := 0
	for _, node := range g.Nodes {
		if node.Alive {
			activeNodes++
		}
	}

	return activeNodes == 1
}

// Envia um PUT para o KeyValueStore
func (g *Gossip) Put(key, value string) {
	if g.OperationMode != nil {
		g.OperationMode.Put(key, value)
	} else {
		g.KeyValueStore.Put(key, value)
	}
}

// Envia um GET para o KeyValueStore
func (g *Gossip) Get(key string) (string, *vectorclock.VectorClock, bool) {
	if g.OperationMode != nil {
		return g.OperationMode.Get(key)
	} else {
		return g.KeyValueStore.Get(key)
	}
}

// Envia um DELETE para o KeyValueStore (implementar no KeyValueStore, se ainda não estiver feito)
func (g *Gossip) Delete(key string) {
	// Método Delete não implementado
	slog.Warn("Delete operation is not yet implemented in KeyValueStore.")
}

// Imprime os nós ativos no cluster
func (g *Gossip) PrintNodes() {
	g.Mutex.Lock()
	defer g.Mutex.Unlock()

	for id, node := range g.Nodes {
		status := "alive"
		if !node.Alive {
			status = "dead"
		}
		slog.Info("Node status", "node", id, "address", node.Address, "status", status)
	}
}

// Envia uma operação PUT para outro nó responsável pela chave

func (g *Gossip) sendPutToNode(node *Node, key, value string, vc *vectorclock.VectorClock) error {
	conn, err := net.Dial("tcp", node.Address)
	if err != nil {
		slog.Error("[sendPutToNode] Falha ao conectar para PUT", "node", node.ID, "err", err)
		g.markNodeDead(node)
		return err
	}
	defer conn.Close()

	slog.Info("[sendPutToNode] Enviando PUT", "node", node.ID, "key", key, "value", value, "vc", vc.String())
	_, err = fmt.Fprintf(conn, "PUT %s %s %s %s\n", key, value, vc.Serialize(), g.Self.ID)
	if err != nil {
		slog.Error("[sendPutToNode] Falha ao escrever PUT", "node", node.ID, "err", err)
		return err
	}

	// Ler resposta do nó remoto
	resp, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		slog.Error("[sendPutToNode] Falha ao ler resposta do nó remoto", "node", node.ID, "err", err)
		return err
	}
	resp = strings.TrimSpace(resp)
	if resp == "STORED" {
		slog.Info("[sendPutToNode] PUT confirmado pelo nó remoto", "node", node.ID, "key", key)
		return nil
	} else {
		slog.Warn("[sendPutToNode] PUT não confirmado pelo nó remoto", "node", node.ID, "key", key, "resp", resp)
		return fmt.Errorf("PUT não confirmado: %s", resp)
	}
}

// Envia uma operação GET para outro nó e retorna o resultado
func (g *Gossip) sendGetToNode(node *Node, key string) (string, *vectorclock.VectorClock, bool) {
	conn, err := net.Dial("tcp", node.Address)
	if err != nil {
		slog.Warn("Error sending GET to node", "node", node.ID, "err", err)
		g.markNodeDead(node)
		return "", nil, false
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET %s\n", key)

	resp, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		slog.Warn("Error reading GET response from node", "node", node.ID, "err", err)
		return "", nil, false
	}

	resp = strings.TrimSpace(resp)

	parts := strings.Fields(resp)
	if len(parts) >= 2 && parts[0] == "VALUE" {
		value := parts[1]
		var vc *vectorclock.VectorClock
		if len(parts) >= 3 {
			vc = vectorclock.Deserialize(parts[2])
		}
		return value, vc, true

	}

	return "", nil, false
}

// ========== MÉTODOS PARA LEADER-FOLLOWER MODE ==========

// StartElectionProcess inicia o processo de eleição para modo leader-follower
func (g *Gossip) StartElectionProcess() {
	if g.Config == nil || !g.Config.IsLeaderFollowerMode() {
		return
	}

	slog.Info("Starting election process for leader-follower mode")

	// Inicia monitoramento de heartbeat
	go g.monitorLeaderHeartbeat()

	// Inicia primeira eleição após delay aleatório baseado no ID
	go func() {
		delay := time.Duration(len(g.Self.ID)) * 100 * time.Millisecond
		time.Sleep(delay)
		g.startElection()
	}()
}

// monitorLeaderHeartbeat monitora heartbeats do leader e inicia eleição se necessário
func (g *Gossip) monitorLeaderHeartbeat() {
	ticker := time.NewTicker(g.Config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if g.Config.IsLeaderFollowerMode() && !g.IsLeader {
				if time.Since(g.LastHeartbeat) > g.Config.ElectionTimeout {
					// Verifica se está sozinho no cluster antes de iniciar eleição
					if g.isSingleActiveNode() {
						slog.Info("Detected as single active node, becoming leader immediately")
						g.Mutex.Lock()
						g.Term++
						currentTerm := g.Term
						g.Mutex.Unlock()
						go g.becomeLeader(currentTerm)
					} else {
						slog.Warn("Leader heartbeat timeout, starting election")
						go g.startElection()
					}
				}
			}
		}
	}
}

// startElection inicia processo de eleição de líder
func (g *Gossip) startElection() {
	g.Mutex.Lock()
	g.Term++
	g.IsLeader = false
	g.VotedFor = g.Self.ID // Vota em si mesmo
	currentTerm := g.Term
	g.Mutex.Unlock()

	slog.Info("Starting leader election", "term", currentTerm, "node", g.Self.ID)

	votes := 1 // Voto próprio
	totalRequests := 0
	aliveNodes := 0 // Contador de nós vivos
	voteChan := make(chan bool, len(g.Nodes))
	responseChan := make(chan bool, len(g.Nodes)) // Canal para contar respostas

	// Solicita votos de outros nós e conta nós vivos
	for id, node := range g.Nodes {
		if id == g.Self.ID {
			aliveNodes++ // Conta este nó como vivo
			continue
		}

		// Verifica se o nó está marcado como morto
		if !node.Alive {
			slog.Debug("Skipping dead node in election", "node", node.ID)
			continue
		}

		aliveNodes++
		totalRequests++
		go g.requestVoteWithResponse(node, currentTerm, voteChan, responseChan)
	}

	// Casos especiais baseados em nós vivos detectados
	if aliveNodes == 1 {
		// Único nó vivo no cluster, torna-se leader automaticamente
		slog.Info("Single active node detected, becoming leader automatically", "aliveNodes", aliveNodes)
		g.becomeLeader(currentTerm)
		return
	}

	if totalRequests == 0 {
		// Todos os outros nós estão mortos, mas ainda pode haver nós na lista
		slog.Info("No other nodes to request votes from, becoming leader", "aliveNodes", aliveNodes)
		g.becomeLeader(currentTerm)
		return
	}

	// Coleta votos com timeout
	timeout := time.After(g.Config.ElectionTimeout)
	responses := 0
	staticMajority := len(g.Nodes)/2 + 1 // Maioria baseada em todos os nós

	for votes < staticMajority && responses < totalRequests {
		select {
		case vote := <-voteChan:
			if vote {
				votes++
			}
		case <-responseChan:
			responses++
			// Verifica se pode formar maioria com nós respondentes
			activeNodes := responses + 1 // +1 para incluir este nó
			dynamicMajority := activeNodes/2 + 1

			// Relaxa regra de mínimo: em cluster de 2 nós, 1 voto próprio é suficiente
			minVotes := 1
			if activeNodes >= 3 {
				minVotes = 2 // Só exige 2 votos se há 3+ nós ativos
			}

			if votes >= dynamicMajority && votes >= minVotes {
				slog.Info("Dynamic majority achieved", "votes", votes, "activeNodes", activeNodes, "dynamicMajority", dynamicMajority, "minVotes", minVotes)
				g.becomeLeader(currentTerm)
				return
			}
		case <-timeout:
			// Timeout: verifica se tem pelo menos maioria dos nós ativos
			activeNodes := responses + 1
			dynamicMajority := activeNodes/2 + 1

			// Relaxa regra de mínimo: em cluster de 2 nós, 1 voto próprio é suficiente
			minVotes := 1
			if activeNodes >= 3 {
				minVotes = 2
			}

			if votes >= dynamicMajority && votes >= minVotes {
				slog.Info("Election won with active majority after timeout", "votes", votes, "activeNodes", activeNodes, "minVotes", minVotes)
				g.becomeLeader(currentTerm)
				return
			}

			// Caso especial: se não há respostas, assume que está sozinho
			if responses == 0 && votes == 1 {
				slog.Info("No responses received, assuming single node cluster", "votes", votes, "aliveNodes", aliveNodes)
				g.becomeLeader(currentTerm)
				return
			}

			slog.Warn("Election timeout", "votes", votes, "needed", staticMajority, "responses", responses, "activeNodes", activeNodes, "dynamicMajority", dynamicMajority)
			return
		}
	}

	// Ganhou a eleição com maioria estática
	if votes >= staticMajority {
		g.becomeLeader(currentTerm)
	} else {
		slog.Warn("Election failed", "votes", votes, "needed", staticMajority, "responses", responses)
	}
}

// requestVote solicita voto de um nó específico
func (g *Gossip) requestVote(node *Node, term int64, voteChan chan bool) {
	g.requestVoteWithResponse(node, term, voteChan, nil)
}

// requestVoteWithResponse solicita voto e notifica sobre resposta
func (g *Gossip) requestVoteWithResponse(node *Node, term int64, voteChan chan bool, responseChan chan bool) {
	conn, err := net.Dial("tcp", node.Address)
	if err != nil {
		slog.Error("Failed to connect for vote request", "node", node.ID, "err", err)
		voteChan <- false
		if responseChan != nil {
			responseChan <- true // Contabiliza como resposta (mesmo que falha)
		}
		return
	}
	defer conn.Close()

	fmt.Fprintf(conn, "VOTE_REQUEST %d %s\n", term, g.Self.ID)

	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		slog.Error("Failed to read vote response", "node", node.ID, "err", err)
		voteChan <- false
		if responseChan != nil {
			responseChan <- true
		}
		return
	}

	response = strings.TrimSpace(response)
	vote := (response == "VOTE_GRANTED")
	voteChan <- vote
	if responseChan != nil {
		responseChan <- true
	}
}

// becomeLeader torna este nó o líder
func (g *Gossip) becomeLeader(term int64) {
	g.Mutex.Lock()
	g.IsLeader = true
	g.Term = term
	g.LeaderState = LeaderState{
		CurrentTerm: term,
		LeaderID:    g.Self.ID,
		LastActive:  time.Now(),
	}
	g.Mutex.Unlock()

	slog.Info("Became leader", "term", term, "node", g.Self.ID)

	// Inicia envio de heartbeats
	go g.sendHeartbeats()

	// Anuncia liderança para todos os nós
	go g.announceLeadership()
}

// sendHeartbeats envia heartbeats periódicos enquanto for líder
func (g *Gossip) sendHeartbeats() {
	ticker := time.NewTicker(g.Config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			g.Mutex.Lock()
			isLeader := g.IsLeader
			term := g.Term
			g.Mutex.Unlock()

			if !isLeader {
				return
			}

			for id, node := range g.Nodes {
				if id == g.Self.ID {
					continue
				}
				go g.sendHeartbeat(node, term)
			}
		}
	}
}

// sendHeartbeat envia heartbeat para um nó específico
func (g *Gossip) sendHeartbeat(node *Node, term int64) {
	conn, err := net.Dial("tcp", node.Address)
	if err != nil {
		slog.Error("Failed to send heartbeat", "node", node.ID, "err", err)
		return
	}
	defer conn.Close()

	fmt.Fprintf(conn, "HEARTBEAT %d %s\n", term, g.Self.ID)
}

// announceLeadership anuncia liderança para todos os nós
func (g *Gossip) announceLeadership() {
	for id, node := range g.Nodes {
		if id == g.Self.ID {
			continue
		}
		go g.sendLeaderAnnouncement(node)
	}
}

// sendLeaderAnnouncement envia anúncio de liderança para um nó
func (g *Gossip) sendLeaderAnnouncement(node *Node) {
	conn, err := net.Dial("tcp", node.Address)
	if err != nil {
		slog.Error("Failed to announce leadership", "node", node.ID, "err", err)
		return
	}
	defer conn.Close()

	g.Mutex.Lock()
	term := g.Term
	g.Mutex.Unlock()

	fmt.Fprintf(conn, "LEADER_ANNOUNCE %d %s\n", term, g.Self.ID)
}

// ========== HANDLERS PARA MENSAGENS LEADER-FOLLOWER ==========

// handleVoteRequest processa requisições de voto
func (g *Gossip) handleVoteRequest(parts []string, conn net.Conn) {
	if len(parts) < 3 {
		fmt.Fprintf(conn, "VOTE_DENIED\n")
		return
	}

	term, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		fmt.Fprintf(conn, "VOTE_DENIED\n")
		return
	}
	candidateID := parts[2]

	g.Mutex.Lock()
	defer g.Mutex.Unlock()

	// Lógica de votação melhorada para evitar split-brain
	canVote := false
	reason := ""

	if term > g.Term {
		// Termo mais alto sempre pode receber voto
		canVote = true
		reason = "higher term"
	} else if term == g.Term && (g.VotedFor == "" || g.VotedFor == candidateID) {
		// Mesmo termo: vota se não votou ainda ou já votou neste candidato
		canVote = true
		reason = "same term, eligible"
	} else if term >= g.Term-1 && g.VotedFor == candidateID {
		// Termo próximo: permite re-voto no mesmo candidato para resolver split-brain
		canVote = true
		reason = "recent term, same candidate"
	}

	if canVote {
		g.Term = term
		g.VotedFor = candidateID
		g.IsLeader = false
		fmt.Fprintf(conn, "VOTE_GRANTED\n")
		slog.Info("Granted vote", "candidate", candidateID, "term", term, "reason", reason)
	} else {
		fmt.Fprintf(conn, "VOTE_DENIED\n")
		slog.Debug("Denied vote", "candidate", candidateID, "term", term, "currentTerm", g.Term, "votedFor", g.VotedFor, "reason", "conditions not met")
	}
}

// handleHeartbeat processa heartbeats do líder
func (g *Gossip) handleHeartbeat(parts []string, conn net.Conn) {
	if len(parts) < 3 {
		return
	}

	term, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return
	}
	leaderID := parts[2]

	g.Mutex.Lock()
	defer g.Mutex.Unlock()

	if term >= g.Term {
		g.Term = term
		g.IsLeader = false
		g.LeaderState = LeaderState{
			CurrentTerm: term,
			LeaderID:    leaderID,
			LastActive:  time.Now(),
		}
		g.LastHeartbeat = time.Now()
		g.VotedFor = "" // Reset vote for new term
	}

	fmt.Fprintf(conn, "HEARTBEAT_ACK\n")
}

// handleLeaderAnnounce processa anúncios de liderança
func (g *Gossip) handleLeaderAnnounce(parts []string, conn net.Conn) {
	if len(parts) < 3 {
		return
	}

	term, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return
	}
	leaderID := parts[2]

	g.Mutex.Lock()
	defer g.Mutex.Unlock()

	if term >= g.Term {
		g.Term = term
		g.IsLeader = false
		g.LeaderState = LeaderState{
			CurrentTerm: term,
			LeaderID:    leaderID,
			LastActive:  time.Now(),
		}
		g.LastHeartbeat = time.Now()
		slog.Info("New leader announced", "leader", leaderID, "term", term)
	}

	fmt.Fprintf(conn, "LEADER_ACK\n")
}

// handleShutdown processa comandos de shutdown remoto
func (g *Gossip) handleShutdown(parts []string, conn net.Conn) {
	slog.Info("Shutdown command received", "node", g.Self.ID)

	// Responde ao cliente antes de fazer shutdown
	fmt.Fprintf(conn, "SHUTDOWN_ACK node %s shutting down\n", g.Self.ID)

	// Se for o leader, anuncia que está saindo
	if g.IsLeader && g.Config != nil && g.Config.IsLeaderFollowerMode() {
		slog.Info("Leader shutting down, triggering new election", "node", g.Self.ID)
		g.Mutex.Lock()
		g.IsLeader = false
		g.LeaderState.LeaderID = ""
		g.Mutex.Unlock()
	}

	// Pequeno delay para garantir que a resposta seja enviada
	go func() {
		time.Sleep(100 * time.Millisecond)
		slog.Info("Shutting down node", "node", g.Self.ID)
		os.Exit(0)
	}()
}
