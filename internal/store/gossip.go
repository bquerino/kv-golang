package store

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/bquerino/kv-g/internal/vectorclock"
)

type Node struct {
	ID        string
	Address   string
	Alive     bool
	LastCheck time.Time
}

type Gossip struct {
	Nodes          map[string]*Node
	Self           *Node
	Coordinator    *Node
	Interval       time.Duration
	ConsistentHash *ConsistentHashing
	KeyValueStore  *KeyValueStore // Integração com o KeyValueStore
	Mutex          sync.Mutex
}

// Inicializa o Gossip Protocol e configura o Consistent Hashing com vNodes
func NewGossip(selfID, address string, interval time.Duration, vNodes int) *Gossip {
	self := &Node{
		ID:      selfID,
		Address: address,
		Alive:   true,
	}

	gossip := &Gossip{
		Nodes:          make(map[string]*Node),
		Self:           self,
		Interval:       interval,
		ConsistentHash: NewConsistentHashing(vNodes),
	}

	// Inicializa o KeyValueStore integrado com o Gossip e PageManager
	gossip.KeyValueStore, _ = NewKeyValueStore(gossip, gossip.ConsistentHash, 5*time.Second, "data_pages.db")

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
		log.Printf("Error starting TCP server: %v", err)
		return
	}

	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		go g.handleConnection(conn)
	}
}

// Envia uma mensagem de verificação de saúde para o nó
func (g *Gossip) sendMessage(node *Node) {
	conn, err := net.Dial("tcp", node.Address)
	if err != nil {
		log.Printf("Error connecting to node %s: %v", node.ID, err)
		g.markNodeDead(node)
		return
	}
	defer conn.Close()

	// Envia um ping simples
	log.Printf("Sending PING to node %s", node.ID)
	fmt.Fprintf(conn, "PING from %s\n", g.Self.ID)
}

// Lida com uma conexão recebida (PING de outro nó)
func (g *Gossip) handleConnection(conn net.Conn) {
	defer conn.Close()

	var nodeID string
	fmt.Fscanf(conn, "PING from %s\n", &nodeID)

	g.Mutex.Lock()
	defer g.Mutex.Unlock()

	if node, exists := g.Nodes[nodeID]; exists {
		node.LastCheck = time.Now()
		node.Alive = true
		log.Printf("Received PING from node %s", node.ID)
	} else {
		log.Printf("Unknown node: %s", nodeID)
	}
}

// Marca um nó como morto se ele não responder
func (g *Gossip) markNodeDead(node *Node) {
	g.Mutex.Lock()
	defer g.Mutex.Unlock()

	node.Alive = false
	log.Printf("Node %s is marked as dead", node.ID)
	if g.Coordinator != nil && g.Coordinator.ID == node.ID {
		log.Printf("Coordinator %s is down! Initiating election.", node.ID)
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
	log.Println("Starting election...")

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
		log.Printf("Error connecting to node %s during election: %v", node.ID, err)
		g.markNodeDead(node)
		return
	}
	defer conn.Close()

	log.Printf("Sending ELECTION message to node %s", node.ID)
	fmt.Fprintf(conn, "ELECTION from %s\n", g.Self.ID)

	// Espera resposta de "OK"
	var response string
	fmt.Fscanf(conn, "%s\n", &response)
	if response == "OK" {
		log.Printf("Node %s responded to election", node.ID)
		return
	}
}

// Define o nó atual como coordenador
func (g *Gossip) becomeCoordinator() {
	log.Println("Becoming the coordinator.")
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
		log.Printf("Error connecting to node %s to announce coordinator: %v", node.ID, err)
		g.markNodeDead(node)
		return
	}
	defer conn.Close()

	log.Printf("Announcing self as COORDINATOR to node %s", node.ID)
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

// Envia um PUT para o KeyValueStore
func (g *Gossip) Put(key, value string) {
	g.KeyValueStore.Put(key, value)
}

// Envia um GET para o KeyValueStore
func (g *Gossip) Get(key string) (string, *vectorclock.VectorClock, bool) {
	return g.KeyValueStore.Get(key)
}

// Envia um DELETE para o KeyValueStore (implementar no KeyValueStore, se ainda não estiver feito)
func (g *Gossip) Delete(key string) {
	// Adicione o método Delete no KeyValueStore para lidar com a remoção de chaves
	// g.KeyValueStore.Delete(key)
	log.Println("Delete operation is not yet implemented in KeyValueStore.")
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
		log.Printf("Node: %s, Address: %s, Status: %s", id, node.Address, status)
	}
}
