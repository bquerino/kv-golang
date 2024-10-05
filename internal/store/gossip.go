package store

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"
)

// GossipNode representa um nó que comunica via Gossip Protocol
type GossipNode struct {
	address       string               // Endereço do nó
	peers         []string             // Lista de peers
	KVStore       *KVStore             // O KV-Store local (agora exportado)
	incoming      chan GossipMsg       // Canal para mensagens recebidas
	outgoing      chan GossipMsg       // Canal para mensagens enviadas
	hintedHandoff map[string]GossipMsg // Mapa para armazenar o hinted handoff
	hintedFile    string               // Arquivo para persistir o hinted handoff
	timeout       time.Duration        // Timeout para detectar falhas
}

// GossipMsg representa uma mensagem de Gossip
type GossipMsg struct {
	Key         string         // Chave sendo propagada
	Value       string         // Valor da chave
	VectorClock map[string]int // Vector clock
	NodeID      string         // Identificador do nó
}

// NewGossipNode cria um novo nó Gossip
func NewGossipNode(address string, peers []string, kvStore *KVStore, timeout time.Duration, hintedFile string) *GossipNode {
	node := &GossipNode{
		address:       address,
		peers:         peers,   // Mantemos os peers
		KVStore:       kvStore, // Substituímos kvStore por KVStore
		incoming:      make(chan GossipMsg),
		outgoing:      make(chan GossipMsg),
		hintedHandoff: make(map[string]GossipMsg),
		hintedFile:    hintedFile,
		timeout:       timeout,
	}
	node.loadHintedHandoffFromDisk() // Carrega o Hinted Handoff do disco na inicialização
	go node.start()
	return node
}

// start inicia o loop de comunicação Gossip
func (node *GossipNode) start() {
	go node.listen()

	for {
		time.Sleep(time.Duration(2) * time.Second)

		select {
		case msg := <-node.outgoing:
			node.gossipToPeers(msg)
		}
	}
}

// listen inicia o listener TCP para ouvir mensagens Gossip
func (node *GossipNode) listen() {
	listener, err := net.Listen("tcp", node.address)
	if err != nil {
		fmt.Println("Erro ao iniciar listener:", err)
		return
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Erro ao aceitar conexão:", err)
			continue
		}

		go node.handleIncoming(conn)
	}
}

// handleIncoming lida com mensagens recebidas
func (node *GossipNode) handleIncoming(conn net.Conn) {
	defer conn.Close()

	var msg GossipMsg
	err := decode(conn, &msg)
	if err != nil {
		fmt.Println("Erro ao decodificar mensagem:", err)
		return
	}

	// Processar a mensagem recebida, reconciliando os dados no KV-Store
	node.KVStore.Reconcile(msg.Key, msg.Value, msg.VectorClock, msg.NodeID, time.Now())
}

// gossipToPeers envia uma mensagem para os peers com timeout
func (node *GossipNode) gossipToPeers(msg GossipMsg) {
	for _, peer := range node.peers {
		go node.sendToPeerWithTimeout(peer, msg)
	}
}

// sendToPeerWithTimeout envia mensagem a um peer com timeout e aplica hinted handoff se necessário
func (node *GossipNode) sendToPeerWithTimeout(peer string, msg GossipMsg) {
	conn, err := net.DialTimeout("tcp", peer, node.timeout)
	if err != nil {
		fmt.Println("Timeout ou erro ao conectar com o peer:", peer)
		// Se o peer não estiver disponível, aplicar hinted handoff
		node.hintedHandoff[msg.Key] = msg
		node.persistHintedHandoffToDisk() // Persiste no disco após a falha
		fmt.Printf("Hinted Handoff: chave %s armazenada para entrega futura ao peer %s.\n", msg.Key, peer)
		return
	}
	defer conn.Close()

	err = encode(conn, msg)
	if err != nil {
		fmt.Println("Erro ao enviar mensagem para o peer:", err)
	}
}

// persistHintedHandoffToDisk persiste o hinted handoff no disco
func (node *GossipNode) persistHintedHandoffToDisk() {
	file, err := os.Create(node.hintedFile)
	if err != nil {
		fmt.Println("Erro ao criar arquivo de hinted handoff:", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	err = encoder.Encode(node.hintedHandoff)
	if err != nil {
		fmt.Println("Erro ao persistir hinted handoff no disco:", err)
	}
}

// / loadHintedHandoffFromDisk carrega o hinted handoff do disco
func (node *GossipNode) loadHintedHandoffFromDisk() {
	// Tenta abrir o arquivo de hinted handoff
	file, err := os.Open(node.hintedFile)
	if err != nil {
		// Se o arquivo não existir, cria um arquivo vazio
		if os.IsNotExist(err) {
			fmt.Printf("Arquivo de hinted handoff não encontrado. Criando novo arquivo: %s\n", node.hintedFile)
			// Cria um novo arquivo vazio
			file, err = os.Create(node.hintedFile)
			if err != nil {
				fmt.Println("Erro ao criar arquivo de hinted handoff:", err)
				return
			}
			defer file.Close()
			// Inicializa o mapa de hinted handoff como vazio
			node.hintedHandoff = make(map[string]GossipMsg)
			return
		}
		fmt.Println("Erro ao abrir arquivo de hinted handoff:", err)
		return
	}
	defer file.Close()

	// Verifica se o arquivo está vazio
	stat, err := file.Stat()
	if err != nil {
		fmt.Println("Erro ao obter informações do arquivo:", err)
		return
	}

	// Se o arquivo estiver vazio, inicializa o mapa como vazio e retorna
	if stat.Size() == 0 {
		fmt.Printf("Arquivo de hinted handoff %s está vazio. Inicializando mapa vazio.\n", node.hintedFile)
		node.hintedHandoff = make(map[string]GossipMsg)
		return
	}

	// Decodifica o conteúdo do arquivo se ele não estiver vazio
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&node.hintedHandoff)
	if err != nil {
		fmt.Println("Erro ao carregar hinted handoff do disco:", err)
	}
}

// RetryHintedHandoff tenta reenviar as mensagens do hinted handoff
func (node *GossipNode) RetryHintedHandoff() {
	for key, msg := range node.hintedHandoff {
		for _, peer := range node.peers {
			conn, err := net.Dial("tcp", peer)
			if err == nil {
				err = encode(conn, msg)
				if err == nil {
					fmt.Printf("Hinted Handoff resolvido para chave %s no peer %s\n", key, peer)
					delete(node.hintedHandoff, key)
					node.persistHintedHandoffToDisk() // Atualiza o arquivo após remoção
				}
				conn.Close()
			}
		}
	}
}

// encode serializa a mensagem para o formato JSON e a envia pela conexão
func encode(conn net.Conn, msg GossipMsg) error {
	encoder := json.NewEncoder(conn)
	err := encoder.Encode(&msg)
	if err != nil {
		fmt.Println("Erro ao serializar mensagem:", err)
		return err
	}
	return nil
}

// decode desserializa a mensagem recebida via conexão para o formato GossipMsg
func decode(conn net.Conn, msg *GossipMsg) error {
	decoder := json.NewDecoder(conn)
	err := decoder.Decode(msg)
	if err != nil {
		fmt.Println("Erro ao desserializar mensagem:", err)
		return err
	}
	return nil
}
