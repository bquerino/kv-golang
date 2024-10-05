package store

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/bquerino/kv-g/internal/vectorclock"
)

type KVStore struct {
	store        map[string]string                  // Armazena chave-valor
	vectorClocks map[string]vectorclock.VectorClock // Armazena Vector Clocks para cada chave
	pageIdx      int                                // Índice da página atual para persistência
	mu           sync.RWMutex
	nodeID       string      // Identificador do nó (ex: "node1")
	ring         *HashRing   // Anel de hash consistente
	replicas     int         // Número de réplicas
	gossipNode   *GossipNode // Nó de comunicação Gossip
	dataFile     string      // Caminho para o arquivo de persistência
}

// NewKVStore cria um novo KV-Store associado a um nó específico
func NewKVStore(nodeID string, ring *HashRing, replicas int, gossipNode *GossipNode) *KVStore {
	kv := &KVStore{
		store:        make(map[string]string),
		vectorClocks: make(map[string]vectorclock.VectorClock),
		nodeID:       nodeID,
		ring:         ring,
		replicas:     replicas,
		gossipNode:   gossipNode,
		dataFile:     fmt.Sprintf("%s_data.json", nodeID), // Arquivo de persistência associado ao nó
	}

	// Carrega os dados do arquivo na inicialização
	kv.LoadFromDisk()
	return kv
}

// Set adiciona ou atualiza uma chave-valor no KV-Store e salva no disco
func (kv *KVStore) Set(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	fmt.Printf("Definindo chave '%s' com valor '%s' no KV-Store do nó '%s'.\n", key, value, kv.nodeID)

	// Atualiza o vector clock
	if _, exists := kv.vectorClocks[key]; !exists {
		kv.vectorClocks[key] = vectorclock.VectorClock{}
		fmt.Printf("Criando novo Vector Clock para a chave '%s' no nó '%s'.\n", key, kv.nodeID)
	}
	kv.vectorClocks[key].Update(kv.nodeID)
	fmt.Printf("Vector Clock atualizado para a chave '%s': %v\n", key, kv.vectorClocks[key])

	kv.store[key] = value

	// Chamar a função para persistir no disco
	kv.saveToDiskInternal()
	fmt.Println("Dados salvos com sucesso.")
}

// Get recupera o valor associado a uma chave do KV-Store
func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	value, exists := kv.store[key]
	return value, exists
}

// Reconcile faz a reconciliação de uma chave entre nós
func (kv *KVStore) Reconcile(key, incomingValue string, incomingVC vectorclock.VectorClock, incomingNodeID string, incomingTimestamp time.Time) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	existingValue, exists := kv.store[key]
	existingVC, vcExists := kv.vectorClocks[key]

	// Se não houver valor local, adicione o valor recebido diretamente
	if !exists || !vcExists {
		kv.store[key] = incomingValue
		kv.vectorClocks[key] = incomingVC
		kv.SaveToDisk() // Persistir no disco
		fmt.Printf("Chave %s inserida via Gossip no nó %s\n", key, kv.nodeID)
		return
	}

	// Comparar Vector Clocks para decidir qual valor manter
	comparison := existingVC.Compare(incomingVC)

	switch comparison {
	case 1:
		// Local é mais recente, mantendo o valor atual
		fmt.Println("Local é mais recente, mantendo o valor atual.")
	case -1:
		// Valor recebido é mais recente, substitui o valor local
		kv.store[key] = incomingValue
		kv.vectorClocks[key] = incomingVC
		kv.SaveToDisk() // Persistir no disco
		fmt.Printf("Chave %s atualizada via Gossip no nó %s\n", key, kv.nodeID)
	case 0:
		// Conflito, resolvendo com merge
		kv.resolveConflictWithMerge(key, incomingValue, existingValue)
		kv.SaveToDisk() // Persistir no disco
	}
}

// resolveConflictWithMerge resolve o conflito entre dois valores, combinando-os
func (kv *KVStore) resolveConflictWithMerge(key, incomingValue, existingValue string) {
	// Implementação simples: concatenar os valores como uma forma de resolução de conflitos
	mergedValue := mergeValues(existingValue, incomingValue)
	kv.store[key] = mergedValue
	fmt.Printf("Conflito resolvido para chave %s. Valores mesclados: %s\n", key, mergedValue)
}

// mergeValues é uma função auxiliar para combinar dois valores em caso de conflito
func mergeValues(value1, value2 string) string {
	if value1 == value2 {
		return value1 // Se os valores forem iguais, mantenha apenas um
	}
	return value1 + " | " + value2 // Combina os valores com um delimitador
}

// saveToDiskInternal salva os dados no disco sem bloquear o mutex
func (kv *KVStore) saveToDiskInternal() {
	fmt.Printf("Tentando salvar os dados no arquivo %s\n", kv.dataFile)
	file, err := os.Create(kv.dataFile)
	if err != nil {
		fmt.Println("Erro ao criar arquivo de persistência:", err)
		return
	}
	defer func() {
		if err := file.Close(); err != nil {
			fmt.Println("Erro ao fechar o arquivo:", err)
		}
	}()

	encoder := json.NewEncoder(file)
	err = encoder.Encode(kv.store)
	if err != nil {
		fmt.Println("Erro ao salvar os dados no disco:", err)
		return
	}
	fmt.Println("Dados salvos com sucesso no disco.")
}

// SaveToDisk salva os dados do KV-Store no arquivo JSON
func (kv *KVStore) SaveToDisk() {
	kv.mu.Lock() // Reativar o mutex para garantir concorrência segura
	defer kv.mu.Unlock()

	fmt.Printf("Tentando salvar os dados no arquivo %s\n", kv.dataFile)
	file, err := os.Create(kv.dataFile)
	if err != nil {
		fmt.Println("Erro ao criar arquivo de persistência:", err)
		return
	}
	defer func() {
		if err := file.Close(); err != nil {
			fmt.Println("Erro ao fechar o arquivo:", err)
		}
	}()

	encoder := json.NewEncoder(file)
	err = encoder.Encode(kv.store)
	if err != nil {
		fmt.Println("Erro ao salvar os dados no disco:", err)
		return
	}
	fmt.Println("Dados salvos com sucesso no disco.")
}

// LoadFromDisk carrega os dados do arquivo JSON para a memória do KV-Store
func (kv *KVStore) LoadFromDisk() {
	file, err := os.Open(kv.dataFile)
	if err != nil {
		if os.IsNotExist(err) {
			// Se o arquivo não existir, inicializamos com um mapa vazio
			fmt.Println("Nenhum arquivo de persistência encontrado, iniciando um novo armazenamento.")
			return
		}
		fmt.Println("Erro ao abrir o arquivo de persistência:", err)
		return
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&kv.store)
	if err != nil {
		fmt.Println("Erro ao carregar os dados do disco:", err)
	}
}
