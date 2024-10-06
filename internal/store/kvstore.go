package store

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/bquerino/kv-g/internal/vectorclock"
)

const PageSize = 4096 // Tamanho fixo da página (4KB)

type DataItem struct {
	Value       string
	VectorClock *vectorclock.VectorClock // Versão do dado
}

type Hint struct {
	Key       string
	Value     string
	TargetID  string // O nó que deveria receber o dado originalmente
	Timestamp time.Time
}

// KeyValueStore gerencia os dados e lida com escrita em disco, reconciliação, e hinted handoff
type KeyValueStore struct {
	Data            map[string]*DataItem // Armazena os dados na memória
	HintedData      map[string]*Hint     // Armazena dados para hinted handoff
	PageManager     *PageManager         // Gerenciamento de páginas para escrita em disco
	Gossip          *Gossip              // Integração com o protocolo Gossip
	ConsistentHash  *ConsistentHashing   // Integração com Consistent Hashing
	Mutex           sync.Mutex
	HandoffInterval time.Duration // Intervalo para verificar hinted handoff
}

// Page gerencia a estrutura de uma página no disco
type Page struct {
	ID     int64  // Identificador único da página
	Buffer []byte // Buffer de dados da página
	Used   int    // Bytes atualmente usados na página
}

// PageManager gerencia a escrita e leitura de páginas no disco
type PageManager struct {
	File       *os.File
	NextPageID int64
	Mutex      sync.Mutex
}

// Função para inicializar o KeyValueStore com todos os componentes integrados
func NewKeyValueStore(gossip *Gossip, consistentHash *ConsistentHashing, handoffInterval time.Duration, pageFileName string) (*KeyValueStore, error) {
	pageManager, err := NewPageManager(pageFileName)
	if err != nil {
		return nil, err
	}

	return &KeyValueStore{
		Data:            make(map[string]*DataItem),
		HintedData:      make(map[string]*Hint),
		PageManager:     pageManager,
		Gossip:          gossip,
		ConsistentHash:  consistentHash,
		HandoffInterval: handoffInterval,
	}, nil
}

// Função para inicializar o PageManager e abrir o arquivo de páginas
func NewPageManager(filename string) (*PageManager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}

	return &PageManager{
		File:       file,
		NextPageID: 0,
	}, nil
}

// Função para alocar uma nova página
func (pm *PageManager) AllocatePage() *Page {
	pm.Mutex.Lock()
	defer pm.Mutex.Unlock()

	page := &Page{
		ID:     pm.NextPageID,
		Buffer: make([]byte, PageSize),
		Used:   0,
	}
	pm.NextPageID++
	return page
}

// Função para escrever uma página no disco
func (pm *PageManager) WritePage(page *Page) error {
	pm.Mutex.Lock()
	defer pm.Mutex.Unlock()

	offset := page.ID * PageSize
	_, err := pm.File.Seek(offset, 0)
	if err != nil {
		return err
	}
	_, err = pm.File.Write(page.Buffer)
	if err != nil {
		return err
	}
	return nil
}

// Função para ler uma página do disco
func (pm *PageManager) ReadPage(pageID int64) (*Page, error) {
	pm.Mutex.Lock()
	defer pm.Mutex.Unlock()

	offset := pageID * PageSize
	_, err := pm.File.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	buffer := make([]byte, PageSize)
	_, err = pm.File.Read(buffer)
	if err != nil {
		return nil, err
	}

	return &Page{
		ID:     pageID,
		Buffer: buffer,
		Used:   PageSize,
	}, nil
}

// Função para persistir dados em uma página no disco
func (kv *KeyValueStore) writeDataToDisk(key, value string) {
	page := kv.PageManager.AllocatePage()

	// Escreve a chave e o valor no buffer da página
	binaryKey := []byte(key)
	binaryValue := []byte(value)

	copy(page.Buffer, binaryKey)
	copy(page.Buffer[len(binaryKey):], binaryValue)

	err := kv.PageManager.WritePage(page)
	if err != nil {
		log.Printf("Error writing page for key %s: %v", key, err)
	} else {
		log.Printf("Wrote key %s to disk", key)
	}
}

func (kv *KeyValueStore) Put(key, value string) {
	kv.Mutex.Lock()
	defer kv.Mutex.Unlock()

	vnode := kv.ConsistentHash.GetNode(key)

	// Se o nó responsável pela chave está offline, fazer hinted handoff
	if !kv.Gossip.IsNodeAlive(vnode.ID) {
		log.Printf("Node %s is down. Storing hinted handoff for key %s", vnode.ID, key)
		kv.HintedData[key] = &Hint{
			Key:       key,
			Value:     value,
			TargetID:  vnode.ID,
			Timestamp: time.Now(),
		}
		return
	}

	// Se a chave já existe, faz merge dos vector clocks
	if item, exists := kv.Data[key]; exists {
		item.VectorClock.Increment(kv.Gossip.Self.ID) // Incrementa o Vector Clock local
		log.Printf("Updated key %s with new value. VectorClock: %s", key, item.VectorClock.String())
		item.Value = value
	} else {
		// Se for um novo dado, cria um Vector Clock e adiciona
		vc := vectorclock.NewVectorClock()
		vc.Increment(kv.Gossip.Self.ID)
		kv.Data[key] = &DataItem{
			Value:       value,
			VectorClock: vc,
		}
		log.Printf("Stored key %s with initial VectorClock: %s", key, vc.String())
	}

	// Persistir o dado no disco usando páginas
	kv.writeDataToDisk(key, value)
}

func (kv *KeyValueStore) Get(key string) (string, *vectorclock.VectorClock, bool) {
	kv.Mutex.Lock()
	defer kv.Mutex.Unlock()

	vnode := kv.ConsistentHash.GetNode(key)

	// Verifica se o nó responsável está online
	if kv.Gossip.IsNodeAlive(vnode.ID) {
		if item, exists := kv.Data[key]; exists {
			return item.Value, item.VectorClock, true
		}
		log.Printf("Key %s not found in node %s", key, vnode.ID)
	} else {
		log.Printf("Node %s is down. Key %s might be in hinted handoff.", vnode.ID, key)
	}

	// Se não estiver na memória, tenta carregar do disco
	value, found := kv.readDataFromDisk(key)
	if found {
		return value, nil, true
	}

	return "", nil, false
}

// Função para ler dados de uma página do disco
func (kv *KeyValueStore) readDataFromDisk(key string) (string, bool) {
	pageID := kv.getPageIDForKey(key)

	page, err := kv.PageManager.ReadPage(pageID)
	if err != nil {
		log.Printf("Error reading page for key %s: %v", key, err)
		return "", false
	}

	value := string(page.Buffer)
	log.Printf("Read key %s from disk", key)
	return value, true
}

// Função que mapeia uma chave para um ID de página
func (kv *KeyValueStore) getPageIDForKey(key string) int64 {
	return int64(len(key)) // Exemplo simples de mapeamento de chave para página
}

// Função para processar hinted handoff e reenviar dados para o nó de destino quando ele voltar
func (kv *KeyValueStore) StartHintedHandoff() {
	ticker := time.NewTicker(kv.HandoffInterval)
	for range ticker.C {
		kv.processHintedHandoff()
	}
}

// Processa hinted handoffs e tenta reenviar os dados para o nó original
func (kv *KeyValueStore) processHintedHandoff() {
	kv.Mutex.Lock()
	defer kv.Mutex.Unlock()

	for key, hint := range kv.HintedData {
		if kv.Gossip.IsNodeAlive(hint.TargetID) {
			log.Printf("Reapplying hinted handoff for key %s to node %s", key, hint.TargetID)
			kv.Data[key] = &DataItem{
				Value: hint.Value,
			}
			delete(kv.HintedData, key) // Remove o hint após a transferência
		} else {
			log.Printf("Node %s still down, keeping hinted handoff for key %s", hint.TargetID, key)
		}
	}
}

// Função para resolver conflitos de escrita concorrente usando Vector Clocks
func (kv *KeyValueStore) ResolveConflicts(key string, newValue string, newVectorClock *vectorclock.VectorClock) {
	kv.Mutex.Lock()
	defer kv.Mutex.Unlock()

	if item, exists := kv.Data[key]; exists {
		comparison := item.VectorClock.Compare(newVectorClock)
		switch comparison {
		case -1: // Novo dado é mais recente
			log.Printf("Key %s updated with more recent value. New VectorClock: %s", key, newVectorClock.String())
			item.Value = newValue
			item.VectorClock.Merge(newVectorClock)
		case 0: // Conflito detectado
			log.Printf("Conflict detected for key %s. Keeping both versions.", key)
		case 1: // Dado existente é mais recente, nenhuma atualização aplicada
			log.Printf("Existing value for key %s is more recent. No update applied.", key)
		}
	} else {
		kv.Data[key] = &DataItem{
			Value:       newValue,
			VectorClock: newVectorClock,
		}
		log.Printf("Stored new key %s with VectorClock: %s", key, newVectorClock.String())
	}
}
