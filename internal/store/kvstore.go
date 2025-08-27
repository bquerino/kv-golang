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

// Hint representa um item que precisa ser enviado a um nó que estava
// indisponível no momento da escrita original. Além do valor, também
// carregamos o VectorClock para garantir conciliação quando o nó voltar.
type Hint struct {
	Key         string
	Value       string
	TargetID    string // O nó que deveria receber o dado originalmente
	Timestamp   time.Time
	VectorClock *vectorclock.VectorClock
}

// KeyValueStore gerencia os dados e lida com escrita em disco, reconciliação, e hinted handoff
type KeyValueStore struct {
	Data            map[string]*DataItem        // Armazena os dados na memória
	HintedData      map[string]map[string]*Hint // Armazena dados para hinted handoff
	PageManager     *PageManager                // Gerenciamento de páginas para escrita em disco
	Gossip          *Gossip                     // Integração com o protocolo Gossip
	ConsistentHash  *ConsistentHashing          // Integração com Consistent Hashing
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
		HintedData:      make(map[string]map[string]*Hint),
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

// Put armazena a chave localmente e propaga a atualização para todos os nós
// conhecidos. Caso algum nó esteja indisponível, o valor é guardado em
// HintedData para posterior entrega.
func (kv *KeyValueStore) Put(key, value string) {
	vc := kv.putLocal(key, value)
	kv.broadcastPut(key, value, vc)
}

// broadcastPut envia a operação PUT para todos os nós exceto o próprio
// processando hinted handoff quando necessário.
func (kv *KeyValueStore) broadcastPut(key, value string, vc *vectorclock.VectorClock) {
	for id, node := range kv.Gossip.Nodes {
		if id == kv.Gossip.Self.ID {
			continue
		}

		if !kv.Gossip.IsNodeAlive(id) {
			log.Printf("Node %s is down. Storing hinted handoff for key %s", id, key)
			kv.addHint(key, value, vc, id)
			continue
		}

		kv.Gossip.sendPutToNode(node, key, value, vc)
	}
}

func (kv *KeyValueStore) addHint(key, value string, vc *vectorclock.VectorClock, targetID string) {
	kv.Mutex.Lock()
	defer kv.Mutex.Unlock()
	if _, ok := kv.HintedData[key]; !ok {
		kv.HintedData[key] = make(map[string]*Hint)
	}
	kv.HintedData[key][targetID] = &Hint{
		Key:         key,
		Value:       value,
		TargetID:    targetID,
		Timestamp:   time.Now(),
		VectorClock: vc,
	}
}

// Get tenta primeiro recuperar o valor localmente. Caso não exista, envia uma
// requisição ao nó responsável pela chave. Esse caminho adicional garante que
// todos os nós possam responder leituras mesmo que o responsável esteja
// indisponível.
func (kv *KeyValueStore) Get(key string) (string, *vectorclock.VectorClock, bool) {
	if value, vc, found := kv.getLocal(key); found {
		return value, vc, true
	}

	vnode := kv.ConsistentHash.GetNode(key)

	if vnode.ID != kv.Gossip.Self.ID {
		if !kv.Gossip.IsNodeAlive(vnode.ID) {
			log.Printf("Node %s is down. Key %s might be in hinted handoff.", vnode.ID, key)
			return "", nil, false
		}
		return kv.Gossip.sendGetToNode(vnode, key)
	}

	return "", nil, false
}

// putLocal armazena a chave localmente
func (kv *KeyValueStore) putLocal(key, value string) *vectorclock.VectorClock {
	kv.Mutex.Lock()
	defer kv.Mutex.Unlock()

	var vc *vectorclock.VectorClock
	if item, exists := kv.Data[key]; exists {
		item.VectorClock.Increment(kv.Gossip.Self.ID)
		log.Printf("Updated key %s with new value. VectorClock: %s", key, item.VectorClock.String())
		item.Value = value
		vc = item.VectorClock
	} else {
		vc = vectorclock.NewVectorClock()

		vc.Increment(kv.Gossip.Self.ID)
		kv.Data[key] = &DataItem{Value: value, VectorClock: vc}
		log.Printf("Stored key %s with initial VectorClock: %s", key, vc.String())
	}

	kv.writeDataToDisk(key, value)
	return vc
}

// getLocal retorna o valor armazenado localmente
func (kv *KeyValueStore) getLocal(key string) (string, *vectorclock.VectorClock, bool) {
	kv.Mutex.Lock()
	defer kv.Mutex.Unlock()

	if item, exists := kv.Data[key]; exists {
		return item.Value, item.VectorClock, true
	}

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

	for key, hints := range kv.HintedData {
		for targetID, hint := range hints {
			if kv.Gossip.IsNodeAlive(targetID) {
				log.Printf("Reapplying hinted handoff for key %s to node %s", key, targetID)
				if node, ok := kv.Gossip.Nodes[targetID]; ok {
					kv.Gossip.sendPutToNode(node, hint.Key, hint.Value, hint.VectorClock)
				}
				delete(hints, targetID)
			} else {
				log.Printf("Node %s still down, keeping hinted handoff for key %s", targetID, key)
			}
		}
		if len(hints) == 0 {
			delete(kv.HintedData, key)
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
