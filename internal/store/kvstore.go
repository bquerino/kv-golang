package store

import (
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bquerino/kv-golang/internal/metrics"
	"github.com/bquerino/kv-golang/internal/vectorclock"
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
	LogFile         *os.File                    // Arquivo de log append-only
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
	logFile, err := os.OpenFile(pageFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &KeyValueStore{
		Data:            make(map[string]*DataItem),
		HintedData:      make(map[string]map[string]*Hint),
		LogFile:         logFile,
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
		slog.Error("[PageManager] Seek failed", "pageID", pageID, "err", err)
		return nil, err
	}

	buffer := make([]byte, PageSize)
	n, err := pm.File.Read(buffer)
	if err != nil {
		if err.Error() == "EOF" {
			slog.Debug("[PageManager] Read EOF, returning empty page", "pageID", pageID)
			return &Page{ID: pageID, Buffer: make([]byte, PageSize), Used: 0}, nil
		}
		slog.Error("[PageManager] Read failed", "pageID", pageID, "err", err)
		return nil, err
	}
	slog.Debug("[PageManager] Read bytes", "n", n, "pageID", pageID)
	return &Page{
		ID:     pageID,
		Buffer: buffer,
		Used:   n,
	}, nil
}

// Função para persistir dados em uma página no disco
func (kv *KeyValueStore) writeDataToDisk(key, value string) {
	// Log append-only: escreve 'key:value' como nova linha
	line := key + ":" + value + "\n"
	if kv.LogFile != nil {
		_, err := kv.LogFile.WriteString(line)
		if err != nil {
			slog.Error("[LogFile] Falha ao persistir dado", "key", key, "err", err)
		} else {
			slog.Debug("[LogFile] Persistido", "key", key, "value", value)
		}
	}
}

// Put armazena a chave localmente e propaga a atualização para todos os nós
// conhecidos. Caso algum nó esteja indisponível, o valor é guardado em
// HintedData para posterior entrega.
func (kv *KeyValueStore) Put(key, value string) {
	start := time.Now()
	defer func() {
		metrics.RequestDuration.WithLabelValues("PUT").Observe(time.Since(start).Seconds())
		metrics.RequestsTotal.WithLabelValues("PUT").Inc()
	}()

	slog.Info("[Put] Iniciando PUT", "key", key, "value", value)
	vc := kv.putLocal(key, value)

	// Replicar para todos os nós (exceto ele mesmo)
	slog.Debug("[Put] Broadcast PUT para todos os nós", "self", kv.Gossip.Self.ID, "key", key)
	replicas := 0
	failures := 0

	for id, node := range kv.Gossip.Nodes {
		if id == kv.Gossip.Self.ID {
			continue
		}
		if !kv.Gossip.IsNodeAlive(id) {
			slog.Warn("[Put] Node está down. Salvando hinted handoff.", "node", id, "key", key)
			kv.addHint(key, value, vc, id)
			continue
		}

		replicationStart := time.Now()
		slog.Info("[Put] Enviando PUT para nó", "node", id, "key", key)
		err := kv.Gossip.sendPutToNode(node, key, value, vc)

		if err != nil {
			slog.Error("[Put] Falha ao enviar PUT", "node", id, "key", key, "err", err)
			metrics.ReplicationFailuresTotal.WithLabelValues(id).Inc()
			failures++
		} else {
			slog.Info("[Put] PUT enviado com sucesso", "node", id, "key", key)
			metrics.ReplicationLatency.WithLabelValues(id).Observe(time.Since(replicationStart).Seconds())
			replicas++
		}
	}

	metrics.ReplicationSuccessTotal.WithLabelValues().Add(float64(replicas))
	if failures > 0 {
		metrics.ReplicationFailuresTotal.WithLabelValues("").Add(float64(failures))
	}
}

// broadcastPut envia a operação PUT para todos os nós exceto o próprio
// processando hinted handoff quando necessário.
func (kv *KeyValueStore) broadcastPut(key, value string, vc *vectorclock.VectorClock, excludeID string) {
	for id, node := range kv.Gossip.Nodes {
		if id == kv.Gossip.Self.ID || id == excludeID {
			continue
		}

		if !kv.Gossip.IsNodeAlive(id) {
			slog.Warn("[broadcastPut] Node está down. Salvando hinted handoff", "node", id, "key", key)
			kv.addHint(key, value, vc, id)
			continue
		}

		slog.Info("[broadcastPut] Enviando PUT", "node", id, "key", key)
		err := kv.Gossip.sendPutToNode(node, key, value, vc)
		if err != nil {
			slog.Error("[broadcastPut] Falha ao enviar PUT", "node", id, "key", key, "err", err)
		} else {
			slog.Info("[broadcastPut] PUT enviado com sucesso", "node", id, "key", key)
		}
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
	start := time.Now()
	defer func() {
		metrics.RequestDuration.WithLabelValues("GET").Observe(time.Since(start).Seconds())
		metrics.RequestsTotal.WithLabelValues("GET").Inc()
	}()

	if value, vc, found := kv.getLocal(key); found {
		return value, vc, true
	}

	vnode := kv.ConsistentHash.GetNode(key)

	if vnode.ID != kv.Gossip.Self.ID {
		if !kv.Gossip.IsNodeAlive(vnode.ID) {
			slog.Warn("Node is down, key might be in hinted handoff", "node", vnode.ID, "key", key)
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
		slog.Debug("Updated key with new value", "key", key, "vectorclock", item.VectorClock.String())
		item.Value = value
		vc = item.VectorClock
	} else {
		vc = vectorclock.NewVectorClock()

		vc.Increment(kv.Gossip.Self.ID)
		kv.Data[key] = &DataItem{Value: value, VectorClock: vc}
		slog.Debug("Stored key with initial VectorClock", "key", key, "vectorclock", vc.String())
	}

	slog.Debug("[putLocal] Persistindo chave no disco", "key", key)
	kv.writeDataToDisk(key, value)
	return vc
}

// getLocal retorna o valor armazenado localmente
func (kv *KeyValueStore) getLocal(key string) (string, *vectorclock.VectorClock, bool) {
	kv.Mutex.Lock()
	defer kv.Mutex.Unlock()

	if item, exists := kv.Data[key]; exists {
		slog.Debug("[getLocal] Chave encontrada na memória", "key", key)
		return item.Value, item.VectorClock, true
	}

	value, found := kv.readDataFromDisk(key)
	if found {
		slog.Debug("[getLocal] Chave encontrada no disco", "key", key)
		return value, nil, true
	}

	slog.Debug("[getLocal] Chave não encontrada", "key", key)
	return "", nil, false
}

// Função para ler dados de uma página do disco
func (kv *KeyValueStore) readDataFromDisk(key string) (string, bool) {
	// Busca a última ocorrência da chave no arquivo de log
	if kv.LogFile == nil {
		return "", false
	}
	stat, err := kv.LogFile.Stat()
	if err != nil {
		slog.Error("[LogFile] Falha ao obter stat", "err", err)
		return "", false
	}
	size := stat.Size()
	buf := make([]byte, size)
	_, err = kv.LogFile.ReadAt(buf, 0)
	if err != nil {
		slog.Error("[LogFile] Falha ao ler arquivo", "err", err)
		return "", false
	}
	lines := string(buf)
	var found string
	for _, line := range strings.Split(lines, "\n") {
		if strings.HasPrefix(line, key+":") {
			found = strings.TrimPrefix(line, key+":")
		}
	}
	if found != "" {
		slog.Debug("[LogFile] Valor encontrado no disco", "key", key, "value", found)
		return found, true
	}
	slog.Debug("[LogFile] Valor não encontrado no disco", "key", key)
	return "", false
}

// Função que mapeia uma chave para um ID de página
func (kv *KeyValueStore) getPageIDForKey(key string) int64 {
	return 0 // Não usado com log append-only
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
				slog.Info("[HintedHandoff] Reaplicando hinted handoff", "key", key, "node", targetID)
				if node, ok := kv.Gossip.Nodes[targetID]; ok {
					kv.Gossip.sendPutToNode(node, hint.Key, hint.Value, hint.VectorClock)
				}
				delete(hints, targetID)
			} else {
				slog.Debug("[HintedHandoff] Node ainda down, mantendo hinted handoff", "node", targetID, "key", key)
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
			slog.Info("[ResolveConflicts] Valor mais recente recebido", "key", key, "vectorclock", newVectorClock.String())
			item.Value = newValue
			item.VectorClock.Merge(newVectorClock)
		case 0: // Conflito detectado
			slog.Warn("[ResolveConflicts] Conflito detectado, realizando merge", "key", key)
			// Estratégia: merge do VectorClock e atualização do valor (pode ser customizada)
			item.VectorClock.Merge(newVectorClock)
			item.Value = newValue // ou manter ambos, se for necessário
		case 1: // Dado existente é mais recente, nenhuma atualização aplicada
			slog.Debug("[ResolveConflicts] Valor local mais recente, ignorando update", "key", key)
		}
		kv.writeDataToDisk(key, item.Value)
	} else {
		kv.Data[key] = &DataItem{
			Value:       newValue,
			VectorClock: newVectorClock,
		}
		slog.Info("[ResolveConflicts] Nova chave armazenada", "key", key, "vectorclock", newVectorClock.String())
		kv.writeDataToDisk(key, newValue)
	}
}
