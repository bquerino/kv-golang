package store

import (
	"crypto/sha256"
	"sort"
	"strconv"
)

// HashRing representa um anel de hash consistente
type HashRing struct {
	nodes       []int          // Lista de hashes dos nós virtuais
	nodeMap     map[int]string // Mapa do hash para o nó físico
	virtualNode int            // Número de nós virtuais por nó físico
}

// NewHashRing cria um novo anel de hash consistente
func NewHashRing(virtualNode int) *HashRing {
	return &HashRing{
		nodes:       []int{},
		nodeMap:     make(map[int]string),
		virtualNode: virtualNode,
	}
}

// AddNode adiciona um nó físico ao anel, com seus nós virtuais
func (h *HashRing) AddNode(node string) {
	for i := 0; i < h.virtualNode; i++ {
		// Criar nós virtuais, cada um com um hash diferente
		virtualNodeID := node + "#" + strconv.Itoa(i)
		hash := int(hash(virtualNodeID))
		h.nodes = append(h.nodes, hash)
		h.nodeMap[hash] = node
	}

	// Ordenar os nós virtuais para manter o anel ordenado
	sort.Ints(h.nodes)
}

// GetNode retorna o nó responsável por uma chave específica
func (h *HashRing) GetNode(key string) string {
	if len(h.nodes) == 0 {
		return ""
	}

	hash := int(hash(key))

	// Encontrar o primeiro nó com hash maior ou igual ao da chave
	idx := sort.Search(len(h.nodes), func(i int) bool {
		return h.nodes[i] >= hash
	})

	// Se passarmos do último nó, usamos o primeiro nó no anel (efeito "circular")
	if idx == len(h.nodes) {
		idx = 0
	}

	return h.nodeMap[h.nodes[idx]]
}

// GetNodes retorna múltiplos nós responsáveis pela replicação da chave
func (h *HashRing) GetNodes(key string, replicas int) []string {
	if len(h.nodes) == 0 || replicas <= 0 {
		return nil
	}

	hash := int(hash(key))
	idx := sort.Search(len(h.nodes), func(i int) bool {
		return h.nodes[i] >= hash
	})

	var result []string
	for i := 0; i < replicas; i++ {
		if idx == len(h.nodes) {
			idx = 0
		}
		result = append(result, h.nodeMap[h.nodes[idx]])
		idx++
	}

	return result
}

// Função hash simples usando SHA-256
func hash(s string) uint32 {
	hash := sha256.Sum256([]byte(s))
	return (uint32(hash[0]) << 24) | (uint32(hash[1]) << 16) | (uint32(hash[2]) << 8) | uint32(hash[3])
}
