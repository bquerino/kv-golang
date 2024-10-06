package store

import (
	"crypto/sha1"
	"fmt"
	"log"
	"sort"
)

// Definição da estrutura ConsistentHashing
type ConsistentHashing struct {
	VNodes       int                      // Número de nós virtuais (vNodes)
	HashFunction func(data string) uint32 // Função de hash
	SortedHashes []uint32                 // Lista de hashes ordenados
	HashMap      map[uint32]*Node         // Mapa de hashes para os nós
}

// Função para criar um novo ConsistentHashing
func NewConsistentHashing(vNodes int) *ConsistentHashing {
	return &ConsistentHashing{
		VNodes:       vNodes,
		HashFunction: defaultHashFunction, // Função de hash padrão (SHA-1)
		SortedHashes: []uint32{},
		HashMap:      make(map[uint32]*Node),
	}
}

// Função de hash padrão (SHA-1)
func defaultHashFunction(data string) uint32 {
	hash := sha1.New()
	hash.Write([]byte(data))
	sum := hash.Sum(nil)
	return uint32(sum[0])<<24 | uint32(sum[1])<<16 | uint32(sum[2])<<8 | uint32(sum[3])
}

// Adiciona um nó ao anel de Consistent Hashing
func (ch *ConsistentHashing) AddNode(node *Node) {
	for i := 0; i < ch.VNodes; i++ {
		vnodeKey := fmt.Sprintf("%s-%d", node.ID, i)
		hash := ch.HashFunction(vnodeKey)

		ch.SortedHashes = append(ch.SortedHashes, hash)
		ch.HashMap[hash] = node
	}

	sort.Slice(ch.SortedHashes, func(i, j int) bool {
		return ch.SortedHashes[i] < ch.SortedHashes[j]
	})
}

// Remove um nó do anel de Consistent Hashing
func (ch *ConsistentHashing) RemoveNode(nodeID string) {
	for i := 0; i < ch.VNodes; i++ {
		vnodeKey := fmt.Sprintf("%s-%d", nodeID, i)
		hash := ch.HashFunction(vnodeKey)

		delete(ch.HashMap, hash)

		// Remover o hash da lista de hashes ordenados
		for idx, h := range ch.SortedHashes {
			if h == hash {
				ch.SortedHashes = append(ch.SortedHashes[:idx], ch.SortedHashes[idx+1:]...)
				break
			}
		}
	}
}

// Retorna o nó apropriado para uma chave, baseado no Consistent Hashing
func (ch *ConsistentHashing) GetNode(key string) *Node {
	if len(ch.SortedHashes) == 0 {
		log.Panicln("No nodes available in the Consistent Hashing ring.")
		return nil
	}

	hash := ch.HashFunction(key)
	idx := sort.Search(len(ch.SortedHashes), func(i int) bool {
		return ch.SortedHashes[i] >= hash
	})

	// Se não encontrar um valor maior, retorna o primeiro nó
	if idx == len(ch.SortedHashes) {
		idx = 0
	}

	return ch.HashMap[ch.SortedHashes[idx]]
}
