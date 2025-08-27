package vectorclock

import (
	"fmt"
	"strconv"
	"strings"
)

type VectorClock struct {
	Clock map[string]int // Mapa que associa NodeID ao contador
}

// Inicializa um VectorClock vazio
func NewVectorClock() *VectorClock {
	return &VectorClock{
		Clock: make(map[string]int),
	}
}

// Incrementa o contador para um determinado nó (NodeID)
func (vc *VectorClock) Increment(nodeID string) {
	vc.Clock[nodeID]++
}

// Atualiza o VectorClock com outro VectorClock (merge)
func (vc *VectorClock) Merge(other *VectorClock) {
	for nodeID, counter := range other.Clock {
		if currentCounter, exists := vc.Clock[nodeID]; !exists || counter > currentCounter {
			vc.Clock[nodeID] = counter
		}
	}
}

// Compara dois Vector Clocks para determinar a relação entre eles
// Retorna:
//
//	-1: se vc é "menor" (mais antigo) que o outro
//	 1: se vc é "maior" (mais recente) que o outro
//	 0: se vc e outro estão em conflito (concorrentes)
func (vc *VectorClock) Compare(other *VectorClock) int {
	isLess := false
	isGreater := false

	for nodeID, counter := range vc.Clock {
		if otherCounter, exists := other.Clock[nodeID]; exists {
			if counter < otherCounter {
				isLess = true
			} else if counter > otherCounter {
				isGreater = true
			}
		} else {
			isGreater = true
		}
	}

	for nodeID := range other.Clock {
		if _, exists := vc.Clock[nodeID]; !exists {
			isLess = true
		}
	}

	if isLess && !isGreater {
		return -1 // vc é mais antigo
	} else if isGreater && !isLess {
		return 1 // vc é mais recente
	}
	return 0 // Conflito
}

// Retorna uma string que representa o estado atual do VectorClock
func (vc *VectorClock) String() string {
	return fmt.Sprintf("%v", vc.Clock)
}

// Serialize converte o VectorClock em uma string no formato node:counter,node:counter
func (vc *VectorClock) Serialize() string {
	parts := make([]string, 0, len(vc.Clock))
	for node, counter := range vc.Clock {
		parts = append(parts, fmt.Sprintf("%s:%d", node, counter))
	}
	return strings.Join(parts, ",")
}

// Deserialize cria um VectorClock a partir da representação em string
func Deserialize(s string) *VectorClock {
	vc := NewVectorClock()
	if s == "" {
		return vc
	}
	pairs := strings.Split(s, ",")
	for _, p := range pairs {
		elems := strings.SplitN(p, ":", 2)
		if len(elems) != 2 {
			continue
		}
		c, err := strconv.Atoi(elems[1])
		if err != nil {
			continue
		}
		vc.Clock[elems[0]] = c
	}
	return vc
}
