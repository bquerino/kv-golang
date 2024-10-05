package vectorclock

// VectorClock representa um "vetor de tempo" para reconciliação de dados
type VectorClock map[string]int

// Update atualiza o VectorClock de acordo com um novo nó
func (vc VectorClock) Update(nodeID string) {
	vc[nodeID]++
}

// Compare dois VectorClocks e retorna:
// -1 se "other" é mais novo, 1 se "vc" é mais novo, e 0 se são concorrentes
func (vc VectorClock) Compare(other VectorClock) int {
	isLess, isGreater := false, false

	for nodeID, timestamp := range vc {
		otherTimestamp, exists := other[nodeID]
		if !exists || timestamp > otherTimestamp {
			isGreater = true
		} else if timestamp < otherTimestamp {
			isLess = true
		}
	}

	// Se é menor e maior simultaneamente, são concorrentes
	if isLess && isGreater {
		return 0 // Concorrente
	} else if isGreater {
		return 1 // Este VectorClock é mais recente
	}
	return -1 // O outro VectorClock é mais recente
}

// Concorrente verifica se dois VectorClocks são concorrentes
func (vc VectorClock) Concorrente(other VectorClock) bool {
	return vc.Compare(other) == 0
}
