package store

import (
	"fmt"
	"log/slog"

	"github.com/bquerino/kv-golang/internal/vectorclock"
)

type OperationMode interface {
	Put(key, value string) error
	Get(key string) (string, *vectorclock.VectorClock, bool)
	HandlePut(key, value string, vc *vectorclock.VectorClock, fromNode string) error
	HandleGet(key string) (string, *vectorclock.VectorClock, bool)
	IsWriteAllowed() bool
	IsReadAllowed() bool
}

// LeaderlessMode - implementação do modo atual (sem alterações no comportamento)
type LeaderlessMode struct {
	kvStore *KeyValueStore
	gossip  *Gossip
}

func NewLeaderlessMode(kvStore *KeyValueStore, gossip *Gossip) *LeaderlessMode {
	return &LeaderlessMode{
		kvStore: kvStore,
		gossip:  gossip,
	}
}

func (lm *LeaderlessMode) Put(key, value string) error {
	lm.kvStore.Put(key, value)
	return nil
}

func (lm *LeaderlessMode) Get(key string) (string, *vectorclock.VectorClock, bool) {
	return lm.kvStore.Get(key)
}

func (lm *LeaderlessMode) HandlePut(key, value string, vc *vectorclock.VectorClock, fromNode string) error {
	lm.kvStore.ResolveConflicts(key, value, vc)
	return nil
}

func (lm *LeaderlessMode) HandleGet(key string) (string, *vectorclock.VectorClock, bool) {
	return lm.kvStore.getLocal(key)
}

func (lm *LeaderlessMode) IsWriteAllowed() bool { return true }
func (lm *LeaderlessMode) IsReadAllowed() bool  { return true }

// LeaderFollowerMode - nova implementação para modo leader-follower
type LeaderFollowerMode struct {
	kvStore *KeyValueStore
	gossip  *Gossip
}

func NewLeaderFollowerMode(kvStore *KeyValueStore, gossip *Gossip) *LeaderFollowerMode {
	return &LeaderFollowerMode{
		kvStore: kvStore,
		gossip:  gossip,
	}
}

func (lfm *LeaderFollowerMode) Put(key, value string) error {
	if !lfm.IsWriteAllowed() {
		return fmt.Errorf("writes only allowed on leader node")
	}

	slog.Info("[LeaderFollowerMode] Processing PUT as leader", "key", key, "value", value)

	// Leader processa o write localmente
	vc := lfm.kvStore.putLocal(key, value)

	// Replica para todos os followers
	return lfm.replicateToFollowers(key, value, vc)
}

func (lfm *LeaderFollowerMode) Get(key string) (string, *vectorclock.VectorClock, bool) {
	// Reads podem ser feitos em qualquer nó (leader ou follower)
	return lfm.kvStore.getLocal(key)
}

func (lfm *LeaderFollowerMode) HandlePut(key, value string, vc *vectorclock.VectorClock, fromNode string) error {
	// Followers apenas aceitam writes do leader atual
	if !lfm.isFromCurrentLeader(fromNode) {
		return fmt.Errorf("writes only accepted from current leader, got from: %s", fromNode)
	}

	slog.Info("[LeaderFollowerMode] Applying PUT from leader", "key", key, "leader", fromNode)
	lfm.kvStore.ResolveConflicts(key, value, vc)
	return nil
}

func (lfm *LeaderFollowerMode) HandleGet(key string) (string, *vectorclock.VectorClock, bool) {
	return lfm.kvStore.getLocal(key)
}

func (lfm *LeaderFollowerMode) IsWriteAllowed() bool {
	return lfm.gossip.IsLeader
}

func (lfm *LeaderFollowerMode) IsReadAllowed() bool {
	return true // Reads permitidos em qualquer nó
}

func (lfm *LeaderFollowerMode) isFromCurrentLeader(fromNode string) bool {
	lfm.gossip.Mutex.Lock()
	defer lfm.gossip.Mutex.Unlock()
	return lfm.gossip.LeaderState.LeaderID == fromNode
}

func (lfm *LeaderFollowerMode) replicateToFollowers(key, value string, vc *vectorclock.VectorClock) error {
	var errors []error
	successCount := 0
	aliveFollowers := 0

	lfm.gossip.Mutex.Lock()
	nodes := make(map[string]*Node)
	for id, node := range lfm.gossip.Nodes {
		if id != lfm.gossip.Self.ID {
			nodes[id] = node
			// Só conta nós vivos como followers elegíveis
			if node.Alive {
				aliveFollowers++
			}
		}
	}
	lfm.gossip.Mutex.Unlock()

	// Caso especial: cluster de nó único
	if aliveFollowers == 0 {
		slog.Info("[LeaderFollowerMode] Single node cluster detected, no replication needed", "key", key)
		return nil
	}

	// Envia para todos os followers
	for id, node := range nodes {
		if !lfm.gossip.IsNodeAlive(id) {
			slog.Warn("[LeaderFollowerMode] Follower is down, storing hint", "follower", id, "key", key)
			lfm.kvStore.addHint(key, value, vc, id)
			continue
		}

		err := lfm.gossip.sendPutToNode(node, key, value, vc)
		if err != nil {
			slog.Error("[LeaderFollowerMode] Failed to replicate to follower", "follower", id, "key", key, "err", err)
			errors = append(errors, fmt.Errorf("follower %s: %w", id, err))
			// Armazena hint para retry posterior
			lfm.kvStore.addHint(key, value, vc, id)
		} else {
			successCount++
			slog.Info("[LeaderFollowerMode] Successfully replicated to follower", "follower", id, "key", key)
		}
	}

	// Política de confirmação baseada em nós vivos: requer maioria dos followers vivos
	requiredSuccesses := aliveFollowers / 2
	if aliveFollowers > 1 {
		requiredSuccesses = (aliveFollowers + 1) / 2 // Maioria real para 2+ nós
	}

	if successCount < requiredSuccesses {
		slog.Warn("[LeaderFollowerMode] Failed to replicate to majority",
			"successful", successCount, "required", requiredSuccesses, "aliveFollowers", aliveFollowers)
		return fmt.Errorf("failed to replicate to majority of followers (%d/%d successful)", successCount, aliveFollowers)
	}

	slog.Info("[LeaderFollowerMode] PUT successfully replicated", "key", key, "replicated_to", successCount, "alive_followers", aliveFollowers)
	return nil
}
