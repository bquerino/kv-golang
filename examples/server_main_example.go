package main

import (
	"log"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/bquerino/kv-golang/internal/config"
	"github.com/bquerino/kv-golang/internal/store"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)

	cfg := parseArgs()
	runServer(cfg)
}

func parseArgs() *config.Config {
	cfg := config.NewConfig()

	if len(os.Args) < 3 {
		log.Fatalf("Usage: server <nodeID> <port> [--mode=leaderless|leader-follower]")
	}

	cfg.NodeID = os.Args[1]
	cfg.Port = os.Args[2]

	// Parse argumentos opcionais
	for i := 3; i < len(os.Args); i++ {
		arg := os.Args[i]

		if strings.HasPrefix(arg, "--mode=") {
			modeStr := strings.TrimPrefix(arg, "--mode=")
			switch modeStr {
			case "leaderless":
				cfg.Mode = config.ModeLeaderless
			case "leader-follower":
				cfg.Mode = config.ModeLeaderFollower
			default:
				log.Fatalf("Invalid mode: %s. Valid modes: leaderless, leader-follower", modeStr)
			}
		} else if strings.HasPrefix(arg, "--election-timeout=") {
			timeoutStr := strings.TrimPrefix(arg, "--election-timeout=")
			if timeout, err := time.ParseDuration(timeoutStr); err == nil {
				cfg.ElectionTimeout = timeout
			}
		} else if strings.HasPrefix(arg, "--heartbeat-interval=") {
			intervalStr := strings.TrimPrefix(arg, "--heartbeat-interval=")
			if interval, err := time.ParseDuration(intervalStr); err == nil {
				cfg.HeartbeatInterval = interval
			}
		}
	}

	return cfg
}

func runServer(cfg *config.Config) {
	address := cfg.NodeID + ":" + cfg.Port

	// Escolhe a implementação baseada na configuração
	var gossip interface {
		StartGossip()
		GossipIn()
		AddNode(string, string)
	}

	if cfg.IsLeaderFollowerMode() {
		// Usa implementação estendida para leader-follower
		gossipExt := store.NewGossipWithConfig(cfg.NodeID, address, 3*time.Second, 3, cfg)
		gossip = gossipExt

		// Configura nós conhecidos
		nodes := map[string]string{
			"node1": "node1:8081",
			"node2": "node2:8082",
			"node3": "node3:8083",
		}
		for id, addr := range nodes {
			if id != cfg.NodeID {
				gossipExt.AddNode(id, addr)
			}
		}

		// Inicia processo de eleição em modo leader-follower
		go gossipExt.StartElectionProcess()

	} else {
		// Usa implementação original para modo leaderless
		gossipOrig := store.NewGossip(cfg.NodeID, address, 3*time.Second, 3)
		gossip = gossipOrig

		// Configura nós conhecidos
		nodes := map[string]string{
			"node1": "node1:8081",
			"node2": "node2:8082",
			"node3": "node3:8083",
		}
		for id, addr := range nodes {
			if id != cfg.NodeID {
				gossipOrig.AddNode(id, addr)
			}
		}

		// Inicia hinted handoff (funcionalidade existente)
		go gossipOrig.KeyValueStore.StartHintedHandoff()
	}

	// Inicia componentes comuns
	go gossip.StartGossip()
	go gossip.GossipIn()

	log.Printf("Node %s started in %s mode on %s", cfg.NodeID, cfg.Mode, address)

	// Mostra informações de configuração
	if cfg.IsLeaderFollowerMode() {
		log.Printf("Leader-Follower mode configuration:")
		log.Printf("  Election timeout: %v", cfg.ElectionTimeout)
		log.Printf("  Heartbeat interval: %v", cfg.HeartbeatInterval)
	} else {
		log.Printf("Leaderless mode - using eventual consistency with gossip protocol")
	}

	select {} // keep running
}
