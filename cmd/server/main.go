package main

import (
	"log"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/bquerino/kv-golang/internal/config"
	"github.com/bquerino/kv-golang/internal/metrics"
	"github.com/bquerino/kv-golang/internal/store"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)

	cfg := parseArgs()
	runServer(cfg)
}

func parseArgs() *config.Config {
	cfg := config.NewConfig()

	slog.Info("Parsing arguments", "args", os.Args)

	if len(os.Args) < 3 {
		log.Fatalf("Usage: server <nodeID> <port> [--mode=leaderless|leader-follower]")
	}

	cfg.NodeID = os.Args[1]
	cfg.Port = os.Args[2]

	slog.Info("Basic config set", "nodeID", cfg.NodeID, "port", cfg.Port)

	// Parse argumentos opcionais
	for i := 3; i < len(os.Args); i++ {
		arg := os.Args[i]
		slog.Info("Processing argument", "index", i, "arg", arg)

		// Suporte para --mode=value e --mode value
		if strings.HasPrefix(arg, "--mode=") {
			modeStr := strings.TrimPrefix(arg, "--mode=")
			slog.Info("Found mode with =", "mode", modeStr)
			switch modeStr {
			case "leaderless":
				cfg.Mode = config.ModeLeaderless
			case "leader-follower":
				cfg.Mode = config.ModeLeaderFollower
			default:
				log.Fatalf("Invalid mode: %s. Valid modes: leaderless, leader-follower", modeStr)
			}
		} else if arg == "--mode" && i+1 < len(os.Args) {
			// Formato: --mode leader-follower
			i++
			modeStr := os.Args[i]
			slog.Info("Found mode with space", "mode", modeStr)
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
			if timeout, err := time.ParseDuration(timeoutStr + "ms"); err == nil {
				cfg.ElectionTimeout = timeout
			}
		} else if arg == "--election-timeout" && i+1 < len(os.Args) {
			// Formato: --election-timeout 5000
			i++
			timeoutStr := os.Args[i]
			if timeout, err := time.ParseDuration(timeoutStr + "ms"); err == nil {
				cfg.ElectionTimeout = timeout
			}
		} else if strings.HasPrefix(arg, "--heartbeat-interval=") {
			intervalStr := strings.TrimPrefix(arg, "--heartbeat-interval=")
			if interval, err := time.ParseDuration(intervalStr + "ms"); err == nil {
				cfg.HeartbeatInterval = interval
			}
		} else if arg == "--heartbeat-interval" && i+1 < len(os.Args) {
			// Formato: --heartbeat-interval 1000
			i++
			intervalStr := os.Args[i]
			if interval, err := time.ParseDuration(intervalStr + "ms"); err == nil {
				cfg.HeartbeatInterval = interval
			}
		}
	}

	slog.Info("Final configuration", "mode", cfg.Mode, "electionTimeout", cfg.ElectionTimeout, "heartbeatInterval", cfg.HeartbeatInterval)

	return cfg
}

func runServer(cfg *config.Config) {
	address := cfg.NodeID + ":" + cfg.Port

	// Inicializar métricas
	metrics.Init()

	// Usa o construtor com configuração
	gossip := store.NewGossipWithConfig(cfg.NodeID, address, 3*time.Second, 3, cfg)

	nodes := map[string]string{
		"node1": "node1:8081",
		"node2": "node2:8082",
		"node3": "node3:8083",
	}
	for id, addr := range nodes {
		if id != cfg.NodeID {
			gossip.AddNode(id, addr)
		}
	}

	go gossip.StartGossip()
	go gossip.GossipIn()
	go gossip.KeyValueStore.StartHintedHandoff()

	// Inicia processo de eleição se estiver em modo leader-follower
	if cfg.IsLeaderFollowerMode() {
		go gossip.StartElectionProcess()
		log.Printf("Node %s started in LEADER-FOLLOWER mode on %s", cfg.NodeID, address)
		log.Printf("Election timeout: %v, Heartbeat interval: %v", cfg.ElectionTimeout, cfg.HeartbeatInterval)
	} else {
		log.Printf("Node %s started in LEADERLESS mode on %s", cfg.NodeID, address)
	}

	// Expor métricas Prometheus
	http.Handle("/metrics", promhttp.Handler())
	metricsPort := "909" + string(cfg.Port[len(cfg.Port)-1]) // 9091, 9092, 9093
	go func() {
		log.Printf("Starting metrics server on port %s", metricsPort)
		if err := http.ListenAndServe(":"+metricsPort, nil); err != nil {
			log.Printf("Failed to start metrics server: %v", err)
		}
	}()

	select {} // keep running
}
