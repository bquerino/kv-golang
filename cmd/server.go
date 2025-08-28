package main

import (
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/bquerino/kv-g/internal/store"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)
	runServer()
}

func runServer() {
	if len(os.Args) < 3 {
		log.Fatalf("Usage: server <nodeID> <port>")
	}

	nodeID := os.Args[1]
	port := os.Args[2]
	address := nodeID + ":" + port // Use o nome do serviço Docker Compose

	gossip := store.NewGossip(nodeID, address, 3*time.Second, 3)

	nodes := map[string]string{
		"node1": "node1:8081",
		"node2": "node2:8082",
		"node3": "node3:8083",
	}
	for id, addr := range nodes {
		if id != nodeID {
			gossip.AddNode(id, addr)
		}
	}

	go gossip.StartGossip()
	go gossip.GossipIn()
	go gossip.KeyValueStore.StartHintedHandoff()

	log.Printf("Node %s started as server on %s", nodeID, address)
	select {} // keep running
}
