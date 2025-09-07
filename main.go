package main

import (
	"log"
	"os"
	"time"

	"github.com/bquerino/kv-golang/internal/store"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatalf("Usage: server <nodeID> <port>")
	}

	nodeID := os.Args[1]
	port := os.Args[2]
	address := nodeID + ":" + port // Use nome do serviço Docker Compose

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
