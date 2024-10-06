package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/bquerino/kv-g/internal/store"
)

func main() {
	// Parâmetros para porta, ID e modo CLI-only
	port := flag.String("port", "8081", "Porta para o nó atual")
	nodeID := flag.String("id", "node1", "ID do nó atual")
	cliOnly := flag.Bool("cli-only", false, "Rodar somente o CLI sem o protocolo Gossip")
	flag.Parse()

	// Inicializar os nós e a comunicação TCP
	gossip, err := initializeCluster(*nodeID, *port)
	if err != nil {
		log.Fatalf("Failed to initialize cluster: %v", err)
	}

	// Se não estiver no modo CLI-only, iniciar o protocolo Gossip
	if !*cliOnly {
		// Start Gossip Protocol (GossipOut)
		go gossip.StartGossip()

		// Iniciar servidor para ouvir conexões (GossipIn)
		go gossip.GossipIn()
	}

	// CLI interativa
	runCLI(gossip)
}

func initializeCluster(nodeID, port string) (*store.Gossip, error) {
	address := fmt.Sprintf("localhost:%s", port)

	gossip := store.NewGossip(nodeID, address, 3*time.Second, 3)

	// Adicionar todos os nós ao cluster
	if nodeID == "node1" {
		gossip.AddNode("node2", "localhost:8082")
		gossip.AddNode("node3", "localhost:8083")
	} else if nodeID == "node2" {
		gossip.AddNode("node1", "localhost:8081")
		gossip.AddNode("node3", "localhost:8083")
	} else if nodeID == "node3" {
		gossip.AddNode("node1", "localhost:8081")
		gossip.AddNode("node2", "localhost:8082")
	}

	return gossip, nil
}

// Função que inicia a interface CLI interativa
func runCLI(gossip *store.Gossip) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Welcome to the KV Store CLI!")
	fmt.Println("-----------------------------")

	for {
		fmt.Print("> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		args := strings.Split(input, " ")

		switch args[0] {
		case "put":
			if len(args) != 3 {
				fmt.Println("Usage: put <key> <value>")
				continue
			}
			key, value := args[1], args[2]
			gossip.Put(key, value)
		case "get":
			if len(args) != 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			key := args[1]
			value, vc, found := gossip.Get(key)
			if found {
				fmt.Printf("Value: %s, VectorClock: %v\n", value, vc)
			} else {
				fmt.Println("Key not found.")
			}
		case "delete":
			if len(args) != 2 {
				fmt.Println("Usage: delete <key>")
				continue
			}
			key := args[1]
			gossip.Delete(key)
		case "nodes":
			gossip.PrintNodes()
		case "exit":
			fmt.Println("Exiting...")
			return
		default:
			fmt.Println("Unknown command. Available commands: put, get, delete, nodes, exit")
		}
	}
}
