package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"

	"github.com/bquerino/kv-g/internal/store"
)

var nodeKV *store.KVStore

// Função para encontrar uma porta livre no intervalo de 10.000 a 65.535
func getFreePortInRange(start, end int) (int, error) {
	for {
		port := rand.Intn(end-start) + start
		address := fmt.Sprintf(":%d", port)
		listener, err := net.Listen("tcp", address)
		if err == nil {
			// Porta encontrada com sucesso
			defer listener.Close()
			return port, nil
		}
		// Se a porta não estiver disponível, tentar outra
	}
}

func main() {
	// Criar anel de hash consistente com 3 nós virtuais por nó físico
	ring := store.NewHashRing(3)

	// Defina o nodeID conforme necessário
	nodeID := "node" // Ajuste o ID do nó conforme necessário

	// Criar o GossipNode e associá-lo ao KV-Store
	nodeGossip := store.NewGossipNode(nodeID, []string{}, nil, 2*time.Second, fmt.Sprintf("%s_hint.json", nodeID))
	nodeKV = store.NewKVStore(nodeID, ring, 3, nodeGossip)

	// Tentar encontrar uma porta livre no intervalo de 10000 a 65535
	port, err := getFreePortInRange(10000, 65535)
	if err != nil {
		fmt.Println("Erro ao tentar obter uma porta livre:", err)
		return
	}

	// Exibe a porta escolhida
	fmt.Printf("Nó %s rodando na porta %d\n", nodeID, port)

	// Iniciar o loop interativo para inserir e consultar chave/valor
	go startKVStoreInteraction()

	// Simular o funcionamento do servidor, aguardando conexões (apenas como placeholder)
	select {} // Impede a finalização da aplicação
}

// Função para interações do KV-Store no console
func startKVStoreInteraction() {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("Digite 'set chave valor' para inserir ou 'get chave' para consultar, ou 'sair' para finalizar:")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		if input == "sair" {
			break
		}

		parts := strings.Fields(input)
		if len(parts) < 2 {
			fmt.Println("Entrada inválida. Por favor, insira no formato 'set chave valor' ou 'get chave'.")
			continue
		}

		command := parts[0]
		key := parts[1]

		switch command {
		case "set":
			if len(parts) != 3 {
				fmt.Println("Entrada inválida. O formato para 'set' é: set chave valor.")
				continue
			}
			value := parts[2]
			nodeKV.Set(key, value)
			fmt.Printf("Chave '%s' com valor '%s' foi armazenada no KV-Store!\n", key, value)
		case "get":
			value, ok := nodeKV.Get(key)
			if ok {
				fmt.Printf("Chave '%s' possui o valor '%s'\n", key, value)
			} else {
				fmt.Printf("Chave '%s' não encontrada no KV-Store.\n", key)
			}
		default:
			fmt.Println("Comando inválido. Use 'set' ou 'get'.")
		}
	}
}
