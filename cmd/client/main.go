package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: client <nodeAddress> <port>")
		return
	}

	nodeAddress := os.Args[1]
	port := os.Args[2]
	address := nodeAddress + ":" + port

	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("Connected to node at %s\n", address)
	for {
		fmt.Print("> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		if input == "exit" {
			fmt.Println("Exiting client...")
			break
		}

		conn, err := net.Dial("tcp", address)
		if err != nil {
			fmt.Printf("Error connecting to node: %v\n", err)
			continue
		}

		// Envia comando para o nó
		fmt.Fprintf(conn, "%s\n", input)

		// Lê resposta do nó
		respReader := bufio.NewReader(conn)
		resp, err := respReader.ReadString('\n')
		if err != nil {
			fmt.Printf("Error reading response: %v\n", err)
			conn.Close()
			continue
		}
		fmt.Printf("Response: %s", resp)
		conn.Close()
	}
}
