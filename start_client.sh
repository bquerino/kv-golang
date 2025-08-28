#!/bin/bash
# Inicia cliente interativo apontando para node1
read -p "Digite o nó (node1/node2/node3): " NODE
read -p "Digite a porta (8081/8082/8083): " PORTA
start /B cmd /C "go run cmd/client.go localhost $PORTA > client_$NODE.log 2>&1"
