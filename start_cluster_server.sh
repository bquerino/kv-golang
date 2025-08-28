#!/bin/bash

# Inicia node1 como servidor (sem CLI)
NODE1_HOST=localhost NODE2_HOST=localhost NODE3_HOST=localhost go run main.go --id node1 --port 8081 --cli-only &

# Inicia node2 como servidor (sem CLI)
NODE1_HOST=localhost NODE2_HOST=localhost NODE3_HOST=localhost go run main.go --id node2 --port 8082 --cli-only &

# Inicia node3 como servidor (sem CLI)
NODE1_HOST=localhost NODE2_HOST=localhost NODE3_HOST=localhost go run main.go --id node3 --port 8083 --cli-only &

# Espera todos os processos terminarem
wait
