#!/bin/bash

# Inicia node1
go run cmd/server.go node1 8081 > node1.log 2>&1
# Inicia node2
go run cmd/server.go node2 8082 > node2.log 2>&1
# Inicia node3
go run cmd/server.go node3 8083 > node3.log 2>&1
