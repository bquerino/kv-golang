#!/bin/bash

# Script para demonstrar identificação de leader e teste de shutdown

echo "=== Teste de Identificação de Leader e Shutdown ==="

# Função para obter status de um nó
get_node_status() {
    local port=$1
    echo "📊 Status do Node na porta $port:"
    echo "status" | nc -w 2 localhost $port 2>/dev/null || echo "❌ Node não está respondendo"
    echo
}

# Função para obter lista de nós
get_nodes_list() {
    local port=$1
    echo "📋 Lista de nós (consultado via porta $port):"
    echo "nodes" | nc -w 2 localhost $port 2>/dev/null || echo "❌ Node não está respondendo"
    echo
}

# Função para identificar o leader
identify_leader() {
    echo "🔍 Identificando o Leader atual..."
    
    for port in 8081 8082 8083; do
        echo "Consultando node na porta $port..."
        result=$(echo "status" | nc -w 2 localhost $port 2>/dev/null)
        
        if [[ $result == *"is_leader:true"* ]]; then
            leader_id=$(echo "$result" | grep -o 'node_id:[^,]*' | cut -d: -f2)
            current_leader=$(echo "$result" | grep -o 'current_leader:[^,]*' | cut -d: -f2)
            term=$(echo "$result" | grep -o 'term:[^,]*' | cut -d: -f2)
            
            echo "🎯 LEADER ENCONTRADO!"
            echo "   Leader ID: $leader_id"
            echo "   Porta: $port" 
            echo "   Term: $term"
            echo "   Status completo: $result"
            echo
            return 0
        fi
    done
    
    echo "❌ Nenhum leader encontrado ou nodes não estão rodando"
    return 1
}

# Função para testar PUT no leader
test_put_operations() {
    echo "🔧 Testando operações PUT..."
    
    for port in 8081 8082 8083; do
        echo "Testando PUT na porta $port..."
        result=$(echo "put test_key_$port value_$port" | nc -w 2 localhost $port 2>/dev/null)
        echo "   Resultado: $result"
    done
    echo
}

# Função para fazer shutdown de um nó específico
shutdown_node_by_port() {
    local target_port=$1
    echo "🔄 Fazendo shutdown do node na porta $target_port..."
    
    # Encontra o PID do processo Go rodando na porta específica
    local pid=$(lsof -ti:$target_port 2>/dev/null)
    
    if [ -n "$pid" ]; then
        echo "   PID encontrado: $pid"
        kill $pid
        sleep 2
        echo "   ✅ Node na porta $target_port foi terminado"
    else
        echo "   ❌ Nenhum processo encontrado na porta $target_port"
    fi
}

# Função para shutdown do leader atual
shutdown_current_leader() {
    echo "🎯 Fazendo shutdown do LEADER atual..."
    
    for port in 8081 8082 8083; do
        result=$(echo "status" | nc -w 2 localhost $port 2>/dev/null)
        
        if [[ $result == *"is_leader:true"* ]]; then
            leader_id=$(echo "$result" | grep -o 'node_id:[^,]*' | cut -d: -f2)
            echo "   Leader identificado: $leader_id na porta $port"
            shutdown_node_by_port $port
            return 0
        fi
    done
    
    echo "   ❌ Leader não encontrado para fazer shutdown"
    return 1
}

# Menu principal
show_menu() {
    echo "=================================="
    echo "OPÇÕES DE TESTE:"
    echo "1. Identificar Leader atual"
    echo "2. Mostrar status de todos os nós"
    echo "3. Testar operações PUT"
    echo "4. Shutdown do Leader atual"
    echo "5. Shutdown de nó específico"
    echo "6. Aguardar nova eleição"
    echo "7. Sair"
    echo "=================================="
    echo
}

# Verifica se há nós rodando
check_nodes_running() {
    echo "🔍 Verificando se há nós rodando..."
    
    local running_nodes=0
    for port in 8081 8082 8083; do
        if lsof -ti:$port >/dev/null 2>&1; then
            echo "   ✅ Node rodando na porta $port"
            ((running_nodes++))
        else
            echo "   ❌ Nenhum node na porta $port"
        fi
    done
    
    if [ $running_nodes -eq 0 ]; then
        echo
        echo "❌ ERRO: Nenhum node está rodando!"
        echo "Para iniciar os nodes em modo leader-follower, execute:"
        echo "./scripts/test-leader-follower.sh"
        echo
        return 1
    fi
    
    echo "   Total de nodes rodando: $running_nodes"
    echo
    return 0
}

# Script principal
main() {
    if ! check_nodes_running; then
        exit 1
    fi
    
    while true; do
        show_menu
        read -p "Escolha uma opção (1-7): " choice
        echo
        
        case $choice in
            1)
                identify_leader
                ;;
            2)
                for port in 8081 8082 8083; do
                    get_node_status $port
                    get_nodes_list $port
                done
                ;;
            3)
                test_put_operations
                ;;
            4)
                shutdown_current_leader
                ;;
            5)
                read -p "Digite a porta do nó para shutdown (8081/8082/8083): " port
                if [[ $port =~ ^(8081|8082|8083)$ ]]; then
                    shutdown_node_by_port $port
                else
                    echo "❌ Porta inválida"
                fi
                ;;
            6)
                echo "⏳ Aguardando nova eleição (10 segundos)..."
                sleep 10
                identify_leader
                ;;
            7)
                echo "👋 Saindo..."
                exit 0
                ;;
            *)
                echo "❌ Opção inválida"
                ;;
        esac
        
        echo
        read -p "Pressione Enter para continuar..."
        echo
    done
}

# Executa o script principal
main
