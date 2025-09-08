#!/bin/bash

echo "🚀 KV-Store Observability - Seletor de Modo"
echo "============================================="
echo

# Função para verificar se serviço está rodando
wait_for_service() {
    local service=$1
    local port=$2
    local max_attempts=30
    local attempt=1
    
    echo "⏳ Aguardando $service ficar disponível na porta $port..."
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s "http://localhost:$port" > /dev/null 2>&1; then
            echo "✅ $service está disponível!"
            return 0
        fi
        
        echo "   Tentativa $attempt/$max_attempts..."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    echo "❌ Timeout aguardando $service"
    return 1
}

# Seleção de modo
echo "Selecione o modo de operação do KV-Store:"
echo "1. 🔄 LEADERLESS (padrão)"
echo "2. 🔄 LEADERLESS (explícito)"
echo "3. 👑 LEADER-FOLLOWER"
echo "4. 🔧 OBSERVABILITY ONLY (desenvolvimento)"
echo

read -p "Escolha uma opção (1-4): " mode_choice
echo

case $mode_choice in
    1)
        COMPOSE_FILE="docker-compose.yml"
        MODE_NAME="LEADERLESS (padrão)"
        ;;
    2)
        COMPOSE_FILE="docker-compose-leaderless.yml"
        MODE_NAME="LEADERLESS (explícito)"
        ;;
    3)
        COMPOSE_FILE="docker-compose-leader-follower.yml"
        MODE_NAME="LEADER-FOLLOWER"
        ;;
    4)
        COMPOSE_FILE="docker-compose-observability.yml"
        MODE_NAME="OBSERVABILITY ONLY"
        ;;
    *)
        echo "❌ Opção inválida, usando modo padrão (LEADERLESS)"
        COMPOSE_FILE="docker-compose.yml"
        MODE_NAME="LEADERLESS (padrão)"
        ;;
esac

echo "📋 Modo selecionado: $MODE_NAME"
echo "📁 Arquivo: $COMPOSE_FILE"
echo

# Limpar ambiente anterior
echo "🧹 Limpando ambiente anterior..."
docker-compose -f "$COMPOSE_FILE" down -v 2>/dev/null

# Configurar dependências
echo "📦 Configurando dependências Go..."
go mod tidy

# Iniciar stack completa
echo "🚀 Iniciando stack: $MODE_NAME..."
docker-compose -f "$COMPOSE_FILE" up --build -d

# Aguardar serviços ficarem disponíveis
wait_for_service "KV-Store" 8080
wait_for_service "Prometheus" 9090
wait_for_service "Grafana" 3000

echo
echo "🎉 Stack iniciada com sucesso!"
echo "📊 Modo: $MODE_NAME"
echo "📁 Arquivo: $COMPOSE_FILE"
echo
echo "🔗 URLs de Acesso:"
echo "   - KV-Store: http://localhost:8080"
echo "   - Node 1: http://localhost:8081"
echo "   - Node 2: http://localhost:8082"
echo "   - Node 3: http://localhost:8083"
echo "   - Prometheus: http://localhost:9090"
echo "   - Grafana: http://localhost:3000 (admin/admin)"
if [ "$mode_choice" != "4" ]; then
    echo "   - AlertManager: http://localhost:9093"
fi
echo

# Menu interativo
while true; do
    echo "===========================================" 
    echo "MENU DE TESTES E OBSERVABILIDADE:"
    echo "1. 📈 Abrir Grafana Dashboard"
    echo "2. 🔥 Executar Teste de Carga K6"
    echo "3. 💥 Executar Teste de Failover K6"
    echo "4. 🧪 Teste Manual de Consistência"
    echo "5. 📊 Ver Métricas Prometheus"
    echo "6. 🔍 Status dos Serviços"
    echo "7. 📋 Logs dos Nós KV-Store"
    echo "8. 🛑 Simular Falha de Nó"
    echo "9. 🔄 Reiniciar Nó"
    echo "10. 📁 Gerar Relatório de Testes"
    echo "11. 🔄 Trocar Modo de Operação"
    echo "12. 🛑 Parar Stack"
    echo "13. ❌ Sair"
    echo "==========================================="
    
    read -p "Escolha uma opção (1-13): " choice
    echo
    
    case $choice in
        1)
            echo "📈 Abrindo Grafana Dashboard..."
            if command -v xdg-open > /dev/null; then
                xdg-open "http://localhost:3000/d/kvstore-dashboard"
            elif command -v open > /dev/null; then
                open "http://localhost:3000/d/kvstore-dashboard"
            else
                echo "   Abra manualmente: http://localhost:3000/d/kvstore-dashboard"
            fi
            ;;
            
        2)
            echo "🔥 Executando Teste de Carga K6..."
            docker-compose -f "$COMPOSE_FILE" --profile testing run --rm k6 run /scripts/load-test.js
            ;;
            
        3)
            echo "💥 Executando Teste de Failover K6..."
            docker-compose -f "$COMPOSE_FILE" --profile testing run --rm k6 run /scripts/failover-test.js
            ;;
            
        4)
            echo "🧪 Executando Teste Manual de Consistência..."
            ./scripts/test-consistency-checks.sh
            ;;
            
        5)
            echo "📊 Abrindo Prometheus..."
            if command -v xdg-open > /dev/null; then
                xdg-open "http://localhost:9090/targets"
            elif command -v open > /dev/null; then
                open "http://localhost:9090/targets"
            else
                echo "   Abra manualmente: http://localhost:9090/targets"
            fi
            ;;
            
        6)
            echo "🔍 Status dos Serviços:"
            docker-compose -f "$COMPOSE_FILE" ps
            ;;
            
        7)
            echo "📋 Logs dos Nós KV-Store:"
            echo "Escolha um nó (1-3) ou 'a' para todos:"
            read -p "> " node_choice
            case $node_choice in
                1) docker-compose -f "$COMPOSE_FILE" logs -f node1 ;;
                2) docker-compose -f "$COMPOSE_FILE" logs -f node2 ;;
                3) docker-compose -f "$COMPOSE_FILE" logs -f node3 ;;
                a) docker-compose -f "$COMPOSE_FILE" logs -f node1 node2 node3 ;;
                *) echo "Opção inválida" ;;
            esac
            ;;
            
        8)
            echo "🛑 Simulando Falha de Nó:"
            echo "Escolha um nó para parar (1-3):"
            read -p "> " node_choice
            case $node_choice in
                1|2|3) 
                    docker-compose -f "$COMPOSE_FILE" stop node$node_choice
                    echo "✅ Node$node_choice parado"
                    ;;
                *) echo "Opção inválida" ;;
            esac
            ;;
            
        9)
            echo "🔄 Reiniciando Nó:"
            echo "Escolha um nó para reiniciar (1-3):"
            read -p "> " node_choice
            case $node_choice in
                1|2|3) 
                    docker-compose -f "$COMPOSE_FILE" start node$node_choice
                    echo "✅ Node$node_choice reiniciado"
                    ;;
                *) echo "Opção inválida" ;;
            esac
            ;;
            
        10)
            echo "📁 Gerando Relatório de Testes..."
            export CURRENT_COMPOSE_FILE="$COMPOSE_FILE"
            ./scripts/generate-test-report.sh
            ;;
            
        11)
            echo "🔄 Trocando Modo de Operação..."
            docker-compose -f "$COMPOSE_FILE" down
            exec "$0"  # Reinicia o script
            ;;
            
        12)
            echo "🛑 Parando Stack..."
            docker-compose -f "$COMPOSE_FILE" down
            echo "✅ Stack parada"
            ;;
            
        13)
            echo "❌ Saindo..."
            break
            ;;
            
        *)
            echo "❌ Opção inválida"
            ;;
    esac
    
    echo
    read -p "Pressione Enter para continuar..."
    echo
done

echo "👋 Obrigado por usar o KV-Store Observability Stack!"
