#!/bin/bash

# Script para comparar os dois modos de operação do KV-Store
# Autor: KV-Store Team
# Uso: ./scripts/compare-modes.sh

set -e

echo "⚖️  COMPARAÇÃO DOS MODOS KV-STORE"
echo "=================================="
echo ""

compare_mode() {
    local mode=$1
    local compose_file=$2
    local description=$3
    
    echo "🔄 Testando modo: $mode"
    echo "📋 Descrição: $description"
    echo "📁 Arquivo: $compose_file"
    echo ""
    
    # Limpa ambiente
    docker-compose -f $compose_file down -v 2>/dev/null || true
    sleep 2
    
    echo "🚀 Iniciando $mode..."
    
    # Executa em background e aguarda
    docker-compose -f $compose_file up --build -d
    
    # Aguarda serviços estarem prontos
    echo "⏳ Aguardando serviços ficarem prontos..."
    sleep 30
    
    # Executa teste rápido
    echo "🧪 Executando teste básico..."
    
    # Teste PUT/GET simples
    curl -s -X POST http://localhost:8080/put \
        -H "Content-Type: application/json" \
        -d '{"key":"test_'$mode'","value":"test_value"}' || echo "❌ PUT falhou"
    
    sleep 1
    
    result=$(curl -s "http://localhost:8080/get?key=test_$mode" | grep -o '"value":"[^"]*"' || echo "❌ GET falhou")
    
    if [[ $result == *"test_value"* ]]; then
        echo "✅ Teste básico passou: $result"
    else
        echo "❌ Teste básico falhou: $result"
    fi
    
    # Para o ambiente
    echo "🛑 Parando $mode..."
    docker-compose -f $compose_file down -v
    echo ""
    echo "─────────────────────────────────────"
    echo ""
}

echo "Este script irá testar ambos os modos sequencialmente:"
echo "1. Leader-Follower (Strong Consistency)"
echo "2. Leaderless (Eventual Consistency)"
echo ""
echo "⚠️  Certifique-se de que nenhum container está rodando antes de continuar."
echo ""
read -p "Pressione Enter para continuar ou Ctrl+C para cancelar..."
echo ""

# Testa Leader-Follower
compare_mode "LEADER-FOLLOWER" "docker-compose-observability.yml" "Strong consistency, single leader"

# Testa Leaderless  
compare_mode "LEADERLESS" "docker-compose-observability-leaderless.yml" "Eventual consistency, distributed"

echo "🎉 COMPARAÇÃO CONCLUÍDA!"
echo ""
echo "📊 Resumo dos Modos:"
echo "┌─────────────────┬─────────────────┬─────────────────┐"
echo "│ Aspecto         │ Leader-Follower │ Leaderless      │"
echo "├─────────────────┼─────────────────┼─────────────────┤"
echo "│ Consistência    │ Strong          │ Eventual        │"
echo "│ Disponibilidade │ Medium          │ High            │"
echo "│ Complexidade    │ Low             │ High            │"
echo "│ Latência        │ Low             │ Variable        │"
echo "└─────────────────┴─────────────────┴─────────────────┘"
echo ""
echo "💡 Para testes completos com observabilidade:"
echo "   • Leader-Follower: ./scripts/run-leader-follower.sh"
echo "   • Leaderless: ./scripts/run-leaderless.sh"
