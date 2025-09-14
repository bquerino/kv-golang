#!/bin/bash

# Script para executar KV-Store em modo Leaderless com observabilidade e testes automáticos
# Autor: KV-Store Team
# Uso: ./scripts/run-leaderless.sh [consistency]

set -e

PROFILE=""
if [[ "$1" == "consistency" ]]; then
    PROFILE="--profile consistency-testing"
    echo "🧪 Modo: LEADERLESS com testes de consistência"
else
    echo "🚀 Modo: LEADERLESS com testes básicos"
fi

echo "📊 Testes automáticos serão executados automaticamente"
echo ""

# Limpa containers anteriores
echo "🧹 Limpando containers anteriores..."
docker-compose -f docker-compose-observability-leaderless.yml down -v 2>/dev/null || true

echo ""
echo "🔧 Construindo e iniciando serviços..."
echo "   - 3 nós KV-Store (leaderless)"
echo "   - Nginx Load Balancer"
echo "   - Prometheus + Grafana + AlertManager"
echo "   - K6 Load Testing (automático)"
if [[ "$1" == "consistency" ]]; then
    echo "   - K6 Consistency Testing (específico leaderless)"
fi
echo ""

# Inicia o stack
docker-compose -f docker-compose-observability-leaderless.yml $PROFILE up --build

echo ""
echo "✅ Stack finalizado!"
echo ""
echo "📊 Acesse os dashboards:"
echo "   • KV-Store: http://localhost:8080"
echo "   • Grafana: http://localhost:3000 (admin/admin)"
echo "   • Prometheus: http://localhost:9090"
echo "   • AlertManager: http://localhost:9094"
echo ""
echo "📁 Resultados dos testes salvos em: ./test-results/"
echo ""
echo "💡 Dica: Use './scripts/run-leaderless.sh consistency' para testes de consistência completos"
