#!/bin/bash

# Script para executar KV-Store em modo Leader-Follower com observabilidade e testes automáticos
# Autor: KV-Store Team
# Uso: ./scripts/run-leader-follower.sh

set -e

echo "🚀 Iniciando KV-Store em modo LEADER-FOLLOWER com Observabilidade..."
echo "📊 Testes automáticos serão executados automaticamente"
echo ""

# Limpa containers anteriores
echo "🧹 Limpando containers anteriores..."
docker-compose -f docker-compose-observability.yml down -v 2>/dev/null || true

echo ""
echo "🔧 Construindo e iniciando serviços..."
echo "   - 3 nós KV-Store (leader-follower)"
echo "   - Nginx Load Balancer"
echo "   - Prometheus + Grafana + AlertManager"
echo "   - K6 Load Testing (automático)"
echo ""

# Inicia o stack
docker-compose -f docker-compose-observability.yml up --build

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
