#!/bin/bash

# Script para limpeza completa do ambiente Docker
# Autor: KV-Store Team
# Uso: ./scripts/clean-docker.sh

set -e

echo "🧹 LIMPEZA COMPLETA DO AMBIENTE DOCKER"
echo "======================================"
echo ""

echo "⚠️  Este script irá:"
echo "   • Parar todos os containers do KV-Store"
echo "   • Remover volumes, networks e imagens órfãs"
echo "   • Limpar cache do Docker"
echo ""

read -p "Tem certeza que deseja continuar? (y/N): " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Operação cancelada pelo usuário."
    exit 0
fi

echo ""
echo "🛑 Parando todos os compose files..."

# Para todos os possíveis compose files
docker-compose down -v 2>/dev/null || true
docker-compose -f docker-compose-leaderless.yml down -v 2>/dev/null || true
docker-compose -f docker-compose-leader-follower.yml down -v 2>/dev/null || true
docker-compose -f docker-compose-observability.yml down -v 2>/dev/null || true
docker-compose -f docker-compose-observability-leaderless.yml down -v 2>/dev/null || true

echo "🗂️  Removendo volumes órfãos..."
docker volume prune -f

echo "🌐 Removendo networks órfãs..."
docker network prune -f

echo "📦 Removendo imagens órfãs..."
docker image prune -f

echo "🧽 Limpando cache de build..."
docker builder prune -f

echo "🔍 Removendo containers parados..."
docker container prune -f

echo ""
echo "✅ Limpeza concluída!"
echo ""
echo "📊 Status atual do Docker:"
echo "─────────────────────────"
docker system df

echo ""
echo "💡 Para iniciar novamente:"
echo "   • Leader-Follower: ./scripts/run-leader-follower.sh"
echo "   • Leaderless: ./scripts/run-leaderless.sh"
