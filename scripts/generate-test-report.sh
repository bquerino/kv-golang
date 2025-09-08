#!/bin/bash

# Script para gerar relatório completo de testes e métricas
REPORT_DIR="reports/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$REPORT_DIR"

# Detectar qual compose file está sendo usado
COMPOSE_FILE=${CURRENT_COMPOSE_FILE:-"docker-compose-observability.yml"}

echo "📁 Gerando relatório completo em: $REPORT_DIR"
echo "📋 Usando arquivo: $COMPOSE_FILE"
echo

# 1. Informações do Sistema
echo "📋 Coletando informações do sistema..."
cat > "$REPORT_DIR/system_info.txt" << EOF
KV-Store Test Report
===================
Timestamp: $(date)
Git Commit: $(git rev-parse HEAD 2>/dev/null || echo "N/A")
Git Branch: $(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "N/A")
Docker Version: $(docker --version)
Docker Compose Version: $(docker-compose --version)

System Information:
- OS: $(uname -s)
- Kernel: $(uname -r)
- Architecture: $(uname -m)
- CPU Cores: $(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo "N/A")
- Memory: $(free -h 2>/dev/null | grep Mem || echo "N/A")
EOF

# 2. Status dos Serviços
echo "🔍 Coletando status dos serviços..."
docker-compose -f "$COMPOSE_FILE" ps > "$REPORT_DIR/services_status.txt" 2>&1

# 3. Métricas Prometheus
echo "📊 Coletando métricas do Prometheus..."
PROMETHEUS_URL="http://localhost:9090"

# Função para fazer query no Prometheus
prometheus_query() {
    local query=$1
    local filename=$2
    
    curl -s -G "$PROMETHEUS_URL/api/v1/query" \
        --data-urlencode "query=$query" \
        | jq -r '.data.result[] | "\(.metric.__name__): \(.value[1])"' > "$REPORT_DIR/$filename" 2>/dev/null
}

# Coletar principais métricas
prometheus_query 'kvstore_request_duration_seconds' 'metrics_request_duration.txt'
prometheus_query 'kvstore_requests_total' 'metrics_requests_total.txt'
prometheus_query 'kvstore_replication_latency_seconds' 'metrics_replication_latency.txt'
prometheus_query 'kvstore_conflicts_total' 'metrics_conflicts.txt'
prometheus_query 'kvstore_consistency_violations_total' 'metrics_consistency_violations.txt'
prometheus_query 'kvstore_leader_elections_total' 'metrics_leader_elections.txt'

# 4. Logs dos Nós
echo "📋 Coletando logs dos nós..."
for node in node1 node2 node3; do
    echo "Coletando logs do $node..."
    docker-compose -f "$COMPOSE_FILE" logs --tail=100 $node > "$REPORT_DIR/logs_$node.txt" 2>&1
done

# 5. Executar K6 Load Test e capturar resultados
echo "🔥 Executando teste de carga K6..."
docker-compose -f "$COMPOSE_FILE" --profile testing run --rm k6 run /scripts/load-test.js > "$REPORT_DIR/k6_load_test.txt" 2>&1

# 6. Executar K6 Failover Test
echo "💥 Executando teste de failover K6..."
docker-compose -f "$COMPOSE_FILE" --profile testing run --rm k6 run /scripts/failover-test.js > "$REPORT_DIR/k6_failover_test.txt" 2>&1

# 7. Teste de Consistência Manual
echo "🧪 Executando teste de consistência manual..."
./scripts/test-consistency-checks.sh > "$REPORT_DIR/consistency_test.txt" 2>&1

# 8. Coletar métricas de recursos do sistema
echo "💻 Coletando métricas de recursos..."
cat > "$REPORT_DIR/resource_usage.txt" << EOF
Resource Usage Report
====================
Timestamp: $(date)

Docker Stats:
$(docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}")

Disk Usage:
$(df -h)

Memory Usage:
$(free -h 2>/dev/null || vm_stat 2>/dev/null || echo "N/A")

Network Connections:
$(netstat -tuln 2>/dev/null | grep -E ':(808[0-9]|909[0-9]|300[0-9])' || echo "N/A")
EOF

# 9. Análise de Performance
echo "⚡ Analisando performance..."
cat > "$REPORT_DIR/performance_analysis.txt" << EOF
Performance Analysis
===================
Timestamp: $(date)

Análise baseada nos logs do K6:

EOF

# Extrair métricas do K6 load test
if [ -f "$REPORT_DIR/k6_load_test.txt" ]; then
    echo "Load Test Results:" >> "$REPORT_DIR/performance_analysis.txt"
    grep -E "(http_req_duration|http_reqs|iterations)" "$REPORT_DIR/k6_load_test.txt" >> "$REPORT_DIR/performance_analysis.txt" 2>/dev/null
    echo "" >> "$REPORT_DIR/performance_analysis.txt"
fi

# Extrair métricas do K6 failover test  
if [ -f "$REPORT_DIR/k6_failover_test.txt" ]; then
    echo "Failover Test Results:" >> "$REPORT_DIR/performance_analysis.txt"
    grep -E "(availability|data_loss|election_time)" "$REPORT_DIR/k6_failover_test.txt" >> "$REPORT_DIR/performance_analysis.txt" 2>/dev/null
    echo "" >> "$REPORT_DIR/performance_analysis.txt"
fi

# 10. Gerar Resumo Executivo
echo "📝 Gerando resumo executivo..."
cat > "$REPORT_DIR/executive_summary.md" << EOF
# KV-Store Test Report - Executive Summary

**Report Generated:** $(date)  
**Report Location:** $REPORT_DIR

## System Overview
- **Nodes Tested:** 3-node cluster
- **Test Duration:** ~5 minutes (load + failover + consistency)
- **Protocols:** Gossip-based replication with vector clocks

## Test Coverage
✅ **Load Testing** - K6 load test with consistency checks  
✅ **Failover Testing** - Leader election and node failure scenarios  
✅ **Consistency Testing** - Read-your-writes, monotonic reads, staleness  
✅ **Performance Monitoring** - P50/P95/P99 latencies, throughput, replication lag  

## Key Metrics Monitored
- **Request Latency:** P50, P95, P99 percentiles
- **Throughput:** Requests per second (RPS)
- **Replication Lag:** Time to propagate changes
- **Conflict Rate:** Concurrent write conflicts
- **Convergence Time:** Time to resolve conflicts
- **Consistency Violations:** Read-your-writes, monotonic reads
- **Resource Usage:** CPU, Memory, Network I/O

## Files Generated
- \`system_info.txt\` - System and environment information
- \`services_status.txt\` - Docker services status
- \`metrics_*.txt\` - Prometheus metrics snapshots
- \`logs_*.txt\` - Application logs for each node
- \`k6_load_test.txt\` - Load test results with consistency checks
- \`k6_failover_test.txt\` - Failover and recovery test results
- \`consistency_test.txt\` - Manual consistency verification
- \`resource_usage.txt\` - System resource consumption
- \`performance_analysis.txt\` - Performance summary and analysis

## Next Steps
1. Review individual test files for detailed results
2. Analyze Grafana dashboards at http://localhost:3000
3. Check Prometheus metrics at http://localhost:9090
4. Investigate any failures in logs and consistency tests

## Troubleshooting
- If tests failed, check \`logs_*.txt\` for error messages
- For performance issues, review \`performance_analysis.txt\`
- For consistency violations, examine \`consistency_test.txt\`
- Resource bottlenecks can be found in \`resource_usage.txt\`
EOF

# 11. Comprimir relatório
echo "📦 Comprimindo relatório..."
tar -czf "$REPORT_DIR.tar.gz" -C "reports" "$(basename $REPORT_DIR)" 2>/dev/null

echo
echo "✅ Relatório completo gerado!"
echo "📁 Localização: $REPORT_DIR"
echo "📦 Arquivo comprimido: $REPORT_DIR.tar.gz"
echo
echo "📋 Arquivos gerados:"
ls -la "$REPORT_DIR/"
echo
echo "💡 Para visualizar o resumo executivo:"
echo "   cat $REPORT_DIR/executive_summary.md"
echo
echo "🌐 Para acessar dashboards:"
echo "   Grafana: http://localhost:3000"
echo "   Prometheus: http://localhost:9090"
